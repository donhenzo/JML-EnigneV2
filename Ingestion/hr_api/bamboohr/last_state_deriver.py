"""
Last-State Deriver

Responsibility: determine the lifecycle action for an incoming HR record
by comparing it against the employee's last successfully reconciled state
in the JmlLastState table.

This replaces the Entra-based action_deriver.py for the webhook path.
The Entra deriver required a live Graph API call per employee and compared
against Entra's current attributes. This deriver compares against what
JML itself last processed — a cleaner, faster, and more reliable signal.

Decision rules (agreed design):

    No last state + Active status     → JOINER  (new employee, first time seen)
    No last state + Inactive status   → SKIP    (terminated before we knew them)
    Last state Active → now Inactive  → LEAVER  (termination event)
    Last state Inactive → now Active  → JOINER  (rehire — treat as new Joiner)
    Last state Active → still Active  → check fields:
        Action-driving diff found     → MOVER
        No meaningful diff            → SKIP
    Last state Active → still Active
        but non-driving fields only   → SKIP    (name change etc. — not a Mover)

Action-driving fields — only these trigger a Mover event:
    department, job_title, employment_type, manager_id, location

Non-driving fields (display_name, upn, start_date) are stored in the
last state for reference but do NOT trigger Mover events. A name change
alone is not a lifecycle event.

Separation of concerns:
    last_state_store.py   → read/write the JmlLastState table
    last_state_deriver.py → compare and derive action (this file)
    webhook.py            → wires fetch → map → derive → dispatch
"""

import logging

logger = logging.getLogger(__name__)

ACTION_JOINER = "Joiner"
ACTION_MOVER = "Mover"
ACTION_LEAVER = "Leaver"
ACTION_SKIP = "Skip"

# Only changes to these fields trigger a Mover event.
# Everything else is stored but not action-driving.
ACTION_DRIVING_FIELDS = frozenset({
    "department",
    "job_title",
    "employment_type",
    "manager_id",
    "location",
})

# HR status values that indicate termination — case-insensitive.
TERMINATION_STATUSES = frozenset({"inactive", "terminated"})


def derive_action(mapped_record: dict, last_state: dict | None) -> str:
    """
    Determine the lifecycle action by comparing the incoming mapped record
    against the last reconciled state.

    Args:
        mapped_record: dict from bamboohr_mapper.map_to_raw_identity()
                       Must contain at minimum: employee_id, status, and
                       the action-driving fields.
        last_state:    dict from last_state_store.get_last_state(), or None
                       if the employee has no prior state.

    Returns:
        "Joiner" | "Mover" | "Leaver" | "Skip"
    """
    employee_id = mapped_record.get("employee_id", "unknown")
    incoming_status = (mapped_record.get("status") or "").strip().lower()
    is_terminated = incoming_status in TERMINATION_STATUSES

    # Check for explicit Leaver override (e.g. from CSV with Action=Leaver)
    explicit_action = (mapped_record.get("action") or "").strip().lower()
    if explicit_action == "leaver":
        is_terminated = True

    # No last state — first time we've seen this employee
    if last_state is None:
        return _derive_no_prior_state(employee_id, is_terminated)

    # Has last state — compare
    last_status = (last_state.get("status") or "").strip().lower()
    was_terminated = last_status in TERMINATION_STATUSES

    return _derive_with_prior_state(
        employee_id, mapped_record, last_state,
        is_terminated, was_terminated,
    )


def _derive_no_prior_state(employee_id: str, is_terminated: bool) -> str:
    """
    No prior state in the last-state store.

    Active   → Joiner (brand new employee)
    Inactive → Skip   (terminated before we knew about them — nothing to do)
    """
    if is_terminated:
        logger.info(
            "Employee %s has no last state and is terminated — Skip "
            "(already gone before we tracked them)",
            employee_id,
        )
        return ACTION_SKIP

    logger.info(
        "Employee %s has no last state and is active — Joiner",
        employee_id,
    )
    return ACTION_JOINER


def _derive_with_prior_state(
    employee_id: str,
    mapped_record: dict,
    last_state: dict,
    is_terminated: bool,
    was_terminated: bool,
) -> str:
    """
    Prior state exists — compare current record against it.
    """
    # Active → Terminated: this is a Leaver
    if is_terminated and not was_terminated:
        logger.info(
            "Employee %s was active, now terminated — Leaver",
            employee_id,
        )
        return ACTION_LEAVER

    # Terminated → Active: this is a rehire — treat as new Joiner
    if not is_terminated and was_terminated:
        logger.info(
            "Employee %s was terminated, now active — Joiner (rehire)",
            employee_id,
        )
        return ACTION_JOINER

    # Terminated → still Terminated: nothing to do
    if is_terminated and was_terminated:
        logger.debug(
            "Employee %s still terminated — Skip",
            employee_id,
        )
        return ACTION_SKIP

    # Active → still Active: check for attribute changes
    changed_fields = _find_changed_driving_fields(mapped_record, last_state)

    if changed_fields:
        logger.info(
            "Employee %s — action-driving fields changed: %s — Mover",
            employee_id,
            ", ".join(sorted(changed_fields)),
        )
        return ACTION_MOVER

    logger.debug(
        "Employee %s — no action-driving changes detected — Skip",
        employee_id,
    )
    return ACTION_SKIP


def _find_changed_driving_fields(
    mapped_record: dict,
    last_state: dict,
) -> list[str]:
    """
    Compare only the action-driving fields between the incoming record
    and the last state. Returns the list of field names that changed.

    Comparison is case-insensitive and whitespace-stripped to avoid
    false Mover triggers from formatting differences.
    """
    changed = []

    for field in ACTION_DRIVING_FIELDS:
        incoming = _normalise(mapped_record.get(field, ""))
        previous = _normalise(last_state.get(field, ""))

        if incoming != previous:
            changed.append(field)

    return changed


def _normalise(value: str) -> str:
    """
    Minimal normalisation for comparison — strip and lowercase.
    Same logic as action_deriver._normalise_for_comparison().
    """
    if value is None:
        return ""
    return str(value).strip().lower()
