"""
Action Deriver

Responsibility: determine what lifecycle event a BambooHR record represents
before it enters the JML pipeline.

This module answers one question: given a record from the HR system,
is this a Joiner, a Mover, a Leaver, or nothing we need to act on?

The decision is based on whether the identity already exists in Entra ID,
whether their attributes have changed, and whether the HR system indicates
a termination.

Decision logic:
    1. Check for termination signal first — if the record indicates the
       employee is terminated, this is a Leaver regardless of anything
       else. Two signals are recognised:
         a. Explicit action override: action == "Leaver" (e.g. from CSV)
         b. HR status field: status == "Inactive" (BambooHR convention)
       If the user doesn't exist in Entra for a Leaver → Skip (already
       gone, nothing to offboard).
    2. Look up the UPN in Entra ID via Graph API
    3. If user does NOT exist → Joiner
    4. If user EXISTS:
        a. Compare department and job title from HR against Entra
        b. If either changed → Mover
        c. If nothing changed → Skip (no action needed)

Separation of concerns:
    bamboohr_client.py   → fetches raw data from BambooHR
    bamboohr_mapper.py   → translates field names
    action_deriver.py    → determines Joiner / Mover / Leaver / Skip (this file)
    ingestion_coordinator.py → wires everything together
"""

import logging

logger = logging.getLogger(__name__)


# Return values — plain strings, not enums, because this runs before
# the record enters the pipeline where JmlAction enums are constructed.
ACTION_JOINER = "Joiner"
ACTION_MOVER = "Mover"
ACTION_LEAVER = "Leaver"
ACTION_SKIP = "Skip"

# HR status values that indicate termination. BambooHR uses "Inactive"
# when an employee is terminated. Add other values here if a different
# HR source uses different terminology — the check is case-insensitive.
TERMINATION_STATUSES = frozenset({"inactive", "terminated"})


def derive_action(mapped_record: dict, graph_client) -> str:
    """
    Determine the lifecycle action for a mapped HR record.

    Args:
        mapped_record: dict from bamboohr_mapper.map_to_raw_identity()
                       Must contain: upn, department, job_title, employee_id
                       May contain: status (BambooHR employment status),
                                    action (explicit override from CSV)
        graph_client:  JmlGraphClient instance for Entra ID lookups

    Returns:
        "Joiner" — identity does not exist in Entra, needs provisioning
        "Mover"  — identity exists but department or job title changed
        "Leaver" — identity is terminated and exists in Entra
        "Skip"   — identity exists and nothing meaningful changed, or
                   identity is terminated but already gone from Entra
    """
    upn = mapped_record.get("upn", "")
    employee_id = mapped_record.get("employee_id", "")

    if not upn:
        logger.warning(
            "No UPN for employee %s — cannot derive action, defaulting to Joiner",
            employee_id
        )
        return ACTION_JOINER

    # Check for termination signal before anything else. A Leaver
    # takes priority over Joiner/Mover classification — if the HR
    # system says this person is terminated, it doesn't matter
    # whether their department also changed.
    if _is_termination(mapped_record):
        return _derive_leaver(upn, employee_id, graph_client)

    # Not a termination — run the existing Joiner/Mover/Skip logic
    return _derive_joiner_or_mover(mapped_record, upn, employee_id, graph_client)


def _is_termination(mapped_record: dict) -> bool:
    """
    Return True if the HR record indicates a termination event.

    Two signals, checked in order:
      1. Explicit action override (e.g. from CSV with Action=Leaver)
      2. HR status field (BambooHR sends status="Inactive" on termination)
    """
    explicit_action = mapped_record.get("action", "")
    if explicit_action.lower() == "leaver":
        return True

    status = mapped_record.get("status", "")
    if status.strip().lower() in TERMINATION_STATUSES:
        return True

    return False


def _derive_leaver(upn: str, employee_id: str, graph_client) -> str:
    """
    Confirm the user exists in Entra before classifying as Leaver.

    If the user is already gone (404), there's nothing to offboard —
    return Skip rather than feeding a Leaver event that would fail at
    Step 1's user fetch.
    """
    try:
        graph_client.get_user(upn)
    except Exception as e:
        error_msg = str(e).lower()
        if "not found" in error_msg or "404" in error_msg:
            logger.info(
                "User %s not found in Entra — already gone, skipping "
                "Leaver (employee: %s)",
                upn, employee_id
            )
            return ACTION_SKIP

        # Graph API error — cannot confirm the user exists. Default to
        # Leaver rather than Skip — it's safer to attempt offboarding
        # and have the pipeline fail at Step 1 than to silently skip
        # a termination event because of a transient network error.
        logger.warning(
            "Graph API error checking %s for Leaver — defaulting to "
            "Leaver anyway (employee: %s): %s",
            upn, employee_id, e
        )
        return ACTION_LEAVER

    logger.info(
        "User %s exists in Entra and HR indicates termination — "
        "action: Leaver (employee: %s)",
        upn, employee_id
    )
    return ACTION_LEAVER


def _derive_joiner_or_mover(
    mapped_record: dict,
    upn: str,
    employee_id: str,
    graph_client,
) -> str:
    """
    Original Joiner/Mover/Skip logic, unchanged from before.
    Extracted into its own function so the termination check runs first.
    """
    try:
        existing_user = graph_client.get_user(upn)
    except Exception as e:
        error_msg = str(e).lower()
        if "not found" in error_msg or "404" in error_msg:
            logger.info(
                "User %s not found in Entra — action: Joiner (employee: %s)",
                upn, employee_id
            )
            return ACTION_JOINER

        logger.error(
            "Graph API error checking %s — cannot derive action, skipping: %s",
            upn, e
        )
        return ACTION_SKIP

    entra_department = existing_user.get("department") or ""
    entra_job_title = existing_user.get("job_title") or ""
    hr_department = mapped_record.get("department", "")
    hr_job_title = mapped_record.get("job_title", "")

    dept_changed = _normalise_for_comparison(hr_department) != _normalise_for_comparison(entra_department)
    title_changed = _normalise_for_comparison(hr_job_title) != _normalise_for_comparison(entra_job_title)

    if dept_changed or title_changed:
        logger.info(
            "User %s exists — attributes changed — action: Mover (employee: %s, "
            "dept: '%s' → '%s', title: '%s' → '%s')",
            upn, employee_id,
            entra_department, hr_department,
            entra_job_title, hr_job_title
        )
        return ACTION_MOVER

    logger.debug(
        "User %s exists — no changes detected — action: Skip (employee: %s)",
        upn, employee_id
    )
    return ACTION_SKIP


def _normalise_for_comparison(value: str) -> str:
    """
    Minimal normalisation for comparing HR values against Entra values.
    Strips whitespace and lowercases so 'Sales' matches 'sales' and
    ' Sales ' matches 'Sales'.

    This is NOT the canonical normalisation — that happens in the pipeline.
    This is just enough to avoid false Mover triggers from case differences.
    """
    return value.strip().lower()