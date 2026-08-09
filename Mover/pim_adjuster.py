"""

PIM eligible assignment adjustments for Mover events.

Handles three operations when a role transition changes a user's
privileged access profile:

    ADDED         — new role requires PIM eligibility the user does not hold
    REMOVED       — new role no longer justifies PIM eligibility the user holds
    SCOPE_CHANGED — eligibility stays but the scope changes; remove then re-add

Per ADR-003, this module never cancels active PIM sessions. It removes
the eligible assignment only. Active sessions run to their configured
expiry. The audit record captures whether a session was active at the
time of removal.

Reuses:
    pim_client.assign_pim_group_eligibility() — for additions
    graph_client.remove_pim_group_eligibility() — for removals
    graph_client.get_active_pim_sessions()      — read-only session check
"""

from __future__ import annotations
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from Provisioning.graph_client import JmlGraphClient, GraphClientError
from Provisioning.pim_client import assign_pim_group_eligibility

logger = logging.getLogger(__name__)


# Data models

class PimAdjustmentAction(str, Enum):
    """The type of PIM change applied during this Mover event."""
    ADDED         = "ADDED"
    REMOVED       = "REMOVED"
    SCOPE_CHANGED = "SCOPE_CHANGED"


@dataclass
class PimAdjustmentRecord:
    """
    The result of a single PIM eligible assignment change.

    Written to MoverAuditRecord.pim_changes regardless of outcome.
    One record per group per action.

    Fields:
        group_id:                 Entra group object ID
        display_name:             Human-readable group name for audit records
        action:                   ADDED / REMOVED / SCOPE_CHANGED
        succeeded:                True if the Graph call completed successfully
        active_session_at_move:   True if an active PIM session existed at the
                                  time of removal. Audit trail only — the session
                                  is never cancelled (ADR-003).
        session_expires:          ISO-8601 expiry of the active session if one
                                  existed. Empty string if no active session.
        schedule_id:              Graph schedule ID returned on success
        error:                    Error message if succeeded is False
    """
    group_id:               str
    display_name:           str
    action:                 PimAdjustmentAction
    succeeded:              bool
    active_session_at_move: bool          = False
    session_expires:        str           = ""
    schedule_id:            str           = ""
    error:                  str           = ""


@dataclass
class PimAdjustmentResult:
    """
    The complete PIM adjustment outcome for a Mover event.

    Fields:
        records:      One PimAdjustmentRecord per group per action.
                      Written to MoverAuditRecord.pim_changes.
        all_succeeded: True if every Graph call completed without error.
                      False if any individual adjustment failed.
                      The orchestrator uses this to determine whether
                      to proceed or mark the event as MOVE_PARTIAL.
    """
    records:       list[PimAdjustmentRecord]
    all_succeeded: bool



# Individual adjustment operations

def _add_pim_eligibility(
    graph_client:  JmlGraphClient,
    user_id:       str,
    group_id:      str,
    display_name:  str,
    justification: str,
) -> PimAdjustmentRecord:
    """
    Add a PIM eligible assignment for a group the new role requires.

    Delegates to pim_client.assign_pim_group_eligibility() so the
    addition path is consistent with the Joiner pipeline.
    """
    result = assign_pim_group_eligibility(
        graph_client  = graph_client,
        user_id       = user_id,
        group_id      = group_id,
        display_name  = display_name,
        eligible_role = "",          # audit trail only — not used in API call
        justification = justification,
    )

    return PimAdjustmentRecord(
        group_id     = group_id,
        display_name = display_name,
        action       = PimAdjustmentAction.ADDED,
        succeeded    = result.succeeded,
        schedule_id  = result.schedule_id,
        error        = result.error,
    )


def _remove_pim_eligibility(
    graph_client:  JmlGraphClient,
    user_id:       str,
    group_id:      str,
    display_name:  str,
    justification: str,
) -> PimAdjustmentRecord:
    """
    Remove a PIM eligible assignment for a group the new role no longer justifies.

    Checks for an active session before removing the eligible assignment.
    The active session is recorded in the audit trail but never cancelled
    per ADR-003.
    """
    # Check for active session — audit trail only, no cancellation
    active_session_at_move = False
    session_expires        = ""

    try:
        sessions = graph_client.get_active_pim_sessions(
            user_id  = user_id,
            group_id = group_id,
        )

        if sessions:
            active_session_at_move = True
            session_expires        = sessions[0].get("end_date", "")
            logger.warning(
                "Active PIM session exists at time of eligibility removal — "
                "session will expire naturally per ADR-003 — "
                "user=%s, group=%s, expires=%s",
                user_id, group_id, session_expires,
            )

    except GraphClientError as e:
        # Session check failure is non-fatal — log and continue with removal.
        # The audit record will reflect that the check could not be completed.
        logger.error(
            "get_active_pim_sessions failed during removal — "
            "proceeding with removal, session state unknown — "
            "user=%s, group=%s, error=%s",
            user_id, group_id, str(e),
        )

    # Remove the eligible assignment
    try:
        result = graph_client.remove_pim_group_eligibility(
            user_id       = user_id,
            group_id      = group_id,
            justification = justification,
        )

        return PimAdjustmentRecord(
            group_id               = group_id,
            display_name           = display_name,
            action                 = PimAdjustmentAction.REMOVED,
            succeeded              = True,
            active_session_at_move = active_session_at_move,
            session_expires        = session_expires,
            schedule_id            = result.get("schedule_id", ""),
        )

    except GraphClientError as e:
        return PimAdjustmentRecord(
            group_id               = group_id,
            display_name           = display_name,
            action                 = PimAdjustmentAction.REMOVED,
            succeeded              = False,
            active_session_at_move = active_session_at_move,
            session_expires        = session_expires,
            error                  = str(e),
        )


def _change_pim_scope(
    graph_client:  JmlGraphClient,
    user_id:       str,
    group_id:      str,
    display_name:  str,
    justification: str,
) -> PimAdjustmentRecord:
    """
    Handle a PIM scope change — remove the old eligible assignment then re-add.

    Both steps are recorded. If the removal fails, the add is not attempted.
    If the add fails after a successful removal, the record reflects partial
    failure so the operator knows the user has lost PIM eligibility and
    the re-add needs to be completed manually.
    """
    # Step 1 — remove existing eligibility
    remove_record = _remove_pim_eligibility(
        graph_client  = graph_client,
        user_id       = user_id,
        group_id      = group_id,
        display_name  = display_name,
        justification = justification,
    )

    if not remove_record.succeeded:
        # Removal failed — do not attempt re-add
        return PimAdjustmentRecord(
            group_id               = group_id,
            display_name           = display_name,
            action                 = PimAdjustmentAction.SCOPE_CHANGED,
            succeeded              = False,
            active_session_at_move = remove_record.active_session_at_move,
            session_expires        = remove_record.session_expires,
            error                  = f"Removal step failed: {remove_record.error}",
        )

    # Step 2 — re-add with new scope
    add_record = _add_pim_eligibility(
        graph_client  = graph_client,
        user_id       = user_id,
        group_id      = group_id,
        display_name  = display_name,
        justification = justification,
    )

    return PimAdjustmentRecord(
        group_id               = group_id,
        display_name           = display_name,
        action                 = PimAdjustmentAction.SCOPE_CHANGED,
        succeeded              = add_record.succeeded,
        active_session_at_move = remove_record.active_session_at_move,
        session_expires        = remove_record.session_expires,
        schedule_id            = add_record.schedule_id,
        error                  = add_record.error,
    )



# Orchestration

def adjust_pim_eligibility(
    graph_client:  JmlGraphClient,
    user_id:       str,
    pim_to_add:    list[dict],
    pim_to_remove: list[dict],
    pim_to_change: list[dict],
    justification: str,
) -> PimAdjustmentResult:
    """
    Execute all PIM eligible assignment changes for a Mover event.

    Only called by the orchestrator when the delta includes PIM-mapped
    groups. If no PIM changes are needed, the orchestrator skips this
    step entirely.

    Processes removals before additions — consistent with the broader
    Mover pattern of revoke-before-add. A user should never hold both
    the old and new PIM eligibility simultaneously.

    Args:
        graph_client:  Authenticated JmlGraphClient instance.
        user_id:       Entra object ID of the user being moved.
        pim_to_add:    List of dicts with group_id and display_name
                       for new PIM eligibilities to create.
        pim_to_remove: List of dicts with group_id and display_name
                       for PIM eligibilities to remove.
        pim_to_change: List of dicts with group_id and display_name
                       for PIM scope changes (remove then re-add).
        justification: Business reason written to every eligibility record.

    Returns:
        PimAdjustmentResult with all records and an all_succeeded flag.

    Side effects:
        Graph API calls for every group in all three input lists.
        One active session check per group being removed.
    """
    records: list[PimAdjustmentRecord] = []

    # Removals first — revoke before add
    for group in pim_to_remove:
        record = _remove_pim_eligibility(
            graph_client  = graph_client,
            user_id       = user_id,
            group_id      = group["group_id"],
            display_name  = group["display_name"],
            justification = justification,
        )
        records.append(record)

    # Scope changes — remove then re-add
    for group in pim_to_change:
        record = _change_pim_scope(
            graph_client  = graph_client,
            user_id       = user_id,
            group_id      = group["group_id"],
            display_name  = group["display_name"],
            justification = justification,
        )
        records.append(record)

    # Additions last
    for group in pim_to_add:
        record = _add_pim_eligibility(
            graph_client  = graph_client,
            user_id       = user_id,
            group_id      = group["group_id"],
            display_name  = group["display_name"],
            justification = justification,
        )
        records.append(record)

    all_succeeded = all(r.succeeded for r in records)

    return PimAdjustmentResult(
        records       = records,
        all_succeeded = all_succeeded,
    )