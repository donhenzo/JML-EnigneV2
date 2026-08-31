"""
Leaver/stages.py

The Leaver pipeline decomposed into independently-testable stages, mirroring
Mover/stages.py. Each stage returns a StageResult (plain, serializable), never
touches the audit_record, and never decides control flow. The driver (sync
today, Durable orchestrator later) reads each StageResult and decides.

This file holds the zero-wait stages (claim → conflict → concurrent → fetch →
disable → revoke → pim → soft delete → verify) plus the helpers moved out of
leaver_http so a Durable activity can reach them without importing the trigger
module. The removal submit/poll/finalize seam lives in
Leaver/provisioning_phases.py, one layer below.

The Leaver is structurally simpler than the Mover: no entitlement resolution,
no delta, no retention, no ADR-009 add-before-remove gate. It has extra
front-of-pipeline shape instead — a conflict-supersede step, and the
disable/revoke fail-safe (ADR-015) that must run before any removal.

Import direction is one-way: this module never imports from leaver_http.
leaver_http composes these stages; nothing here reaches back up.

Client injection: every stage takes its clients as parameters. The sync driver
passes shared clients; a Durable activity passes freshly-built ones. No stage
constructs a client at module scope.
"""

from __future__ import annotations
import json
import logging
import os
import uuid as _uuid
from datetime import datetime, timezone, timedelta

from azure.data.tables import TableServiceClient

from Ingestion.schema import IdentityPayload
from Provisioning.graph_client import JmlGraphClient, GraphClientError, UserNotFoundError
from Leaver.stage_result import StageResult, StageOutcome
from Functions.Event_store.event_store import (
    claim_event,
    acquire_lock,
)
from Functions.Event_store.conflict_queue import check_and_handle_conflict

logger = logging.getLogger(__name__)

LEAVER_EVENT_LOG_TABLE = "LeaverEventLog"
STALE_LOCK_MINUTES     = 10

# Days to hold before soft-deleting the user object (Step 6). Everything before
# this step has already cut off access, so a nonzero hold is safe — it gives
# operators a window for manual review before the identity leaves the directory.
# Default is immediate deletion. Same env var and default as the pre-refactor
# leaver_http.
SOFT_DELETE_HOLD_DAYS = int(os.environ.get("JML_LEAVER_SOFT_DELETE_HOLD_DAYS", "0"))


def soft_delete_hold_seconds() -> int:
    """
    Resolve the soft-delete hold as seconds. JML_LEAVER_SOFT_DELETE_HOLD_SECONDS
    is a test override — when set (nonzero) it wins, so the durable deferred-delete
    timer can be proven in minutes instead of days. Otherwise the hold is
    JML_LEAVER_SOFT_DELETE_HOLD_DAYS converted to seconds. Zero from both means
    immediate deletion.
    """
    seconds_override = int(os.environ.get("JML_LEAVER_SOFT_DELETE_HOLD_SECONDS", "0"))
    if seconds_override > 0:
        return seconds_override
    return SOFT_DELETE_HOLD_DAYS * 86400


def soft_delete_is_deferred() -> bool:
    """True if any hold is configured (days or the seconds test override)."""
    return soft_delete_hold_seconds() > 0


# Helpers moved from leaver_http — live here so a Durable activity can reach
# them without importing the trigger module.

def _is_stale_in_progress(row: dict) -> bool:
    """
    True if an IN_PROGRESS event log row is older than STALE_LOCK_MINUTES.

    A 504-killed run leaves its EventLog row IN_PROGRESS with no terminal
    write. Without this, a reclaimed retry is blocked as QUEUED_CONCURRENT even
    though the JmlEvents lock has already been reclaimed. Same window as the
    event-store lock, so both agree on when a run is dead.
    """
    updated_at = row.get("updated_at", "")
    if not updated_at:
        return False
    try:
        updated = datetime.fromisoformat(updated_at)
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - updated > timedelta(minutes=STALE_LOCK_MINUTES)
    except (ValueError, TypeError):
        return True


def check_concurrent_event(
    table_client: TableServiceClient,
    employee_id:  str,
) -> bool:
    """
    True if a genuinely-live IN_PROGRESS Leaver event exists for this employee.
    A stale IN_PROGRESS row (older than STALE_LOCK_MINUTES) is logged and
    skipped so a reclaimed retry can proceed. Fails closed on a query error
    (returns True) — the same conservative posture as the pre-refactor code.
    """
    try:
        client   = table_client.get_table_client(LEAVER_EVENT_LOG_TABLE)
        entities = client.query_entities(
            query_filter=(
                f"PartitionKey eq '{employee_id}' and status eq 'IN_PROGRESS'"
            )
        )
        for row in entities:
            if _is_stale_in_progress(row):
                logger.warning(
                    "Stale IN_PROGRESS event log row ignored — employee=%s, "
                    "event=%s, updated_at=%s. Prior run likely killed before "
                    "terminal write (504). Allowing reclaimed retry to proceed.",
                    employee_id, row.get("RowKey", ""), row.get("updated_at", ""),
                )
                continue
            return True
        return False
    except Exception as e:
        logger.error(
            "LeaverEventLog concurrent check failed — employee=%s, error=%s",
            employee_id, str(e),
        )
        return True


# Pre-step stages

def stage_claim(
    payload:           IdentityPayload,
    event_id:          str,
    jml_events_client: TableServiceClient,
) -> StageResult:
    """
    Pre-Step — atomic claim in JmlEvents. A duplicate claim is the idempotency
    exit; the driver maps DUPLICATE to the QUEUED_CONCURRENT response string the
    current pipeline returns.
    """
    payload_json = json.dumps({
        "employee_id": payload.employee_id,
        "action":      "Leaver",
        "event_id":    event_id,
    })
    claimed = claim_event(
        table_client   = jml_events_client,
        employee_id    = payload.employee_id,
        action         = "Leaver",
        start_date     = payload.start_date.isoformat(),
        payload_json   = payload_json,
        correlation_id = event_id,
    )
    if not claimed:
        return StageResult(
            ok=True,
            outcome=StageOutcome.DUPLICATE,
            summary="Duplicate event — already claimed in JmlEvents.",
        )
    return StageResult(ok=True, outcome=StageOutcome.PROCEED)


def stage_conflict_check(
    payload:           IdentityPayload,
    event_id:          str,
    jml_events_client: TableServiceClient,
) -> StageResult:
    """
    Pre-Step — conflict handling. For a Leaver this supersedes every Pending
    Joiner/Mover event for the employee and returns SUPERSEDE, which means
    "you have priority, proceed" — not "you were superseded". A Processing
    event (holding a live lock) is left alone; the stale-lock reclaim resets it
    later.

    The outcome is logged and carried in data for observability, but a Leaver
    always proceeds regardless — the driver does not branch on it, matching the
    pre-refactor behaviour.
    """
    outcome = check_and_handle_conflict(
        table_client = jml_events_client,
        employee_id  = payload.employee_id,
        new_event_id = event_id,
        new_action   = "Leaver",
    )
    logger.info(
        "Conflict check — employee=%s, outcome=%s",
        payload.employee_id, outcome,
    )
    return StageResult(
        ok=True,
        outcome=StageOutcome.PROCEED,
        data={"conflict_outcome": outcome},
    )


def stage_concurrent_check(
    payload:      IdentityPayload,
    table_client: TableServiceClient,
) -> StageResult:
    """
    Step 1a — concurrent-event guard via LeaverEventLog (stale-IN_PROGRESS
    window applied). The driver owns the LeaverEventLog writes (IN_PROGRESS on
    proceed, QUEUED_CONCURRENT on queue) — those are audit/state side effects,
    not stage logic.
    """
    if check_concurrent_event(table_client, payload.employee_id):
        return StageResult(
            ok=True,
            outcome=StageOutcome.QUEUED,
            summary="Another Leaver event is in progress for this employee.",
        )
    return StageResult(ok=True, outcome=StageOutcome.PROCEED)


def stage_fetch_current_state(
    payload:           IdentityPayload,
    event_id:          str,
    graph_client:      JmlGraphClient,
    jml_events_client: TableServiceClient,
) -> StageResult:
    """
    Step 1b — fetch the user and current delivered package assignments, build
    current_packages / current_policy_map / package_labels, and acquire the
    JmlEvents lock (post-fetch — the Leaver's lock point, same as the Mover).

    A user-fetch or assignment-fetch failure returns FAILED with failure_step
    and lock_acquired=False in data — the driver routes these to
    _handle_early_failure. A clean UserNotFoundError is distinguished from a
    general Graph error so the warning can say the identity does not exist
    rather than reporting a lookup error.
    """
    try:
        current_user = graph_client.get_user(payload.upn)
        user_id = current_user["id"]
    except UserNotFoundError:
        return _fetch_failure(
            "UserFetch",
            f"Step 1 (UserFetch): the user was not found in Entra ID — UPN "
            f"'{payload.upn}' does not resolve. Offboarding cannot proceed "
            f"against a non-existent identity.",
        )
    except GraphClientError as e:
        return _fetch_failure(
            "UserFetch",
            f"Step 1 (UserFetch): user lookup failed against Graph — {e}.",
        )

    try:
        current_assignments = graph_client.get_current_access_package_assignments(
            user_id=user_id,
        )
    except GraphClientError as e:
        return _fetch_failure(
            "PackageFetch",
            f"Step 1 (PackageFetch): could not read current access package "
            f"assignments — {e}. Offboarding cannot proceed without the "
            f"current-state baseline.",
        )

    current_packages = [
        a["accessPackage"]["id"]
        for a in current_assignments
        if a.get("accessPackage", {}).get("id")
    ]
    current_policy_map = {
        a["accessPackage"]["id"]: a.get("assignmentPolicy", {}).get("id", "")
        for a in current_assignments
        if a.get("accessPackage", {}).get("id")
    }
    package_labels = {
        a["accessPackage"]["id"]: a["accessPackage"].get("displayName", a["accessPackage"]["id"])
        for a in current_assignments
        if a.get("accessPackage", {}).get("id")
    }

    instance_id = str(_uuid.uuid4())
    acquire_lock(
        table_client = jml_events_client,
        employee_id  = payload.employee_id,
        event_id     = event_id,
        instance_id  = instance_id,
    )

    return StageResult(
        ok=True,
        outcome=StageOutcome.PROCEED,
        data={
            "user_id":            user_id,
            "current_packages":   current_packages,
            "current_policy_map": current_policy_map,
            "package_labels":     package_labels,
            "lock_acquired":      True,
        },
    )


# Offboarding action stages

def stage_disable(
    payload:      IdentityPayload,
    user_id:      str,
    graph_client: JmlGraphClient,
) -> StageResult:
    """
    Step 2 — disable the account (accountEnabled=false, ADR-015). The first
    mutation, deliberately: everything after operates on a locked-out account,
    so a downstream failure still fails safe.

    A failure here is recorded as a warning and a failed action but does not
    stop the pipeline — the pre-refactor behaviour. The driver appends the
    action/warning to audit_record.
    """
    try:
        graph_client.disable_user(user_id)
        return StageResult(
            ok=True,
            outcome=StageOutcome.PROCEED,
            report_actions=[{
                "action": "AccountDisabled", "detail": "accountEnabled=false", "succeeded": True,
            }],
        )
    except GraphClientError as e:
        logger.warning("  ✗ account disable failed: %s", e)
        return StageResult(
            ok=True,
            outcome=StageOutcome.PROCEED,
            report_actions=[{
                "action": "AccountDisabled", "detail": str(e), "succeeded": False,
            }],
            report_warnings=[f"Account disable failed: {e}"],
        )


def stage_revoke(
    payload:      IdentityPayload,
    user_id:      str,
    graph_client: JmlGraphClient,
) -> StageResult:
    """
    Step 3 — revoke all sign-in sessions (ADR-015). Disabling the account does
    not invalidate tokens already issued; this forces re-authentication, which
    the disabled account then rejects.

    A failure is recorded but does not stop the pipeline — pre-refactor
    behaviour.
    """
    try:
        graph_client.revoke_sessions(user_id)
        return StageResult(
            ok=True,
            outcome=StageOutcome.PROCEED,
            report_actions=[{
                "action": "SessionsRevoked", "detail": "revokeSignInSessions", "succeeded": True,
            }],
        )
    except GraphClientError as e:
        logger.warning("  ✗ session revocation failed: %s", e)
        return StageResult(
            ok=True,
            outcome=StageOutcome.PROCEED,
            report_actions=[{
                "action": "SessionsRevoked", "detail": str(e), "succeeded": False,
            }],
            report_warnings=[f"Session revocation failed: {e}"],
        )


def stage_pim_terminate(
    payload:      IdentityPayload,
    user_id:      str,
    graph_client: JmlGraphClient,
) -> StageResult:
    """
    Step 5 — discover and terminate every active PIM group session for this
    user, tenant-wide (ADR-016).

    Discovery is live (get_active_pim_assignments_for_user), not policy-derived
    — the Leaver has no entitlement resolution to draw a candidate group list
    from (ADR-014). A missing P2 licence, or any other failure to even check,
    is recorded as a warning and does not block the rest of offboarding — PIM
    termination is an additional control on top of package removal, not a
    prerequisite for it.

    Returns actions and warnings in the StageResult for the driver to append.
    Always PROCEED — this stage never fails the pipeline.
    """
    try:
        active_sessions = graph_client.get_active_pim_assignments_for_user(user_id)
    except GraphClientError as e:
        logger.warning(
            "  ⚠ PIM active-session check failed — employee=%s, error=%s",
            payload.employee_id, e,
        )
        return StageResult(
            ok=True,
            outcome=StageOutcome.PROCEED,
            report_warnings=[
                f"PIM active-session check failed (P2 may be absent, or a real "
                f"Graph error): {e}. Skipping PIM termination."
            ],
        )

    if not active_sessions:
        logger.info("Step 5 — no active PIM sessions found")
        return StageResult(ok=True, outcome=StageOutcome.PROCEED)

    actions: list[dict] = []
    terminated_count = 0
    failed_count = 0

    for session in active_sessions:
        group_id = session.get("group_id", "")
        if not group_id:
            continue
        try:
            graph_client.cancel_pim_session(
                user_id=user_id,
                group_id=group_id,
                justification=f"Leaver offboarding — employee {payload.employee_id}",
            )
            terminated_count += 1
            actions.append({
                "action": "PIMSessionTerminated", "group_id": group_id,
                "detail": "Active session cancelled", "succeeded": True,
            })
            logger.info("  ✓ PIM session on group %s — terminated", group_id)
        except GraphClientError as e:
            failed_count += 1
            actions.append({
                "action": "PIMSessionTerminated", "group_id": group_id,
                "detail": f"Termination failed: {e}", "succeeded": False,
            })
            logger.warning("  ✗ PIM session on group %s — termination failed: %s", group_id, e)

    logger.info("Step 5 complete — %d terminated, %d failed", terminated_count, failed_count)
    return StageResult(ok=True, outcome=StageOutcome.PROCEED, report_actions=actions)


def stage_soft_delete(
    payload:      IdentityPayload,
    user_id:      str,
    graph_client: JmlGraphClient,
) -> StageResult:
    """
    Step 6 — soft delete, subject to the configurable hold.

    hold == 0 deletes now (Graph DELETE → deleted-users container, recoverable
    30 days). hold > 0 defers and logs — everything before this step has already
    locked the account out and stripped access, so a delayed deletion is safe.

    In the durable runtime the orchestrator arms a timer for the hold and fires
    the deferred_delete activity when it wakes (§3.3.15), so a deferred delete is
    actually completed later rather than only logged. In the synchronous runtime
    there is no such timer — the deferral is logged and a re-run or reconciliation
    completes it. This stage only decides defer-vs-delete-now; it does not own the
    timer either way.

    Emits user_deleted (bool) in data so verification and final status can read it.
    """
    if soft_delete_is_deferred():
        hold_seconds = soft_delete_hold_seconds()
        logger.info("  ⊘ soft delete deferred %d second(s) per policy", hold_seconds)
        return StageResult(
            ok=True,
            outcome=StageOutcome.PROCEED,
            data={"user_deleted": False},
            report_warnings=[
                f"Soft delete deferred {SOFT_DELETE_HOLD_DAYS} day(s) "
                f"({hold_seconds}s) per policy — deferred deletion scheduled."
            ],
        )

    try:
        graph_client.delete_user(user_id)
        logger.info("  ✓ user soft-deleted")
        return StageResult(
            ok=True,
            outcome=StageOutcome.PROCEED,
            data={"user_deleted": True},
            report_actions=[{
                "action": "SoftDelete",
                "detail": "User moved to deleted-users container",
                "succeeded": True,
            }],
        )
    except GraphClientError as e:
        logger.warning("  ✗ soft delete failed: %s", e)
        return StageResult(
            ok=True,
            outcome=StageOutcome.PROCEED,
            data={"user_deleted": False},
            report_actions=[{
                "action": "SoftDelete", "detail": str(e), "succeeded": False,
            }],
            report_warnings=[f"Soft delete failed: {e}"],
        )


def stage_verify(
    payload:                  IdentityPayload,
    user_id:                  str,
    user_deleted:             bool,
    packages_removal_failed:  list,
    graph_client:             JmlGraphClient,
) -> StageResult:
    """
    Step 7 — post-offboarding verification against real tenant state. The driver
    owns any propagation wait (sync: time.sleep; durable: timer), so this stage
    does no sleeping.

    Confirms the account is disabled (or, if soft-deleted, that get_user no
    longer resolves — deleted implies disabled) and reports whether packages
    cleared. Emits the verification dict and a verification_error flag the
    driver uses to pick the terminal status.
    """
    verification_error = False
    account_disabled_confirmed = False
    packages_cleared = not packages_removal_failed
    warnings: list[str] = []

    if user_deleted:
        try:
            graph_client.get_user(payload.upn)
            warnings.append(
                "User still resolvable via get_user() immediately after soft "
                "delete — likely Graph propagation lag, not a failed delete."
            )
        except GraphClientError:
            account_disabled_confirmed = True  # deleted implies disabled
    else:
        try:
            refetched = graph_client.get_user(payload.upn)
            account_disabled_confirmed = refetched.get("account_enabled") is False
            if not account_disabled_confirmed:
                warnings.append(
                    "Post-offboarding check: account does not show as disabled "
                    "on re-fetch."
                )
        except GraphClientError as e:
            verification_error = True
            warnings.append(f"Post-offboarding user re-fetch failed: {e}")

    verification = {
        "account_disabled_confirmed": account_disabled_confirmed,
        "packages_cleared":           packages_cleared,
        "user_deleted":               user_deleted,
        "soft_delete_deferred":       soft_delete_is_deferred(),
    }

    return StageResult(
        ok=True,
        outcome=StageOutcome.PROCEED,
        data={
            "verification_error":          verification_error,
            "account_disabled_confirmed":  account_disabled_confirmed,
            "packages_cleared":            packages_cleared,
            "audit_post_offboard_verification": verification,
        },
        report_warnings=warnings,
    )


# Stage-local helpers

def _fetch_failure(failure_step: str, reason: str) -> StageResult:
    return StageResult(
        ok=False,
        outcome=StageOutcome.FAILED,
        data={"failure_step": failure_step, "lock_acquired": False},
        report_warnings=[reason],
        summary=reason,
    )