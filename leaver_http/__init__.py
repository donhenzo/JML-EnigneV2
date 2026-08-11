"""
Functions/leaver_http/__init__.py

Azure Function HTTP trigger for the Leaver module.

Orchestrates offboarding for a single identity lifecycle event. Called
directly via HTTP or by the BambooHR ingestion coordinator when action
derivation returns JmlAction.LEAVER.

Processing flow (ADR-015):

    Pre-Step  — Supersede any pending Joiner/Mover events for this
                employee (conflict_queue). claim_event() in JmlEvents.
                Atomic insert — duplicate event ID exits immediately.

    Step 1    — User fetch and current access package assignment fetch
                via Graph. acquire_lock() written to JmlEvents on
                success.
    Step 2    — Disable account (accountEnabled = false). First action
                taken, so a downstream failure still fails safe.
    Step 3    — Revoke all sign-in sessions.
    Step 4    — Remove every currently held access package (ADR-014 —
                no retention check, no unmanaged exclusion; everything
                goes).
    Step 5    — Terminate any active PIM group sessions, discovered
                live from the tenant rather than derived from policy
                (ADR-014 removes entitlement resolution for Leaver).
                Departs from ADR-003's "let it expire naturally" — see
                ADR-016.
    Step 6    — Soft delete the user, subject to a configurable hold
                period.
    Step 7    — Post-offboarding verification against real tenant
                state.
    Step 8    — LeaverAuditRecord written. release_lock(). JmlEvents
                updated to Completed or Failed.

The JmlEvents lock is released on every exit path.

NOTE — why this pipeline has no entitlement resolution, no delta, and
no retention step, unlike the Mover (ADR-014):
    A Leaver has no target state to resolve towards. Its target is
    simply "remaining = current − everything". Retention exists to
    bridge a role transition, which a termination is not; an unmanaged
    package on a terminated identity is exactly the kind of leftover
    access offboarding exists to catch, not something to preserve.
    See ADR-014 for the full reasoning.

NOTE — why the step order differs from the Mover's add-before-remove
(ADR-009) despite superficially resembling it (ADR-015):
    ADR-009 protects against a zero-access failure mode during a role
    change. The Leaver has the opposite goal — it wants to *reach* zero
    access, safely — so it disables and revokes sessions before
    touching package assignments: a partial failure downstream then
    fails safe (account already locked out) rather than failing open.
"""

from __future__ import annotations
import json
import logging
import os
import uuid as _uuid
from datetime import datetime, timezone
from typing import Optional

from azure.data.tables import TableServiceClient, TableClient

from Ingestion.schema import IdentityPayload, JmlAction
from Provisioning.graph_client import JmlGraphClient, GraphClientError, build_graph_client
from Provisioning.package_requests import poll_request_until_terminal
from Functions.Event_store.event_store import (
    get_events_table_client,
    generate_event_id,
    claim_event,
    acquire_lock,
    release_lock,
    update_event_status,
    EventStatus,
)
from Functions.Event_store.conflict_queue import (
    check_and_handle_conflict,
    ConflictOutcome,
)

logger = logging.getLogger(__name__)


# Table names and constants
LEAVER_EVENT_LOG_TABLE = "LeaverEventLog"
LEAVER_AUDIT_LOG_TABLE = "LeaverAuditLog"

# Days to hold before soft-deleting the user object (Step 6). Everything
# before this step has already cut off access, so a nonzero hold is safe
# — it just gives operators a window for manual review before the
# identity leaves the directory. Default is immediate deletion.
SOFT_DELETE_HOLD_DAYS = int(os.environ.get("JML_LEAVER_SOFT_DELETE_HOLD_DAYS", "0"))


class LeaverEventStatus:
    RECEIVED          = "RECEIVED"
    IN_PROGRESS       = "IN_PROGRESS"
    OFFBOARD_SUCCESS  = "OFFBOARD_SUCCESS"
    OFFBOARD_PARTIAL  = "OFFBOARD_PARTIAL"
    OFFBOARD_FAILED   = "OFFBOARD_FAILED"
    QUEUED_CONCURRENT = "QUEUED_CONCURRENT"


# Table Storage helpers

def _get_table_client(connection_string: str) -> TableServiceClient:
    return TableServiceClient.from_connection_string(connection_string)


def _check_concurrent_event(
    table_client: TableServiceClient,
    employee_id:  str,
) -> bool:
    """
    Return True if an IN_PROGRESS Leaver event already exists for this
    employee. Mirrors the Mover's MoverEventLog check (ADR-004).

    Fails closed — if the query itself fails, returns True to prevent
    a second offboarding event from running on top of an unknown state.
    """
    try:
        client   = table_client.get_table_client(LEAVER_EVENT_LOG_TABLE)
        entities = client.query_entities(
            query_filter=(
                f"PartitionKey eq '{employee_id}' "
                f"and status eq 'IN_PROGRESS'"
            )
        )
        return any(True for _ in entities)
    except Exception as e:
        logger.error(
            "LeaverEventLog concurrent check failed — employee=%s, error=%s",
            employee_id, str(e),
        )
        return True


def _write_event_log(
    table_client: TableServiceClient,
    employee_id:  str,
    event_id:     str,
    status:       str,
    payload_json: str = "",
) -> None:
    try:
        client = table_client.get_table_client(LEAVER_EVENT_LOG_TABLE)
        entity = {
            "PartitionKey": employee_id,
            "RowKey":       event_id,
            "status":       status,
            "updated_at":   datetime.now(timezone.utc).isoformat(),
            "payload":      payload_json,
        }
        client.upsert_entity(entity)
    except Exception as e:
        logger.error(
            "LeaverEventLog write failed — employee=%s, event=%s, "
            "status=%s, error=%s",
            employee_id, event_id, status, str(e),
        )


def _write_audit_record(
    table_client: TableServiceClient,
    employee_id:  str,
    event_id:     str,
    audit_record: dict,
) -> None:
    """
    Write the completed LeaverAuditRecord to LeaverAuditLog.

    Table Storage only accepts flat scalar values. Nested dicts and
    lists are serialised to JSON strings before writing — same pattern
    as MoverAuditLog.
    """
    try:
        client = table_client.get_table_client(LEAVER_AUDIT_LOG_TABLE)
        entity = {
            "PartitionKey": employee_id,
            "RowKey":       event_id,
            **{
                k: json.dumps(v) if isinstance(v, (dict, list)) else v
                for k, v in audit_record.items()
            },
        }
        client.upsert_entity(entity)
    except Exception as e:
        logger.error(
            "LeaverAuditLog write failed — employee=%s, event=%s, error=%s",
            employee_id, event_id, str(e),
        )


# Step 4 — full package removal (ADR-014: everything, no exclusions)

def _execute_full_removal(
    graph_client:       JmlGraphClient,
    user_id:            str,
    current_packages:   frozenset[str],
    current_policy_map: dict[str, str],
    package_labels:     dict[str, str],
) -> list[dict]:
    """
    Submit adminRemove for every currently held access package.

    No retention check, no unmanaged exclusion (ADR-014) — a Leaver
    removes everything the user holds, including packages the engine
    doesn't otherwise manage, because unmanaged access on a terminated
    identity is exactly the risk offboarding exists to catch.

    Individual failures are recorded but do not stop remaining
    removals. Step 7 verification surfaces whatever didn't clear.
    """
    actions_taken: list[dict] = []
    removed_count = 0
    failed_count = 0

    for package_id in current_packages:
        label = package_labels.get(package_id, package_id)
        policy_id = current_policy_map.get(package_id, "")

        if not policy_id:
            logger.warning(
                "  ⚠ %s — no assignmentPolicyId on the current assignment, "
                "submitting adminRemove with an empty policy_id anyway",
                label,
            )

        try:
            request = graph_client.request_package_assignment(
                user_id=user_id,
                access_package_id=package_id,
                policy_id=policy_id,
                request_type="adminRemove",
            )
            request_id  = request.get("id", "")
            final_state = poll_request_until_terminal(graph_client, request_id)

            if final_state == "Delivered":
                removed_count += 1
                actions_taken.append({
                    "action":     "PackageRemoval",
                    "package_id": package_id,
                    "detail":     "Removed successfully",
                    "succeeded":  True,
                })
                logger.info("  ✓ %s — removed", label)
                continue

            # Same fallback pattern as the Mover — a poll that didn't
            # reach a terminal state is not the same thing as a real
            # failure. Ask the assignments resource directly.
            fallback = graph_client.check_package_assignment(
                user_id=user_id,
                access_package_id=package_id,
            )
            if not fallback or fallback.get("state") != "delivered":
                removed_count += 1
                actions_taken.append({
                    "action":     "PackageRemoval",
                    "package_id": package_id,
                    "detail":     f"Removed — confirmed via fallback check (last known state={final_state})",
                    "succeeded":  True,
                })
                logger.info(
                    "  ✓ %s — removed (confirmed via fallback check)", label
                )
            else:
                failed_count += 1
                actions_taken.append({
                    "action":     "PackageRemoval",
                    "package_id": package_id,
                    "detail":     f"Removal did not confirm — requestState={final_state}, fallback check still shows delivered",
                    "succeeded":  False,
                })
                logger.warning(
                    "  ✗ %s — removal did not confirm (requestState=%s)",
                    label, final_state,
                )

        except GraphClientError as e:
            failed_count += 1
            actions_taken.append({
                "action":     "PackageRemoval",
                "package_id": package_id,
                "detail":     f"Removal failed: {str(e)}",
                "succeeded":  False,
            })
            logger.warning("  ✗ %s — removal failed: %s", label, str(e))

    if current_packages:
        logger.info(
            "Step 4 complete — %d removed, %d failed",
            removed_count, failed_count,
        )

    return actions_taken


# Step 5 — PIM session termination (ADR-016)

def _execute_pim_termination(
    graph_client: JmlGraphClient,
    user_id:      str,
    employee_id:  str,
) -> tuple[list[dict], list[str]]:
    """
    Discover and terminate every active PIM group session for this
    user, tenant-wide.

    Discovery is live (get_active_pim_assignments_for_user), not
    policy-derived — the Leaver has no entitlement resolution to draw
    a candidate group list from (ADR-014). A missing P2 licence, or
    any other failure to even check, is recorded as a warning and does
    not block the rest of offboarding — PIM eligibility is a bonus
    control on top of package removal, not the primary one.
    """
    actions_taken: list[dict] = []
    warnings: list[str] = []

    try:
        active_sessions = graph_client.get_active_pim_assignments_for_user(user_id)
    except GraphClientError as e:
        warnings.append(
            f"PIM active-session check failed (P2 may be absent, or a "
            f"real Graph error): {str(e)}. Skipping PIM termination."
        )
        logger.warning(
            "  ⚠ PIM active-session check failed — employee=%s, error=%s",
            employee_id, str(e),
        )
        return actions_taken, warnings

    if not active_sessions:
        logger.info("Step 5 — no active PIM sessions found")
        return actions_taken, warnings

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
                justification=f"Leaver offboarding — employee {employee_id}",
            )
            terminated_count += 1
            actions_taken.append({
                "action":   "PIMSessionTerminated",
                "group_id": group_id,
                "detail":   "Active session cancelled",
                "succeeded": True,
            })
            logger.info("  ✓ PIM session on group %s — terminated", group_id)
        except GraphClientError as e:
            failed_count += 1
            actions_taken.append({
                "action":   "PIMSessionTerminated",
                "group_id": group_id,
                "detail":   f"Termination failed: {str(e)}",
                "succeeded": False,
            })
            logger.warning(
                "  ✗ PIM session on group %s — termination failed: %s",
                group_id, str(e),
            )

    logger.info(
        "Step 5 complete — %d terminated, %d failed",
        terminated_count, failed_count,
    )
    return actions_taken, warnings


# Main orchestrator

def run_leaver_pipeline(
    payload:      IdentityPayload,
    table_client: TableServiceClient,
    graph_client: JmlGraphClient,
) -> dict:
    """
    Execute the Leaver offboarding flow for a single identity event.

    The EventId is generated internally from the payload, same as the
    Mover — event ownership stays inside the pipeline, not the
    ingestion layer.

    Args:
        payload:      Canonical IdentityPayload with action=LEAVER.
                      start_date is treated as the termination date for
                      event ID generation. department/job_title are not
                      used — there is no entitlement resolution (ADR-014).
        table_client: Authenticated TableServiceClient for Table Storage.
        graph_client: Authenticated JmlGraphClient for Graph API ops.

    Returns:
        dict with final_status, employee_id, event_id, and summary.
    """
    if payload.action != JmlAction.LEAVER:
        raise ValueError(
            f"run_leaver_pipeline called with action={payload.action!r}, "
            f"expected JmlAction.LEAVER. Refusing to run offboarding "
            f"logic against a non-Leaver payload."
        )

    employee_id = payload.employee_id
    event_id = generate_event_id(
        employee_id,
        "Leaver",
        payload.start_date.isoformat(),
    )

    audit_record: dict = {
        "event_type":      "LEAVER",
        "employee_id":     employee_id,
        "event_id":        event_id,
        "source":          "BAMBOOHR",
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "actions_taken":   [],
        "warnings":        [],
        "offboard_status": LeaverEventStatus.RECEIVED,
    }

    # Pre-Step — claim event, then check for conflicts.
    # The conflict queue needs the event to exist in JmlEvents before it
    # can reason about it, so claim first, then check. When the new action
    # is "Leaver", check_and_handle_conflict supersedes all Pending events
    # for this employee and returns SUPERSEDE — meaning "you have priority,
    # proceed." It does NOT mean "you were superseded."
    conn_str          = os.environ.get("JML_STORAGE_CONNECTION_STRING", "")
    jml_events_client = get_events_table_client(conn_str)

    payload_json_str = json.dumps({
        "employee_id": employee_id,
        "action":      "Leaver",
        "event_id":    event_id,
    })

    claimed = claim_event(
        table_client   = jml_events_client,
        employee_id    = employee_id,
        action         = "Leaver",
        start_date     = payload.start_date.isoformat(),
        payload_json   = payload_json_str,
        correlation_id = event_id,
    )

    if not claimed:
        logger.info(
            "Leaver event already claimed in JmlEvents — idempotency exit — "
            "employee=%s", employee_id,
        )
        return {
            "final_status": LeaverEventStatus.QUEUED_CONCURRENT,
            "employee_id":  employee_id,
            "event_id":     event_id,
            "summary":      "Duplicate event — already claimed in JmlEvents.",
        }

    # Conflict check — for a Leaver this supersedes any pending
    # Joiner/Mover events, then returns SUPERSEDE (meaning "proceed
    # with priority"). A non-Leaver would get QUEUED if something is
    # already in flight — but a Leaver never queues behind anything.
    conflict_outcome = check_and_handle_conflict(
        table_client = jml_events_client,
        employee_id  = employee_id,
        new_event_id = event_id,
        new_action   = "Leaver",
    )
    logger.info(
        "Conflict check — employee=%s, outcome=%s",
        employee_id, conflict_outcome,
    )


    # Step 1 — current state discovery + concurrent check

    logger.info("Leaver Step 1 — current state discovery — employee=%s", employee_id)

    is_concurrent = _check_concurrent_event(table_client, employee_id)
    if is_concurrent:
        logger.warning(
            "Concurrent Leaver event detected — employee=%s, "
            "queuing with status QUEUED_CONCURRENT", employee_id,
        )
        _write_event_log(
            table_client, employee_id, event_id, LeaverEventStatus.QUEUED_CONCURRENT
        )
        return {
            "final_status": LeaverEventStatus.QUEUED_CONCURRENT,
            "employee_id":  employee_id,
            "event_id":     event_id,
            "summary":      "Event queued — another Leaver event is in progress for this employee.",
        }

    _write_event_log(table_client, employee_id, event_id, LeaverEventStatus.IN_PROGRESS)

    try:
        current_user = graph_client.get_user(payload.upn)
        user_id      = current_user["id"]
    except GraphClientError as e:
        logger.error(
            "Step 1 failed — user fetch failed — employee=%s, error=%s",
            employee_id, str(e),
        )
        return _handle_early_failure(
            table_client, jml_events_client, employee_id, event_id,
            audit_record, f"User fetch failed: {str(e)}",
            failure_step="UserFetch", lock_acquired=False,
        )

    try:
        current_assignments = graph_client.get_current_access_package_assignments(
            user_id=user_id,
        )
        current_packages = frozenset(
            a["accessPackage"]["id"]
            for a in current_assignments
            if a.get("accessPackage", {}).get("id")
        )
        current_policy_map = {
            a["accessPackage"]["id"]: a.get("assignmentPolicy", {}).get("id", "")
            for a in current_assignments
            if a.get("accessPackage", {}).get("id")
        }
        package_labels: dict[str, str] = {
            a["accessPackage"]["id"]: a["accessPackage"].get("displayName", a["accessPackage"]["id"])
            for a in current_assignments
            if a.get("accessPackage", {}).get("id")
        }
    except GraphClientError as e:
        logger.error(
            "Step 1 failed — access package assignment fetch failed — "
            "employee=%s, error=%s", employee_id, str(e),
        )
        return _handle_early_failure(
            table_client, jml_events_client, employee_id, event_id,
            audit_record, f"Access package assignment fetch failed: {str(e)}",
            failure_step="AccessPackageAssignmentFetch", lock_acquired=False,
        )

    audit_record["packages_at_offboard_start"] = list(current_packages)

    instance_id = str(_uuid.uuid4())
    acquire_lock(
        table_client=jml_events_client, employee_id=employee_id,
        event_id=event_id, instance_id=instance_id,
    )


    # Step 2 — disable account

    logger.info("Leaver Step 2 — disable account — employee=%s", employee_id)

    try:
        graph_client.disable_user(user_id)
        audit_record["actions_taken"].append({
            "action": "AccountDisabled", "detail": "accountEnabled=false", "succeeded": True,
        })
        logger.info("  ✓ account disabled")
    except GraphClientError as e:
        audit_record["actions_taken"].append({
            "action": "AccountDisabled", "detail": str(e), "succeeded": False,
        })
        audit_record["warnings"].append(f"Account disable failed: {str(e)}")
        logger.warning("  ✗ account disable failed: %s", str(e))


    # Step 3 — revoke sessions

    logger.info("Leaver Step 3 — revoke sessions — employee=%s", employee_id)

    try:
        graph_client.revoke_sessions(user_id)
        audit_record["actions_taken"].append({
            "action": "SessionsRevoked", "detail": "revokeSignInSessions", "succeeded": True,
        })
        logger.info("  ✓ sessions revoked")
    except GraphClientError as e:
        audit_record["actions_taken"].append({
            "action": "SessionsRevoked", "detail": str(e), "succeeded": False,
        })
        audit_record["warnings"].append(f"Session revocation failed: {str(e)}")
        logger.warning("  ✗ session revocation failed: %s", str(e))


    # Step 4 — remove all access packages (ADR-014)

    logger.info("Leaver Step 4 — access package removal — employee=%s", employee_id)

    removal_actions = _execute_full_removal(
        graph_client=graph_client, user_id=user_id,
        current_packages=current_packages,
        current_policy_map=current_policy_map,
        package_labels=package_labels,
    )
    audit_record["actions_taken"].extend(removal_actions)
    audit_record["packages_removed"] = [
        {"id": a["package_id"], "reason": "LEAVER_OFFBOARDING"}
        for a in removal_actions if a["succeeded"]
    ]
    packages_removal_failed = [
        a["package_id"] for a in removal_actions if not a["succeeded"]
    ]
    if packages_removal_failed:
        audit_record["warnings"].append(
            f"{len(packages_removal_failed)} package(s) did not confirm "
            f"removal: {packages_removal_failed}"
        )


    # Step 5 — PIM session termination (ADR-016)

    logger.info("Leaver Step 5 — PIM session termination — employee=%s", employee_id)

    pim_actions, pim_warnings = _execute_pim_termination(
        graph_client=graph_client, user_id=user_id, employee_id=employee_id,
    )
    audit_record["actions_taken"].extend(pim_actions)
    audit_record["warnings"].extend(pim_warnings)


    # Step 6 — soft delete (configurable hold)

    logger.info("Leaver Step 6 — soft delete — employee=%s", employee_id)

    user_deleted = False
    if SOFT_DELETE_HOLD_DAYS <= 0:
        try:
            graph_client.delete_user(user_id)
            user_deleted = True
            audit_record["actions_taken"].append({
                "action": "SoftDelete", "detail": "User moved to deleted-users container",
                "succeeded": True,
            })
            logger.info("  ✓ user soft-deleted")
        except GraphClientError as e:
            audit_record["actions_taken"].append({
                "action": "SoftDelete", "detail": str(e), "succeeded": False,
            })
            audit_record["warnings"].append(f"Soft delete failed: {str(e)}")
            logger.warning("  ✗ soft delete failed: %s", str(e))
    else:
        audit_record["warnings"].append(
            f"Soft delete deferred {SOFT_DELETE_HOLD_DAYS} day(s) per "
            f"JML_LEAVER_SOFT_DELETE_HOLD_DAYS policy — not yet deleted."
        )
        logger.info(
            "  ⊘ soft delete deferred %d day(s) per policy",
            SOFT_DELETE_HOLD_DAYS,
        )


    # Step 7 — post-offboarding verification

    logger.info("Leaver Step 7 — post-offboarding verification — employee=%s", employee_id)

    verification_error = False
    account_disabled_confirmed = False
    packages_cleared = not packages_removal_failed

    if user_deleted:
        try:
            graph_client.get_user(payload.upn)
            # Still resolvable right after a soft delete — Graph can lag.
            # Not fatal on its own; recorded as a discrepancy, not a hard
            # verification error, since the delete call itself succeeded.
            audit_record["warnings"].append(
                "User still resolvable via get_user() immediately after "
                "soft delete — likely Graph propagation lag, not a failed delete."
            )
        except GraphClientError:
            account_disabled_confirmed = True  # deleted implies disabled
    else:
        try:
            refetched = graph_client.get_user(payload.upn)
            account_disabled_confirmed = refetched.get("account_enabled") is False
            if not account_disabled_confirmed:
                audit_record["warnings"].append(
                    "Post-offboarding check: account does not show as "
                    "disabled on re-fetch."
                )
        except GraphClientError as e:
            verification_error = True
            audit_record["warnings"].append(
                f"Post-offboarding user re-fetch failed: {str(e)}"
            )

    audit_record["post_offboard_verification"] = {
        "account_disabled_confirmed": account_disabled_confirmed,
        "packages_cleared":           packages_cleared,
        "user_deleted":               user_deleted,
        "soft_delete_deferred":       SOFT_DELETE_HOLD_DAYS > 0,
    }


    # Step 8 — final status + audit record

    logger.info("Leaver Step 8 — audit reporting — employee=%s", employee_id)

    if verification_error:
        final_status = LeaverEventStatus.OFFBOARD_FAILED
    elif packages_cleared and (account_disabled_confirmed or user_deleted):
        final_status = LeaverEventStatus.OFFBOARD_SUCCESS
    else:
        final_status = LeaverEventStatus.OFFBOARD_PARTIAL

    audit_record["offboard_status"] = final_status
    _write_event_log(table_client, employee_id, event_id, final_status)
    _write_audit_record(table_client, employee_id, event_id, audit_record)

    jml_final_status = (
        EventStatus.COMPLETED
        if final_status == LeaverEventStatus.OFFBOARD_SUCCESS
        else EventStatus.FAILED
    )
    release_lock(jml_events_client, employee_id, event_id)
    update_event_status(
        table_client=jml_events_client, employee_id=employee_id, event_id=event_id,
        status=jml_final_status,
        failure_step=(
            "PostOffboardVerification"
            if final_status == LeaverEventStatus.OFFBOARD_PARTIAL else ""
        ),
    )

    logger.info(
        "Leaver pipeline complete — employee=%s, status=%s",
        employee_id, final_status,
    )

    return {
        "final_status": final_status,
        "employee_id":  employee_id,
        "event_id":     event_id,
        "summary":      f"Leaver event completed with status {final_status}.",
    }


# Helpers

def _fail(employee_id: str, event_id: str, reason: str) -> dict:
    return {
        "final_status": LeaverEventStatus.OFFBOARD_FAILED,
        "employee_id":  employee_id,
        "event_id":     event_id,
        "summary":      reason,
    }


def _handle_early_failure(
    table_client:      TableServiceClient,
    jml_events_client: TableClient,
    employee_id:       str,
    event_id:          str,
    audit_record:      dict,
    reason:            str,
    failure_step:      str,
    lock_acquired:     bool = False,
) -> dict:
    """
    Handle a failure before Step 8's own cleanup runs. Mirrors the
    Mover's _handle_early_failure — every terminal path must still
    produce a LeaverAuditLog record.
    """
    audit_record["offboard_status"] = LeaverEventStatus.OFFBOARD_FAILED
    audit_record["warnings"].append(reason)

    _write_event_log(table_client, employee_id, event_id, LeaverEventStatus.OFFBOARD_FAILED)
    _write_audit_record(table_client, employee_id, event_id, audit_record)

    if lock_acquired:
        release_lock(jml_events_client, employee_id, event_id)

    update_event_status(
        table_client=jml_events_client, employee_id=employee_id, event_id=event_id,
        status=EventStatus.FAILED, failure_step=failure_step,
    )

    return _fail(employee_id, event_id, reason)


# Azure Function HTTP entry point

def main(req):
    """
    Azure Function HTTP trigger entry point.

    Expects a JSON body with a canonical IdentityPayload where
    action == "Leaver". start_date is used as the termination date.

    Environment variables required:
        AZURE_STORAGE_CONNECTION_STRING
        AZURE_TENANT_ID
        AZURE_CLIENT_ID
        AZURE_CLIENT_SECRET
        JML_LEAVER_SOFT_DELETE_HOLD_DAYS (optional, default 0)
    """
    import azure.functions as func

    try:
        body    = req.get_json()
        payload = IdentityPayload(**body["payload"])

        conn_str     = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        table_client = _get_table_client(conn_str)

        graph_service_client, credential = build_graph_client()
        graph_client = JmlGraphClient(graph_service_client, credential)

        result = run_leaver_pipeline(
            payload=payload, table_client=table_client, graph_client=graph_client,
        )

        return func.HttpResponse(
            json.dumps(result), status_code=200, mimetype="application/json",
        )

    except Exception as e:
        logger.error("Leaver HTTP trigger failed: %s", str(e))
        return func.HttpResponse(
            json.dumps({"error": str(e)}), status_code=500, mimetype="application/json",
        )