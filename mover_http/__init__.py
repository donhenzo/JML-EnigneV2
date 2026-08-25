"""
Functions/mover_http/__init__.py

Azure Function HTTP trigger for the Mover module.

Orchestrates the Mover processing flow for a single identity lifecycle
transition event. Called directly via HTTP or by the BambooHR ingestion
coordinator when action derivation returns JmlAction.MOVER.

Processing flow:

    Pre-Step  — claim_event() in JmlEvents. Atomic insert — duplicate
                event ID exits immediately with no side effects.

    Step 1    — Concurrent event check via MoverEventLog. User fetch
                and access package assignment fetch via Graph.
                acquire_lock() written to JmlEvents on success.

    Step 2    — Entitlement resolution for new and old roles, against
                access_packages (ADR-007), not the legacy groups field.
    Step 3    — Package delta (four sets) and attribute delta.
    Step 4    — Retention evaluation against RetentionRegistry.
    Step 5    — SoD evaluation skipped (ADR-008, see NOTE below).
    Step 6    — Access package additions (ADR-009 Strategy A: add first).
                Each submitted request is polled to a terminal
                requestState before Step 7 runs.
    Step 7    — Access package removals — gated on Step 6 succeeding for
                every package. Attribute patch also happens here.
    Step 8    — Post-move assignment verification and governance check.
    Step 9    — MoverAuditRecord written. release_lock(). JmlEvents
                updated to Completed or Failed.

The JmlEvents lock is released on every exit path.

NOTE — Step 1 migration (access package assignments):
    current_packages is sourced from get_current_access_package_assignments(),
    not memberOf. Named current_packages / target_packages throughout to
    reflect that these hold access package IDs, not raw group object IDs.
    calculate_delta() itself is still generic over "groups" — its parameter
    names are unchanged, only the values passed in have changed meaning.

NOTE — Step 2/3 migration (access_packages, not legacy groups):
    EntitlementResult.groups is documented in mapping_resolver.py as legacy
    — pre-ADR-007 departments not yet migrated. Every rule in the current
    role_mapping_rules.json resolves via accessPackageId/policyId, so
    new_resolved.groups and old_resolved.groups are empty for all of them.
    Target and managed-catalogue calculation must read
    EntitlementResult.access_packages instead, or delta computation
    silently produces "add nothing, remove nothing" against a non-empty
    current_packages set.

NOTE — Step 6/7 reordering (ADR-009):
    The legacy group-based Mover removed before adding, because order
    never mattered for direct group membership. Access packages are
    different: ADR-009 requires adminAdd-confirm-delivered BEFORE
    adminRemove, so a failed addition never leaves the user with less
    access than before the move. There is no guaranteed 1:1 pairing
    between an added and a removed package in a given delta, so the gate
    is all-or-nothing: removals only proceed if every package in
    packages_to_add reached a Delivered state. A failed addition is not
    a separate early-exit path — it simply shows up as MISSING in Step 8's
    verification, which already produces MOVE_PARTIAL.

NOTE — SoD enforcement (ADR-008):
    The Python preventive SoD check (previously Step 5, evaluate_mover_sod())
    has been removed from this pipeline. Separation of Duties is enforced
    at the platform level via Entra ID Entitlement Management access package
    incompatibility policies — Microsoft blocks an adminAdd that would create
    a conflicting assignment before it ever reaches this engine.
    Mover.sod_reevaluator.py is not deleted. It is repurposed as a detective,
    backup layer intended to run separately, not as a preventive gate here.
    A platform rejection surfaces through Step 6's polling as a Denied or
    Failed requestState — see ADR-011 (deferred) for the pre-flight
    incompatibility check that will make this the rare case rather than
    the primary handling path.
"""

from __future__ import annotations
import json
import logging
import os
import time
import uuid as _uuid
from datetime import datetime, timezone
from typing import Optional

from azure.data.tables import TableServiceClient

from Ingestion.schema import IdentityPayload, JmlAction, EmploymentType
from Mapping.mapping_loader import load_mapping_rules
from Mapping.mapping_resolver import resolve_entitlements
from Provisioning.graph_client import (
    JmlGraphClient,
    GraphClientError,
    build_graph_client,
)
from Mover.delta_engine import calculate_delta
from Mover.attribute_delta import calculate_attribute_delta
from Mover.retention_evaluator import evaluate_all_retentions
from Mover.post_move_verifier import (
    verify_post_move_state,
    PostMoveStatus,
)
from Functions.Event_store.event_store import (
    get_events_table_client,
    generate_event_id,
    claim_event,
    acquire_lock,
    release_lock,
    update_event_status,
    EventStatus,
)

logger = logging.getLogger(__name__)


# Table names and constants
MOVER_EVENT_LOG_TABLE = "MoverEventLog"
MOVER_AUDIT_LOG_TABLE = "MoverAuditLog"
RETENTION_TABLE        = "RetentionRegistry"
STALE_LOCK_MINUTES     = 10

# Polling for access package assignmentRequest delivery (ADR-009).
# Mirrors the Joiner's polling pattern — typically delivers in 1-2
# minutes, occasionally longer. Production should move to async handoff
# rather than synchronous waiting; tracked as known debt from the Joiner
# build, inherited here.
PACKAGE_POLL_MAX_ATTEMPTS     = int(os.environ.get("JML_PACKAGE_POLL_MAX_ATTEMPTS", "60"))
PACKAGE_POLL_INTERVAL_SECONDS = int(os.environ.get("JML_PACKAGE_POLL_INTERVAL_SECONDS", "5"))
TERMINAL_REQUEST_STATES = frozenset({"Delivered", "Denied", "Failed", "Canceled"})


# MoverEvent status values
class MoverEventStatus:
    RECEIVED          = "RECEIVED"
    IN_PROGRESS       = "IN_PROGRESS"
    MOVE_SUCCESS      = "MOVE_SUCCESS"
    MOVE_PARTIAL      = "MOVE_PARTIAL"
    MOVE_FAILED       = "MOVE_FAILED"
    QUEUED_CONCURRENT = "QUEUED_CONCURRENT"


# Table Storage helpers

def _get_table_client(connection_string: str) -> TableServiceClient:
    return TableServiceClient.from_connection_string(connection_string)


from datetime import datetime, timezone, timedelta

def _is_stale_in_progress(row: dict) -> bool:
    """
    True if an IN_PROGRESS event log row is older than STALE_LOCK_MINUTES.

    A 504-killed run leaves its EventLog row IN_PROGRESS with no terminal
    write. Without this, the reclaimed retry is blocked as QUEUED_CONCURRENT
    even though the JmlEvents lock has already been reclaimed. Same staleness
    window as the event store lock, so both agree on when a run is dead.
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


def _check_concurrent_event(
    table_client: TableServiceClient,
    employee_id:  str,
) -> bool:
    """..."""
    try:
        client   = table_client.get_table_client(MOVER_EVENT_LOG_TABLE)
        entities = client.query_entities(
            query_filter=(
                f"PartitionKey eq '{employee_id}' "
                f"and status eq 'IN_PROGRESS'"
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
            "MoverEventLog concurrent check failed — employee=%s, error=%s",
            employee_id, str(e),
        )
        return True


def _write_event_log(
    table_client:       TableServiceClient,
    employee_id:        str,
    event_id:           str,
    status:             str,
    payload_json:       str = "",
    retention_applied:   Optional[bool] = None,
) -> None:
    """
    Write or update a MoverEventLog entry.

    retention_applied is written as an explicit tag on the row itself
    when known (True/False), not just buried in the MoverAuditLog JSON
    blob — so an operator or a future dashboard can query MoverEventLog
    directly for "which events involved a retention decision" without
    parsing every audit record. None (the default) means retention
    hadn't been evaluated yet at the point this write happened — e.g.
    the early Step 1/2 failure paths, which never reach Step 4 — and
    the field is omitted from the entity rather than written as False,
    so "not yet known" stays distinguishable from "evaluated, none
    applied."
    """
    try:
        client = table_client.get_table_client(MOVER_EVENT_LOG_TABLE)
        entity = {
            "PartitionKey": employee_id,
            "RowKey":       event_id,
            "status":       status,
            "updated_at":   datetime.now(timezone.utc).isoformat(),
            "payload":      payload_json,
        }
        if retention_applied is not None:
            entity["retention_applied"] = retention_applied
        client.upsert_entity(entity)
    except Exception as e:
        logger.error(
            "MoverEventLog write failed — employee=%s, event=%s, "
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
    Write the completed MoverAuditRecord to MoverAuditLog.

    Table Storage only accepts flat scalar values. Nested dicts and
    lists are serialised to JSON strings before writing.
    """
    try:
        client = table_client.get_table_client(MOVER_AUDIT_LOG_TABLE)
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
            "MoverAuditLog write failed — employee=%s, event=%s, error=%s",
            employee_id, event_id, str(e),
        )


# Manager resolution

def _resolve_manager_id(
    graph_client: JmlGraphClient,
    manager_id:   Optional[str],
) -> Optional[str]:
    """
    Resolve a manager employee number to an Entra object ID.

    Returns the Entra object ID if found, None if not provided or not
    found. A missing manager is not fatal for a Mover event — it is
    recorded in the audit trail as a warning.
    """
    if not manager_id:
        return None
    try:
        user = graph_client.get_user(manager_id)
        return user["id"]
    except Exception as e:
        logger.warning(
            "Manager resolution failed — manager_id=%s, error=%s. "
            "Proceeding without manager update.",
            manager_id, str(e),
        )
        return None


# Access package assignment request polling

def _poll_request_until_terminal(
    graph_client:     JmlGraphClient,
    request_id:       str,
    max_attempts:     int = PACKAGE_POLL_MAX_ATTEMPTS,
    interval_seconds: int = PACKAGE_POLL_INTERVAL_SECONDS,
) -> str:
    """
    Poll an assignmentRequest until it reaches a terminal requestState.

    Returns the terminal requestState string (Delivered, Denied, Failed,
    Canceled) or "TimedOut" if the poll window is exhausted first.
    """
    for _ in range(max_attempts):
        status = graph_client.get_assignment_request_status(request_id)
        state  = status.get("requestState", "")
        if state in TERMINAL_REQUEST_STATES:
            return state
        time.sleep(interval_seconds)

    return "TimedOut"


# Access package addition/removal execution

def _execute_package_additions(
    graph_client:    JmlGraphClient,
    user_id:         str,
    packages_to_add: frozenset[str],
    policy_map:      dict[str, str],
    package_labels:  dict[str, str],
) -> tuple[list[dict], frozenset[str], bool]:
    """
    Submit adminAdd requests for new access packages (ADR-009 Strategy A).

    Idempotent via check_package_assignment(). Each submitted request is
    polled to a terminal requestState before this function returns, since
    Step 7's removal gate needs to know the final outcome of every
    addition, not just that a request was accepted.

    Args:
        policy_map:     access_package_id → policy_id, built from
                       new_resolved.access_packages. A package with no
                       entry here cannot be submitted — recorded as a
                       failure.
        package_labels: access_package_id → human-readable label, for
                       log lines only. Never used for logic.

    Returns:
        actions_taken:    audit-record entries for every package.
        delivered:        package IDs that reached Delivered.
        all_succeeded:    True only if every package in packages_to_add
                          reached Delivered (or the set was empty).
                          Gates Step 7 removals per ADR-009.
    """
    actions_taken: list[dict] = []
    delivered: set[str] = set()
    all_succeeded = True
    delivered_count = 0
    failed_count = 0

    for package_id in packages_to_add:
        label = package_labels.get(package_id, package_id)
        policy_id = policy_map.get(package_id)

        if not policy_id:
            all_succeeded = False
            failed_count += 1
            actions_taken.append({
                "action":     "PackageAddition",
                "package_id": package_id,
                "detail":     "No policy_id resolved for this package — cannot submit request",
                "succeeded":  False,
            })
            logger.warning("  ✗ %s — no policy_id resolved, cannot submit", label)
            continue

        try:
            existing = graph_client.check_package_assignment(
                user_id=user_id,
                access_package_id=package_id,
            )
            if existing and existing.get("state") == "delivered":
                delivered.add(package_id)
                delivered_count += 1
                actions_taken.append({
                    "action":     "PackageAddition",
                    "package_id": package_id,
                    "detail":     "Already delivered — skipped (idempotent)",
                    "succeeded":  True,
                })
                logger.info("  ✓ %s — already delivered, skipped", label)
                continue

            request = graph_client.request_package_assignment(
                user_id=user_id,
                access_package_id=package_id,
                policy_id=policy_id,
                request_type="adminAdd",
            )
            request_id  = request.get("id", "")
            final_state = _poll_request_until_terminal(graph_client, request_id)

            if final_state == "Delivered":
                delivered.add(package_id)
                delivered_count += 1
                actions_taken.append({
                    "action":     "PackageAddition",
                    "package_id": package_id,
                    "detail":     "Delivered",
                    "succeeded":  True,
                })
                logger.info("  ✓ %s — added", label)
            elif final_state in ("Denied", "Failed", "Canceled"):
                all_succeeded = False
                failed_count += 1
                actions_taken.append({
                    "action":     "PackageAddition",
                    "package_id": package_id,
                    "detail":     f"AdditionDeniedByPlatform — requestState={final_state}",
                    "succeeded":  False,
                })
                logger.warning(
                    "  ✗ %s — rejected by platform (requestState=%s)",
                    label, final_state,
                )
            else:
                # Polling didn't reach a terminal requestState (usually a
                # transient network read-timeout, not a real platform
                # failure — seen repeatedly in testing where the package
                # actually delivered and we simply lost track of it).
                # Before declaring failure, ask the assignments resource
                # directly — a different Graph endpoint from the one that
                # just timed out, so it's a genuine second opinion, not
                # just a retry of the same flaky call.
                fallback = graph_client.check_package_assignment(
                    user_id=user_id,
                    access_package_id=package_id,
                )
                if fallback and fallback.get("state") == "delivered":
                    delivered.add(package_id)
                    delivered_count += 1
                    actions_taken.append({
                        "action":     "PackageAddition",
                        "package_id": package_id,
                        "detail":     f"Delivered — confirmed via fallback check after poll did not reach a terminal state (last known state={final_state})",
                        "succeeded":  True,
                    })
                    logger.info(
                        "  ✓ %s — added (confirmed via fallback check; "
                        "poll itself did not reach a terminal state)",
                        label,
                    )
                else:
                    all_succeeded = False
                    failed_count += 1
                    actions_taken.append({
                        "action":     "PackageAddition",
                        "package_id": package_id,
                        "detail":     f"Did not reach a terminal state within the poll window, and fallback check found no delivered assignment — last known state={final_state}",
                        "succeeded":  False,
                    })
                    logger.warning(
                        "  ✗ %s — no confirmation within poll window and "
                        "fallback check found nothing delivered (last "
                        "known state=%s)",
                        label, final_state,
                    )

        except GraphClientError as e:
            all_succeeded = False
            failed_count += 1
            actions_taken.append({
                "action":     "PackageAddition",
                "package_id": package_id,
                "detail":     f"Addition failed: {str(e)}",
                "succeeded":  False,
            })
            logger.warning("  ✗ %s — addition failed: %s", label, str(e))

    if packages_to_add:
        logger.info(
            "Step 6 complete — %d added, %d failed",
            delivered_count, failed_count,
        )

    return actions_taken, frozenset(delivered), all_succeeded


def _execute_package_removals(
    graph_client:       JmlGraphClient,
    user_id:            str,
    remove_confirmed:   frozenset[str],
    current_policy_map: dict[str, str],
    package_labels:     dict[str, str],
) -> list[dict]:
    """
    Submit adminRemove requests for old access packages (ADR-009 —
    only called once Step 6 additions have all delivered).

    policy_id for removal comes from the user's actual current
    assignments (current_policy_map, built at Step 1) rather than
    re-deriving from old_resolved — the real assignment on the tenant
    is the authoritative source, and covers packages held outside of
    what the current rules.json would resolve.

    Individual failures are recorded but do not stop remaining
    removals — a partial removal set still leaves the user with less
    stale access than none at all, and Step 8 verification will
    surface exactly what didn't clear.

    package_labels is for log lines only, never for logic.
    """
    actions_taken: list[dict] = []
    removed_count = 0
    failed_count = 0

    for package_id in remove_confirmed:
        label = package_labels.get(package_id, package_id)
        policy_id = current_policy_map.get(package_id, "")

        if not policy_id:
            logger.warning(
                "  ⚠ %s — no assignmentPolicyId found on the current "
                "assignment, submitting adminRemove with an empty "
                "policy_id anyway",
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
            final_state = _poll_request_until_terminal(graph_client, request_id)

            if final_state == "Delivered":
                removed_count += 1
                actions_taken.append({
                    "action":     "PackageRemoval",
                    "package_id": package_id,
                    "detail":     "Removed successfully",
                    "succeeded":  True,
                })
                logger.info("  ✓ %s — removed", label)
            else:
                # Same fallback pattern as additions — a poll that didn't
                # reach a terminal state is not the same thing as a real
                # failure. Ask the assignments resource directly: if
                # there's no longer a delivered assignment for this
                # package, the removal succeeded regardless of what the
                # request-status poll reported.
                fallback = graph_client.check_package_assignment(
                    user_id=user_id,
                    access_package_id=package_id,
                )
                if not fallback or fallback.get("state") != "delivered":
                    removed_count += 1
                    actions_taken.append({
                        "action":     "PackageRemoval",
                        "package_id": package_id,
                        "detail":     f"Removed — confirmed via fallback check after poll did not reach a terminal state (last known state={final_state})",
                        "succeeded":  True,
                    })
                    logger.info(
                        "  ✓ %s — removed (confirmed via fallback check; "
                        "poll itself did not reach a terminal state)",
                        label,
                    )
                else:
                    failed_count += 1
                    actions_taken.append({
                        "action":     "PackageRemoval",
                        "package_id": package_id,
                        "detail":     f"Removal did not confirm — requestState={final_state}, and fallback check still shows a delivered assignment",
                        "succeeded":  False,
                    })
                    logger.warning(
                        "  ✗ %s — removal did not confirm (requestState=%s, "
                        "fallback check still shows delivered)",
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

    if remove_confirmed:
        logger.info(
            "Step 7 complete — %d removed, %d failed",
            removed_count, failed_count,
        )

    return actions_taken


# Attribute update
def _execute_attribute_update(
    graph_client: JmlGraphClient,
    user_id:      str,
    patch_dict:   dict,
) -> tuple[bool, str]:
    """
    PATCH the user's changed Entra ID attributes.

    manager and usageLocation are excluded from the PATCH body. manager
    requires a separate Graph endpoint; usageLocation requires an ISO
    3166-1 alpha-2 country code, and the source carries city names —
    excluded until a location-to-country mapping is added to
    canonical_lookup.json.

    Returns (succeeded, error_message).
    """
    body = {
        field: value
        for field, value in patch_dict.items()
        if field not in ("manager", "usageLocation")
    }

    if not body:
        return True, ""

    try:
        graph_client.patch_user(user_id, body)
        logger.info(
            "Attribute update applied — user=%s, fields=%s",
            user_id, list(body.keys()),
        )
        return True, ""
    except GraphClientError as e:
        logger.error(
            "Attribute update failed — user=%s, error=%s",
            user_id, str(e),
        )
        return False, str(e)


# Main orchestrator

def run_mover_pipeline(
    payload:      IdentityPayload,
    table_client: TableServiceClient,
    graph_client: JmlGraphClient,
) -> dict:
    """
    Execute the Mover processing flow for a single identity event.

    The EventId is generated internally from the payload. Callers pass
    only the payload and clients — no external event ID is accepted.
    This mirrors the Joiner pattern and keeps event ownership inside
    the pipeline, not in the ingestion layer.

    Args:
        payload:      Canonical IdentityPayload with action=MOVER.
                      Department, job_title, and employment_type reflect
                      the NEW role — the state the user is moving TO.
        table_client: Authenticated TableServiceClient for all Table Storage ops.
        graph_client: Authenticated JmlGraphClient for all Graph API ops.

    Returns:
        dict with final_status, employee_id, event_id, and summary.

    Side effects:
        Reads and writes MoverEventLog and MoverAuditLog tables.
        Reads RetentionRegistry table.
        Graph API calls at Steps 1, 6, 7, 8, and 9.
        PowerShell validation engine call at Step 8.
    """
    employee_id = payload.employee_id

    # EventId is owned by the pipeline, not by the caller.
    # The same deterministic hash is produced regardless of which path
    # (CSV, API, HTTP trigger) invokes this function.
    event_id = generate_event_id(
        employee_id,
        "Mover",
        payload.start_date.isoformat(),
    )

    audit_record: dict = {
        "event_type":       "MOVE",
        "employee_id":      employee_id,
        "event_id":         event_id,
        "source":           "BAMBOOHR",
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "actions_taken":    [],
        "warnings":         [],
        "post_move_status": MoverEventStatus.RECEIVED,
    }

    # Pre-Step — Claim event in JmlEvents.
    conn_str          = os.environ.get("JML_STORAGE_CONNECTION_STRING", "")
    jml_events_client = get_events_table_client(conn_str)

    payload_json_str = json.dumps({
        "employee_id": employee_id,
        "action":      "Mover",
        "event_id":    event_id,
    })

    claimed = claim_event(
        table_client   = jml_events_client,
        employee_id    = employee_id,
        action         = "Mover",
        start_date     = payload.start_date.isoformat(),
        payload_json   = payload_json_str,
        correlation_id = event_id,
    )

    if not claimed:
        logger.info(
            "Mover event already claimed in JmlEvents — idempotency exit — "
            "employee=%s", employee_id,
        )
        return {
            "final_status": MoverEventStatus.QUEUED_CONCURRENT,
            "employee_id":  employee_id,
            "event_id":     event_id,
            "summary":      "Duplicate event — already claimed in JmlEvents.",
        }


    # Step 1 — Current state discovery + concurrent event check

    logger.info(
        "Mover Step 1 — current state discovery — employee=%s", employee_id
    )

    is_concurrent = _check_concurrent_event(table_client, employee_id)
    if is_concurrent:
        logger.warning(
            "Concurrent Mover event detected — employee=%s, "
            "queuing with status QUEUED_CONCURRENT",
            employee_id,
        )
        _write_event_log(
            table_client = table_client,
            employee_id  = employee_id,
            event_id     = event_id,
            status       = MoverEventStatus.QUEUED_CONCURRENT,
        )
        return {
            "final_status": MoverEventStatus.QUEUED_CONCURRENT,
            "employee_id":  employee_id,
            "event_id":     event_id,
            "summary":      (
                "Event queued — another Mover event is in progress "
                "for this employee."
            ),
        }

    _write_event_log(
        table_client = table_client,
        employee_id  = employee_id,
        event_id     = event_id,
        status       = MoverEventStatus.IN_PROGRESS,
    )

    # Fetch current Entra user
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

    # Fetch current access package assignments.
    # current_policy_map carries each package's actual assignmentPolicyId
    # from the tenant — Step 7 removals use this rather than re-deriving
    # a policy from rules.json, since the real assignment is authoritative.
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
        # Best-effort human label per package — real displayName where
        # Entra already has the assignment (currently held packages),
        # falling back to rule_id for packages not yet held. Used only
        # for log readability; audit_record always stores the raw ID.
        package_labels: dict[str, str] = {
            a["accessPackage"]["id"]: a["accessPackage"].get("displayName", a["accessPackage"]["id"])
            for a in current_assignments
            if a.get("accessPackage", {}).get("id")
        }
    except GraphClientError as e:
        logger.error(
            "Step 1 failed — access package assignment fetch failed — "
            "employee=%s, error=%s",
            employee_id, str(e),
        )
        return _handle_early_failure(
            table_client, jml_events_client, employee_id, event_id,
            audit_record, f"Access package assignment fetch failed: {str(e)}",
            failure_step="AccessPackageAssignmentFetch", lock_acquired=False,
        )

    # Acquire processing lock in JmlEvents.
    instance_id = str(_uuid.uuid4())
    acquire_lock(
        table_client = jml_events_client,
        employee_id  = employee_id,
        event_id     = event_id,
        instance_id  = instance_id,
    )

    # Current attributes for attribute delta
    current_attributes: dict = {
        "department":     current_user.get("department"),
        "jobTitle":       current_user.get("job_title"),
        "officeLocation": None,
        "usageLocation":  None,
        "employeeType":   None,
    }


    # Step 2 — Target state calculation

    logger.info(
        "Mover Step 2 — target state calculation — employee=%s, "
        "normalized department=%r, job_title=%r",
        employee_id, payload.department, payload.job_title,
    )

    try:
        _rules_path = os.environ.get(
            "JML_MAPPING_RULES_PATH", "config/role_mapping_rules.json"
        )
        mapping_rules = load_mapping_rules(rules_path=_rules_path)
    except Exception as e:
        return _handle_early_failure(
            table_client, jml_events_client, employee_id, event_id,
            audit_record, f"Mapping rules load failed: {str(e)}",
            failure_step="MappingRulesLoad", lock_acquired=True,
        )

    # Resolve entitlements for the NEW role (payload carries new state).
    # access_packages, not the legacy .groups field — see module NOTE.
    new_resolved = resolve_entitlements(
        rules           = mapping_rules,
        department      = payload.department,
        job_title       = payload.job_title,
        employment_type = payload.employment_type.value,
        employee_id     = payload.employee_id,
    )
    target_packages = frozenset(
        ap.access_package_id for ap in new_resolved.access_packages
    )
    new_policy_map = {
        ap.access_package_id: ap.policy_id for ap in new_resolved.access_packages
    }

    # Build managed catalogue — every access package ID defined anywhere
    # in rules.json, regardless of whether it matched this identity.
    managed_catalogue = frozenset(
        pkg_id
        for rule in mapping_rules
        for pkg_id in [rule.get("entitlements", {}).get("accessPackageId")]
        if pkg_id
    )

    # Resolve entitlements for the OLD role using current Entra attributes.
    # Used only to fill in a rule_id label fallback for package_labels
    # (removed packages that don't have a displayName yet at this point).
    # Removal policy comes from
    # current_policy_map instead, not this resolution.
    old_resolved = resolve_entitlements(
        rules           = mapping_rules,
        department      = current_user.get("department") or payload.department,
        job_title       = current_user.get("job_title") or payload.job_title,
        employment_type = payload.employment_type.value,
        employee_id     = payload.employee_id,
    )

    # Fill in rule_id as a fallback label for any package not already
    # covered by a real displayName from current_assignments — mainly
    # matters for packages being newly added, which have no displayName
    # to draw on yet.
    for ap in new_resolved.access_packages:
        package_labels.setdefault(ap.access_package_id, ap.rule_id)
    for ap in old_resolved.access_packages:
        package_labels.setdefault(ap.access_package_id, ap.rule_id)

    # Resolve manager ID for attribute delta
    resolved_manager_id = _resolve_manager_id(graph_client, payload.manager_id)

    incoming_attributes: dict = {
        "department":    payload.department,
        "jobTitle":      payload.job_title,
        "manager":       resolved_manager_id,
        "employeeType":  payload.employment_type.value,
        "usageLocation": payload.location,
    }


    # Step 3 — Delta analysis

    logger.info(
        "Mover Step 3 — delta analysis — employee=%s", employee_id
    )

    # calculate_delta()'s own parameter names (current_groups, target_groups)
    # are unchanged — delta_engine.py is generic over any object ID set.
    # Only the values passed here have changed meaning, from group IDs
    # to access package IDs.
    delta = calculate_delta(
        current_groups    = current_packages,
        target_groups     = target_packages,
        managed_catalogue = managed_catalogue,
    )

    attr_delta = calculate_attribute_delta(
        current_attributes  = current_attributes,
        incoming_attributes = incoming_attributes,
    )

    audit_record["from_department"]   = current_user.get("department", "")
    audit_record["to_department"]     = payload.department
    audit_record["from_title"]        = current_user.get("job_title", "")
    audit_record["to_title"]          = payload.job_title
    audit_record["attribute_changes"] = {
        change.field: {
            "from": change.from_value,
            "to":   change.to_value,
        }
        for change in attr_delta.changes
    }
    audit_record["unmanaged_packages"] = [
        {"id": pkg_id, "action": "NOT_PROCESSED"}
        for pkg_id in delta.unmanaged
    ]

    if delta.unmanaged:
        for pkg_id in delta.unmanaged:
            label = package_labels.get(pkg_id, pkg_id)
            logger.info(
                "  ⊘ %s — unmanaged, not in any rule's entitlements — "
                "excluded from all delta logic (NOT_PROCESSED)",
                label,
            )
        logger.info(
            "Step 3 — %d unmanaged package(s) detected, left untouched",
            len(delta.unmanaged),
        )


    # Step 4 — Retention evaluation

    retention_result = evaluate_all_retentions(
        employee_id   = employee_id,
        resource_ids  = delta.groups_to_remove,
        resource_type = "accessPackage",
        table_client  = table_client,
    )

    audit_record["packages_retained"] = [
        {
            "id":               d.resource_id,
            "retention_reason": d.record.reason if d.record else "",
            "review_date":      (
                d.record.review_date.isoformat() if d.record else ""
            ),
        }
        for d in retention_result.decisions
        if d.outcome.value == "RETAINED"
    ]

    if delta.groups_to_remove:
        retained_count = 0
        for d in retention_result.decisions:
            label = package_labels.get(d.resource_id, d.resource_id)
            if d.outcome.value == "RETAINED":
                retained_count += 1
                until = d.record.review_date.isoformat() if d.record else "unknown"
                reason = d.record.reason if d.record else ""
                logger.info(
                    "  ⊘ %s — retention detected, excluded from removal "
                    "(until=%s, reason=%r)",
                    label, until, reason,
                )
            elif d.outcome.value == "EXPIRED":
                logger.info(
                    "  → %s — retention record expired, proceeding with removal",
                    label,
                )
            else:
                logger.info(
                    "  → %s — no retention record, proceeding with removal",
                    label,
                )
        logger.info(
            "Step 4 complete — %d retained, %d confirmed for removal",
            retained_count, len(retention_result.remove_confirmed),
        )


    # Step 5 — SoD evaluation (ADR-008 / ADR-011 deferred)
    #
    # Preventive SoD enforcement happens at the platform level via Entra
    # ID Entitlement Management access package incompatibility policies.
    # No Python check runs here — see the module docstring NOTE.
    #
    # When ADR-011's pre-flight incompatibility check lands, this is
    # where the strategy decision gets logged, e.g.:
    #   if incompatible:
    #       logger.info("SoD conflict detected — %s vs %s — using Strategy B (remove-then-add)", ...)
    #   else:
    #       logger.info("No SoD conflict detected — using Strategy A (add-then-remove)")
    # Until then, Strategy A is the only path, unconditionally.

    logger.info(
        "Step 5 — no pre-flight SoD check (platform-enforced, ADR-008; "
        "pre-flight incompatibility check is ADR-011, not yet built) — "
        "employee=%s", employee_id,
    )
    audit_record["sod_evaluation"] = "SoDEvaluationSkipped-ADR008"
    audit_record["sod_escalations"] = []


    # Step 6 — Access package additions (ADR-009 Strategy A: add first)

    logger.info(
        "Mover Step 6 — access package additions — employee=%s", employee_id
    )

    addition_actions, delivered_additions, additions_all_succeeded = _execute_package_additions(
        graph_client    = graph_client,
        user_id         = user_id,
        packages_to_add = delta.groups_to_add,
        policy_map      = new_policy_map,
        package_labels  = package_labels,
    )
    audit_record["actions_taken"].extend(addition_actions)
    audit_record["packages_added"] = [
        {"id": a["package_id"]}
        for a in addition_actions
        if a["succeeded"]
    ]

    if not delta.groups_to_add:
        logger.info(
            "Step 6 — no packages to add for this transition — employee=%s",
            employee_id,
        )
    elif additions_all_succeeded:
        logger.info(
            "Step 6 — all %d addition(s) delivered — employee=%s",
            len(delta.groups_to_add), employee_id,
        )
    else:
        audit_record["warnings"].append(
            "One or more package additions did not deliver — "
            "removals skipped this pass per ADR-009 (never remove old "
            "access before new access is confirmed). See actions_taken "
            "for which package(s) failed."
        )
        logger.warning(
            "Step 6 — not all additions delivered — removals skipped "
            "this pass — employee=%s", employee_id,
        )


    # Step 7 — Access package removals + attribute update
    #
    # Removals only proceed if every addition in Step 6 delivered.
    # This is the ADR-009 safety gate: a failed addition must never be
    # followed by a removal, or the user ends up with strictly less
    # access than before the move.

    logger.info(
        "Mover Step 7 — access package removals — employee=%s", employee_id
    )

    if additions_all_succeeded:
        removal_actions = _execute_package_removals(
            graph_client        = graph_client,
            user_id             = user_id,
            remove_confirmed    = retention_result.remove_confirmed,
            current_policy_map  = current_policy_map,
            package_labels      = package_labels,
        )
        audit_record["actions_taken"].extend(removal_actions)
        audit_record["packages_removed"] = [
            {"id": a["package_id"], "reason": "ROLE_CHANGE"}
            for a in removal_actions
            if a["succeeded"]
        ]
        recently_removed = frozenset(
            a["package_id"] for a in removal_actions if a["succeeded"]
        )

        # Attribute PATCH commits the "new role" onto the user object, so it
        # belongs to the same transaction as the removals and is gated on the
        # same ADR-009 condition. Writing the new department/title while the
        # additions have NOT all delivered would leave the identity's
        # attributes claiming the new role while its packages still reflect the
        # old one — a half-applied state that a later run's post-move
        # verification (and the governance engine) would then read against an
        # inconsistent object.
        attr_succeeded, attr_error = _execute_attribute_update(
            graph_client = graph_client,
            user_id      = user_id,
            patch_dict   = attr_delta.to_patch_dict(),
        )
        if not attr_succeeded:
            audit_record["warnings"].append(
                f"Attribute update failed: {attr_error}"
            )
    else:
        audit_record["packages_removed"] = []
        recently_removed = frozenset()
        audit_record["warnings"].append(
            "Attribute update deferred — package additions did not all "
            "deliver, so the role transition is not committed this pass "
            "(ADR-009). Department/title remain at their previous values "
            "until a retry lands every addition."
        )


    # Step 8 — Post-move verification

    logger.info(
        "Mover Step 8 — post-move verification — employee=%s", employee_id
    )

    verification = verify_post_move_state(
        graph_client     = graph_client,
        user_id          = user_id,
        employee_id      = employee_id,
        unchanged        = delta.unchanged,
        retain_set       = retention_result.retain_set,
        packages_to_add  = delta.groups_to_add,
        unmanaged        = delta.unmanaged,
        recently_removed = recently_removed,
    )

    audit_record["post_move_verification"] = {
        "status":              verification.status.value,
        "discrepancies":       [
            {"package_id": d.resource_id, "kind": d.kind}
            for d in verification.discrepancies
        ],
        "governance_passed":   (
            verification.governance_result.passed
            if verification.governance_result else False
        ),
        "governance_warnings": (
            verification.governance_result.warning_summary()
            if verification.governance_result else []
        ),
    }


    # Step 9 — Final status + audit record

    logger.info(
        "Mover Step 9 — audit reporting — employee=%s", employee_id
    )

    if verification.status == PostMoveStatus.VERIFICATION_ERROR:
        final_status = MoverEventStatus.MOVE_FAILED
    elif verification.status == PostMoveStatus.MOVE_PARTIAL:
        final_status = MoverEventStatus.MOVE_PARTIAL
    else:
        final_status = MoverEventStatus.MOVE_SUCCESS

    audit_record["post_move_status"] = final_status
    _write_event_log(
        table_client, employee_id, event_id, final_status,
        retention_applied=bool(retention_result.retain_set),
    )
    _write_audit_record(table_client, employee_id, event_id, audit_record)

    jml_final_status = (
        EventStatus.COMPLETED
        if final_status == MoverEventStatus.MOVE_SUCCESS
        else EventStatus.FAILED
    )
    release_lock(jml_events_client, employee_id, event_id)
    update_event_status(
        table_client = jml_events_client,
        employee_id  = employee_id,
        event_id     = event_id,
        status       = jml_final_status,
        failure_step = (
            "PostMoveVerification"
            if final_status == MoverEventStatus.MOVE_PARTIAL
            else ""
        ),
    )

    logger.info(
        "Mover pipeline complete — employee=%s, status=%s",
        employee_id, final_status,
    )

    return {
        "final_status": final_status,
        "employee_id":  employee_id,
        "event_id":     event_id,
        "summary":      f"Mover event completed with status {final_status}.",
    }


# Helpers

def _fail(employee_id: str, event_id: str, reason: str) -> dict:
    """Return a standard failure response dict."""
    return {
        "final_status": MoverEventStatus.MOVE_FAILED,
        "employee_id":  employee_id,
        "event_id":     event_id,
        "summary":      reason,
    }


def _handle_early_failure(
    table_client:       TableServiceClient,
    jml_events_client:  TableServiceClient,
    employee_id:        str,
    event_id:            str,
    audit_record:        dict,
    reason:              str,
    failure_step:        str,
    lock_acquired:        bool = False,
) -> dict:
    """
    Handle a failure that occurs before Step 9's own cleanup runs.

    Every terminal path in this pipeline must produce a MoverAuditLog
    record — the Audit Layer principle is "a report for every event
    regardless of outcome," and an early Step 1/2 failure is still an
    outcome. Before this helper existed, early failures wrote only to
    MoverEventLog, leaving MoverAuditLog silent for exactly the events
    most worth investigating.

    Also releases the JmlEvents lock when lock_acquired is True. A
    failure after acquire_lock() but before Step 9 would otherwise
    leak the lock for the full STALE_LOCK_MINUTES window.
    """
    audit_record["post_move_status"] = MoverEventStatus.MOVE_FAILED
    audit_record["warnings"].append(reason)

    _write_event_log(
        table_client, employee_id, event_id, MoverEventStatus.MOVE_FAILED
    )
    _write_audit_record(table_client, employee_id, event_id, audit_record)

    if lock_acquired:
        release_lock(jml_events_client, employee_id, event_id)

    update_event_status(
        table_client = jml_events_client,
        employee_id  = employee_id,
        event_id     = event_id,
        status       = EventStatus.FAILED,
        failure_step = failure_step,
    )

    return _fail(employee_id, event_id, reason)


# Azure Function HTTP entry point

def main(req):
    """
    Azure Function HTTP trigger entry point.

    Expects a JSON body with a canonical IdentityPayload.
    event_id is no longer accepted from the caller — it is generated
    internally by run_mover_pipeline() from the payload fields.

    Environment variables required:
        AZURE_STORAGE_CONNECTION_STRING
        AZURE_TENANT_ID
        AZURE_CLIENT_ID
        AZURE_CLIENT_SECRET
        JML_VALIDATION_ENGINE_URL
    """
    import azure.functions as func

    try:
        body = req.get_json()
        raw = body["payload"]
        if isinstance(raw.get("start_date"), str):
            from datetime import date
            raw["start_date"] = date.fromisoformat(raw["start_date"])
        if isinstance(raw.get("employment_type"), str):
            raw["employment_type"] = EmploymentType(raw["employment_type"])
        if isinstance(raw.get("action"), str):
            raw["action"] = JmlAction(raw["action"])
        payload = IdentityPayload(**raw)

        conn_str     = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        table_client = _get_table_client(conn_str)
        graph_service_client, credential = build_graph_client()
        graph_client = JmlGraphClient(graph_service_client, credential)

        result = run_mover_pipeline(
            payload      = payload,
            table_client = table_client,
            graph_client = graph_client,
        )
        return func.HttpResponse(
            json.dumps(result),
            status_code = 200,
            mimetype    = "application/json",
        )
    except Exception as e:
        logger.error("Mover HTTP trigger failed: %s", str(e))
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code = 500,
            mimetype    = "application/json",
        )