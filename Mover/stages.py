"""
Mover/stages.py

The Mover pipeline decomposed into independently-testable stages, mirroring
Joiner/stages.py. Each stage returns a StageResult (plain, serializable),
never touches the audit_record, and never decides control flow. The driver
(sync today, Durable orchestrator later) reads each StageResult and decides.

This file holds the zero-wait stages (claim → concurrent → fetch → resolve →
delta → retention) plus the helpers moved out of mover_http so a Durable
activity can reach them without importing the trigger module. The
submit/poll/finalize provisioning seam and verify/finalize stages land
alongside once the sync driver is proven identical.

Import direction is one-way: this module never imports from mover_http.
mover_http composes these stages; nothing here reaches back up.

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
from Mapping.mapping_loader import load_mapping_rules
from Mapping.mapping_resolver import resolve_entitlements
from Provisioning.graph_client import JmlGraphClient, GraphClientError
from Mover.delta_engine import calculate_delta
from Mover.attribute_delta import calculate_attribute_delta
from Mover.retention_evaluator import evaluate_all_retentions, RetentionOutcome
from Mover.post_move_verifier import verify_post_move_state, PostMoveStatus
from Mover.stage_result import StageResult, StageOutcome
from Functions.Event_store.event_store import (
    claim_event,
    acquire_lock,
)

logger = logging.getLogger(__name__)

MOVER_EVENT_LOG_TABLE = "MoverEventLog"
STALE_LOCK_MINUTES    = 10
RETAINED              = RetentionOutcome.RETAINED.value


# Helpers moved from mover_http — live here so a Durable activity can
# reach them without importing the trigger module.

def _is_stale_in_progress(row: dict) -> bool:
    """
    True if an IN_PROGRESS event log row is older than STALE_LOCK_MINUTES.

    A 504-killed run leaves its EventLog row IN_PROGRESS with no terminal
    write. Without this, a reclaimed retry is blocked as QUEUED_CONCURRENT
    even though the JmlEvents lock has already been reclaimed. Same window
    as the event-store lock, so both agree on when a run is dead.
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
    True if a genuinely-live IN_PROGRESS Mover event exists for this
    employee. A stale IN_PROGRESS row (older than STALE_LOCK_MINUTES) is
    logged and skipped so a reclaimed retry can proceed. Fails closed on
    a query error (returns True) — the same conservative posture as the
    original.
    """
    try:
        client   = table_client.get_table_client(MOVER_EVENT_LOG_TABLE)
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
            "MoverEventLog concurrent check failed — employee=%s, error=%s",
            employee_id, str(e),
        )
        return True


def resolve_manager_id(
    graph_client: JmlGraphClient,
    manager_id:   str | None,
) -> str | None:
    """
    Resolve a manager employee number to an Entra object ID.

    Returns the Entra object ID if found, None if not provided or not
    found. A missing manager is not fatal for a Mover — recorded as a
    warning by the caller. The result feeds incoming_attributes["manager"]
    for the attribute delta but is excluded from the actual PATCH body.
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


# Zero-wait stages

def stage_claim(
    payload:           IdentityPayload,
    event_id:          str,
    jml_events_client: TableServiceClient,
) -> StageResult:
    """
    Pre-Step — atomic claim in JmlEvents. A duplicate claim is the
    idempotency exit; the driver maps DUPLICATE to the QUEUED_CONCURRENT
    response string the current pipeline returns.
    """
    payload_json = json.dumps({
        "employee_id": payload.employee_id,
        "action":      "Mover",
        "event_id":    event_id,
    })
    claimed = claim_event(
        table_client   = jml_events_client,
        employee_id    = payload.employee_id,
        action         = "Mover",
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


def stage_concurrent_check(
    payload:      IdentityPayload,
    table_client: TableServiceClient,
) -> StageResult:
    """
    Step 1a — concurrent-event guard via MoverEventLog (stale-IN_PROGRESS
    window applied). The driver owns the MoverEventLog writes (IN_PROGRESS
    on proceed, QUEUED_CONCURRENT on queue) — those are audit/state side
    effects, not stage logic.
    """
    if check_concurrent_event(table_client, payload.employee_id):
        return StageResult(
            ok=True,
            outcome=StageOutcome.QUEUED,
            summary="Another Mover event is in progress for this employee.",
        )
    return StageResult(ok=True, outcome=StageOutcome.PROCEED)


def stage_fetch_current_state(
    payload:           IdentityPayload,
    event_id:          str,
    graph_client:      JmlGraphClient,
    jml_events_client: TableServiceClient,
) -> StageResult:
    """
    Step 1b — fetch the user and current delivered package assignments,
    build current_packages / current_policy_map / package_labels /
    current_attributes, and acquire the JmlEvents lock (post-fetch — the
    Mover's lock point).

    A user-fetch or assignment-fetch failure returns FAILED with
    failure_step and lock_acquired=False in data — the driver routes these
    to _handle_early_failure.
    """
    try:
        current_user = graph_client.get_user(payload.upn)
        user_id = current_user["id"]
    except GraphClientError as e:
        return _fetch_failure("UserFetch", f"User fetch failed: {e}")

    try:
        current_assignments = graph_client.get_current_access_package_assignments(
            user_id=user_id,
        )
    except GraphClientError as e:
        return _fetch_failure(
            "AccessPackageAssignmentFetch",
            f"Access package assignment fetch failed: {e}",
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

    current_attributes = {
        "department":     current_user.get("department"),
        "jobTitle":       current_user.get("job_title"),
        "officeLocation": None,
        "usageLocation":  None,
        "employeeType":   None,
    }

    return StageResult(
        ok=True,
        outcome=StageOutcome.PROCEED,
        data={
            "user_id":            user_id,
            "current_department": current_user.get("department", ""),
            "current_job_title":  current_user.get("job_title", ""),
            "current_packages":   current_packages,
            "current_policy_map": current_policy_map,
            "package_labels":     package_labels,
            "current_attributes": current_attributes,
            "lock_acquired":      True,
        },
    )


def stage_resolve(
    payload:            IdentityPayload,
    current_department: str,
    current_job_title:  str,
    package_labels:     dict,
    graph_client:       JmlGraphClient,
) -> StageResult:
    """
    Step 2 — load rules, resolve new + old role, build managed catalogue and
    new_policy_map, extend package_labels with rule_id fallbacks, resolve the
    manager, build incoming_attributes.

    A rules-load failure returns FAILED (MappingRulesLoad) with
    lock_acquired=True — the lock is already held, so the driver routes to
    _handle_early_failure and releases it.
    """
    try:
        rules_path = os.environ.get(
            "JML_MAPPING_RULES_PATH", "config/role_mapping_rules.json"
        )
        mapping_rules = load_mapping_rules(rules_path=rules_path)
    except Exception as e:
        return StageResult(
            ok=False,
            outcome=StageOutcome.FAILED,
            data={"failure_step": "MappingRulesLoad", "lock_acquired": True},
            report_warnings=[f"Mapping rules load failed: {e}"],
            summary=f"Mapping rules load failed: {e}",
        )

    new_resolved = resolve_entitlements(
        rules           = mapping_rules,
        department      = payload.department,
        job_title       = payload.job_title,
        employment_type = payload.employment_type.value,
        employee_id     = payload.employee_id,
    )
    target_packages = [ap.access_package_id for ap in new_resolved.access_packages]
    new_policy_map = {
        ap.access_package_id: ap.policy_id for ap in new_resolved.access_packages
    }

    managed_catalogue = [
        pkg_id
        for rule in mapping_rules
        for pkg_id in [rule.get("entitlements", {}).get("accessPackageId")]
        if pkg_id
    ]

    old_resolved = resolve_entitlements(
        rules           = mapping_rules,
        department      = current_department or payload.department,
        job_title       = current_job_title or payload.job_title,
        employment_type = payload.employment_type.value,
        employee_id     = payload.employee_id,
    )

    labels = dict(package_labels)
    for ap in new_resolved.access_packages:
        labels.setdefault(ap.access_package_id, ap.rule_id)
    for ap in old_resolved.access_packages:
        labels.setdefault(ap.access_package_id, ap.rule_id)

    resolved_manager_id = resolve_manager_id(graph_client, payload.manager_id)

    incoming_attributes = {
        "department":    payload.department,
        "jobTitle":      payload.job_title,
        "manager":       resolved_manager_id,
        "employeeType":  payload.employment_type.value,
        "usageLocation": payload.location,
    }

    return StageResult(
        ok=True,
        outcome=StageOutcome.PROCEED,
        data={
            "target_packages":     target_packages,
            "new_policy_map":      new_policy_map,
            "managed_catalogue":   managed_catalogue,
            "package_labels":      labels,
            "incoming_attributes": incoming_attributes,
        },
    )


def stage_delta(
    payload:             IdentityPayload,
    current_department:  str,
    current_job_title:   str,
    current_packages:    list,
    target_packages:     list,
    managed_catalogue:   list,
    current_attributes:  dict,
    incoming_attributes: dict,
) -> StageResult:
    """
    Step 3 — pure package delta + attribute delta. Emits the four package
    sets the provisioning seam needs, the attribute patch_dict, and the
    delta audit fields (audit_* keys the driver copies into audit_record).
    """
    delta = calculate_delta(
        current_groups    = frozenset(current_packages),
        target_groups     = frozenset(target_packages),
        managed_catalogue = frozenset(managed_catalogue),
    )
    attr_delta = calculate_attribute_delta(
        current_attributes  = current_attributes,
        incoming_attributes = incoming_attributes,
    )

    attribute_changes = {
        change.field: {"from": change.from_value, "to": change.to_value}
        for change in attr_delta.changes
    }

    return StageResult(
        ok=True,
        outcome=StageOutcome.PROCEED,
        data={
            "groups_to_add":            sorted(delta.groups_to_add),
            "groups_to_remove":         sorted(delta.groups_to_remove),
            "unchanged":                sorted(delta.unchanged),
            "unmanaged":                sorted(delta.unmanaged),
            "patch_dict":               attr_delta.to_patch_dict(),
            "audit_from_department":    current_department,
            "audit_to_department":      payload.department,
            "audit_from_title":         current_job_title,
            "audit_to_title":           payload.job_title,
            "audit_attribute_changes":  attribute_changes,
            "audit_unmanaged_packages": [
                {"id": pkg_id, "action": "NOT_PROCESSED"}
                for pkg_id in sorted(delta.unmanaged)
            ],
        },
    )


def stage_retention(
    payload:          IdentityPayload,
    groups_to_remove: list,
    table_client:     TableServiceClient,
) -> StageResult:
    """
    Step 4 — retention evaluation against RetentionRegistry.

    Emits the full decision set (RETAINED / EXPIRED / NO_RECORD) as plain
    primitives plus a summary count, so the complete retention outcome is
    persisted in MoverAuditLog and queryable later without having watched
    the log stream. Dates are reduced to ISO strings here so nothing
    date-typed crosses a future activity boundary.
    """
    retention_result = evaluate_all_retentions(
        employee_id   = payload.employee_id,
        resource_ids  = frozenset(groups_to_remove),
        resource_type = "accessPackage",
        table_client  = table_client,
    )

    decisions = []
    retained_count = 0
    expired_count = 0
    no_record_count = 0

    for d in retention_result.decisions:
        outcome = d.outcome.value
        decisions.append({
            "package_id":     d.resource_id,
            "outcome":        outcome,
            "reason":         d.record.reason if d.record else "",
            "review_date":    d.record.review_date.isoformat() if d.record else "",
        })
        if outcome == RETAINED:
            retained_count += 1
        elif outcome == "EXPIRED":
            expired_count += 1
        else:
            no_record_count += 1

    packages_retained = [
        {"id": d["package_id"], "retention_reason": d["reason"], "review_date": d["review_date"]}
        for d in decisions
        if d["outcome"] == RETAINED
    ]

    return StageResult(
        ok=True,
        outcome=StageOutcome.PROCEED,
        data={
            "remove_confirmed":         sorted(retention_result.remove_confirmed),
            "retain_set":               sorted(retention_result.retain_set),
            "audit_packages_retained":  packages_retained,
            "audit_retention_decisions": decisions,
            "audit_retention_summary": {
                "evaluated":  len(decisions),
                "retained":   retained_count,
                "expired":    expired_count,
                "no_record":  no_record_count,
                "removed":    len(retention_result.remove_confirmed),
            },
            "retention_applied":        bool(retention_result.retain_set),
        },
    )

def stage_verify(
    payload:          IdentityPayload,
    user_id:          str,
    unchanged:        list,
    retain_set:       list,
    packages_to_add:  list,
    unmanaged:        list,
    recently_removed: list,
    graph_client:     JmlGraphClient,
) -> StageResult:
    """
    Step 8 — post-move verification. Wraps verify_post_move_state with
    delay_seconds=0: the driver owns the propagation wait (sync: time.sleep;
    durable: timer), so no sleep happens inside the stage.

    Reduces PostMoveVerificationResult (enum + discrepancy objects +
    GovernanceResult) to plain primitives — the status string, discrepancy
    dicts, governance bool + warnings list — so nothing non-serializable
    crosses a future activity boundary. The driver maps the status string to
    the terminal MoverEventStatus, exactly as the current Step 9 does.
    """
    verification = verify_post_move_state(
        graph_client     = graph_client,
        user_id          = user_id,
        employee_id      = payload.employee_id,
        employment_type  = payload.employment_type.value,
        unchanged        = frozenset(unchanged),
        retain_set       = frozenset(retain_set),
        packages_to_add  = frozenset(packages_to_add),
        unmanaged        = frozenset(unmanaged),
        recently_removed = frozenset(recently_removed),
        delay_seconds    = 0,
    )

    post_move_verification = {
        "status":        verification.status.value,
        "discrepancies": [
            {"package_id": d.resource_id, "kind": d.kind}
            for d in verification.discrepancies
        ],
        "governance_passed": (
            verification.governance_result.passed
            if verification.governance_result else False
        ),
        "governance_failures": (
            verification.governance_result.failure_summary()
            if verification.governance_result else []
        ),
        "governance_warnings": (
            verification.governance_result.warning_summary()
            if verification.governance_result else []
        ),
    }

    return StageResult(
        ok=True,
        outcome=StageOutcome.PROCEED,
        data={
            "verification_status":         verification.status.value,
            "audit_post_move_verification": post_move_verification,
        },
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