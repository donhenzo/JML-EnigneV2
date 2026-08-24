"""
Joiner/stages.py

The Joiner pipeline as a sequence of independent stages. Each stage:
  - takes plain inputs (an IdentityPayload or primitives, optional clients)
  - does one job
  - returns a StageResult describing what happened, in serializable data

Stages never touch DecisionReport and never decide control flow. They record
what happened as report_actions/report_warnings/hold_reasons; the driver (the
sync joiner_pipeline today, a Durable orchestrator later) reads the
StageResult, applies the report entries, and decides whether to continue.

Clients are optional-injection: a stage builds what it needs if not given one.
The sync driver passes shared clients (fast, testable); a Durable activity
calls the stage with no clients and lets it build its own.
"""

from __future__ import annotations

import logging

from Ingestion.schema import IdentityPayload
from Normalization.lookup_loader import load_lookup_table
from Normalization.normalizer import Normalizer
from Joiner.stage_result import StageResult, StageOutcome

from Functions.Event_store.event_store import (
    get_events_table_client,
    generate_event_id,
    claim_event,
    check_active_event,
    EventStatus,
)

from Functions.Event_store.conflict_queue import (
    check_and_handle_conflict,
    ConflictOutcome,
)

from Mapping.mapping_loader import load_mapping_rules
from Mapping.mapping_resolver import resolve_entitlements

from Validation.validation_gate import pre_provision_validate, post_provision_validate
from datetime import date
from Audit.models import DecisionReport, ReportEvent
from Mapping.mapping_resolver import (
    EntitlementResult,
    AccessPackageAssignment,
    PimGroup,
)
from Provisioning.provisioner import provision_joiner
from Provisioning.graph_client import build_graph_client, JmlGraphClient

from Functions.Event_store.event_store import (
    release_lock,
    update_event_status,
)
from Functions.Event_store.conflict_queue import release_next_queued_event


logger = logging.getLogger(__name__)


def stage_normalize(
    payload: IdentityPayload,
    normalizer: Normalizer | None = None,
    lookup_path: str = "config/canonical_lookup.json",
) -> StageResult:
    """
    Resolve raw department/job_title to canonical values.

    On success: outcome=PROCEED, data carries the normalized payload as a dict
    so the next stage can reconstruct it. On failure: outcome=HELD with the
    unresolved-field reasons — the driver routes the record to the hold queue.

    normalizer is optional-injection: the sync driver builds one once and passes
    it in (avoids reloading the lookup table per record); a Durable activity
    calls with none and this builds one from lookup_path.
    """
    if normalizer is None:
        normalizer = Normalizer(load_lookup_table(lookup_path))

    result = normalizer.normalize(payload)

    if not result.passed:
        logger.warning(
            "Normalization failed — employee=%s, failures=%s",
            payload.employee_id, result.failures,
        )
        return StageResult(
            ok=False,
            outcome=StageOutcome.HELD,
            hold_reasons=result.failures,
            summary=f"Normalization failed: {result.failures}",
        )

    normalized = result.payload
    return StageResult(
        ok=True,
        outcome=StageOutcome.PROCEED,
        data={"payload": normalized.to_dict()},
        report_actions=[{
            "action": "NormalizationPassed",
            "detail": (
                f"department={normalized.department}, "
                f"job_title={normalized.job_title}"
            ),
            "succeeded": True,
        }],
        summary="Normalization passed",
    )


def stage_claim_event(
    payload: IdentityPayload,
    correlation_id: str = "",
    events_client=None,
    connection_string: str = "",
) -> StageResult:
    """
    Claim the event slot before any provisioning work begins.

    Folds three of today's inline steps into one stage:
      1. Active-event / stale-lock check (was run_pipeline Step 3, the GAP-002
         remediation) — if another instance is actively PROCESSING this
         employee, skip. check_active_event auto-reclaims stale locks.
      2. Atomic claim (claim_event) — the idempotency gate. A duplicate exits
         cleanly. Reclaim of a Failed/stale event is handled inside claim_event
         (ADR-013), so a reclaimable event returns claimed=True here and
         proceeds, exactly as a fresh claim would.
      3. Deterministic event_id derivation, returned in data for later stages.

    Outcomes:
      PROCEED   — claimed (or reclaimed); event_id in data.
      SKIPPED   — another instance is actively processing this employee.
      DUPLICATE — event already exists and is not reclaimable; idempotency exit.

    events_client is optional-injection: the sync driver passes a shared client;
    a Durable activity calls with none and this builds one from connection_string.
    """
    if events_client is None:
        events_client = get_events_table_client(connection_string)

    employee_id = payload.employee_id

    active = check_active_event(
        table_client=events_client,
        employee_id=employee_id,
    )

    if active and active.status == EventStatus.PROCESSING:
        logger.info(
            "Active event in progress — employee=%s, locked_by=%s, skipping",
            employee_id, active.locked_by,
        )
        return StageResult(
            ok=True,
            outcome=StageOutcome.SKIPPED,
            report_actions=[{
                "action": "ActiveEventSkipped",
                "detail": f"Another instance processing this employee (locked by {active.locked_by})",
                "succeeded": True,
            }],
            summary="Active event in progress — skipped",
        )

    import json
    payload_json = json.dumps({
        "employee_id": employee_id,
        "upn":         payload.upn,
        "action":      payload.action.value,
        "start_date":  payload.start_date.isoformat(),
    })

    claimed = claim_event(
        table_client=events_client,
        employee_id=employee_id,
        action=payload.action.value,
        start_date=payload.start_date.isoformat(),
        payload_json=payload_json,
        correlation_id=correlation_id,
    )

    event_id = generate_event_id(
        employee_id,
        payload.action.value,
        payload.start_date.isoformat(),
    )

    if not claimed:
        logger.info("Duplicate event — employee=%s, event_id=%s", employee_id, event_id)
        return StageResult(
            ok=True,
            outcome=StageOutcome.DUPLICATE,
            data={"event_id": event_id},
            report_actions=[{
                "action": "DuplicateEventSkipped",
                "detail": "Event already exists in event store, idempotency exit",
                "succeeded": True,
            }],
            summary="Duplicate event — idempotency exit",
        )

    return StageResult(
        ok=True,
        outcome=StageOutcome.PROCEED,
        data={"event_id": event_id},
        summary=f"Event claimed — event_id={event_id}",
    )

def stage_conflict_check(
    payload: IdentityPayload,
    event_id: str,
    events_client=None,
    connection_string: str = "",
) -> StageResult:
    """
    Run the FIFO conflict queue for this employee.

    If another event is already active for the same employee, this event is
    parked (QUEUED) to run after the active one settles. Otherwise PROCEED.

    For the Joiner this is only ever PROCEED or QUEUED — the SUPERSEDE outcome
    is a Leaver-only path (a Leaver cancels pending Joiner/Mover events), which
    never applies here. event_id comes from stage_claim_event's data.

    events_client is optional-injection, same pattern as the claim stage.
    """
    if events_client is None:
        events_client = get_events_table_client(connection_string)

    outcome = check_and_handle_conflict(
        table_client=events_client,
        employee_id=payload.employee_id,
        new_event_id=event_id,
        new_action=payload.action.value,
    )

    if outcome == ConflictOutcome.QUEUED:
        logger.info(
            "Event queued behind an active event — employee=%s, event_id=%s",
            payload.employee_id, event_id,
        )
        return StageResult(
            ok=True,
            outcome=StageOutcome.QUEUED,
            data={"event_id": event_id},
            report_actions=[{
                "action": "EventQueued",
                "detail": "Active event in progress — queued behind existing event",
                "succeeded": True,
            }],
            report_warnings=["Event queued — will process after active event completes"],
            summary="Event queued behind an active event",
        )

    return StageResult(
        ok=True,
        outcome=StageOutcome.PROCEED,
        data={"event_id": event_id},
        summary="No conflict — proceeding",
    )

def stage_resolve_entitlements(
    payload: IdentityPayload,
    event_id: str,
    mapping_rules: list | None = None,
    mapping_rules_path: str = "config/role_mapping_rules.json",
) -> StageResult:
    """
    Resolve which Access Packages this identity should hold, from policy.

    Pure evaluation — no Graph calls, no storage. Every matched rule
    contributes; the result is the union, each package traceable to its rule_id.

    Always PROCEED — resolving to zero packages is a valid (if warned) outcome,
    not a failure. The resolved entitlements ride back in data as dicts so the
    provision stage can rebuild them without re-resolving.

    mapping_rules is optional-injection: the sync driver loads the rules once
    and passes them in; a Durable activity calls with none and this loads them
    from mapping_rules_path.
    """
    if mapping_rules is None:
        mapping_rules = load_mapping_rules(mapping_rules_path)

    entitlements = resolve_entitlements(
        rules=mapping_rules,
        department=payload.department,
        job_title=payload.job_title,
        employment_type=payload.employment_type.value,
        employee_id=payload.employee_id,
    )

    access_packages = [
        {
            "rule_id":                ap.rule_id,
            "access_package_id":      ap.access_package_id,
            "policy_id":              ap.policy_id,
            "duration_override_days": ap.duration_override_days,
        }
        for ap in entitlements.access_packages
    ]

    pim_groups = [
        {
            "group_id":       pg.group_id,
            "display_name":   pg.display_name,
            "eligible_role":  pg.eligible_role,
            "justification":  pg.justification,
            "duration_hours": pg.duration_hours,
        }
        for pg in entitlements.pim_groups
    ]

    report_actions = [{
        "action": "EntitlementsResolved",
        "detail": (
            f"matched_rules={entitlements.matched_rule_ids}, "
            f"access_packages={[ap['access_package_id'] for ap in access_packages]}"
        ),
        "succeeded": True,
    }]

    report_warnings = []
    if not entitlements.matched_rule_ids:
        report_warnings.append(
            "No mapping rules matched — user will have no access package assignments"
        )

    return StageResult(
        ok=True,
        outcome=StageOutcome.PROCEED,
        data={
            "event_id":         event_id,
            "matched_rule_ids": entitlements.matched_rule_ids,
            "access_packages":  access_packages,
            "pim_groups":       pim_groups,
        },
        report_actions=report_actions,
        report_warnings=report_warnings,
        summary=f"Resolved {len(access_packages)} access package(s)",
    )

def stage_pre_validate(
    payload: IdentityPayload,
    event_id: str,
) -> StageResult:
    """
    Pre-provision governance gate — evaluate the canonical payload before any
    Entra object exists. Wraps pre_provision_validate (which threads the
    synthetic identity ID into the PreProvision call, ADR-017).

    Outcomes:
      PROCEED — validation passed; warnings ride back for the driver to record.
      HELD    — validation failed; the driver routes the record to the hold
                queue using hold_reasons, and marks the event Failed with
                failure_step=PreProvisionValidation.

    No client injection needed — the validation gate builds its own HTTP call
    (or returns the skipped result when JML_SKIP_VALIDATION_ENGINE is set).
    """
    result = pre_provision_validate(payload)

    if not result.passed:
        logger.warning(
            "Pre-provision validation failed — employee=%s, failures=%s",
            payload.employee_id, result.failure_summary(),
        )
        return StageResult(
            ok=False,
            outcome=StageOutcome.HELD,
            data={"event_id": event_id, "failure_step": "PreProvisionValidation"},
            hold_reasons=result.failure_summary(),
            summary=f"Pre-provision validation failed: {result.failure_summary()}",
        )

    return StageResult(
        ok=True,
        outcome=StageOutcome.PROCEED,
        data={"event_id": event_id},
        report_warnings=result.warning_summary(),
        summary="Pre-provision validation passed",
    )


def _rebuild_entitlements(access_packages: list[dict], pim_groups: list[dict]) -> EntitlementResult:
    """
    Reconstruct an EntitlementResult from the plain dicts stage_resolve_entitlements
    put in data. Only the fields provision_joiner reads are rebuilt — access_packages
    and pim_groups. Legacy .groups/.rbac_roles stay empty (ADR-007: not provisioned).
    """
    result = EntitlementResult()
    result.access_packages = [
        AccessPackageAssignment(
            rule_id=ap["rule_id"],
            access_package_id=ap["access_package_id"],
            policy_id=ap["policy_id"],
            duration_override_days=ap.get("duration_override_days"),
        )
        for ap in access_packages
    ]
    result.pim_groups = [
        PimGroup(
            group_id=pg["group_id"],
            display_name=pg["display_name"],
            eligible_role=pg["eligible_role"],
            justification=pg["justification"],
            duration_hours=pg["duration_hours"],
        )
        for pg in pim_groups
    ]
    return result


def _extract_report_actions(report: DecisionReport) -> list[dict]:
    """Flatten a report's ActionRecords back to add_action kwargs — the shim's read-back."""
    return [
        {"action": a.action, "detail": a.detail, "succeeded": a.succeeded}
        for a in report.actions_taken
    ]


def stage_provision(
    payload: IdentityPayload,
    event_id: str,
    access_packages: list[dict],
    pim_groups: list[dict] | None = None,
    graph_client: JmlGraphClient | None = None,
) -> StageResult:
    """
    Provision the identity: create the Entra user, submit and poll access
    package assignments, assign PIM eligibility. Wraps provision_joiner
    (ADR-007), which is left untouched.

    THE POLL LIVES HERE. This is the stage that, at the Durable step, splits
    into submit -> durable timer -> check. For the sync refactor it stays whole.

    Report shim: provision_joiner mutates a DecisionReport as it always has.
    This stage gives it a LOCAL throwaway report, then reads the actions back
    out as serializable report_actions — so the stage honours the no-mutation
    contract without touching provisioner.py. The driver's real report is never
    passed in here.

    Outcomes:
      PROCEED — provisioning succeeded; entra_id in data.
      FAILED  — an execution failure after the lock; failure_step/detail in data,
                report_actions still carry everything that happened before the break.

    graph_client is optional-injection: the sync driver passes a shared client;
    a Durable activity calls with none and this builds one.
    """
    if graph_client is None:
        graph_service_client, credential = build_graph_client()
        graph_client = JmlGraphClient(graph_service_client, credential)

    entitlements = _rebuild_entitlements(access_packages, pim_groups or [])

    # Local throwaway report — provision_joiner mutates this, not the driver's.
    local_report = DecisionReport(
        upn=payload.upn,
        employee_id=payload.employee_id,
        event=ReportEvent.JOINER,
    )

    provisioning_result = provision_joiner(
        payload=payload,
        entitlements=entitlements,
        report=local_report,
        graph_client=graph_client,
        event_status=EventStatus.PROCESSING,
    )

    report_actions = _extract_report_actions(local_report)
    report_warnings = list(local_report.warnings)

    if not provisioning_result.succeeded:
        return StageResult(
            ok=False,
            outcome=StageOutcome.FAILED,
            data={
                "event_id":       event_id,
                "failure_step":   provisioning_result.failure_step,
                "failure_detail": provisioning_result.failure_detail,
                "entra_id":       provisioning_result.entra_id,
            },
            report_actions=report_actions,
            report_warnings=report_warnings,
            summary=(
                f"Provisioning failed at {provisioning_result.failure_step}: "
                f"{provisioning_result.failure_detail}"
            ),
        )

    return StageResult(
        ok=True,
        outcome=StageOutcome.PROCEED,
        data={"event_id": event_id, "entra_id": provisioning_result.entra_id},
        report_actions=report_actions,
        report_warnings=report_warnings,
        summary=f"Provisioning succeeded — entra_id={provisioning_result.entra_id}",
    )

def stage_post_validate(
    entra_id: str,
    event_id: str,
    employee_id: str = "",
) -> StageResult:
    """
    Post-provision governance gate — validate the real, now-provisioned Entra
    object against expected state. Wraps post_provision_validate, which queries
    the live tenant via the object ID (this stage DOES hit Graph, unlike the
    pre-provision synthetic-snapshot gate).

    Outcomes:
      PROCEED — validation passed; warnings ride back for the driver to record.
      FAILED  — provisioning completed but the object didn't reach the expected
                state. Distinct from a provisioning error: the driver marks the
                event Failed with failure_step=PostProvisionValidation.

    No client injection — post_provision_validate builds its own HTTP call to the
    validation engine (or returns the skipped result when the engine is skipped).
    """
    result = post_provision_validate(
        entra_object_id=entra_id,
        employee_id=employee_id,
    )

    if not result.passed:
        logger.warning(
            "Post-provision validation failed — object_id=%s, failures=%s",
            entra_id, result.failure_summary(),
        )
        return StageResult(
            ok=False,
            outcome=StageOutcome.FAILED,
            data={"event_id": event_id, "failure_step": "PostProvisionValidation"},
            report_actions=[{
                "action": "PostProvisionValidationFailed",
                "detail": f"failures={result.failure_summary()}",
                "succeeded": False,
            }],
            summary=f"Post-provision validation failed: {result.failure_summary()}",
        )

    return StageResult(
        ok=True,
        outcome=StageOutcome.PROCEED,
        data={"event_id": event_id},
        report_warnings=result.warning_summary(),
        summary="Post-provision validation passed",
    )




def stage_finalize(
    payload: IdentityPayload,
    event_id: str,
    final_status: str,
    failure_step: str = "",
    events_client=None,
    connection_string: str = "",
) -> StageResult:
    """
    Terminal bookkeeping for the event: release the lock, set the event's
    final status, and advance the conflict queue.

    Called on every terminal path — success OR post-lock failure — so the lock
    is always released and the next queued event always gets its chance. This is
    the counterpart to stage_claim_event: claim acquires the lock, finalize
    releases it.

    final_status is EventStatus.COMPLETED or EventStatus.FAILED. On COMPLETED the
    next queued event is auto-released; on FAILED it is held for review (that
    predecessor-failed logic lives in release_next_queued_event).

    Always PROCEED — finalize is bookkeeping, not a gate. Its report_actions
    note what the queue did. events_client is optional-injection.
    """
    if events_client is None:
        events_client = get_events_table_client(connection_string)

    employee_id = payload.employee_id

    release_lock(events_client, employee_id, event_id)
    update_event_status(
        table_client=events_client,
        employee_id=employee_id,
        event_id=event_id,
        status=final_status,
        failure_step=failure_step,
    )

    next_event = release_next_queued_event(
        table_client=events_client,
        employee_id=employee_id,
        predecessor_status=final_status,
    )

    report_actions = []
    if next_event:
        if final_status == EventStatus.COMPLETED:
            detail = f"Next queued event auto-released — event_id={next_event.event_id}"
        else:
            detail = (
                f"Next queued event held for review — event_id={next_event.event_id} "
                f"— predecessor failed at {failure_step}"
            )
        report_actions.append({
            "action": "QueueAdvanced",
            "detail": detail,
            "succeeded": True,
        })

    return StageResult(
        ok=True,
        outcome=StageOutcome.PROCEED,
        data={"event_id": event_id, "final_status": final_status},
        report_actions=report_actions,
        summary=f"Finalized — status={final_status}",
    )