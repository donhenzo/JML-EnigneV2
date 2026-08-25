import os
import uuid
from dataclasses import asdict

from Ingestion.schema import IdentityPayload, EmploymentType, JmlAction
from Normalization.lookup_loader import load_lookup_table
from Normalization.normalizer import Normalizer
from Functions.Event_store.event_store import (
    get_events_table_client, update_event_status, acquire_lock, EventStatus,
)
from Hold_queue.queue_manager import HoldQueueManager
from Hold_queue.azure_table_hold_queue_store import (
    AzureTableHoldQueueStore, get_hold_queue_table_client,
)
from Provisioning.graph_client import build_graph_client, JmlGraphClient
from Provisioning.provisioner import (
    ProvisioningResult, PendingPackage,
    check_or_create_user, submit_access_packages,
    poll_access_packages_once, packages_all_terminal,
    record_access_package_results,
)
from Mapping.mapping_resolver import AccessPackageAssignment
from Audit.models import DecisionReport, ReportEvent
from Joiner.stages import (
    stage_claim_event, stage_conflict_check, stage_resolve_entitlements,
    stage_pre_validate, stage_post_validate, stage_finalize,
)
from Joiner.stage_result import StageOutcome


def _payload_from_dict(raw: dict) -> IdentityPayload:
    from datetime import date
    r = dict(raw)
    if isinstance(r.get("start_date"), str):
        r["start_date"] = date.fromisoformat(r["start_date"])
    if isinstance(r.get("employment_type"), str):
        r["employment_type"] = EmploymentType(r["employment_type"])
    if isinstance(r.get("action"), str):
        r["action"] = JmlAction(r["action"])
    r.pop("synthetic_id", None)
    return IdentityPayload(**r)


def _events_client():
    return get_events_table_client(os.environ.get("JML_STORAGE_CONNECTION_STRING", ""))


def _fresh_report(payload: IdentityPayload) -> DecisionReport:
    return DecisionReport(upn=payload.upn, employee_id=payload.employee_id, event=ReportEvent.JOINER)


# Activity 1: pre-provision (normalize .. pre_validate) 
def pre_provision(payload_dict: dict) -> dict:
    conn = os.environ.get("JML_STORAGE_CONNECTION_STRING", "")
    events_client = get_events_table_client(conn)
    hold_queue = HoldQueueManager(AzureTableHoldQueueStore(get_hold_queue_table_client(conn)))
    payload = _payload_from_dict(payload_dict)

    normalizer = Normalizer(load_lookup_table(
        os.environ.get("LOCAL_LOOKUP_PATH", "config/canonical_lookup.json")))
    norm = normalizer.normalize(payload)
    if not norm.passed:
        hold_queue.create_from_normalization_failure(payload=norm.payload, reasons=norm.failures)
        return {"final_status": "HELD", "employee_id": payload.employee_id,
                "summary": f"Normalization failed: {norm.failures}"}
    normalised = norm.payload

    claim = stage_claim_event(normalised, correlation_id="", events_client=events_client)
    event_id = claim.data["event_id"]
    if claim.outcome == StageOutcome.DUPLICATE:
        return {"final_status": "DUPLICATE", "employee_id": payload.employee_id, "event_id": event_id,
                "summary": "Duplicate event — already claimed in JmlEvents."}
    if claim.outcome == StageOutcome.SKIPPED:
        return {"final_status": "SKIPPED", "employee_id": payload.employee_id, "event_id": event_id,
                "summary": "Active event already processing this employee."}

    conflict = stage_conflict_check(normalised, event_id=event_id, events_client=events_client)
    if conflict.outcome == StageOutcome.QUEUED:
        return {"final_status": "QUEUED", "employee_id": payload.employee_id, "event_id": event_id,
                "summary": "Event queued — another event in progress."}

    resolve = stage_resolve_entitlements(normalised, event_id=event_id)

    pre = stage_pre_validate(normalised, event_id=event_id)
    if pre.outcome == StageOutcome.HELD:
        hold_queue.create_from_validation_failure(payload=normalised, reasons=pre.hold_reasons)
        update_event_status(table_client=events_client, employee_id=payload.employee_id,
                            event_id=event_id, status=EventStatus.FAILED, failure_step="PreProvisionValidation")
        return {"final_status": "HELD", "employee_id": payload.employee_id, "event_id": event_id,
                "summary": f"Pre-provision validation failed: {pre.hold_reasons}"}

    return {
        "final_status": "PROCEED",
        "employee_id": payload.employee_id,
        "event_id": event_id,
        "normalised_payload": normalised.to_dict(),
        "access_packages": resolve.data["access_packages"],
        "pim_groups": resolve.data["pim_groups"],
    }


# Activity 2: create user (acquire lock + create/find) 
def create_user(pre_result: dict) -> dict:
    events_client = _events_client()
    payload = _payload_from_dict(pre_result["normalised_payload"])
    event_id = pre_result["event_id"]

    acquire_lock(events_client, payload.employee_id, event_id, str(uuid.uuid4()))

    gsc, cred = build_graph_client()
    graph_client = JmlGraphClient(gsc, cred)
    report = _fresh_report(payload)
    result = ProvisioningResult()

    entra_id = check_or_create_user(payload, report, graph_client, EventStatus.PROCESSING, result)
    if not entra_id:
        return {**pre_result, "final_status": "FAILED",
                "failure_step": result.failure_step or "UserCreation",
                "failure_detail": result.failure_detail}

    return {**pre_result, "final_status": "PROCEED", "entra_id": entra_id}


# Activity 3: submit packages 
def submit_packages(state: dict) -> dict:
    payload = _payload_from_dict(state["normalised_payload"])
    gsc, cred = build_graph_client()
    graph_client = JmlGraphClient(gsc, cred)
    report = _fresh_report(payload)
    result = ProvisioningResult()

    access_packages = [
        AccessPackageAssignment(
            rule_id=ap["rule_id"], access_package_id=ap["access_package_id"],
            policy_id=ap["policy_id"], duration_override_days=ap.get("duration_override_days"),
        )
        for ap in state["access_packages"]
    ]

    if not access_packages:
        return {**state, "final_status": "PROCEED", "pending": []}

    pending = submit_access_packages(state["entra_id"], access_packages, report, graph_client, result)
    if pending is None:
        return {**state, "final_status": "FAILED",
                "failure_step": result.failure_step, "failure_detail": result.failure_detail,
                "pending": []}

    return {**state, "final_status": "PROCEED", "pending": [asdict(p) for p in pending]}


#  Activity 4: one poll pass 
def check_packages(state: dict) -> dict:
    gsc, cred = build_graph_client()
    graph_client = JmlGraphClient(gsc, cred)

    pending = [PendingPackage(**d) for d in state["pending"]]
    poll_access_packages_once(pending, graph_client)
    all_terminal = packages_all_terminal(pending)

    return {**state, "pending": [asdict(p) for p in pending], "all_terminal": all_terminal}


#  Activity 5: record results + post_validate + finalize 
def record_and_finalize(state: dict) -> dict:
    events_client = _events_client()
    payload = _payload_from_dict(state["normalised_payload"])
    event_id = state["event_id"]

    # If a prior activity already failed (create/submit), finalize FAILED.
    if state.get("final_status") == "FAILED":
        stage_finalize(payload, event_id=event_id, final_status=EventStatus.FAILED,
                       failure_step=state.get("failure_step", ""), events_client=events_client)
        return {"final_status": "FAILED", "employee_id": payload.employee_id, "event_id": event_id,
                "summary": f"Provisioning failed at {state.get('failure_step')}"}

    report = _fresh_report(payload)
    result = ProvisioningResult()
    pending = [PendingPackage(**d) for d in state.get("pending", [])]

    packages_ok = record_access_package_results(pending, report, result) if pending else True

    if not packages_ok:
        stage_finalize(payload, event_id=event_id, final_status=EventStatus.FAILED,
                       failure_step=result.failure_step, events_client=events_client)
        return {"final_status": "FAILED", "employee_id": payload.employee_id, "event_id": event_id,
                "summary": f"Package assignment failed: {result.failure_detail}"}

    entra_id = state["entra_id"]
    post = stage_post_validate(entra_id=entra_id, event_id=event_id, employee_id=payload.employee_id)
    if post.outcome == StageOutcome.FAILED:
        stage_finalize(payload, event_id=event_id, final_status=EventStatus.FAILED,
                       failure_step="PostProvisionValidation", events_client=events_client)
        return {"final_status": "FAILED", "employee_id": payload.employee_id, "event_id": event_id,
                "summary": f"Post-provision validation failed: {post.summary}"}

    stage_finalize(payload, event_id=event_id, final_status=EventStatus.COMPLETED, events_client=events_client)
    return {
        "final_status": "COMPLETED",
        "employee_id": payload.employee_id,
        "event_id": event_id,
        "entra_id": entra_id,
        "summary": f"Joiner provisioning succeeded — {len(state.get('access_packages', []))} package(s) assigned.",
    }