import os

from Ingestion.schema import IdentityPayload, EmploymentType, JmlAction
from Normalization.lookup_loader import load_lookup_table
from Normalization.normalizer import Normalizer
from Functions.Event_store.event_store import (
    get_events_table_client, update_event_status, EventStatus,
)
from Hold_queue.queue_manager import HoldQueueManager
from Hold_queue.azure_table_hold_queue_store import (
    AzureTableHoldQueueStore, get_hold_queue_table_client,
)
from Provisioning.graph_client import build_graph_client, JmlGraphClient
from Joiner.stages import (
    stage_claim_event, stage_conflict_check, stage_resolve_entitlements,
    stage_pre_validate, stage_provision, stage_post_validate, stage_finalize,
)
from Joiner.stage_result import StageOutcome


def _payload_from_dict(raw: dict) -> IdentityPayload:
    """Reconstruct IdentityPayload from a start-request or threaded dict."""
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


def pre_provision(payload_dict: dict) -> dict:
    """normalize -> claim -> conflict -> resolve -> pre_validate."""
    conn = os.environ.get("JML_STORAGE_CONNECTION_STRING", "")
    events_client = get_events_table_client(conn)
    hold_queue = HoldQueueManager(AzureTableHoldQueueStore(get_hold_queue_table_client(conn)))

    payload = _payload_from_dict(payload_dict)
    report_actions, report_warnings = [], []

    normalizer = Normalizer(load_lookup_table(
        os.environ.get("LOCAL_LOOKUP_PATH", "config/canonical_lookup.json")))
    norm = normalizer.normalize(payload)
    if not norm.passed:
        hold_queue.create_from_normalization_failure(payload=norm.payload, reasons=norm.failures)
        return {"final_status": "HELD", "employee_id": payload.employee_id,
                "summary": f"Normalization failed: {norm.failures}"}
    normalised = norm.payload
    report_actions.append({"action": "NormalizationPassed",
                           "detail": f"department={normalised.department}, job_title={normalised.job_title}",
                           "succeeded": True})

    def collect(r):
        report_actions.extend(r.report_actions)
        report_warnings.extend(r.report_warnings)

    claim = stage_claim_event(normalised, correlation_id="", events_client=events_client)
    collect(claim)
    event_id = claim.data["event_id"]
    if claim.outcome == StageOutcome.DUPLICATE:
        return {"final_status": "DUPLICATE", "employee_id": payload.employee_id, "event_id": event_id,
                "summary": "Duplicate event — already claimed in JmlEvents."}
    if claim.outcome == StageOutcome.SKIPPED:
        return {"final_status": "SKIPPED", "employee_id": payload.employee_id, "event_id": event_id,
                "summary": "Active event already processing this employee."}

    conflict = stage_conflict_check(normalised, event_id=event_id, events_client=events_client)
    collect(conflict)
    if conflict.outcome == StageOutcome.QUEUED:
        return {"final_status": "QUEUED", "employee_id": payload.employee_id, "event_id": event_id,
                "summary": "Event queued — another event in progress."}

    resolve = stage_resolve_entitlements(normalised, event_id=event_id)
    collect(resolve)

    pre = stage_pre_validate(normalised, event_id=event_id)
    collect(pre)
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
        "report_actions": report_actions,
        "report_warnings": report_warnings,
    }


def provision(pre_result: dict) -> dict:
    """Acquire lock + provision (whole stage_provision, poll included — step 1)."""
    import uuid
    from Functions.Event_store.event_store import acquire_lock

    conn = os.environ.get("JML_STORAGE_CONNECTION_STRING", "")
    events_client = get_events_table_client(conn)
    normalised = _payload_from_dict(pre_result["normalised_payload"])
    event_id = pre_result["event_id"]

    gsc, cred = build_graph_client()
    graph_client = JmlGraphClient(gsc, cred)

    acquire_lock(events_client, normalised.employee_id, event_id, str(uuid.uuid4()))

    result = stage_provision(
        normalised, event_id=event_id,
        access_packages=pre_result["access_packages"],
        pim_groups=pre_result["pim_groups"],
        graph_client=graph_client,
    )

    out = {
        "final_status": "FAILED" if result.outcome == StageOutcome.FAILED else "PROCEED",
        "event_id": event_id,
        "entra_id": result.data.get("entra_id"),
        "failure_step": result.data.get("failure_step", ""),
        "provision_report_actions": result.report_actions,
        "provision_report_warnings": result.report_warnings,
    }
    return out


def finalize(combined: dict) -> dict:
    """post_validate (if provisioned) + finalize (release lock, status, queue)."""
    conn = os.environ.get("JML_STORAGE_CONNECTION_STRING", "")
    events_client = get_events_table_client(conn)
    normalised = _payload_from_dict(combined["normalised_payload"])
    event_id = combined["event_id"]

    if combined.get("final_status") == "FAILED":
        fin = stage_finalize(normalised, event_id=event_id,
                             final_status=EventStatus.FAILED,
                             failure_step=combined.get("failure_step", ""),
                             events_client=events_client)
        return {"final_status": "FAILED", "employee_id": normalised.employee_id, "event_id": event_id,
                "summary": f"Provisioning failed at {combined.get('failure_step')}"}

    entra_id = combined["entra_id"]
    post = stage_post_validate(entra_id=entra_id, event_id=event_id, employee_id=normalised.employee_id)
    if post.outcome == StageOutcome.FAILED:
        stage_finalize(normalised, event_id=event_id, final_status=EventStatus.FAILED,
                       failure_step="PostProvisionValidation", events_client=events_client)
        return {"final_status": "FAILED", "employee_id": normalised.employee_id, "event_id": event_id,
                "summary": f"Post-provision validation failed: {post.summary}"}

    stage_finalize(normalised, event_id=event_id, final_status=EventStatus.COMPLETED, events_client=events_client)
    return {
        "final_status": "COMPLETED",
        "employee_id": normalised.employee_id,
        "event_id": event_id,
        "entra_id": entra_id,
        "summary": f"Joiner provisioning succeeded — {len(combined.get('access_packages', []))} package(s) assigned.",
    }