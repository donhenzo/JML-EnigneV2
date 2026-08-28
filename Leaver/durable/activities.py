"""
Leaver/durable/activities.py

Durable activity functions for the Leaver pipeline. Each activity is a thin
wrapper: it builds its own clients (activities can't receive live clients —
they run in separate invocations), calls the already-proven stages
(Leaver/stages.py) and provisioning phases (Leaver/provisioning_phases.py), and
returns plain serializable dicts that cross the orchestrator boundary as JSON.

The orchestrator owns all flow and all waits. These activities never sleep and
never loop. The Leaver is simpler than the Mover: one removal loop, no add loop,
no ADR-009 gate. The disable/revoke fail-safe (ADR-015) runs inside pre, before
the removal loop, so a partial failure downstream still leaves the account
locked out.

Behavior is identical to the synchronous run_leaver_pipeline — this is the same
composition, wait-hoisted into the orchestrator.
"""

import json
import os

from azure.data.tables import TableServiceClient

from Ingestion.schema import IdentityPayload, EmploymentType, JmlAction
from Provisioning.graph_client import build_graph_client, JmlGraphClient
from Functions.Event_store.event_store import (
    get_events_table_client, generate_event_id,
    release_lock, update_event_status, EventStatus,
)
from Leaver.stage_result import StageOutcome
from Leaver.stages import (
    stage_claim, stage_conflict_check, stage_concurrent_check,
    stage_fetch_current_state, stage_disable, stage_revoke,
    stage_pim_terminate, stage_soft_delete, stage_verify,
)
from Leaver.provisioning_phases import (
    PendingPackage,
    submit_removals, poll_packages_once, packages_all_terminal,
    finalize_removals,
)


LEAVER_EVENT_LOG_TABLE = "LeaverEventLog"
LEAVER_AUDIT_LOG_TABLE = "LeaverAuditLog"


# Helpers — same construction pattern as the Mover durable activities.

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


def _table_client() -> TableServiceClient:
    return TableServiceClient.from_connection_string(
        os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    )


def _events_client() -> TableServiceClient:
    return get_events_table_client(
        os.environ.get("JML_STORAGE_CONNECTION_STRING", "")
    )


def _graph_client() -> JmlGraphClient:
    gsc, cred = build_graph_client()
    return JmlGraphClient(gsc, cred)


def _write_event_log(table_client, employee_id, event_id, status):
    from datetime import datetime, timezone
    try:
        client = table_client.get_table_client(LEAVER_EVENT_LOG_TABLE)
        entity = {
            "PartitionKey": employee_id,
            "RowKey":       event_id,
            "status":       status,
            "updated_at":   datetime.now(timezone.utc).isoformat(),
            "payload":      "",
        }
        client.upsert_entity(entity)
    except Exception:
        pass


def _write_audit_record(table_client, employee_id, event_id, audit_record):
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
    except Exception:
        pass


def _new_audit_record(employee_id, event_id):
    from datetime import datetime, timezone
    return {
        "event_type":      "LEAVER",
        "employee_id":     employee_id,
        "event_id":        event_id,
        "source":          "BAMBOOHR",
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "actions_taken":   [],
        "warnings":        [],
        "offboard_status": "RECEIVED",
    }


def _apply_reports(audit_record, result):
    if result.report_actions:
        audit_record["actions_taken"].extend(result.report_actions)
    if result.report_warnings:
        audit_record["warnings"].extend(result.report_warnings)


# Activity: pre — claim -> conflict supersede -> concurrent -> fetch+lock ->
# disable -> revoke. All the zero-wait, pre-removal work. Disable and revoke
# (ADR-015) run here, before the removal loop, so a downstream failure still
# fails safe. Short-circuits on duplicate / concurrent / fetch-failure.

def pre(state: dict) -> dict:
    payload = _payload_from_dict(state)
    employee_id = payload.employee_id
    event_id = generate_event_id(employee_id, "Leaver", payload.start_date.isoformat())

    table_client      = _table_client()
    jml_events_client = _events_client()
    graph_client      = _graph_client()
    audit_record      = _new_audit_record(employee_id, event_id)

    # claim
    claim = stage_claim(payload, event_id, jml_events_client)
    if claim.outcome == StageOutcome.DUPLICATE:
        return {"final_status": "QUEUED_CONCURRENT", "employee_id": employee_id,
                "event_id": event_id,
                "summary": "Duplicate event — already claimed in JmlEvents."}

    # conflict supersede (Leaver supersedes pending Joiner/Mover, then proceeds)
    stage_conflict_check(payload, event_id, jml_events_client)

    # concurrent
    concurrent = stage_concurrent_check(payload, table_client)
    if concurrent.outcome == StageOutcome.QUEUED:
        _write_event_log(table_client, employee_id, event_id, "QUEUED_CONCURRENT")
        return {"final_status": "QUEUED_CONCURRENT", "employee_id": employee_id,
                "event_id": event_id,
                "summary": "Event queued — another Leaver event is in progress for this employee."}

    _write_event_log(table_client, employee_id, event_id, "IN_PROGRESS")

    # fetch + lock
    fetch = stage_fetch_current_state(payload, event_id, graph_client, jml_events_client)
    if fetch.outcome == StageOutcome.FAILED:
        _apply_reports(audit_record, fetch)
        return _early_fail(table_client, jml_events_client, employee_id, event_id,
                           audit_record, fetch.summary,
                           fetch.data["failure_step"],
                           fetch.data.get("lock_acquired", False))

    user_id = fetch.data["user_id"]
    audit_record["packages_at_offboard_start"] = list(fetch.data["current_packages"])

    # disable (ADR-015) — first mutation, before any removal
    disable = stage_disable(payload, user_id, graph_client)
    _apply_reports(audit_record, disable)

    # revoke (ADR-015)
    revoke = stage_revoke(payload, user_id, graph_client)
    _apply_reports(audit_record, revoke)

    return {
        "final_status":       "PROCEED",
        "employee_id":        employee_id,
        "event_id":           event_id,
        "payload_dict":       state,
        "user_id":            user_id,
        "current_packages":   fetch.data["current_packages"],
        "current_policy_map": fetch.data["current_policy_map"],
        "package_labels":     fetch.data["package_labels"],
        "audit_record":       audit_record,
    }


# Activity: submit — submit adminRemove for every current package. No op param;
# the Leaver only ever removes.

def submit(state: dict) -> dict:
    graph_client = _graph_client()
    pending = submit_removals(
        graph_client, state["user_id"],
        frozenset(state["current_packages"]),
        state["current_policy_map"], state["package_labels"],
    )
    return {**state, "pending": [p.to_dict() for p in pending]}


# Activity: check — one poll pass over the current pending set.

def check(state: dict) -> dict:
    graph_client = _graph_client()
    pending = [PendingPackage.from_dict(d) for d in state["pending"]]
    poll_packages_once(pending, graph_client)
    all_terminal = packages_all_terminal(pending)
    return {**state, "pending": [p.to_dict() for p in pending], "all_terminal": all_terminal}


# Activity: finalize — finalize the removal set (fallback-confirm, write audit
# actions). No gate, no attribute PATCH — the Leaver removes everything and
# patches nothing.

def finalize(state: dict) -> dict:
    graph_client = _graph_client()
    audit_record = state["audit_record"]
    pending = [PendingPackage.from_dict(d) for d in state["pending"]]

    actions = finalize_removals(graph_client, state["user_id"], pending)
    audit_record["actions_taken"].extend(actions)
    audit_record["packages_removed"] = [
        {"id": a["package_id"], "reason": "LEAVER_OFFBOARDING"}
        for a in actions if a["succeeded"]
    ]
    packages_removal_failed = [a["package_id"] for a in actions if not a["succeeded"]]
    if packages_removal_failed:
        audit_record["warnings"].append(
            f"{len(packages_removal_failed)} package(s) did not confirm "
            f"removal: {packages_removal_failed}"
        )

    return {**state, "audit_record": audit_record,
            "packages_removal_failed": packages_removal_failed}


# Activity: verify_finalize — PIM termination + soft delete + verification +
# audit write + lock release + JmlEvents terminal. None of these poll, so they
# fold into one terminal activity. The orchestrator's terminal step.

def verify_finalize(state: dict) -> dict:
    payload = _payload_from_dict(state["payload_dict"])
    employee_id = payload.employee_id
    event_id = state["event_id"]
    user_id = state["user_id"]
    audit_record = state["audit_record"]
    packages_removal_failed = state.get("packages_removal_failed", [])

    table_client      = _table_client()
    jml_events_client = _events_client()
    graph_client      = _graph_client()

    # PIM termination (best-effort, ADR-016)
    pim = stage_pim_terminate(payload, user_id, graph_client)
    _apply_reports(audit_record, pim)

    # soft delete (hold branch, ADR-015)
    soft_delete = stage_soft_delete(payload, user_id, graph_client)
    _apply_reports(audit_record, soft_delete)
    user_deleted = soft_delete.data.get("user_deleted", False)

    # verification
    verify = stage_verify(
        payload, user_id, user_deleted, packages_removal_failed, graph_client,
    )
    _apply_reports(audit_record, verify)
    audit_record["post_offboard_verification"] = verify.data["audit_post_offboard_verification"]

    verification_error         = verify.data["verification_error"]
    account_disabled_confirmed = verify.data["account_disabled_confirmed"]
    packages_cleared           = verify.data["packages_cleared"]

    if verification_error:
        final_status = "OFFBOARD_FAILED"
    elif packages_cleared and (account_disabled_confirmed or user_deleted):
        final_status = "OFFBOARD_SUCCESS"
    else:
        final_status = "OFFBOARD_PARTIAL"

    audit_record["offboard_status"] = final_status
    _write_event_log(table_client, employee_id, event_id, final_status)
    _write_audit_record(table_client, employee_id, event_id, audit_record)

    jml_final = EventStatus.COMPLETED if final_status == "OFFBOARD_SUCCESS" else EventStatus.FAILED
    release_lock(jml_events_client, employee_id, event_id)
    update_event_status(
        table_client=jml_events_client, employee_id=employee_id, event_id=event_id,
        status=jml_final,
        failure_step="PostOffboardVerification" if final_status == "OFFBOARD_PARTIAL" else "",
    )

    return {"final_status": final_status, "employee_id": employee_id,
            "event_id": event_id,
            "summary": f"Leaver event completed with status {final_status}."}


def _early_fail(table_client, jml_events_client, employee_id, event_id,
                audit_record, reason, failure_step, lock_acquired):
    audit_record["offboard_status"] = "OFFBOARD_FAILED"
    audit_record["warnings"].append(reason)
    _write_event_log(table_client, employee_id, event_id, "OFFBOARD_FAILED")
    _write_audit_record(table_client, employee_id, event_id, audit_record)
    if lock_acquired:
        release_lock(jml_events_client, employee_id, event_id)
    update_event_status(
        table_client=jml_events_client, employee_id=employee_id, event_id=event_id,
        status=EventStatus.FAILED, failure_step=failure_step,
    )
    return {"final_status": "OFFBOARD_FAILED", "employee_id": employee_id,
            "event_id": event_id, "summary": reason}