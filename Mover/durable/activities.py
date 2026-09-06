"""
Mover/durable/activities.py

Durable activity functions for the Mover pipeline. Each activity is a thin
wrapper: it builds its own clients (activities can't receive live clients —
they run in separate invocations), calls the already-proven stages
(Mover/stages.py) and provisioning phases (Mover/provisioning_phases.py), and
returns plain serializable dicts that cross the orchestrator boundary as JSON.

The orchestrator owns all flow and all waits. These activities never sleep,
never loop, and never decide ordering — the submit/check/finalize triad is
generic over an operation ("add" or "remove") so the orchestrator alone
decides the sequence. That is what keeps ADR-009 (add-before-remove) a flow
decision in the orchestrator, and what would let ADR-011 Strategy B
(remove-before-add) become a future orchestrator branch with no change here.

Behavior is identical to the synchronous run_mover_pipeline — this is the
same composition, wait-hoisted into the orchestrator.
"""

import json
import os

from azure.data.tables import TableServiceClient

from Ingestion.schema import IdentityPayload, EmploymentType, JmlAction
from Provisioning.graph_client import build_graph_client, JmlGraphClient
from Functions.Event_store.event_store import (
    get_events_table_client, release_lock, update_event_status, EventStatus,
)
from Mover.stage_result import StageOutcome
from Mover.stages import (
    stage_claim, stage_concurrent_check, stage_fetch_current_state,
    stage_resolve, stage_delta, stage_retention, stage_preflight_sod,
    stage_verify,
)
from Hold_queue.queue_manager import HoldQueueManager, InMemoryHoldQueueStore
from Mover.provisioning_phases import (
    PendingPackage,
    submit_additions, submit_removals,
    poll_packages_once, packages_all_terminal,
    finalize_additions, finalize_removals,
    apply_attribute_update,
)


MOVER_EVENT_LOG_TABLE = "MoverEventLog"
MOVER_AUDIT_LOG_TABLE = "MoverAuditLog"


# Helpers — same construction pattern as the Joiner durable activities.

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


def _write_event_log(table_client, employee_id, event_id, status,
                     retention_applied=None):
    from datetime import datetime, timezone
    try:
        client = table_client.get_table_client(MOVER_EVENT_LOG_TABLE)
        entity = {
            "PartitionKey": employee_id,
            "RowKey":       event_id,
            "status":       status,
            "updated_at":   datetime.now(timezone.utc).isoformat(),
            "payload":      "",
        }
        if retention_applied is not None:
            entity["retention_applied"] = retention_applied
        client.upsert_entity(entity)
    except Exception:
        pass


def _write_audit_record(table_client, employee_id, event_id, audit_record):
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
    except Exception:
        pass


def _new_audit_record(employee_id, event_id):
    from datetime import datetime, timezone
    return {
        "event_type":       "MOVE",
        "employee_id":      employee_id,
        "event_id":         event_id,
        "source":           "BAMBOOHR",
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "actions_taken":    [],
        "warnings":         [],
        "post_move_status": "RECEIVED",
    }


# Activity: pre — claim -> concurrent -> fetch+lock -> resolve -> delta -> retention.
# All the zero-wait stages. Short-circuits on duplicate/concurrent/fetch-failure.

def pre(state: dict) -> dict:
    from Functions.Event_store.event_store import generate_event_id

    payload = _payload_from_dict(state)
    employee_id = payload.employee_id
    event_id = generate_event_id(employee_id, "Mover", payload.start_date.isoformat())

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

    # concurrent
    concurrent = stage_concurrent_check(payload, table_client)
    if concurrent.outcome == StageOutcome.QUEUED:
        _write_event_log(table_client, employee_id, event_id, "QUEUED_CONCURRENT")
        return {"final_status": "QUEUED_CONCURRENT", "employee_id": employee_id,
                "event_id": event_id,
                "summary": "Event queued — another Mover event is in progress for this employee."}

    _write_event_log(table_client, employee_id, event_id, "IN_PROGRESS")

    # fetch + lock
    fetch = stage_fetch_current_state(payload, event_id, graph_client, jml_events_client)
    if fetch.outcome == StageOutcome.FAILED:
        for w in fetch.report_warnings:
            audit_record["warnings"].append(w)
        return _early_fail(table_client, jml_events_client, employee_id, event_id,
                           audit_record, fetch.summary,
                           fetch.data["failure_step"],
                           fetch.data.get("lock_acquired", False))

    # resolve
    resolve = stage_resolve(
        payload, fetch.data["current_department"], fetch.data["current_job_title"],
        fetch.data["package_labels"], graph_client,
    )
    if resolve.outcome == StageOutcome.FAILED:
        for w in resolve.report_warnings:
            audit_record["warnings"].append(w)
        return _early_fail(table_client, jml_events_client, employee_id, event_id,
                           audit_record, resolve.summary,
                           resolve.data["failure_step"],
                           resolve.data.get("lock_acquired", True))

    # delta
    delta = stage_delta(
        payload, fetch.data["current_department"], fetch.data["current_job_title"],
        fetch.data["current_packages"], resolve.data["target_packages"],
        resolve.data["managed_catalogue"], fetch.data["current_attributes"],
        resolve.data["incoming_attributes"],
    )
    audit_record["from_department"]    = delta.data["audit_from_department"]
    audit_record["to_department"]      = delta.data["audit_to_department"]
    audit_record["from_title"]         = delta.data["audit_from_title"]
    audit_record["to_title"]           = delta.data["audit_to_title"]
    audit_record["attribute_changes"]  = delta.data["audit_attribute_changes"]
    audit_record["unmanaged_packages"] = delta.data["audit_unmanaged_packages"]

    # retention
    retention = stage_retention(payload, delta.data["groups_to_remove"], table_client)
    audit_record["packages_retained"]   = retention.data["audit_packages_retained"]
    audit_record["retention_decisions"] = retention.data["audit_retention_decisions"]
    audit_record["retention_summary"]   = retention.data["audit_retention_summary"]

    # Step 5 — pre-flight SoD (ADR-020)
    keep_set = (
        set(delta.data["unchanged"])
        | set(retention.data["retain_set"])
        | set(delta.data["unmanaged"])
    )
    preflight = stage_preflight_sod(
        delta.data["groups_to_add"],
        retention.data["remove_confirmed"],
        keep_set,
        graph_client,
        resolve.data["package_labels"],
    )
    audit_record["sod_evaluation"] = preflight.data.get("decisions", {})

    if preflight.outcome == StageOutcome.HELD:
        audit_record["warnings"].extend(preflight.hold_reasons)
        audit_record["sod_escalations"] = preflight.data["blocked_packages"]

        hold_manager = HoldQueueManager(InMemoryHoldQueueStore())
        hold_manager.create_from_sod_block(payload, preflight.data["blocked_packages"])

        _write_event_log(table_client, employee_id, event_id, "SOD_HELD")
        _write_audit_record(table_client, employee_id, event_id, audit_record)
        release_lock(jml_events_client, employee_id, event_id)
        update_event_status(
            table_client=jml_events_client, employee_id=employee_id,
            event_id=event_id, status=EventStatus.FAILED,
            failure_step="PreFlightSoD",
        )
        return {
            "final_status": "SOD_HELD", "employee_id": employee_id,
            "event_id": event_id, "summary": preflight.summary,
        }

    audit_record["sod_escalations"] = []

    return {
        "final_status":       "PROCEED",
        "employee_id":        employee_id,
        "event_id":           event_id,
        "payload_dict":       state,
        "user_id":            fetch.data["user_id"],
        "package_labels":     resolve.data["package_labels"],
        "new_policy_map":     resolve.data["new_policy_map"],
        "current_policy_map": fetch.data["current_policy_map"],
        "patch_dict":         delta.data["patch_dict"],
        "groups_to_add":      delta.data["groups_to_add"],
        "unchanged":          delta.data["unchanged"],
        "unmanaged":          delta.data["unmanaged"],
        "remove_confirmed":   retention.data["remove_confirmed"],
        "retain_set":         retention.data["retain_set"],
        "retention_applied":  retention.data["retention_applied"],
        "reorder":            preflight.data.get("reorder", False),
        "audit_record":       audit_record,
    }


# Activity: submit — generic over op ("add" | "remove").
# The orchestrator sets state["op"] and state["packages"] before calling.

def submit(state: dict) -> dict:
    op = state["op"]
    graph_client = _graph_client()
    user_id = state["user_id"]

    if op == "add":
        pending = submit_additions(
            graph_client, user_id, frozenset(state["groups_to_add"]),
            state["new_policy_map"], state["package_labels"],
        )
    else:  # "remove"
        pending = submit_removals(
            graph_client, user_id, frozenset(state["remove_confirmed"]),
            state["current_policy_map"], state["package_labels"],
        )

    return {**state, "pending": [p.to_dict() for p in pending]}


# Activity: check — one poll pass over the current pending set. Generic over op.

def check(state: dict) -> dict:
    graph_client = _graph_client()
    pending = [PendingPackage.from_dict(d) for d in state["pending"]]
    poll_packages_once(pending, graph_client)
    all_terminal = packages_all_terminal(pending)
    return {**state, "pending": [p.to_dict() for p in pending], "all_terminal": all_terminal}


# Activity: finalize_op — finalize the current pending set. Generic over op.
# For "add" it also returns additions_all_succeeded (the ADR-009 gate input).
# For "remove" it also applies the attribute PATCH (same in-gate transaction
# as the sync driver's Step 7).

def finalize_op(state: dict) -> dict:
    op = state["op"]
    graph_client = _graph_client()
    user_id = state["user_id"]
    audit_record = state["audit_record"]
    pending = [PendingPackage.from_dict(d) for d in state["pending"]]

    if op == "add":
        actions, delivered, all_succeeded = finalize_additions(
            graph_client, user_id, pending, frozenset(state["groups_to_add"]),
        )
        audit_record["actions_taken"].extend(actions)
        audit_record["packages_added"] = [
            {"id": a["package_id"]} for a in actions if a["succeeded"]
        ]
        if not state["groups_to_add"]:
            pass
        elif not all_succeeded:
            audit_record["warnings"].append(
                "One or more package additions did not deliver — removals skipped "
                "this pass per ADR-009 (never remove old access before new access "
                "is confirmed). See actions_taken for which package(s) failed."
            )
        return {**state, "audit_record": audit_record,
                "additions_all_succeeded": all_succeeded}

    # op == "remove"
    actions = finalize_removals(
        graph_client, user_id, pending, frozenset(state["remove_confirmed"]),
    )
    audit_record["actions_taken"].extend(actions)
    audit_record["packages_removed"] = [
        {"id": a["package_id"], "reason": "ROLE_CHANGE"}
        for a in actions if a["succeeded"]
    ]
    recently_removed = [a["package_id"] for a in actions if a["succeeded"]]

    attr_succeeded, attr_error = apply_attribute_update(
        graph_client, user_id, state["patch_dict"],
    )
    if not attr_succeeded:
        audit_record["warnings"].append(f"Attribute update failed: {attr_error}")

    return {**state, "audit_record": audit_record, "recently_removed": recently_removed}


# Activity: verify_finalize — post-move verification + audit write + lock release
# + JmlEvents terminal. The orchestrator's terminal step for a PROCEED path.

def verify_finalize(state: dict) -> dict:
    payload = _payload_from_dict(state["payload_dict"])
    employee_id = payload.employee_id
    event_id = state["event_id"]
    audit_record = state["audit_record"]

    table_client      = _table_client()
    jml_events_client = _events_client()
    graph_client      = _graph_client()

    # If additions gated the removals off, the deferral warning and empty
    # removals were set by the orchestrator path (no remove finalize ran).
    recently_removed = state.get("recently_removed", [])
    if "packages_removed" not in audit_record:
        audit_record["packages_removed"] = []

    verify = stage_verify(
        payload, state["user_id"], state["unchanged"], state["retain_set"],
        state["groups_to_add"], state["unmanaged"], recently_removed, graph_client,
    )
    audit_record["post_move_verification"] = verify.data["audit_post_move_verification"]
    verification_status = verify.data["verification_status"]

    if verification_status == "VERIFICATION_ERROR":
        final_status = "MOVE_FAILED"
    elif verification_status == "MOVE_PARTIAL":
        final_status = "MOVE_PARTIAL"
    else:
        final_status = "MOVE_SUCCESS"

    audit_record["post_move_status"] = final_status
    _write_event_log(table_client, employee_id, event_id, final_status,
                     retention_applied=state.get("retention_applied"))
    _write_audit_record(table_client, employee_id, event_id, audit_record)

    jml_final = EventStatus.COMPLETED if final_status == "MOVE_SUCCESS" else EventStatus.FAILED
    release_lock(jml_events_client, employee_id, event_id)
    update_event_status(
        table_client=jml_events_client, employee_id=employee_id, event_id=event_id,
        status=jml_final,
        failure_step="PostMoveVerification" if final_status == "MOVE_PARTIAL" else "",
    )

    return {"final_status": final_status, "employee_id": employee_id,
            "event_id": event_id,
            "summary": f"Mover event completed with status {final_status}."}


def _early_fail(table_client, jml_events_client, employee_id, event_id,
                audit_record, reason, failure_step, lock_acquired):
    audit_record["post_move_status"] = "MOVE_FAILED"
    _write_event_log(table_client, employee_id, event_id, "MOVE_FAILED")
    _write_audit_record(table_client, employee_id, event_id, audit_record)
    if lock_acquired:
        release_lock(jml_events_client, employee_id, event_id)
    update_event_status(
        table_client=jml_events_client, employee_id=employee_id, event_id=event_id,
        status=EventStatus.FAILED, failure_step=failure_step,
    )
    return {"final_status": "MOVE_FAILED", "employee_id": employee_id,
            "event_id": event_id, "summary": reason}