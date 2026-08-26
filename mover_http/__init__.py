"""
Functions/mover_http/__init__.py

Azure Function HTTP trigger for the Mover module.

Thin driver: composes the extracted Mover stages (Mover/stages.py) and the
sleep-free provisioning phases (Mover/provisioning_phases.py) into the Mover
processing flow. The stages own the business logic and return serializable
StageResults; the phases own package submission/polling with the wait hoisted
out; this driver owns the DecisionReport-equivalent (audit_record), the
MoverEventLog/MoverAuditLog/JmlEvents writes, the two time.sleep poll loops,
and the ADR-009 gate.

The stage/phase split is what makes the pipeline Durable-ready: at the Durable
step this same composition becomes an orchestrator, the time.sleep loops become
durable timers, and each stage becomes an activity — with no change to the
stages or phases themselves. The synchronous path here is the behavior baseline
that migration must preserve exactly.

Processing flow (unchanged from the pre-refactor pipeline):

    Pre-Step  — claim_event() in JmlEvents (stage_claim). Duplicate exits.
    Step 1    — concurrent check (stage_concurrent_check) + current-state
                fetch and lock (stage_fetch_current_state).
    Step 2    — new/old entitlement resolution (stage_resolve).
    Step 3    — package + attribute delta (stage_delta).
    Step 4    — retention evaluation (stage_retention).
    Step 5    — SoD skipped (ADR-008); driver writes the audit constants.
    Step 6    — additions: submit → poll loop (time.sleep) → finalize.
    Step 7    — removals + attribute PATCH, gated on Step 6 all-delivered
                (ADR-009). Same submit → poll → finalize shape.
    Step 8    — post-move verification (stage_verify).
    Step 9    — final status, audit write, release_lock, JmlEvents terminal.

The JmlEvents lock is released on every exit path.

See Mover/stages.py and Mover/provisioning_phases.py for the extracted logic
and the ADR notes (ADR-007 access packages, ADR-008 platform SoD, ADR-009
add-before-remove, ADR-011 deferred pre-flight incompatibility).
"""

from __future__ import annotations
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

from azure.data.tables import TableServiceClient

from Ingestion.schema import IdentityPayload, JmlAction, EmploymentType
from Provisioning.graph_client import JmlGraphClient, build_graph_client
from Mover.stage_result import StageOutcome
from Mover.stages import (
    stage_claim,
    stage_concurrent_check,
    stage_fetch_current_state,
    stage_resolve,
    stage_delta,
    stage_retention,
    stage_verify,
)
from Mover.provisioning_phases import (
    PendingPackage,
    submit_additions,
    poll_packages_once,
    packages_all_terminal,
    finalize_additions,
    submit_removals,
    finalize_removals,
    apply_attribute_update,
)
from Functions.Event_store.event_store import (
    get_events_table_client,
    generate_event_id,
    release_lock,
    update_event_status,
    EventStatus,
)

logger = logging.getLogger(__name__)


# Table names and constants
MOVER_EVENT_LOG_TABLE = "MoverEventLog"
MOVER_AUDIT_LOG_TABLE = "MoverAuditLog"

# Polling for access package assignmentRequest delivery (ADR-009).
# The sleep now lives in this driver's poll loops, not inside the phase
# functions — that seam is what lets the Durable orchestrator swap
# time.sleep for a durable timer without touching the phases.
PACKAGE_POLL_MAX_ATTEMPTS     = int(os.environ.get("JML_PACKAGE_POLL_MAX_ATTEMPTS", "60"))
PACKAGE_POLL_INTERVAL_SECONDS = int(os.environ.get("JML_PACKAGE_POLL_INTERVAL_SECONDS", "5"))


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


def _write_event_log(
    table_client:      TableServiceClient,
    employee_id:       str,
    event_id:          str,
    status:            str,
    payload_json:      str = "",
    retention_applied: Optional[bool] = None,
) -> None:
    """
    Write or update a MoverEventLog entry. retention_applied is written as an
    explicit row tag only when known (Step 9); the early-failure and
    in-progress writes omit it, so "not yet evaluated" stays distinguishable
    from "evaluated, none applied."
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
            "MoverEventLog write failed — employee=%s, event=%s, status=%s, error=%s",
            employee_id, event_id, status, str(e),
        )


def _write_audit_record(
    table_client: TableServiceClient,
    employee_id:  str,
    event_id:     str,
    audit_record: dict,
) -> None:
    """
    Write the completed MoverAuditRecord to MoverAuditLog. Table Storage only
    accepts flat scalars — nested dicts/lists are JSON-serialized first.
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


# Poll-loop composition — the sync side of the sleep-free seam.
# Submit is already done by the caller; this drives poll_once + time.sleep
# up to the poll window, then returns for the caller to finalize. The
# Durable orchestrator will replace time.sleep with a durable timer over the
# same poll_packages_once / packages_all_terminal phase functions.

def _poll_until_terminal(
    pending:      list[PendingPackage],
    graph_client: JmlGraphClient,
) -> list[PendingPackage]:
    for _ in range(PACKAGE_POLL_MAX_ATTEMPTS):
        if packages_all_terminal(pending):
            return pending
        poll_packages_once(pending, graph_client)
        if packages_all_terminal(pending):
            return pending
        time.sleep(PACKAGE_POLL_INTERVAL_SECONDS)
    return pending


# Main orchestrator

def run_mover_pipeline(
    payload:      IdentityPayload,
    table_client: TableServiceClient,
    graph_client: JmlGraphClient,
) -> dict:
    """
    Execute the Mover processing flow for a single identity event by composing
    the extracted stages and provisioning phases. EventId is generated
    internally from the payload — callers pass only the payload and clients.

    Returns a dict with final_status, employee_id, event_id, and summary.
    """
    employee_id = payload.employee_id
    event_id = generate_event_id(
        employee_id, "Mover", payload.start_date.isoformat()
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

    conn_str          = os.environ.get("JML_STORAGE_CONNECTION_STRING", "")
    jml_events_client = get_events_table_client(conn_str)

    # Pre-Step — claim
    claim = stage_claim(payload, event_id, jml_events_client)
    if claim.outcome == StageOutcome.DUPLICATE:
        logger.info(
            "Mover event already claimed in JmlEvents — idempotency exit — employee=%s",
            employee_id,
        )
        return {
            "final_status": MoverEventStatus.QUEUED_CONCURRENT,
            "employee_id":  employee_id,
            "event_id":     event_id,
            "summary":      "Duplicate event — already claimed in JmlEvents.",
        }

    # Step 1 — concurrent check
    logger.info("Mover Step 1 — current state discovery — employee=%s", employee_id)

    concurrent = stage_concurrent_check(payload, table_client)
    if concurrent.outcome == StageOutcome.QUEUED:
        logger.warning(
            "Concurrent Mover event detected — employee=%s, queuing with status QUEUED_CONCURRENT",
            employee_id,
        )
        _write_event_log(
            table_client, employee_id, event_id, MoverEventStatus.QUEUED_CONCURRENT
        )
        return {
            "final_status": MoverEventStatus.QUEUED_CONCURRENT,
            "employee_id":  employee_id,
            "event_id":     event_id,
            "summary":      "Event queued — another Mover event is in progress for this employee.",
        }

    _write_event_log(
        table_client, employee_id, event_id, MoverEventStatus.IN_PROGRESS
    )

    # Step 1 — current-state fetch + lock
    fetch = stage_fetch_current_state(payload, event_id, graph_client, jml_events_client)
    if fetch.outcome == StageOutcome.FAILED:
        for w in fetch.report_warnings:
            audit_record["warnings"].append(w)
        return _handle_early_failure(
            table_client, jml_events_client, employee_id, event_id,
            audit_record, fetch.summary,
            failure_step=fetch.data["failure_step"],
            lock_acquired=fetch.data.get("lock_acquired", False),
        )

    user_id            = fetch.data["user_id"]
    current_packages   = fetch.data["current_packages"]
    current_policy_map = fetch.data["current_policy_map"]
    package_labels     = fetch.data["package_labels"]
    current_attributes = fetch.data["current_attributes"]
    current_department = fetch.data["current_department"]
    current_job_title  = fetch.data["current_job_title"]

    # Step 2 — target state
    logger.info(
        "Mover Step 2 — target state calculation — employee=%s, "
        "normalized department=%r, job_title=%r",
        employee_id, payload.department, payload.job_title,
    )

    resolve = stage_resolve(
        payload, current_department, current_job_title, package_labels, graph_client
    )
    if resolve.outcome == StageOutcome.FAILED:
        for w in resolve.report_warnings:
            audit_record["warnings"].append(w)
        return _handle_early_failure(
            table_client, jml_events_client, employee_id, event_id,
            audit_record, resolve.summary,
            failure_step=resolve.data["failure_step"],
            lock_acquired=resolve.data.get("lock_acquired", True),
        )

    target_packages     = resolve.data["target_packages"]
    new_policy_map       = resolve.data["new_policy_map"]
    managed_catalogue    = resolve.data["managed_catalogue"]
    package_labels       = resolve.data["package_labels"]
    incoming_attributes  = resolve.data["incoming_attributes"]

    # Step 3 — delta
    logger.info("Mover Step 3 — delta analysis — employee=%s", employee_id)

    delta = stage_delta(
        payload, current_department, current_job_title,
        current_packages, target_packages, managed_catalogue,
        current_attributes, incoming_attributes,
    )
    audit_record["from_department"]    = delta.data["audit_from_department"]
    audit_record["to_department"]      = delta.data["audit_to_department"]
    audit_record["from_title"]         = delta.data["audit_from_title"]
    audit_record["to_title"]           = delta.data["audit_to_title"]
    audit_record["attribute_changes"]  = delta.data["audit_attribute_changes"]
    audit_record["unmanaged_packages"] = delta.data["audit_unmanaged_packages"]

    groups_to_add    = delta.data["groups_to_add"]
    groups_to_remove = delta.data["groups_to_remove"]
    unchanged        = delta.data["unchanged"]
    unmanaged        = delta.data["unmanaged"]
    patch_dict       = delta.data["patch_dict"]

    if unmanaged:
        for pkg_id in unmanaged:
            label = package_labels.get(pkg_id, pkg_id)
            logger.info(
                "  ⊘ %s — unmanaged, not in any rule's entitlements — "
                "excluded from all delta logic (NOT_PROCESSED)",
                label,
            )
        logger.info(
            "Step 3 — %d unmanaged package(s) detected, left untouched",
            len(unmanaged),
        )

       # Step 4 — retention
    retention = stage_retention(payload, groups_to_remove, table_client)
    audit_record["packages_retained"]   = retention.data["audit_packages_retained"]
    audit_record["retention_decisions"] = retention.data["audit_retention_decisions"]
    audit_record["retention_summary"]   = retention.data["audit_retention_summary"]

    remove_confirmed  = retention.data["remove_confirmed"]
    retain_set        = retention.data["retain_set"]
    retention_applied = retention.data["retention_applied"]

    if groups_to_remove:
        for d in retention.data["audit_retention_decisions"]:
            label = package_labels.get(d["package_id"], d["package_id"])
            if d["outcome"] == "RETAINED":
                logger.info(
                    "  ⊘ %s — retention detected, excluded from removal (until=%s, reason=%r)",
                    label, d["review_date"] or "unknown", d["reason"],
                )
            elif d["outcome"] == "EXPIRED":
                logger.info("  → %s — retention record expired, proceeding with removal", label)
            else:
                logger.info("  → %s — no retention record, proceeding with removal", label)
        summary = retention.data["audit_retention_summary"]
        logger.info(
            "Step 4 complete — %d retained, %d expired, %d no-record, %d confirmed for removal",
            summary["retained"], summary["expired"], summary["no_record"], summary["removed"],
        )
    # Step 6 — additions (submit → poll loop → finalize)
    logger.info("Mover Step 6 — access package additions — employee=%s", employee_id)

    add_pending = submit_additions(
        graph_client, user_id, frozenset(groups_to_add), new_policy_map, package_labels
    )
    _poll_until_terminal(add_pending, graph_client)
    addition_actions, delivered_additions, additions_all_succeeded = finalize_additions(
        graph_client, user_id, add_pending, frozenset(groups_to_add)
    )
    audit_record["actions_taken"].extend(addition_actions)
    audit_record["packages_added"] = [
        {"id": a["package_id"]} for a in addition_actions if a["succeeded"]
    ]

    if not groups_to_add:
        logger.info("Step 6 — no packages to add for this transition — employee=%s", employee_id)
    elif additions_all_succeeded:
        logger.info(
            "Step 6 — all %d addition(s) delivered — employee=%s",
            len(groups_to_add), employee_id,
        )
    else:
        audit_record["warnings"].append(
            "One or more package additions did not deliver — removals skipped "
            "this pass per ADR-009 (never remove old access before new access "
            "is confirmed). See actions_taken for which package(s) failed."
        )
        logger.warning(
            "Step 6 — not all additions delivered — removals skipped this pass — employee=%s",
            employee_id,
        )

    # Step 7 — removals + attribute PATCH, gated on ADR-009
    logger.info("Mover Step 7 — access package removals — employee=%s", employee_id)

    if additions_all_succeeded:
        remove_pending = submit_removals(
            graph_client, user_id, frozenset(remove_confirmed),
            current_policy_map, package_labels,
        )
        _poll_until_terminal(remove_pending, graph_client)
        removal_actions = finalize_removals(
            graph_client, user_id, remove_pending, frozenset(remove_confirmed)
        )
        audit_record["actions_taken"].extend(removal_actions)
        audit_record["packages_removed"] = [
            {"id": a["package_id"], "reason": "ROLE_CHANGE"}
            for a in removal_actions if a["succeeded"]
        ]
        recently_removed = [
            a["package_id"] for a in removal_actions if a["succeeded"]
        ]

        attr_succeeded, attr_error = apply_attribute_update(
            graph_client, user_id, patch_dict
        )
        if not attr_succeeded:
            audit_record["warnings"].append(f"Attribute update failed: {attr_error}")
    else:
        audit_record["packages_removed"] = []
        recently_removed = []
        audit_record["warnings"].append(
            "Attribute update deferred — package additions did not all deliver, "
            "so the role transition is not committed this pass (ADR-009). "
            "Department/title remain at their previous values until a retry "
            "lands every addition."
        )

    # Step 8 — verification
    logger.info("Mover Step 8 — post-move verification — employee=%s", employee_id)

    verify = stage_verify(
        payload, user_id, unchanged, retain_set, groups_to_add,
        unmanaged, recently_removed, graph_client,
    )
    audit_record["post_move_verification"] = verify.data["audit_post_move_verification"]
    verification_status = verify.data["verification_status"]

    # Step 9 — final status + audit write
    logger.info("Mover Step 9 — audit reporting — employee=%s", employee_id)

    if verification_status == "VERIFICATION_ERROR":
        final_status = MoverEventStatus.MOVE_FAILED
    elif verification_status == "MOVE_PARTIAL":
        final_status = MoverEventStatus.MOVE_PARTIAL
    else:
        final_status = MoverEventStatus.MOVE_SUCCESS

    audit_record["post_move_status"] = final_status
    _write_event_log(
        table_client, employee_id, event_id, final_status,
        retention_applied=retention_applied,
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
        "Mover pipeline complete — employee=%s, status=%s", employee_id, final_status
    )

    return {
        "final_status": final_status,
        "employee_id":  employee_id,
        "event_id":     event_id,
        "summary":      f"Mover event completed with status {final_status}.",
    }


# Helpers

def _fail(employee_id: str, event_id: str, reason: str) -> dict:
    return {
        "final_status": MoverEventStatus.MOVE_FAILED,
        "employee_id":  employee_id,
        "event_id":     event_id,
        "summary":      reason,
    }


def _handle_early_failure(
    table_client:      TableServiceClient,
    jml_events_client: TableServiceClient,
    employee_id:       str,
    event_id:          str,
    audit_record:      dict,
    reason:            str,
    failure_step:      str,
    lock_acquired:     bool = False,
) -> dict:
    """
    Handle a failure before Step 9's cleanup runs: write the MoverAuditLog
    record (a report for every event), release the JmlEvents lock if held,
    mark JmlEvents Failed. reason is already appended to warnings by the
    caller before this runs, so it is NOT re-appended here.
    """
    audit_record["post_move_status"] = MoverEventStatus.MOVE_FAILED

    _write_event_log(table_client, employee_id, event_id, MoverEventStatus.MOVE_FAILED)
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
    Azure Function HTTP trigger. Expects a JSON body with a canonical
    IdentityPayload under "payload". event_id is generated internally by
    run_mover_pipeline() from the payload fields.
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
            json.dumps(result), status_code=200, mimetype="application/json",
        )
    except Exception as e:
        logger.error("Mover HTTP trigger failed: %s", str(e))
        return func.HttpResponse(
            json.dumps({"error": str(e)}), status_code=500, mimetype="application/json",
        )