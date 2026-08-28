"""
Functions/leaver_http/__init__.py

Azure Function HTTP trigger for the Leaver module — thin synchronous driver.

Orchestrates offboarding for a single identity lifecycle event. Called
directly via HTTP or by the BambooHR ingestion coordinator when action
derivation returns JmlAction.LEAVER.

This module holds no business logic. It composes the stages in Leaver/stages.py
and the removal seam in Leaver/provisioning_phases.py, owns the audit_record
dict and every Table Storage write, and owns the removal poll wait as a
time.sleep loop. The Durable orchestrator (a later step) composes the same
stages and phases with durable timers instead of time.sleep — the two paths
share one implementation and differ only in who owns the waiting.

Processing flow (ADR-015):

    Pre-Step  — claim_event() in JmlEvents (atomic; duplicate exits).
                check_and_handle_conflict() supersedes pending Joiner/Mover
                events for this employee.
    Step 1    — Concurrent-event guard, then user + current package fetch,
                then acquire_lock().
    Step 2    — Disable account (accountEnabled=false). First mutation, so a
                downstream failure still fails safe.
    Step 3    — Revoke all sign-in sessions.
    Step 4    — Remove every currently held access package (ADR-014).
    Step 5    — Terminate active PIM group sessions (ADR-016).
    Step 6    — Soft delete the user, subject to a configurable hold.
    Step 7    — Post-offboarding verification against real tenant state.
    Step 8    — LeaverAuditRecord written. release_lock(). JmlEvents updated.

The JmlEvents lock is released on every exit path.
"""

from __future__ import annotations
import json
import logging
import os
import time
from datetime import datetime, timezone

from azure.data.tables import TableServiceClient, TableClient

from Ingestion.schema import IdentityPayload, JmlAction, EmploymentType
from Provisioning.graph_client import JmlGraphClient, build_graph_client
from Functions.Event_store.event_store import (
    get_events_table_client,
    generate_event_id,
    release_lock,
    update_event_status,
    EventStatus,
)
from Leaver.stage_result import StageOutcome
from Leaver.stages import (
    stage_claim,
    stage_conflict_check,
    stage_concurrent_check,
    stage_fetch_current_state,
    stage_disable,
    stage_revoke,
    stage_pim_terminate,
    stage_soft_delete,
    stage_verify,
)
from Leaver.provisioning_phases import (
    submit_removals,
    poll_packages_once,
    packages_all_terminal,
    finalize_removals,
)

logger = logging.getLogger(__name__)

LEAVER_EVENT_LOG_TABLE = "LeaverEventLog"
LEAVER_AUDIT_LOG_TABLE = "LeaverAuditLog"

# Removal poll window — same env vars and defaults the Joiner and Mover use, so
# the sync driver and the future durable orchestrator share one window.
POLL_MAX_ATTEMPTS      = int(os.environ.get("JML_PACKAGE_POLL_MAX_ATTEMPTS", "60"))
POLL_INTERVAL_SECONDS  = int(os.environ.get("JML_PACKAGE_POLL_INTERVAL_SECONDS", "5"))


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
            "LeaverEventLog write failed — employee=%s, event=%s, status=%s, error=%s",
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

    Table Storage only accepts flat scalar values. Nested dicts and lists are
    serialised to JSON strings before writing — same pattern as MoverAuditLog.
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


def _run_removal_loop(
    graph_client:       JmlGraphClient,
    user_id:            str,
    current_packages:   frozenset[str],
    current_policy_map: dict[str, str],
    package_labels:     dict[str, str],
) -> list[dict]:
    """
    Step 4 — submit every adminRemove, poll to terminal with a time.sleep wait
    between passes, then finalize. This is the synchronous composition of the
    removal seam; the durable orchestrator composes the same phase functions
    with timers. The wait lives here in the caller, never in the phases.
    """
    pending = submit_removals(
        graph_client=graph_client,
        user_id=user_id,
        current_packages=current_packages,
        current_policy_map=current_policy_map,
        package_labels=package_labels,
    )

    for _ in range(POLL_MAX_ATTEMPTS):
        if packages_all_terminal(pending):
            break
        poll_packages_once(pending, graph_client)
        if packages_all_terminal(pending):
            break
        time.sleep(POLL_INTERVAL_SECONDS)

    return finalize_removals(
        graph_client=graph_client,
        user_id=user_id,
        pending=pending,
    )


# Main orchestrator

def run_leaver_pipeline(
    payload:      IdentityPayload,
    table_client: TableServiceClient,
    graph_client: JmlGraphClient,
) -> dict:
    """
    Execute the Leaver offboarding flow for a single identity event.

    The EventId is generated internally from the payload, same as the Mover —
    event ownership stays inside the pipeline, not the ingestion layer.

    Returns a dict with final_status, employee_id, event_id, and summary.
    """
    if payload.action != JmlAction.LEAVER:
        raise ValueError(
            f"run_leaver_pipeline called with action={payload.action!r}, "
            f"expected JmlAction.LEAVER. Refusing to run offboarding logic "
            f"against a non-Leaver payload."
        )

    employee_id = payload.employee_id
    event_id = generate_event_id(employee_id, "Leaver", payload.start_date.isoformat())

    conn_str          = os.environ.get("JML_STORAGE_CONNECTION_STRING", "")
    jml_events_client = get_events_table_client(conn_str)

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

    # Pre-Step — claim, then conflict supersede.
    claim = stage_claim(payload, event_id=event_id, jml_events_client=jml_events_client)
    if claim.outcome == StageOutcome.DUPLICATE:
        logger.info(
            "Leaver event already claimed in JmlEvents — idempotency exit — employee=%s",
            employee_id,
        )
        return {
            "final_status": LeaverEventStatus.QUEUED_CONCURRENT,
            "employee_id":  employee_id,
            "event_id":     event_id,
            "summary":      claim.summary,
        }

    stage_conflict_check(payload, event_id=event_id, jml_events_client=jml_events_client)

    # Step 1 — concurrent guard, then current-state discovery + lock.
    logger.info("Leaver Step 1 — current state discovery — employee=%s", employee_id)

    concurrent = stage_concurrent_check(payload, table_client=table_client)
    if concurrent.outcome == StageOutcome.QUEUED:
        logger.warning(
            "Concurrent Leaver event detected — employee=%s, queuing with "
            "status QUEUED_CONCURRENT", employee_id,
        )
        _write_event_log(table_client, employee_id, event_id, LeaverEventStatus.QUEUED_CONCURRENT)
        return {
            "final_status": LeaverEventStatus.QUEUED_CONCURRENT,
            "employee_id":  employee_id,
            "event_id":     event_id,
            "summary":      concurrent.summary,
        }

    _write_event_log(table_client, employee_id, event_id, LeaverEventStatus.IN_PROGRESS)

    fetch = stage_fetch_current_state(
        payload, event_id=event_id,
        graph_client=graph_client, jml_events_client=jml_events_client,
    )
    if fetch.outcome == StageOutcome.FAILED:
        reason = fetch.report_warnings[0] if fetch.report_warnings else fetch.summary
        logger.error("Step 1 failed — %s — employee=%s", fetch.data.get("failure_step"), employee_id)
        return _handle_early_failure(
            table_client, jml_events_client, employee_id, event_id,
            audit_record, reason,
            failure_step=fetch.data.get("failure_step", "Step1"),
            lock_acquired=fetch.data.get("lock_acquired", False),
        )

    user_id            = fetch.data["user_id"]
    current_packages   = frozenset(fetch.data["current_packages"])
    current_policy_map = fetch.data["current_policy_map"]
    package_labels     = fetch.data["package_labels"]

    audit_record["packages_at_offboard_start"] = list(current_packages)

    # Step 2 — disable account.
    logger.info("Leaver Step 2 — disable account — employee=%s", employee_id)
    disable = stage_disable(payload, user_id=user_id, graph_client=graph_client)
    _apply_stage_reports(audit_record, disable)

    # Step 3 — revoke sessions.
    logger.info("Leaver Step 3 — revoke sessions — employee=%s", employee_id)
    revoke = stage_revoke(payload, user_id=user_id, graph_client=graph_client)
    _apply_stage_reports(audit_record, revoke)

    # Step 4 — remove all access packages (ADR-014).
    logger.info("Leaver Step 4 — access package removal — employee=%s", employee_id)
    removal_actions = _run_removal_loop(
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

    # Step 5 — PIM session termination (ADR-016).
    logger.info("Leaver Step 5 — PIM session termination — employee=%s", employee_id)
    pim = stage_pim_terminate(payload, user_id=user_id, graph_client=graph_client)
    _apply_stage_reports(audit_record, pim)

    # Step 6 — soft delete (configurable hold).
    logger.info("Leaver Step 6 — soft delete — employee=%s", employee_id)
    soft_delete = stage_soft_delete(payload, user_id=user_id, graph_client=graph_client)
    _apply_stage_reports(audit_record, soft_delete)
    user_deleted = soft_delete.data.get("user_deleted", False)

    # Step 7 — post-offboarding verification.
    logger.info("Leaver Step 7 — post-offboarding verification — employee=%s", employee_id)
    verify = stage_verify(
        payload, user_id=user_id, user_deleted=user_deleted,
        packages_removal_failed=packages_removal_failed, graph_client=graph_client,
    )
    _apply_stage_reports(audit_record, verify)
    audit_record["post_offboard_verification"] = verify.data["audit_post_offboard_verification"]

    verification_error         = verify.data["verification_error"]
    account_disabled_confirmed = verify.data["account_disabled_confirmed"]
    packages_cleared           = verify.data["packages_cleared"]

    # Step 8 — final status + audit record.
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

    logger.info("Leaver pipeline complete — employee=%s, status=%s", employee_id, final_status)

    return {
        "final_status": final_status,
        "employee_id":  employee_id,
        "event_id":     event_id,
        "summary":      f"Leaver event completed with status {final_status}.",
    }


# Helpers

def _apply_stage_reports(audit_record: dict, result) -> None:
    """Fold a stage's report_actions and report_warnings into the audit_record."""
    if result.report_actions:
        audit_record["actions_taken"].extend(result.report_actions)
    if result.report_warnings:
        audit_record["warnings"].extend(result.report_warnings)


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
    Handle a failure before Step 8's own cleanup runs. Every terminal path must
    still produce a LeaverAuditLog record.
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

    Expects a JSON body with a canonical IdentityPayload where action ==
    "Leaver". start_date is used as the termination date.

    Environment variables required:
        AZURE_STORAGE_CONNECTION_STRING
        AZURE_TENANT_ID
        AZURE_CLIENT_ID
        AZURE_CLIENT_SECRET
        JML_LEAVER_SOFT_DELETE_HOLD_DAYS (optional, default 0)
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