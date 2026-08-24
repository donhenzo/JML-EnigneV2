"""
Functions/joiner_http/__init__.py

Azure Function HTTP trigger for the JML Joiner pipeline.

WHY THIS EXISTS:
    This is the top-level orchestrator for the Joiner flow. It wires
    every sub script together. CSV ingestion, normalisation, event store,
    conflict queue, entitlement mapping, validation, provisioning, and
    audit — into a single ordered sequence for each identity record.

    It is split into two parts so the pipeline logic can run anywhere:

    run_pipeline() — pure Python, no Azure SDK dependency. Called by
        scripts/run_local.py for local runs and by tests directly.
        All Phase 1 pipeline logic lives here.

    main() — thin Azure Functions HTTP entry point. Reads the request,
        resolves config from env vars, and calls run_pipeline(). The
        azure.functions import is deferred inside this function so the
        module loads cleanly in any Python environment.

FULL SEQUENCE PER RECORD:
    1.  Parse CSV or HR API input.
    2.  Construct IdentityPayload
    3.  Normalise (department + job_title canonicalisation)
    4.  Check for stale locks — reclaim if >10 minutes old
    5.  Claim event          — idempotency gate, exits on duplicate
    6.  Conflict check       — FIFO queue, parks event if one is active
    7.  Resolve entitlements — mapping rules determine access packages
    8.  Pre-provision validation gate (PowerShell governance engine)
    9.  Acquire lock
    10. Provision via Graph API — access package assignment (ADR-007)
    11. Post-provision validation
    12. Release lock + mark event Completed
    13. Release next queued event (if any)
    14. Write audit report

    One DecisionReport is written per record regardless of outcome.

SoD NOTE (ADR-008):
    There is no Python Separation of Duties evaluation in this pipeline.
    Package incompatibility is enforced natively by Entra at request
    time — a Denied requestState on an access package assignment
    (see Provisioning/provisioner.py::_assign_access_packages) is the
    platform-level replacement for the old pre-provision SoD gate that
    used to run here as Step 8. sod_policies.json, Governance/SoD/, and
    the SoD hold-record path are no longer part of the Joiner flow. They
    may still be referenced by the Mover pipeline, which has not yet
    been migrated — do not delete the Governance/SoD/ module itself
    until that migration happens.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from Ingestion.schema import IdentityPayload, EmploymentType, JmlAction
from Normalization.lookup_loader import load_lookup_table
from Normalization.normalizer import Normalizer
from Hold_queue.models import HoldStatus
from Hold_queue.queue_manager import HoldQueueManager
from Hold_queue.azure_table_hold_queue_store import (
    AzureTableHoldQueueStore,
    get_hold_queue_table_client,
)
from Ingestion.csv_parser import parse_csv
from Audit.models import (
    DecisionReport,
    ReportEvent,
    NormalizationStatus,
    ValidationStatus,
)
from Audit.report_writer import write_report_to_file
from Mapping.mapping_loader import load_mapping_rules
from Mapping.mapping_resolver import resolve_entitlements
from Functions.Event_store.event_store import (
    get_events_table_client,
    generate_event_id,
    claim_event,
    acquire_lock,
    release_lock,
    update_event_status,
    check_active_event,
    EventStatus,
)
from Functions.Event_store.conflict_queue import (
    check_and_handle_conflict,
    ConflictOutcome,
    release_next_queued_event,
)

from Provisioning.provisioner import provision_joiner
from Provisioning.graph_client import build_graph_client, JmlGraphClient
from Audit.run_summary_writer import write_run_summary

from Joiner.stages import (
    stage_normalize, stage_claim_event, stage_conflict_check,
    stage_resolve_entitlements, stage_pre_validate, stage_provision,
    stage_post_validate, stage_finalize,
)
from Joiner.stage_result import StageOutcome

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Summary of a single pipeline run. Returned by run_pipeline()."""
    total:     int  = 0
    succeeded: int  = 0
    held:      int  = 0
    failed:    int  = 0
    errors:    list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "total":     self.total,
            "succeeded": self.succeeded,
            "held":      self.held,
            "failed":    self.failed,
            "errors":    self.errors,
        }

def _execute_joiner_stages(
    normalised_payload: IdentityPayload,
    report:             DecisionReport,
    events_client,
    graph_client:       JmlGraphClient | None,
    hold_queue,
) -> dict:
    """
    Run the post-normalization Joiner stages for one normalized identity:
    claim -> conflict -> resolve -> pre-validate -> provision -> post-validate
    -> finalize.

    Shared by both drivers. Assumes the payload is already normalized (the
    caller owns normalization and its failure reporting, which differs between
    single-payload and batch). Populates the passed report via each stage's
    report entries and the validation status enums. Owns the hold-queue writes
    for validation failures.

    Returns {final_status, event_id?, entra_id?, summary}. Does NOT write the
    report file and does NOT touch batch counters — the caller owns those.
    """
    employee_id = normalised_payload.employee_id

    def apply(result):
        for action in result.report_actions:
            report.add_action(**action)
        for warning in result.report_warnings:
            report.add_warning(warning)

    # Claim
    claim = stage_claim_event(normalised_payload, correlation_id="", events_client=events_client)
    apply(claim)
    event_id = claim.data["event_id"]

    if claim.outcome == StageOutcome.DUPLICATE:
        return {"final_status": "DUPLICATE", "employee_id": employee_id, "event_id": event_id, "summary": "Duplicate event — already claimed in JmlEvents."}
    if claim.outcome == StageOutcome.SKIPPED:
        return {"final_status": "SKIPPED", "employee_id": employee_id, "event_id": event_id, "summary": "Active event already processing this employee."}

    # Conflict
    conflict = stage_conflict_check(normalised_payload, event_id=event_id, events_client=events_client)
    apply(conflict)
    if conflict.outcome == StageOutcome.QUEUED:
        return {"final_status": "QUEUED", "employee_id": employee_id, "event_id": event_id, "summary": "Event queued — another event in progress for this employee."}

    # Resolve
    resolve = stage_resolve_entitlements(normalised_payload, event_id=event_id)
    apply(resolve)

    # Pre-provision validation
    pre = stage_pre_validate(normalised_payload, event_id=event_id)
    apply(pre)
    if pre.outcome == StageOutcome.HELD:
        report.validation_status = ValidationStatus.FAILED
        for reason in pre.hold_reasons:
            report.add_hold_reason(reason)
        hold_record = hold_queue.create_from_validation_failure(
            payload=normalised_payload, reasons=pre.hold_reasons,
        )
        report.hold_record_id = hold_record.record_id
        update_event_status(
            table_client=events_client, employee_id=employee_id,
            event_id=event_id, status=EventStatus.FAILED, failure_step="PreProvisionValidation",
        )
        return {"final_status": "HELD", "employee_id": employee_id, "event_id": event_id, "summary": f"Pre-provision validation failed: {pre.hold_reasons}"}

    report.validation_status = ValidationStatus.PASSED

    # Graph client guard
    if graph_client is None:
        report.add_action(
            action="ProvisioningSkipped",
            detail="Graph client not available — check credentials in local.settings.json",
            succeeded=False,
        )
        update_event_status(
            table_client=events_client, employee_id=employee_id,
            event_id=event_id, status=EventStatus.FAILED, failure_step="GraphClientUnavailable",
        )
        return {"final_status": "FAILED", "employee_id": employee_id, "event_id": event_id, "summary": "Graph client not available"}

    # Acquire lock and provision
    instance_id = str(uuid.uuid4())
    acquire_lock(events_client, employee_id, event_id, instance_id)

    provision = stage_provision(
        normalised_payload, event_id=event_id,
        access_packages=resolve.data["access_packages"],
        pim_groups=resolve.data["pim_groups"],
        graph_client=graph_client,
    )
    apply(provision)

    if provision.outcome == StageOutcome.FAILED:
        finalize = stage_finalize(
            normalised_payload, event_id=event_id,
            final_status=EventStatus.FAILED, failure_step=provision.data["failure_step"],
            events_client=events_client,
        )
        apply(finalize)
        return {"final_status": "FAILED", "employee_id": employee_id, "event_id": event_id, "summary": f"Provisioning failed at {provision.data['failure_step']}: {provision.data.get('failure_detail')}"}

    entra_id = provision.data["entra_id"]

    # Post-provision validation
    post = stage_post_validate(entra_id=entra_id, event_id=event_id, employee_id=employee_id)
    apply(post)

    if post.outcome == StageOutcome.FAILED:
        report.validation_status = ValidationStatus.FAILED
        finalize = stage_finalize(
            normalised_payload, event_id=event_id,
            final_status=EventStatus.FAILED, failure_step="PostProvisionValidation",
            events_client=events_client,
        )
        apply(finalize)
        return {"final_status": "FAILED", "employee_id": employee_id, "event_id": event_id, "summary": f"Post-provision validation failed: {post.summary}"}

    # Success
    finalize = stage_finalize(
        normalised_payload, event_id=event_id,
        final_status=EventStatus.COMPLETED, events_client=events_client,
    )
    apply(finalize)

    logger.info(f"Joiner pipeline complete — employee={employee_id}, entra_id={entra_id}")
    return {
        "final_status": "COMPLETED",
        "employee_id": employee_id,
        "event_id": event_id,
        "entra_id": entra_id,
        "summary": f"Joiner provisioning succeeded — {len(resolve.data['access_packages'])} package(s) assigned.",
    }





def run_pipeline(
    csv_path:       str,
    lookup_path:    str,
    output_dir:     str,
    correlation_id: str = "local",
) -> PipelineResult:
    """
    Run the Phase 1 JML Joiner pipeline against a CSV file or HR API data.

    Flow per record:
        Parse CSV → construct IdentityPayload → normalize
        → check for stale locks → claim event → conflict check
        → resolve entitlements
        → pre-provision validation → acquire lock
        → provision → post-provision validation
        → release lock → release next queued event → audit report

    One DecisionReport is written per record regardless of outcome.
    """
    result = PipelineResult()

    connection_string  = os.environ.get("JML_STORAGE_CONNECTION_STRING", "")
    mapping_rules_path = os.environ.get(
        "JML_MAPPING_RULES_PATH", "config/role_mapping_rules.json"
    )

    hold_queue_client = get_hold_queue_table_client(connection_string)
    hold_queue        = HoldQueueManager(AzureTableHoldQueueStore(hold_queue_client))
    events_client     = get_events_table_client(connection_string)

    try:
        _graph_service_client, _credential = build_graph_client()
        graph_client = JmlGraphClient(_graph_service_client, _credential)
    except Exception as e:
        logger.error(f"Failed to build Graph client: {e}")
        graph_client = None

    lookup     = load_lookup_table(lookup_path)
    normalizer = Normalizer(lookup)

    try:
        mapping_rules = load_mapping_rules(mapping_rules_path)
    except Exception as e:
        logger.error(f"Failed to load mapping rules: {e}")
        mapping_rules = []

    all_reports: list[DecisionReport] = []

    csv_content  = Path(csv_path).read_text(encoding="utf-8-sig")
    parse_result = parse_csv(csv_content)

    # Parse rejections 
    # Record never reached provisioning — failed structural CSV validation.
    # Builds audit trail of the failure reasons.
    for raw_row in parse_result.rejected_rows:
        employee_id = raw_row.get("EmployeeId", "unknown")
        upn         = raw_row.get("UPN", "unknown")
        reason      = raw_row.get("rejection_reason", "CSV parse error")

        hold_queue.create_from_parse_error(
            employee_id=employee_id,
            upn=upn,
            reasons=[reason],
            raw_row=raw_row,
        )

        report = DecisionReport(
            upn=                 upn,
            employee_id=         employee_id,
            event=               ReportEvent.JOINER,
            correlation_id=      correlation_id,
            normalization_status=NormalizationStatus.FAILED,
            validation_status=   ValidationStatus.SKIPPED,
        )
        report.add_hold_reason(reason)
        all_reports.append(report)
        _write_report(report, output_dir, result)

        result.total += 1
        result.held  += 1

    # Valid rows 
    for raw_row in parse_result.valid_rows:

        # Step 1 — Construct IdentityPayload
        try:
            payload = IdentityPayload(
                employee_id=    raw_row.employee_id,
                upn=            raw_row.upn,
                display_name=   raw_row.display_name,
                department=     raw_row.department_raw,
                job_title=      raw_row.job_title_raw,
                manager_id=     raw_row.manager_id,
                start_date=     raw_row.start_date,
                employment_type=EmploymentType(raw_row.employment_type_raw),
                location=       raw_row.location,
                action=         JmlAction(raw_row.action_raw),
                retain_roles=   raw_row.retain_roles,
                retain_list=    raw_row.retain_list,
            )
        except ValueError as exc:
            report = DecisionReport(
                upn=                 raw_row.upn,
                employee_id=         raw_row.employee_id,
                event=               ReportEvent.JOINER,
                correlation_id=      correlation_id,
                normalization_status=NormalizationStatus.FAILED,
                validation_status=   ValidationStatus.SKIPPED,
            )
            report.add_hold_reason(f"Invalid field value: {exc}")
            all_reports.append(report)
            _write_report(report, output_dir, result)
            result.total += 1
            result.held  += 1
            continue

                # Step 2 — Normalise
        report = DecisionReport(
            upn=           payload.upn,
            employee_id=   payload.employee_id,
            event=         ReportEvent.JOINER,
            correlation_id=correlation_id,
        )

        norm_result = normalizer.normalize(payload)

        if not norm_result.passed:
            report.normalization_status = NormalizationStatus.FAILED
            report.validation_status    = ValidationStatus.SKIPPED
            for reason in norm_result.failures:
                report.add_hold_reason(reason)
            hold_record = hold_queue.create_from_normalization_failure(
                payload=norm_result.payload, reasons=norm_result.failures,
            )
            report.hold_record_id = hold_record.record_id
            all_reports.append(report)
            _write_report(report, output_dir, result)
            result.total += 1
            result.held  += 1
            continue

        report.normalization_status = NormalizationStatus.PASSED
        report.add_action(
            action="NormalizationPassed",
            detail=f"department={norm_result.payload.department}, job_title={norm_result.payload.job_title}",
        )

        # Stages 3-10 via the shared sequence
        stage_result = _execute_joiner_stages(
            normalised_payload=norm_result.payload,
            report=report,
            events_client=events_client,
            graph_client=graph_client,
            hold_queue=hold_queue,
        )

        all_reports.append(report)
        _write_report(report, output_dir, result)
        result.total += 1

        _COUNTER_MAP = {
            "COMPLETED": "succeeded", "DUPLICATE": "succeeded",
            "QUEUED": "succeeded", "SKIPPED": "succeeded",
            "HELD": "held", "FAILED": "failed",
        }
        setattr(result, _COUNTER_MAP[stage_result["final_status"]],
                getattr(result, _COUNTER_MAP[stage_result["final_status"]]) + 1)

    return result

def _write_report(
    report:     DecisionReport,
    output_dir: str,
    result:     PipelineResult,
) -> None:
    """Write one audit report. Failures recorded but never suppress pipeline."""
    try:
        write_report_to_file(report, output_dir)
    except Exception as exc:
        msg = f"Audit write failed for {report.upn}: {exc}"
        logger.exception(msg)
        result.errors.append(msg)

def joiner_pipeline(
    payload:      IdentityPayload,
    table_client,
    graph_client: JmlGraphClient,
) -> dict:
    """
    Single-payload Joiner driver. Normalizes, then runs the shared stage
    sequence. The HTTP trigger's entry point.
    """
    employee_id = payload.employee_id
    connection_string = os.environ.get("JML_STORAGE_CONNECTION_STRING", "")
    output_dir = os.environ.get("LOCAL_REPORT_DIR", "/tmp/jml_reports")

    hold_queue_client = get_hold_queue_table_client(connection_string)
    hold_queue = HoldQueueManager(AzureTableHoldQueueStore(hold_queue_client))
    events_client = get_events_table_client(connection_string)

    lookup = load_lookup_table(os.environ.get("LOCAL_LOOKUP_PATH", "config/canonical_lookup.json"))
    normalizer = Normalizer(lookup)

    report = DecisionReport(
        upn=payload.upn,
        employee_id=employee_id,
        event=ReportEvent.JOINER,
    )

    norm = normalizer.normalize(payload)
    if not norm.passed:
        report.normalization_status = NormalizationStatus.FAILED
        report.validation_status = ValidationStatus.SKIPPED
        for reason in norm.failures:
            report.add_hold_reason(reason)
        hold_queue.create_from_normalization_failure(payload=norm.payload, reasons=norm.failures)
        _write_report_single(report, output_dir)
        return {"final_status": "HELD", "employee_id": employee_id, "summary": f"Normalization failed: {norm.failures}"}

    report.normalization_status = NormalizationStatus.PASSED
    report.add_action(
        action="NormalizationPassed",
        detail=f"department={norm.payload.department}, job_title={norm.payload.job_title}",
        succeeded=True,
    )

    result = _execute_joiner_stages(
        normalised_payload=norm.payload,
        report=report,
        events_client=events_client,
        graph_client=graph_client,
        hold_queue=hold_queue,
    )
    _write_report_single(report, output_dir)
    return result

def _write_report_single(report: DecisionReport, output_dir: str) -> None:
    """Write one audit report in single-payload mode."""
    try:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        write_report_to_file(report, output_dir)
    except Exception as exc:
        logger.error(f"Audit write failed for {report.upn}: {exc}")


def main(req):
    """
    Azure Functions HTTP trigger entry point.

    Accepts two request formats:
        {"payload": {...}}  — single identity, direct processing
        {"csv_path": "..."}  — batch CSV processing (existing mode)

    The payload format matches Mover and Leaver HTTP triggers, so all
    three pipelines accept the same JSON shape from webhooks and callers.
    """
    try:
        import azure.functions as func
    except ImportError:
        raise RuntimeError(
            "azure.functions is not installed. "
            "Use run_pipeline() directly or scripts/run_local.py for local runs."
        )

    try:
        body = req.get_json()
    except (ValueError, TypeError):
        body = {}

    # Direct payload mode — matches Mover/Leaver pattern
    if "payload" in body:
        try:
            from datetime import date
            raw = body["payload"]
            if isinstance(raw.get("start_date"), str):
                raw["start_date"] = date.fromisoformat(raw["start_date"])
            if isinstance(raw.get("employment_type"), str):
                raw["employment_type"] = EmploymentType(raw["employment_type"])
            if isinstance(raw.get("action"), str):
                raw["action"] = JmlAction(raw["action"])
            payload = IdentityPayload(**raw)

            graph_service_client, credential = build_graph_client()
            graph_client = JmlGraphClient(graph_service_client, credential)

            conn_str = os.environ.get("JML_STORAGE_CONNECTION_STRING", "")
            from azure.data.tables import TableServiceClient
            table_client = TableServiceClient.from_connection_string(conn_str)

            result = joiner_pipeline(
                payload=payload,
                table_client=table_client,
                graph_client=graph_client,
            )

            return func.HttpResponse(
                json.dumps(result), status_code=200, mimetype="application/json",
            )

        except Exception as e:
            logger.error("Joiner HTTP trigger failed: %s", str(e))
            return func.HttpResponse(
                json.dumps({"error": str(e)}), status_code=500, mimetype="application/json",
            )

    # CSV path mode — existing behaviour
    correlation_id = req.headers.get("x-ms-client-request-id", "unknown")
    lookup_path = os.environ.get(
        "LOCAL_LOOKUP_PATH", "/tmp/jml_config/canonical_lookup.json"
    )
    output_dir = os.environ.get("LOCAL_REPORT_DIR", "/tmp/jml_reports")

    content_type = req.headers.get("Content-Type", "")
    try:
        csv_path = _extract_csv_path(req, content_type)
    except ValueError as exc:
        return func.HttpResponse(
            body=json.dumps({"error": str(exc)}),
            status_code=400,
            mimetype="application/json",
        )

    pipeline_result = run_pipeline(
        csv_path=csv_path,
        lookup_path=lookup_path,
        output_dir=output_dir,
        correlation_id=correlation_id,
    )

    return func.HttpResponse(
        body=json.dumps(pipeline_result.to_dict()),
        status_code=200,
        mimetype="application/json",
    )


def _extract_csv_path(req, content_type: str) -> str:
    """Pulls the CSV out of the HTTP request."""
    if "multipart/form-data" in content_type:
        file_bytes = req.files.get("file")
        if not file_bytes:
            raise ValueError("Multipart request missing 'file' field.")
        tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="wb")
        tmp.write(file_bytes.read())
        tmp.flush()
        return tmp.name

    if "application/json" in content_type:
        try:
            body = req.get_json() or {}
        except (ValueError, TypeError):
            body = {}
        csv_path = body.get("csv_path")
        if not csv_path:
            raise ValueError("JSON body missing 'csv_path' field.")
        return csv_path

    raise ValueError(
        "Unsupported Content-Type. "
        "Use multipart/form-data (CSV upload) or application/json with csv_path."
    )