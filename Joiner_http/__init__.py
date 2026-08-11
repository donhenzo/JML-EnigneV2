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
from Validation.validation_gate import pre_provision_validate, post_provision_validate
from Provisioning.provisioner import provision_joiner
from Provisioning.graph_client import build_graph_client, JmlGraphClient
from Audit.run_summary_writer import write_run_summary


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
                payload=norm_result.payload,
                reasons=norm_result.failures,
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
            detail=(
                f"department={norm_result.payload.department}, "
                f"job_title={norm_result.payload.job_title}"
            ),
        )
        normalised_payload = norm_result.payload

        # Step 3 — Check for stale locks (GAP-002 remediation)
        active = check_active_event(
            table_client=events_client,
            employee_id= normalised_payload.employee_id,
        )

        if active and active.status == EventStatus.PROCESSING:
            logger.info(
                f"Active event in progress — employee={normalised_payload.employee_id}, "
                f"locked_by={active.locked_by}, exiting"
            )
            report.add_action(
                action="ActiveEventSkipped",
                detail=f"Another instance processing this employee (locked by {active.locked_by})",
                succeeded=True,
            )
            all_reports.append(report)
            _write_report(report, output_dir, result)
            result.total     += 1
            result.succeeded += 1
            continue

        # Step 4 — Claim event
        import json as _json
        payload_json = _json.dumps({
            "employee_id": normalised_payload.employee_id,
            "upn":         normalised_payload.upn,
            "action":      normalised_payload.action.value,
            "start_date":  normalised_payload.start_date.isoformat(),
        })

        claimed = claim_event(
            table_client=   events_client,
            employee_id=    normalised_payload.employee_id,
            action=         normalised_payload.action.value,
            start_date=     normalised_payload.start_date.isoformat(),
            payload_json=   payload_json,
            correlation_id= correlation_id,
        )

        if not claimed:
            logger.info(
                f"Duplicate event — skipping — "
                f"employee={normalised_payload.employee_id}"
            )
            report.add_action(
                action="DuplicateEventSkipped",
                detail="Event already exists in event store, idempotency exit",
                succeeded=True,
            )
            all_reports.append(report)
            _write_report(report, output_dir, result)
            result.total     += 1
            result.succeeded += 1
            continue

        event_id = generate_event_id(
            normalised_payload.employee_id,
            normalised_payload.action.value,
            normalised_payload.start_date.isoformat(),
        )

        # Step 5 — Conflict check
        conflict_outcome = check_and_handle_conflict(
            table_client= events_client,
            employee_id=  normalised_payload.employee_id,
            new_event_id= event_id,
            new_action=   normalised_payload.action.value,
        )

        if conflict_outcome == ConflictOutcome.QUEUED:
            report.add_action(
                action="EventQueued",
                detail="Active event in progress — queued behind existing event",
                succeeded=True,
            )
            report.add_warning(
                "Event queued — will process after active event completes"
            )
            all_reports.append(report)
            _write_report(report, output_dir, result)
            result.total     += 1
            result.succeeded += 1
            continue

        # Step 6 — Resolve entitlements
        entitlements = resolve_entitlements(
            rules=           mapping_rules,
            department=      normalised_payload.department,
            job_title=       normalised_payload.job_title,
            employment_type= normalised_payload.employment_type.value,
            employee_id=     normalised_payload.employee_id,
        )

        report.add_action(
            action="EntitlementsResolved",
            detail=(
                f"matched_rules={entitlements.matched_rule_ids}, "
                f"access_packages="
                f"{[ap.access_package_id for ap in entitlements.access_packages]}"
            ),
            succeeded=True,
        )

        if not entitlements.matched_rule_ids:
            report.add_warning(
                "No mapping rules matched — user will have no access package assignments"
            )

        # Step 7 — Pre-provision validation gate (PowerShell governance engine)
        validation_result = pre_provision_validate(normalised_payload)

        if not validation_result.passed:
            report.validation_status = ValidationStatus.FAILED

            for failure in validation_result.failure_summary():
                report.add_hold_reason(failure)

            hold_record = hold_queue.create_from_validation_failure(
                payload=normalised_payload,
                reasons=validation_result.failure_summary(),
            )
            report.hold_record_id = hold_record.record_id

            update_event_status(
                table_client= events_client,
                employee_id=  normalised_payload.employee_id,
                event_id=     event_id,
                status=       EventStatus.FAILED,
                failure_step= "PreProvisionValidation",
            )

            all_reports.append(report)
            _write_report(report, output_dir, result)
            result.total += 1
            result.held  += 1
            continue

        report.validation_status = ValidationStatus.PASSED
        for warning in validation_result.warning_summary():
            report.add_warning(warning)

        # Step 8 — Guard: Graph client must be available before acquiring lock.
        if graph_client is None:
            report.add_action(
                action="ProvisioningSkipped",
                detail="Graph client not available — check credentials in local.settings.json",
                succeeded=False,
            )
            update_event_status(
                table_client= events_client,
                employee_id=  normalised_payload.employee_id,
                event_id=     event_id,
                status=       EventStatus.FAILED,
                failure_step= "GraphClientUnavailable",
            )
            all_reports.append(report)
            _write_report(report, output_dir, result)
            result.total  += 1
            result.failed += 1
            continue

        instance_id = str(uuid.uuid4())
        acquire_lock(events_client, normalised_payload.employee_id, event_id, instance_id)

        provisioning_result = provision_joiner(
            payload=      normalised_payload,
            entitlements= entitlements,
            report=       report,
            graph_client= graph_client,
            event_status= EventStatus.PROCESSING,
        )

        if not provisioning_result.succeeded:
            release_lock(events_client, normalised_payload.employee_id, event_id)
            update_event_status(
                table_client= events_client,
                employee_id=  normalised_payload.employee_id,
                event_id=     event_id,
                status=       EventStatus.FAILED,
                failure_step= provisioning_result.failure_step,
            )

            next_event = release_next_queued_event(
                table_client=       events_client,
                employee_id=        normalised_payload.employee_id,
                predecessor_status= EventStatus.FAILED,
            )
            if next_event:
                logger.warning(
                    f"Next queued event held for review — employee={normalised_payload.employee_id}, "
                    f"event_id={next_event.event_id} — predecessor failed at {provisioning_result.failure_step}"
                )

            all_reports.append(report)
            _write_report(report, output_dir, result)
            result.total  += 1
            result.failed += 1
            continue

        # Step 9 — Post-provision validation
        post_result = post_provision_validate(
            entra_object_id=provisioning_result.entra_id,
            employee_id=    normalised_payload.employee_id,
        )

        if not post_result.passed:
            report.validation_status = ValidationStatus.FAILED
            report.add_action(
                action="PostProvisionValidationFailed",
                detail=f"failures={post_result.failure_summary()}",
                succeeded=False,
            )
            release_lock(events_client, normalised_payload.employee_id, event_id)
            update_event_status(
                table_client= events_client,
                employee_id=  normalised_payload.employee_id,
                event_id=     event_id,
                status=       EventStatus.FAILED,
                failure_step= "PostProvisionValidation",
            )

            next_event = release_next_queued_event(
                table_client=       events_client,
                employee_id=        normalised_payload.employee_id,
                predecessor_status= EventStatus.FAILED,
            )
            if next_event:
                logger.warning(
                    f"Next queued event held for review — employee={normalised_payload.employee_id}, "
                    f"event_id={next_event.event_id} — predecessor failed post-provision validation"
                )

            all_reports.append(report)
            _write_report(report, output_dir, result)
            result.total  += 1
            result.failed += 1
            continue

        for warning in post_result.warning_summary():
            report.add_warning(warning)

        # Step 10 — Provisioning succeeded
        release_lock(events_client, normalised_payload.employee_id, event_id)
        update_event_status(
            table_client=events_client,
            employee_id= normalised_payload.employee_id,
            event_id=    event_id,
            status=      EventStatus.COMPLETED,
        )

        next_event = release_next_queued_event(
            table_client=       events_client,
            employee_id=        normalised_payload.employee_id,
            predecessor_status= EventStatus.COMPLETED,
        )
        if next_event:
            logger.info(
                f"Next queued event auto-released — employee={normalised_payload.employee_id}, "
                f"event_id={next_event.event_id}, action={next_event.action}"
            )

        all_reports.append(report)
        _write_report(report, output_dir, result)
        result.total     += 1
        result.succeeded += 1

    logger.info(
        "Pipeline complete — total=%d, succeeded=%d, held=%d, failed=%d",
        result.total,
        result.succeeded,
        result.held,
        result.failed,
    )

    write_run_summary(
        reports=       all_reports,
        output_dir=    output_dir,
        trigger_type=  "local",
        correlation_id=correlation_id,
    )

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


def main(req):
    """
    Azure Functions HTTP trigger entry point.

    The azure.functions import lives inside this function so the module
    loads cleanly in any Python environment without the SDK installed.
    """
    try:
        import azure.functions as func
    except ImportError:
        raise RuntimeError(
            "azure.functions is not installed. "
            "Use run_pipeline() directly or scripts/run_local.py for local runs."
        )

    correlation_id = req.headers.get("x-ms-client-request-id", "unknown")
    lookup_path    = os.environ.get(
        "LOCAL_LOOKUP_PATH", "/tmp/jml_config/canonical_lookup.json"
    )
    output_dir = os.environ.get("LOCAL_REPORT_DIR", "/tmp/jml_reports")

    content_type = req.headers.get("Content-Type", "")
    try:
        csv_path = _extract_csv_path(req, content_type)
    except ValueError as exc:
        return func.HttpResponse(
            body=       json.dumps({"error": str(exc)}),
            status_code=400,
            mimetype=   "application/json",
        )

    pipeline_result = run_pipeline(
        csv_path=      csv_path,
        lookup_path=   lookup_path,
        output_dir=    output_dir,
        correlation_id=correlation_id,
    )

    return func.HttpResponse(
        body=       json.dumps(pipeline_result.to_dict()),
        status_code=200,
        mimetype=   "application/json",
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