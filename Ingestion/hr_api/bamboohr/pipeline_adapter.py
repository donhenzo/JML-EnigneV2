"""
Pipeline Adapter — HR API to JML Pipeline Bridge

Responsibility: accept a single raw identity dict from the HR API
ingestion layer and run it through the same pipeline logic that
CSV records go through.

This exists because run_pipeline() in __init__.py is coupled to CSV
input — it reads a file, parses rows, and iterates. The HR API
integration needs to feed individual records into the same downstream
logic without touching the CSV path.

This adapter:
    1. Constructs an IdentityPayload from the raw mapped dict
    2. Routes by action:
       - Leaver → skip normalisation (ADR-014), hand off to run_leaver_pipeline
       - Mover  → normalise, hand off to run_mover_pipeline
       - Joiner → normalise, run through inline provisioning steps

It does NOT duplicate pipeline logic. It reuses the same normaliser,
hold queue, event store, validation gate, and provisioner that
run_pipeline() uses. The only difference is the input source.
"""

import json
import logging
import os
import uuid
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
    EventStatus,
)
from Functions.Event_store.conflict_queue import (
    check_and_handle_conflict,
    ConflictOutcome,
)
from Validation.validation_gate import pre_provision_validate, post_provision_validate
from Provisioning.provisioner import provision_joiner
from Provisioning.graph_client import JmlGraphClient
from Functions.mover_http import run_mover_pipeline
from Functions.leaver_http import run_leaver_pipeline
from azure.data.tables import TableServiceClient

logger = logging.getLogger(__name__)


class PipelineContext:
    """
    Shared resources initialised once and reused across multiple
    run_single_record() calls in the same session.

    Avoids rebuilding the normaliser, loading mapping rules, and
    reconnecting to Azure Table Storage on every record.
    """

    def __init__(
        self,
        graph_client: JmlGraphClient,
        connection_string: str = "",
        lookup_path: str = "config/canonical_lookup.json",
        mapping_rules_path: str = "",
        output_dir: str = "reports",
        correlation_id: str = "api",
    ):
        self.graph_client = graph_client
        self.output_dir = output_dir
        self.correlation_id = correlation_id

        if not connection_string:
            connection_string = os.environ.get("JML_STORAGE_CONNECTION_STRING", "")

        if not mapping_rules_path:
            mapping_rules_path = os.environ.get(
                "JML_MAPPING_RULES_PATH", "config/role_mapping_rules.json"
            )

        # Shared infrastructure — built once
        self.connection_string = connection_string
        self.hold_queue_client = get_hold_queue_table_client(connection_string)
        self.hold_queue = HoldQueueManager(
            AzureTableHoldQueueStore(self.hold_queue_client)
        )
        self.events_client = get_events_table_client(connection_string)
        self.lookup = load_lookup_table(lookup_path)
        self.normalizer = Normalizer(self.lookup)

        try:
            self.mapping_rules = load_mapping_rules(mapping_rules_path)
        except Exception as e:
            logger.error("Failed to load mapping rules: %s", e)
            self.mapping_rules = []


def run_single_record(mapped: dict, ctx: PipelineContext) -> bool:
    """
    Process a single HR API record through the full JML pipeline.

    This is the per-record equivalent of what run_pipeline() does
    for each CSV row. Same normalisation, same validation gates,
    same provisioning, same audit trail.

    Args:
        mapped: raw identity dict from bamboohr_mapper.map_to_raw_identity()
                Must include 'action' set by the action deriver.
        ctx:    PipelineContext with shared resources

    Returns:
        True if the record was processed successfully (provisioned or
        idempotently skipped). False if it was held or failed.
    """
    employee_id = mapped.get("employee_id", "unknown")
    upn = mapped.get("upn", "unknown")
    action_str = mapped.get("action", "")

    # Step 1 — Resolve raw employment type to canonical value before
    # constructing the payload. BambooHR sends "Full-Time", the enum
    # expects "Employee". The canonical lookup handles this translation.
    raw_emp_type = mapped.get("employment_type", "")
    emp_type_lookup = ctx.lookup.get("employment_type", {})
    resolved_emp_type = emp_type_lookup.get(raw_emp_type.lower(), raw_emp_type)

    # Parse start_date string to a date object — IdentityPayload expects date, not str.
    # For Leaver events, use termination_date instead of hireDate — the event
    # ID hash (EmployeeId + Action + StartDate) needs the actual termination
    # date so a re-hire after a Leaver produces a distinct event, and so the
    # Leaver event ID doesn't collide with the original Joiner's.
    from datetime import date as date_type

    if action_str == "Leaver" and mapped.get("termination_date"):
        start_date_str = mapped["termination_date"]
    else:
        start_date_str = mapped.get("start_date", "")
    try:
        start_date = date_type.fromisoformat(start_date_str) if start_date_str else date_type.today()
    except ValueError:
        logger.warning(
            "Invalid start_date '%s' for employee %s — using today",
            start_date_str, employee_id
        )
        start_date = date_type.today()

    # For Leaver records, employment_type may not parse cleanly from
    # a terminated HR record — fall back to EMPLOYEE rather than
    # failing the payload construction over a field the Leaver
    # pipeline never reads (ADR-014).
    try:
        employment_type = EmploymentType(resolved_emp_type)
    except ValueError:
        if action_str == "Leaver":
            employment_type = EmploymentType.EMPLOYEE
        else:
            # Non-Leaver: let the original error surface
            logger.error(
                "Invalid employment type '%s' for employee %s",
                resolved_emp_type, employee_id
            )
            report = DecisionReport(
                upn=upn,
                employee_id=employee_id,
                event=ReportEvent.JOINER,
                correlation_id=ctx.correlation_id,
                normalization_status=NormalizationStatus.FAILED,
                validation_status=ValidationStatus.SKIPPED,
            )
            report.add_hold_reason(f"Invalid employment type: {resolved_emp_type}")
            _write_report(report, ctx.output_dir)
            return False

    try:
        payload = IdentityPayload(
            employee_id=employee_id,
            upn=upn,
            display_name=mapped.get("display_name", ""),
            department=mapped.get("department", "") if action_str != "Leaver" else None,
            job_title=mapped.get("job_title", "") if action_str != "Leaver" else None,
            manager_id=mapped.get("manager_id") or None,
            start_date=start_date,
            employment_type=employment_type,
            location=mapped.get("location") or None,
            action=JmlAction(action_str),
            retain_roles=mapped.get("retain_roles", False),
            retain_list=mapped.get("retain_list", []),
        )
    except ValueError as exc:
        logger.error(
            "Invalid field value for employee %s: %s", employee_id, exc
        )
        report = DecisionReport(
            upn=upn,
            employee_id=employee_id,
            event=ReportEvent.JOINER,
            correlation_id=ctx.correlation_id,
            normalization_status=NormalizationStatus.FAILED,
            validation_status=ValidationStatus.SKIPPED,
        )
        report.add_hold_reason(f"Invalid field value: {exc}")
        _write_report(report, ctx.output_dir)
        return False

    # Step 2 — Route by action
    #
    # Leaver routes BEFORE normalisation — the Leaver pipeline has no
    # entitlement resolution (ADR-014), so it never reads department
    # or job_title. Running those through the normaliser would only
    # risk holding the record on fields that don't matter. This
    # matches what run_leaver_csv_mode does in run_local.py.
    #
    # Mover and Joiner normalise first, then route.

    if payload.action == JmlAction.LEAVER:
        return _run_leaver_record(payload, ctx)

    # Step 3 — Normalise (Joiner and Mover only)
    report = DecisionReport(
        upn=upn,
        employee_id=employee_id,
        event=ReportEvent.JOINER,
        correlation_id=ctx.correlation_id,
    )

    norm_result = ctx.normalizer.normalize(payload)

    if not norm_result.passed:
        report.normalization_status = NormalizationStatus.FAILED
        report.validation_status = ValidationStatus.SKIPPED

        for reason in norm_result.failures:
            report.add_hold_reason(reason)

        hold_record = ctx.hold_queue.create_from_normalization_failure(
            payload=norm_result.payload,
            reasons=norm_result.failures,
        )
        report.hold_record_id = hold_record.record_id
        _write_report(report, ctx.output_dir)
        return False

    report.normalization_status = NormalizationStatus.PASSED
    report.add_action(
        action="NormalizationPassed",
        detail=(
            f"department={norm_result.payload.department}, "
            f"job_title={norm_result.payload.job_title}"
        ),
    )
    normalised_payload = norm_result.payload

    # Step 4 — Route Mover or Joiner
    if normalised_payload.action == JmlAction.MOVER:
        return _run_mover_record(normalised_payload, ctx)

    return _run_joiner_record(normalised_payload, report, ctx)


def _run_leaver_record(
    payload: IdentityPayload,
    ctx: PipelineContext,
) -> bool:
    """
    Route a Leaver record to run_leaver_pipeline.

    Builds the TableServiceClient from the stored connection string and
    delegates to run_leaver_pipeline(). No normalisation needed —
    ADR-014 removed entitlement resolution for the Leaver, so
    department/job_title are irrelevant and are set to None on the
    payload before it reaches here.

    Returns True on OFFBOARD_SUCCESS, False on any other status.
    """
    table_client = TableServiceClient.from_connection_string(
        ctx.connection_string
    )

    result = run_leaver_pipeline(
        payload      = payload,
        table_client = table_client,
        graph_client = ctx.graph_client,
    )

    final_status = result.get("final_status", "OFFBOARD_FAILED")

    logger.info(
        "Leaver pipeline result — employee=%s, status=%s",
        payload.employee_id, final_status,
    )

    return final_status == "OFFBOARD_SUCCESS"


def _run_mover_record(
    payload: IdentityPayload,
    ctx: PipelineContext,
) -> bool:
    """
    Route a Mover record to run_mover_pipeline.

    Builds the TableServiceClient from the stored connection string and
    delegates to run_mover_pipeline(). The EventId is generated inside
    the pipeline — not here. The ingestion layer does not own event IDs.

    Returns True on MOVE_SUCCESS, False on any other status.
    """
    table_client = TableServiceClient.from_connection_string(
        ctx.connection_string
    )

    result = run_mover_pipeline(
        payload      = payload,
        table_client = table_client,
        graph_client = ctx.graph_client,
    )

    final_status = result.get("final_status", "MOVE_FAILED")

    logger.info(
        "Mover pipeline result — employee=%s, status=%s",
        payload.employee_id, final_status,
    )

    return final_status == "MOVE_SUCCESS"


def _run_joiner_record(
    normalised_payload: IdentityPayload,
    report: DecisionReport,
    ctx: PipelineContext,
) -> bool:
    """
    Run a Joiner record through the existing provisioning path.

    This is the original run_single_record() logic from Step 3 onwards,
    extracted into its own function so the Mover routing branch is clean.
    """
    employee_id  = normalised_payload.employee_id
    event_id_str = ""

    # Step 3 — Claim event (idempotency guard)
    payload_json = json.dumps({
        "employee_id": normalised_payload.employee_id,
        "upn":         normalised_payload.upn,
        "action":      normalised_payload.action.value,
        "start_date":  normalised_payload.start_date.isoformat(),
    })

    claimed = claim_event(
        table_client   = ctx.events_client,
        employee_id    = normalised_payload.employee_id,
        action         = normalised_payload.action.value,
        start_date     = normalised_payload.start_date.isoformat(),
        payload_json   = payload_json,
        correlation_id = ctx.correlation_id,
    )

    if not claimed:
        logger.info(
            "Duplicate event — skipping — employee=%s", employee_id
        )
        report.add_action(
            action    = "DuplicateEventSkipped",
            detail    = "Event already exists in event store — idempotency exit",
            succeeded = True,
        )
        _write_report(report, ctx.output_dir)
        return True

    event_id_str = generate_event_id(
        normalised_payload.employee_id,
        normalised_payload.action.value,
        normalised_payload.start_date.isoformat(),
    )

    # Step 4 — Conflict check
    conflict_outcome = check_and_handle_conflict(
        table_client = ctx.events_client,
        employee_id  = normalised_payload.employee_id,
        new_event_id = event_id_str,
        new_action   = normalised_payload.action.value,
    )

    if conflict_outcome == ConflictOutcome.QUEUED:
        report.add_action(
            action    = "EventQueued",
            detail    = "Active event in progress — queued behind existing event",
            succeeded = True,
        )
        report.add_warning(
            "Event queued — will process after active event completes"
        )
        _write_report(report, ctx.output_dir)
        return True

    # Step 5 — Resolve entitlements
    entitlements = resolve_entitlements(
        rules           = mapping_rules_from_ctx(ctx),
        department      = normalised_payload.department,
        job_title       = normalised_payload.job_title,
        employment_type = normalised_payload.employment_type.value,
        employee_id     = normalised_payload.employee_id,
    )

    report.add_action(
        action    = "EntitlementsResolved",
        detail    = (
            f"matched_rules={entitlements.matched_rule_ids}, "
            f"groups={entitlements.groups}, "
            f"rbac_roles={len(entitlements.rbac_roles)}"
        ),
        succeeded = True,
    )

    if not entitlements.matched_rule_ids:
        report.add_warning(
            "No mapping rules matched — user will have no group or RBAC assignments"
        )

    # Step 6 — Pre-provision validation gate
    validation_result = pre_provision_validate(normalised_payload)

    if not validation_result.passed:
        report.validation_status = ValidationStatus.FAILED

        for failure in validation_result.failure_summary():
            report.add_hold_reason(failure)

        hold_record = ctx.hold_queue.create_from_validation_failure(
            payload = normalised_payload,
            reasons = validation_result.failure_summary(),
        )
        report.hold_record_id = hold_record.record_id

        update_event_status(
            table_client = ctx.events_client,
            employee_id  = normalised_payload.employee_id,
            event_id     = event_id_str,
            status       = EventStatus.FAILED,
            failure_step = "PreProvisionValidation",
        )
        _write_report(report, ctx.output_dir)
        return False

    report.validation_status = ValidationStatus.PASSED
    for warning in validation_result.warning_summary():
        report.add_warning(warning)

    # Step 7 — Guard: Graph client required
    if ctx.graph_client is None:
        report.add_action(
            action    = "ProvisioningSkipped",
            detail    = "Graph client not available — check credentials",
            succeeded = False,
        )
        update_event_status(
            table_client = ctx.events_client,
            employee_id  = normalised_payload.employee_id,
            event_id     = event_id_str,
            status       = EventStatus.FAILED,
            failure_step = "GraphClientUnavailable",
        )
        _write_report(report, ctx.output_dir)
        return False

    # Step 8 — Acquire lock and provision
    instance_id = str(uuid.uuid4())
    acquire_lock(
        ctx.events_client, normalised_payload.employee_id,
        event_id_str, instance_id,
    )

    provisioning_result = provision_joiner(
        payload      = normalised_payload,
        entitlements = entitlements,
        report       = report,
        graph_client = ctx.graph_client,
        event_status = EventStatus.PROCESSING,
    )

    if not provisioning_result.succeeded:
        release_lock(
            ctx.events_client, normalised_payload.employee_id, event_id_str
        )
        update_event_status(
            table_client = ctx.events_client,
            employee_id  = normalised_payload.employee_id,
            event_id     = event_id_str,
            status       = EventStatus.FAILED,
            failure_step = provisioning_result.failure_step,
        )
        _write_report(report, ctx.output_dir)
        return False

    # Step 9 — Post-provision validation
    post_result = post_provision_validate(
        entra_object_id = provisioning_result.entra_id,
        employee_id     = normalised_payload.employee_id,
    )

    if not post_result.passed:
        report.validation_status = ValidationStatus.FAILED
        report.add_action(
            action    = "PostProvisionValidationFailed",
            detail    = f"failures={post_result.failure_summary()}",
            succeeded = False,
        )
        release_lock(
            ctx.events_client, normalised_payload.employee_id, event_id_str
        )
        update_event_status(
            table_client = ctx.events_client,
            employee_id  = normalised_payload.employee_id,
            event_id     = event_id_str,
            status       = EventStatus.FAILED,
            failure_step = "PostProvisionValidation",
        )
        _write_report(report, ctx.output_dir)
        return False

    for warning in post_result.warning_summary():
        report.add_warning(warning)

    # Step 10 — Success
    release_lock(
        ctx.events_client, normalised_payload.employee_id, event_id_str
    )
    update_event_status(
        table_client = ctx.events_client,
        employee_id  = normalised_payload.employee_id,
        event_id     = event_id_str,
        status       = EventStatus.COMPLETED,
    )

    _write_report(report, ctx.output_dir)

    logger.info(
        "Record processed successfully — employee=%s, upn=%s, action=%s",
        employee_id, normalised_payload.upn, normalised_payload.action.value,
    )
    return True


def mapping_rules_from_ctx(ctx: PipelineContext):
    """Return the mapping rules from the context."""
    return ctx.mapping_rules


def _write_report(report: DecisionReport, output_dir: str) -> None:
    """Write the audit report to disk."""
    try:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        write_report_to_file(report, output_dir)
    except Exception as e:
        logger.error("Failed to write audit report: %s", e)