"""
Ingestion Coordinator

Responsibility: wire the HR API fetcher, mapper, action deriver, and
JML pipeline together. This is the single entry point for all HR-driven
ingestion modes.

Three modes, one atomic unit:
    run_single(bamboohr_id)  → fetch one employee, derive action, run pipeline
    run_delta()              → poll for changes since last checkpoint, run each
    run_bulk(bamboohr_ids)   → process a confirmed list, run each

Every mode ultimately calls run_single(). The other two are just different
ways of deciding which employee IDs to process.

Idempotency is NOT enforced here — it is enforced downstream in the event
store via claim_event(). This module's job is to get the right records to
the pipeline in the right shape. The pipeline handles the rest.

Separation of concerns:
    bamboohr_client.py        → knows BambooHR's API
    bamboohr_mapper.py        → translates field names
    action_deriver.py         → determines Joiner / Mover / Skip
    system_state.py           → tracks last poll timestamp
    pipeline_adapter.py       → bridges HR API records to the existing pipeline
    ingestion_coordinator.py  → orchestrates all of the above (this file)
"""

import logging
from dataclasses import dataclass, field

from Ingestion.hr_api.bamboohr.bamboohr_client import (
    get_employee,
    get_changed_employees,
    resolve_bamboohr_id,
)
from Ingestion.hr_api.bamboohr.bamboohr_mapper import map_to_raw_identity
from Ingestion.hr_api.action_deriver import derive_action, ACTION_SKIP
from Ingestion.hr_api.system_state import (
    get_poll_checkpoint,
    mark_poll_started,
    save_poll_checkpoint,
    RunStatus,
)
from Ingestion.hr_api.bamboohr.pipeline_adapter import PipelineContext, run_single_record


logger = logging.getLogger(__name__)


@dataclass
class IngestionResult:
    """
    Summary of what the ingestion coordinator processed.
    Returned by all three modes so the caller knows what happened.
    """
    total_fetched: int = 0
    joiner_count: int = 0
    mover_count: int = 0
    skipped_count: int = 0
    failed_count: int = 0
    errors: list = field(default_factory=list)


def run_single(
    identifier: str,
    ctx: PipelineContext,
) -> dict | None:
    """
    Fetch one employee from BambooHR, derive the lifecycle action,
    and feed the record into the JML pipeline.

    This is the atomic unit. Every other mode calls this.

    Args:
        identifier: any of:
                     - Employee number (e.g. "Acc003")
                     - Work email / UPN (e.g. "sarah@contoso.com")
                     - BambooHR internal ID (e.g. "121")
        ctx:        PipelineContext with shared pipeline resources

    Returns:
        The mapped identity dict if processed, or None if skipped/failed.
    """
    # Resolve whatever the caller passed to a BambooHR internal ID
    try:
        bamboohr_id = resolve_bamboohr_id(identifier)
    except ValueError as e:
        logger.error("Could not resolve '%s': %s", identifier, e)
        return None

    # Fetch the raw employee record from BambooHR
    try:
        raw = get_employee(bamboohr_id)
    except Exception as e:
        logger.error(
            "Failed to fetch employee %s from BambooHR: %s",
            bamboohr_id, e
        )
        return None

    # Map BambooHR fields to the JML engine's raw identity shape
    mapped = map_to_raw_identity(raw)
    employee_id = mapped["employee_id"]

    # Determine what lifecycle event this record represents
    action = derive_action(mapped, ctx.graph_client)

    if action == ACTION_SKIP:
        logger.info(
            "Employee %s (%s) — no changes detected, skipping.",
            employee_id, mapped["upn"]
        )
        return None

    # Set the derived action on the mapped record before pipeline entry
    mapped["action"] = action

    logger.info(
        "Employee %s (%s) — action: %s — entering pipeline.",
        employee_id, mapped["upn"], action
    )

    # Hand off to the pipeline adapter
    success = run_single_record(mapped, ctx)

    # Stamp the outcome onto the mapped record so the caller
    # can distinguish provisioned from held/failed
    mapped["_pipeline_success"] = success

    if not success:
        logger.warning(
            "Pipeline returned failure for employee %s (%s)",
            employee_id, mapped["upn"]
        )

    return mapped


def run_delta(
    ctx: PipelineContext,
    state_table_client,
) -> IngestionResult:
    """
    Poll BambooHR for all employees changed since the last checkpoint,
    derive actions, and process each through the pipeline.

    This is the scheduled/automatic mode. Safe to run repeatedly —
    the checkpoint only advances on success, and the event store
    deduplicates via claim_event().

    Args:
        ctx:                PipelineContext with shared pipeline resources
        state_table_client: Azure Table Storage client for JmlSystemState

    Returns:
        IngestionResult summarising what was processed.
    """
    result = IngestionResult()

    # Read the last successful poll timestamp
    checkpoint = get_poll_checkpoint(state_table_client)
    since = checkpoint.last_successful_poll
    logger.info("Delta poll starting — fetching changes since %s", since)

    # Mark the poll as running before any processing
    etag = mark_poll_started(state_table_client)

    # Fetch changed employees from BambooHR
    try:
        changed_records = get_changed_employees(since)
    except Exception as e:
        logger.error("Failed to fetch changed employees: %s", e)
        save_poll_checkpoint(
            state_table_client,
            poll_timestamp=since,
            last_event_id="",
            records_processed=0,
            status=RunStatus.FAILED,
            etag=etag
        )
        result.failed_count = 1
        result.errors.append(str(e))
        return result

    result.total_fetched = len(changed_records)

    if not changed_records:
        logger.info("No changed employees found since %s", since)
        save_poll_checkpoint(
            state_table_client,
            poll_timestamp=since,
            last_event_id=checkpoint.last_event_id,
            records_processed=0,
            status=RunStatus.SUCCESS,
            etag=etag
        )
        return result

    # Process each changed employee through run_single
    last_event_id = ""
    for record in changed_records:
        bamboohr_id = str(record.get("id", ""))
        if not bamboohr_id:
            logger.warning("Changed record with no ID — skipping.")
            result.skipped_count += 1
            continue

        processed = run_single(bamboohr_id, ctx)

        if processed is None:
            result.skipped_count += 1
        elif not processed.get("_pipeline_success", False):
            result.failed_count += 1
        elif processed.get("action") == "Joiner":
            result.joiner_count += 1
            last_event_id = processed.get("employee_id", "")
        elif processed.get("action") == "Mover":
            result.mover_count += 1
            last_event_id = processed.get("employee_id", "")

    # Checkpoint advances only if we got this far without crashing
    save_poll_checkpoint(
        state_table_client,
        poll_timestamp=_now_utc(),
        last_event_id=last_event_id,
        records_processed=result.total_fetched,
        status=RunStatus.SUCCESS,
        etag=etag
    )

    logger.info(
        "Delta poll complete — fetched: %d, joiners: %d, movers: %d, skipped: %d",
        result.total_fetched, result.joiner_count,
        result.mover_count, result.skipped_count
    )

    return result


def run_bulk(
    bamboohr_ids: list[str],
    ctx: PipelineContext,
    confirmed: bool = False,
) -> IngestionResult:
    """
    Process a specific list of BambooHR employee IDs through the pipeline.

    This is the manual/operator mode. Never called automatically.
    The confirmed flag MUST be True — without it, nothing runs.

    Args:
        bamboohr_ids: list of BambooHR internal numeric IDs to process
        ctx:          PipelineContext with shared pipeline resources
        confirmed:    must be True to proceed — safety guard

    Returns:
        IngestionResult summarising what was processed.
    """
    result = IngestionResult()
    result.total_fetched = len(bamboohr_ids)

    if not confirmed:
        logger.warning(
            "Bulk run called without confirmation — %d employees "
            "will NOT be processed. Pass confirmed=True to proceed.",
            len(bamboohr_ids)
        )
        return result

    logger.info(
        "Bulk run starting — %d employees to process.", len(bamboohr_ids)
    )

    for bamboohr_id in bamboohr_ids:
        try:
            processed = run_single(bamboohr_id, ctx)

            if processed is None:
                result.skipped_count += 1
            elif not processed.get("_pipeline_success", False):
                result.failed_count += 1
            elif processed.get("action") == "Joiner":
                result.joiner_count += 1
            elif processed.get("action") == "Mover":
                result.mover_count += 1

        except Exception as e:
            logger.error(
                "Bulk run — failed for ID %s: %s", bamboohr_id, e
            )
            result.failed_count += 1
            result.errors.append(f"{bamboohr_id}: {e}")

    logger.info(
        "Bulk run complete — total: %d, joiners: %d, movers: %d, "
        "skipped: %d, failed: %d",
        result.total_fetched, result.joiner_count,
        result.mover_count, result.skipped_count, result.failed_count
    )

    return result


def _now_utc() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()