"""
System State Store — Azure Table Storage

Responsibility: persist operational state for the JML ingestion layer.
This is the engine's memory of what it has done — not what happened to
identities (that's the event store), but what the engine itself did.

Primary use case: delta polling. The engine needs to know "when did I
last successfully poll the HR system?" so it can ask for changes since
that timestamp. Without this, every poll returns the full change history
and the engine relies solely on the event store for duplicate suppression.
With this, the poll window is narrowed to only what's new.

Design decisions:
    - Stored in Azure Table Storage, same account as the event store.
      Separate table (JmlSystemState) — different lifecycle, different
      access pattern, no coupling to event data.

    - Each state entry is a single row keyed by a logical name.
      The poll checkpoint is one row. Future checkpoints (e.g. per-HR-source
      sync cursors) are additional rows — no schema change required.

    - last_run_status tracks whether the poll completed or failed.
      On failure, the timestamp does NOT advance — the next poll re-processes
      the same window. The event store's claim_event() catches duplicates,
      so re-processing is safe.

    - etag is stored for optimistic concurrency. If two poll runs overlap
      (shouldn't happen, but might under crash recovery), the second write
      fails on etag mismatch rather than silently overwriting.

    - Corruption recovery: if the state row is missing or unreadable,
      the system falls back to a safe default (24 hours ago). This means
      a worst-case re-process of one day's changes, all deduplicated by
      the event store.

Table: JmlSystemState
    PartitionKey: "SYSTEM"
    RowKey:       logical name (e.g. "HR_POLL_CHECKPOINT")
"""

import logging
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass

from azure.data.tables import TableServiceClient, TableClient, UpdateMode
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

logger = logging.getLogger(__name__)

SYSTEM_STATE_TABLE = "JmlSystemState"
PARTITION_KEY = "SYSTEM"

# Checkpoint row keys — add new ones here as the system grows
POLL_CHECKPOINT_KEY = "HR_POLL_CHECKPOINT"


class RunStatus:
    SUCCESS = "Success"
    FAILED = "Failed"
    RUNNING = "Running"


@dataclass
class PollCheckpoint:
    """
    Represents the last known state of a delta poll run.

    last_successful_poll: ISO 8601 timestamp of the last poll that completed
                          without errors. The next poll uses this as the
                          'since' parameter.
    last_event_id:        event ID of the last event processed in that run.
                          Useful for debugging and replay verification.
    last_run_status:      Success / Failed / Running — tracks whether the
                          poll completed cleanly.
    etag:                 Azure Table Storage etag for optimistic concurrency.
    last_updated:         when this checkpoint row was last written.
    records_processed:    how many records the last run handled.
    """
    last_successful_poll: str
    last_event_id: str
    last_run_status: str
    etag: str
    last_updated: str
    records_processed: int


def get_system_state_table_client(connection_string: str) -> TableClient:
    """
    Construct and return a TableClient for JmlSystemState.

    Same auth pattern as the event store — connection string for local dev,
    swap to Managed Identity in production. Keeps auth decisions in one place.
    """
    service = TableServiceClient.from_connection_string(connection_string)
    return service.get_table_client(SYSTEM_STATE_TABLE)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_default_timestamp() -> str:
    """
    Fallback timestamp when no checkpoint exists.
    Returns 24 hours ago — worst case, the engine re-processes one day
    of changes. The event store deduplicates via claim_event().
    """
    return (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()


def get_poll_checkpoint(table_client: TableClient) -> PollCheckpoint:
    """
    Read the current poll checkpoint from Azure Table Storage.

    If the row does not exist (first run or corruption), returns a safe
    default with last_successful_poll set to 24 hours ago. The event store
    handles any duplicates from re-processing that window.

    If the row exists but is unreadable, logs a warning and returns the
    same safe default rather than crashing the pipeline.
    """
    try:
        row = table_client.get_entity(
            partition_key=PARTITION_KEY,
            row_key=POLL_CHECKPOINT_KEY
        )

        return PollCheckpoint(
            last_successful_poll=row.get("LastSuccessfulPoll", _safe_default_timestamp()),
            last_event_id=row.get("LastEventId", ""),
            last_run_status=row.get("LastRunStatus", ""),
            etag=row.metadata.get("etag", ""),
            last_updated=row.get("LastUpdated", ""),
            records_processed=int(row.get("RecordsProcessed", 0)),
        )

    except ResourceNotFoundError:
        logger.info(
            "No poll checkpoint found — first run or reset. "
            "Using safe default (24 hours ago)."
        )
        return PollCheckpoint(
            last_successful_poll=_safe_default_timestamp(),
            last_event_id="",
            last_run_status="",
            etag="",
            last_updated="",
            records_processed=0,
        )

    except Exception as e:
        logger.warning(
            "Failed to read poll checkpoint: %s — using safe default.", e
        )
        return PollCheckpoint(
            last_successful_poll=_safe_default_timestamp(),
            last_event_id="",
            last_run_status="",
            etag="",
            last_updated="",
            records_processed=0,
        )


def mark_poll_started(table_client: TableClient) -> str:
    """
    Mark the poll as running before processing begins.

    Creates the row if it doesn't exist, updates it if it does.
    Returns the etag of the written row — callers pass this to
    save_poll_checkpoint() for optimistic concurrency.

    If the pipeline crashes mid-run, the row shows Running with
    the old LastSuccessfulPoll timestamp. The next run sees this
    and re-processes from that timestamp safely.
    """
    entity = {
        "PartitionKey": PARTITION_KEY,
        "RowKey": POLL_CHECKPOINT_KEY,
        "LastRunStatus": RunStatus.RUNNING,
        "LastUpdated": _now_utc(),
    }

    try:
        # Try update first (row exists)
        result = table_client.update_entity(entity, mode=UpdateMode.MERGE)
        etag = result.get("etag", "")
    except ResourceNotFoundError:
        # First run — create the row
        result = table_client.create_entity(entity)
        etag = result.get("etag", "")

    logger.info("Poll marked as running.")
    return etag


def save_poll_checkpoint(
    table_client: TableClient,
    poll_timestamp: str,
    last_event_id: str,
    records_processed: int,
    status: str,
    etag: str = ""
) -> None:
    """
    Write the poll checkpoint after a run completes (success or failure).

    On success: LastSuccessfulPoll advances to poll_timestamp.
    On failure: LastSuccessfulPoll stays unchanged — the next run
                re-processes the same window.

    The etag parameter enables optimistic concurrency. If another process
    updated the checkpoint between mark_poll_started() and this call,
    the write fails rather than silently overwriting. Pass an empty string
    to skip the concurrency check (e.g. during initial setup).

    Args:
        table_client:      Azure Table Storage client
        poll_timestamp:     the 'since' value used for this poll run
        last_event_id:     event ID of the last record processed
        records_processed: total records handled in this run
        status:            RunStatus.SUCCESS or RunStatus.FAILED
        etag:              etag from mark_poll_started() for concurrency
    """
    # Only advance the timestamp on success — on failure, keep the old
    # value so the next run re-processes the same window.
    entity = {
        "PartitionKey": PARTITION_KEY,
        "RowKey": POLL_CHECKPOINT_KEY,
        "LastRunStatus": status,
        "LastEventId": last_event_id,
        "RecordsProcessed": records_processed,
        "LastUpdated": _now_utc(),
    }

    if status == RunStatus.SUCCESS:
        entity["LastSuccessfulPoll"] = poll_timestamp

    try:
        if etag:
            from azure.core import MatchConditions
            table_client.update_entity(
                entity,
                mode=UpdateMode.MERGE,
                etag=etag,
                match_condition=MatchConditions.IfNotModified
            )
        else:
            table_client.update_entity(entity, mode=UpdateMode.MERGE)

        logger.info(
            "Poll checkpoint saved — status=%s, records=%d, timestamp=%s",
            status, records_processed, poll_timestamp
        )

    except Exception as e:
        # Concurrency conflict or storage error. Log but do not crash —
        # the event store is the hard idempotency guard. A stale checkpoint
        # means the next poll re-processes some records, which is safe.
        logger.error("Failed to save poll checkpoint: %s", e)


def reset_poll_checkpoint(table_client: TableClient) -> None:
    """
    Delete the poll checkpoint row entirely.

    Used for manual recovery — forces the next poll to use the safe
    default (24 hours ago) and re-process from there.

    Safe because the event store deduplicates via claim_event().
    """
    try:
        table_client.delete_entity(
            partition_key=PARTITION_KEY,
            row_key=POLL_CHECKPOINT_KEY
        )
        logger.info("Poll checkpoint reset — next run will use safe default.")
    except ResourceNotFoundError:
        logger.info("Poll checkpoint already absent — nothing to reset.")