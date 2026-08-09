# Hold_queue/azure_table_hold_queue_store.py
#
# Azure Table Storage backend for the hold queue.
# Implements the HoldQueueStore protocol defined in queue_manager.py.
#
# Drop-in replacement for InMemoryHoldQueueStore.
# The HoldQueueManager and all state machine logic are untouched.
# Only the persistence layer changes.
#
# Table: JmlHoldQueue
#   PartitionKey — employee_id  (enables efficient list_by_employee queries)
#   RowKey       — record_id    (UUID, unique per hold record)

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from azure.data.tables import TableClient, UpdateMode
from azure.core.exceptions import ResourceNotFoundError

from Hold_queue.models import HoldRecord, HoldStatus

logger = logging.getLogger(__name__)

HOLD_QUEUE_TABLE = "JmlHoldQueue"



# get_hold_queue_table_client
# Single construction point — same pattern as event store.
# Replace body in Block G to switch to Managed Identity.

def get_hold_queue_table_client(connection_string: str) -> TableClient:
    """
    Construct and return a TableClient for JmlHoldQueue.

    This is the only place that knows about auth.
    Swap this function body in Block G for Managed Identity — nothing
    else in this module needs to change.
    """
    from azure.data.tables import TableServiceClient
    service = TableServiceClient.from_connection_string(connection_string)
    return service.get_table_client(HOLD_QUEUE_TABLE)



# AzureTableHoldQueueStore
# Implements HoldQueueStore protocol against Azure Table Storage.

class AzureTableHoldQueueStore:
    """
    Azure Table Storage hold queue backend.

    Purpose:
        Persist hold records across function invocations so held records
        survive between runs and can be reviewed and released independently.

    Inputs:
        table_client — pre-constructed TableClient for JmlHoldQueue.
                       Build once at pipeline entry point, pass in here.

    Side effects:
        Reads and writes to Azure Table Storage on every operation.

    Security:
        No credentials stored in this class. Auth is handled by the
        TableClient passed in at construction time.
    """

    def __init__(self, table_client: TableClient) -> None:
        self._client = table_client

    def save(self, record: HoldRecord) -> None:
        """
        Persist a hold record to Azure Table Storage.

        Creates a new row if the record is new.
        Updates the existing row if the record already exists.
        Called on every state transition by HoldQueueManager.
        """
        entity = _record_to_entity(record)

        try:
            self._client.upsert_entity(entity, mode=UpdateMode.REPLACE)
            logger.debug(
                f"Hold record saved — record_id={record.record_id}, "
                f"employee={record.employee_id}, status={record.status.value}"
            )
        except Exception as e:
            logger.error(
                f"Failed to save hold record {record.record_id}: {e}"
            )
            raise

    def get(self, record_id: str) -> Optional[HoldRecord]:
        """
        Retrieve a hold record by record_id.

        Returns None if the record does not exist.

        Note: requires employee_id to form the PartitionKey. Because the
        protocol only provides record_id, this method scans all partitions.
        For a targeted lookup, use get_by_employee_and_id() directly.
        """
        # Scan all rows matching this RowKey across all partitions.
        # In practice hold records are always accessed via employee context
        # so this scan is rare — list_by_employee is the primary access path.
        try:
            rows = self._client.query_entities(
                query_filter=f"RowKey eq '{record_id}'"
            )
            for row in rows:
                return _entity_to_record(row)
            return None
        except Exception as e:
            logger.error(f"Failed to get hold record {record_id}: {e}")
            raise

    def list_by_status(self, status: HoldStatus) -> list[HoldRecord]:
        """
        Return all hold records in the given status.

        Used by HoldQueueManager.get_held_records() and get_failed_records().
        Scans all partitions — acceptable because this is an operator query,
        not a per-event hot path.
        """
        try:
            rows = self._client.query_entities(
                query_filter=f"Status eq '{status.value}'"
            )
            return [_entity_to_record(row) for row in rows]
        except Exception as e:
            logger.error(f"Failed to list hold records by status {status}: {e}")
            raise

    def list_by_employee(self, employee_id: str) -> list[HoldRecord]:
        """
        Return all hold records for a given employee.

        Uses PartitionKey filter — efficient single-partition query.
        """
        try:
            rows = self._client.query_entities(
                query_filter=f"PartitionKey eq '{employee_id}'"
            )
            return [_entity_to_record(row) for row in rows]
        except Exception as e:
            logger.error(
                f"Failed to list hold records for employee {employee_id}: {e}"
            )
            raise



# Serialization helpers
# HoldRecord ↔ Azure Table Storage entity.

def _record_to_entity(record: HoldRecord) -> dict:
    """
    Convert a HoldRecord to an Azure Table Storage entity dict.

    PartitionKey = employee_id — groups all records for one person together.
    RowKey       = record_id   — unique identifier per hold record.

    Lists (failure_reasons, retain_list) are serialised to JSON strings
    because Azure Table Storage does not support array types.
    Datetimes are stored as ISO 8601 strings for the same reason.
    """
    return {
        "PartitionKey":      record.employee_id,
        "RowKey":            record.record_id,
        "EmployeeId":        record.employee_id,
        "UPN":               record.upn or "",
        "Status":            record.status.value,
        "FailureReasons":    json.dumps(record.failure_reasons),
        "PayloadSnapshot":   record.payload_snapshot or "",
        "ManualOverride":    record.manual_override,
        "OverrideNote":      record.override_note or "",
        "RetryCount":        record.retry_count,
        "LastAttempt":       (
            record.last_attempt.isoformat()
            if record.last_attempt else ""
        ),
        "LastUpdated":       (
            record.last_updated.isoformat()
            if record.last_updated else ""
        ),
    }


def _entity_to_record(entity: dict) -> HoldRecord:
    """
    Convert an Azure Table Storage entity dict back to a HoldRecord.

    Reverses _record_to_entity. Handles empty strings for optional fields
    and deserialises JSON strings back to lists.
    """
    last_attempt_raw = entity.get("LastAttempt", "")
    last_updated_raw = entity.get("LastUpdated", "")

    return HoldRecord(
        record_id=       entity["RowKey"],
        employee_id=     entity.get("EmployeeId", entity["PartitionKey"]),
        upn=             entity.get("UPN", ""),
        status=          HoldStatus(entity["Status"]),
        failure_reasons= json.loads(entity.get("FailureReasons", "[]")),
        payload_snapshot=entity.get("PayloadSnapshot") or None,
        manual_override= bool(entity.get("ManualOverride", False)),
        override_note=   entity.get("OverrideNote") or None,
        retry_count=     int(entity.get("RetryCount", 0)),
        last_attempt=    (
            datetime.fromisoformat(last_attempt_raw)
            if last_attempt_raw else None
        ),
        last_updated=    (
            datetime.fromisoformat(last_updated_raw)
            if last_updated_raw else datetime.now(timezone.utc)
        ),
    )