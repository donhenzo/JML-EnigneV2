"""
Last-State Store — Azure Table Storage

Responsibility: persist and retrieve the last successfully reconciled HR state
for each employee. This is the single source of truth the last-state deriver
reads from to classify webhooks as Joiner / Mover / Leaver / Skip.

Semantic contract:
    "Last state" means the last HR snapshot that was SUCCESSFULLY processed
    by the JML pipeline — not the last webhook received. A webhook that
    dispatched an orchestrator but failed mid-pipeline does NOT update the
    last state. Only a confirmed success writes here.

Table layout — JmlLastState:
    PartitionKey  = employee_id (HR-meaningful, e.g. "Acc001")
    RowKey        = "current"   (one snapshot per employee, not history)

Stored fields mirror the mapped HR record so the deriver can compare
field-by-field without calling BambooHR or Entra again.

Leaver handling:
    When an employee is terminated, their row is updated with
    status="Inactive" and last_action="Leaver". The row is NOT deleted.
    This enables clean rehire detection — a new webhook for a previously
    terminated employee will see the old row and derive "Joiner" (rehire).

Environment:
    JML_STORAGE_CONNECTION_STRING — Azure Storage connection string
    JML_LAST_STATE_TABLE          — table name (default: JmlLastState)
"""

import logging
import os
from datetime import datetime, timezone

from azure.data.tables import TableClient, TableServiceClient, UpdateMode
from azure.core.exceptions import ResourceNotFoundError

logger = logging.getLogger(__name__)

_DEFAULT_TABLE_NAME = "JmlLastState"
_ROW_KEY = "current"


def get_last_state_table_client(connection_string: str = "") -> TableClient:
    """
    Build an Azure Table client for the JmlLastState table.

    Creates the table if it does not exist — safe to call on cold start.

    Args:
        connection_string: Azure Storage connection string.
                           Falls back to JML_STORAGE_CONNECTION_STRING env var.

    Returns:
        TableClient bound to the JmlLastState table.
    """
    if not connection_string:
        connection_string = os.environ.get("JML_STORAGE_CONNECTION_STRING", "")

    if not connection_string:
        raise EnvironmentError(
            "JML_STORAGE_CONNECTION_STRING is required for the last-state store."
        )

    table_name = os.environ.get("JML_LAST_STATE_TABLE", _DEFAULT_TABLE_NAME)

    service = TableServiceClient.from_connection_string(connection_string)
    service.create_table_if_not_exists(table_name)

    logger.info("Last-state store connected — table: %s", table_name)
    return service.get_table_client(table_name)


def get_last_state(table_client: TableClient, employee_id: str) -> dict | None:
    """
    Retrieve the last reconciled state for an employee.

    Args:
        table_client: TableClient from get_last_state_table_client()
        employee_id:  HR-meaningful employee ID (PartitionKey)

    Returns:
        Dict of stored fields if the employee has a last state, or None if
        no record exists (i.e. first time we've seen this employee).
    """
    try:
        entity = table_client.get_entity(
            partition_key=employee_id,
            row_key=_ROW_KEY,
        )
    except ResourceNotFoundError:
        return None

    # Strip Azure Table metadata keys — return only our business fields
    return {
        k: v for k, v in entity.items()
        if not k.startswith("odata.") and k not in ("PartitionKey", "RowKey", "Timestamp")
    }


def save_last_state(table_client: TableClient, employee_id: str, mapped_record: dict) -> None:
    """
    Write (upsert) the last reconciled state for an employee.

    Call this ONLY after the pipeline has successfully completed for this
    employee. Never on dispatch — only on confirmed success.

    The mapped_record should come from bamboohr_mapper.map_to_raw_identity()
    with the action already set by the deriver. We store a flattened copy
    of the fields the deriver needs, plus metadata.

    Args:
        table_client:   TableClient from get_last_state_table_client()
        employee_id:    HR-meaningful employee ID (PartitionKey)
        mapped_record:  raw identity dict from the mapper + action
    """
    entity = {
        "PartitionKey": employee_id,
        "RowKey": _ROW_KEY,

        # Identity fields — stored for diffing on next webhook
        "upn":             mapped_record.get("upn", ""),
        "display_name":    mapped_record.get("display_name", ""),
        "department":      mapped_record.get("department", ""),
        "job_title":       mapped_record.get("job_title", ""),
        "employment_type": mapped_record.get("employment_type", ""),
        "manager_id":      mapped_record.get("manager_id", ""),
        "location":        mapped_record.get("location", ""),
        "start_date":      mapped_record.get("start_date", ""),
        "termination_date": mapped_record.get("termination_date", ""),

        # Status and action metadata
        "status":          mapped_record.get("status", ""),
        "last_action":     mapped_record.get("action", ""),
        "last_reconciled": datetime.now(timezone.utc).isoformat(),
    }

    table_client.upsert_entity(entity, mode=UpdateMode.REPLACE)

    logger.info(
        "Last state saved — employee=%s, action=%s, status=%s",
        employee_id,
        entity["last_action"],
        entity["status"],
    )


def mark_terminated(table_client: TableClient, employee_id: str, mapped_record: dict) -> None:
    """
    Update the last state to reflect a successful Leaver reconciliation.

    This is a convenience wrapper around save_last_state that ensures the
    status is explicitly set to "Inactive" and last_action to "Leaver",
    even if the mapped record has ambiguous values.

    The row is NOT deleted — it stays so that a future webhook for a
    rehired employee sees the old terminated state and derives "Joiner".

    Args:
        table_client:   TableClient from get_last_state_table_client()
        employee_id:    HR-meaningful employee ID
        mapped_record:  raw identity dict from the mapper
    """
    record = dict(mapped_record)
    record["status"] = "Inactive"
    record["action"] = "Leaver"
    save_last_state(table_client, employee_id, record)

    logger.info("Last state marked terminated — employee=%s", employee_id)
