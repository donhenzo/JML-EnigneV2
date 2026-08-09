# Functions/event_store/event_store.py
#
# JML event store — Azure Table Storage backend.
# Provides idempotency and concurrency control for all pipeline runs.
#
# All functions accept a pre-constructed TableClient.
# Auth method (connection string vs Managed Identity) is resolved once
# at the pipeline entry point — never inside this module.

import hashlib
import logging
import uuid
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass

from azure.data.tables import TableServiceClient, TableClient, UpdateMode
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

logger = logging.getLogger(__name__)

STALE_LOCK_MINUTES = 10
EVENTS_TABLE       = "JmlEvents"


class EventStatus:
    PENDING    = "Pending"
    PROCESSING = "Processing"
    COMPLETED  = "Completed"
    FAILED     = "Failed"
    SUPERSEDED = "Superseded"


@dataclass
class JmlEvent:
    employee_id:    str
    event_id:       str
    status:         str
    action:         str
    correlation_id: str
    locked_at:      str | None
    locked_by:      str | None
    last_updated:   str
    retry_count:    int
    payload:        str



# get_events_table_client
# Single construction point for the TableClient.
# Dev: pass connection_string. Production (Block G): swap to Managed Identity.
# Nothing else in this module constructs a client.
def get_events_table_client(connection_string: str) -> TableClient:
    """
    Construct and return a TableClient for JmlEvents.

    This is the only place in the event store that knows about auth.
    Replace this function body in Block G to switch to Managed Identity
    without touching any other function.

    Dev usage:
        client = get_events_table_client(os.environ["JML_STORAGE_CONNECTION_STRING"])

    Production usage (Block G):
        client = get_events_table_client_mi(os.environ["JML_STORAGE_ACCOUNT_NAME"])
    """
    service = TableServiceClient.from_connection_string(connection_string)
    return service.get_table_client(EVENTS_TABLE)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_event_id(employee_id: str, action: str, start_date: str) -> str:
    """
    Generate a deterministic event ID from the identity event key fields.

    Same input always produces the same 32-character hex string.
    StartDate included so a re-hire after a Leaver produces a distinct event.
    """
    raw = f"{employee_id}|{action}|{start_date}".lower()
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def claim_event(
    table_client: TableClient,
    employee_id: str,
    action: str,
    start_date: str,
    payload_json: str,
    correlation_id: str = ""
) -> bool:
    """
    Claim an event slot before processing begins.

    Returns True if claimed, False if already exists (duplicate — exit cleanly).
    """
    event_id = generate_event_id(employee_id, action, start_date)

    entity = {
        "PartitionKey":  employee_id,
        "RowKey":        event_id,
        "Status":        EventStatus.PENDING,
        "Action":        action,
        "CorrelationId": correlation_id or str(uuid.uuid4()),
        "LockedAt":      None,
        "LockedBy":      None,
        "LastUpdated":   _now_utc(),
        "RetryCount":    0,
        "Payload":       payload_json,
    }

    try:
        table_client.create_entity(entity)
        logger.info(
            f"Event claimed — employee={employee_id}, action={action}, "
            f"event_id={event_id}"
        )
        return True
    except ResourceExistsError:
        logger.info(
            f"Duplicate event — employee={employee_id}, action={action}, "
            f"event_id={event_id}. Exiting cleanly."
        )
        return False


def get_event(
    table_client: TableClient,
    employee_id: str,
    event_id: str
) -> JmlEvent | None:
    """Read an event row. Returns None if not found."""
    try:
        row = table_client.get_entity(
            partition_key=employee_id,
            row_key=event_id
        )
        return JmlEvent(
            employee_id=    row["PartitionKey"],
            event_id=       row["RowKey"],
            status=         row["Status"],
            action=         row["Action"],
            correlation_id= row.get("CorrelationId", ""),
            locked_at=      row.get("LockedAt"),
            locked_by=      row.get("LockedBy"),
            last_updated=   row.get("LastUpdated", ""),
            retry_count=    int(row.get("RetryCount", 0)),
            payload=        row.get("Payload", ""),
        )
    except ResourceNotFoundError:
        return None


def update_event_status(
    table_client: TableClient,
    employee_id: str,
    event_id: str,
    status: str,
    failure_step: str = ""
) -> None:
    """Transition the Status field on an event row."""
    updates = {
        "PartitionKey": employee_id,
        "RowKey":       event_id,
        "Status":       status,
        "LastUpdated":  _now_utc(),
    }
    if failure_step:
        updates["FailureStep"] = failure_step

    table_client.update_entity(updates, mode=UpdateMode.MERGE)
    logger.info(
        f"Event status updated — employee={employee_id}, "
        f"event_id={event_id}, status={status}"
        + (f", failure_step={failure_step}" if failure_step else "")
    )


def acquire_lock(
    table_client: TableClient,
    employee_id: str,
    event_id: str,
    instance_id: str
) -> None:
    """Acquire a processing lock — prevents duplicate concurrent processing."""
    table_client.update_entity(
        {
            "PartitionKey": employee_id,
            "RowKey":       event_id,
            "Status":       EventStatus.PROCESSING,
            "LockedAt":     _now_utc(),
            "LockedBy":     instance_id,
            "LastUpdated":  _now_utc(),
        },
        mode=UpdateMode.MERGE
    )
    logger.info(
        f"Lock acquired — employee={employee_id}, "
        f"event_id={event_id}, instance={instance_id}"
    )


def release_lock(
    table_client: TableClient,
    employee_id: str,
    event_id: str
) -> None:
    """Release the processing lock on completion or failure."""
    table_client.update_entity(
        {
            "PartitionKey": employee_id,
            "RowKey":       event_id,
            "LockedAt":     None,
            "LockedBy":     None,
            "LastUpdated":  _now_utc(),
        },
        mode=UpdateMode.MERGE
    )
    logger.info(
        f"Lock released — employee={employee_id}, event_id={event_id}"
    )


def is_stale_lock(event: JmlEvent) -> bool:
    """Return True if the event lock is older than STALE_LOCK_MINUTES."""
    if not event.locked_at:
        return False
    try:
        locked_time = datetime.fromisoformat(event.locked_at)
        if locked_time.tzinfo is None:
            locked_time = locked_time.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - locked_time > timedelta(minutes=STALE_LOCK_MINUTES)
    except (ValueError, TypeError):
        return True


def reclaim_stale_event(
    table_client: TableClient,
    employee_id: str,
    event_id: str,
    current_retry_count: int
) -> None:
    """Reset a stale locked event to Pending for retry."""
    table_client.update_entity(
        {
            "PartitionKey": employee_id,
            "RowKey":       event_id,
            "Status":       EventStatus.PENDING,
            "LockedAt":     None,
            "LockedBy":     None,
            "RetryCount":   current_retry_count + 1,
            "LastUpdated":  _now_utc(),
        },
        mode=UpdateMode.MERGE
    )
    logger.warning(
        f"Stale lock reclaimed — employee={employee_id}, "
        f"event_id={event_id}, retry_count={current_retry_count + 1}"
    )


def check_active_event(
    table_client: TableClient,
    employee_id: str
) -> JmlEvent | None:
    """
    Query for any active (Processing or Pending) event for this EmployeeId.
    Reclaims stale locks automatically. Returns None if no active event exists.
    """
    rows = table_client.query_entities(
        query_filter=f"PartitionKey eq '{employee_id}'"
    )

    for row in rows:
        status = row.get("Status", "")
        if status not in (EventStatus.PENDING, EventStatus.PROCESSING):
            continue

        event = JmlEvent(
            employee_id=    row["PartitionKey"],
            event_id=       row["RowKey"],
            status=         status,
            action=         row["Action"],
            correlation_id= row.get("CorrelationId", ""),
            locked_at=      row.get("LockedAt"),
            locked_by=      row.get("LockedBy"),
            last_updated=   row.get("LastUpdated", ""),
            retry_count=    int(row.get("RetryCount", 0)),
            payload=        row.get("Payload", ""),
        )

        if status == EventStatus.PROCESSING and is_stale_lock(event):
            reclaim_stale_event(
                table_client, employee_id,
                event.event_id, event.retry_count
            )
            continue

        return event

    return None