# Functions/event_store/conflict_queue.py
#
# FIFO conflict queue for the JML event store.
# Handles ordering when a new event arrives for an identity that already
# has an active event in progress.
#
# Rules:
#   Active event exists       → new event queued (Status: Pending, QueuedAt set)
#   Previous event Completed  → next Pending event released automatically
#   Previous event Failed     → next Pending event held for human review
#   Leaver arrives            → all Pending events superseded, Leaver claims immediately
#
# This file handles ordering logic only.
# All persistence operations delegate to event_store.py.

import logging
from datetime import datetime, timezone

from azure.data.tables import TableClient, UpdateMode

from Functions.Event_store.event_store import (
    EventStatus,
    JmlEvent,
    _now_utc,
    update_event_status,
)

logger = logging.getLogger(__name__)


class ConflictOutcome:
    """
    String constants returned by check_and_handle_conflict().
    Using a class instead of an enum keeps the values plain strings,
    which makes logging and downstream string comparisons straightforward.
    """
    PROCEED   = "Proceed"    # No conflict, event can be proecessed
    QUEUED    = "Queued"     # Another event is active — this one is queued until the previous process clears 
    SUPERSEDE = "Supersede"  # Leaver arrives — it jumps the queue and cancels Pending events


def check_and_handle_conflict(
    table_client: TableClient,
    employee_id: str,
    new_event_id: str,
    new_action: str
) -> str:
    """
    Entry point for conflict detection. Called immediately after a new JML event
    is written to the store, before any provisioning work begins.

    The table_client is passed in from the pipeline — this function never
    constructs its own client, keeping storage concerns outside this layer.

    Decision tree:
        1. New action is Leaver  → supersede all Pending events, return Supersede
        2. Active events exist   → Queue the new event, return Queued
        3. No conflict           → return Proceed

    Returns a ConflictOutcome string. The caller uses this to decide whether
    to continue processing or exit early.
    """
    active_events = _get_active_events(table_client, employee_id, new_event_id)

    if new_action == "Leaver":
        # Leaver always takes priority. 
        if active_events:
            _supersede_pending_events(table_client, employee_id, active_events)
            logger.info(
                f"Leaver supersede — employee={employee_id}, "
                f"superseded {len(active_events)} pending event(s)"
            )
        return ConflictOutcome.SUPERSEDE

    if active_events:
        # An active event is already in flight.
        # queue the new event so it runs after the current one settles.
        _mark_as_queued(table_client, employee_id, new_event_id)
        logger.info(
            f"Event queued — employee={employee_id}, "
            f"event_id={new_event_id}, waiting behind "
            f"{len(active_events)} active event(s)"
        )
        return ConflictOutcome.QUEUED

    return ConflictOutcome.PROCEED


def release_next_queued_event(
    table_client: TableClient,
    employee_id: str,
    predecessor_status: str
) -> JmlEvent | None:
    """
    Called after an event finishes (Completed or Failed) to advance the queue.

    Behaviour depends on how the predecessor ended:
        Completed → release the oldest Pending event back to normal processing
        Failed    → hold the next event and flag it for manual review

    Holding on failure is intentional: if the predecessor left the identity
    in a partial state (e.g. group memberships half-assigned), blindly running
    the next event could make things worse.

    Returns the next JmlEvent if one was waiting, or None if the queue was empty.
    """
    next_event = _get_oldest_queued_event(table_client, employee_id)

    if not next_event:
        logger.info(f"No queued events found for employee={employee_id}")
        return None

    if predecessor_status == EventStatus.COMPLETED:
        table_client.update_entity(
            {
                "PartitionKey": employee_id,
                "RowKey":       next_event.event_id,
                "Status":       EventStatus.PENDING,
                "QueuedAt":     None,          # Clear the queue timestamp — event is active again
                "LastUpdated":  _now_utc(),
            },
            mode=UpdateMode.MERGE
        )
        logger.info(
            f"Queued event auto-released — employee={employee_id}, "
            f"event_id={next_event.event_id}"
        )

    elif predecessor_status == EventStatus.FAILED:
        table_client.update_entity(
            {
                "PartitionKey": employee_id,
                "RowKey":       next_event.event_id,
                "Status":       "Held",
                "HoldReason":   (
                    "Predecessor event failed. Identity may be in partial "
                    "state. Manual review required before processing."
                ),
                "LastUpdated":  _now_utc(),
            },
            mode=UpdateMode.MERGE
        )
        logger.warning(
            f"Queued event held — employee={employee_id}, "
            f"event_id={next_event.event_id} — predecessor failed"
        )

    return next_event



# Internal helpers
def _get_active_events(
    client,
    employee_id: str,
    exclude_event_id: str
) -> list[dict]:
    """
    Return all Pending or Processing events for this identity, excluding the
    newly arrived event itself (we don't want it to conflict with itself).

    Queries the full partition for the employee and filters in memory.
    Azure Table Storage doesn't support OR filters in a single query, so
    the status check happens after the fetch.
    """
    rows = client.query_entities(
        query_filter=f"PartitionKey eq '{employee_id}'"
    )
    active = []
    for row in rows:
        if row["RowKey"] == exclude_event_id:
            continue
        if row.get("Status") in (EventStatus.PENDING, EventStatus.PROCESSING):
            active.append(row)
    return active


def _get_oldest_queued_event(client, employee_id: str) -> JmlEvent | None:
    """
    Find the next event to release from the queue.

    A queued event is one that is Pending AND has a QueuedAt timestamp —
    the timestamp is what distinguishes "waiting in line" from "ready to run".
    Sorting by QueuedAt enforces strict FIFO ordering.

    Returns None if no queued events exist for this identity.
    """
    rows = client.query_entities(
        query_filter=f"PartitionKey eq '{employee_id}'"
    )
    queued = [
        row for row in rows
        if row.get("Status") == EventStatus.PENDING
        and row.get("QueuedAt")  # Must have a queue timestamp to be considered "waiting"
    ]
    if not queued:
        return None

    queued.sort(key=lambda r: r.get("QueuedAt", ""))  # Oldest first — strict FIFO
    row = queued[0]

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


def _mark_as_queued(client, employee_id: str, event_id: str) -> None:
    """
    Stamp a new event with QueuedAt to park it behind the active event.
    The QueuedAt timestamp is the tiebreaker used by _get_oldest_queued_event()
    to preserve arrival order when multiple events are waiting.
    """
    client.update_entity(
        {
            "PartitionKey": employee_id,
            "RowKey":       event_id,
            "QueuedAt":     _now_utc(),
            "LastUpdated":  _now_utc(),
        },
        mode=UpdateMode.MERGE
    )


def _supersede_pending_events(
    client,
    employee_id: str,
    active_events: list[dict]
) -> None:
    """
    Cancel all Pending events for an identity when a Leaver arrives.

    Only Pending events are superseded — a Processing event is already running
    and can't be safely interrupted here. That case is expected to be handled
    upstream by the pipeline orchestrator.
    """
    for row in active_events:
        if row.get("Status") == EventStatus.PENDING:
            client.update_entity(
                {
                    "PartitionKey": employee_id,
                    "RowKey":       row["RowKey"],
                    "Status":       EventStatus.SUPERSEDED,
                    "LastUpdated":  _now_utc(),
                },
                mode=UpdateMode.MERGE
            )
            logger.info(
                f"Event superseded by Leaver — employee={employee_id}, "
                f"event_id={row['RowKey']}"
            )