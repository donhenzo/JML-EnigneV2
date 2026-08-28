"""
One-off cleanup for the two orphaned sync-Leaver runs (E900, TIMER_TEST_01).

Both 504'd mid-removal before Step 8, leaving:
  - JmlEvents:      lock orphaned, Status stuck at Processing (or already
                    reclaimed to Pending by _handle_existing_event / check_active_event)
  - LeaverEventLog: row stuck at IN_PROGRESS
  - LeaverAuditLog: no row written

This records both dead runs honestly:
  - JmlEvents      -> Failed, lock cleared, failure_step=Timeout504
  - LeaverEventLog -> OFFBOARD_FAILED

Uses the engine's own event-store functions so it respects the real schema.
Run once from the repo root with the venv active:  python reset_orphaned_leavers.py

Idempotent: safe to run more than once. If a row is already Pending
(auto-reclaimed), this still forces it to Failed so the dead run reads as dead.
"""

import os
from datetime import datetime, timezone

from azure.data.tables import TableServiceClient
from Functions.Event_store.event_store import (
    get_events_table_client,
    release_lock,
    update_event_status,
    get_event,
    EventStatus,
)

LEAVER_EVENT_LOG_TABLE = "LeaverEventLog"

# The two orphaned runs — (employee_id, event_id from the 504'd runs).
ORPHANS = [
    ("E900",          "1eb457d83d7564b3d1ef4b0c593ae6a6"),
    # TIMER_TEST_01's event_id was hash("TIMER_TEST_01|Leaver|2026-08-28").
    # Fill in the real RowKey from its JmlEvents row before running that one.
    # ("TIMER_TEST_01", "<event_id_from_its_jmlevents_row>"),
]


def reset_jml_events(events_client, employee_id: str, event_id: str) -> None:
    before = get_event(events_client, employee_id, event_id)
    if before is None:
        print(f"  JmlEvents: no row for {employee_id}/{event_id} — nothing to reset")
        return
    print(f"  JmlEvents: current Status={before.status}, LockedBy={before.locked_by}")

    release_lock(events_client, employee_id, event_id)
    update_event_status(
        table_client=events_client,
        employee_id=employee_id,
        event_id=event_id,
        status=EventStatus.FAILED,
        failure_step="Timeout504",
    )
    after = get_event(events_client, employee_id, event_id)
    print(f"  JmlEvents: reset -> Status={after.status}, LockedBy={after.locked_by!r}")


def reset_leaver_event_log(conn_str: str, employee_id: str, event_id: str) -> None:
    service = TableServiceClient.from_connection_string(conn_str)
    client = service.get_table_client(LEAVER_EVENT_LOG_TABLE)
    entity = {
        "PartitionKey": employee_id,
        "RowKey":       event_id,
        "status":       "OFFBOARD_FAILED",
        "updated_at":   datetime.now(timezone.utc).isoformat(),
    }
    client.upsert_entity(entity)
    print(f"  LeaverEventLog: set {employee_id}/{event_id} -> OFFBOARD_FAILED")


def _load_conn_str() -> str:
    """
    Resolve the storage connection string.

    Prefer the shell env, but fall back to local.settings.json (the same
    file the Functions host reads), since a plain `python script.py` run
    does not load it automatically the way `func start` does.
    """
    for var in ("JML_STORAGE_CONNECTION_STRING", "AZURE_STORAGE_CONNECTION_STRING"):
        if os.environ.get(var):
            return os.environ[var]

    import json
    with open("local.settings.json") as f:
        values = json.load(f).get("Values", {})
    for var in ("JML_STORAGE_CONNECTION_STRING", "AZURE_STORAGE_CONNECTION_STRING"):
        if values.get(var):
            return values[var]

    raise SystemExit(
        "No storage connection string found in the shell env or "
        "local.settings.json (looked for JML_STORAGE_CONNECTION_STRING "
        "and AZURE_STORAGE_CONNECTION_STRING)."
    )


def main() -> None:
    conn_str = _load_conn_str()
    events_client = get_events_table_client(conn_str)

    for employee_id, event_id in ORPHANS:
        print(f"\n=== {employee_id} / {event_id} ===")
        reset_jml_events(events_client, employee_id, event_id)
        reset_leaver_event_log(conn_str, employee_id, event_id)

    print("\nDone. Both tables now record the dead run(s) as Failed / OFFBOARD_FAILED.")


if __name__ == "__main__":
    main()