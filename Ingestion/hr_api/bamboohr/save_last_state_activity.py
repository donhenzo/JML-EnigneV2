"""
Save Last-State Activity — Durable Functions Activity

Responsibility: write the mapped HR record to JmlLastState after a
durable orchestrator completes successfully.

This is a Durable Functions activity, not a standalone function. It is
called by the Joiner, Mover, and Leaver orchestrators as their final
step on success. The webhook does NOT write to the last-state store —
only a confirmed pipeline success triggers this.

The orchestrator passes the mapped_record (raw BambooHR → mapper output)
so we store the actual HR values, not the normalised/typed payload.
This keeps the deriver comparing raw-to-raw on the next webhook.

For Leaver events, mark_terminated() is used instead of save_last_state()
to ensure the row is explicitly set to Inactive — enabling clean rehire
detection on the next webhook for this employee.
"""

import logging
import os

import azure.durable_functions as df

from Ingestion.hr_api.bamboohr.last_state_store import (
    get_last_state_table_client,
    save_last_state,
    mark_terminated,
)

logger = logging.getLogger(__name__)

# Lazy-loaded — shared across activity invocations in the same worker
_last_state_client = None


def _get_client():
    global _last_state_client
    if _last_state_client is None:
        conn = os.environ.get("JML_STORAGE_CONNECTION_STRING", "")
        _last_state_client = get_last_state_table_client(conn)
    return _last_state_client


def main(input: dict) -> dict:
    """
    Activity entry point — called by the orchestrator.

    Args:
        input: dict with:
            mapped_record: raw identity dict from bamboohr_mapper
            employee_id:   HR-meaningful employee ID
            action:        the derived action (Joiner/Mover/Leaver)

    Returns:
        dict with status and employee_id for orchestrator logging.
    """
    mapped_record = input.get("mapped_record", {})
    employee_id = input.get("employee_id", "unknown")
    action = input.get("action", "")

    try:
        client = _get_client()

        if action == "Leaver":
            mark_terminated(client, employee_id, mapped_record)
        else:
            save_last_state(client, employee_id, mapped_record)

        return {"status": "saved", "employee_id": employee_id}

    except Exception as e:
        # Last-state write failure is non-fatal — the pipeline already
        # succeeded. Log the error but don't fail the orchestration.
        # The next webhook for this employee will still work — it will
        # just see stale state and may re-derive the same action,
        # which the event store's idempotency guard will catch.
        logger.error(
            "Failed to save last state for employee %s: %s",
            employee_id, e,
        )
        return {"status": "failed", "employee_id": employee_id, "error": str(e)}