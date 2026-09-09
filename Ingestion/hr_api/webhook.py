"""
Ingestion/hr_api/webhook.py

Webhook receive → fetch → map → derive (last-state) → dispatch.

Receives a BambooHR webhook, fetches the full record, maps it,
derives the lifecycle action by comparing against the last reconciled
state in JmlLastState, builds a clean IdentityPayload, and starts the
correct durable orchestration (Joiner / Mover / Leaver).

The last-state deriver replaces the Entra-based deriver — no Graph API
calls are needed to classify the action. The last state is read from
Azure Table Storage, which was seeded by bootstrap_last_state.py and is
kept current by the orchestrators on successful completion.

The mapped record is passed alongside the payload so the orchestrator
can write it back to JmlLastState after success — the webhook does NOT
write to the last-state store (write on success, not on dispatch).
"""

import json
import logging
import os

import azure.functions as func
import azure.durable_functions as df

from Ingestion.hr_api.bamboohr.bamboohr_client import get_employee
from Ingestion.hr_api.bamboohr.bamboohr_mapper import map_to_raw_identity
from Ingestion.hr_api.bamboohr.last_state_deriver import derive_action
from Ingestion.hr_api.bamboohr.last_state_store import (
    get_last_state_table_client,
    get_last_state,
)
from Ingestion.hr_api.bamboohr.payload_builder import build_identity_payload
from Normalization.lookup_loader import load_lookup_table

logger = logging.getLogger(__name__)

# Action → orchestrator function name
_ORCHESTRATOR_MAP = {
    "Joiner": "joiner_durable_orchestrator",
    "Mover":  "mover_durable_orchestrator",
    "Leaver": "leaver_durable_orchestrator",
}

# Lazy-loaded shared resources — built once per Function App instance
_lookup_table = None
_last_state_client = None


def _get_lookup_table() -> dict:
    """Lazy-load the canonical lookup table."""
    global _lookup_table
    if _lookup_table is None:
        _lookup_table = load_lookup_table("config/canonical_lookup.json")
    return _lookup_table


def _get_last_state_client():
    """Lazy-load the JmlLastState table client."""
    global _last_state_client
    if _last_state_client is None:
        conn = os.environ.get("JML_STORAGE_CONNECTION_STRING", "")
        _last_state_client = get_last_state_table_client(conn)
    return _last_state_client


async def handle(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    try:
        body = req.get_json()
    except (ValueError, TypeError):
        logger.warning("Webhook received non-JSON body")
        return func.HttpResponse(
            json.dumps({"error": "invalid JSON body"}),
            status_code=400,
            mimetype="application/json",
        )

    employee_ids = _extract_employee_ids(body)
    if not employee_ids:
        logger.warning("Webhook payload contained no employee IDs: %s", body)
        return func.HttpResponse(
            json.dumps({"error": "no employee IDs found in payload"}),
            status_code=400,
            mimetype="application/json",
        )

    logger.info(
        "Webhook received — %d employee(s): %s",
        len(employee_ids),
        employee_ids,
    )

    lookup = _get_lookup_table()
    last_state_client = _get_last_state_client()
    client = df.DurableOrchestrationClient(starter)

    results = []
    for emp_id in employee_ids:
        try:
            # Fetch and map
            raw = get_employee(emp_id)
            mapped = map_to_raw_identity(raw)

            employee_id = mapped.get("employee_id", emp_id)

            # Derive action from last state — no Entra call needed
            last_state = get_last_state(last_state_client, employee_id)
            action = derive_action(mapped, last_state)
            mapped["action"] = action

            # Dispatch to the correct orchestration or skip
            orchestrator = _ORCHESTRATOR_MAP.get(action)
            if orchestrator:
                payload = build_identity_payload(mapped, lookup)
                payload_dict = payload.to_dict()

                # Pass the mapped record alongside the payload so the
                # orchestrator can write it to JmlLastState on success.
                orchestrator_input = {
                    "payload": payload_dict,
                    "mapped_record": mapped,
                }

                instance_id = await client.start_new(
                    orchestrator, None, orchestrator_input,
                )
                logger.info(
                    "Employee %s → %s — started %s (instance: %s)",
                    emp_id, action, orchestrator, instance_id,
                )
                results.append({
                    "employee_id": emp_id,
                    "action": action,
                    "instance_id": instance_id,
                    "status": "dispatched",
                })
            else:
                logger.info("Employee %s → Skip — no orchestration needed", emp_id)
                results.append({
                    "employee_id": emp_id,
                    "action": "Skip",
                    "status": "skipped",
                })

        except Exception as e:
            logger.error("Failed to process employee %s: %s", emp_id, e)
            results.append({"employee_id": emp_id, "status": "error", "error": str(e)})

    return func.HttpResponse(
        json.dumps({"status": "processed", "results": results}, default=str),
        status_code=200,
        mimetype="application/json",
    )


def _extract_employee_ids(body: dict) -> list[str]:
    """Pull employee IDs from whichever webhook shape arrives."""
    ids: list[str] = []

    # Event-based webhook: {"type": "...", "data": {"employeeId": "4"}}
    data = body.get("data")
    if isinstance(data, dict):
        emp_id = data.get("employeeId")
        if emp_id is not None:
            return [str(emp_id)]

    # Standard webhook: {"employees": [{"id": "123"}, ...]}
    employees = body.get("employees")
    if isinstance(employees, list):
        for emp in employees:
            emp_id = emp.get("id") if isinstance(emp, dict) else None
            if emp_id is not None:
                ids.append(str(emp_id))
        return ids

    # Single-event shorthand: {"employee_id": "123"}
    single = body.get("employee_id")
    if single is not None:
        return [str(single)]

    return ids
