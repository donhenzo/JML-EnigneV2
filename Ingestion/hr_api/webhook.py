"""
Ingestion/hr_api/webhook.py

Steps 1–4 — Webhook receive → fetch → map → derive → dispatch.

Receives a BambooHR webhook, fetches the full record, maps it,
derives the lifecycle action, and starts the correct durable
orchestration (Joiner / Mover / Leaver). Skips are logged only.
"""

import json
import logging

import azure.functions as func
import azure.durable_functions as df

from Ingestion.hr_api.bamboohr.bamboohr_client import get_employee
from Ingestion.hr_api.bamboohr.bamboohr_mapper import map_to_raw_identity
from Ingestion.hr_api.action_deriver import derive_action
from Provisioning.graph_client import build_graph_client, JmlGraphClient

logger = logging.getLogger(__name__)

# Action → orchestrator function name
_ORCHESTRATOR_MAP = {
    "Joiner": "joiner_durable_orchestrator",
    "Mover":  "mover_durable_orchestrator",
    "Leaver": "leaver_durable_orchestrator",
}


def _get_graph_client() -> JmlGraphClient:
    """Build a Graph client for Entra ID lookups during action derivation."""
    graph_service_client, credential = build_graph_client()
    return JmlGraphClient(graph_service_client, credential)


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

    try:
        graph_client = _get_graph_client()
    except Exception as e:
        logger.error("Failed to build Graph client: %s", e)
        return func.HttpResponse(
            json.dumps({"error": f"Graph client init failed: {e}"}),
            status_code=500,
            mimetype="application/json",
        )

    # Step 4: durable client for dispatching orchestrations
    client = df.DurableOrchestrationClient(starter)

    results = []
    for emp_id in employee_ids:
        try:
            # Fetch and map
            raw = get_employee(emp_id)
            mapped = map_to_raw_identity(raw)

            # Derive action
            action = derive_action(mapped, graph_client)
            mapped["action"] = action

            # Dispatch to the correct orchestration or skip
            orchestrator = _ORCHESTRATOR_MAP.get(action)
            if orchestrator:
                instance_id = await client.start_new(orchestrator, None, mapped)
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