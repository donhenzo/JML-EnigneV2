"""
Ingestion/hr_api/webhook.py



Receives a BambooHR webhook notification, validates the request,
extracts the employee ID, and returns 200. No fetch, no derivation,
no dispatch yet — those come in Steps 2–4.

BambooHR sends a POST when an employee record changes. The payload
shape varies by webhook configuration, but always contains at least
one employee identifier. This handler normalizes that into a list
of employee IDs and acknowledges receipt.
"""

import json
import logging

import azure.functions as func

logger = logging.getLogger(__name__)

# Step 1 — Webhook shell.

async def handle(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    # Parse the incoming JSON body.
    try:
        body = req.get_json()
    except (ValueError, TypeError):
        logger.warning("Webhook received non-JSON body")
        return func.HttpResponse(
            json.dumps({"error": "invalid JSON body"}),
            status_code=400,
            mimetype="application/json",
        )

    # Extract employee IDs from the webhook payload.
    # BambooHR webhooks can arrive in several shapes:
    #   - {"employees": [{"id": "123"}, ...]}   (standard webhook)
    #   - {"employee_id": "123"}                 (single-event shorthand)
    # We normalize to a list of string IDs.
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

    # Step 1 stops here. Steps 2–4 will add:
    #   2. Fetch full record from BambooHR for each employee ID
    #   3. Map fields + derive action (Joiner/Mover/Leaver/Skip)
    #   4. Start the correct durable orchestration

    return func.HttpResponse(
        json.dumps({
            "status": "received",
            "employee_ids": employee_ids,
            "actions_taken": "none — shell only",
        }),
        status_code=200,
        mimetype="application/json",
    )


def _extract_employee_ids(body: dict) -> list[str]:
    """Pull employee IDs from whichever webhook shape arrives."""
    ids: list[str] = []

    # Standard BambooHR webhook: list of employee objects.
    employees = body.get("employees")
    if isinstance(employees, list):
        for emp in employees:
            emp_id = emp.get("id") if isinstance(emp, dict) else None
            if emp_id is not None:
                ids.append(str(emp_id))
        return ids

    # Single-event shorthand.
    single = body.get("employee_id")
    if single is not None:
        return [str(single)]

    return ids