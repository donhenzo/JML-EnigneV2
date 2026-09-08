"""
Ingestion/hr_api/webhook.py


Receives a BambooHR webhook notification, extracts employee IDs,
fetches the full record for each from BambooHR, and maps it to
the JML engine's raw identity shape.

Steps 3–4 will add action derivation and durable dispatch.
"""

import json
import logging

import azure.functions as func

from Ingestion.hr_api.bamboohr.bamboohr_client import get_employee
from Ingestion.hr_api.bamboohr.bamboohr_mapper import map_to_raw_identity

logger = logging.getLogger(__name__)


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

    # Step 2: fetch full record from BambooHR and map each employee.
    results = []
    for emp_id in employee_ids:
        try:
            raw = get_employee(emp_id)
            mapped = map_to_raw_identity(raw)
            results.append({"employee_id": emp_id, "status": "mapped", "mapped": mapped})
            logger.info("Fetched and mapped employee %s", emp_id)
        except Exception as e:
            logger.error("Failed to fetch/map employee %s: %s", emp_id, e)
            results.append({"employee_id": emp_id, "status": "error", "error": str(e)})

    # Steps 3–4 will add action derivation and durable dispatch here.

    return func.HttpResponse(
        json.dumps({"status": "processed", "results": results}, default=str),
        status_code=200,
        mimetype="application/json",
    )


def _extract_employee_ids(body: dict) -> list[str]:
    """Pull employee IDs from whichever webhook shape arrives."""
    ids: list[str] = []

    employees = body.get("employees")
    if isinstance(employees, list):
        for emp in employees:
            emp_id = emp.get("id") if isinstance(emp, dict) else None
            if emp_id is not None:
                ids.append(str(emp_id))
        return ids

    single = body.get("employee_id")
    if single is not None:
        return [str(single)]

    return ids