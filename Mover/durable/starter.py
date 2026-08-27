"""
Mover/durable/starter.py

HTTP starter for the Mover orchestration. Returns 202 with a status URL
immediately; the orchestration runs the pipeline. Mirrors the Joiner starter.
"""

import logging
import azure.functions as func
import azure.durable_functions as df

logger = logging.getLogger(__name__)


async def start(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    try:
        body = req.get_json()
    except (ValueError, TypeError):
        return func.HttpResponse(
            '{"error": "invalid JSON body"}', status_code=400, mimetype="application/json"
        )
    if "payload" not in body:
        return func.HttpResponse(
            '{"error": "expected {\\"payload\\": {...}}"}', status_code=400, mimetype="application/json"
        )
    instance_id = await client.start_new("mover_durable_orchestrator", None, body["payload"])
    logger.info("Started mover orchestration — instance_id=%s", instance_id)
    return client.create_check_status_response(req, instance_id)