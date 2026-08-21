import logging

import azure.functions as func
import azure.durable_functions as df

logger = logging.getLogger(__name__)


async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    instance_id = await client.start_new("hello_orchestrator", None, None)
    logger.info("Started orchestration — instance_id=%s", instance_id)
    return client.create_check_status_response(req, instance_id)