import azure.durable_functions as df

TERMINAL_EARLY_EXITS = {"HELD", "DUPLICATE", "QUEUED", "SKIPPED"}


def orchestrator_function(context: df.DurableOrchestrationContext):
    payload_dict = context.get_input()

    pre = yield context.call_activity("joiner_pre_provision_activity", payload_dict)
    if pre["final_status"] in TERMINAL_EARLY_EXITS:
        return pre

    provision = yield context.call_activity("joiner_provision_activity", pre)
    if provision["final_status"] == "FAILED":
        finalize_input = {**pre, **provision, "final_status": "FAILED"}
        return (yield context.call_activity("joiner_finalize_activity", finalize_input))

    finalize_input = {**pre, **provision}
    return (yield context.call_activity("joiner_finalize_activity", finalize_input))


build = df.Orchestrator.create(orchestrator_function)