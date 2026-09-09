import azure.durable_functions as df

TERMINAL_EARLY_EXITS = {"HELD", "DUPLICATE", "QUEUED", "SKIPPED"}

PROPAGATION_WAIT_SECONDS = 15
POLL_INTERVAL_SECONDS = 5
POLL_MAX_ATTEMPTS = 60


def orchestrator_function(context: df.DurableOrchestrationContext):
    raw_input = context.get_input()

    # Unwrap — webhook sends {"payload": {...}, "mapped_record": {...}},
    # HTTP starters send a flat payload dict.
    if "payload" in raw_input and "mapped_record" in raw_input:
        payload_dict = raw_input["payload"]
        mapped_record = raw_input["mapped_record"]
    else:
        payload_dict = raw_input
        mapped_record = None

    pre = yield context.call_activity("joiner_pre_provision_activity", payload_dict)
    if pre["final_status"] in TERMINAL_EARLY_EXITS:
        return pre

    created = yield context.call_activity("joiner_create_user_activity", pre)
    if created["final_status"] == "FAILED":
        return (yield context.call_activity("joiner_record_finalize_activity", created))

    # Propagation wait — durable timer, not a blocking sleep.
    yield context.create_timer(
        context.current_utc_datetime + _seconds(PROPAGATION_WAIT_SECONDS))

    submitted = yield context.call_activity("joiner_submit_packages_activity", created)
    if submitted["final_status"] == "FAILED":
        return (yield context.call_activity("joiner_record_finalize_activity", submitted))

    # Poll loop — timer + check, orchestrator-driven. This is the 504 fix:
    # the orchestrator sleeps via timer (holding no compute) instead of the
    # worker blocking on time.sleep.
    state = submitted
    for _ in range(POLL_MAX_ATTEMPTS):
        checked = yield context.call_activity("joiner_check_packages_activity", state)
        state = checked
        if checked.get("all_terminal"):
            break
        yield context.create_timer(
            context.current_utc_datetime + _seconds(POLL_INTERVAL_SECONDS))

    result = yield context.call_activity("joiner_record_finalize_activity", state)

    # Write the mapped record to JmlLastState on success.
    # Non-fatal — the pipeline already succeeded; a failure here just
    # means the next webhook may re-derive the same action.
    if mapped_record and result.get("final_status") not in TERMINAL_EARLY_EXITS:
        yield context.call_activity("save_last_state_activity", {
            "mapped_record": mapped_record,
            "employee_id": mapped_record.get("employee_id", "unknown"),
            "action": mapped_record.get("action", "Joiner"),
        })

    return result


def _seconds(n):
    from datetime import timedelta
    return timedelta(seconds=n)


build = df.Orchestrator.create(orchestrator_function)
