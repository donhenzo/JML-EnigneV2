"""
Mover/durable/orchestrator.py

The Mover orchestration. Owns all flow and all waits: the two poll loops are
timer-driven (create_timer holds no compute, so a delivery past the ~230s
gateway limit completes instead of 504ing — this is the whole point), and the
ADR-009 add-before-remove gate is a branch here, never inside an activity.

Flow:
    pre  -> early-exit on QUEUED_CONCURRENT / MOVE_FAILED / SOD_HELD
    IF pre["reorder"] (ADR-011 Strategy B — a conflicting held package is
       being removed anyway, so remove it before the add to avoid a platform
       incompatibility rejection):
        submit(remove) -> [check + timer]* -> finalize(remove) + attribute PATCH
        submit(add)    -> [check + timer]* -> finalize(add)
    ELSE (Strategy A — add-before-remove, ADR-009 default):
        submit(add) -> [check + timer]* -> finalize(add)  => additions_all_succeeded
        IF additions_all_succeeded:
            submit(remove) -> [check + timer]* -> finalize(remove) + attribute PATCH
        ELSE:
            skip removals + PATCH, record the ADR-009 deferral
    verify_finalize

Because submit/check/finalize are generic over op, both strategies reuse the
same three activities with op flipped — no activity change for either branch.
"""
import azure.durable_functions as df

TERMINAL_EARLY_EXITS = {"QUEUED_CONCURRENT", "MOVE_FAILED", "SOD_HELD"}
POLL_INTERVAL_SECONDS = 5
POLL_MAX_ATTEMPTS = 60


def _seconds(n):
    from datetime import timedelta
    return timedelta(seconds=n)


def _run_poll_loop(context, state):
    """Submit is already done; drive check + timer until terminal or window end."""
    for _ in range(POLL_MAX_ATTEMPTS):
        checked = yield context.call_activity("mover_check_activity", state)
        state = checked
        if checked.get("all_terminal"):
            break
        yield context.create_timer(
            context.current_utc_datetime + _seconds(POLL_INTERVAL_SECONDS)
        )
    return state


def orchestrator_function(context: df.DurableOrchestrationContext):
    payload_dict = context.get_input()
    pre = yield context.call_activity("mover_pre_activity", payload_dict)

    if pre["final_status"] in TERMINAL_EARLY_EXITS:
        return pre

    # --- ADR-011 Strategy B: remove-before-add ---
    # The pre-flight (mover_pre_activity -> stage_preflight_sod) has already
    # decided this; the orchestrator's only job is to honour it.
    if pre.get("reorder"):
        remove_state = {**pre, "op": "remove"}
        remove_state = yield context.call_activity("mover_submit_activity", remove_state)
        remove_state = yield from _run_poll_loop(context, remove_state)
        remove_state = yield context.call_activity("mover_finalize_op_activity", remove_state)

        add_state = {**remove_state, "op": "add"}
        add_state = yield context.call_activity("mover_submit_activity", add_state)
        add_state = yield from _run_poll_loop(context, add_state)
        add_state = yield context.call_activity("mover_finalize_op_activity", add_state)

        final_state = add_state

    else:
        # --- Strategy A: add-before-remove (ADR-009 default) ---
        add_state = {**pre, "op": "add"}
        add_state = yield context.call_activity("mover_submit_activity", add_state)
        add_state = yield from _run_poll_loop(context, add_state)
        add_state = yield context.call_activity("mover_finalize_op_activity", add_state)

        # --- ADR-009 gate (orchestrator owns the flow decision) ---
        if add_state.get("additions_all_succeeded"):
            remove_state = {**add_state, "op": "remove"}
            remove_state = yield context.call_activity("mover_submit_activity", remove_state)
            remove_state = yield from _run_poll_loop(context, remove_state)
            remove_state = yield context.call_activity("mover_finalize_op_activity", remove_state)
            final_state = remove_state
        else:
            # Additions did not all deliver — skip removals + attribute PATCH,
            # record the ADR-009 deferral. Mirrors the sync driver's else branch.
            audit_record = add_state["audit_record"]
            audit_record["packages_removed"] = []
            audit_record["warnings"].append(
                "Attribute update deferred — package additions did not all deliver, "
                "so the role transition is not committed this pass (ADR-009). "
                "Department/title remain at their previous values until a retry "
                "lands every addition."
            )
            final_state = {**add_state, "audit_record": audit_record, "recently_removed": []}

    return (yield context.call_activity("mover_verify_finalize_activity", final_state))


build = df.Orchestrator.create(orchestrator_function)