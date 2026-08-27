"""
Mover/durable/orchestrator.py

The Mover orchestration. Owns all flow and all waits: the two poll loops are
timer-driven (create_timer holds no compute, so a delivery past the ~230s
gateway limit completes instead of 504ing — this is the whole point), and the
ADR-009 add-before-remove gate is a branch here, never inside an activity.

Flow (Strategy A — add-before-remove, the only path built today):
    pre  -> early-exit on QUEUED_CONCURRENT / MOVE_FAILED
    submit(add) -> [check + timer]* -> finalize(add)  => additions_all_succeeded
    IF additions_all_succeeded:
        submit(remove) -> [check + timer]* -> finalize(remove) + attribute PATCH
    ELSE:
        skip removals + PATCH, record the ADR-009 deferral
    verify_finalize

ADR-011 (deferred, NOT built): a pre-flight incompatibility check would choose
between this Strategy A and a Strategy B (remove-before-add) for conflicting
packages. Because submit/check/finalize are generic over op, Strategy B would
be a branch added at the marked point below that runs the remove-triad first —
no activity change. Not implemented; the marker is a placeholder only.
"""

import azure.durable_functions as df

TERMINAL_EARLY_EXITS = {"QUEUED_CONCURRENT", "MOVE_FAILED"}
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

    # --- Additions (Strategy A: add first) ---
    add_state = {**pre, "op": "add"}
    add_state = yield context.call_activity("mover_submit_activity", add_state)
    add_state = yield from _run_poll_loop(context, add_state)
    add_state = yield context.call_activity("mover_finalize_op_activity", add_state)

    # --- ADR-009 gate (orchestrator owns the flow decision) ---
    # ADR-011 Strategy B (remove-before-add) would branch here for conflicting
    # packages, reusing the same submit/check/finalize activities with op flipped.
    # Not built.
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