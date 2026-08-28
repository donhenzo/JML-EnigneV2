"""
Leaver/durable/orchestrator.py

The Leaver orchestration. Owns all flow and the one wait: the removal poll loop
is timer-driven (create_timer holds no compute, so an offboarding whose removals
take past the ~230s gateway limit completes instead of 504ing — this is the
whole point).

Simpler than the Mover: one poll loop, no add loop, no ADR-009 gate. The
disable/revoke fail-safe (ADR-015) already ran inside the pre activity, before
any removal, so by the time the removal loop runs the account is already locked
out.

Flow:
    pre  -> early-exit on QUEUED_CONCURRENT / OFFBOARD_FAILED
            (pre also disables + revokes, ADR-015)
    submit(remove) -> [check + timer]* -> finalize(remove)
    verify_finalize  (PIM terminate + soft delete + verify + audit + terminal)
"""

import azure.durable_functions as df

TERMINAL_EARLY_EXITS = {"QUEUED_CONCURRENT", "OFFBOARD_FAILED"}
POLL_INTERVAL_SECONDS = 5
POLL_MAX_ATTEMPTS = 60


def _seconds(n):
    from datetime import timedelta
    return timedelta(seconds=n)


def _run_poll_loop(context, state):
    """Submit is already done; drive check + timer until terminal or window end."""
    for _ in range(POLL_MAX_ATTEMPTS):
        checked = yield context.call_activity("leaver_check_activity", state)
        state = checked
        if checked.get("all_terminal"):
            break
        yield context.create_timer(
            context.current_utc_datetime + _seconds(POLL_INTERVAL_SECONDS)
        )
    return state


def orchestrator_function(context: df.DurableOrchestrationContext):
    payload_dict = context.get_input()

    pre = yield context.call_activity("leaver_pre_activity", payload_dict)
    if pre["final_status"] in TERMINAL_EARLY_EXITS:
        return pre

    state = yield context.call_activity("leaver_submit_activity", pre)
    state = yield from _run_poll_loop(context, state)
    state = yield context.call_activity("leaver_finalize_activity", state)

    return (yield context.call_activity("leaver_verify_finalize_activity", state))


build = df.Orchestrator.create(orchestrator_function)