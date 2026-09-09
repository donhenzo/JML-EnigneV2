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
    -> if soft delete was deferred: timer(hold) -> deferred_delete
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
    raw_input = context.get_input()

    # Unwrap — webhook sends {"payload": {...}, "mapped_record": {...}},
    # HTTP starters send a flat payload dict.
    if "payload" in raw_input and "mapped_record" in raw_input:
        payload_dict = raw_input["payload"]
        mapped_record = raw_input["mapped_record"]
    else:
        payload_dict = raw_input
        mapped_record = None

    pre = yield context.call_activity("leaver_pre_activity", payload_dict)
    if pre["final_status"] in TERMINAL_EARLY_EXITS:
        return pre

    state = yield context.call_activity("leaver_submit_activity", pre)
    state = yield from _run_poll_loop(context, state)
    state = yield context.call_activity("leaver_finalize_activity", state)

    result = yield context.call_activity("leaver_verify_finalize_activity", state)

    # Deferred-delete branch. The offboarding is already complete and its audit
    # record written by verify_finalize; the account is locked out and stripped.
    # If a soft-delete hold was configured, sleep it out on a durable timer (holds
    # no compute, survives restarts), then complete the deletion. The deferred_delete
    # activity re-checks the user is still disabled before deleting, so a re-hire
    # during the hold is not clobbered.
    if result.get("soft_delete_deferred") and result.get("hold_seconds", 0) > 0:
        yield context.create_timer(
            context.current_utc_datetime + _seconds(result["hold_seconds"])
        )
        result = yield context.call_activity("leaver_deferred_delete_activity", result)

    # Write the mapped record to JmlLastState on success.
    # Uses mark_terminated via action="Leaver" — the row stays so
    # a future webhook for a rehired employee derives "Joiner".
    if mapped_record:
        yield context.call_activity("save_last_state_activity", {
            "mapped_record": mapped_record,
            "employee_id": mapped_record.get("employee_id", "unknown"),
            "action": "Leaver",
        })

    return result


build = df.Orchestrator.create(orchestrator_function)
