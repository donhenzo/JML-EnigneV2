"""

Separation of Duties re-evaluation for Mover events.

Runs two passes against sod_policies.json before any Graph API write occurs:

    Pass A — Full proposed post-move access set:
              effective_access = unchanged ∪ retain_set ∪ groups_to_add
              A block violation here triggers HOLD_FOR_REVIEW.
              No access changes are applied until a human resolves it.

    Pass B — groups_to_add in isolation.
              Catches self-conflicting role mappings independent of
              what the user currently holds or retains.

This module composes the effective access set and orchestrates both passes.
The actual policy intersection logic is delegated to sod_checker.evaluate_sod().

No Graph API calls. No I/O. No side effects.
sod_policies.json is loaded by the orchestrator and passed in.
"""

from __future__ import annotations
from dataclasses import dataclass

from Governance.SoD.sod_checker import evaluate_sod
from Governance.SoD.sod_models import (
    SoDAction,
    SoDCheckResult,
    SoDPolicy,
    EvaluationContext,
    DetectionPath,
    FetchStatus,
)



# Data models


@dataclass(frozen=True)
class MoverSoDPassResult:
    """
    The result of a single SoD evaluation pass.

    Fields:
        pass_label:       Human-readable label for audit records ("PassA" or "PassB").
        effective_access: The group set that was evaluated in this pass.
        result:           The raw SoDCheckResult from sod_checker.evaluate_sod().
        has_block:        True if any active violation carries action BLOCK.
                          The orchestrator reads this flag directly — it does
                          not need to inspect individual violations to decide
                          what to do next.
    """
    pass_label:       str
    effective_access: frozenset[str]
    result:           SoDCheckResult
    has_block:        bool


@dataclass(frozen=True)
class MoverSoDResult:
    """
    The complete two-pass SoD re-evaluation result for a Mover event.

    Fields:
        pass_a:       Result of the full post-move access set evaluation.
        pass_b:       Result of groups_to_add evaluated in isolation.
        should_hold:  True if either pass produced a block violation.
                      This is the top-level flag the orchestrator checks.
                      A True value means the event must enter HOLD_FOR_REVIEW.
                      No access changes are applied.
    """
    pass_a:      MoverSoDPassResult
    pass_b:      MoverSoDPassResult
    should_hold: bool



# Internal helpers
def _run_pass(
    pass_label:       str,
    effective_access: frozenset[str],
    policies:         list[SoDPolicy],
    detection_path:   DetectionPath,
) -> MoverSoDPassResult:
    """
    Run a single SoD evaluation pass and wrap the result.

    current_groups is always passed as an empty list because the
    effective access set is fully composed by the caller before this
    function is called. evaluate_sod() would union current + requested
    internally — passing the composed set as requested_groups and an
    empty list as current_groups produces the same result without
    double-composing.

    FetchStatus is always COMPLETE here because the set composition
    already happened upstream. The fail-closed gate in evaluate_sod()
    only triggers for MOVER/LEAVER context with a degraded fetch —
    that concern belongs to the orchestrator that fetches from Graph,
    not to this pure evaluation layer.

    Args:
        pass_label:       Label for audit records — "PassA" or "PassB".
        effective_access: The fully composed group set to evaluate.
        policies:         Loaded SoDPolicy list from sod_policies.json.
        detection_path:   PRE_PROVISION for all Mover SoD checks.

    Returns:
        MoverSoDPassResult with the raw result and a has_block flag.
    """
    result = evaluate_sod(
        requested_groups            = list(effective_access),
        current_groups              = [],
        current_groups_fetch_status = FetchStatus.COMPLETE,
        policies                    = policies,
        context                     = EvaluationContext.MOVER,
        detection_path              = detection_path,
    )

    has_block = any(
        v.action == SoDAction.BLOCK and not v.exception_applied
        for v in result.violations
    )

    return MoverSoDPassResult(
        pass_label       = pass_label,
        effective_access = effective_access,
        result           = result,
        has_block        = has_block,
    )



# Main evaluation
def evaluate_mover_sod(
    unchanged:        frozenset[str],
    retain_set:       frozenset[str],
    groups_to_add:    frozenset[str],
    remove_confirmed: frozenset[str],
    policies:         list[SoDPolicy],
) -> MoverSoDResult:
    """
    Run two-pass SoD re-evaluation for a Mover event.

    Must be called after retention_evaluator has produced retain_set
    and before any Graph API write occurs (Step 5 in the 10-step flow).

    Pass A evaluates the full proposed post-move access set:
        effective_access = unchanged ∪ retain_set ∪ groups_to_add

    Pass B evaluates groups_to_add in isolation to catch self-conflicting
    role mappings regardless of what the user currently holds or retains.

    Args:
        unchanged:     Groups the user holds that are still valid after the move.
                       From MoverDelta.unchanged.
        retain_set:    Groups that survived retention evaluation.
                       From RetentionResult.retain_set.
        groups_to_add: Groups the new role requires that the user does not hold.
                       From MoverDelta.groups_to_add.
        policies:      Loaded SoDPolicy list. The orchestrator loads
                       sod_policies.json once and passes it in.
                       This function does not load files.

    Returns:
        MoverSoDResult with both pass results and a top-level should_hold flag.

    Security:
        If Pass A finds a block violation, the orchestrator must not apply
        any access changes. Both removal and addition are frozen until a
        human resolves the conflict — ADR-001 (Fail Closed).

        Pass B block violations also set should_hold — a self-conflicting
        role mapping is as serious as a retained-access conflict.
    """
   # Compose the full effective access set for Pass A.
    # remove_confirmed groups still exist on the user at this point —
    # the removal executes at Step 6, after this evaluation completes.
    # Including them ensures a conflict between incoming and outgoing
    # access is detected before any Graph write occurs.
    full_post_move_access: frozenset[str] = (
        unchanged | retain_set | groups_to_add | remove_confirmed
    )

    pass_a = _run_pass(
        pass_label       = "PassA",
        effective_access = full_post_move_access,
        policies         = policies,
        detection_path   = DetectionPath.PRE_PROVISION,
    )

    pass_b = _run_pass(
        pass_label       = "PassB",
        effective_access = groups_to_add,
        policies         = policies,
        detection_path   = DetectionPath.PRE_PROVISION,
    )

    should_hold = pass_a.has_block or pass_b.has_block

    return MoverSoDResult(
        pass_a      = pass_a,
        pass_b      = pass_b,
        should_hold = should_hold,
    )