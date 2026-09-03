"""
Post-move verification for Mover events.

After package additions and removals have executed (ADR-009: additions
first, confirmed delivered, then removals), this module re-fetches the
user's actual access package assignments and confirms they match the
expected post-move state.

Expected state is:
    unchanged ∪ retain_set ∪ packages_to_add

Any discrepancy between expected and actual is recorded in the result.
The orchestrator uses the result to set the final event status:
    MOVE_SUCCESS  — actual state matches expected state exactly
    MOVE_PARTIAL  — discrepancies found (this is also how an addition
                    that failed to deliver surfaces — it's simply MISSING
                    from actual, no separate early-exit path needed)

This module also runs the in-process PostProvision governance check
(ADR-019) against the real Entra object — the same check the Joiner runs
after provisioning. It is DETECTIVE: a finding means non-compliant access
already exists; the response is to mark the move MOVE_PARTIAL and record the
reason, not to remediate (remediation is the reconciliation pipeline's job,
ADR-012). It reads the user's real group memberships (memberOf) and evaluates
employment/tier against governance_model.json and Separation of Duties against
sod_policies.json.

Graph API eventual consistency:
    Access package assignment writes can take time to propagate. A
    configurable delay is applied before fetching. The orchestrator
    passes delay_seconds in so tests can set it to zero without mocking
    time.

    Removal propagation lag:
    A package that was successfully removed may still appear in the
    assignment fetch during the post-move check due to Graph eventual
    consistency. The orchestrator passes recently_removed so the
    verifier can exclude them from the UNEXPECTED check. A package the
    engine just removed appearing in the fetch is a transient
    propagation state, not a real discrepancy.
"""

from __future__ import annotations
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from Provisioning.graph_client import JmlGraphClient, GraphClientError
from governance.postprovision import (
    run_postprovision,
    load_governance_model,
    load_sod_policies,
    GovernanceResult,
)

logger = logging.getLogger(__name__)

# Default delay before re-fetching package assignments after writes.
# Accounts for Graph API eventual consistency.
DEFAULT_CONSISTENCY_DELAY_SECONDS = 10


# Data models

class PostMoveStatus(str, Enum):
    """
    The outcome of post-move verification.

    MOVE_SUCCESS  — actual Entra state matches expected state exactly.
                    Governance validation passed.
    MOVE_PARTIAL  — assignment discrepancies found, or governance
                    validation returned failures, or the governance
                    read could not complete.
    VERIFICATION_ERROR — Graph API call to fetch actual package state
                         failed. Cannot determine whether the move
                         succeeded. Orchestrator marks the event
                         MOVE_FAILED.
    """
    MOVE_SUCCESS       = "MOVE_SUCCESS"
    MOVE_PARTIAL       = "MOVE_PARTIAL"
    VERIFICATION_ERROR = "VERIFICATION_ERROR"


@dataclass
class AssignmentDiscrepancy:
    """
    A single discrepancy between expected and actual package assignments.

    Fields:
        resource_id: Entra access package ID
        kind:        "MISSING" — expected but not found in actual state.
                     This is also how a failed adminAdd surfaces — no
                     separate failure path needed upstream.
                     "UNEXPECTED" — found in actual state but not expected.
    """
    resource_id: str
    kind:        str    # "MISSING" | "UNEXPECTED"


@dataclass
class PostMoveVerificationResult:
    """
    The complete result of post-move verification.

    Fields:
        status:            MOVE_SUCCESS, MOVE_PARTIAL, or VERIFICATION_ERROR.
        expected_packages: The package set the user should hold after the
                           move. unchanged ∪ retain_set ∪ packages_to_add.
        actual_packages:   The package set fetched from Entra ID after the
                           move. Empty if the fetch failed.
        discrepancies:     List of AssignmentDiscrepancy — packages that
                           are missing or unexpected relative to expected
                           state. Empty on MOVE_SUCCESS.
        governance_result: The GovernanceResult from the in-process
                           PostProvision check (ADR-019). None if the
                           memberOf read could not complete.
        error:             Error message if status is VERIFICATION_ERROR,
                           or if the governance read failed.
    """
    status:            PostMoveStatus
    expected_packages: frozenset[str]
    actual_packages:   frozenset[str]
    discrepancies:     list[AssignmentDiscrepancy]
    governance_result: Optional[GovernanceResult]
    error:             str = ""


# Graph fetch

def _fetch_actual_packages(
    graph_client: JmlGraphClient,
    user_id:      str,
) -> frozenset[str]:
    """
    Fetch the user's current delivered access package assignments from
    Entra ID.

    Reuses the same Graph call as Mover Step 1 — current state is
    defined identically whether it's being read before or after the move.

    Returns a frozenset of access package IDs.
    Raises GraphClientError on failure — caller handles it.
    """
    assignments = graph_client.get_current_access_package_assignments(
        user_id=user_id,
    )
    return frozenset(
        a["accessPackage"]["id"]
        for a in assignments
        if a.get("accessPackage", {}).get("id")
    )


# Discrepancy calculation

def _calculate_discrepancies(
    expected:         frozenset[str],
    actual:           frozenset[str],
    unmanaged:        frozenset[str] = frozenset(),
    recently_removed: frozenset[str] = frozenset(),
) -> list[AssignmentDiscrepancy]:
    """
    Compare expected and actual package sets and return discrepancies.

    MISSING    — in expected but not in actual. A package the move
                 should have added or retained was not found in the
                 tenant. This is also how a failed adminAdd surfaces.

    UNEXPECTED — in actual but not in expected, and not excluded by
                 unmanaged or recently_removed.

    Exclusions from the UNEXPECTED check:
        unmanaged        — packages intentionally outside the engine's
                           managed catalogue. Recorded separately in
                           the audit record, not a discrepancy here.
        recently_removed — packages successfully removed this event but
                           possibly not yet propagated. A 200 from the
                           adminRemove confirms the request was accepted;
                           this exclusion prevents a false MOVE_PARTIAL
                           from propagation lag alone.
    """
    discrepancies: list[AssignmentDiscrepancy] = []

    for resource_id in expected - actual:
        discrepancies.append(AssignmentDiscrepancy(
            resource_id = resource_id,
            kind        = "MISSING",
        ))

    excluded = unmanaged | recently_removed
    for resource_id in (actual - expected) - excluded:
        discrepancies.append(AssignmentDiscrepancy(
            resource_id = resource_id,
            kind        = "UNEXPECTED",
        ))

    return discrepancies


# Main verifier

def verify_post_move_state(
    graph_client:     JmlGraphClient,
    user_id:          str,
    employee_id:      str,
    employment_type:  str,
    unchanged:        frozenset[str],
    retain_set:       frozenset[str],
    packages_to_add:  frozenset[str],
    unmanaged:        frozenset[str] = frozenset(),
    recently_removed: frozenset[str] = frozenset(),
    delay_seconds:    int = DEFAULT_CONSISTENCY_DELAY_SECONDS,
) -> PostMoveVerificationResult:
    """
    Verify the user's Entra ID state after a Mover event completes.

    Runs two checks in sequence:
        1. Assignment check — re-fetches actual access package
           assignments and compares against expected state
           (unchanged ∪ retain_set ∪ packages_to_add).
        2. Governance check — in-process PostProvision (ADR-019) against
           the user's real group memberships.

    The assignment check is the gate for VERIFICATION_ERROR: if it fails,
    the move outcome is unknown and the governance check is skipped. A
    governance-read failure is softer — the move landed, but governance
    could not be confirmed, so the result is MOVE_PARTIAL, not FAILED.

    Args:
        graph_client:     Authenticated JmlGraphClient instance.
        user_id:          Entra object ID of the moved user.
        employee_id:      HR source identifier — used for log context only.
        employment_type:  The user's employment type (Employee/Contractor/
                          Guest/Intern) — the governance check evaluates each
                          held group's allowed_employment against it.
        unchanged:        Packages the user held that are still valid
                          post-move. From MoverDelta.unchanged.
        retain_set:       Packages that survived retention evaluation.
                          From RetentionResult.retain_set.
        packages_to_add:  Packages added by the new role mapping.
                          From MoverDelta.groups_to_add.
        unmanaged:        Packages outside the managed catalogue —
                          excluded from the UNEXPECTED discrepancy check.
                          From MoverDelta.unmanaged.
        recently_removed: Packages successfully removed this event —
                          excluded from the UNEXPECTED check to avoid
                          false positives from Graph propagation lag.
        delay_seconds:    Seconds to wait before fetching actual state.
                          Accounts for Graph eventual consistency.
                          Pass 0 in tests.

    Returns:
        PostMoveVerificationResult with status, discrepancies, and
        governance result.

    Side effects:
        Sleeps for delay_seconds before the Graph fetch.
        Two Graph API calls — access package assignments, and memberOf.
    """
    expected_packages: frozenset[str] = unchanged | retain_set | packages_to_add

    if delay_seconds > 0:
        logger.info(
            "Post-move verification — waiting %ds for Graph consistency — "
            "employee=%s, user_id=%s",
            delay_seconds, employee_id, user_id,
        )
        time.sleep(delay_seconds)

    # Step 1 — fetch actual access package assignments
    try:
        actual_packages = _fetch_actual_packages(
            graph_client = graph_client,
            user_id      = user_id,
        )
    except GraphClientError as e:
        logger.error(
            "Post-move verification — assignment fetch failed — "
            "employee=%s, user_id=%s, error=%s",
            employee_id, user_id, str(e),
        )
        return PostMoveVerificationResult(
            status             = PostMoveStatus.VERIFICATION_ERROR,
            expected_packages  = expected_packages,
            actual_packages    = frozenset(),
            discrepancies      = [],
            governance_result  = None,
            error              = str(e),
        )

    # Step 2 — calculate discrepancies
    discrepancies = _calculate_discrepancies(
        expected         = expected_packages,
        actual           = actual_packages,
        unmanaged        = unmanaged,
        recently_removed = recently_removed,
    )

    if discrepancies:
        logger.warning(
            "Post-move assignment discrepancies found — employee=%s, "
            "missing=%d, unexpected=%d",
            employee_id,
            sum(1 for d in discrepancies if d.kind == "MISSING"),
            sum(1 for d in discrepancies if d.kind == "UNEXPECTED"),
        )
    else:
        logger.info(
            "Post-move assignments match expected state — employee=%s",
            employee_id,
        )

    # Step 3 — in-process governance validation against real memberOf (ADR-019).
    # Detective: reports, does not remediate. A memberOf read failure does not
    # fail the move (the packages already landed) — it leaves governance
    # unconfirmed, which resolves to MOVE_PARTIAL below.
    logger.info(
        "Post-move governance validation (in-process) — employee=%s, user_id=%s",
        employee_id, user_id,
    )

    governance_result: Optional[GovernanceResult] = None
    governance_error = ""
    try:
        member_of = graph_client.get_user_group_memberships(user_id)
        governance_result = run_postprovision(
            payload          = {"employment_type": employment_type},
            member_of        = member_of,
            governance_model = load_governance_model(),
            sod_policies     = load_sod_policies(),
        )
        if not governance_result.passed:
            logger.warning(
                "Post-move governance validation failed — employee=%s, failures=%s",
                employee_id, governance_result.failure_summary(),
            )
    except GraphClientError as e:
        logger.error(
            "Post-move governance — memberOf fetch failed — employee=%s, error=%s",
            employee_id, str(e),
        )
        governance_error = str(e)

    # Final status: assignments verified above, so this is never
    # VERIFICATION_ERROR. Discrepancies, a governance failure, or an
    # unconfirmed governance read all resolve to MOVE_PARTIAL.
    governance_failed = governance_result is None or not governance_result.passed

    if discrepancies or governance_failed:
        status = PostMoveStatus.MOVE_PARTIAL
    else:
        status = PostMoveStatus.MOVE_SUCCESS

    logger.info(
        "Post-move verification complete — employee=%s, status=%s",
        employee_id, status.value,
    )

    return PostMoveVerificationResult(
        status             = status,
        expected_packages  = expected_packages,
        actual_packages    = actual_packages,
        discrepancies      = discrepancies,
        governance_result  = governance_result,
        error              = governance_error,
    )