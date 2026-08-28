"""
Leaver/provisioning_phases.py

The Leaver's package-removal execution layer, split into sleep-free phases so
the CALLER owns waiting. Mirrors Mover/provisioning_phases.py: the synchronous
driver composes these with time.sleep; the Durable orchestrator composes the
SAME functions with durable timers. The phase functions never sleep — that
seam is what lets the sync and durable paths share one implementation.

This is a layer BELOW the stages. It knows about PendingPackage, JmlGraphClient,
and plain audit-action dicts — nothing about StageResult, stages, or drivers.
It does not import stage_result.py.

The Leaver is all-removal. Unlike the Mover there is no addition loop, no
ADR-009 add-before-remove gate, and no all_succeeded flag — there is nothing
to order removals against. Individual removal failures are recorded but do not
stop the set: a partial removal still leaves the user with less access than
doing nothing would, and post-offboarding verification (Step 7) surfaces
whatever didn't clear.

FIELD NAME — read `state`, not `requestState`:
    The assignmentRequests resource carries a `state` field with lowercase
    terminal values: delivered / denied / canceled / failed. There is no
    `requestState` field on this resource. The pre-refactor Leaver polled a
    `requestState` field that does not exist, so the poll never observed a
    terminal state and ran its full attempt window on every package, only
    recovering the true outcome through the fallback below. Reading `state`
    is what lets the poll terminate on actual delivery. This is the one
    deliberate deviation from the pre-refactor removal code.

THE FALLBACK STAYS:
    A poll that did not reach a terminal state is not the same as a real
    failure — it is usually a transient read-timeout on a request Entra
    actually processed. The fallback asks the `assignments` resource directly
    (a different Graph endpoint) whether the package is still delivered. This
    is the backstop that kept the pre-refactor Leaver correct-but-slow; the
    field fix just makes it the rare exception instead of the primary path.

LOGGING:
    Only state transitions are logged during polling — not every poll attempt —
    so log output reads the same whether a package clears in 10 seconds or
    5 minutes.
"""

from __future__ import annotations
import logging
from dataclasses import dataclass

from Provisioning.graph_client import JmlGraphClient, GraphClientError

logger = logging.getLogger(__name__)

# assignmentRequests terminal `state` values (v1.0 API, lowercase). The field
# is `state`, NOT `requestState`. "delivered" is the success value for a
# removal request; denied / canceled / failed are the non-delivered terminals.
TERMINAL_REQUEST_STATES = frozenset({"delivered", "denied", "canceled", "failed"})


@dataclass
class PendingPackage:
    """
    Tracks one access package adminRemove request through submission and
    delivery.

    Serializable (str/bool only) so it can cross a Durable activity boundary
    as a dict when the poll loop becomes orchestrator-driven. No process-local
    clocks — those don't survive a handoff between activities on different
    workers.

    request_type is always "adminRemove" on the Leaver path — the field is kept
    for shape-parity with the Mover's PendingPackage, which uses both add and
    remove, so a copied helper reads the same in both pipelines.
    """
    access_package_id: str
    policy_id:         str
    request_type:      str  = "adminRemove"
    label:             str  = ""     # human label for logs only, never logic
    request_id:        str  = ""
    state:             str  = ""     # raw state from assignmentRequests
    previous_state:    str  = ""
    submitted:         bool = False
    error:             str  = ""

    def to_dict(self) -> dict:
        return {
            "access_package_id": self.access_package_id,
            "policy_id":         self.policy_id,
            "request_type":      self.request_type,
            "label":             self.label,
            "request_id":        self.request_id,
            "state":             self.state,
            "previous_state":    self.previous_state,
            "submitted":         self.submitted,
            "error":             self.error,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "PendingPackage":
        return cls(
            access_package_id= raw["access_package_id"],
            policy_id=         raw["policy_id"],
            request_type=      raw.get("request_type", "adminRemove"),
            label=             raw.get("label", ""),
            request_id=        raw.get("request_id", ""),
            state=             raw.get("state", ""),
            previous_state=    raw.get("previous_state", ""),
            submitted=         raw.get("submitted", False),
            error=             raw.get("error", ""),
        )


def submit_removals(
    graph_client:       JmlGraphClient,
    user_id:            str,
    current_packages:   frozenset[str],
    current_policy_map: dict[str, str],
    package_labels:     dict[str, str],
) -> list[PendingPackage]:
    """
    Submit an adminRemove for every currently held access package (ADR-014 —
    everything, no exclusions).

    No pre-check: a removal of something already gone just 404s harmlessly, so
    the Leaver removes unconditionally. policy_id comes from the user's actual
    current assignment (current_policy_map, captured at fetch), not a
    re-derivation — the real tenant assignment is authoritative. No poll, no
    sleep.
    """
    pending: list[PendingPackage] = []

    for package_id in current_packages:
        label = package_labels.get(package_id, package_id)
        policy_id = current_policy_map.get(package_id, "")

        pkg = PendingPackage(
            access_package_id= package_id,
            policy_id=         policy_id,
            request_type=      "adminRemove",
            label=             label,
        )

        if not policy_id:
            logger.warning(
                "  ⚠ %s — no assignmentPolicyId on the current assignment, "
                "submitting adminRemove with an empty policy_id anyway",
                label,
            )

        try:
            request = graph_client.request_package_assignment(
                user_id=user_id,
                access_package_id=package_id,
                policy_id=policy_id,
                request_type="adminRemove",
            )
            pkg.request_id = request.get("id", "")
            pkg.submitted = True
            pkg.state = "submitted"
            logger.info("  %s — adminRemove submitted (request_id=%s)", label, pkg.request_id)

        except GraphClientError as e:
            pkg.error = str(e)
            pkg.state = "submission_failed"
            logger.warning("  ✗ %s — removal submission failed: %s", label, e)

        pending.append(pkg)

    return pending


def poll_packages_once(
    pending:      list[PendingPackage],
    graph_client: JmlGraphClient,
) -> list[PendingPackage]:
    """
    One poll pass: fetch the `state` of each still-pending removal request and
    update it in place. Returns the same list. No sleep — the caller loops and
    owns the wait between passes (sync: time.sleep; durable: timer).

    Reads the `state` field (lowercase terminal values), never `requestState`.
    Only transitions are logged.
    """
    still_pending = [
        p for p in pending
        if p.submitted and p.state not in TERMINAL_REQUEST_STATES
    ]

    for pkg in still_pending:
        try:
            status = graph_client.get_assignment_request_status(pkg.request_id)
            new_state = status.get("state", "")
            if new_state and new_state != pkg.state:
                logger.info("  %s: %s → %s", pkg.label, pkg.state, new_state)
                pkg.previous_state = pkg.state
                pkg.state = new_state
        except GraphClientError as e:
            logger.warning("  Poll error for %s: %s", pkg.label, e)

    return pending


def packages_all_terminal(pending: list[PendingPackage]) -> bool:
    """True if every submitted removal request has reached a terminal `state`."""
    return not [
        p for p in pending
        if p.submitted and p.state not in TERMINAL_REQUEST_STATES
    ]


def finalize_removals(
    graph_client: JmlGraphClient,
    user_id:      str,
    pending:      list[PendingPackage],
) -> list[dict]:
    """
    Read final removal states, apply the fallback second-opinion to anything
    non-terminal, and return actions_taken. Individual failures are recorded
    but do not stop the set (ADR-014) — a partial removal still leaves less
    access than none, and post-offboarding verification surfaces what didn't
    clear.

    Fallback: if the `assignments` resource no longer shows a delivered
    assignment for the package, the removal succeeded regardless of what the
    request poll reported — a genuine second opinion from a different endpoint.
    """
    actions_taken: list[dict] = []
    removed_count = 0
    failed_count = 0

    for pkg in pending:
        label = pkg.label

        if not pkg.submitted:
            failed_count += 1
            actions_taken.append({
                "action":     "PackageRemoval",
                "package_id": pkg.access_package_id,
                "detail":     f"Removal failed: {pkg.error}",
                "succeeded":  False,
            })
            logger.warning("  ✗ %s — removal failed: %s", label, pkg.error)
            continue

        if pkg.state == "delivered":
            removed_count += 1
            actions_taken.append({
                "action":     "PackageRemoval",
                "package_id": pkg.access_package_id,
                "detail":     "Removed successfully",
                "succeeded":  True,
            })
            logger.info("  ✓ %s — removed", label)
            continue

        fallback = graph_client.check_package_assignment(
            user_id=user_id,
            access_package_id=pkg.access_package_id,
        )
        if not fallback or fallback.get("state") != "delivered":
            removed_count += 1
            actions_taken.append({
                "action":     "PackageRemoval",
                "package_id": pkg.access_package_id,
                "detail":     f"Removed — confirmed via fallback check after poll did not reach a terminal state (last known state={pkg.state})",
                "succeeded":  True,
            })
            logger.info(
                "  ✓ %s — removed (confirmed via fallback check; poll itself did not reach a terminal state)",
                label,
            )
        else:
            failed_count += 1
            actions_taken.append({
                "action":     "PackageRemoval",
                "package_id": pkg.access_package_id,
                "detail":     f"Removal did not confirm — state={pkg.state}, and fallback check still shows a delivered assignment",
                "succeeded":  False,
            })
            logger.warning(
                "  ✗ %s — removal did not confirm (state=%s, fallback check still shows delivered)",
                label, pkg.state,
            )

    if pending:
        logger.info("Step 4 complete — %d removed, %d failed", removed_count, failed_count)

    return actions_taken