"""
Mover/provisioning_phases.py

The Mover's package-provisioning execution layer, split into sleep-free phases
so the CALLER owns waiting. Mirrors Provisioning/provisioner.py (the Joiner's
proven phase structure): the synchronous driver composes these with time.sleep;
the Durable orchestrator composes the SAME functions with durable timers. The
phase functions never sleep — that seam is what lets the sync and durable paths
share one implementation.

This is a layer BELOW the stages. It knows about PendingPackage, JmlGraphClient,
and plain audit-action dicts — nothing about StageResult, stages, or drivers.
It does not import stage_result.py. A thin stage wrapper adapts phase output
into a StageResult when composing.

PHASE STRUCTURE (two loops — additions then removals, ADR-009):
    submit_additions          — submit all adminAdd requests, return pending
    submit_removals           — submit all adminRemove requests, return pending
    poll_packages_once        — one poll pass over pending (no sleep), shared
    packages_all_terminal     — have all submitted packages reached terminal
    finalize_additions        — fallback-confirm, write audit actions, return
                                (actions, delivered, all_succeeded)  ← ADR-009 gate
    finalize_removals         — fallback-confirm, write audit actions, return actions
    apply_attribute_update    — PATCH changed attributes via httpx (patch_user)

TWO GRAPH RESOURCES, TWO FIELD NAMES (both correct, not an inconsistency):
    assignmentRequests → poll reads `requestState`, terminal values are
                         capitalized: Delivered / Denied / Failed / Canceled.
    assignments        → fallback reads `state`, delivered value is lowercase
                         "delivered". This is the second-opinion resource, a
                         genuinely different Graph endpoint from the one polled.

LOGGING:
    Only state transitions are logged during polling — not every poll attempt —
    so log output is the same whether a package takes 10 seconds or 5 minutes.
"""

from __future__ import annotations
import logging
from dataclasses import dataclass

from Provisioning.graph_client import JmlGraphClient, GraphClientError

logger = logging.getLogger(__name__)

# assignmentRequests terminal requestState values — capitalized, read raw
# (never lowercased). The Mover keys off `== "Delivered"` exactly.
TERMINAL_REQUEST_STATES = frozenset({"Delivered", "Denied", "Failed", "Canceled"})


@dataclass
class PendingPackage:
    """
    Tracks one access package assignmentRequest through submission and delivery.

    Serializable (str/bool only) so it can cross a Durable activity boundary as
    a dict when the poll loop becomes orchestrator-driven. No process-local
    clocks — those don't survive a handoff between activities on different
    workers.

    request_type distinguishes the two loops: "adminAdd" or "adminRemove". The
    poll and terminal-check logic is identical for both; submit and finalize
    differ, which is why request_type rides on the record.
    """
    access_package_id: str
    policy_id:         str
    request_type:      str          # "adminAdd" | "adminRemove"
    label:             str  = ""     # human label for logs only, never logic
    request_id:        str  = ""
    state:             str  = ""     # raw requestState from assignmentRequests
    previous_state:    str  = ""
    submitted:         bool = False
    skipped:           bool = False  # additions only — already delivered
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
            "skipped":           self.skipped,
            "error":             self.error,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "PendingPackage":
        return cls(
            access_package_id= raw["access_package_id"],
            policy_id=         raw["policy_id"],
            request_type=      raw["request_type"],
            label=             raw.get("label", ""),
            request_id=        raw.get("request_id", ""),
            state=             raw.get("state", ""),
            previous_state=    raw.get("previous_state", ""),
            submitted=         raw.get("submitted", False),
            skipped=           raw.get("skipped", False),
            error=             raw.get("error", ""),
        )


# Shared poll — identical for additions and removals

def poll_packages_once(
    pending:      list[PendingPackage],
    graph_client: JmlGraphClient,
) -> list[PendingPackage]:
    """
    One poll pass: fetch requestState of each still-pending package and update
    its state in place. Returns the same list. No sleep — the caller loops and
    owns the wait between passes (sync: time.sleep; durable: timer). Reads the
    raw `requestState` value (capitalized), never lowercased. Only transitions
    are logged.
    """
    still_pending = [
        p for p in pending
        if p.submitted and p.state not in TERMINAL_REQUEST_STATES
    ]

    for pkg in still_pending:
        try:
            status = graph_client.get_assignment_request_status(pkg.request_id)
            new_state = status.get("requestState", "")
            if new_state and new_state != pkg.state:
                logger.info("  %s: %s → %s", pkg.label, pkg.state, new_state)
                pkg.previous_state = pkg.state
                pkg.state = new_state
        except GraphClientError as e:
            logger.warning("  Poll error for %s: %s", pkg.label, e)

    return pending


def packages_all_terminal(pending: list[PendingPackage]) -> bool:
    """True if every submitted (non-skipped) package has reached a terminal requestState."""
    return not [
        p for p in pending
        if p.submitted and p.state not in TERMINAL_REQUEST_STATES
    ]


# Additions (ADR-009 Strategy A: add first, confirm delivered)

def submit_additions(
    graph_client:    JmlGraphClient,
    user_id:         str,
    packages_to_add: frozenset[str],
    policy_map:      dict[str, str],
    package_labels:  dict[str, str],
) -> list[PendingPackage]:
    """
    Submit an adminAdd for each package to add. Idempotent via
    check_package_assignment: a package already delivered is marked skipped and
    not re-requested. A package with no resolved policy_id is recorded as a
    failed submission (submitted=False, error set) so finalize can gate on it.
    No poll, no sleep.
    """
    pending: list[PendingPackage] = []

    for package_id in packages_to_add:
        label = package_labels.get(package_id, package_id)
        policy_id = policy_map.get(package_id)

        pkg = PendingPackage(
            access_package_id= package_id,
            policy_id=         policy_id or "",
            request_type=      "adminAdd",
            label=             label,
        )

        if not policy_id:
            pkg.error = "No policy_id resolved for this package — cannot submit request"
            pkg.state = "submission_failed"
            logger.warning("  ✗ %s — no policy_id resolved, cannot submit", label)
            pending.append(pkg)
            continue

        try:
            existing = graph_client.check_package_assignment(
                user_id=user_id,
                access_package_id=package_id,
            )
            if existing and existing.get("state") == "delivered":
                pkg.skipped = True
                pkg.state = "Delivered"
                logger.info("  ✓ %s — already delivered, skipped", label)
                pending.append(pkg)
                continue

            request = graph_client.request_package_assignment(
                user_id=user_id,
                access_package_id=package_id,
                policy_id=policy_id,
                request_type="adminAdd",
            )
            pkg.request_id = request.get("id", "")
            pkg.submitted = True
            pkg.state = "submitted"
            logger.info("  %s — adminAdd submitted (request_id=%s)", label, pkg.request_id)

        except GraphClientError as e:
            pkg.error = str(e)
            pkg.state = "submission_failed"
            logger.warning("  ✗ %s — addition submission failed: %s", label, e)

        pending.append(pkg)

    return pending


def finalize_additions(
    graph_client: JmlGraphClient,
    user_id:      str,
    pending:      list[PendingPackage],
    packages_to_add: frozenset[str],
) -> tuple[list[dict], frozenset[str], bool]:
    """
    Read final addition states, apply the fallback second-opinion to anything
    non-terminal, and return (actions_taken, delivered, all_succeeded).

    all_succeeded is the ADR-009 gate: True only if every package in
    packages_to_add reached Delivered (or the set was empty). Gates Step 7
    removals + attribute patch in the driver — the phase does not decide flow.

    Fallback: a package that did not reach Delivered via the poll is checked
    against the `assignments` resource (state == "delivered", lowercase) — a
    genuine second opinion from a different endpoint, since a poll that never
    reached a terminal state is usually a transient read-timeout on a package
    that actually delivered.
    """
    actions_taken: list[dict] = []
    delivered: set[str] = set()
    all_succeeded = True
    delivered_count = 0
    failed_count = 0

    for pkg in pending:
        label = pkg.label

        if pkg.skipped:
            delivered.add(pkg.access_package_id)
            delivered_count += 1
            actions_taken.append({
                "action":     "PackageAddition",
                "package_id": pkg.access_package_id,
                "detail":     "Already delivered — skipped (idempotent)",
                "succeeded":  True,
            })
            continue

        if not pkg.submitted:
            # Submission failed (no policy_id or a submit error).
            all_succeeded = False
            failed_count += 1
            detail = (
                "No policy_id resolved for this package — cannot submit request"
                if pkg.error.startswith("No policy_id")
                else f"Addition failed: {pkg.error}"
            )
            actions_taken.append({
                "action":     "PackageAddition",
                "package_id": pkg.access_package_id,
                "detail":     detail,
                "succeeded":  False,
            })
            continue

        if pkg.state == "Delivered":
            delivered.add(pkg.access_package_id)
            delivered_count += 1
            actions_taken.append({
                "action":     "PackageAddition",
                "package_id": pkg.access_package_id,
                "detail":     "Delivered",
                "succeeded":  True,
            })
            logger.info("  ✓ %s — added", label)

        elif pkg.state in ("Denied", "Failed", "Canceled"):
            all_succeeded = False
            failed_count += 1
            actions_taken.append({
                "action":     "PackageAddition",
                "package_id": pkg.access_package_id,
                "detail":     f"AdditionDeniedByPlatform — requestState={pkg.state}",
                "succeeded":  False,
            })
            logger.warning("  ✗ %s — rejected by platform (requestState=%s)", label, pkg.state)

        else:
            # Non-terminal within the poll window — fallback second opinion.
            fallback = graph_client.check_package_assignment(
                user_id=user_id,
                access_package_id=pkg.access_package_id,
            )
            if fallback and fallback.get("state") == "delivered":
                delivered.add(pkg.access_package_id)
                delivered_count += 1
                actions_taken.append({
                    "action":     "PackageAddition",
                    "package_id": pkg.access_package_id,
                    "detail":     f"Delivered — confirmed via fallback check after poll did not reach a terminal state (last known state={pkg.state})",
                    "succeeded":  True,
                })
                logger.info(
                    "  ✓ %s — added (confirmed via fallback check; poll itself did not reach a terminal state)",
                    label,
                )
            else:
                all_succeeded = False
                failed_count += 1
                actions_taken.append({
                    "action":     "PackageAddition",
                    "package_id": pkg.access_package_id,
                    "detail":     f"Did not reach a terminal state within the poll window, and fallback check found no delivered assignment — last known state={pkg.state}",
                    "succeeded":  False,
                })
                logger.warning(
                    "  ✗ %s — no confirmation within poll window and fallback check found nothing delivered (last known state=%s)",
                    label, pkg.state,
                )

    if packages_to_add:
        logger.info("Step 6 complete — %d added, %d failed", delivered_count, failed_count)

    return actions_taken, frozenset(delivered), all_succeeded


# Removals (ADR-009: only after additions all delivered; gated in the driver)

def submit_removals(
    graph_client:       JmlGraphClient,
    user_id:            str,
    remove_confirmed:   frozenset[str],
    current_policy_map: dict[str, str],
    package_labels:     dict[str, str],
) -> list[PendingPackage]:
    """
    Submit an adminRemove for each confirmed removal. No pre-check — a removal
    of something already gone just 404s harmlessly (deliberate asymmetry with
    additions, which do pre-check). policy_id comes from the user's actual
    current assignment (current_policy_map, captured at fetch), not a
    re-derivation — the real tenant assignment is authoritative. No sleep.
    """
    pending: list[PendingPackage] = []

    for package_id in remove_confirmed:
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
                "  ⚠ %s — no assignmentPolicyId found on the current assignment, "
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


def finalize_removals(
    graph_client: JmlGraphClient,
    user_id:      str,
    pending:      list[PendingPackage],
    remove_confirmed: frozenset[str],
) -> list[dict]:
    """
    Read final removal states, apply the fallback second-opinion to anything
    non-terminal, and return actions_taken. Individual failures are recorded
    but do not stop the set — a partial removal still leaves less stale access
    than none, and post-move verification surfaces what didn't clear.

    Fallback (mirror of additions, inverted): if the `assignments` resource no
    longer shows a delivered assignment for the package, the removal succeeded
    regardless of what the request poll reported.
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

        if pkg.state == "Delivered":
            removed_count += 1
            actions_taken.append({
                "action":     "PackageRemoval",
                "package_id": pkg.access_package_id,
                "detail":     "Removed successfully",
                "succeeded":  True,
            })
            logger.info("  ✓ %s — removed", label)
        else:
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
                    "detail":     f"Removal did not confirm — requestState={pkg.state}, and fallback check still shows a delivered assignment",
                    "succeeded":  False,
                })
                logger.warning(
                    "  ✗ %s — removal did not confirm (requestState=%s, fallback check still shows delivered)",
                    label, pkg.state,
                )

    if remove_confirmed:
        logger.info("Step 7 complete — %d removed, %d failed", removed_count, failed_count)

    return actions_taken


# Attribute update

def apply_attribute_update(
    graph_client: JmlGraphClient,
    user_id:      str,
    patch_dict:   dict,
) -> tuple[bool, str]:
    """
    PATCH the user's changed Entra attributes via the httpx patch_user method.

    manager and usageLocation are excluded from the body. manager needs a
    separate Graph endpoint; usageLocation needs an ISO 3166-1 alpha-2 country
    code and the source carries city names. An all-excluded patch is a no-op
    (returns success without a Graph call).

    Returns (succeeded, error_message).
    """
    body = {
        field: value
        for field, value in patch_dict.items()
        if field not in ("manager", "usageLocation")
    }

    if not body:
        return True, ""

    try:
        graph_client.patch_user(user_id, body)
        logger.info("Attribute update applied — user=%s, fields=%s", user_id, list(body.keys()))
        return True, ""
    except GraphClientError as e:
        logger.error("Attribute update failed — user=%s, error=%s", user_id, str(e))
        return False, str(e)