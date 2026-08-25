# provisioning/provisioner.py
#
# Joiner provisioning execution layer.
#
# WHY THIS EXISTS:
#   The mapping resolver decides what entitlements a new joiner gets.
#   The Graph client knows how to call the API.
#   This file owns the sequence and orchestrates those two pieces into
#   a single, audited, retry-safe provisioning run for one identity.
#
# SEQUENCE:
#   1. UPN existence check — retry guard, skip creation if user already exists
#   2. Create Entra ID user
#   3. Submit all access package requests (ADR-007), then poll until every
#      package reaches a terminal state.
#   4. PIM group eligibility assignment — unchanged, non-blocking.
#
# PHASE STRUCTURE (for Durable Functions):
#   The provisioning phases are separated into sleep-free functions so the
#   CALLER owns the waiting. The synchronous provision_joiner() composes them
#   with time.sleep; the Durable orchestrator composes the same functions with
#   durable timers (create_timer) instead. The phase functions themselves never
#   sleep — that seam is what lets both the sync and durable paths share one
#   implementation instead of duplicating provisioning logic.
#
#   check_or_create_user          — create/find user, return entra_id (no sleep)
#   submit_access_packages        — Phase 1: submit all requests, return pending
#   poll_access_packages_once     — one poll pass over pending (no sleep)
#   packages_all_terminal         — have all submitted packages reached terminal
#   record_access_package_results — Phase 3: write report actions, set failure
#
# LOGGING:
#   Only state transitions are logged during polling — not every poll attempt —
#   so log output is the same whether a package takes 10 seconds or 5 minutes.

import logging
import time
from dataclasses import dataclass

from Audit.models import DecisionReport
from Ingestion.schema import IdentityPayload
from Mapping.mapping_resolver import EntitlementResult
from Provisioning.graph_client import (
    JmlGraphClient,
    GraphClientError,
    UserNotFoundError,
)
from Provisioning.pim_client import assign_pim_group_eligibility, PimEligibilityResult


logger = logging.getLogger(__name__)

TERMINAL_STATES = {"delivered", "denied", "failed", "canceled"}

PACKAGE_POLL_INTERVAL_SECONDS = 5.0
PACKAGE_POLL_MAX_ATTEMPTS = 60

USER_PROPAGATION_WAIT_SECONDS = 15


@dataclass
class ProvisioningResult:
    succeeded:      bool = False
    entra_id:       str  = ""
    failure_step:   str  = ""
    failure_detail: str  = ""


@dataclass
class PendingPackage:
    """
    Tracks one access package assignment through submission and delivery.

    Serializable (str/bool only) so it can cross a Durable activity boundary as
    a dict when the poll loop becomes orchestrator-driven. The monotonic
    submitted_at timing was removed for this reason — process-local clocks don't
    survive a handoff between activities on different workers.
    """
    rule_id:           str
    access_package_id: str
    policy_id:         str
    request_id:        str  = ""
    state:             str  = ""
    previous_state:    str  = ""
    submitted:         bool = False
    skipped:           bool = False
    error:             str  = ""


def provision_joiner(
    payload:      IdentityPayload,
    entitlements: EntitlementResult,
    report:       DecisionReport,
    graph_client: JmlGraphClient,
    event_status: str = ""
) -> ProvisioningResult:
    """
    Synchronous Joiner provisioning. Composes the sleep-free phase functions
    with time.sleep for the waits. The Durable orchestrator composes the SAME
    phase functions with durable timers instead — this function is the sync
    caller, not the only caller.
    """
    result = ProvisioningResult()

    entra_id = check_or_create_user(payload, report, graph_client, event_status, result)
    if not entra_id:
        return result
    result.entra_id = entra_id

    # Propagation wait — Entitlement Management resolves targetId against a
    # separate index that lags user creation; without this the first package
    # assignment can fail with SubjectNotFound. Only wait after a real creation,
    # not a retry-resume where the user already exists.
    logger.info(f"Waiting {USER_PROPAGATION_WAIT_SECONDS}s for user propagation...")
    time.sleep(USER_PROPAGATION_WAIT_SECONDS)

    if entitlements.access_packages:
        pending = submit_access_packages(
            entra_id, entitlements.access_packages, report, graph_client, result
        )
        if pending is None:
            return result

        for _ in range(PACKAGE_POLL_MAX_ATTEMPTS):
            if packages_all_terminal(pending):
                break
            poll_access_packages_once(pending, graph_client)
            if packages_all_terminal(pending):
                break
            time.sleep(PACKAGE_POLL_INTERVAL_SECONDS)

        if not record_access_package_results(pending, report, result):
            return result

    if entitlements.pim_groups:
        pim_ok = _assign_pim_eligibility(
            user_id=     entra_id,
            employee_id= payload.employee_id,
            pim_groups=  entitlements.pim_groups,
            report=      report,
            graph_client=graph_client,
            result=      result,
        )
        if not pim_ok:
            return result

    result.succeeded = True
    logger.info(
        f"Provisioning complete — employee={payload.employee_id}, "
        f"upn={payload.upn}, entra_id={entra_id}"
    )
    return result


def check_or_create_user(
    payload:      IdentityPayload,
    report:       DecisionReport,
    graph_client: JmlGraphClient,
    event_status: str,
    result:       ProvisioningResult
) -> str:
    """
    Find or create the Entra user. Returns entra_id, or "" on failure (setting
    result.failure_*). Does NOT wait for propagation — the caller owns that wait
    (sync path: time.sleep; durable path: an orchestrator timer). This is the
    change from the old _check_or_create_user, which slept internally.
    """
    try:
        existing = graph_client.get_user(payload.upn)

        if event_status == "Processing":
            report.add_action(
                action="UserCreationSkipped",
                detail=(
                    f"User {payload.upn} already exists — "
                    f"resuming from retry. object_id={existing['id']}"
                ),
                succeeded=True
            )
            logger.info(
                f"Retry detected — user exists, skipping creation — "
                f"employee={payload.employee_id}, entra_id={existing['id']}"
            )
            return existing["id"]

        report.add_action(
            action="UserCreationFailed",
            detail=(
                f"UPN {payload.upn} already exists in Entra ID "
                "and this is not a retry. Possible duplicate identity."
            ),
            succeeded=False
        )
        result.failure_step   = "UserCreation"
        result.failure_detail = f"UPN conflict: {payload.upn} already exists"
        logger.error(
            f"UPN conflict — employee={payload.employee_id}, upn={payload.upn}"
        )
        return ""

    except UserNotFoundError:
        pass

    except GraphClientError as e:
        report.add_action(
            action="UserCreationFailed",
            detail=f"Graph API error checking UPN existence: {e}",
            succeeded=False
        )
        result.failure_step   = "UPNCheck"
        result.failure_detail = str(e)
        return ""

    try:
        created = graph_client.create_user(payload)
        report.add_action(
            action="UserCreated",
            detail=f"upn={created['upn']}, object_id={created['id']}",
            succeeded=True
        )
        logger.info(
            f"User created — employee={payload.employee_id}, "
            f"upn={payload.upn}, entra_id={created['id']}"
        )
        return created["id"]

    except GraphClientError as e:
        report.add_action(
            action="UserCreationFailed",
            detail=str(e),
            succeeded=False
        )
        result.failure_step   = "UserCreation"
        result.failure_detail = str(e)
        logger.error(
            f"User creation failed — employee={payload.employee_id}: {e}"
        )
        return ""


def submit_access_packages(
    user_id:         str,
    access_packages,
    report:          DecisionReport,
    graph_client:    JmlGraphClient,
    result:          ProvisioningResult,
) -> list[PendingPackage] | None:
    """
    Phase 1: check-existing and submit each access package request. Returns the
    pending list (carrying request_ids for the poll phase), or None on a
    submission failure (setting result.failure_*). No poll, no sleep.
    """
    pending: list[PendingPackage] = []

    for ap in access_packages:
        pkg = PendingPackage(
            rule_id=ap.rule_id,
            access_package_id=ap.access_package_id,
            policy_id=ap.policy_id,
        )

        try:
            existing = graph_client.check_package_assignment(user_id, ap.access_package_id)

            if existing:
                pkg.skipped = True
                pkg.state = "skipped"
                report.add_action(
                    action="AccessPackageAssignmentSkipped",
                    detail=(
                        f"package={ap.access_package_id} (rule={ap.rule_id}) "
                        "— assignment already exists, skipped (retry)"
                    ),
                    succeeded=True,
                )
                logger.info(f"  {ap.rule_id} — already assigned, skipped")
                pending.append(pkg)
                continue

            request = graph_client.request_package_assignment(
                user_id=user_id,
                access_package_id=ap.access_package_id,
                policy_id=ap.policy_id,
                duration_override_days=ap.duration_override_days,
            )
            pkg.request_id = request.get("id", "")
            pkg.submitted = True
            pkg.state = "submitted"
            logger.info(f"  {ap.rule_id} — request submitted (request_id={pkg.request_id})")

        except GraphClientError as e:
            pkg.error = str(e)
            pkg.state = "submission_failed"
            report.add_action(
                action="AccessPackageSubmissionFailed",
                detail=f"package={ap.access_package_id} (rule={ap.rule_id}): {e}",
                succeeded=False,
            )
            logger.error(f"  {ap.rule_id} — submission failed: {e}")

        pending.append(pkg)

    submission_failures = [p for p in pending if p.state == "submission_failed"]
    if submission_failures:
        result.failure_step = "AccessPackageSubmission"
        result.failure_detail = f"{len(submission_failures)} package(s) failed to submit"
        return None

    return pending


def poll_access_packages_once(
    pending:      list[PendingPackage],
    graph_client: JmlGraphClient,
) -> list[PendingPackage]:
    """
    One poll pass: fetch the current status of each still-pending package and
    update its state in place. Returns the same list. No sleep — the caller
    loops and owns the wait between passes (sync: time.sleep; durable: timer).
    Only state transitions are logged, so repeated same-state polls are silent.
    """
    still_pending = [
        p for p in pending if p.submitted and p.state not in TERMINAL_STATES
    ]

    for pkg in still_pending:
        try:
            status = graph_client.get_assignment_request_status(pkg.request_id)
            new_state = status.get("state", "").lower()
            if new_state != pkg.state:
                logger.info(f"  {pkg.rule_id}: {pkg.state} → {new_state}")
                pkg.previous_state = pkg.state
                pkg.state = new_state
        except GraphClientError as e:
            logger.warning(f"  Poll error for {pkg.rule_id}: {e}")

    return pending


def packages_all_terminal(pending: list[PendingPackage]) -> bool:
    """True if every submitted (non-skipped) package has reached a terminal state."""
    return not [
        p for p in pending if p.submitted and p.state not in TERMINAL_STATES
    ]


def record_access_package_results(
    pending: list[PendingPackage],
    report:  DecisionReport,
    result:  ProvisioningResult,
) -> bool:
    """
    Phase 3: read final package states, write the report action per package, and
    set result.failure_* if any package did not deliver. Returns all_ok. No sleep.
    """
    total_submitted = len([p for p in pending if p.submitted])
    total_delivered = 0
    total_failed = 0
    all_ok = True

    for pkg in pending:
        if pkg.skipped:
            continue

        if pkg.state == "delivered":
            total_delivered += 1
            report.add_action(
                action="AccessPackageAssigned",
                detail=f"package={pkg.access_package_id} (rule={pkg.rule_id})",
                succeeded=True,
            )

        elif pkg.state == "denied":
            total_failed += 1
            report.add_action(
                action="AccessPackageAssignmentDenied",
                detail=(
                    f"package={pkg.access_package_id} (rule={pkg.rule_id}) "
                    "— denied by Entra, likely Separation of Duties "
                    "incompatibility (ADR-008)"
                ),
                succeeded=False,
            )
            all_ok = False

        elif pkg.state in TERMINAL_STATES:
            total_failed += 1
            report.add_action(
                action="AccessPackageAssignmentFailed",
                detail=f"package={pkg.access_package_id} (rule={pkg.rule_id}) — state={pkg.state}",
                succeeded=False,
            )
            all_ok = False

        else:
            total_failed += 1
            report.add_action(
                action="AccessPackageAssignmentTimeout",
                detail=(
                    f"package={pkg.access_package_id} (rule={pkg.rule_id}) "
                    f"— still in state={pkg.state} after "
                    f"{PACKAGE_POLL_MAX_ATTEMPTS * PACKAGE_POLL_INTERVAL_SECONDS:.0f}s"
                ),
                succeeded=False,
            )
            all_ok = False

    if all_ok:
        logger.info(
            f"  Provisioning complete — submitted: {total_submitted}, "
            f"delivered: {total_delivered}"
        )
    else:
        logger.error(
            f"  Provisioning incomplete — submitted: {total_submitted}, "
            f"delivered: {total_delivered}, failed: {total_failed}"
        )
        failed_pkgs = [p for p in pending if not p.skipped and p.state != "delivered"]
        result.failure_step = "AccessPackageAssignment"
        result.failure_detail = ", ".join(f"{p.rule_id}={p.state}" for p in failed_pkgs)

    return all_ok


def _assign_pim_eligibility(
    user_id:      str,
    employee_id:  str,
    pim_groups:   list,
    report:       DecisionReport,
    graph_client: JmlGraphClient,
    result:       ProvisioningResult,
) -> bool:
    """PIM failure is non-blocking — returns True always. UNCHANGED from before."""
    for pim_group in pim_groups:
        pim_result = assign_pim_group_eligibility(
            graph_client=  graph_client,
            user_id=       user_id,
            group_id=      pim_group.group_id,
            display_name=  pim_group.display_name,
            eligible_role= pim_group.eligible_role,
            justification= pim_group.justification,
            duration_hours=pim_group.duration_hours,
        )

        if pim_result.succeeded:
            detail = (
                f"group={pim_group.display_name} — "
                f"eligible for {pim_group.eligible_role}"
            )
            if pim_result.already_existed:
                detail += " (already existed — retry)"
            report.add_action(
                action="PimEligibilityAssigned",
                detail=detail,
                succeeded=True,
            )
            logger.info(
                f"PIM eligibility assigned — employee={employee_id}, "
                f"group={pim_group.display_name}, role={pim_group.eligible_role}"
            )

        else:
            error_msg = pim_result.error or ""
            is_license_error = (
                "AadPremiumLicenseRequired" in error_msg
                or "P2" in error_msg
                or "Governance license" in error_msg
            )

            if is_license_error:
                detail = (
                    f"group={pim_group.display_name} — "
                    f"eligible for {pim_group.eligible_role} — "
                    f"skipped: tenant requires Entra ID P2 for PIM. "
                    f"Access package assignments completed successfully."
                )
                report.add_action(
                    action="PimEligibilitySkipped",
                    detail=detail,
                    succeeded=True,
                )
                report.add_warning(detail)
                logger.warning(
                    f"PIM eligibility skipped — no P2 license — "
                    f"employee={employee_id}, group={pim_group.display_name}"
                )

            else:
                detail = (
                    f"group={pim_group.display_name} — "
                    f"eligible for {pim_group.eligible_role} — "
                    f"error: {pim_result.error}"
                )
                report.add_action(
                    action="PimEligibilityFailed",
                    detail=detail,
                    succeeded=False,
                )
                report.add_warning(
                    f"PIM eligibility failed (non-blocking) — "
                    f"group={pim_group.display_name}, "
                    f"role={pim_group.eligible_role} — "
                    f"provisioning continued. Error: {pim_result.error}"
                )
                logger.warning(
                    f"PIM eligibility failed (non-blocking) — "
                    f"employee={employee_id}, group={pim_group.display_name}, "
                    f"error={pim_result.error}"
                )

    return True