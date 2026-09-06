"""
Hold_queue/queue_manager.py
Manages hold queue state transitions and record lifecycle.
Enforces valid state transitions. Logs all transitions. Persists records
to Azure Table Storage in production; uses in-memory dict for local/test.
Phase 0 scope: in-memory store with a swap interface for Table Storage.
The storage backend is injected so the manager logic is testable without
an Azure dependency.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from typing import Protocol

from Hold_queue.models import (
    APPROVABLE_STATES,
    TERMINAL_STATES,
    HoldRecord,
    HoldStatus,
)
from Ingestion.schema import IdentityPayload

logger = logging.getLogger(__name__)


# Storage backend protocol 

class HoldQueueStore(Protocol):
    """
    Interface for hold queue persistence.
    Implement this for Azure Table Storage in production. The in-memory
    implementation below satisfies this protocol for local use and tests.
    """
    def save(self, record: HoldRecord) -> None: ...
    def get(self, record_id: str) -> HoldRecord | None: ...
    def list_by_status(self, status: HoldStatus) -> list[HoldRecord]: ...
    def list_by_employee(self, employee_id: str) -> list[HoldRecord]: ...


# In-memory store 

class InMemoryHoldQueueStore:
    """
    In-memory hold queue store for local development and testing.
    Not suitable for production — no persistence across function invocations.
    Swap with AzureTableHoldQueueStore when connecting to Azure Storage.
    """
    def __init__(self) -> None:
        self._records: dict[str, HoldRecord] = {}

    def save(self, record: HoldRecord) -> None:
        self._records[record.record_id] = record

    def get(self, record_id: str) -> HoldRecord | None:
        return self._records.get(record_id)

    def list_by_status(self, status: HoldStatus) -> list[HoldRecord]:
        return [r for r in self._records.values() if r.status == status]

    def list_by_employee(self, employee_id: str) -> list[HoldRecord]:
        return [r for r in self._records.values() if r.employee_id == employee_id]

    def all(self) -> list[HoldRecord]:
        """Return all records. For testing / inspection only."""
        return list(self._records.values())


# Valid state transitions 
# Maps current state → set of valid next states.
# Any transition not in this map is rejected at runtime.
#
VALID_TRANSITIONS: dict[HoldStatus, set[HoldStatus]] = {
    HoldStatus.RECEIVED:             {HoldStatus.NORMALIZED, HoldStatus.NORMALIZATION_FAILED},
    HoldStatus.NORMALIZED:           {HoldStatus.HELD, HoldStatus.VALIDATION_FAILED},
    HoldStatus.NORMALIZATION_FAILED: {HoldStatus.HELD},
    HoldStatus.VALIDATION_FAILED:    {HoldStatus.HELD},
    HoldStatus.HELD:                 {HoldStatus.APPROVED, HoldStatus.FAILED},
    HoldStatus.APPROVED:             {HoldStatus.PROVISIONING},
    HoldStatus.PROVISIONING:         {HoldStatus.PROVISIONED, HoldStatus.FAILED},
    HoldStatus.PROVISIONED:          {HoldStatus.COMPLETED, HoldStatus.FAILED},
    HoldStatus.COMPLETED:            set(),           # Terminal
    HoldStatus.FAILED:               {HoldStatus.HELD},  # Can be reset for retry
}


# Queue manager 

class HoldQueueManager:
    """
    Manages the hold queue state machine.

    Purpose:
        Create, transition, and retrieve hold records. Enforce valid state
        transitions. All changes are persisted via the injected store.

    Inputs:
        store: Any object satisfying the HoldQueueStore protocol.

    Security considerations:
        Manual overrides are explicitly flagged and must be traceable in
        the audit log. The manager records override metadata but does not
        make the provisioning decision — that responsibility stays in the
        calling layer.
    """
    def __init__(self, store: HoldQueueStore) -> None:
        self._store = store

    # Record creation

    def create_from_parse_error(
        self,
        employee_id: str,
        upn:         str,
        reasons:     list[str],
        raw_row:     dict | None = None,
    ) -> HoldRecord:
        """
        Create a hold record for a CSV parse failure.

        The record starts at RECEIVED and immediately transitions to HELD
        with the parse error reasons. Parse errors cannot proceed to
        normalization.
        """
        record = HoldRecord(
            record_id=       _new_id(),
            employee_id=     employee_id,
            upn=             upn,
            status=          HoldStatus.RECEIVED,
            failure_reasons= reasons,
            payload_snapshot=json.dumps(raw_row) if raw_row else None,
        )
        self._store.save(record)
        logger.info(
            "Hold record created from parse error: %s (employee=%s)",
            record.record_id,
            employee_id,
        )
        self.transition(record, HoldStatus.NORMALIZATION_FAILED)
        self.transition(record, HoldStatus.HELD)
        return record

    def create_from_normalization_failure(
        self,
        payload: IdentityPayload,
        reasons: list[str],
    ) -> HoldRecord:
        """
        Create a hold record for a normalization failure.

        Starts at RECEIVED → NORMALIZATION_FAILED → HELD.
        """
        record = HoldRecord(
            record_id=       _new_id(),
            employee_id=     payload.employee_id,
            upn=             payload.upn,
            status=          HoldStatus.RECEIVED,
            failure_reasons= reasons,
            payload_snapshot=_serialize_payload(payload),
        )
        self._store.save(record)
        self.transition(record, HoldStatus.NORMALIZATION_FAILED)
        self.transition(record, HoldStatus.HELD)
        logger.info(
            "Hold record created from normalization failure: %s (employee=%s)",
            record.record_id,
            payload.employee_id,
        )
        return record

    def create_from_validation_failure(
        self,
        payload: IdentityPayload,
        reasons: list[str],
    ) -> HoldRecord:
        """
        Create a hold record for a pre-provision validation failure.

        Raised by the PowerShell governance validation gate — covers
        identity attribute rules (missing manager, duplicate UPN, invalid
        employment type, etc.).

        Starts at RECEIVED → NORMALIZED → VALIDATION_FAILED → HELD.

        For SoD-specific violations use create_from_sod_violation() so
        the hold record is clearly distinguishable in operator review and
        the audit trail.
        """
        record = HoldRecord(
            record_id=       _new_id(),
            employee_id=     payload.employee_id,
            upn=             payload.upn,
            status=          HoldStatus.RECEIVED,
            failure_reasons= reasons,
            payload_snapshot=_serialize_payload(payload),
        )
        self._store.save(record)
        self.transition(record, HoldStatus.NORMALIZED)
        self.transition(record, HoldStatus.VALIDATION_FAILED)
        self.transition(record, HoldStatus.HELD)
        logger.info(
            "Hold record created from validation failure: %s (employee=%s)",
            record.record_id,
            payload.employee_id,
        )
        return record

    def create_from_sod_violation(
        self,
        payload:    IdentityPayload,
        violations: list,               # list[SoDViolation] — not typed here to
                                        # avoid a circular import between Hold_queue
                                        # and Governance. Caller passes the list,
                                        # this method serialises it.
    ) -> HoldRecord:
        """
        Create a hold record for a Separation of Duties violation.

        WHY THIS IS SEPARATE FROM create_from_validation_failure():
            SoD blocks and PowerShell governance blocks both route through
            VALIDATION_FAILED → HELD, but they have different remediation
            paths. A missing-manager block is fixed by updating the HR record.
            An SoD block requires an exception approval workflow or a policy
            change. Keeping them in separate hold records means an operator
            looking at the hold queue can immediately distinguish the two
            categories and route them to the right remediation owner.

        STATE PATH:
            RECEIVED → NORMALIZED → VALIDATION_FAILED → HELD

        FAILURE REASONS FORMAT:
            Each SoDViolation is serialised into a structured string so the
            hold record is self-contained for operator review:
                [SOD-001] Payment Approver / Payment Processor (Critical/Block)
                Conflicting groups: SG_Finance_PaymentApprovers, SG_Finance_PaymentProcessors
                Compensating control: Requires CISO sign-off.

        Inputs:
            payload:    Normalised IdentityPayload for the blocked identity.
            violations: List of SoDViolation objects from sod_checker.

        Output:
            HoldRecord in HELD state with structured SoD reasons and a
            payload snapshot for operator review.
        """
        reasons = _serialise_sod_violations(violations)

        record = HoldRecord(
            record_id=       _new_id(),
            employee_id=     payload.employee_id,
            upn=             payload.upn,
            status=          HoldStatus.RECEIVED,
            failure_reasons= reasons,
            payload_snapshot=_serialize_payload(payload),
        )
        self._store.save(record)
        self.transition(record, HoldStatus.NORMALIZED)
        self.transition(record, HoldStatus.VALIDATION_FAILED)
        self.transition(record, HoldStatus.HELD)

        logger.warning(
            "Hold record created from SoD violation: %s "
            "(employee=%s, violations=%d, policies=%s)",
            record.record_id,
            payload.employee_id,
            len(violations),
            [v.policy_id for v in violations],
        )
        return record

    def create_from_sod_block(
        self,
        payload:          IdentityPayload,
        blocked_packages: list[dict],
    ) -> HoldRecord:
        """
        Create a hold record for a Mover pre-flight SoD block (ADR-020).

        Called when the Mover pre-flight gate determines that a package the
        move would add is incompatible with a package the user is keeping.
        The event cannot proceed without operator review.

        State path: RECEIVED -> NORMALIZED -> VALIDATION_FAILED -> HELD

        Inputs:
            payload:          normalised IdentityPayload for the blocked event.
            blocked_packages: list of dicts from stage_preflight_sod, each
                              with package_id, package_label, conflicting_packages,
                              conflicting_labels.

        Output:
            HoldRecord in HELD state with structured SoD block reasons.
        """
        reasons = _serialise_sod_blocks(blocked_packages)

        record = HoldRecord(
            record_id=       _new_id(),
            employee_id=     payload.employee_id,
            upn=             payload.upn,
            status=          HoldStatus.RECEIVED,
            failure_reasons= reasons,
            payload_snapshot=_serialize_payload(payload),
        )
        self._store.save(record)
        self.transition(record, HoldStatus.NORMALIZED)
        self.transition(record, HoldStatus.VALIDATION_FAILED)
        self.transition(record, HoldStatus.HELD)

        logger.warning(
            "Hold record created from Mover SoD block (ADR-020): %s "
            "(employee=%s, blocked_packages=%d)",
            record.record_id,
            payload.employee_id,
            len(blocked_packages),
        )
        return record

    # State transitions

    def transition(self, record: HoldRecord, new_status: HoldStatus) -> None:
        """
        Transition a hold record to a new state.

        Purpose:
            Enforce valid state transitions and persist the change.

        Raises:
            ValueError if the transition is not permitted from the current state.
        """
        allowed = VALID_TRANSITIONS.get(record.status, set())
        if new_status not in allowed:
            raise ValueError(
                f"Invalid transition for record {record.record_id}: "
                f"{record.status.value} → {new_status.value}. "
                f"Allowed from {record.status.value}: "
                f"{[s.value for s in allowed]}"
            )
        old_status     = record.status
        record.status  = new_status
        record.last_updated = datetime.utcnow()
        self._store.save(record)
        logger.info(
            "Hold record %s: %s → %s (employee=%s)",
            record.record_id,
            old_status.value,
            new_status.value,
            record.employee_id,
        )

    def approve(
        self,
        record:        HoldRecord,
        override_note: str | None = None,
    ) -> None:
        """
        Manually approve a held record for provisioning.

        Purpose:
            Allow an operator to release a held record. Sets manual_override
            flag so the audit log can trace the exception.

        Raises:
            ValueError if the record is not in an approvable state.
        """
        if record.status not in APPROVABLE_STATES:
            raise ValueError(
                f"Record {record.record_id} cannot be approved from state "
                f"'{record.status.value}'. Must be in: "
                f"{[s.value for s in APPROVABLE_STATES]}"
            )
        record.manual_override = True
        record.override_note   = override_note
        self.transition(record, HoldStatus.APPROVED)
        logger.warning(
            "Hold record %s manually approved by operator "
            "(employee=%s, note=%r). "
            "This override must appear in the audit log.",
            record.record_id,
            record.employee_id,
            override_note,
        )

    def record_attempt(self, record: HoldRecord) -> None:
        """
        Record a provisioning attempt against a hold record.

        Increments retry_count and sets last_attempt timestamp.
        Does not change state — caller transitions to PROVISIONING separately.
        """
        record.retry_count  += 1
        record.last_attempt  = datetime.utcnow()
        record.last_updated  = datetime.utcnow()
        self._store.save(record)

    # Queries 

    def get_held_records(self) -> list[HoldRecord]:
        """Return all records currently in the HELD state."""
        return self._store.list_by_status(HoldStatus.HELD)

    def get_failed_records(self) -> list[HoldRecord]:
        """Return all records in the FAILED state."""
        return self._store.list_by_status(HoldStatus.FAILED)

    def get_by_employee(self, employee_id: str) -> list[HoldRecord]:
        """Return all hold records for a given employee."""
        return self._store.list_by_employee(employee_id)


# Helpers 

def _new_id() -> str:
    return str(uuid.uuid4())


def _serialize_payload(payload: IdentityPayload) -> str:
    """
    Serialize an IdentityPayload to JSON for storage in the hold record.

    Stores a snapshot so the record is self-contained for operator review
    even if the original CSV is no longer available.
    """
    return json.dumps({
        "employee_id":    payload.employee_id,
        "upn":            payload.upn,
        "display_name":   payload.display_name,
        "department":     payload.department,
        "job_title":      payload.job_title,
        "manager_id":     payload.manager_id,
        "start_date":     payload.start_date.isoformat(),
        "employment_type":payload.employment_type.value,
        "location":       payload.location,
        "action":         payload.action.value,
        "retain_roles":   payload.retain_roles,
        "retain_list":    payload.retain_list,
    })


def _serialise_sod_violations(violations: list) -> list[str]:
    """
    Serialise a list of SoDViolation objects into structured reason strings.

    Each string is self-contained for operator review — it carries the
    policy ID, name, severity, action, conflicting groups, and any
    compensating control guidance.

    Format per violation:
        [SOD-001] Payment Approver / Payment Processor (Critical/Block)
        Conflicting groups: SG_Finance_PaymentApprovers, SG_Finance_PaymentProcessors
        Compensating control: Requires CISO sign-off.

    This format is written to:
        - HoldRecord.failure_reasons  (hold queue)
        - DecisionReport.hold_reasons (audit report)

    Keeping both in sync ensures an operator reading either surface sees
    the same structured information.
    """
    reasons = []
    for v in violations:
        groups = ", ".join(v.conflicting_groups)
        line = (
            f"[{v.policy_id}] {v.policy_name} "
            f"({v.risk_rating.value}/{v.action.value})\n"
            f"Conflicting groups: {groups}"
        )
        if v.compensating_control:
            line += f"\nCompensating control: {v.compensating_control}"
        if v.exception_applied:
            line += "\nException applied — violation permitted by pre-approved exception."
        reasons.append(line)
    return reasons


def _serialise_sod_blocks(blocked_packages: list[dict]) -> list[str]:
    """
    Serialise Mover pre-flight SoD block data into structured reason strings.

    Format per blocked package:
        [SoD Pre-Flight] Package "Finance Approver" (abc-123)
        Conflicts with kept packages: Treasury Admin (def-456), Payments (ghi-789)
        Action: BLOCK — event held for operator review (ADR-020).
    """
    reasons = []
    for b in blocked_packages:
        conflicts = ", ".join(
            f"{label} ({pid})"
            for label, pid in zip(b["conflicting_labels"], b["conflicting_packages"])
        )
        line = (
            f"[SoD Pre-Flight] Package \"{b['package_label']}\" ({b['package_id']})\n"
            f"Conflicts with kept packages: {conflicts}\n"
            f"Action: BLOCK — event held for operator review (ADR-020)."
        )
        reasons.append(line)
    return reasons