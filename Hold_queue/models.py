"""
Hold_queue/models.py

State model for the JML hold queue.

A record is held whenever it cannot proceed through the pipeline without
human intervention. The hold queue carries formal states — "held" is not
a state, it is a bucket. Operators must be able to release, retry, or
reject records safely.

This module defines the data structures only. State transitions are
enforced in queue_manager.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional



# Hold queue state lifecycle
class HoldStatus(str, Enum):
    """
    Formal state lifecycle for a held identity record.

    Transitions are enforced by queue_manager.py. Not all transitions
    are valid from every state — see queue_manager for the allowed graph.
    """
    RECEIVED            = "Received"           # Ingested from CSV
    NORMALIZED          = "Normalized"         # Lookup applied successfully
    NORMALIZATION_FAILED = "NormalizationFailed"  # Unknown attribute, cannot resolve
    VALIDATION_FAILED   = "ValidationFailed"   # Passed normalization, failed pre-provision rules
    HELD                = "Held"               # Awaiting manual review or override
    APPROVED            = "Approved"           # Manually released for provisioning
    PROVISIONING        = "Provisioning"       # Actively being processed
    PROVISIONED         = "Provisioned"        # Complete, pending post-validation
    COMPLETED           = "Completed"          # Post-validation passed, audit log written
    FAILED              = "Failed"             # Provisioning or post-validation error


# States from which a record can be manually approved for provisioning.
APPROVABLE_STATES = {HoldStatus.HELD}

# States that represent a terminal outcome — no further processing.
TERMINAL_STATES = {HoldStatus.COMPLETED, HoldStatus.FAILED}

# States that represent an active hold requiring human attention.
ACTIVE_HOLD_STATES = {
    HoldStatus.NORMALIZATION_FAILED,
    HoldStatus.VALIDATION_FAILED,
    HoldStatus.HELD,
}


# Hold queue record
@dataclass
class HoldRecord:
    """
    A single record in the hold queue.

    Purpose:
        Track an identity event that could not proceed automatically.
        Carries enough inforamtion for an operator to understand what failed
        and decide on how to handle it.

    Security considerations:
        ManualOverride marks records where an operator explicitly approved
        provisioning despite a failed validation. These must be written
        to the audit log with the override flag set — they are exceptions
        to normal policy and must be traceable.
    """

    # Unique identifier for this hold record.
    record_id: str

    # The EmployeeId from the identity payload.
    employee_id: str

    # UPN from the identity payload. May be partial/constructed.
    upn: str

    # Current lifecycle state.
    status: HoldStatus

    # Human-readable description of why this record was held.
    # Multiple reasons are stored separately, not concatenated.
    failure_reasons: list[str] = field(default_factory=list)

    # UTC timestamp of the most recent status transition.
    last_updated: datetime = field(default_factory=datetime.utcnow)

    # UTC timestamp of the last provisioning attempt (None if not yet attempted).
    last_attempt: Optional[datetime] = None

    # Number of provisioning attempts made.
    retry_count: int = 0

    # True if an operator explicitly approved this record despite failures.
    # Must be written to the audit log.
    manual_override: bool = False

    # Optional operator note added at approval time.
    override_note: Optional[str] = None

    # Serialized IdentityPayload at the time of hold (JSON string).
    # Stored so the record is self-contained for operator review.
    payload_snapshot: Optional[str] = None

    def is_held(self) -> bool:
        """True if this record is currently awaiting human action."""
        return self.status in ACTIVE_HOLD_STATES

    def is_terminal(self) -> bool:
        """True if this record has reached a final state."""
        return self.status in TERMINAL_STATES

    def can_be_approved(self) -> bool:
        """True if an operator can release this record for provisioning."""
        return self.status in APPROVABLE_STATES

    def __repr__(self) -> str:
        return (
            f"HoldRecord("
            f"record_id={self.record_id!r}, "
            f"employee_id={self.employee_id!r}, "
            f"status={self.status.value!r})"
        )