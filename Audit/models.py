"""
Audit/models.py

Decision output report schema for the JML engine.

Every identity event produces one DecisionReport — written regardless of
success or failure. This is the primary audit trail. The report schema
is defined here; report_writer.py handles serialization and persistence.

All fields are defined in the architecture decision log (Decision 3).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

# Version of the JML engine writing this report.
# Increment this when the engine schema changes in a non-backward-compatible way.
ENGINE_VERSION = "0.1.0"


# Enumerations
class ReportEvent(str, Enum):
    JOINER          = "Joiner"
    MOVER           = "Mover"
    LEAVER          = "Leaver"
    RECONCILIATION  = "Reconciliation"


class ValidationStatus(str, Enum):
    PASSED  = "Passed"
    FAILED  = "Failed"
    SKIPPED = "Skipped"  # Not yet reached (e.g. held before validation ran)


class NormalizationStatus(str, Enum):
    PASSED       = "Passed"
    FAILED       = "Failed"
    PARTIAL_HOLD = "PartialHold"  # Some fields resolved, some did not



# Action record
@dataclass
class ActionRecord:
    """
    A single discrete action executed during provisioning.

    Examples:
        ActionRecord(action="UserCreated", detail="UPN: alice@contoso.com")
        ActionRecord(action="AddedToGroup", detail="Sales-Team")
        ActionRecord(action="AssignedRBACRole", detail="Contributor on /subscriptions/...")
    """
    action: str
    detail: str = ""
    # UTC timestamp when this action completed.
    timestamp: datetime = field(default_factory=datetime.utcnow)
    # True if this action succeeded; False if it failed mid-sequence.
    succeeded: bool = True



# Decision report
@dataclass
class DecisionReport:
    """
    The structured audit output for a single JML identity event.

    Purpose:
        Provide a complete, immutable record of what the engine did (or
        attempted to do) for one identity lifecycle event. Written to
        persistent storage regardless of success or failure.

    Security considerations:
        This is the authoritative audit trail. It must be written before
        the function returns. A missing report is an audit gap. Reports
        must never be deleted — only appended to.

    Fields are a direct implementation of Decision 3 (Decision Output
    Report Schema) in the architecture decision log.
    """

    # Identity identifiers 

    # UPN of the user this event relates to.
    upn: str

    # HR source identifier.
    employee_id: str

    # Event metadata

    event: ReportEvent

    # UTC timestamp when the engine began processing this event.
    timestamp: datetime = field(default_factory=datetime.utcnow)

    # Version of the engine that produced this report.
    engine_version: str = ENGINE_VERSION

    # Azure Function invocation ID for correlation with platform logs.
    correlation_id: Optional[str] = None

    # Processing outcomes 
    validation_status: ValidationStatus = ValidationStatus.SKIPPED
    normalization_status: NormalizationStatus = NormalizationStatus.PASSED

    # Actions 
    # Ordered list of actions executed. Populated as each step runs so a
    # partial failure report shows exactly what succeeded before the error.
    actions_taken: list[ActionRecord] = field(default_factory=list)

    #  Warnings 
    # Non-blocking issues noted during processing. Examples:
    # - manually assigned group memberships not managed by the engine
    # - location value not in lookup table (kept raw value)
    warnings: list[str] = field(default_factory=list)

    # Hold / failure information 
    # Populated if the record was held at any stage.
    hold_reasons: list[str] = field(default_factory=list)

    # True if a manual operator override was used to release a held record.
    manual_override: bool = False

    # Operator note attached at override time (if any).
    override_note: Optional[str] = None

    # ID of the hold record this event was released from (if applicable).
    hold_record_id: Optional[str] = None

    # Convenience methods

    def add_action(
        self,
        action: str,
        detail: str = "",
        succeeded: bool = True,
    ) -> None:
        """Append a completed action to the report."""
        self.actions_taken.append(
            ActionRecord(action=action, detail=detail, succeeded=succeeded)
        )

    def add_warning(self, warning: str) -> None:
        """Append a non-blocking warning to the report."""
        self.warnings.append(warning)

    def add_hold_reason(self, reason: str) -> None:
        """Append a hold reason. Call once per failure."""
        self.hold_reasons.append(reason)

    @property
    def overall_success(self) -> bool:
        """
        True if the event completed successfully.

        Requires:
            - normalization passed (or partial hold was cleared by operator)
            - validation passed
            - all actions succeeded
        """
        if self.validation_status == ValidationStatus.FAILED:
            return False
        if self.normalization_status == NormalizationStatus.FAILED:
            return False
        failed_actions = [a for a in self.actions_taken if not a.succeeded]
        return len(failed_actions) == 0

    def __repr__(self) -> str:
        return (
            f"DecisionReport("
            f"upn={self.upn!r}, "
            f"event={self.event.value!r}, "
            f"success={self.overall_success}, "
            f"actions={len(self.actions_taken)})"
        )