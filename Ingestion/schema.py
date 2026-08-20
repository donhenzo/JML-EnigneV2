"""
Ingestion/schema.py

Canonical identity schema for the JML engine.

This is the single data contract. Every downstream component — normalization,
validation, provisioning, audit — reads from this object. No component
accepts raw CSV or ad-hoc field names.

All fields are defined here. Normalization populates them. Nothing downstream
invents new fields.
"""

from __future__ import annotations
import hashlib
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Optional

#  Identity lifecycle actions supported by the JML pipeline.
# These actions determine how downstream provisioning behaves.

class JmlAction(str, Enum):
    JOINER = "Joiner"
    MOVER = "Mover"
    LEAVER = "Leaver"


class EmploymentType(str, Enum):
    EMPLOYEE = "Employee"
    CONTRACTOR = "Contractor"
    GUEST = "Guest"


@dataclass
class IdentityPayload:
    """
    Canonical identity object.

    Constructed from a CSV row after normalization. This is the only object
    passed between pipeline stages. Raw CSV values must not leak downstream.

    Fields marked Optional may be absent from source data but must be
    explicitly set to None — no field should be missing from the object.

    Security consideration: this object travels through provisioning and
    audit layers. Never log the full object in production without scrubbing
    sensitive fields (e.g. manager relationships, employment type).
    """

    #  Core identity properties 
    employee_id: str                           # Unique HR source identifier
    upn: str                                   # User principal name — constructed or provided
    display_name: str                          # Normalized full name
    department: Optional[str]                  # Normalized via canonical lookup; None = unresolved
    job_title: Optional[str]                   # Normalized via canonical lookup; None = unresolved
    manager_id: Optional[str]                  # EmployeeId of the manager
    start_date: date                           # ISO 8601 — enforced as a date object, not a string
    employment_type: EmploymentType            # Employee / Contractor / Guest
    location: Optional[str]                    # Office or region

    # Lifecycle control 
    action: JmlAction                          # Joiner / Mover / Leaver

    # Mover-specific retention behavior
    #  # When True, existing access should be preserved during a mover event.
    # This prevents automatic role removal during department transitions. 
    retain_roles: bool = False                 # True = retain all existing access across transition
    retain_list: list[str] = field(default_factory=list)  # Selective list of role/group IDs to retain

    # provides which HR source this identity came from. Source-qualifies
    # the synthetic identity ID (ADR-017) so records from different HR systems
    # (BambooHR, later OrangeHRM) can't collide on the same employee_id.
    # Defaults to the only source in use today; a second source's mapper sets
    # this explicitly.
    source: str = "BAMBOOHR"

    #  Normalization state set by the normalization layer, not parsed from CSV 
    normalization_passed: bool = False
    normalization_failures: list[str] = field(default_factory=list)
   
    #Determine whether the payload contains enough information to proceed through the pipeline.
    def is_normalizable(self) -> bool:
        """True only when all required normalizable fields are resolved."""
        return self.department is not None and self.job_title is not None

    @property
    def synthetic_id(self) -> str:
        """
        Deterministic per-identity ID: sha256(source:employee_id), 32 hex chars.

        Distinct from the event store's per-event EventId (ADR-017): the synthetic
        ID is stable across every event and lifecycle stage for one identity, where
        EventId is deliberately per-event. This is the handle for the pre-provision
        synthetic snapshot; the synthetic_id -> entra_object_id mapping and the
        last-state store that also key on it are deferred to their own work items.

        Lowercased before hashing to match generate_event_id's normalization, so
        casing drift between ingestion paths can't produce two IDs for one identity.
        """
        raw = f"{self.source}:{self.employee_id}".lower()
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    # helps with Backwards compatibility alias for older callers.
    def is_normalized(self) -> bool:
        return self.is_normalizable()