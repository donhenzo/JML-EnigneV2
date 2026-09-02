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
    INTERN = "Intern"


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
    #  Core identity properties — required and normalized. These are the fields that uniquely identify. 
    
    employee_id: str                           # Unique HR source identifier
    upn: str                                   # User principal name — constructed or provided
    display_name: str                          # Normalized full name
    department: Optional[str]                  # Normalized via canonical lookup; None = unresolved
    job_title: Optional[str]                   # Normalized via canonical lookup; None = unresolved
    start_date: date                           # ISO 8601 — enforced as a date object, not a string
    employment_type: EmploymentType            # Employee / Contractor / Guest

    # Lifecycle control
    action: JmlAction                          # Joiner / Mover / Leaver

    # Optional source attributes — absent-tolerant. manager_id and location
    # may not arrive from every HR source or every lifecycle event; both are
    # already treated as non-fatal downstream (manager is resolved best-effort
    # and excluded from the attribute PATCH; location feeds usageLocation,
    # which is also excluded from the PATCH). Defaulted so a payload without
    # them constructs cleanly rather than failing at the schema boundary.
    manager_id: Optional[str] = None           # EmployeeId of the manager
    location: Optional[str] = None             # Office or region

    # Mover-specific retention behavior
    retain_roles: bool = False
    retain_list: list[str] = field(default_factory=list)
    source: str = "BAMBOOHR"

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

    def to_dict(self) -> dict:
        """
        Plain-dict form for crossing a stage/activity boundary as JSON.
        Enums flatten to their .value, date to ISO string. source and the
        derived synthetic_id are included so a stage that needs provenance
        doesn't have to recompute it.
        """
        return {
            "employee_id":     self.employee_id,
            "upn":             self.upn,
            "display_name":    self.display_name,
            "department":      self.department,
            "job_title":       self.job_title,
            "manager_id":      self.manager_id,
            "start_date":      self.start_date.isoformat(),
            "employment_type": self.employment_type.value,
            "location":        self.location,
            "action":          self.action.value,
            "source":          self.source,
            "retain_roles":    self.retain_roles,
            "retain_list":     self.retain_list,
            "synthetic_id":    self.synthetic_id,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "IdentityPayload":
        """
        Reconstruct from the dict form. Inverse of to_dict. synthetic_id is
        a computed property, not a field, so it's ignored on the way back in
        — it's re-derived from source + employee_id.
        """
        return cls(
            employee_id=     raw["employee_id"],
            upn=             raw["upn"],
            display_name=    raw["display_name"],
            department=      raw.get("department"),
            job_title=       raw.get("job_title"),
            manager_id=      raw.get("manager_id"),
            start_date=      date.fromisoformat(raw["start_date"]),
            employment_type= EmploymentType(raw["employment_type"]),
            location=        raw.get("location"),
            action=          JmlAction(raw["action"]),
            source=          raw.get("source", "BAMBOOHR"),
            retain_roles=    raw.get("retain_roles", False),
            retain_list=     raw.get("retain_list", []),
        )