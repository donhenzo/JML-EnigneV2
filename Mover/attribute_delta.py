"""

Pure attribute-level diff for Mover events.

Compares current Entra ID user attributes against incoming HR payload attributes
and returns a structured diff of what changed, what stayed the same, and what
fields were present in one source but missing in the other.

No Graph API calls. No I/O. No side effects.

The orchestrator is responsible for:
    - Fetching current attributes from Entra ID
    - Resolving manager employee_id to an Entra object ID before calling this module
    - Applying the resulting AttributeDelta to update the user object via Graph
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


# Tracked attributes

TRACKED_ATTRIBUTES: frozenset[str] = frozenset({
    "department",
    "jobTitle",
    "manager",           # resolved to Entra object ID before this function is called
    "officeLocation",
    "costCenter",        # extension attribute
    "employeeType",
    "usageLocation",
})



# Data models

@dataclass(frozen=True)
class AttributeChange:
    """
    A single attribute that changed value between current and incoming state.

    Fields:
        field:      The attribute name (one of TRACKED_ATTRIBUTES).
        from_value: The value currently held in Entra ID.
        to_value:   The value from the incoming HR payload.
    """
    field:      str
    from_value: Optional[str]
    to_value:   Optional[str]


@dataclass(frozen=True)
class AttributeDelta:
    """
    The complete attribute diff for a single Mover event.

    frozen=True so this can be used as a value object and compared
    directly in unit tests.

    Fields:
        changes:   Attributes whose values differ between current and incoming.
                   These are the fields the orchestrator will PATCH on the Entra object.
        unchanged: Attributes present in both sources with identical values.
                   Recorded for completeness — no action needed.
        missing:   Attributes present in the incoming payload but absent from
                   the current Entra snapshot. Treated as a new value being set.
        extra:     Attributes present in the current Entra snapshot but absent
                   from the incoming payload. Not touched — the engine does not
                   remove attributes it did not set.
    """
    changes:   tuple[AttributeChange, ...]
    unchanged: frozenset[str]
    missing:   frozenset[str]
    extra:     frozenset[str]

    def has_changes(self) -> bool:
        """Returns True if any tracked attribute value differs."""
        return len(self.changes) > 0

    def to_patch_dict(self) -> dict[str, Optional[str]]:
        """
        Returns a flat dict of changed attributes ready for a Graph API PATCH call.

        Only includes fields from changes and missing — the fields the orchestrator
        needs to write to Entra ID. Unchanged and extra fields are excluded.
        """
        return {change.field: change.to_value for change in self.changes}



# Delta calculation

def calculate_attribute_delta(
    current_attributes:  dict[str, Optional[str]],
    incoming_attributes: dict[str, Optional[str]],
) -> AttributeDelta:
    """
    Calculate the attribute-level diff between current Entra state and
    the incoming HR payload.

    Only attributes listed in TRACKED_ATTRIBUTES are evaluated.
    All other attributes are ignored regardless of what is passed in.

    Args:
        current_attributes:  Flat dict of the user's current Entra ID attributes.
                             Keys should match TRACKED_ATTRIBUTES field names.
                             manager value must already be resolved to an Entra object ID.
        incoming_attributes: Flat dict of the incoming HR payload attributes.
                             Same key naming convention as current_attributes.
                             manager value must already be resolved to an Entra object ID.

    Returns:
        AttributeDelta with changes, unchanged, missing, and extra sets.

    Notes:
        This function is deterministic. The same inputs always produce the
        same output. The orchestrator is responsible for any timestamping.
    """
    changes:   list[AttributeChange] = []
    unchanged: set[str] = set()
    missing:   set[str] = set()
    extra:     set[str] = set()

    for field in TRACKED_ATTRIBUTES:
        in_current  = field in current_attributes
        in_incoming = field in incoming_attributes

        # Present in incoming but absent from current — treat as a new value.
        if in_incoming and not in_current:
            missing.add(field)
            continue

        # Present in current but absent from incoming — do not touch.
        if in_current and not in_incoming:
            extra.add(field)
            continue

        # Present in both — compare values.
        if not in_current and not in_incoming:
            # Field not provided by either side — skip silently.
            continue

        current_value  = current_attributes[field]
        incoming_value = incoming_attributes[field]

        if current_value != incoming_value:
            changes.append(AttributeChange(
                field      = field,
                from_value = current_value,
                to_value   = incoming_value,
            ))
        else:
            unchanged.add(field)

    return AttributeDelta(
        changes   = tuple(changes),
        unchanged = frozenset(unchanged),
        missing   = frozenset(missing),
        extra     = frozenset(extra),
    )