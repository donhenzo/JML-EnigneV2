"""
Delta engine for calculating group membership changes in Mover events.

Pure delta calculation for Mover events.

Computes four group sets from the difference between a user's current Entra ID
group membership and their target membership derived from role_mapping_rules.json.

No Graph API calls. No I/O. No side effects.
All inputs and outputs are frozensets to enforce immutability.
The orchestrator is responsible for timestamping audit records.
"""

from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class MoverDelta:
    """
    The result of a delta calculation for a single Mover event.

    All fields are frozensets of group object IDs (strings).
    This dataclass is frozen so it can be used safely as a value object
    and compared directly in unit tests without equality edge cases.

    Fields:
        groups_to_add:    Groups the user needs but does not currently hold.
        groups_to_remove: Groups the user currently holds but should not after the move.
                          Does not include unmanaged groups — those are never touched.
        unchanged:        Groups the user holds that are still correct after the move.
        unmanaged:        Groups the user holds that do not appear in the managed
                          catalogue. These are excluded from all delta logic.
    """
    groups_to_add: frozenset[str]
    groups_to_remove: frozenset[str]
    unchanged: frozenset[str]
    unmanaged: frozenset[str]


def calculate_delta(
    current_groups: frozenset[str],
    target_groups: frozenset[str],
    managed_catalogue: frozenset[str],
) -> MoverDelta:
    """
    Calculate the group membership delta for a Mover event.

    Args:
        current_groups:    Group object IDs the user currently holds in Entra ID.
        target_groups:     Group object IDs the user should hold after the move,
                           as resolved by mapping_resolver against the new role payload.
        managed_catalogue: All group object IDs defined anywhere in role_mapping_rules.json.
                           Used to identify unmanaged groups in current membership.

    Returns:
        MoverDelta with four non-overlapping group sets.

    Security:
        Unmanaged groups are never placed in groups_to_remove. The engine
        does not touch access it does not own. Unmanaged groups are surfaced
        in the audit record for human review.

    Notes:
        This function is deterministic. The same inputs always produce the
        same output. The caller is responsible for any timestamping.
    """

    # Groups the user holds that the managed catalogue does not recognise.
    # These are excluded from all delta logic — not removed, not evaluated for SoD.
    unmanaged = current_groups - managed_catalogue

    # Restrict delta calculation to managed groups only.
    # Unmanaged groups are invisible to the delta engine from this point.
    managed_current = current_groups & managed_catalogue

    # Standard set operations across the managed subset.
    groups_to_add    = target_groups - managed_current
    groups_to_remove = managed_current - target_groups
    unchanged        = managed_current & target_groups

    return MoverDelta(
        groups_to_add=groups_to_add,
        groups_to_remove=groups_to_remove,
        unchanged=unchanged,
        unmanaged=unmanaged,
    )