"""
Provisioning/pim_client.py

PIM group eligibility assignment — delegates to JmlGraphClient.

The Graph API call itself lives in graph_client.py alongside all other
Graph operations. This module owns the result handling and audit logging
so provisioner.py stays clean.
"""

import logging
from dataclasses import dataclass
from Provisioning.graph_client import JmlGraphClient, GraphClientError

logger = logging.getLogger(__name__)


@dataclass
class PimEligibilityResult:
    """
    Result of a single PIM group eligibility assignment attempt.

    succeeded      — True if eligibility was assigned or already existed
    group_id       — object ID of the PIM group
    display_name   — human-readable group name for audit reporting
    schedule_id    — Graph schedule ID (empty if already existed)
    already_existed— True if the eligibility was already in place (idempotent)
    error          — error message if succeeded is False
    """
    succeeded:       bool
    group_id:        str
    display_name:    str
    schedule_id:     str  = ""
    already_existed: bool = False
    error:           str  = ""


def assign_pim_group_eligibility(
    graph_client:  JmlGraphClient,
    user_id:       str,
    group_id:      str,
    display_name:  str,
    eligible_role: str,
    justification: str,
    duration_hours: int = 8,
) -> PimEligibilityResult:
    """
    Add a user as an eligible member of a PIM-enabled security group.

    Delegates the Graph API call to JmlGraphClient.assign_pim_group_eligibility().
    Returns a structured result the provisioner can act on without
    knowing anything about the underlying API call.

    Inputs:
        graph_client   — authenticated JmlGraphClient instance
        user_id        — Entra object ID of the user being provisioned
        group_id       — object ID of the role-assignable PIM group
        display_name   — group name for log and audit context
        eligible_role  — Entra role the group is eligible for (audit trail only)
        justification  — business reason written to the eligibility record
        duration_hours — not used in the API call (PIM policy on the group
                         controls activation duration) — kept for audit trail
    """
    try:
        result = graph_client.assign_pim_group_eligibility(
            user_id=      user_id,
            group_id=     group_id,
            justification=justification,
        )

        return PimEligibilityResult(
            succeeded=       True,
            group_id=        group_id,
            display_name=    display_name,
            schedule_id=     result.get("schedule_id", ""),
            already_existed= result.get("already_existed", False),
        )

    except GraphClientError as e:
        error_str = str(e)
        logger.error(
            f"PIM eligibility assignment failed — "
            f"user={user_id}, group={display_name}, error={error_str}"
        )
        return PimEligibilityResult(
            succeeded=    False,
            group_id=     group_id,
            display_name= display_name,
            error=        error_str,
        )