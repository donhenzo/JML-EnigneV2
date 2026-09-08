"""
Ingestion/hr_api/bamboohr/payload_builder.py

Single place where raw HR mapper output becomes a typed IdentityPayload.

Kept in its own module so the webhook can import it without pulling in
the full pipeline_adapter dependency tree (which requires Functions.*,
Hold_queue.*, Audit.*, etc. — modules that may not be on sys.path in
every Azure Function).
"""

import logging
from datetime import date as date_type

from Ingestion.schema import IdentityPayload, EmploymentType, JmlAction

logger = logging.getLogger(__name__)


def build_identity_payload(mapped: dict, lookup: dict) -> IdentityPayload:
    """
    Convert a raw mapper dict into a clean IdentityPayload.

    Handles employment type resolution via the canonical lookup table,
    date parsing, Leaver-specific termination date logic, and stripping
    of extra fields (bamboohr_id, termination_date) that are not part
    of the IdentityPayload contract.

    Both the synchronous pipeline adapter and the webhook dispatcher
    call this function.

    Args:
        mapped: raw identity dict from bamboohr_mapper.map_to_raw_identity()
                Must include 'action' set by the action deriver.
        lookup: canonical lookup table from load_lookup_table()

    Returns:
        IdentityPayload ready for normalisation and pipeline entry.

    Raises:
        ValueError: if a required field has an invalid value that cannot
                    be resolved (e.g. unknown employment type on a
                    non-Leaver record).
    """
    employee_id = mapped.get("employee_id", "unknown")
    action_str = mapped.get("action", "")

    # Resolve raw employment type to canonical enum value.
    # BambooHR sends "Full-Time", the enum expects "Employee".
    raw_emp_type = mapped.get("employment_type", "")
    emp_type_lookup = lookup.get("employment_type", {})
    resolved_emp_type = emp_type_lookup.get(raw_emp_type.lower(), raw_emp_type)

    # Parse start_date string to a date object.
    # For Leaver events, use termination_date instead of hireDate so the
    # event ID hash (EmployeeId + Action + StartDate) produces a distinct
    # event from the original Joiner.
    if action_str == "Leaver" and mapped.get("termination_date"):
        start_date_str = mapped["termination_date"]
    else:
        start_date_str = mapped.get("start_date", "")
    try:
        start_date = date_type.fromisoformat(start_date_str) if start_date_str else date_type.today()
    except ValueError:
        logger.warning(
            "Invalid start_date '%s' for employee %s — using today",
            start_date_str, employee_id
        )
        start_date = date_type.today()

    # For Leaver records, employment_type may not parse cleanly —
    # fall back to EMPLOYEE rather than failing on a field the Leaver
    # pipeline never reads (ADR-014).
    try:
        employment_type = EmploymentType(resolved_emp_type)
    except ValueError:
        if action_str == "Leaver":
            employment_type = EmploymentType.EMPLOYEE
        else:
            raise ValueError(
                f"Invalid employment type '{resolved_emp_type}' "
                f"for employee {employee_id}"
            )

    return IdentityPayload(
        employee_id=employee_id,
        upn=mapped.get("upn", "unknown"),
        display_name=mapped.get("display_name", ""),
        department=mapped.get("department", "") if action_str != "Leaver" else None,
        job_title=mapped.get("job_title", "") if action_str != "Leaver" else None,
        manager_id=mapped.get("manager_id") or None,
        start_date=start_date,
        employment_type=employment_type,
        location=mapped.get("location") or None,
        action=JmlAction(action_str),
        retain_roles=mapped.get("retain_roles", False),
        retain_list=mapped.get("retain_list", []),
    )
