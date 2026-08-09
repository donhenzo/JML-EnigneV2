"""
BambooHR Field Mapper

Responsibility: translate BambooHR API field names to the JML engine's
raw identity field shape. That is all this module does.

Output is intentionally raw — field values are NOT normalised here.
The existing Normalization/normalizer.py handles canonical resolution.

Separation of concerns:
    bamboohr_client.py  → knows BambooHR's API
    bamboohr_mapper.py  → knows the field name translation (this file)
    normalizer.py       → knows canonical values
    action_deriver.py   → knows Joiner vs Mover vs Leaver

Two IDs exist in BambooHR:
    id              → BambooHR's internal auto-assigned numeric ID (used for API calls)
    employeeNumber  → the HR-meaningful ID set by the organisation (e.g. "Acc001")

The JML engine uses employeeNumber as employee_id because:
    - It is the ID HR teams know and use
    - It appears on payslips, contracts, and org charts
    - It is portable across HR systems (if you switch from BambooHR, the employee number stays)
    - The EventId hash (SHA-256 of EmployeeId + Action + StartDate) must use the stable HR ID

The BambooHR internal id is kept as bamboohr_id so the fetcher can still make API calls
when needed (e.g. delta poll returns an internal ID, fetcher needs it for the URL).

BambooHR field         → IdentityPayload field
─────────────────────────────────────────────────
employeeNumber         → employee_id (HR-meaningful)
id                     → bamboohr_id (API-internal, not in IdentityPayload)
firstName + lastName   → display_name
workEmail              → upn
department             → department (raw)
jobTitle               → job_title (raw)
employmentHistoryStatus → employment_type (raw, e.g. "Full-Time")
hireDate               → start_date
terminationDate        → termination_date (used as start_date for Leaver events)
status                 → status (Active/Inactive — drives Leaver detection)
supervisorEId          → manager_id
location               → location (raw)
"""

import logging

logger = logging.getLogger(__name__)


def map_to_raw_identity(bamboohr_record: dict) -> dict:
    """
    Translate a single BambooHR employee record into the raw identity
    shape the JML pipeline expects.

    Does NOT set the 'action' field — that is determined by the
    action deriver after the fetch and map steps complete.

    Args:
        bamboohr_record: raw dict from bamboohr_client.get_employee()

    Returns:
        dict matching the raw IdentityPayload field structure.
        Null/missing BambooHR fields become empty strings — the
        normaliser will catch these and route to the hold queue.

        Also includes 'bamboohr_id' for API lookups — this field
        is NOT part of IdentityPayload and is stripped before
        the record enters the pipeline.
    """
    first = bamboohr_record.get("firstName") or ""
    last = bamboohr_record.get("lastName") or ""
    display_name = f"{first} {last}".strip()

    # supervisorEId is the numeric manager ID from BambooHR.
    # It comes as a string like "9" or None if no manager is assigned.
    supervisor = bamboohr_record.get("supervisorEId")
    manager_id = str(supervisor) if supervisor else ""

    # employeeNumber is the HR-meaningful ID (e.g. "Acc001").
    # If the organisation has not set it, fall back to BambooHR's internal ID.
    employee_number = bamboohr_record.get("employeeNumber") or ""
    bamboohr_id = str(bamboohr_record.get("id", ""))
    employee_id = employee_number if employee_number else bamboohr_id

    mapped = {
        "employee_id":      employee_id,
        "bamboohr_id":      bamboohr_id,
        "upn":              bamboohr_record.get("workEmail") or "",
        "display_name":     display_name,
        "department":       bamboohr_record.get("department") or "",
        "job_title":        bamboohr_record.get("jobTitle") or "",
        "employment_type":  bamboohr_record.get("employmentHistoryStatus") or "",
        "start_date":       bamboohr_record.get("hireDate") or "",
        "termination_date": bamboohr_record.get("terminationDate") or "",
        "status":           bamboohr_record.get("status") or "",
        "manager_id":       manager_id,
        "location":         bamboohr_record.get("location") or "",
        "action":           "",
        "retain_roles":     False,
        "retain_list":      [],
    }

    logger.debug(
        "Mapped BambooHR employee %s (internal: %s) → %s (%s, %s, status=%s)",
        employee_id, bamboohr_id, display_name,
        mapped["department"], mapped["job_title"], mapped["status"]
    )

    return mapped