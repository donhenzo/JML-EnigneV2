"""
BambooHR API Client — Fetcher Layer

Responsibility: fetch raw employee data from BambooHR and return it unchanged.
This module knows about BambooHR's API. Nothing else.

No normalisation. No action derivation. No pipeline logic.
Everything returned here is raw — downstream modules handle interpretation.

Security note: credentials are loaded from config/hr_api_config.json
which is gitignored. Never commit API keys to version control.
Environment variables are supported as a fallback for CI/CD or Azure Functions.
"""

import os
import json
import logging
from pathlib import Path
from requests import Session
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError, Timeout, ConnectionError

logger = logging.getLogger(__name__)

# Path to the credentials config file, relative to the project root.
_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent.parent / "Config" / "hr_api_config.json"

# Fields fetched per individual employee call.
# The directory endpoint only returns a subset — these additional fields
# are required by the JML engine and must be fetched separately.
#
# hireDate              → start_date in IdentityPayload
# employmentHistoryStatus → employment_type (Employee / Contractor etc.)
# supervisorEId         → numeric manager employee ID (not a name string)
#
_INDIVIDUAL_FIELDS = [
    "employeeNumber",       # HR-meaningful ID (e.g. "Acc001") — this is YOUR employee ID
    "firstName",
    "lastName",
    "workEmail",
    "department",
    "jobTitle",
    "employmentHistoryStatus",
    "location",
    "supervisorEId",        # numeric manager ID — NOT the name string from directory
    "hireDate",
]


def _get_credentials() -> tuple[str, str]:
    """
    Load BambooHR credentials from config/hr_api_config.json.
    Falls back to environment variables if the config file is missing
    (e.g. in Azure Functions where secrets come from app settings).

    The config file is gitignored — credentials never reach version control.
    """
    # Try config file first
    if _CONFIG_PATH.exists():
        try:
            with open(_CONFIG_PATH, "r") as f:
                config = json.load(f)

            bamboohr = config.get("bamboohr", {})
            domain = bamboohr.get("domain", "")
            api_key = bamboohr.get("api_key", "")

            if domain and api_key:
                return domain, api_key

            logger.warning(
                "hr_api_config.json found but missing domain or api_key — "
                "falling back to environment variables."
            )
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("Failed to parse hr_api_config.json: %s — falling back to environment variables.", e)

    # Fallback to environment variables
    domain = os.environ.get("BAMBOOHR_DOMAIN", "")
    api_key = os.environ.get("BAMBOOHR_API_KEY", "")

    if not domain or not api_key:
        raise EnvironmentError(
            "BambooHR credentials not found. Either:\n"
            f"  1. Create {_CONFIG_PATH} with domain and api_key, or\n"
            "  2. Set BAMBOOHR_DOMAIN and BAMBOOHR_API_KEY environment variables."
        )

    return domain, api_key


def _build_session(api_key: str) -> Session:
    """
    Build a requests Session with auth and headers set once.
    Reusing one session per fetch run avoids repeated connection overhead.
    """
    session = Session()
    session.auth = HTTPBasicAuth(api_key, "x")  # BambooHR: key = username, password ignored
    session.headers.update({"Accept": "application/json"})
    return session


def _base_url(domain: str) -> str:
    return f"https://api.bamboohr.com/api/gateway.php/{domain}/v1"


def get_employee_ids() -> list[str]:
    """
    Fetch all employee IDs from the BambooHR directory.

    The directory endpoint is lightweight — it returns summary records only.
    We use it solely to get the list of IDs to iterate over.

    Returns:
        List of employee ID strings (e.g. ["4", "5", "6", ...])

    Raises:
        EnvironmentError: if credentials are missing
        HTTPError: if BambooHR returns a non-2xx response
        ConnectionError / Timeout: on network failure
    """
    domain, api_key = _get_credentials()
    session = _build_session(api_key)
    url = f"{_base_url(domain)}/employees/directory"

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except HTTPError as e:
        logger.error("BambooHR directory fetch failed: %s %s", response.status_code, response.text)
        raise
    except (ConnectionError, Timeout) as e:
        logger.error("BambooHR directory request failed: %s", str(e))
        raise

    employees = response.json().get("employees", [])
    ids = [str(emp["id"]) for emp in employees if emp.get("id")]

    logger.info("BambooHR directory returned %d employee IDs", len(ids))
    return ids


# Module-level cache for the employee directory and number-to-ID mapping.
# Built once on first resolve call, reused for all subsequent lookups
# in the same run. Avoids fetching the directory + individual records
# for every employee in a batch.
_directory_cache: list[dict] = []
_directory_cache_built: bool = False
_employee_number_cache: dict[str, str] = {}


def _build_directory_cache() -> tuple[list[dict], dict[str, str]]:
    """
    Fetch the directory once and build a mapping of employeeNumber → internal ID.
    The directory itself gives us email → ID. For employeeNumber we need to
    fetch each individual record, but we only do this once per run.
    """
    global _directory_cache, _directory_cache_built, _employee_number_cache

    if _directory_cache_built:
        return _directory_cache, _employee_number_cache

    domain, api_key = _get_credentials()
    session = _build_session(api_key)
    url = f"{_base_url(domain)}/employees/directory"

    dir_response = session.get(url, timeout=30)
    dir_response.raise_for_status()

    employees: list[dict] = dir_response.json().get("employees", [])
    _directory_cache = employees

    # Build the employeeNumber → internal ID mapping by fetching each record once
    number_map: dict[str, str] = {}
    for emp in employees:
        emp_id = str(emp.get("id", ""))
        try:
            full_record = get_employee(emp_id)
            emp_number = (full_record.get("employeeNumber") or "").strip().lower()
            if emp_number:
                number_map[emp_number] = emp_id
        except Exception:
            continue

    _employee_number_cache = number_map
    _directory_cache_built = True

    logger.info(
        "Directory cache built — %d employees, %d employee numbers mapped",
        len(employees), len(number_map)
    )

    return _directory_cache, _employee_number_cache


def resolve_bamboohr_id(identifier: str) -> str:
    """
    Resolve an employee number (e.g. "Acc003"), UPN (e.g. "sarah@contoso.com"),
    or BambooHR internal ID (e.g. "121") to the BambooHR internal numeric ID
    required for API calls.

    Accepts any of:
        - Employee number (employeeNumber field, e.g. "Acc003")
        - Work email / UPN (workEmail field)
        - BambooHR internal ID (returned as-is after validation)

    Uses a module-level cache so the directory and employee number lookups
    only happen once per run, not once per employee.

    Returns the BambooHR internal ID string.
    Raises ValueError if the identifier cannot be resolved.
    """
    # If it's purely numeric, check if it's a valid internal ID first
    if identifier.isdigit():
        try:
            get_employee(identifier)
            return identifier
        except Exception:
            pass

    # Build or reuse the directory cache
    try:
        directory, number_map = _build_directory_cache()
    except Exception as e:
        raise ValueError(f"Failed to build BambooHR directory cache: {e}")

    identifier_lower = identifier.strip().lower()

    # Check employeeNumber cache first — most common lookup path
    if identifier_lower in number_map:
        resolved_id = number_map[identifier_lower]
        logger.info(
            "Resolved '%s' → BambooHR ID %s (matched on employeeNumber, cached)",
            identifier, resolved_id
        )
        return resolved_id

    # Fall back to email match from directory
    for emp in directory:
        emp_id = str(emp.get("id", ""))
        email = (emp.get("workEmail") or "").strip().lower()

        if email == identifier_lower:
            logger.info(
                "Resolved '%s' → BambooHR ID %s (matched on email, cached)",
                identifier, emp_id
            )
            return emp_id

    raise ValueError(
        f"Could not resolve '{identifier}' to a BambooHR employee. "
        f"Tried: employee number, work email, and internal ID."
    )


def get_employee(employee_id: str) -> dict:
    """
    Fetch a single employee's full record from BambooHR.

    This is the atomic fetch unit. Every other fetch function calls this.
    Returns raw BambooHR field values with no transformation applied.

    Args:
        employee_id: BambooHR's numeric employee ID as a string

    Returns:
        Raw BambooHR employee dict. Fields may be null — the caller handles that.
        Always includes 'id' so the caller knows which employee this belongs to.

    Raises:
        EnvironmentError: if credentials are missing
        HTTPError: if BambooHR returns a non-2xx response
        ConnectionError / Timeout: on network failure
    """
    domain, api_key = _get_credentials()
    session = _build_session(api_key)

    fields_param = ",".join(_INDIVIDUAL_FIELDS)
    url = f"{_base_url(domain)}/employees/{employee_id}/?fields={fields_param}"

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except HTTPError as e:
        logger.error(
            "BambooHR employee fetch failed for ID %s: %s %s",
            employee_id, response.status_code, response.text
        )
        raise
    except (ConnectionError, Timeout) as e:
        logger.error("BambooHR request failed for employee ID %s: %s", employee_id, str(e))
        raise

    record = response.json()

    # Always stamp the ID onto the record — the individual endpoint does not
    # always return it, but downstream modules need it as employee_id.
    record["id"] = employee_id

    logger.debug("Fetched employee ID %s: %s %s", employee_id,
                 record.get("firstName", ""), record.get("lastName", ""))
    return record


def get_changed_employees(since: str) -> list[dict]:
    """
    Fetch all employees whose records changed since a given date.

    This is the delta poll entry point. It returns only employees
    BambooHR flags as changed — not the entire directory.

    Used for:
      - Joiner detection (new hire date falls after last poll)
      - Mover detection (department / job title changed)
      - Eventually Leaver detection (termination status changed)

    Args:
        since: ISO 8601 date string (e.g. "2026-05-01"). BambooHR accepts
               date-only format for this endpoint.

    Returns:
        List of full employee records (same shape as get_employee()).
        May be empty if nothing changed in the window.

    Raises:
        EnvironmentError: if credentials are missing
        HTTPError: if BambooHR returns a non-2xx response
        ConnectionError / Timeout: on network failure
    """
    domain, api_key = _get_credentials()
    session = _build_session(api_key)

    fields_param = ",".join(_INDIVIDUAL_FIELDS)
    url = (
        f"{_base_url(domain)}/employees/changed/?"
        f"since={since}&type=inserted,updated&fields={fields_param}"
    )

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except HTTPError as e:
        logger.error(
            "BambooHR changed employees fetch failed (since=%s): %s %s",
            since, response.status_code, response.text
        )
        raise
    except (ConnectionError, Timeout) as e:
        logger.error("BambooHR changed employees request failed: %s", str(e))
        raise

    # The changed endpoint returns a dict keyed by employee ID.
    # We convert it to a list and stamp the ID onto each record
    # so the shape matches get_employee() output.
    raw = response.json().get("employees", {})

    records = []
    for emp_id, record in raw.items():
        record["id"] = emp_id
        records.append(record)

    logger.info(
        "BambooHR changed employees (since %s): %d records returned",
        since, len(records)
    )
    return records