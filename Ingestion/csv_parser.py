"""
Ingestion/csv_parser.py

Parses a JML CSV file into structured rows for the identity pipeline.

Purpose
-------
Convert raw CSV input into validated row objects that can be safely passed
to the normalization layer.

This module performs:
- CSV structure validation
- Basic type coercion (dates, booleans)
- Row-level validation of required fields

This module deliberately does NOT:
- Resolve canonical values (department, job title, etc.)
- Apply business logic
- Construct IdentityPayload objects

Those responsibilities belong to the normalization layer.

Output
------
Valid rows are returned as RawIdentityRow objects.
Invalid rows are returned unchanged with a rejection_reason so the pipeline
can route them to the hold queue.
"""

from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from datetime import date
from io import StringIO
from typing import Iterator

logger = logging.getLogger(__name__)

# Columns that must exist and contain values for every row.
REQUIRED_COLUMNS = {
    "EmployeeId",
    "UPN",
    "DisplayName",
    "Department",
    "JobTitle",
    "StartDate",
    "EmploymentType",
    "Action",
}

# Optional columns that may appear in the CSV, if not parsed it sets to NON.
OPTIONAL_COLUMNS = {
    "ManagerId",
    "Location",
    "RetainRoles",
    "RetainList",
}

@dataclass
class RawIdentityRow:
    """
    A CSV row after structural validation and basic type coercion.

    Some fields remain as raw strings (department, job_title, employment_type).
    These must be resolved by the normalization layer using canonical lookup
    tables.
    """

    employee_id: str
    upn: str
    display_name: str
    department_raw: str        # Raw value — normalization resolves this
    job_title_raw: str          # Raw value — normalization resolves this
    manager_id: str | None
    start_date: date
    employment_type_raw: str     # Raw value — normalization resolves this
    location: str | None
    action_raw: str              # Raw value — normalization resolves thi
    retain_roles: bool
    retain_list: list[str]

@dataclass
class CsvParseResult:
    """
    this produces a result of a CSV parsing run.

    valid_rows:
        Rows that passed validation and can proceed to normalization.

    rejected_rows:
        are sent to the Hold queue for operator review. 
    """
    valid_rows: list[RawIdentityRow]
    rejected_rows: list[dict]


def parse_csv(content: str) -> CsvParseResult:
    """
    Parse CSV content into validated RawIdentityRow objects.

    The function never raises parsing errors for individual rows.
    Invalid rows are captured and returned in rejected_rows.

    If the CSV structure itself is invalid (e.g., missing headers),
    the entire file is rejected.
    """
    valid_rows: list[RawIdentityRow] = []
    rejected_rows: list[dict] = []

    reader = csv.DictReader(StringIO(content))

    # Validate header structure before reading rows
    if reader.fieldnames is None:
        logger.error("CSV has no headers — cannot parse")
        return CsvParseResult(valid_rows=[], rejected_rows=[])

    missing_columns = REQUIRED_COLUMNS - set(reader.fieldnames)
    if missing_columns:
        logger.error("CSV missing required columns: %s", missing_columns)
        return CsvParseResult(valid_rows=[], rejected_rows=[])

    for row in reader:
        result = _parse_row(row)

        if isinstance(result, RawIdentityRow):
            valid_rows.append(result)
        else:
            rejected_rows.append(result)

            logger.warning(
                "Row rejected [EmployeeId=%s]: %s",
                row.get("EmployeeId", "UNKNOWN"),
                result.get("rejection_reason"),
            )

    return CsvParseResult(valid_rows=valid_rows, rejected_rows=rejected_rows)

def _parse_row(row: dict) -> RawIdentityRow | dict:
    """
    Validate and coerce a single CSV row.

    Returns:
        RawIdentityRow on success.

        An Original row dictionary with a 'rejection_reason' field if
        validation fails.
    """

    # Ensure required fields exist and contain values
    for col in REQUIRED_COLUMNS:
        if not row.get(col, "").strip():
            row["rejection_reason"] = f"Missing required field: {col}"
            return row

    # Parse StartDate (expected ISO format: YYYY-MM-DD)
    try:
        start_date = date.fromisoformat(row["StartDate"].strip())
    except ValueError:
        row["rejection_reason"] = (
            f"Invalid StartDate format: '{row['StartDate']}' — expected YYYY-MM-DD"
        )
        return row

    # Parse RetainRoles (defaults to False)
    retain_roles_raw = row.get("RetainRoles", "").strip().lower()
    retain_roles = retain_roles_raw in ("true", "1", "yes")

    retain_list = _parse_retain_list(row.get("RetainList", ""))

    return RawIdentityRow(
        employee_id=row["EmployeeId"].strip(),
        upn=row["UPN"].strip(),
        display_name=row["DisplayName"].strip(),
        department_raw=row["Department"].strip(),
        job_title_raw=row["JobTitle"].strip(),
        manager_id=row.get("ManagerId", "").strip() or None,
        start_date=start_date,
        employment_type_raw=row["EmploymentType"].strip(),
        location=row.get("Location", "").strip() or None,
        action_raw=row["Action"].strip(),
        retain_roles=retain_roles,
        retain_list=retain_list,
    )

def _parse_retain_list(value: str) -> list[str]:
    """
    Parse the RetainList column.

    Accepted formats:
    - JSON array (preferred)
    - Comma-separated list

    Invalid values are treated as empty lists because this field
    is optional and should not block data ingestion.
    """
    value = value.strip()
    if not value:
        return []

    # Try JSON format first
    if value.startswith("["):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed]
        except json.JSONDecodeError:
            pass

    # Fallbacks to comma-separated values
    return [item.strip() for item in value.split(",") if item.strip()]