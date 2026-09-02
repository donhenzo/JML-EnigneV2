"""
Governance/preprovision.py

PreProvision governance checks — attribute-only, zero Graph. Evaluated on the
canonical payload before any Microsoft Graph write. Membership, SoD, and
cross-plane checks are out of scope here: they need a provisioned object and
run in PostProvision or the standalone scanner.

Ports the payload-mode blocking rules from the validation engine:
  IDENT-002  UPN format
  ENT-004    employment type vs job title
  JOIN-001   employment status must be Active

The employment-status check degrades gracefully: it no-ops when the payload
carries no status and activates automatically once employment_status is threaded
through the schema. Its primary enforcement is the action deriver (terminated
routes to Leaver upstream); here it is defense-in-depth.
"""

import re
from dataclasses import dataclass, field

UPN_PATTERN = r"^[^@]+@[^@]+\.[^@]+$"
RESTRICTED_EMPLOYMENT_TYPES = ("Contractor", "Guest", "Intern")
RESTRICTED_JOB_TITLE_PATTERNS = ("Manager", "Director", "Head", "HOD", "Executive")
REQUIRED_EMPLOYMENT_STATUS = "Active"


@dataclass
class Finding:
    rule_id: str
    severity: str
    details: str


@dataclass
class GovernanceResult:
    passed: bool
    failures: list[Finding] = field(default_factory=list)
    matched_rule_ids: list[str] = field(default_factory=list)


def _check_upn_format(upn: str) -> Finding | None:
    if re.match(UPN_PATTERN, upn or ""):
        return None
    return Finding("IDENT-002", "High", f"UPN '{upn}' is missing or malformed.")


def _check_employment_job_title(employment_type: str, job_title: str) -> Finding | None:
    if employment_type not in RESTRICTED_EMPLOYMENT_TYPES:
        return None
    if not any(re.search(pattern, job_title or "") for pattern in RESTRICTED_JOB_TITLE_PATTERNS):
        return None
    return Finding(
        "ENT-004",
        "Critical",
        f"Employment type '{employment_type}' is not permitted for "
        f"management-tier role '{job_title}'.",
    )


def _check_employment_status(status: str) -> Finding | None:
    # No-op until the payload carries a status; the action deriver is the primary
    # enforcement for terminated identities (they route to Leaver upstream).
    status = (status or "").strip()
    if not status:
        return None
    if status == "Unknown":
        return Finding("JOIN-001", "High", "Employment status could not be determined; verify HR sync.")
    if status != REQUIRED_EMPLOYMENT_STATUS:
        return Finding("JOIN-001", "High", f"Employment status is '{status}'; expected '{REQUIRED_EMPLOYMENT_STATUS}'.")
    return None


def run_preprovision(payload: dict) -> GovernanceResult:
    """Evaluate the attribute-only gate on a payload dict (IdentityPayload.to_dict())."""
    candidates = [
        _check_upn_format(payload.get("upn", "")),
        _check_employment_job_title(
            payload.get("employment_type", ""), payload.get("job_title", "")
        ),
        _check_employment_status(payload.get("employment_status", "")),
    ]
    failures = [finding for finding in candidates if finding is not None]

    return GovernanceResult(
        passed=not failures,
        failures=failures,
        matched_rule_ids=[finding.rule_id for finding in failures],
    )