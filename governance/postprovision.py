"""
Governance/postprovision.py

PostProvision governance check — DETECTIVE, runs after delivery against the
provisioned user's real group memberships (memberOf). Two checks:

  ENT-002       employment type vs the classification of each held group
                (governance_model.json — tier / allowed_employment / privileged)
  SOD/<id>      Separation of Duties intersection against sod_policies.json v2

This is a detective control, not preventive: a finding means non-compliant
access already exists. The response is to mark the event failed/partial and
record the finding for review — it does NOT remediate (ADR-019; remediation is
the reconciliation pipeline's job, ADR-012). The dangerous conflicting
combinations are prevented upstream by platform SoD (ADR-008); this backstops
the Warn-only pairs, direct-assignment drift, and misconfiguration.

Groups not present in governance_model.json are treated as unmanaged and
skipped, mirroring the Mover's NOT_PROCESSED.
"""

import json
import os
from dataclasses import dataclass, field

GOVERNANCE_MODEL_PATH = os.environ.get(
    "JML_GOVERNANCE_MODEL_PATH", "config/governance_model.json"
)
SOD_POLICIES_PATH = os.environ.get(
    "JML_SOD_POLICIES_PATH", "config/sod_policies.json"
)


@dataclass
class Finding:
    rule_id: str
    severity: str
    details: str


@dataclass
class GovernanceResult:
    passed: bool
    failures: list[Finding] = field(default_factory=list)
    warnings: list[Finding] = field(default_factory=list)
    matched_rule_ids: list[str] = field(default_factory=list)

    def failure_summary(self) -> list[str]:
        return [f"[{f.rule_id}] {f.details}" for f in self.failures]

    def warning_summary(self) -> list[str]:
        return [f"[{w.rule_id}] {w.details}" for w in self.warnings]


def load_governance_model(path: str = GOVERNANCE_MODEL_PATH) -> dict:
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def load_sod_policies(path: str = SOD_POLICIES_PATH) -> dict:
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _check_employment_tier(
    employment_type: str,
    held_ids: set[str],
    name_by_id: dict,
    governance_model: dict,
) -> list[Finding]:
    """Flag any held group whose allowed_employment excludes this employment type.
    Groups absent from the model are unmanaged and skipped."""
    findings = []
    groups = governance_model.get("groups", {})

    for group_id in held_ids:
        classification = groups.get(group_id)
        if classification is None:
            continue

        if employment_type in classification.get("allowed_employment", []):
            continue

        name = classification.get("display_name", name_by_id.get(group_id, group_id))
        privileged = " privileged" if classification.get("privileged") else ""
        tier = classification.get("tier", "")
        findings.append(Finding(
            "ENT-002",
            "High",
            f"Employment type '{employment_type}' is not permitted in{privileged} "
            f"{tier}-tier group '{name}'.",
        ))

    return findings


def _check_sod(
    held_ids: set[str],
    name_by_id: dict,
    sod_policies: dict,
) -> tuple[list[Finding], list[Finding]]:
    """Intersect held groups against each SoD pair. Block conflicts are failures,
    Warn conflicts are warnings."""
    failures = []
    warnings = []

    for policy in sod_policies.get("policies", []):
        hit_a = held_ids & set(policy.get("set_a", []))
        hit_b = held_ids & set(policy.get("set_b", []))
        if not (hit_a and hit_b):
            continue

        names_a = ", ".join(name_by_id.get(g, g) for g in sorted(hit_a))
        names_b = ", ".join(name_by_id.get(g, g) for g in sorted(hit_b))
        finding = Finding(
            f"SOD/{policy.get('id', '')}",
            policy.get("risk_rating", "High"),
            f"[{policy.get('id', '')}] {policy.get('name', '')} — "
            f"holds conflicting groups: {names_a} + {names_b}",
        )

        if policy.get("action") == "Block":
            failures.append(finding)
        else:
            warnings.append(finding)

    return failures, warnings


def run_postprovision(
    payload: dict,
    member_of: list[dict],
    governance_model: dict,
    sod_policies: dict,
) -> GovernanceResult:
    """Evaluate the detective gate against a user's real group memberships.

    payload           — IdentityPayload.to_dict() (needs employment_type)
    member_of         — [{group_id, display_name}] from get_user_group_memberships
    governance_model  — load_governance_model()
    sod_policies      — load_sod_policies()
    """
    held_ids = {g["group_id"] for g in member_of if g.get("group_id")}
    name_by_id = {
        g["group_id"]: g.get("display_name", "")
        for g in member_of if g.get("group_id")
    }
    employment_type = payload.get("employment_type", "")

    failures = _check_employment_tier(employment_type, held_ids, name_by_id, governance_model)
    sod_failures, sod_warnings = _check_sod(held_ids, name_by_id, sod_policies)
    failures.extend(sod_failures)

    return GovernanceResult(
        passed=not failures,
        failures=failures,
        warnings=sod_warnings,
        matched_rule_ids=[f.rule_id for f in failures + sod_warnings],
    )