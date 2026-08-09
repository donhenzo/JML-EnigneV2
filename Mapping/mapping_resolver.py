# mapping/mapping_resolver.py
#
# Evaluates all mapping rules against a canonical IdentityPayload.
# Returns the union of matched entitlements across all matching rules,
# plus the rule IDs that matched — required for the audit trail.
#
# Rules are evaluated in priority order (ascending).
# All matching rules contribute entitlements — evaluation never stops
# at first match. This is what allows baseline + role rules to combine.

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class PimGroup:
    """
    A PIM-eligible group resolved from the mapping rules.
    The engine adds the user as an eligible member of this group.
    The group must already hold an eligible Entra role assignment via PIM.
    """
    group_id:       str
    display_name:   str
    eligible_role:  str
    justification:  str
    duration_hours: int = 8


@dataclass
class AccessPackageAssignment:
    """
    An access package resolved from the mapping rules (ADR-007).
    The engine requests this package/policy pair for the identity via
    Entitlement Management, replacing direct group assignment.

    duration_override_days is only set when a rule needs a different
    expiration than its assignment policy already defines. None means
    the policy's own expiration applies — the normal case.
    """
    rule_id:                str
    access_package_id:      str
    policy_id:              str
    duration_override_days: int | None = None


@dataclass
class EntitlementResult:
    """
    Output of the resolver for a single identity.

    groups           — deduplicated list of group IDs to assign (legacy,
                        pre-ADR-007 departments not yet migrated)
    rbac_roles       — deduplicated list of RBAC role assignments
    access_packages  — deduplicated list of AccessPackageAssignment,
                        one per matched rule that resolves to a package
    matched_rule_ids — IDs of every rule that matched, in evaluation order
    """
    groups: list[str] = field(default_factory=list)
    rbac_roles: list[dict] = field(default_factory=list)
    pim_groups:       list = field(default_factory=list)  # list[PimGroup]
    access_packages:  list = field(default_factory=list)  # list[AccessPackageAssignment]
    matched_rule_ids: list[str] = field(default_factory=list)


def _condition_matches(condition: dict, identity_value: str | None) -> bool:
    """
    Evaluate a single rule condition against an identity field value.

    Supports exact, contains, and startsWith, per the operator set
    documented in DEVELOPER.md. Case-insensitive comparison.
    Returns False if the identity value is None or empty.
    """
    if not identity_value:
        return False

    operator = condition.get("operator")
    rule_value = condition.get("value", "")

    identity_value_lower = identity_value.strip().lower()
    rule_value_lower = rule_value.strip().lower()

    if operator == "exact":
        return identity_value_lower == rule_value_lower

    if operator == "contains":
        return rule_value_lower in identity_value_lower

    if operator == "startsWith":
        return identity_value_lower.startswith(rule_value_lower)

    # Unknown operator — log and treat as no match rather than erroring.
    logger.warning(f"Unknown rule condition operator '{operator}' — skipping condition")
    return False


def _rule_matches(rule: dict, department: str | None, job_title: str | None, employment_type: str | None = None) -> bool:
    """
    Evaluate all conditions in a rule against the identity.
    All conditions must match for the rule to match (AND logic).
    A rule with no conditions never matches.

    Condition keys match role_mapping_rules.json's actual casing —
    "jobTitle" and "employmentType", camelCase, not "job_title" /
    "employment_type". A prior version of this function checked the
    snake_case names, which never matched any real rule's condition
    dict — silently reducing every rule's matching logic to department
    alone, regardless of what jobTitle or employmentType conditions
    the rule actually declared. Confirmed and fixed 4 August 2026 after
    three mutually-exclusive Finance rules all matched simultaneously
    for the same identity in testing.
    """
    conditions = rule.get("conditions", {})

    if not conditions:
        logger.warning(f"Rule {rule.get('id')} has no conditions — skipping")
        return False

    if "department" in conditions:
        if not _condition_matches(conditions["department"], department):
            return False

    if "jobTitle" in conditions:
        if not _condition_matches(conditions["jobTitle"], job_title):
            return False

    # Employment type condition — allows rules to target Employee vs
    # Contractor vs Guest without relying on job title as a proxy.
    if "employmentType" in conditions:
        if not _condition_matches(conditions["employmentType"], employment_type):
            return False

    return True


def resolve_entitlements(
    rules:           list[dict],
    department:      str | None,
    job_title:       str | None,
    employment_type: str | None = None,
    employee_id:     str = ""
) -> EntitlementResult:
    """
    Evaluate all rules against an identity and return the union of
    matched entitlements.

    Inputs:
        rules           — sorted rule list from mapping_loader
        department      — canonical department value from IdentityPayload
        job_title       — canonical job title value from IdentityPayload
        employment_type — canonical employment type (Employee/Contractor/Guest)
        employee_id     — used only for log context

    Output:
        EntitlementResult with deduplicated groups, rbac_roles,
        access_packages, and matched_rule_ids.

    No rules match → returns empty EntitlementResult. Not an error.
    The pipeline treats an empty result as a warning, not a failure.
    """
    result = EntitlementResult()

    # Accumulate before deduplication
    all_groups:          list[str] = []
    all_rbac_roles:      list[dict] = []
    all_pim_groups:      list = []
    all_access_packages: list[AccessPackageAssignment] = []

    for rule in rules:
        rule_id = rule.get("id", "UNKNOWN")

        if _rule_matches(rule, department, job_title, employment_type):
            entitlements = rule.get("entitlements", {})

            matched_groups = entitlements.get("groups", [])
            matched_roles  = entitlements.get("rbac_roles", [])

            all_groups.extend(matched_groups)
            all_rbac_roles.extend(matched_roles)
            result.matched_rule_ids.append(rule_id)

            access_package_id = entitlements.get("accessPackageId")
            policy_id = entitlements.get("policyId")
            if access_package_id and policy_id:
                all_access_packages.append(AccessPackageAssignment(
                    rule_id=rule_id,
                    access_package_id=access_package_id,
                    policy_id=policy_id,
                    duration_override_days=entitlements.get("durationOverrideDays"),
                ))

            for entry in entitlements.get("pimGroups", []):
                gid = entry.get("id", "").strip()
                if gid:
                    all_pim_groups.append(PimGroup(
                        group_id=      gid,
                        display_name=  entry.get("displayName", gid),
                        eligible_role= entry.get("eligibleRole", ""),
                        justification= entry.get("justification", "Provisioned by JML engine"),
                        duration_hours=int(entry.get("durationHours", 8)),
                    ))

            logger.debug(
                f"Rule {rule_id} matched for employee {employee_id} — "
                f"groups: {matched_groups}, roles: {matched_roles}, "
                f"access_package: {access_package_id}"
            )

    # Deduplicate groups — preserve first-seen order
    seen_groups: set[str] = set()
    for g in all_groups:
        if g not in seen_groups:
            result.groups.append(g)
            seen_groups.add(g)

    # Deduplicate RBAC roles — role + scope pair must both match
    seen_roles: set[tuple] = set()
    for r in all_rbac_roles:
        key = (r.get("role"), r.get("scope"))
        if key not in seen_roles:
            result.rbac_roles.append(r)
            seen_roles.add(key)

    # Deduplicate access packages — accessPackageId + policyId pair must
    # both match. Two rules resolving the same package under different
    # policies would be a rules.json authoring error, not something to
    # silently collapse, so the pair is the dedup key rather than just
    # the package ID.
    seen_packages: set[tuple] = set()
    for ap in all_access_packages:
        key = (ap.access_package_id, ap.policy_id)
        if key not in seen_packages:
            result.access_packages.append(ap)
            seen_packages.add(key)

    # Deduplicate PIM groups by group_id — preserve first-seen order
    seen_pim_ids: set[str] = set()
    for pg in all_pim_groups:
        if pg.group_id not in seen_pim_ids:
            result.pim_groups.append(pg)
            seen_pim_ids.add(pg.group_id)

    if result.matched_rule_ids:
        logger.info(
            f"Entitlement resolution complete for employee {employee_id} — "
            f"matched rules: {result.matched_rule_ids}, "
            f"groups: {result.groups}, "
            f"rbac_roles: {len(result.rbac_roles)}, "
            f"access_packages: {[ap.access_package_id for ap in result.access_packages]}"
        )
    else:
        logger.warning(
            f"No rules matched for employee {employee_id} — "
            f"department='{department}', job_title='{job_title}', "
            f"employment_type='{employment_type}'"
        )

    return result