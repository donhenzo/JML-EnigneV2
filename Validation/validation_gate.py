# validation/validation_gate.py
#
# Python-side validation gate for the JML pipeline.
# Calls the PowerShell validation engine over HTTP.
#
# WHY THIS EXISTS:
#   The validation engine is a separate PowerShell Function app that evaluates
#   identity and RBAC state against a governance rule set. This module is the
#   Python side of that contract — it builds the request, handles transport
#   errors, and translates the response into a structured ValidationResult
#   the pipeline can act on.
#
# TWO VALIDATION MODES:
#   PreProvision  — called before any Entra ID object is created.
#                   Validates the canonical payload against rules.
#                   Validation engine uses synthetic data — no Graph calls.
#   PostProvision — called after provisioning completes.
#                   Validates actual tenant state against expected state.
#                   Validation engine makes real Graph API calls in this mode.
#
# UPN CHECK:
#   check_upn_exists() is handled here, not by the validation engine.
#   The engine cannot detect UPN conflicts in PreProvision mode because it
#   never hits Graph. This check must run before pre_provision_validate().
#
# ENV VARS REQUIRED:
#   JML_VALIDATION_ENGINE_URL — HTTP endpoint of the PowerShell Function app
#   JML_VALIDATION_ENGINE_KEY — Function key (optional, only needed in deployment)
#
# LOCAL TESTING WITHOUT THE VALIDATION ENGINE:
#   JML_SKIP_VALIDATION_ENGINE — set to "true" to bypass both HTTP calls
#   entirely and return an always-passing ValidationResult. This exists so
#   the Joiner/Mover pipelines can be tested end to end (event store,
#   normalization, entitlement resolution, access package provisioning)
#   without a live PowerShell process. Every skip is logged as a WARNING
#   and recorded as an ENGINE-SKIPPED warning finding in the audit report,
#   so a test run is never visually indistinguishable from a governed one.
#   This must never be set in a deployed environment — it disables
#   governance validation entirely.

import logging
import os
import requests
from dataclasses import dataclass, field

from Ingestion.schema import IdentityPayload

logger = logging.getLogger(__name__)

# PreProvision runs fast — synthetic snapshot, no Graph calls.
# PostProvision may be slower — real tenant scan with Graph API calls.
PRE_PROVISION_TIMEOUT_SECONDS  = 30
POST_PROVISION_TIMEOUT_SECONDS = 60

SKIP_VALIDATION_ENGINE_ENV_VAR = "JML_SKIP_VALIDATION_ENGINE"


@dataclass
class ValidationResult:
    """
    Structured result from the validation engine HTTP endpoint.
    Returned by both pre_provision_validate() and post_provision_validate().

    passed           — True if no blocking violations found
    failures         — blocking findings, each with ruleId, category, severity, details
    warnings         — non-blocking findings; recorded in the audit report but
                       do not stop provisioning
    matched_rule_ids — IDs of every rule that fired, kept for the audit trail
    mode             — PreProvision or PostProvision
    raw_response     — full response dict for debugging
    """
    passed:           bool
    failures:         list[dict] = field(default_factory=list)
    warnings:         list[dict] = field(default_factory=list)
    matched_rule_ids: list[str]  = field(default_factory=list)
    mode:             str        = ""
    raw_response:     dict       = field(default_factory=dict)

    def failure_summary(self) -> list[str]:
        """Flatten failures into plain strings for the hold queue reasons list."""
        return [
            f"[{f.get('ruleId', 'UNKNOWN')}] {f.get('details', '')}"
            for f in self.failures
        ]

    def warning_summary(self) -> list[str]:
        """Flatten warnings into plain strings for the audit report."""
        return [
            f"[{f.get('ruleId', 'UNKNOWN')}] {f.get('details', '')}"
            for f in self.warnings
        ]


def _validation_engine_skipped() -> bool:
    """
    True if JML_SKIP_VALIDATION_ENGINE is set to a truthy value.

    Lets the Joiner/Mover pipelines be tested end to end without a live
    PowerShell validation engine process. This is a local testing
    convenience only; it must never be set in a deployed environment.
    """
    return os.environ.get(SKIP_VALIDATION_ENGINE_ENV_VAR, "").lower() in ("1", "true", "yes")


def _skipped_result(mode: str) -> ValidationResult:
    """
    A ValidationResult that always passes, used when the validation
    engine is deliberately skipped. matched_rule_ids stays empty and
    raw_response carries a marker so this is visibly distinguishable
    from a real engine response in the audit trail, not silently
    indistinguishable from an actual pass.
    """
    return ValidationResult(
        passed=True,
        failures=[],
        warnings=[{
            "ruleId":   "ENGINE-SKIPPED",
            "category": "ValidationEngine",
            "severity": "Informational",
            "details":  (
                f"Validation engine was skipped ({SKIP_VALIDATION_ENGINE_ENV_VAR}=true). "
                "This result is not a real governance evaluation."
            ),
        }],
        mode=mode,
        raw_response={"skipped": True},
    )


def _call_validation_engine(
    body: dict,
    timeout: int,
    mode: str
) -> ValidationResult:
    """
    POST to the validation engine and return a ValidationResult.

    This is the shared transport layer used by both pre and post provision
    functions. It handles every failure mode — missing config, HTTP errors,
    timeouts, connection failures — and returns a failed ValidationResult
    rather than raising. The pipeline treats any failure here as a validation
    failure and routes the record to the hold queue.

    Structured error rule IDs (CONFIG-001, ENGINE-001..004) let operators
    distinguish transport failures from actual rule violations in the audit trail.
    """
    url = os.environ.get("JML_VALIDATION_ENGINE_URL", "")
    key = os.environ.get("JML_VALIDATION_ENGINE_KEY", "")

    if not url:
        logger.error("JML_VALIDATION_ENGINE_URL is not set in environment")
        return ValidationResult(
            passed=False,
            failures=[{
                "ruleId":   "CONFIG-001",
                "category": "Configuration",
                "severity": "Critical",
                "details":  "JML_VALIDATION_ENGINE_URL is not configured."
            }],
            mode=mode
        )

    headers = {"Content-Type": "application/json"}
    if key:
        headers["x-functions-key"] = key  # Only sent if configured — local runs skip auth

    try:
        response = requests.post(url, json=body, headers=headers, timeout=timeout)

        if response.status_code != 200:
            logger.error(
                f"Validation engine returned HTTP {response.status_code} "
                f"for {mode} — body: {response.text[:500]}"
            )
            return ValidationResult(
                passed=False,
                failures=[{
                    "ruleId":   "ENGINE-001",
                    "category": "ValidationEngine",
                    "severity": "Critical",
                    "details":  (
                        f"Validation engine returned HTTP "
                        f"{response.status_code}: {response.text[:200]}"
                    )
                }],
                mode=mode
            )

        data = response.json()

        return ValidationResult(
            passed=           data.get("passed", False),
            failures=         data.get("failures", []),
            warnings=         data.get("warnings", []),
            matched_rule_ids= data.get("matchedRuleIds", []),
            mode=             mode,
            raw_response=     data
        )

    except requests.Timeout:
        logger.error(f"Validation engine timed out after {timeout}s for {mode}")
        return ValidationResult(
            passed=False,
            failures=[{
                "ruleId":   "ENGINE-002",
                "category": "ValidationEngine",
                "severity": "Critical",
                "details":  f"Validation engine timed out after {timeout}s."
            }],
            mode=mode
        )

    except requests.ConnectionError:
        logger.error(f"Could not connect to validation engine at {url} for {mode}")
        return ValidationResult(
            passed=False,
            failures=[{
                "ruleId":   "ENGINE-003",
                "category": "ValidationEngine",
                "severity": "Critical",
                "details":  f"Could not connect to validation engine at {url}."
            }],
            mode=mode
        )

    except Exception as e:
        logger.error(f"Unexpected error calling validation engine: {e}")
        return ValidationResult(
            passed=False,
            failures=[{
                "ruleId":   "ENGINE-004",
                "category": "ValidationEngine",
                "severity": "Critical",
                "details":  f"Unexpected validation engine error: {str(e)}"
            }],
            mode=mode
        )


def pre_provision_validate(payload: IdentityPayload) -> ValidationResult:
    """
    Validate a canonical identity payload before any Entra ID object is created.

    The validation engine operates on the payload alone in this mode — it builds
    a synthetic identity snapshot and evaluates it against the rule set without
    making any Graph API calls. This keeps PreProvision fast and side-effect free.

    The pipeline must not proceed to provisioning if this returns passed=False.
    Route the record to the hold queue using result.failure_summary() as reasons.

    If JML_SKIP_VALIDATION_ENGINE is set, returns an always-passing result
    without making the HTTP call at all — see the module docstring.
    """
    if _validation_engine_skipped():
        logger.warning(
            f"Validation engine skipped ({SKIP_VALIDATION_ENGINE_ENV_VAR}=true) — "
            f"PreProvision — employee={payload.employee_id}"
        )
        return _skipped_result("PreProvision")

    body = {
        "mode": "PreProvision",
        "payload": {
            "EmployeeId":     payload.employee_id,
            "UPN":            payload.upn,
            "DisplayName":    payload.display_name,
            "Department":     payload.department,
            "JobTitle":       payload.job_title,
            "StartDate":      payload.start_date.isoformat(),
            "EmploymentType": payload.employment_type.value,
            "Action":         payload.action.value,
            "ManagerId":      payload.manager_id or "",
            "Location":       payload.location or "",
            "RetainRoles":    payload.retain_roles,
            "RetainList":     payload.retain_list or [],
        }
    }

    logger.info(
        f"Pre-provision validation — employee={payload.employee_id}, upn={payload.upn}"
    )

    result = _call_validation_engine(
        body=body,
        timeout=PRE_PROVISION_TIMEOUT_SECONDS,
        mode="PreProvision"
    )

    if result.passed:
        logger.info(f"Pre-provision validation passed — employee={payload.employee_id}")
    else:
        logger.warning(
            f"Pre-provision validation failed — employee={payload.employee_id}, "
            f"failures={result.failure_summary()}"
        )

    return result

def post_provision_validate(entra_object_id: str, employee_id: str = "") -> ValidationResult:
    """
    Validate a provisioned Entra ID object against expected state.

    Called after provisioning completes. The validation engine uses the
    Entra Object ID to query the real tenant and confirms required group
    memberships, RBAC assignments, and absence of legacy groups.

    A passed=False result here means provisioning completed but did not
    reach the expected state. The event should be marked Failed with class
    PostProvisionValidationFailed — distinguishable from a provisioning
    error in the audit report.

    employee_id is only used for log context — it is not sent to the engine.

    If JML_SKIP_VALIDATION_ENGINE is set, returns an always-passing result
    without making the HTTP call at all — see the module docstring.
    """
    if _validation_engine_skipped():
        logger.warning(
            f"Validation engine skipped ({SKIP_VALIDATION_ENGINE_ENV_VAR}=true) — "
            f"PostProvision — object_id={entra_object_id}, employee={employee_id}"
        )
        return _skipped_result("PostProvision")

    body = {
        "mode": "PostProvision",
        "targetUserId": entra_object_id
    }

    logger.info(
        f"Post-provision validation — object_id={entra_object_id}, employee={employee_id}"
    )

    result = _call_validation_engine(
        body=body,
        timeout=POST_PROVISION_TIMEOUT_SECONDS,
        mode="PostProvision"
    )

    # Log raw response from validation engine
    logger.info(
        f"Post-provision raw response — object_id={entra_object_id}: {result.raw_response}"
    )

    if result.passed:
        logger.info(f"Post-provision validation passed — object_id={entra_object_id}")
    else:
        logger.warning(
            f"Post-provision validation failed — object_id={entra_object_id}, "
            f"failures={result.failure_summary()}"
        )

    return result


def check_upn_exists(upn: str, graph_client) -> bool:
    """
    Check whether a UPN is already taken in Entra ID before provisioning.

    This is a JML engine responsibility, not delegated to the validation engine.
    The validation engine never calls Graph in PreProvision mode, so it cannot
    detect UPN conflicts. This function must run before pre_provision_validate().

    On any unexpected Graph error, returns True (treats UPN as taken) rather
    than False — a conservative default that prevents accidental duplicate
    user creation in the event of a transient API failure.

    Inputs:
        upn          — user principal name to check
        graph_client — authenticated Graph API client (built in Block E)

    Returns:
        True  — UPN exists; route to hold queue with reason DuplicateUPN
        False — UPN is available; provisioning may proceed
    """
    try:
        graph_client.get_user(upn)
        logger.warning(f"UPN conflict detected — {upn} already exists in Entra ID")
        return True

    except Exception as e:
        error_message = str(e).lower()
        if "not found" in error_message or "404" in error_message:
            return False
        # Unexpected error — treat as exists rather than risk creating a duplicate
        logger.error(
            f"Unexpected error checking UPN {upn}: {e}. "
            "Treating as exists to prevent potential duplicate."
        )
        return True