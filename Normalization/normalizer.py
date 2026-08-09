"""
Normalization/normalizer.py

Applies the canonical lookup table to an IdentityPayload.

Resolves raw HR attribute values to their standardized equivalents.
Unknown values that cannot be resolved are NOT provisioned — the identity
is flagged for the hold queue with an explicit failure reason.

This layer runs BEFORE the validation engine. Its job is to clean the
data into a state that validation can reason about. A payload with
unresolved attributes should never reach the provisioning layer.

Side effects:
    None. Pure transformation — reads from lookup table, mutates a copy
    of the payload. No external calls.
"""

from __future__ import annotations

import logging
from copy import deepcopy
from dataclasses import dataclass
from Ingestion.schema import IdentityPayload

logger = logging.getLogger(__name__)



# Result type
@dataclass
class NormalizationResult:
    """
    The outcome of normalizing a single IdentityPayload.

    Callers check `passed` first. If False, the `failures` list describes
    exactly what could not be resolved. The `payload` is always returned —
    even on failure — so callers can log the partial state.
    """
    payload: IdentityPayload
    passed: bool
    failures: list[str]

    @property
    def partial(self) -> bool:
        """True if some fields resolved but at least one did not."""
        return not self.passed and len(self.failures) < 2



# Normalizer

class Normalizer:
    """
    Applies canonical lookup to identity payloads.

    Purpose:
        Resolve raw HR attribute values to standardized equivalents using
        the loaded lookup table. Flag payloads with unresolvable attributes
        so they can be held for human review rather than provisioned with
        bad data.

    Inputs:
        lookup_table: LookupTable loaded by lookup_loader.py

    Usage:
        normalizer = Normalizer(lookup_table)
        result = normalizer.normalize(payload)
        if not result.passed:
            # send to hold queue
    """

    def __init__(self, lookup_table: dict) -> None:
        self._table = lookup_table

    def normalize(self, payload: IdentityPayload) -> NormalizationResult:
        """
        Normalize an IdentityPayload against the canonical lookup table.

        Purpose:
            Attempt to resolve department and job_title to their canonical
            values. Mark the payload with normalization outcome.

        Inputs:
            payload: IdentityPayload from the CSV parser. Not mutated —
                     a copy is returned.

        Outputs:
            NormalizationResult with:
                - payload: updated copy with resolved values and flags set
                - passed: True only if all required fields resolved
                - failures: list of human-readable reasons for any failure

        Side effects:
            Logs a warning for each unresolvable value.
        """
        # Work on a copy — callers may need the original for comparison.
        result_payload = deepcopy(payload)
        failures: list[str] = []

        # department 
        dept_resolved = self._resolve(
            category="department",
            raw_value=payload.department,
            field_name="Department",
            failures=failures,
        )
        if dept_resolved is not None:
            result_payload.department = dept_resolved

        # job_title 
        title_resolved = self._resolve(
            category="job_title",
            raw_value=payload.job_title,
            field_name="JobTitle",
            failures=failures,
        )
        if title_resolved is not None:
            result_payload.job_title = title_resolved

        # optional: location 
        # Location failure is a warning only — it does not block provisioning.
        if payload.location and "location" in self._table:
            loc_key = payload.location.lower().strip()
            if loc_key in self._table["location"]:
                result_payload.location = self._table["location"][loc_key]
            else:
                logger.warning(
                    "Location '%s' not in lookup table for employee %s — "
                    "keeping raw value.",
                    payload.location,
                    payload.employee_id,
                )

        # set normalization outcome on payload 
        passed = len(failures) == 0
        result_payload.normalization_passed = passed
        result_payload.normalization_failures = failures

        if passed:
            logger.info(
                "Normalization passed for employee %s (%s)",
                payload.employee_id,
                payload.upn,
            )
        else:
            logger.warning(
                "Normalization failed for employee %s (%s): %s",
                payload.employee_id,
                payload.upn,
                failures,
            )

        return NormalizationResult(
            payload=result_payload,
            passed=passed,
            failures=failures,
        )

    def _resolve(
        self,
        category: str,
        raw_value: str | None,
        field_name: str,
        failures: list[str],
    ) -> str | None:
        """
        Attempt to resolve a single field value against the lookup table.

        Returns the canonical value if found, None if not found.
        Appends a descriptive failure reason to `failures` if resolution
        fails.

        None raw_value (missing field) is treated as an unresolvable
        value — missing required attributes must be held, not guessed.
        """
        if category not in self._table:
            # Category not in lookup table at all — pass through as-is.
            return raw_value

        if not raw_value:
            failures.append(
                f"{field_name} is missing — cannot resolve to canonical value."
            )
            return None

        key = raw_value.lower().strip()
        if key in self._table[category]:
            return self._table[category][key]

        failures.append(
            f"{field_name} value '{raw_value}' is not in the canonical lookup "
            f"table. Add it to the lookup table or correct the source data."
        )
        return None