"""

Evaluates retention records for resources flagged for removal in a Mover event.

Generalized to be resource-oriented (ADR — RetentionRegistry generalization,
4 August 2026): keyed on resourceType + resourceId rather than group only.
accessPackage is the only resourceType populated today. group and
applicationRole can be added later with no schema change.

Splits resource_ids from MoverDelta into two sets:
    - remove_confirmed: resources with no retention record, or an expired one
    - retain_set:       resources with a valid, unexpired retention record

I/O is isolated to fetch_retention_record(). The decision logic in
evaluate_retention() is pure and unit-testable without mocking Table Storage.
"""

from __future__ import annotations
import logging
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Optional

from azure.core.exceptions import ResourceNotFoundError
from azure.data.tables import TableServiceClient

logger = logging.getLogger(__name__)


# Data models

class RetentionOutcome(Enum):
    RETAINED  = "RETAINED"    # valid record, date in future
    EXPIRED   = "EXPIRED"     # record exists but date has passed
    NO_RECORD = "NO_RECORD"   # nothing in RetentionRegistry


@dataclass(frozen=True)
class RetentionRecord:
    """
    A single entry from the RetentionRegistry table in Azure Table Storage.

    RowKey is a composite f"{resource_type}:{resource_id}" so resource
    types sharing the same table can never collide on ID alone.

    Fields:
        employee_id:      PartitionKey — HR source identifier
        resource_type:     e.g. "accessPackage", "group", "applicationRole"
        resource_id:       Entra object ID of the resource (access package ID,
                           group ID, etc. — meaning depends on resource_type)
        granted_by:        UPN of the approver who created the retention
        granted_date:      ISO-8601 date the retention was created
        review_date:       ISO-8601 expiry date — retention is valid up to
                           and including this date
        reason:            Business justification string
        source:            MANUAL | ACCESS_REQUEST | EXCEPTION
        retained_through:  event_id of the move that triggered this decision
    """
    employee_id:      str
    resource_type:    str
    resource_id:      str
    granted_by:       str
    granted_date:     date
    review_date:      date
    reason:           str
    source:           str
    retained_through: str


@dataclass
class ResourceRetentionDecision:
    """
    The outcome of evaluating a single resource against the RetentionRegistry.

    Fields:
        resource_type: e.g. "accessPackage"
        resource_id:   Entra object ID of the resource
        outcome:       RetentionOutcome enum value
        record:        The RetentionRecord if one existed, None otherwise.
                       Preserved for audit trail regardless of outcome.
    """
    resource_type: str
    resource_id:   str
    outcome:       RetentionOutcome
    record:        Optional[RetentionRecord]


@dataclass
class RetentionResult:
    """
    The complete output of evaluate_all_retentions().

    Fields:
        remove_confirmed: Resource IDs with no record or an expired record.
                          These proceed to removal.
        retain_set:       Resource IDs with a valid, unexpired retention
                          record. Excluded from removal.
        decisions:        Full per-resource decision list for the audit
                          record. Every ID in the input set gets an entry.
    """
    remove_confirmed: frozenset[str]
    retain_set:       frozenset[str]
    decisions:        list[ResourceRetentionDecision]


# Table Storage fetch

def fetch_retention_record(
    employee_id:   str,
    resource_type: str,
    resource_id:   str,
    table_client:  TableServiceClient,
    table_name:    str = "RetentionRegistry",
) -> Optional[RetentionRecord]:
    """
    Fetch a single retention record from Azure Table Storage.

    PartitionKey = employee_id
    RowKey       = f"{resource_type}:{resource_id}"

    Returns a RetentionRecord if found, None if no entry exists.

    Args:
        employee_id:   HR source identifier for the employee.
        resource_type: e.g. "accessPackage" — must match how the record
                       was written.
        resource_id:   Entra object ID being checked.
        table_client:  Injected TableServiceClient — not created internally.
        table_name:    Table name, defaults to RetentionRegistry.

    Side effects:
        One GET request to Azure Table Storage per call.
    """
    row_key = f"{resource_type}:{resource_id}"
    entity = None

    try:
        client = table_client.get_table_client(table_name)
        entity = client.get_entity(
            partition_key=employee_id,
            row_key=row_key,
        )

        return RetentionRecord(
            employee_id      = entity["PartitionKey"],
            resource_type    = entity.get("resourceType", resource_type),
            resource_id      = entity.get("resourceId", resource_id),
            granted_by       = entity["granted_by"],
            granted_date     = date.fromisoformat(entity["granted_date"]),
            review_date      = date.fromisoformat(entity["review_date"]),
            reason           = entity["reason"],
            source           = entity["source"],
            retained_through = entity.get("retained_through", ""),
        )

    except ResourceNotFoundError:
        # No entity at this PartitionKey/RowKey — the expected path for
        # most resources, no logging needed.
        return None

    except Exception as e:
        # The entity exists (or the lookup itself failed some other
        # way) but something about it couldn't be used — a missing
        # field, a bad date format, a table/permission problem. This
        # must NOT be silently treated the same as "no record" — that
        # previously hid real data-entry and schema errors behind a
        # false NO_RECORD outcome. Fail loud, fail closed: log it and
        # still return None so the caller proceeds to remove_confirmed
        # rather than crash the whole Mover event over a bad retention
        # row, but the operator finds out why.
        #
        # entity's actual keys/values are logged here specifically so
        # a field-name mismatch (e.g. "GrantedDate" inserted vs.
        # "granted_date" expected) is visible on the spot, rather than
        # requiring a separate manual export to diagnose.
        actual_fields = dict(entity) if entity is not None else "entity fetch itself failed"
        logger.error(
            "Retention record lookup failed for PartitionKey=%s, "
            "RowKey=%s in table %s — treating as NO_RECORD, but this is "
            "NOT a normal 'no record exists' case. Error: %s. "
            "Actual entity contents: %s",
            employee_id, row_key, table_name, str(e), actual_fields,
        )
        return None


# Decision logic

def evaluate_retention(
    resource_type: str,
    resource_id:   str,
    record:        Optional[RetentionRecord],
    today:         date,
) -> ResourceRetentionDecision:
    """
    Apply retention decision logic for a single resource.

    Pure function — no I/O. Takes a record (or None) and today's date,
    returns a ResourceRetentionDecision.

    Args:
        resource_type: e.g. "accessPackage" — carried through for the
                       audit record, not used in the decision itself.
        resource_id:   Entra object ID being evaluated.
        record:        RetentionRecord from Table Storage, or None if no
                       entry exists.
        today:         The reference date for expiry comparison. Passed
                       in explicitly so tests can control it without
                       mocking.

    Returns:
        ResourceRetentionDecision with outcome RETAINED, EXPIRED, or
        NO_RECORD.
    """
    if record is None:
        return ResourceRetentionDecision(
            resource_type=resource_type,
            resource_id=resource_id,
            outcome=RetentionOutcome.NO_RECORD,
            record=None,
        )

    if record.review_date >= today:
        return ResourceRetentionDecision(
            resource_type=resource_type,
            resource_id=resource_id,
            outcome=RetentionOutcome.RETAINED,
            record=record,
        )

    # Record exists but review_date has passed — retention expired.
    return ResourceRetentionDecision(
        resource_type=resource_type,
        resource_id=resource_id,
        outcome=RetentionOutcome.EXPIRED,
        record=record,
    )


# Orchestration

def evaluate_all_retentions(
    employee_id:   str,
    resource_ids:  frozenset[str],
    table_client:  TableServiceClient,
    resource_type: str = "accessPackage",
    today:         Optional[date] = None,
    table_name:    str = "RetentionRegistry",
) -> RetentionResult:
    """
    Evaluate retention records for every resource flagged for removal.

    Calls fetch_retention_record() and evaluate_retention() per resource,
    then splits results into remove_confirmed and retain_set.

    Args:
        employee_id:   HR source identifier — used as PartitionKey.
        resource_ids:  Resource IDs flagged for removal — for the Mover,
                       this is MoverDelta.groups_to_remove (which holds
                       access package IDs, despite the field name).
        table_client:  Injected TableServiceClient.
        resource_type: Defaults to "accessPackage" — the only resource
                       type currently populated in the registry.
        today:         Reference date for expiry checks. Defaults to
                       date.today() if not provided. Pass explicitly
                       in tests.
        table_name:    RetentionRegistry table name.

    Returns:
        RetentionResult with remove_confirmed, retain_set, and full
        decisions list.

    Side effects:
        One Table Storage GET per resource in resource_ids.
    """
    if today is None:
        today = date.today()

    decisions:        list[ResourceRetentionDecision] = []
    remove_confirmed: set[str] = set()
    retain_set:       set[str] = set()

    for resource_id in resource_ids:
        record = fetch_retention_record(
            employee_id=employee_id,
            resource_type=resource_type,
            resource_id=resource_id,
            table_client=table_client,
            table_name=table_name,
        )

        decision = evaluate_retention(
            resource_type=resource_type,
            resource_id=resource_id,
            record=record,
            today=today,
        )

        decisions.append(decision)

        if decision.outcome == RetentionOutcome.RETAINED:
            retain_set.add(resource_id)
        else:
            # NO_RECORD and EXPIRED both result in removal.
            remove_confirmed.add(resource_id)

    return RetentionResult(
        remove_confirmed=frozenset(remove_confirmed),
        retain_set=frozenset(retain_set),
        decisions=decisions,
    )