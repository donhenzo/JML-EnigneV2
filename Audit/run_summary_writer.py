# Audit/run_summary_writer.py
#
# Writes a single JSON run summary file per pipeline execution.
#
# WHY THIS EXISTS:
#   Individual event reports are the immutable audit trail, one file per
#   record, written regardless of outcome. They are the source of truth.
#   This file produces something different: a single operational snapshot
#   of an entire run — total counts, held record IDs, and failure reasons
#   in one place so an operator can assess a run without opening dozens of
#   individual files.
#
# The summary file is additive — it does not replace or modify event reports.
# Losing a summary is recoverable (rebuild from event reports).
# That is why write failures are swallowed here rather than raised.

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from Audit.models import DecisionReport

logger = logging.getLogger(__name__)


def write_run_summary(
    reports:        list[DecisionReport],
    output_dir:     str,
    trigger_type:   str = "local",
    correlation_id: str = "",
) -> str:
    """
    Write a run summary JSON file for a completed pipeline execution.

    The summary filename includes a timestamp tag so multiple runs writing
    to the same output directory do not overwrite each other.

    Held records and failed records are kept separate in the output:
        held    — records that did not reach provisioning (normalisation,
                  validation, or parse failures). Needs human review.
        failed  — records that reached provisioning but did not complete.
                  Typically retryable once the root cause is resolved.

    Never raises — any exception is logged and an empty string is returned.
    A summary write failure must never mask the pipeline result or prevent
    the caller from reporting a successful run.

    Args:
        reports        — DecisionReport objects collected across the pipeline run
        output_dir     — directory to write the summary file into
        trigger_type   — how the pipeline was invoked: HTTP, Timer, or local
        correlation_id — links this summary back to individual event reports

    Returns:
        Path to the written summary file, or an empty string on failure.
    """
    try:
        timestamp     = datetime.now(timezone.utc).isoformat()
        timestamp_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

        total     = len(reports)
        succeeded = sum(1 for r in reports if r.overall_success)
        held      = sum(1 for r in reports if r.hold_reasons)
        failed    = sum(
            1 for r in reports
            if not r.overall_success and not r.hold_reasons
        )

        # Held records — did not reach provisioning. Include reasons so
        # operators know why without opening the individual event report.
        held_records = [
            {
                "employee_id":  r.employee_id,
                "upn":          r.upn,
                "hold_reasons": r.hold_reasons,
            }
            for r in reports
            if r.hold_reasons
        ]

        # Failed records — provisioning was attempted but did not complete.
        # Only failed actions are included to keep the summary focused.
        failed_records = [
            {
                "employee_id": r.employee_id,
                "upn":         r.upn,
                "failed_actions": [
                    {
                        "action": a.action,
                        "detail": a.detail,
                    }
                    for a in r.actions_taken
                    if not a.succeeded
                ],
            }
            for r in reports
            if not r.overall_success and not r.hold_reasons
        ]

        summary = {
            "run_timestamp":   timestamp,
            "trigger_type":    trigger_type,
            "correlation_id":  correlation_id,
            "total":           total,
            "succeeded":       succeeded,
            "held":            held,
            "failed":          failed,
            "held_records":    held_records,
            "failed_records":  failed_records,
        }

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Prefix with underscore so the summary sorts to the top of the
        # directory listing alongside the per-record event report files.
        filename    = f"_run_summary_{timestamp_tag}.json"
        target_path = output_path / filename

        with open(target_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        logger.info(f"Run summary written: {target_path}")
        return str(target_path)

    except Exception as e:
        logger.error(f"Failed to write run summary: {e}")
        return ""