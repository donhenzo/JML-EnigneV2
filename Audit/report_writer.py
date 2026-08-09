"""
Audit/report_writer.py

Serializes DecisionReport objects and persists them to storage.

Phase 0: writes JSON files to local disk or Azure Blob Storage.
Every event gets its own report file. Reports are never overwritten —
each write uses a unique path derived from employee_id + timestamp.

Side effects:
    - Writes to disk or Azure Blob Storage.
    - Logs the output path of every report written.

Security considerations:
    - Reports must be written before the function returns. A missing
      report is an audit gap.
    - Reports must never be deleted, treat blob storage as append-only.
    - Access to the reports container should be restricted to the JML
      engine's Managed Identity and authorized auditors only.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

from Audit.models import DecisionReport

logger = logging.getLogger(__name__)



# Serialization
def report_to_dict(report: DecisionReport) -> dict:
    """
    Convert a DecisionReport to a JSON-serializable dict.

    All datetime and date objects are converted to ISO 8601 strings.
    Enums are converted to their string values.
    """
    raw = asdict(report)
    return _serialize_types(raw)


def _serialize_types(obj: Any) -> Any:
    """Recursively convert non-JSON-serializable types."""
    if isinstance(obj, dict):
        return {k: _serialize_types(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize_types(i) for i in obj]
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    # Enum values are already strings via str(Enum) mixin on our enums.
    return obj



# Local file writer
def write_report_to_file(
    report: DecisionReport,
    output_dir: str | Path,
) -> Path:
    """
    Write a DecisionReport as a JSON file to a local directory.

    Purpose:
        Phase 0 / local development output. Each report gets a unique
        filename derived from employee_id and event timestamp to prevent
        collisions and support time-ordered browsing.

    Inputs:
        report:     The completed DecisionReport.
        output_dir: Directory path to write reports into. Created if absent.

    Outputs:
        Path of the written report file.

    Side effects:
        Creates output_dir if it does not exist.
        Writes one JSON file per call.
    """
    dir_path = Path(output_dir)
    dir_path.mkdir(parents=True, exist_ok=True)

    filename = _report_filename(report)
    file_path = dir_path / filename

    report_dict = report_to_dict(report)
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(report_dict, f, indent=2)

    logger.info(
        "Audit report written: %s (upn=%s, event=%s, success=%s)",
        file_path,
        report.upn,
        report.event.value,
        report.overall_success,
    )
    return file_path


# Azure Blob writer
def write_report_to_blob(
    report: DecisionReport,
    account_url: str,
    container_name: str,
) -> str:
    """
    Write a DecisionReport as a JSON blob to Azure Blob Storage.

    Purpose:
        Production audit persistence. Each report is a separate blob.
        Blob paths are structured for time-ordered browsing:
            reports/{year}/{month}/{filename}.json

    Inputs:
        report:         The completed DecisionReport.
        account_url:    e.g. "https://<account>.blob.core.windows.net"
        container_name: Container to write reports into.

    Outputs:
        The full blob path written.

    Side effects:
        Makes a network call to Azure Blob Storage.

    Security considerations:
        Uses DefaultAzureCredential — Managed Identity in production.
        The reports container should be configured with immutability
        policies to prevent deletion or overwrite.
    """
    try:
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import BlobServiceClient
    except ImportError as exc:
        raise ImportError(
            "azure-identity and azure-storage-blob are required for blob writing. "
            "Install them or use write_report_to_file() for local use."
        ) from exc

    filename = _report_filename(report)
    ts = report.timestamp
    blob_path = f"reports/{ts.year}/{ts.month:02d}/{filename}"

    report_json = json.dumps(report_to_dict(report), indent=2)

    credential = DefaultAzureCredential()
    client = BlobServiceClient(account_url=account_url, credential=credential)
    blob_client = client.get_blob_client(container=container_name, blob=blob_path)

    blob_client.upload_blob(
        report_json.encode("utf-8"),
        overwrite=False,  # Never overwrite each report is immutable.
    )

    logger.info(
        "Audit report written to blob: %s/%s (upn=%s, event=%s, success=%s)",
        container_name,
        blob_path,
        report.upn,
        report.event.value,
        report.overall_success,
    )
    return blob_path


# Helpers
def _report_filename(report: DecisionReport) -> str:
    """
    Generate a unique, sortable filename for a report.

    Format: {employee_id}_{event}_{timestamp_utc}.json
    Timestamp is ISO 8601 with colons replaced by hyphens for filesystem safety.
    """
    ts = report.timestamp.strftime("%Y%m%dT%H%M%S")
    safe_employee_id = report.employee_id.replace("/", "-").replace("\\", "-")
    event = report.event.value.lower()
    return f"{safe_employee_id}_{event}_{ts}.json"