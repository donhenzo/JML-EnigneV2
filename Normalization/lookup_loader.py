"""
Normalization/lookup_loader.py

Loads the canonical lookup table from a JSON file.

The lookup table maps known attribute variants to standardized values.
It is stored in Azure Storage and loaded at function startup — not on every
row — so it can be updated without redeployment.

Unknown values are not an error here. The normalizer handles the None case
and routes the record to the hold queue. This loader's only job is to
produce the mapping dict cleanly.

"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def load_lookup_table(source: str | Path) -> dict[str, dict[str, str]]:
    """
    Load the canonical lookup table from a JSON file path.

    Inputs:
        source: File path to the JSON lookup table. In production this will be
                a path to a file downloaded from Azure Blob Storage before the
                function runs. Local path in tests.

    Outputs:
        Dict keyed by field name (e.g. 'department', 'job_title', 'action',
        'employment_type'), each containing a variant→canonical mapping.

    Side effects:
        Logs a warning if expected top-level keys are missing. Returns an empty
        dict section for missing keys rather than raising — a missing section
        means every value in that category will be unresolvable (held).

    Expected JSON shape:
        {
            "department": {
                "sales": "Sales",
                "sales mgr": "Sales",
                "eng": "Engineering"
            },
            "job_title": {
                "sales mgr": "Sales Manager",
                "sr eng": "Senior Engineer"
            },
            "employment_type": {
                "emp": "Employee",
                "perm": "Employee",
                "ftc": "Contractor"
            },
            "action": {
                "join": "Joiner",
                "new starter": "Joiner",
                "transfer": "Mover",
                "term": "Leaver"
            }
        }
    """
    path = Path(source)

    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.error("Lookup table not found at path: %s", path)
        raise
    except OSError as exc:
        logger.error("Failed to read lookup table: %s", exc)
        raise

    try:
        data: dict[str, Any] = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error("Lookup table is not valid JSON: %s", exc)
        raise

    required_sections = {"department", "job_title", "employment_type", "action"}
    result: dict[str, dict[str, str]] = {}

    # Warn on missing required sections — return empty dict so normalizer
    # can flag every value in that category as unresolvable rather than crashing.
    for section in required_sections:
        if section not in data:
            logger.warning(
                "Lookup table missing section: '%s' — all values will be unresolvable",
                section,
            )
            result[section] = {}

    # Load all sections present in the JSON — including optional ones like location.
    # Keys are lowercased for case-insensitive matching in the normalizer.
    for section, mappings in data.items():
        result[section] = {
            str(variant).strip().lower(): str(canonical).strip()
            for variant, canonical in mappings.items()
        }

    return result