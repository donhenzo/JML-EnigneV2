# mapping/mapping_loader.py
#
# Loads the role mapping rule file from disk.
# Returns the parsed rule list ready for the resolver to evaluate.
#
# Phase 1 loads from disk. Azure Storage path added when the
# Function app is deployed — same pattern as lookup_loader.py.

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def load_mapping_rules(rules_path: str) -> list[dict]:
    """
    Load the role mapping rule set from a JSON file.

    Inputs:
        rules_path — path to role_mapping_rules.json

    Output:
        List of rule dicts, sorted ascending by priority.
        Lower priority number evaluates first.

    Raises:
        FileNotFoundError if the path does not exist.
        ValueError if the JSON is missing the top-level 'rules' key
        or if the file is empty.
    """
    path = Path(rules_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Mapping rules file not found: {rules_path}"
        )

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    if "rules" not in raw:
        raise ValueError(
            f"Mapping rules file is missing top-level 'rules' key: {rules_path}"
        )

    rules = raw["rules"]

    if not rules:
        raise ValueError(
            f"Mapping rules file contains an empty rule list: {rules_path}"
        )

    # Sort by priority ascending — lower number evaluates first.
    # Rules without a priority field sort to the end.
    rules_sorted = sorted(rules, key=lambda r: r.get("priority", 9999))

    logger.info(
        f"Mapping rules loaded — {len(rules_sorted)} rules from {rules_path}"
    )

    return rules_sorted