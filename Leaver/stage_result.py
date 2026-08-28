"""
Leaver/stage_result.py

The contract every Leaver stage returns. Deliberate mirror of
Mover/stage_result.py (itself a mirror of the Joiner's), copied rather than
imported so the Leaver pipeline stays self-contained and a change to another
pipeline can never reach it. The files carry the same shape; if one changes,
change the others.

Stages describe what happened in plain, serializable data. They never touch
the audit_record, never hold a client, never decide control flow. The driver
(sync today, Durable orchestrator later) reads the StageResult and decides
what to do next.

Why serializable: at the Durable step each stage becomes an activity, and
activity inputs/outputs must cross a process boundary as JSON. A StageResult
carrying only primitives, dicts, and lists crosses cleanly. Anything richer
(a live client, a dataclass with methods) would not.
"""

from __future__ import annotations
from dataclasses import dataclass, field


class StageOutcome:
    """
    How a stage ended, from the driver's point of view. Plain strings (not an
    enum) so a StageResult round-trips through JSON with no custom encoding at
    the Durable boundary.

    The Leaver path uses PROCEED / DUPLICATE / QUEUED / FAILED. HELD and
    SKIPPED are retained for contract parity with the Joiner and Mover but are
    unused here — the Leaver has no hold queue and no governance gate that
    could hold an event; a failure after the lock is a Failed event, not a
    held record.
    """
    PROCEED   = "proceed"    # stage succeeded, continue to the next stage
    HELD      = "held"       # (unused on Leaver) routed to a hold queue
    FAILED    = "failed"     # execution failure after the lock — mark event Failed
    DUPLICATE = "duplicate"  # idempotency exit — event already claimed
    QUEUED    = "queued"     # conflict — another Leaver event is in progress
    SKIPPED   = "skipped"    # (unused on Leaver) nothing to do


@dataclass
class StageResult:
    """
    The single return shape for every stage.

    ok             — did the stage do its job without an execution failure.
                     Distinct from outcome: a stage can be ok=True and still
                     return a non-PROCEED outcome (DUPLICATE/QUEUED are correct,
                     successful outcomes, not errors).
    outcome        — a StageOutcome string telling the driver what to do next.
    data           — plain serializable payload the next stage needs
                     (e.g. {"user_id": "...", "current_packages": [...]}).
                     Dicts, lists, primitives only — no clients, no dataclasses.
    report_actions — audit-action entries the DRIVER appends to
                     audit_record["actions_taken"]. Each is a plain dict
                     (action / detail / succeeded, plus package_id or group_id).
    report_warnings— plain strings the driver appends to audit_record["warnings"].
    hold_reasons   — unused on the Leaver path; kept for contract parity.
    summary        — one-line human summary for the driver's return dict / logs.
    """
    ok:              bool
    outcome:         str
    data:            dict       = field(default_factory=dict)
    report_actions:  list[dict] = field(default_factory=list)
    report_warnings: list[str]  = field(default_factory=list)
    hold_reasons:    list[str]  = field(default_factory=list)
    summary:         str        = ""

    def to_dict(self) -> dict:
        """Plain-dict form for the Durable activity boundary."""
        return {
            "ok":              self.ok,
            "outcome":         self.outcome,
            "data":            self.data,
            "report_actions":  self.report_actions,
            "report_warnings": self.report_warnings,
            "hold_reasons":    self.hold_reasons,
            "summary":         self.summary,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "StageResult":
        """Reconstruct from the dict form an activity returns."""
        return cls(
            ok=              raw["ok"],
            outcome=         raw["outcome"],
            data=            raw.get("data", {}),
            report_actions=  raw.get("report_actions", []),
            report_warnings= raw.get("report_warnings", []),
            hold_reasons=    raw.get("hold_reasons", []),
            summary=         raw.get("summary", ""),
        )