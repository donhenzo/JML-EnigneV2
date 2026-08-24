"""
Joiner/stage_result.py

The contract every pipeline stage returns. Stages describe what happened
in plain, serializable data — they never touch DecisionReport, never hold
a client, never decide control flow. The driver (sync today, Durable
orchestrator later) reads the StageResult and decides what to do next.

Why serializable: at the Durable step each stage becomes an activity, and
activity inputs/outputs must cross a process boundary as JSON. A StageResult
carrying only primitives, dicts, and lists crosses cleanly. Anything richer
(a live client, a DecisionReport, a dataclass with methods) would not.
"""

from __future__ import annotations

from dataclasses import dataclass, field


class StageOutcome:
    """
    How a stage ended, from the driver's point of view. Kept as plain
    strings (not an enum) so a StageResult round-trips through JSON with
    no custom encoding at the Durable boundary.
    """
    PROCEED   = "proceed"    # stage succeeded, continue to the next stage
    HELD      = "held"       # routed to the hold queue — a governance/data stop
    FAILED    = "failed"     # execution failure after the lock — mark event Failed
    DUPLICATE = "duplicate"  # idempotency exit — event already claimed
    QUEUED    = "queued"     # conflict queue parked this event behind another
    SKIPPED   = "skipped"    # nothing to do (e.g. active event already processing)


@dataclass
class StageResult:
    """
    The single return shape for every stage.

    ok            — did the stage do its job without an execution failure.
                    Distinct from outcome: a stage can be ok=True and still
                    return outcome=HELD (a governance stop is a correct,
                    successful outcome, not an error).
    outcome       — a StageOutcome string telling the driver what to do next.
    data          — plain serializable payload the next stage needs
                    (e.g. {"event_id": "...", "entra_id": "..."}). Dicts,
                    lists, primitives only — no clients, no dataclasses.
    report_actions— audit entries the DRIVER applies to the DecisionReport.
                    Each is a dict of kwargs for report.add_action(...), e.g.
                    {"action": "NormalizationPassed", "detail": "...",
                     "succeeded": True}. The stage records what happened; the
                    driver owns the report object.
    report_warnings— plain strings the driver passes to report.add_warning().
    hold_reasons  — plain strings the driver passes to report.add_hold_reason()
                    when outcome is HELD.
    summary       — one-line human summary for the driver's return dict / logs.
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