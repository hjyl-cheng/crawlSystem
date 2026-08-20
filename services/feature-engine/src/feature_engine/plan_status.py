from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


PLAN_OBSERVATION_KINDS = ("about", "video", "agent")


@dataclass(frozen=True, slots=True)
class DailyPlanStatusDecision:
    status: str
    error_code: str | None


def logical_plan_outcomes(
    plan: Mapping[str, Any], outcomes: Mapping[str, str]
) -> dict[str, str]:
    return {
        kind: outcomes[kind]
        for kind in PLAN_OBSERVATION_KINDS
        if bool(plan.get(f"run_{kind}")) and kind in outcomes
    }


def reduce_daily_plan_status(
    plan: Mapping[str, Any], outcomes: Mapping[str, str]
) -> DailyPlanStatusDecision:
    expected = {
        kind for kind in PLAN_OBSERVATION_KINDS if bool(plan.get(f"run_{kind}"))
    }
    logical_outcomes = logical_plan_outcomes(plan, outcomes)
    invalid = {
        outcome
        for outcome in logical_outcomes.values()
        if outcome not in {"complete", "partial", "failed"}
    }
    if invalid:
        raise ValueError(f"invalid Daily Plan outcomes: {sorted(invalid)}")

    observed = set(logical_outcomes)
    if any(outcome == "failed" for outcome in logical_outcomes.values()):
        return DailyPlanStatusDecision("failed", "crawler_observation_failed")
    if observed != expected:
        return DailyPlanStatusDecision("running", None)
    if any(outcome == "partial" for outcome in logical_outcomes.values()):
        return DailyPlanStatusDecision("partial", "crawler_observation_partial")
    return DailyPlanStatusDecision("succeeded", None)
