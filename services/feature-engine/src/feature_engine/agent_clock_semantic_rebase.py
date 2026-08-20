from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import argparse
import json
import os
from typing import Any, Callable, Mapping
from uuid import uuid4

from .applier import FEATURE_STATE_COLUMNS
from .database_topology import validate_shared_feature_database
from .policy import (
    AgentClockDecision,
    AgentPolicyConfig,
    decide_agent_due,
    runtime_policy_configs,
)
from .rebuild import _policy_from_row, _state_from_row
from .runtime_environment import required_environment
from .state import ChannelFeatureState


CONFIRMATION = "semantic-agent-clock-rebase"
ACTIVE_PLAN_STATUSES = ("planned", "dispatching", "dispatched", "running")


@dataclass(frozen=True, slots=True)
class AgentPlanRebaseResolution:
    plan_id: str
    channel_id: str
    previous_status: str
    target_status: str | None
    target_error_code: str | None
    outcomes: dict[str, str]
    reason: str


@dataclass(frozen=True, slots=True)
class AgentClockSemanticRebaseResult:
    apply: bool
    rebase_id: str
    policy_version: str
    eligible_clocks: int
    changed_clocks: int
    unchanged_clocks: int
    due_on_or_before_today_before: int
    due_on_or_before_today_after: int
    resolved_plans: int
    cancelled_agent_only_plans: int
    succeeded_mixed_plans: int
    partial_mixed_plans: int
    failed_mixed_plans: int
    blocked_plans: int
    tier_counts: dict[int, int]


def _column_name(description: Any) -> str:
    return description.name if hasattr(description, "name") else description[0]


def _row(cursor: Any) -> dict[str, Any] | None:
    value = cursor.fetchone()
    if value is None:
        return None
    if isinstance(value, Mapping):
        return dict(value)
    return {
        _column_name(description): item
        for description, item in zip(cursor.description, value, strict=True)
    }


def _rows(cursor: Any) -> list[dict[str, Any]]:
    columns = [_column_name(description) for description in cursor.description]
    return [
        dict(value) if isinstance(value, Mapping) else dict(zip(columns, value, strict=True))
        for value in cursor.fetchall()
    ]


def semantic_agent_rebase_decision(
    state: ChannelFeatureState,
    *,
    channel_id: str,
    config: AgentPolicyConfig,
) -> AgentClockDecision:
    if state.last_agent_observed_at is None:
        raise ValueError("semantic Agent Clock rebase requires an Agent observation")
    return decide_agent_due(
        state,
        observed_at=state.last_agent_observed_at,
        outcome="complete",
        baseline=False,
        output_changed=state.agent_output_changed,
        evidence_count=state.last_agent_evidence_count or 0,
        channel_id=channel_id,
        config=config,
    )


def resolve_rebased_agent_plan(
    plan: Mapping[str, Any], outcomes: Mapping[str, str]
) -> AgentPlanRebaseResolution:
    expected = {
        kind for kind in ("about", "video") if bool(plan.get(f"run_{kind}"))
    }
    logical = {kind: outcomes[kind] for kind in expected if kind in outcomes}
    base = {
        "plan_id": str(plan["plan_id"]),
        "channel_id": str(plan["channel_id"]),
        "previous_status": str(plan["status"]),
        "outcomes": logical,
    }
    if not expected:
        return AgentPlanRebaseResolution(
            **base,
            target_status="cancelled",
            target_error_code="agent_clock_semantic_policy_rebase",
            reason="agent_only_plan_superseded_by_semantic_clock_rebase",
        )
    if set(logical) != expected:
        return AgentPlanRebaseResolution(
            **base,
            target_status=None,
            target_error_code=None,
            reason="non_agent_observation_missing",
        )
    invalid = {value for value in logical.values() if value not in {"complete", "partial", "failed"}}
    if invalid:
        return AgentPlanRebaseResolution(
            **base,
            target_status=None,
            target_error_code=None,
            reason="non_agent_observation_invalid",
        )
    if any(value == "failed" for value in logical.values()):
        target_status = "failed"
        error_code = "crawler_observation_failed"
    elif any(value == "partial" for value in logical.values()):
        target_status = "partial"
        error_code = "crawler_observation_partial"
    else:
        target_status = "succeeded"
        error_code = None
    return AgentPlanRebaseResolution(
        **base,
        target_status=target_status,
        target_error_code=error_code,
        reason="agent_requirement_superseded_non_agent_outcomes_preserved",
    )


def _load_plan_resolutions(
    cursor: Any,
    *,
    channel_ids: list[str],
    today: date,
    lock: bool,
) -> tuple[AgentPlanRebaseResolution, ...]:
    lock_clause = "FOR UPDATE OF plan" if lock else ""
    cursor.execute(
        f"""
        SELECT plan.plan_id::text AS plan_id,plan.channel_id,
               plan.run_about,plan.run_video,plan.status,
               latest.observation_kind,latest.outcome
        FROM feature_clock.daily_channel_plans plan
        LEFT JOIN LATERAL (
          SELECT DISTINCT ON (inbox.observation_kind)
                 inbox.observation_kind,inbox.outcome
          FROM feature_clock.crawler_event_inbox inbox
          WHERE inbox.plan_id=plan.plan_id AND inbox.status='applied'
          ORDER BY inbox.observation_kind,inbox.kind_sequence DESC
        ) latest ON true
        WHERE plan.channel_id=ANY(%s::text[])
          AND plan.run_agent
          AND plan.plan_day<=%s
          AND plan.status=ANY(%s::text[])
        ORDER BY plan.plan_id,latest.observation_kind
        {lock_clause}
        """,
        (channel_ids, today, list(ACTIVE_PLAN_STATUSES)),
    )
    grouped: dict[str, dict[str, Any]] = {}
    for row in _rows(cursor):
        current = grouped.setdefault(
            str(row["plan_id"]), {"plan": row, "outcomes": {}}
        )
        if row["observation_kind"] is not None:
            current["outcomes"][str(row["observation_kind"])] = str(row["outcome"])
    return tuple(
        resolve_rebased_agent_plan(value["plan"], value["outcomes"])
        for _, value in sorted(grouped.items())
    )


def rebase_agent_clocks(
    connection_factory: Callable[[], Any],
    *,
    apply: bool,
    today: date | None = None,
) -> AgentClockSemanticRebaseResult:
    utc_today = today or datetime.now(timezone.utc).date()
    rebase_id = str(uuid4())
    connection = connection_factory()
    try:
        with connection.transaction():
            with connection.cursor() as cursor:
                if apply:
                    cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                cursor.execute(
                    """
                    SELECT *
                    FROM feature_clock.rule_policy_definitions
                    WHERE status='active' AND effective_from<=now()
                    ORDER BY effective_from DESC,policy_version DESC
                    LIMIT 1
                    FOR SHARE
                    """
                )
                policy_row = _row(cursor)
                if policy_row is None:
                    raise RuntimeError("no active Agent Clock policy is effective")
                policy = _policy_from_row(policy_row)
                agent_config = runtime_policy_configs(policy).agent
                state_columns = ",".join(
                    f"state.{column}" for column in FEATURE_STATE_COLUMNS
                )
                lock_clause = "FOR UPDATE OF clock" if apply else ""
                cursor.execute(
                    f"""
                    SELECT state.channel_id,{state_columns},
                           clock.agent_due_at AS clock_agent_due_at,
                           clock.agent_due_day AS clock_agent_due_day,
                           clock.agent_tier AS clock_agent_tier,
                           clock.about_due_at AS clock_about_due_at,
                           clock.about_due_day AS clock_about_due_day,
                           clock.video_due_at AS clock_video_due_at,
                           clock.video_due_day AS clock_video_due_day,
                           clock.policy_version AS clock_policy_version,
                           clock.clock_version AS channel_clock_version
                    FROM feature_clock.channel_feature_state state
                    JOIN feature_clock.channel_clock_state clock USING (channel_id)
                    WHERE clock.lifecycle_status='active'
                      AND state.last_agent_observed_at IS NOT NULL
                    ORDER BY state.channel_id
                    {lock_clause}
                    """
                )
                eligible = _rows(cursor)
                candidates: list[tuple[dict[str, Any], ChannelFeatureState, AgentClockDecision]] = []
                due_before = 0
                due_after = 0
                tier_counts: Counter[int] = Counter()
                future_channel_ids: list[str] = []
                for row in eligible:
                    state = _state_from_row(row)
                    decision = semantic_agent_rebase_decision(
                        state,
                        channel_id=str(row["channel_id"]),
                        config=agent_config,
                    )
                    candidates.append((row, state, decision))
                    due_before += int(row["clock_agent_due_day"] <= utc_today)
                    due_after += int(decision.due_day <= utc_today)
                    tier_counts[decision.tier_days] += 1
                    if decision.due_day > utc_today:
                        future_channel_ids.append(str(row["channel_id"]))

                resolutions = _load_plan_resolutions(
                    cursor,
                    channel_ids=future_channel_ids,
                    today=utc_today,
                    lock=apply,
                )
                blocked = tuple(item for item in resolutions if item.target_status is None)
                if apply and blocked:
                    raise RuntimeError(
                        f"refusing Agent Clock rebase: {len(blocked)} active plans lack non-Agent evidence"
                    )
                resolution_by_channel = {item.channel_id: item for item in resolutions}
                changed = 0
                if apply:
                    for resolution in resolutions:
                        assert resolution.target_status is not None
                        cursor.execute(
                            """
                            UPDATE feature_clock.daily_channel_plans
                            SET status=%s,error_code=%s,
                                lease_owner=NULL,lease_expires_at=NULL,
                                finished_at=COALESCE(finished_at,now()),
                                completed_at=COALESCE(completed_at,now()),updated_at=now()
                            WHERE plan_id=%s AND status=%s
                            """,
                            (
                                resolution.target_status,
                                resolution.target_error_code,
                                resolution.plan_id,
                                resolution.previous_status,
                            ),
                        )
                        if cursor.rowcount != 1:
                            raise RuntimeError("Agent plan rebase lost its optimistic lock")

                for row, state, decision in candidates:
                    channel_id = str(row["channel_id"])
                    resolution = resolution_by_channel.get(channel_id)
                    is_changed = (
                        decision.due_at != row["clock_agent_due_at"]
                        or decision.due_day != row["clock_agent_due_day"]
                        or decision.tier_days != int(row["clock_agent_tier"])
                        or policy.policy_version != row["clock_policy_version"]
                        or resolution is not None
                    )
                    if not is_changed:
                        continue
                    changed += 1
                    if not apply:
                        continue
                    next_run_at = min(
                        row["clock_about_due_at"], row["clock_video_due_at"], decision.due_at
                    )
                    next_run_day = min(
                        row["clock_about_due_day"], row["clock_video_due_day"], decision.due_day
                    )
                    clock_before = int(row["channel_clock_version"])
                    clock_after = clock_before + 1
                    cursor.execute(
                        """
                        UPDATE feature_clock.channel_clock_state
                        SET agent_due_at=%s,agent_due_day=%s,agent_tier=%s,
                            channel_next_run_at=%s,channel_next_run_day=%s,
                            policy_version=%s,feature_state_version=%s,
                            clock_version=%s,updated_at=now()
                        WHERE channel_id=%s AND clock_version=%s
                        """,
                        (
                            decision.due_at,decision.due_day,decision.tier_days,
                            next_run_at,next_run_day,policy.policy_version,
                            state.state_version,clock_after,channel_id,clock_before,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise RuntimeError("Agent Clock semantic rebase lost its optimistic lock")
                    plan_summary = None
                    if resolution is not None:
                        plan_summary = {
                            "plan_id": resolution.plan_id,
                            "previous_status": resolution.previous_status,
                            "target_status": resolution.target_status,
                            "non_agent_outcomes": resolution.outcomes,
                            "reason": resolution.reason,
                        }
                    cursor.execute(
                        """
                        INSERT INTO feature_clock.clock_decision_log (
                          decision_id,channel_id,clock_kind,decision_mode,
                          previous_due_at,previous_due_day,decided_due_at,decided_due_day,
                          tier,reason_codes,feature_state_version,feature_summary_json,
                          policy_version,reference_distribution_version,
                          clock_version_before,clock_version_after
                        ) VALUES (
                          %s,%s,'agent','repair',%s,%s,%s,%s,%s,%s,%s,%s::jsonb,
                          %s,%s,%s,%s
                        )
                        """,
                        (
                            str(uuid4()),channel_id,
                            row["clock_agent_due_at"],row["clock_agent_due_day"],
                            decision.due_at,decision.due_day,decision.tier_days,
                            list(dict.fromkeys((*decision.reason_codes, "agent_semantic_policy_rebase"))),
                            state.state_version,
                            json.dumps(
                                {
                                    **decision.feature_summary,
                                    "agent_semantic_rebase_id": rebase_id,
                                    "rebase_day": utc_today.isoformat(),
                                    "repair_scope": "all_active_observed_agent_clocks",
                                    "superseded_agent_plan": plan_summary,
                                },
                                separators=(",", ":"),sort_keys=True,
                            ),
                            policy.policy_version,state.reference_distribution_version,
                            clock_before,clock_after,
                        ),
                    )

        target_counts = Counter(
            item.target_status for item in resolutions if item.target_status is not None
        )
        agent_only = sum(
            item.target_status == "cancelled" for item in resolutions
        )
        return AgentClockSemanticRebaseResult(
            apply=apply,rebase_id=rebase_id,policy_version=policy.policy_version,
            eligible_clocks=len(eligible),changed_clocks=changed,
            unchanged_clocks=len(eligible)-changed,
            due_on_or_before_today_before=due_before,
            due_on_or_before_today_after=due_after,
            resolved_plans=len(resolutions)-len(blocked),
            cancelled_agent_only_plans=agent_only,
            succeeded_mixed_plans=target_counts["succeeded"],
            partial_mixed_plans=target_counts["partial"],
            failed_mixed_plans=target_counts["failed"],
            blocked_plans=len(blocked),tier_counts=dict(sorted(tier_counts.items())),
        )
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rebase all observed Agent Clocks onto the active semantic policy."
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args()
    if args.apply and args.confirm != CONFIRMATION:
        parser.error(f"--apply requires --confirm {CONFIRMATION}")
    if args.confirm and not args.apply:
        parser.error("--confirm is only valid with --apply")

    import psycopg

    database_url = required_environment(os.environ, "FEATURE_DATABASE_URL")
    expected_database = required_environment(os.environ, "EXPECTED_FEATURE_DATABASE")
    expected_user = required_environment(os.environ, "EXPECTED_FEATURE_DATABASE_USER")

    def connect() -> Any:
        return psycopg.connect(database_url, options="-c timezone=UTC")

    validate_shared_feature_database(
        connect,
        expected_database=expected_database,
        expected_user=expected_user,
        required_feature_relations=(
            "feature_clock.channel_clock_state",
            "feature_clock.channel_feature_state",
            "feature_clock.clock_decision_log",
            "feature_clock.daily_channel_plans",
            "feature_clock.crawler_event_inbox",
        ),
    )
    print(json.dumps(asdict(rebase_agent_clocks(connect, apply=args.apply)), sort_keys=True))


if __name__ == "__main__":
    main()
