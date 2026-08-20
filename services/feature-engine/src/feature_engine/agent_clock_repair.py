from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import argparse
import json
import os
from typing import Any, Callable, Mapping
from uuid import uuid4

from .clock_window import clock_due_at_for_day
from .database_topology import validate_shared_feature_database
from .policy import (
    AGENT_FORWARD_SPREAD_VERSION,
    agent_forward_spread_max_days,
    stable_agent_forward_offset,
)
from .runtime_environment import required_environment


@dataclass(frozen=True, slots=True)
class AgentClockRepairResult:
    apply: bool
    eligible_clocks: int
    changed_clocks: int
    unchanged_clocks: int
    tier_counts: dict[int, int]
    offset_counts: dict[int, int]


def repaired_agent_due_day(
    *,
    channel_id: str,
    policy_version: str,
    observed_day: date,
    tier_days: int,
) -> tuple[date, int]:
    offset = stable_agent_forward_offset(
        channel_id,
        policy_version=policy_version,
        tier_days=tier_days,
    )
    return observed_day + timedelta(days=tier_days + offset), offset


def _column_name(description: Any) -> str:
    return description.name if hasattr(description, "name") else description[0]


def _rows(cursor: Any) -> list[dict[str, Any]]:
    columns = [_column_name(description) for description in cursor.description]
    return [
        dict(value) if isinstance(value, Mapping) else dict(zip(columns, value, strict=True))
        for value in cursor.fetchall()
    ]


def repair_agent_clocks(
    connection_factory: Callable[[], Any],
    *,
    apply: bool,
    today: date | None = None,
) -> AgentClockRepairResult:
    utc_today = today or datetime.now(timezone.utc).date()
    connection = connection_factory()
    try:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                cursor.execute(
                    """
                    SELECT policy_version
                    FROM feature_clock.rule_policy_definitions
                    WHERE status='active' AND effective_from<=now()
                    ORDER BY effective_from DESC,policy_version DESC
                    LIMIT 1
                    FOR SHARE
                    """
                )
                active_policy = cursor.fetchone()
                if active_policy is None:
                    raise RuntimeError("no active Agent Clock policy is effective")
                policy_version = str(active_policy[0])
                cursor.execute(
                    """
                    WITH latest_agent_decision AS (
                      SELECT DISTINCT ON (channel_id)
                             decision_id,channel_id,decided_due_day,tier,
                             reason_codes,clock_version_after
                      FROM feature_clock.clock_decision_log
                      WHERE clock_kind='agent'
                      ORDER BY channel_id,clock_version_after DESC,decided_at DESC
                    )
                    SELECT clock.channel_id,clock.agent_due_at,clock.agent_due_day,
                           clock.agent_tier,clock.clock_version,
                           clock.about_due_at,clock.about_due_day,
                           clock.video_due_at,clock.video_due_day,
                           state.last_agent_observed_at,state.state_version,
                           state.reference_distribution_version,
                           decision.decision_id AS previous_decision_id,
                           decision.reason_codes AS previous_reason_codes
                    FROM feature_clock.channel_clock_state clock
                    JOIN feature_clock.channel_feature_state state USING (channel_id)
                    JOIN latest_agent_decision decision USING (channel_id)
                    WHERE clock.lifecycle_status='active'
                      AND state.last_agent_observed_at IS NOT NULL
                      AND clock.agent_due_day>%s
                      AND clock.agent_tier=decision.tier
                      AND (
                        clock.agent_due_day=decision.decided_due_day
                        OR (
                          'initial_bootstrap_spread'=ANY(decision.reason_codes)
                          AND clock.agent_due_day<(
                            (state.last_agent_observed_at AT TIME ZONE 'UTC')::date
                            + clock.agent_tier
                          )
                        )
                      )
                      AND NOT ('agent_forward_load_spread'=ANY(decision.reason_codes))
                      AND NOT EXISTS (
                        SELECT 1
                        FROM feature_clock.daily_channel_plans plan
                        WHERE plan.channel_id=clock.channel_id
                          AND plan.run_agent
                          AND plan.status IN (
                            'planned','dispatching','dispatched','running'
                          )
                      )
                    ORDER BY clock.channel_id
                    FOR UPDATE OF clock
                    """,
                    (utc_today,),
                )
                eligible = _rows(cursor)
                changed = 0
                tier_counts: Counter[int] = Counter()
                offset_counts: Counter[int] = Counter()
                for row in eligible:
                    observed_day = row["last_agent_observed_at"].astimezone(
                        timezone.utc
                    ).date()
                    tier = int(row["agent_tier"])
                    decided_due_day, offset = repaired_agent_due_day(
                        channel_id=str(row["channel_id"]),
                        policy_version=policy_version,
                        observed_day=observed_day,
                        tier_days=tier,
                    )
                    if decided_due_day <= row["agent_due_day"]:
                        continue
                    changed += 1
                    tier_counts[tier] += 1
                    offset_counts[offset] += 1
                    if not apply:
                        continue

                    decided_due_at = clock_due_at_for_day(decided_due_day)
                    clock_before = int(row["clock_version"])
                    clock_after = clock_before + 1
                    next_run_day = min(
                        row["about_due_day"], row["video_due_day"], decided_due_day
                    )
                    next_run_at = min(
                        row["about_due_at"], row["video_due_at"], decided_due_at
                    )
                    cursor.execute(
                        """
                        UPDATE feature_clock.channel_clock_state
                        SET agent_due_at=%s,agent_due_day=%s,
                            channel_next_run_at=%s,channel_next_run_day=%s,
                            clock_version=%s,updated_at=now()
                        WHERE channel_id=%s AND clock_version=%s
                          AND agent_due_day=%s
                        """,
                        (
                            decided_due_at,
                            decided_due_day,
                            next_run_at,
                            next_run_day,
                            clock_after,
                            row["channel_id"],
                            clock_before,
                            row["agent_due_day"],
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise RuntimeError("Agent Clock repair lost its optimistic lock")

                    maximum = agent_forward_spread_max_days(tier)
                    summary = {
                        "agent_base_due_day": (
                            observed_day + timedelta(days=tier)
                        ).isoformat(),
                        "agent_forward_spread_days": offset,
                        "agent_forward_spread_max_days": maximum,
                        "agent_forward_spread_version": AGENT_FORWARD_SPREAD_VERSION,
                        "last_agent_observed_day": observed_day.isoformat(),
                        "previous_decision_id": str(row["previous_decision_id"]),
                        "repair_scope": "future_unassigned_active_agent_clocks",
                    }
                    reasons = list(
                        dict.fromkeys(
                            [
                                *(row["previous_reason_codes"] or ()),
                                "agent_forward_load_spread",
                                "agent_forward_load_spread_repair",
                            ]
                        )
                    )
                    cursor.execute(
                        """
                        INSERT INTO feature_clock.clock_decision_log (
                          decision_id,channel_id,clock_kind,decision_mode,
                          previous_due_at,previous_due_day,
                          decided_due_at,decided_due_day,tier,reason_codes,
                          feature_state_version,feature_summary_json,policy_version,
                          reference_distribution_version,
                          clock_version_before,clock_version_after
                        ) VALUES (
                          %s,%s,'agent','repair',%s,%s,%s,%s,%s,%s,
                          %s,%s::jsonb,%s,%s,%s,%s
                        )
                        """,
                        (
                            str(uuid4()),
                            row["channel_id"],
                            row["agent_due_at"],
                            row["agent_due_day"],
                            decided_due_at,
                            decided_due_day,
                            tier,
                            reasons,
                            int(row["state_version"]),
                            json.dumps(summary, separators=(",", ":"), sort_keys=True),
                            policy_version,
                            row["reference_distribution_version"],
                            clock_before,
                            clock_after,
                        ),
                    )
        return AgentClockRepairResult(
            apply=apply,
            eligible_clocks=len(eligible),
            changed_clocks=changed,
            unchanged_clocks=len(eligible) - changed,
            tier_counts=dict(sorted(tier_counts.items())),
            offset_counts=dict(sorted(offset_counts.items())),
        )
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Move eligible future Agent Clocks into stable forward spread windows."
    )
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

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
        ),
    )
    result = repair_agent_clocks(connect, apply=args.apply)
    print(json.dumps(asdict(result), separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
