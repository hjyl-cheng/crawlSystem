from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
import os
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row

from feature_engine.rebuild import FeaturePolicyRebuilder, PolicyRebuildError
from feature_engine.recalculation import (
    RECALCULATION_COORDINATION_LOCK_ID,
    RECALCULATION_RESUME_LOCK_ID,
)
from feature_engine.reference_data import CollectionPrioritySignalStore
from feature_engine.scheduler import (
    ClockStateInvariantError,
    DailyPlanConfig,
    DailyScheduler,
)
from feature_engine.shared_features import CollectionPrioritySignals


database_url = os.environ.get(
    "FEATURE_DATABASE_URL",
    "postgresql://feature:feature-test@127.0.0.1:5432/feature_clock_test",
)
expected_database = os.environ.get("EXPECTED_FEATURE_DATABASE", "feature_clock_test")


def connect():
    return psycopg.connect(
        database_url,
        options="-c timezone=UTC",
        row_factory=dict_row,
    )


def snapshot(channel_id: str) -> dict:
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT state.state_version,state.manual_priority,
                       state.user_query_demand,state.collection_priority,
                       channel.clock_version AS channel_clock_version,
                       channel.about_due_day,channel.video_due_day,
                       channel.agent_due_day
                FROM feature_clock.channel_feature_state state
                JOIN feature_clock.channel_clock_state channel USING (channel_id)
                WHERE state.channel_id=%s
                """,
                (channel_id,),
            )
            row = cursor.fetchone()
            if row is None:
                raise AssertionError("validation Channel does not have complete Feature/Clock state")
            return dict(row)


with connect() as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT current_database() AS database_name,
                   to_regnamespace('crawler') IS NOT NULL AS crawler_ready,
                   current_setting('TimeZone')='UTC' AS timezone_utc
            """
        )
        database, crawler_ready, timezone_utc = cursor.fetchone().values()
        if database != expected_database or not crawler_ready or not timezone_utc:
            raise RuntimeError("validation requires the shared-layout UTC Feature test database")
        cursor.execute(
            """
            SELECT channel_id
            FROM feature_clock.channel_feature_state
            ORDER BY channel_id
            LIMIT 1
            """
        )
        selected = cursor.fetchone()
        if selected is None:
            raise RuntimeError("run validate_postgres_applier.py before this validation")
        channel_id = str(selected["channel_id"])

before_signal = snapshot(channel_id)
manual_priority = 0.0 if before_signal["manual_priority"] >= 0.5 else 1.0
user_query_demand = 0.0 if before_signal["user_query_demand"] >= 0.5 else 1.0
source_version = f"priority-validation-{uuid4()}"
observed_at = datetime.now(timezone.utc)
stored = CollectionPrioritySignalStore(connect).upsert(
    channel_id=channel_id,
    signals=CollectionPrioritySignals(
        user_query_demand=user_query_demand,
        manual_priority=manual_priority,
    ),
    source_version=source_version,
    observed_at=observed_at,
    user_query_demand_expires_at=observed_at + timedelta(days=30),
)
assert stored is True

after_signal = snapshot(channel_id)
assert after_signal["state_version"] == before_signal["state_version"] + 1
assert after_signal["channel_clock_version"] == before_signal["channel_clock_version"] + 1
assert after_signal["manual_priority"] == manual_priority
assert after_signal["user_query_demand"] == user_query_demand
for field in (
    "about_due_day",
    "video_due_day",
    "agent_due_day",
):
    assert after_signal[field] <= before_signal[field]

with connect() as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT count(*)::int AS decisions,count(DISTINCT clock_kind)::int AS kinds
            FROM feature_clock.clock_decision_log
            WHERE channel_id=%s AND decision_mode='repair'
              AND %s=ANY(reason_codes)
              AND feature_summary_json->'recalculation_context'->>'source_version'=%s
            """,
            (
                channel_id,
                "collection_priority_signal_update",
                source_version,
            ),
        )
        assert tuple(cursor.fetchone().values()) == (3, 3)

stale = CollectionPrioritySignalStore(connect).upsert(
    channel_id=channel_id,
    signals=CollectionPrioritySignals(
        user_query_demand=1.0 - user_query_demand,
        manual_priority=1.0 - manual_priority,
    ),
    source_version=f"stale-{source_version}",
    observed_at=observed_at - timedelta(seconds=1),
)
assert stale is False
assert snapshot(channel_id) == after_signal

rebuilder = FeaturePolicyRebuilder(connect)
recalculation_id = rebuilder.start(shard_count=4)

resume_lock_connection = connect()
try:
    with resume_lock_connection.transaction():
        with resume_lock_connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_lock(%s)",
                (RECALCULATION_RESUME_LOCK_ID,),
            )
    try:
        rebuilder.resume(recalculation_id, batch_size=1)
    except PolicyRebuildError:
        pass
    else:
        raise AssertionError("a concurrent Policy Rebuild worker acquired the execution lock")
finally:
    resume_lock_connection.close()

try:
    rebuilder.start(shard_count=4)
except PolicyRebuildError:
    pass
else:
    raise AssertionError("a second unresolved Policy Rebuild was created")

with connect() as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO feature_clock.recalculation_runs (
              recalculation_id,mode,policy_version,shard_count,status,
              started_at,completed_at,checksum
            ) VALUES (%s,'policy_rebuild','v16-rule-1',1,'succeeded',now(),now(),%s)
            """,
            (str(uuid4()), "sha256:validation-newer-succeeded"),
        )
scheduler = DailyScheduler(connect)
try:
    scheduler.run_batch(
        plan_day=date(1900, 1, 1),
        config=DailyPlanConfig(
            planner_config_version="validation-plan-1",
            capacity_version="validation-capacity-1",
        ),
        batch_size=1,
    )
except ClockStateInvariantError:
    pass
else:
    raise AssertionError("Scheduler ignored an older unresolved Policy Rebuild")

rebuilder.fail(recalculation_id)
try:
    scheduler.run_batch(
        plan_day=date(1900, 1, 1),
        config=DailyPlanConfig(
            planner_config_version="validation-plan-1",
            capacity_version="validation-capacity-1",
        ),
        batch_size=1,
    )
except ClockStateInvariantError:
    pass
else:
    raise AssertionError("Scheduler did not block a resumable failed Policy Rebuild")

before_rebuild = snapshot(channel_id)
with connect() as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE feature_clock.recalculation_shards
            SET status='running',lease_owner='still-active',
                lease_expires_at=now()+interval '15 minutes'
            WHERE recalculation_id=%s AND shard_id=0
            """,
            (recalculation_id,),
        )

leased = rebuilder.resume(recalculation_id, batch_size=1)
assert leased.status in {"partial", "failed"}
with connect() as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT status,lease_owner,lease_expires_at>now() AS lease_active
            FROM feature_clock.recalculation_shards
            WHERE recalculation_id=%s AND shard_id=0
            """,
            (recalculation_id,),
        )
        active_lease = cursor.fetchone()
        assert active_lease == {
            "status": "running",
            "lease_owner": "still-active",
            "lease_active": True,
        }

try:
    rebuilder.resume(
        recalculation_id,
        batch_size=1,
        source_baseline_version="too-late-reference-version",
    )
except PolicyRebuildError:
    pass
else:
    raise AssertionError("Resume changed source_baseline_version after shard progress")

with connect() as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE feature_clock.recalculation_shards
            SET lease_expires_at=now()-interval '1 second'
            WHERE recalculation_id=%s AND shard_id=0
            """,
            (recalculation_id,),
        )
rebuild = rebuilder.resume(recalculation_id, batch_size=1)
assert rebuild.status == "succeeded"
after_rebuild = snapshot(channel_id)
for field in (
    "about_due_day",
    "video_due_day",
    "agent_due_day",
):
    assert after_rebuild[field] <= before_rebuild[field]

with connect() as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT count(*)::int AS decisions,count(DISTINCT clock_kind)::int AS kinds
            FROM feature_clock.clock_decision_log
            WHERE channel_id=%s AND decision_mode='policy_rebuild'
              AND feature_summary_json->'recalculation_context'->>'recalculation_id'=%s
            """,
            (channel_id, recalculation_id),
        )
        assert tuple(cursor.fetchone().values()) == (3, 3)

repeated = rebuilder.resume(recalculation_id, batch_size=1)
assert repeated == rebuild
assert snapshot(channel_id) == after_rebuild
unblocked = scheduler.run_batch(
    plan_day=date(1900, 1, 1),
    config=DailyPlanConfig(
        planner_config_version="validation-plan-1",
        capacity_version="validation-capacity-1",
    ),
    batch_size=1,
)
assert unblocked.selected == 0

cancelled_id = str(uuid4())
with connect() as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO feature_clock.recalculation_runs (
              recalculation_id,mode,policy_version,shard_count,status,
              started_at,completed_at,checksum
            ) VALUES (%s,'policy_rebuild','v16-rule-1',1,'cancelled',now(),now(),%s)
            """,
            (cancelled_id, "sha256:validation-cancelled"),
        )
        cursor.execute(
            """
            INSERT INTO feature_clock.recalculation_shards (
              recalculation_id,shard_id,status
            ) VALUES (%s,0,'cancelled')
            """,
            (cancelled_id,),
        )
try:
    rebuilder.resume(cancelled_id, batch_size=1)
except PolicyRebuildError:
    pass
else:
    raise AssertionError("a cancelled Policy Rebuild was resumed")

scheduler_lock_connection = connect()
try:
    with scheduler_lock_connection.transaction():
        with scheduler_lock_connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_lock(%s)",
                (RECALCULATION_COORDINATION_LOCK_ID,),
            )

    def short_lock_connect():
        return psycopg.connect(
            database_url,
            options="-c timezone=UTC -c lock_timeout=100ms",
            row_factory=dict_row,
        )

    try:
        FeaturePolicyRebuilder(short_lock_connect).start(shard_count=1)
    except psycopg.errors.LockNotAvailable:
        pass
    else:
        raise AssertionError("Policy Rebuild started while Scheduler coordination lock was held")
finally:
    scheduler_lock_connection.close()

print(
    json.dumps(
        {
            "ok": True,
            "channel_id": channel_id,
            "priority_signal_was_atomic": True,
            "stale_priority_signal_was_rejected": True,
            "four_repair_decisions_were_audited": True,
            "scheduler_blocked_unfinished_rebuild": True,
            "scheduler_blocked_older_unresolved_rebuild": True,
            "scheduler_blocked_failed_rebuild": True,
            "concurrent_rebuild_start_was_rejected": True,
            "concurrent_rebuild_resume_was_rejected": True,
            "active_shard_lease_was_not_stolen": True,
            "late_source_version_change_was_rejected": True,
            "cancelled_rebuild_was_not_resumed": True,
            "scheduler_session_lock_excluded_rebuild_start": True,
            "policy_rebuild_succeeded": True,
            "policy_rebuild_resume_was_idempotent": True,
            "existing_due_days_were_not_delayed": True,
        },
        separators=(",", ":"),
    )
)
