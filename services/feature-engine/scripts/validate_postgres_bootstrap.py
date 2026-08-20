from __future__ import annotations

from datetime import date
from hashlib import sha256
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row

from feature_engine.bootstrap import (
    BASELINE_BUNDLE_FORMAT,
    BootstrapError,
    FeatureBootstrapper,
    load_baseline_bundle,
)
from feature_engine.recalculation import RECALCULATION_RESUME_LOCK_ID
from feature_engine.scheduler import ClockStateInvariantError, DailyPlanConfig, DailyScheduler


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


def about_event(channel_id: str, subscriber_count: int) -> dict:
    payload = {
        "subscriber_count": subscriber_count,
        "subscriber_count_status": "exact",
        "total_view_count": subscriber_count * 100,
        "total_view_count_status": "exact",
        "total_video_count": 10,
        "total_video_count_status": "exact",
    }
    facts = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return {
        "event_id": str(uuid4()),
        "event_type": "crawler.observation.recorded",
        "event_version": 1,
        "observation_id": str(uuid4()),
        "channel_id": channel_id,
        "observation_kind": "about",
        "kind_sequence": 1,
        "observed_at": "2026-07-20T12:00:00Z",
        "outcome": "complete",
        "crawler_version": "qy-v16-bootstrap-validation",
        "payload_hash": f"sha256:{sha256(facts.encode()).hexdigest()}",
        "payload": payload,
    }


def write_bundle(directory: Path) -> Path:
    events = sorted(
        [
            about_event("UC-bootstrap-a", 100),
            about_event("UC-bootstrap-b", 200),
        ],
        key=lambda event: (
            event["channel_id"],
            event["observation_kind"],
            event["kind_sequence"],
        ),
    )
    event_bytes = b"".join(
        json.dumps(event, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
        for event in events
    )
    events_path = directory / "events.ndjson"
    events_path.write_bytes(event_bytes)
    manifest = {
        "schema_version": 1,
        "bundle_format": BASELINE_BUNDLE_FORMAT,
        "baseline_version": "bootstrap-validation-1",
        "source_database": "bullmq_crawler_migration",
        "source_schema": "crawler",
        "source_snapshot_id": "bootstrap-validation-snapshot-1",
        "exported_at": "2026-07-21T00:00:00Z",
        "events_file": events_path.name,
        "event_count": len(events),
        "channel_count": 2,
        "byte_count": len(event_bytes),
        "events_sha256": f"sha256:{sha256(event_bytes).hexdigest()}",
    }
    manifest_path = directory / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    return manifest_path


with connect() as connection:
    with connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS crawler")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS crawler.channels (channel_id TEXT PRIMARY KEY)"
        )
        cursor.execute(
            """
            SELECT current_database() AS database_name,
                   to_regnamespace('crawler') IS NOT NULL AS crawler_ready,
                   current_setting('TimeZone')='UTC' AS timezone_utc,
                   (SELECT count(*) FROM feature_clock.recalculation_runs)=0 AS clean_runs,
                   (SELECT count(*) FROM feature_clock.channel_feature_state)=0 AS clean_state
            """
        )
        safety = cursor.fetchone()
        if (
            safety["database_name"] != expected_database
            or not safety["crawler_ready"]
            or not safety["timezone_utc"]
            or not safety["clean_runs"]
            or not safety["clean_state"]
        ):
            raise RuntimeError("validation requires a clean shared-layout UTC test database")

with TemporaryDirectory() as temporary:
    bundle = load_baseline_bundle(write_bundle(Path(temporary)))
    bootstrapper = FeatureBootstrapper(connect)
    recalculation_id = bootstrapper.start(bundle, shard_count=4)

    scheduler = DailyScheduler(connect)
    try:
        scheduler.run_batch(
            plan_day=date(2026, 7, 21),
            config=DailyPlanConfig(
                planner_config_version="bootstrap-validation-plan",
                capacity_version="bootstrap-validation-capacity",
            ),
            batch_size=1,
        )
    except ClockStateInvariantError:
        pass
    else:
        raise AssertionError("Scheduler did not block the unresolved initial Bootstrap")

    lock_connection = connect()
    try:
        with lock_connection.transaction():
            with lock_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_lock(%s)",
                    (RECALCULATION_RESUME_LOCK_ID,),
                )
        try:
            bootstrapper.resume(recalculation_id, bundle, batch_size=1)
        except BootstrapError:
            pass
        else:
            raise AssertionError("a concurrent Bootstrap worker acquired the execution lock")
    finally:
        lock_connection.close()

    result = bootstrapper.resume(recalculation_id, bundle, batch_size=1)
    assert result.status == "succeeded"
    assert result.processed_events == 2
    assert result.processed_channels == 2
    assert result.failed_channels == 0
    assert result.succeeded_shards == 4
    assert result.failed_shards == 0

    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT count(*)::int AS channels,
                       count(*) FILTER (
                         WHERE about_due_day BETWEEN DATE '2026-07-22' AND DATE '2026-07-28'
                           AND video_due_day BETWEEN DATE '2026-07-22' AND DATE '2026-07-28'
                           AND agent_due_day BETWEEN DATE '2026-08-20' AND DATE '2026-10-19'
                           AND channel_next_run_day=LEAST(
                             about_due_day,video_due_day,agent_due_day
                           )
                       )::int AS valid_spread
                FROM feature_clock.channel_clock_state
                """
            )
            spread = cursor.fetchone()
            assert (spread["channels"], spread["valid_spread"]) == (2, 2)
            cursor.execute(
                """
                SELECT
                  (SELECT count(*) FROM feature_clock.channel_observation_checkpoints
                   WHERE last_applied_sequence=1)::int AS checkpoints,
                  (SELECT count(*) FROM feature_clock.bootstrap_channel_receipts)::int AS receipts,
                  (SELECT count(*) FROM feature_clock.clock_decision_log
                   WHERE decision_mode='bootstrap'
                     AND 'initial_bootstrap_spread'=ANY(reason_codes))::int AS spread_decisions,
                  (SELECT count(*) FROM feature_clock.crawler_event_inbox
                   WHERE pending_payload_json IS NOT NULL)::int AS retained_payloads
                """
            )
            audit = cursor.fetchone()
            assert tuple(audit.values()) == (2, 2, 6, 0)
            cursor.execute(
                """
                SELECT channel_id,clock_version
                FROM feature_clock.channel_clock_state
                ORDER BY channel_id
                """
            )
            versions_before = tuple((row["channel_id"], row["clock_version"]) for row in cursor)

    repeated = bootstrapper.resume(recalculation_id, bundle, batch_size=1)
    assert repeated == result
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT channel_id,clock_version FROM feature_clock.channel_clock_state ORDER BY channel_id"
            )
            versions_after = tuple((row["channel_id"], row["clock_version"]) for row in cursor)
    assert versions_after == versions_before

    unblocked = scheduler.run_batch(
        plan_day=date(1900, 1, 1),
        config=DailyPlanConfig(
            planner_config_version="bootstrap-validation-plan",
            capacity_version="bootstrap-validation-capacity",
        ),
        batch_size=1,
    )
    assert unblocked.selected == 0

print(
    json.dumps(
        {
            "ok": True,
            "manifest_was_validated_before_import": True,
            "scheduler_blocked_unresolved_bootstrap": True,
            "concurrent_bootstrap_worker_was_rejected": True,
            "event_sequences_were_checkpointed": True,
            "feature_and_clock_coverage_matched_manifest": True,
            "initial_due_days_were_stably_spread": True,
            "bootstrap_resume_was_idempotent": True,
            "applied_inbox_payloads_were_cleared": True,
        },
        separators=(",", ":"),
    )
)
