from __future__ import annotations

import json
import os

import psycopg
from psycopg.rows import dict_row

from feature_engine.bootstrap import FeatureBootstrapper, load_baseline_bundle
from feature_engine.bootstrap_service import validate_bundle_expectations


def required(name: str) -> str:
    value = str(os.environ.get(name) or "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


database_url = required("FEATURE_DATABASE_URL")
expected_database = required("EXPECTED_FEATURE_DATABASE")
expected_user = required("EXPECTED_FEATURE_DATABASE_USER")
manifest_path = required("QY_BASELINE_MANIFEST_PATH")


def connect():
    return psycopg.connect(
        database_url,
        options="-c timezone=UTC",
        row_factory=dict_row,
    )


bundle = load_baseline_bundle(manifest_path)
validate_bundle_expectations(bundle, os.environ)

with connect() as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT current_database() AS database_name,
                   current_user AS database_user,
                   current_setting('TimeZone')='UTC' AS timezone_utc,
                   to_regnamespace('crawler') IS NOT NULL AS crawler_ready,
                   to_regclass('crawler.channels') IS NOT NULL AS channels_ready,
                   NOT has_table_privilege(current_user,'crawler.channels','SELECT')
                     AS crawler_tables_isolated,
                   to_regclass('feature_clock.bootstrap_channel_receipts') IS NOT NULL
                     AS bootstrap_ready,
                   (SELECT count(*) FROM feature_clock.recalculation_runs)=0 AS clean_runs,
                   (SELECT count(*) FROM feature_clock.channel_feature_state)=0 AS clean_state
            """
        )
        safety = cursor.fetchone()
        if (
            safety is None
            or safety["database_name"] != expected_database
            or safety["database_user"] != expected_user
            or not all(
                safety[name]
                for name in (
                    "timezone_utc",
                    "crawler_ready",
                    "channels_ready",
                    "crawler_tables_isolated",
                    "bootstrap_ready",
                    "clean_runs",
                    "clean_state",
                )
            )
        ):
            raise RuntimeError(
                "cross-language validation requires a clean shared Crawler/Feature database"
            )

result = FeatureBootstrapper(connect).bootstrap(bundle, shard_count=4, batch_size=2)
if result.status != "succeeded":
    raise RuntimeError(f"qy Baseline Bootstrap ended with status {result.status}")

with connect() as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
              (SELECT count(*) FROM feature_clock.channel_feature_state)::int
                AS feature_channels,
              (SELECT count(*) FROM feature_clock.channel_clock_state)::int
                AS channel_clocks,
              (SELECT count(*) FROM feature_clock.channel_observation_checkpoints)::int
                AS checkpoints,
              (SELECT count(*) FROM feature_clock.bootstrap_channel_receipts)::int
                AS receipts,
              (SELECT count(*) FROM feature_clock.crawler_event_inbox
               WHERE pending_payload_json IS NOT NULL)::int AS retained_payloads
            """
        )
        audit = cursor.fetchone()

expected_channels = bundle.manifest.channel_count
if audit is None or any(
    int(audit[field]) != expected_channels
    for field in ("feature_channels", "channel_clocks", "receipts")
):
    raise RuntimeError("Feature Bootstrap coverage does not match the qy Manifest")
if int(audit["checkpoints"]) != len(bundle.checkpoint_sequences):
    raise RuntimeError("Feature checkpoints do not match qy Sequence watermarks")
if int(audit["retained_payloads"]) != 0:
    raise RuntimeError("applied qy Baseline payloads were retained in the Feature Inbox")

print(
    json.dumps(
        {
            "ok": True,
            "baseline_version": bundle.manifest.baseline_version,
            "source_snapshot_id": bundle.manifest.source_snapshot_id,
            "event_count": bundle.manifest.event_count,
            "channel_count": expected_channels,
            "checkpoint_count": len(bundle.checkpoint_sequences),
            "feature_bootstrap_status": result.status,
            "shared_crawler_database": True,
            "crawler_tables_isolated": True,
            "applied_payloads_cleared": True,
        },
        separators=(",", ":"),
    )
)
