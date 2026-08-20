from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
import os
from uuid import uuid4

import psycopg

from feature_engine.applier import EventConflict, FeatureObservationApplier
from feature_engine.events import canonical_payload_hash


def event(
    *,
    channel_id: str,
    sequence: int,
    observed_at: str,
    subscribers: int | None,
    views: int | None,
    videos: int | None,
    outcome: str = "complete",
) -> dict:
    status = "exact" if outcome == "complete" else "unavailable"
    facts = {
        "subscriber_count": subscribers,
        "subscriber_count_status": status,
        "total_view_count": views,
        "total_view_count_status": status,
        "total_video_count": videos,
        "total_video_count_status": status,
    }
    body = json.dumps(facts, separators=(",", ":"))
    return {
        "event_id": str(uuid4()),
        "event_type": "crawler.observation.recorded",
        "event_version": 1,
        "observation_id": str(uuid4()),
        "channel_id": channel_id,
        "observation_kind": "about",
        "kind_sequence": sequence,
        "observed_at": observed_at,
        "outcome": outcome,
        "crawler_version": "qy-v16-validation",
        "payload_hash": f"sha256:{sha256(body.encode()).hexdigest()}",
        "payload": facts,
    }


def domain_event(
    *,
    channel_id: str,
    observation_kind: str,
    sequence: int,
    observed_at: str,
    payload: dict,
    outcome: str = "complete",
) -> dict:
    return {
        "event_id": str(uuid4()),
        "event_type": "crawler.observation.recorded",
        "event_version": 1,
        "observation_id": str(uuid4()),
        "channel_id": channel_id,
        "observation_kind": observation_kind,
        "kind_sequence": sequence,
        "observed_at": observed_at,
        "outcome": outcome,
        "crawler_version": "qy-v16-validation",
        "payload_hash": canonical_payload_hash(payload),
        "payload": payload,
    }


database_url = os.environ.get(
    "FEATURE_DATABASE_URL",
    "postgresql://feature:feature-test@127.0.0.1:5432/feature_clock_test",
)
channel_id = f"UCfeaturevalidation{uuid4()}"
first = event(
    channel_id=channel_id,
    sequence=1,
    observed_at="2026-07-20T00:00:00Z",
    subscribers=1000,
    views=10000,
    videos=20,
)
second = event(
    channel_id=channel_id,
    sequence=2,
    observed_at="2026-07-22T00:00:00Z",
    subscribers=1200,
    views=11000,
    videos=22,
)
failed = event(
    channel_id=channel_id,
    sequence=3,
    observed_at="2026-07-23T00:00:00Z",
    subscribers=None,
    views=None,
    videos=None,
    outcome="failed",
)


def connect():
    return psycopg.connect(database_url, options="-c timezone=UTC")


with connect() as connection:
    with connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS crawler")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS crawler.channels (channel_id TEXT PRIMARY KEY)"
        )


applier = FeatureObservationApplier(connect)
waiting = applier.apply_crawler_observation(second)
assert waiting.status == "waiting_gap"
assert waiting.last_applied_sequence == 0

applied = applier.apply_crawler_observation(first)
assert applied.status == "applied"
assert applied.last_applied_sequence == 2
assert applied.drained_event_ids == (second["event_id"],)

duplicate = applier.apply_crawler_observation(first)
assert duplicate.duplicate is True
assert duplicate.status == "applied"
assert duplicate.last_applied_sequence == 2

conflict = deepcopy(first)
conflict["payload"]["subscriber_count"] = 1001
body = json.dumps(conflict["payload"], separators=(",", ":"))
conflict["payload_hash"] = f"sha256:{sha256(body.encode()).hexdigest()}"
try:
    applier.apply_crawler_observation(conflict)
except EventConflict:
    pass
else:
    raise AssertionError("same event identity with different facts was not rejected")

failed_result = applier.apply_crawler_observation(failed)
assert failed_result.status == "applied"
assert failed_result.last_applied_sequence == 3

outcome_conflict = deepcopy(failed)
outcome_conflict["outcome"] = "partial"
try:
    applier.apply_crawler_observation(outcome_conflict)
except EventConflict:
    pass
else:
    raise AssertionError("same event identity with a different outcome was not rejected")


def clocks() -> tuple:
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT channel.about_due_day,channel.video_due_day,
                       channel.agent_due_day,channel.clock_version,
                       channel.feature_state_version
                FROM feature_clock.channel_clock_state channel
                WHERE channel.channel_id=%s
                """,
                (channel_id,),
            )
            return cursor.fetchone()


discovery_payload = {
    "pages": 1,
    "items": 2,
    "anchor_matched": True,
    "stop_reason": "anchor_matched",
    "parse_gap_count": 0,
    "first_seen": [
        {
            "video_id": "validation-new-1",
            "position": 1,
            "content_type": "video",
            "published_at": "2026-07-24T00:00:00Z",
            "published_at_precision": "second",
        },
        {
            "video_id": "validation-new-2",
            "position": 2,
            "content_type": "video",
            "published_at": "2026-07-23T00:00:00Z",
            "published_at_precision": "second",
        },
    ],
    "first_seen_count": 2,
    "detail_success_count": 2,
    "detail_failure_count": 0,
}
sampling_payload = {
    "recent_count": 8,
    "stale_ratio": 0.5,
    "selected_count": 4,
    "success_count": 4,
    "failure_count": 0,
    "next_count": 1,
    "comparable_view_count": 4,
    "view_changed_count": 2,
    "view_delta_total": 250,
    "engagement_changed_count": 1,
}
video = domain_event(
    channel_id=channel_id,
    observation_kind="video",
    sequence=1,
    observed_at="2026-07-24T02:00:00Z",
    payload={
        "discovery": {"outcome": "complete", "payload": discovery_payload},
        "recent_sampling": {"outcome": "complete", "payload": sampling_payload},
    },
)
before_video = clocks()
assert applier.apply_crawler_observation(video).status == "applied"
after_video = clocks()
assert after_video[0] == before_video[0]
assert after_video[2] == before_video[2]
assert after_video[3] == before_video[3] + 1

agent_payload = {
    "output_hash": f"sha256:{'a' * 64}",
    "category_level_1": "Technology",
    "category_level_2": ["AI", "Software"],
    "tag_count": 10,
    "evidence_count": 18,
    "active_subscriber_ratio": 35,
    "fulfilled_plan_count": 1,
}
agent = domain_event(
    channel_id=channel_id,
    observation_kind="agent",
    sequence=1,
    observed_at="2026-07-24T03:00:00Z",
    payload=agent_payload,
)
before_agent = clocks()
assert applier.apply_crawler_observation(agent).status == "applied"
after_agent = clocks()
assert after_agent[0:2] == before_agent[0:2]
assert after_agent[3] == before_agent[3] + 1

failed_agent = domain_event(
    channel_id=channel_id,
    observation_kind="agent",
    sequence=2,
    observed_at="2026-07-25T03:00:00Z",
    payload={"failed_plan_count": 1},
    outcome="failed",
)
before_failed_agent = clocks()
assert applier.apply_crawler_observation(failed_agent).status == "applied"
assert clocks() == before_failed_agent

with connect() as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT last_applied_sequence
            FROM feature_clock.channel_observation_checkpoints
            WHERE channel_id=%s AND observation_kind='about'
            """,
            (channel_id,),
        )
        assert cursor.fetchone()[0] == 3

        cursor.execute(
            """
            SELECT subscriber_velocity_ewma,view_velocity_ewma,video_count_delta,state_version
            FROM feature_clock.channel_feature_state
            WHERE channel_id=%s
            """,
            (channel_id,),
        )
        assert cursor.fetchone() == (100.0, 500.0, 2, 4)

        cursor.execute(
            """
            SELECT
              channel.channel_next_run_day = LEAST(
                channel.about_due_day,channel.video_due_day,channel.agent_due_day
              ) AS channel_min_ok,
              channel.clock_version,
              channel.feature_state_version
            FROM feature_clock.channel_clock_state channel
            WHERE channel.channel_id=%s
            """,
            (channel_id,),
        )
        assert cursor.fetchone() == (True, 4, 4)

        cursor.execute(
            """
            SELECT
              count(*) FILTER (WHERE status='applied')::int,
              count(*) FILTER (WHERE pending_payload_json IS NOT NULL)::int
            FROM feature_clock.crawler_event_inbox
            WHERE channel_id=%s
            """,
            (channel_id,),
        )
        assert cursor.fetchone() == (6, 0)

        cursor.execute(
            """
            SELECT count(*)::int
            FROM feature_clock.clock_decision_log
            WHERE channel_id=%s AND clock_kind='about'
            """,
            (channel_id,),
        )
        assert cursor.fetchone()[0] == 2

        cursor.execute(
            """
            SELECT count(*)::int
            FROM feature_clock.clock_decision_log
            WHERE channel_id=%s
              AND clock_kind IN ('video','agent')
            """,
            (channel_id,),
        )
        assert cursor.fetchone()[0] == 2

utc_channel_id = f"UCfeatureutc{uuid4()}"
offset_event = event(
    channel_id=utc_channel_id,
    sequence=1,
    observed_at="2026-07-21T00:30:00+08:00",
    subscribers=1000,
    views=10000,
    videos=20,
)
assert applier.apply_crawler_observation(offset_event).status == "applied"
with connect() as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT inbox.observed_at,clock.about_due_day,current_setting('TimeZone')
            FROM feature_clock.crawler_event_inbox inbox
            JOIN feature_clock.channel_clock_state clock USING (channel_id)
            WHERE inbox.event_id=%s
            """,
            (offset_event["event_id"],),
        )
        utc_observed_at, utc_about_due_day, session_timezone = cursor.fetchone()
        assert utc_observed_at.isoformat() == "2026-07-20T16:30:00+00:00"
        assert utc_about_due_day.isoformat() == "2026-07-27"
        assert session_timezone == "UTC"

print(
    json.dumps(
        {
            "ok": True,
            "sequence_gap_waited": True,
            "gap_drained_in_order": True,
            "duplicate_was_idempotent": True,
            "identity_conflict_rejected": True,
            "envelope_conflict_rejected": True,
            "failed_event_did_not_advance_clock": True,
            "four_domain_clocks_advanced_independently": True,
            "failed_agent_did_not_advance_clock": True,
            "channel_due_invariant": True,
            "single_video_clock": True,
            "applied_payloads_cleared": True,
            "offset_timestamp_normalized_to_utc": True,
            "postgres_session_timezone_utc": True,
        },
        separators=(",", ":"),
    )
)
