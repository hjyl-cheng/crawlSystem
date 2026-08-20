from __future__ import annotations

from collections import defaultdict
import json
import os
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from feature_engine.applier import FeatureObservationApplier
from feature_engine.events import CrawlerObservationRecorded
from feature_engine.runtime_environment import required_environment


TERMINAL_PLAN_STATUSES = ("succeeded", "partial", "failed", "cancelled")
OBSERVATION_KIND_ORDER = {"about": 1, "video": 2, "agent": 3}


def required(name: str) -> str:
    value = str(os.environ.get(name, "")).strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


class BorrowedConnection:
    """Let the applier use savepoints without closing the outer repair transaction."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)

    def close(self) -> None:
        return None


def _read_jsonl_events(path: str) -> list[dict[str, Any]]:
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise RuntimeError("cannot read REPLAY_EVENTS_JSONL_FILE") from error
    payloads: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as error:
            raise RuntimeError(
                f"invalid replay event JSON on line {line_number}"
            ) from error
        if not isinstance(payload, dict):
            raise RuntimeError(f"replay event on line {line_number} must be an object")
        payloads.append(payload)
    return payloads


def load_events(channel_id: str, *, jsonl_path: str) -> list[CrawlerObservationRecorded]:
    payloads = _read_jsonl_events(jsonl_path)
    events = [
        CrawlerObservationRecorded.from_mapping(payload)
        for payload in payloads
    ]
    if not events:
        raise RuntimeError(f"no replay events found for {channel_id}")

    events.sort(
        key=lambda event: (
            event.observed_at,
            OBSERVATION_KIND_ORDER.get(event.observation_kind, 5),
            event.kind_sequence,
            event.event_id,
        )
    )

    sequences: dict[str, list[int]] = defaultdict(list)
    for event in events:
        if event.channel_id != channel_id:
            raise RuntimeError("Crawler Outbox event belongs to another Channel")
        sequences[event.observation_kind].append(event.kind_sequence)
    for kind, values in sequences.items():
        expected = list(range(1, max(values) + 1))
        if sorted(values) != expected:
            raise RuntimeError(f"{kind} sequences are not contiguous from 1: {values}")
    return events


def snapshot(connection: Any, channel_id: str) -> dict[str, Any]:
    state = connection.execute(
        """
        SELECT state_version,last_subscriber_count,about_metric_confidence,
               recent30_video_count,cardinality(recent_publish_interval_days) AS interval_count,
               last_about_observed_at,
               last_discovery_observed_at,last_recent_sampling_at,last_agent_observed_at
        FROM feature_clock.channel_feature_state
        WHERE channel_id=%s
        """,
        (channel_id,),
    ).fetchone()
    clock = connection.execute(
        """
        SELECT about_due_at,about_tier,video_due_at,video_tier,
               agent_due_at,agent_tier,
               policy_version,feature_state_version,clock_version
        FROM feature_clock.channel_clock_state
        WHERE channel_id=%s
        """,
        (channel_id,),
    ).fetchone()
    checkpoints = connection.execute(
        """
        SELECT observation_kind,last_applied_sequence
        FROM feature_clock.channel_observation_checkpoints
        WHERE channel_id=%s
        ORDER BY observation_kind
        """,
        (channel_id,),
    ).fetchall()
    return {
        "state": dict(state) if state is not None else None,
        "clock": dict(clock) if clock is not None else None,
        "checkpoints": [dict(row) for row in checkpoints],
    }


def main() -> None:
    database_url = required_environment(os.environ, "REPLAY_DATABASE_URL")
    channel_id = required("REPLAY_CHANNEL_ID")
    confirmation = required("REPLAY_CHANNEL_CONFIRMATION")
    jsonl_path = required("REPLAY_EVENTS_JSONL_FILE")
    if confirmation != channel_id:
        raise RuntimeError("REPLAY_CHANNEL_CONFIRMATION must exactly match REPLAY_CHANNEL_ID")
    expected_count = int(required("REPLAY_EXPECTED_EVENT_COUNT"))
    if expected_count <= 0:
        raise RuntimeError("REPLAY_EXPECTED_EVENT_COUNT must be positive")

    with psycopg.connect(
        database_url,
        autocommit=True,
        options="-c timezone=UTC",
        row_factory=dict_row,
    ) as connection:
        database = connection.execute(
            "SELECT current_database() AS database,current_user AS user_name"
        ).fetchone()
        events = load_events(channel_id, jsonl_path=jsonl_path)
        if len(events) != expected_count:
            raise RuntimeError(
                f"expected {expected_count} Crawler Outbox events, found {len(events)}"
            )
        active_plans = connection.execute(
            """
            SELECT count(*)::int AS count
            FROM feature_clock.daily_channel_plans
            WHERE channel_id=%s AND status<>ALL(%s)
            """,
            (channel_id, list(TERMINAL_PLAN_STATUSES)),
        ).fetchone()["count"]
        if active_plans:
            raise RuntimeError(f"Channel has {active_plans} non-terminal Daily Plan(s)")
        before = snapshot(connection, channel_id)

        results = []
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                cursor.execute(
                    "DELETE FROM feature_clock.crawler_event_inbox WHERE channel_id=%s",
                    (channel_id,),
                )
                cursor.execute(
                    "DELETE FROM feature_clock.channel_observation_checkpoints WHERE channel_id=%s",
                    (channel_id,),
                )
                cursor.execute(
                    "DELETE FROM feature_clock.channel_clock_state WHERE channel_id=%s",
                    (channel_id,),
                )
                cursor.execute(
                    "DELETE FROM feature_clock.channel_feature_state WHERE channel_id=%s",
                    (channel_id,),
                )

            applier = FeatureObservationApplier(
                lambda: BorrowedConnection(connection)
            )
            for event in events:
                result = applier.apply_crawler_observation(event)
                if result.status != "applied" or result.duplicate:
                    raise RuntimeError(
                        f"event {event.event_id} replay returned {result.status}"
                    )
                results.append(
                    {
                        "event_id": result.event_id,
                        "kind": event.observation_kind,
                        "sequence": event.kind_sequence,
                        "status": result.status,
                    }
                )

            after = snapshot(connection, channel_id)
            if after["state"] is None or after["clock"] is None:
                raise RuntimeError("replay did not restore Feature State and Clock State")
            if len(after["checkpoints"]) != len({event.observation_kind for event in events}):
                raise RuntimeError("replay did not restore every Observation checkpoint")

    print(
        json.dumps(
            {
                "database": database,
                "channel_id": channel_id,
                "event_count": len(events),
                "events": results,
                "before": before,
                "after": after,
            },
            default=str,
            ensure_ascii=True,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
