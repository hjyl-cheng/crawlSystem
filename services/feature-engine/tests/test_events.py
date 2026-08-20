from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
from hashlib import sha256
import unittest
from uuid import uuid4

from feature_engine.events import (
    CrawlerObservationRecorded,
    EventValidationError,
    FailedDomainPayload,
    canonical_payload_hash,
)


def event(payload: dict | None = None, *, outcome: str = "complete", sequence: int = 1) -> dict:
    facts = payload or {
        "subscriber_count": 1000,
        "subscriber_count_status": "exact",
        "total_view_count": 2000,
        "total_view_count_status": "exact",
        "total_video_count": 30,
        "total_video_count_status": "exact",
    }
    body = json.dumps(facts, separators=(",", ":"))
    return {
        "event_id": str(uuid4()),
        "event_type": "crawler.observation.recorded",
        "event_version": 1,
        "observation_id": str(uuid4()),
        "channel_id": "UCexample",
        "observation_kind": "about",
        "kind_sequence": sequence,
        "observed_at": "2026-07-20T12:00:00Z",
        "outcome": outcome,
        "crawler_version": "qy-v16",
        "payload_hash": f"sha256:{sha256(body.encode()).hexdigest()}",
        "payload": facts,
    }


class CrawlerEventTests(unittest.TestCase):
    def test_accepts_the_minimal_about_event(self) -> None:
        source = event(
            {
                "subscriber_count": 1234,
                "subscriber_count_status": "exact",
                "total_view_count": 98765,
                "total_view_count_status": "exact",
                "total_video_count": 42,
                "total_video_count_status": "exact",
            }
        )
        self.assertEqual(
            source["payload_hash"],
            "sha256:d876fbf5f26c0280782a349db9f42ac1437a6b23188688297efa587c44221f22",
        )
        parsed = CrawlerObservationRecorded.from_mapping(source)
        self.assertEqual(parsed.payload.subscriber_count, 1234)
        self.assertEqual(parsed.payload.resolved_metric_count, 3)

    def test_normalizes_offset_observation_time_to_utc(self) -> None:
        source = event()
        source["observed_at"] = "2026-07-21T00:30:00+08:00"

        parsed = CrawlerObservationRecorded.from_mapping(source)

        self.assertEqual(
            parsed.observed_at,
            datetime(2026, 7, 20, 16, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(parsed.as_pending_payload()["observed_at"], "2026-07-20T16:30:00Z")

    def test_rejects_payload_hash_tampering(self) -> None:
        source = event()
        source["payload"]["total_view_count"] = 2001
        with self.assertRaisesRegex(EventValidationError, "payload_hash"):
            CrawlerObservationRecorded.from_mapping(source)

    def test_rejects_extra_about_fields(self) -> None:
        source = event()
        source["payload"]["description"] = "must not cross the event seam"
        body = json.dumps(source["payload"], separators=(",", ":"))
        source["payload_hash"] = f"sha256:{sha256(body.encode()).hexdigest()}"
        with self.assertRaisesRegex(EventValidationError, "unexpected"):
            CrawlerObservationRecorded.from_mapping(source)

    def test_rejects_string_to_integer_coercion(self) -> None:
        source = event()
        source["kind_sequence"] = "1"

        with self.assertRaisesRegex(EventValidationError, "kind_sequence.*integer"):
            CrawlerObservationRecorded.from_mapping(source)

    def test_failed_event_cannot_hide_a_resolved_metric(self) -> None:
        source = deepcopy(event(outcome="failed"))
        with self.assertRaisesRegex(EventValidationError, "failed About"):
            CrawlerObservationRecorded.from_mapping(source)

    def test_failed_about_can_report_an_explicit_removed_channel(self) -> None:
        payload = {
            "failure_kind": "channel_removed",
            "attempt_count": 1,
            "removed_reason": "channel_not_found",
        }
        source = event(payload, outcome="failed")
        source["payload_hash"] = canonical_payload_hash(payload)

        parsed = CrawlerObservationRecorded.from_mapping(source)

        self.assertIsInstance(parsed.payload, FailedDomainPayload)
        self.assertEqual(parsed.payload.failure_kind, "channel_removed")
        self.assertEqual(parsed.payload.removed_reason, "channel_not_found")


if __name__ == "__main__":
    unittest.main()
