from __future__ import annotations

from datetime import date, datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from uuid import uuid4

from feature_engine.bootstrap import (
    BASELINE_BUNDLE_FORMAT,
    BaselineBundleValidationError,
    FeatureBootstrapper,
    load_baseline_bundle,
    stable_bootstrap_offset,
    stable_bootstrap_shard,
)


def about_event(channel_id: str, sequence: int = 1) -> dict:
    payload = {
        "subscriber_count": 100,
        "subscriber_count_status": "exact",
        "total_view_count": 1000,
        "total_view_count_status": "exact",
        "total_video_count": 10,
        "total_video_count_status": "exact",
    }
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return {
        "event_id": str(uuid4()),
        "event_type": "crawler.observation.recorded",
        "event_version": 1,
        "observation_id": str(uuid4()),
        "channel_id": channel_id,
        "observation_kind": "about",
        "kind_sequence": sequence,
        "observed_at": "2026-07-20T12:00:00Z",
        "outcome": "complete",
        "crawler_version": "qy-v16-test",
        "payload_hash": f"sha256:{sha256(body.encode()).hexdigest()}",
        "payload": payload,
    }


def write_bundle(directory: Path, events: list[dict], **manifest_overrides) -> Path:
    events.sort(
        key=lambda event: (
            event["channel_id"],
            event["observation_kind"],
            event["kind_sequence"],
        )
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
        "baseline_version": "baseline-2026-07-20-1",
        "source_database": "bullmq_crawler_migration",
        "source_schema": "crawler",
        "source_snapshot_id": "snapshot-1",
        "exported_at": "2026-07-20T13:00:00Z",
        "events_file": "events.ndjson",
        "event_count": len(events),
        "channel_count": len({event["channel_id"] for event in events}),
        "byte_count": len(event_bytes),
        "events_sha256": f"sha256:{sha256(event_bytes).hexdigest()}",
    }
    manifest.update(manifest_overrides)
    manifest_path = directory / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path


class BaselineBundleTests(unittest.TestCase):
    def test_bootstrap_spread_contains_only_the_three_active_clocks(self) -> None:
        exported_at = datetime(2026, 7, 20, 13, tzinfo=timezone.utc)
        bundle = SimpleNamespace(
            manifest=SimpleNamespace(
                exported_at=exported_at,
                baseline_version="baseline-fixed-profile",
            )
        )
        policy = SimpleNamespace(
            about_config=SimpleNamespace(baseline_interval_days=7),
            discovery_config=SimpleNamespace(fallback_interval_days=7),
            agent_config=SimpleNamespace(bootstrap_min_days=60, bootstrap_max_days=90),
        )
        row = {
            "about_due_at": datetime(2026, 7, 27, tzinfo=timezone.utc),
            "about_due_day": date(2026, 7, 27),
            "about_tier": 7,
            "last_about_observed_at": None,
            "video_due_at": datetime(2026, 7, 27, tzinfo=timezone.utc),
            "video_due_day": date(2026, 7, 27),
            "video_tier": 7,
            "last_discovery_observed_at": None,
            "agent_due_at": datetime(2026, 10, 18, tzinfo=timezone.utc),
            "agent_due_day": date(2026, 10, 18),
            "agent_tier": 90,
            "last_agent_observed_at": None,
        }

        schedule = FeatureBootstrapper._spread_schedule(
            bundle,
            channel_id="UC-fixed-profile",
            row=row,
            policy=policy,
        )

        self.assertNotIn("profile", schedule)
        self.assertEqual(set(schedule), {"about", "video", "agent"})

    def test_validates_complete_bundle_and_sequence_watermarks(self) -> None:
        with TemporaryDirectory() as temporary:
            manifest_path = write_bundle(
                Path(temporary),
                [
                    about_event("UC-bootstrap-2"),
                    about_event("UC-bootstrap-1", 2),
                    about_event("UC-bootstrap-1", 1),
                ],
            )

            bundle = load_baseline_bundle(manifest_path)

        self.assertEqual(bundle.manifest.channel_count, 2)
        self.assertEqual(len(bundle.events), 3)
        self.assertEqual(
            bundle.checkpoint_sequences,
            (
                ("UC-bootstrap-1", "about", 2),
                ("UC-bootstrap-2", "about", 1),
            ),
        )
        self.assertEqual(bundle.manifest.exported_at.tzinfo, timezone.utc)

    def test_rejects_content_hash_before_returning_a_bundle(self) -> None:
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            manifest_path = write_bundle(directory, [about_event("UC-bootstrap")])
            (directory / "events.ndjson").write_text("tampered\n", encoding="utf-8")

            with self.assertRaisesRegex(BaselineBundleValidationError, "byte_count|SHA-256"):
                load_baseline_bundle(manifest_path)

    def test_rejects_a_sequence_gap_even_when_manifest_hashes_match(self) -> None:
        with TemporaryDirectory() as temporary:
            manifest_path = write_bundle(Path(temporary), [about_event("UC-bootstrap", 2)])

            with self.assertRaisesRegex(BaselineBundleValidationError, "contiguous from 1"):
                load_baseline_bundle(manifest_path)

    def test_rejects_a_channel_with_only_failed_observations(self) -> None:
        event = about_event("UC-bootstrap")
        event["outcome"] = "failed"
        event["payload"] = {
            "subscriber_count": None,
            "subscriber_count_status": "unavailable",
            "total_view_count": None,
            "total_view_count_status": "unavailable",
            "total_video_count": None,
            "total_video_count_status": "unavailable",
        }
        body = json.dumps(event["payload"], ensure_ascii=False, separators=(",", ":"))
        event["payload_hash"] = f"sha256:{sha256(body.encode()).hexdigest()}"
        with TemporaryDirectory() as temporary:
            manifest_path = write_bundle(Path(temporary), [event])

            with self.assertRaisesRegex(BaselineBundleValidationError, "no non-failed"):
                load_baseline_bundle(manifest_path)

    def test_rejects_manifest_path_escape(self) -> None:
        with TemporaryDirectory() as temporary:
            manifest_path = write_bundle(
                Path(temporary),
                [about_event("UC-bootstrap")],
                events_file="../events.ndjson",
            )

            with self.assertRaisesRegex(BaselineBundleValidationError, "file name"):
                load_baseline_bundle(manifest_path)

    def test_stable_hash_helpers_are_bounded_and_repeatable(self) -> None:
        self.assertEqual(
            stable_bootstrap_shard("UC-bootstrap", 16),
            stable_bootstrap_shard("UC-bootstrap", 16),
        )
        first = stable_bootstrap_offset(
            "UC-bootstrap", "agent", "baseline-1", 30, 90
        )
        second = stable_bootstrap_offset(
            "UC-bootstrap", "agent", "baseline-1", 30, 90
        )
        self.assertEqual(first, second)
        self.assertGreaterEqual(first, 30)
        self.assertLessEqual(first, 90)


if __name__ == "__main__":
    unittest.main()
