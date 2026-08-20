from __future__ import annotations

from hashlib import sha256
import importlib.util
import inspect
import json
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4

SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "ops"
    / "feature-migration"
    / "replay_channel_feature_state.py"
)
SPEC = importlib.util.spec_from_file_location("replay_channel_feature_state", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
load_events = MODULE.load_events


def about_event(*, channel_id: str, sequence: int, observed_at: str) -> dict:
    payload = {
        "subscriber_count": 1000,
        "subscriber_count_status": "exact",
        "total_view_count": 2000,
        "total_view_count_status": "exact",
        "total_video_count": 30,
        "total_video_count_status": "exact",
    }
    body = json.dumps(payload, separators=(",", ":"))
    return {
        "event_id": str(uuid4()),
        "event_type": "crawler.observation.recorded",
        "event_version": 1,
        "observation_id": str(uuid4()),
        "channel_id": channel_id,
        "observation_kind": "about",
        "kind_sequence": sequence,
        "observed_at": observed_at,
        "outcome": "complete",
        "crawler_version": "qy-v16",
        "payload_hash": f"sha256:{sha256(body.encode()).hexdigest()}",
        "payload": payload,
    }


class ReplayChannelFeatureStateTests(unittest.TestCase):
    def test_replay_uses_only_the_three_active_domains(self) -> None:
        self.assertEqual(
            MODULE.OBSERVATION_KIND_ORDER,
            {"about": 1, "video": 2, "agent": 3},
        )
        self.assertNotIn("last_profile_observed_at", inspect.getsource(MODULE.snapshot))

    def test_loads_and_orders_jsonl_without_crawler_database_access(self) -> None:
        events = [
            about_event(
                channel_id="UCexample",
                sequence=2,
                observed_at="2026-07-21T12:00:00Z",
            ),
            about_event(
                channel_id="UCexample",
                sequence=1,
                observed_at="2026-07-20T12:00:00Z",
            ),
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.write_text(
                "\n".join(json.dumps(event) for event in events),
                encoding="utf-8",
            )

            parsed = load_events("UCexample", jsonl_path=str(path))

        self.assertEqual([event.kind_sequence for event in parsed], [1, 2])

    def test_rejects_an_event_for_another_channel(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.write_text(
                json.dumps(
                    about_event(
                        channel_id="UCother",
                        sequence=1,
                        observed_at="2026-07-20T12:00:00Z",
                    )
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "belongs to another Channel"):
                load_events("UCexample", jsonl_path=str(path))

    def test_rejects_invalid_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.write_text("not-json\n", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "line 1"):
                load_events("UCexample", jsonl_path=str(path))


if __name__ == "__main__":
    unittest.main()
