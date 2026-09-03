from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import unittest

from feature_engine.events import CrawlerObservationRecorded


class CrawlerProducerContractTests(unittest.TestCase):
    def test_real_crawler_video_samples_are_accepted_by_feature(self) -> None:
        repository_root = Path(__file__).resolve().parents[3]
        crawler_root = repository_root / "services" / "qybullmq"
        emitter = crawler_root / "scripts" / "emitFeatureContractSamples.mjs"
        node_binary = os.environ.get("NODE_BINARY", "node")
        completed = subprocess.run(
            [node_binary, str(emitter)],
            cwd=crawler_root,
            check=True,
            capture_output=True,
            text=True,
        )
        samples = json.loads(completed.stdout)

        self.assertEqual(len(samples), 2)
        for source in samples:
            with self.subTest(sequence=source["kind_sequence"]):
                parsed = CrawlerObservationRecorded.from_mapping(source)
                self.assertEqual(parsed.as_pending_payload()["payload"], source["payload"])

        self.assertIn("activity", samples[0]["payload"])
        self.assertNotIn("activity", samples[1]["payload"])
        self.assertEqual(
            samples[0]["payload"]["activity_evidence"]["policy_version"],
            "incremental-video-activity-v5",
        )
        self.assertEqual(
            samples[1]["payload"]["activity_evidence"]["evidence_scan_stop_reason"],
            "row_limit",
        )
