from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import unittest

from feature_engine.events import CrawlerObservationRecorded


QY_CRAWLER_ROOT = os.environ.get("QY_CRAWLER_ROOT")


@unittest.skipUnless(
    QY_CRAWLER_ROOT,
    "set QY_CRAWLER_ROOT to run the cross-repository Crawler contract gate",
)
class CrawlerProducerContractTests(unittest.TestCase):
    def test_real_crawler_video_samples_are_accepted_by_feature(self) -> None:
        crawler_root = Path(QY_CRAWLER_ROOT).resolve()
        emitter = crawler_root / "scripts" / "emitFeatureContractSamples.mjs"
        completed = subprocess.run(
            ["node", str(emitter)],
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

        candidate = samples[0]["payload"]["discovery"]
        self.assertEqual(candidate["outcome"], "complete")
        self.assertEqual(candidate["payload"]["stop_reason"], "candidate_limit_processed")

        overlapping = samples[1]["payload"]["discovery"]
        self.assertEqual(overlapping["outcome"], "partial")
        self.assertEqual(overlapping["payload"]["detail_success_count"], 30)
        self.assertEqual(overlapping["payload"]["detail_failure_count"], 23)
