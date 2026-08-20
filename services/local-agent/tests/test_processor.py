import copy
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from qy_channel_profile.contracts import (
    AnalysisPolicy,
    ChannelSnapshot,
    ProfileAnalysisRequest,
)
from qy_channel_profile.errors import SnapshotError
from qy_channel_profile.processor import ChannelProfileProcessor
from qy_channel_profile.processor import PROCESSOR_VERSION
from qy_channel_profile.creator_evidence import extract_creator_evidence
from qy_channel_profile.agent_contract import to_agent_payload


def snapshot_value():
    return {
        "channel": {
            "channel_id": "UC1234567890123456789012",
            "channel_url": "https://www.youtube.com/@example",
            "title": "Maria Cozinha",
            "handle": "@mariacozinha",
            "country": "Brazil",
            "country_canonical_name": "Brazil",
            "about_description": "Sou uma mulher de 31 anos. Receitas simples e comida brasileira.",
            "keywords": ["receitas", "cozinha", "comida"],
            "subscriber_count": 100000,
            "total_view_count": 5000000,
            "total_video_count": 300,
            "external_links": [],
        },
        "contents": [
            {
                "source_content_id": f"video-{index}",
                "content_type": "video",
                "title": f"Receita fácil {index}",
                "description": "Hoje vamos cozinhar uma receita deliciosa.",
                "keywords": ["receita", "cozinha"],
                "hashtags": ["receitas"],
                "published_at": f"2026-08-{8-index:02d}T12:00:00Z",
                "first_seen_at": f"2026-08-{8-index:02d}T13:00:00Z",
                "view_count": 20000 + index * 1000,
                "view_count_status": "exact",
                "like_count": 1000,
                "comment_count": 100,
            }
            for index in range(5)
        ],
        "as_of": "2026-08-09T00:00:00Z",
        "replay_quality": "current_exact",
    }


class ProcessorTest(unittest.TestCase):
    def setUp(self):
        self.processor = ChannelProfileProcessor()
        self.snapshot = ChannelSnapshot.from_mapping(snapshot_value())

    def analyze(self, url="raw input url", policy=AnalysisPolicy.COMPLETE_ESTIMATE):
        return self.processor.analyze(
            ProfileAnalysisRequest(
                channel_id=self.snapshot.channel_id,
                input_url=url,
                as_of=datetime(2026, 8, 9, tzinfo=timezone.utc),
                policy=policy,
            ),
            self.snapshot,
        )

    def test_complete_contract_and_distribution_totals(self):
        result = self.analyze()
        payload = result.to_dict()
        self.assertEqual(len(payload["facts"]), 10)
        self.assertEqual(payload["facts"]["country"]["value"], "Brazil")
        self.assertEqual(payload["facts"]["creator_gender"]["value"], "female")
        self.assertEqual(payload["facts"]["creator_age_range"]["value"], 31)
        self.assertEqual(payload["facts"]["creator_language"]["value"], "Portuguese")
        self.assertEqual(payload["facts"]["channel_categories"]["value"]["level_1"], "Food")
        audience_languages = {
            row["language"] for row in payload["facts"]["audience_language"]["value"]
        }
        self.assertNotIn("Turkish", audience_languages)
        self.assertEqual(sum(row["percentage"] for row in payload["facts"]["audience_region"]["value"]), 100)
        self.assertEqual(
            sum(row[gender] for row in payload["facts"]["audience_age_gender"]["value"] for gender in ("male", "female")),
            100,
        )
        self.assertEqual(
            sum(row["percentage"] for row in payload["facts"]["channel_tags"]["value"]["top_5_distribution"]),
            100,
        )

    def test_input_url_is_correlation_only(self):
        left = self.analyze("first input").to_dict()
        right = self.analyze("second input").to_dict()
        self.assertNotEqual(left["input_url"], right["input_url"])
        self.assertEqual(left["facts"], right["facts"])
        self.assertEqual(left["snapshot"], right["snapshot"])

    def test_deterministic(self):
        self.assertEqual(self.analyze().to_dict(), self.analyze().to_dict())

    def test_creator_evidence_is_extracted_once_per_analysis(self):
        with patch(
            "qy_channel_profile.features.extract_creator_evidence",
            wraps=extract_creator_evidence,
        ) as extractor:
            result = self.analyze()

        self.assertEqual(extractor.call_count, 1)
        self.assertEqual(result.processor["version"], PROCESSOR_VERSION)

    def test_evidence_first_abstains_on_unvalidated_audience_prior(self):
        payload = self.analyze(policy=AnalysisPolicy.EVIDENCE_FIRST).to_dict()
        self.assertIsNone(payload["facts"]["audience_region"]["value"])
        self.assertIsNone(payload["facts"]["audience_age_gender"]["value"])
        self.assertIsNone(payload["facts"]["audience_language"]["value"])
        self.assertIsNone(payload["facts"]["active_subscriber_ratio"]["value"])
        self.assertEqual(payload["analysis_status"], "partial_failure")

    def test_string_policy_is_coerced_at_the_interface(self):
        request = ProfileAnalysisRequest(
            channel_id=self.snapshot.channel_id,
            input_url="input",
            as_of=self.snapshot.as_of,
            policy="evidence_first",
        )
        payload = self.processor.analyze(request, self.snapshot).to_dict()
        self.assertEqual(request.policy, AnalysisPolicy.EVIDENCE_FIRST)
        self.assertIsNone(payload["facts"]["audience_region"]["value"])

    def test_snapshot_hash_changes_with_content_stats(self):
        changed = copy.deepcopy(snapshot_value())
        changed["contents"][0]["view_count"] += 1
        other = ChannelSnapshot.from_mapping(changed)
        self.assertNotEqual(self.snapshot.hashes()["content_stats_hash"], other.hashes()["content_stats_hash"])
        self.assertEqual(self.snapshot.hashes()["content_text_hash"], other.hashes()["content_text_hash"])

    def test_snapshot_hash_ignores_content_row_order(self):
        changed = copy.deepcopy(snapshot_value())
        changed["contents"].reverse()
        other = ChannelSnapshot.from_mapping(changed)
        self.assertEqual(self.snapshot.hashes(), other.hashes())

    def test_snapshot_rejects_future_content(self):
        changed = copy.deepcopy(snapshot_value())
        changed["contents"][0]["published_at"] = "2026-08-10T00:00:00Z"
        with self.assertRaises(SnapshotError):
            ChannelSnapshot.from_mapping(changed)

    def test_active_ratio_ignores_repeated_upload_placeholder_rows(self):
        changed = copy.deepcopy(snapshot_value())
        for content in changed["contents"][:3]:
            content["title"] = "Uploads from Maria Cozinha"
            content["view_count"] = 50_000_000
        snapshot = ChannelSnapshot.from_mapping(changed)
        result = self.processor.analyze(
            ProfileAnalysisRequest(
                channel_id=snapshot.channel_id,
                input_url="input",
                as_of=snapshot.as_of,
                policy=AnalysisPolicy.COMPLETE_ESTIMATE,
            ),
            snapshot,
        ).to_dict()
        active = result["facts"]["active_subscriber_ratio"]
        self.assertLess(active["value"], 50)
        self.assertEqual(active["metadata"]["excluded_placeholder_count"], 3)

    def test_complete_estimate_fills_sparse_hidden_statistics_channel(self):
        changed = copy.deepcopy(snapshot_value())
        changed["channel"]["subscriber_count"] = None
        changed["channel"]["total_view_count"] = None
        changed["channel"]["total_video_count"] = 0
        changed["channel"]["about_description"] = ""
        changed["channel"]["summary"] = ""
        changed["channel"]["keywords"] = []
        changed["contents"] = []
        snapshot = ChannelSnapshot.from_mapping(changed)
        result = self.processor.analyze(
            ProfileAnalysisRequest(
                channel_id=snapshot.channel_id,
                input_url="sparse input",
                as_of=snapshot.as_of,
                policy=AnalysisPolicy.COMPLETE_ESTIMATE,
            ),
            snapshot,
        )
        payload = to_agent_payload(result)
        self.assertEqual(result.analysis_status, "completed_with_estimates")
        self.assertEqual(payload["creator_language"], "Portuguese")
        self.assertIsInstance(payload["active_subscriber_ratio"], int)
        self.assertTrue(result.facts["active_subscriber_ratio"].metadata["subscriber_count_estimated"])


if __name__ == "__main__":
    unittest.main()
