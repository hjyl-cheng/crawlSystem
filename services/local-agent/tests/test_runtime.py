import json
import unittest
from io import StringIO
from unittest.mock import patch

from qy_channel_profile.cli import main
from qy_channel_profile.contracts import AnalysisPolicy
from qy_channel_profile.processor import ChannelProfileProcessor
from qy_channel_profile.runtime import analyze_runtime_request, snapshot_from_runtime

from tests.test_processor import snapshot_value


class RuntimeReplacementTest(unittest.TestCase):
    def test_runtime_batch_emits_agent_payloads(self):
        processor = ChannelProfileProcessor()
        report = analyze_runtime_request(
            {
                "policy": AnalysisPolicy.COMPLETE_ESTIMATE.value,
                "envelopes": [{
                    "channel_id": "UC1234567890123456789012",
                    "input_url": "https://www.youtube.com/@example",
                    "snapshot": snapshot_value(),
                }],
            },
            processor,
        )
        self.assertEqual(report["status"], "ok")
        self.assertEqual(len(report["results"]), 1)
        self.assertEqual(len(report["errors"]), 0)
        payload = report["results"][0]["payload"]
        self.assertEqual(payload["input_url"], "https://www.youtube.com/@example")
        self.assertEqual(payload["creator_gender"], "female")
        self.assertEqual(payload["creator_age_range"], 31)
        self.assertEqual(payload["channel_categories"]["level_1"], "Food")
        self.assertEqual(len(report["results"][0]["input_content_ids"]), 5)

    def test_invalid_comment_page_is_dropped_instead_of_failing_the_channel(self):
        snapshot = snapshot_value()
        snapshot["contents"][1]["comments_first_page"] = {
            "version": 1,
            "sort": "TOP_COMMENTS",
            "collected_at": "2026-08-08T12:00:00Z",
            "returned_count": 1,
            "comments": [{"comment_id": "c2", "text": "valid", "position": 1}],
        }
        snapshot["contents"][0]["comments_first_page"] = {
            "version": 1,
            "sort": "NEWEST",
            "collected_at": "2026-08-08T12:00:00Z",
            "returned_count": 1,
            "comments": [{"comment_id": "c1", "text": "oi", "position": 1}],
        }
        parsed = snapshot_from_runtime(snapshot)
        self.assertIsNone(parsed.contents[0].comments_first_page)
        self.assertEqual(parsed.contents[1].comments_first_page.comments[0].comment_id, "c2")
        self.assertEqual(
            parsed.provenance["invalid_comment_pages"],
            [{
                "content_id": "video-0",
                "error": "comments_first_page.sort must be TOP_COMMENTS",
            }],
        )

    def test_one_bad_envelope_does_not_abort_the_batch(self):
        processor = ChannelProfileProcessor()
        report = analyze_runtime_request(
            {
                "envelopes": [
                    {"channel_id": "bad", "snapshot": {"channel": {}}},
                    {
                        "channel_id": "UC1234567890123456789012",
                        "input_url": "https://www.youtube.com/@example",
                        "snapshot": snapshot_value(),
                    },
                ],
            },
            processor,
        )
        self.assertEqual(len(report["results"]), 1)
        self.assertEqual(len(report["errors"]), 1)
        self.assertEqual(report["errors"][0]["channel_id"], "bad")

    def test_cli_reads_stdin_and_writes_runtime_json(self):
        request = {
            "envelopes": [{
                "channel_id": "UC1234567890123456789012",
                "input_url": "https://www.youtube.com/@example",
                "snapshot": snapshot_value(),
            }],
        }
        stdout = StringIO()
        with patch("sys.stdin", StringIO(json.dumps(request))), patch("sys.stdout", stdout):
            code = main(["analyze-runtime"])
        self.assertEqual(code, 0)
        report = json.loads(stdout.getvalue())
        self.assertEqual(report["results"][0]["payload"]["creator_gender"], "female")
