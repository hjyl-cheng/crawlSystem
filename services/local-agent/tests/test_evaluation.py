import csv
import tempfile
import unittest
from pathlib import Path

from qy_channel_profile.evaluation import (
    build_agreement_report,
    build_detailed_comparison,
    write_detailed_comparison_outputs,
)


class EvaluationTest(unittest.TestCase):
    def test_report_calls_comparison_agreement_not_accuracy(self):
        row = {
            "result": {
                "channel_id": "UC1",
                "snapshot": {"quality": "approximate_as_of"},
                "facts": {
                    "country": {"value": "Brazil"},
                    "creator_gender": {"value": "female"},
                    "creator_age_range": {"value": 30},
                    "creator_language": {"value": "Portuguese"},
                    "audience_region": {"value": [{"region": "Brazil", "percentage": 100}]},
                    "audience_age_gender": {"value": [{"age_range": "18-24", "male": 50, "female": 50}]},
                    "audience_language": {"value": [{"language": "Portuguese", "percentage": 100}]},
                    "active_subscriber_ratio": {"value": 30},
                    "channel_tags": {"value": {"tags": ["Food"]}},
                    "channel_categories": {"value": {"level_1": "Food", "level_2": ["Cooking Tutorials"]}},
                },
            },
            "agent_reference": {
                "metrics_json": {
                    "audience_profile_agent": {
                        "country": {"value": "Brazil"},
                        "creator_gender": {"value": "female"},
                        "creator_age_range": {"value": 32},
                        "creator_language": {"value": "Portuguese"},
                        "audience_region": {"value": [{"region": "Brazil", "percentage": 100}]},
                        "audience_age_gender": {"value": [{"age_range": "18-24", "male": 40, "female": 60}]},
                        "audience_language": {"value": [{"language": "Portuguese", "percentage": 100}]},
                        "active_subscriber_ratio": {"value": 35},
                        "channel_tags": {"value": {"tags": ["Food", "Cooking"]}},
                        "channel_categories": {"value": {"level_1": "Food", "level_2": ["Cooking Tutorials"]}},
                    }
                }
            },
        }
        report = build_agreement_report([row])
        self.assertEqual(report["report_type"], "frozen_agent_agreement_not_accuracy")
        self.assertEqual(report["fields"]["country"]["agreement"], 1.0)
        self.assertEqual(report["fields"]["creator_age_range"]["mean_absolute_difference"], 2.0)

    def test_free_tags_also_get_a_controlled_vocabulary_comparison(self):
        row = {
            "result": {
                "channel_id": "UC1",
                "facts": {
                    "country": {"value": None},
                    "creator_gender": {"value": None},
                    "creator_age_range": {"value": None},
                    "creator_language": {"value": None},
                    "audience_region": {"value": None},
                    "audience_age_gender": {"value": None},
                    "audience_language": {"value": None},
                    "active_subscriber_ratio": {"value": None},
                    "channel_tags": {"value": {"tags": ["Football"]}},
                    "channel_categories": {"value": None},
                },
            },
            "agent_reference": {
                "channel_tags": {"tags": ["Football Curiosities"]},
            },
        }
        report = build_agreement_report([row])
        self.assertEqual(report["fields"]["channel_tags"]["mean_jaccard"], 0.0)
        self.assertEqual(
            report["fields"]["channel_tags.controlled_vocabulary"]["mean_jaccard"],
            0.75,
        )

    def test_detailed_report_writes_ten_field_rows_and_tables(self):
        row = {
            "result": {
                "channel_id": "UC1",
                "input_url": "https://www.youtube.com/@one",
                "snapshot": {"channel_title": "Channel One"},
                "facts": {
                    "country": {"value": "Brazil", "source_type": "observed", "evidence_strength": "explicit", "evidence_confidence": 1.0},
                    "creator_gender": {"value": "female"},
                    "creator_age_range": {"value": 30},
                    "creator_language": {"value": "Portuguese"},
                    "audience_region": {"value": [{"region": "Brazil", "percentage": 100}]},
                    "audience_age_gender": {"value": [{"age_range": "18-24", "male": 50, "female": 50}]},
                    "audience_language": {"value": [{"language": "Portuguese", "percentage": 100}]},
                    "active_subscriber_ratio": {"value": 30},
                    "channel_tags": {"value": {"tags": ["Food"]}},
                    "channel_categories": {"value": {"level_1": "Food", "level_2": ["Cooking Tutorials"]}},
                },
            },
            "agent_reference": {
                field: value
                for field, value in {
                    "country": "Brazil",
                    "creator_gender": "female",
                    "creator_age_range": 31,
                    "creator_language": "Portuguese",
                    "audience_region": [{"region": "Brazil", "percentage": 100}],
                    "audience_age_gender": [{"age_range": "18-24", "male": 40, "female": 60}],
                    "audience_language": [{"language": "Portuguese", "percentage": 100}],
                    "active_subscriber_ratio": 32,
                    "channel_tags": {"tags": ["Food"]},
                    "channel_categories": {"level_1": "Food", "level_2": ["Cooking Tutorials"]},
                }.items()
            },
        }
        report = build_detailed_comparison([row])
        self.assertEqual(report["channels"], 1)
        self.assertEqual(len(report["details"]), 10)
        with tempfile.TemporaryDirectory() as directory:
            markdown = Path(directory) / "report.md"
            csv_path = Path(directory) / "report.csv"
            write_detailed_comparison_outputs(
                report,
                markdown_path=markdown,
                csv_path=csv_path,
                source_name="fixture.jsonl",
            )
            rendered = markdown.read_text(encoding="utf-8")
            self.assertIn("# QY 频道画像：1 个频道本地模块与历史 Agent 对比", rendered)
            self.assertIn("## country", rendered)
            with csv_path.open(encoding="utf-8") as handle:
                self.assertEqual(len(list(csv.DictReader(handle))), 10)


if __name__ == "__main__":
    unittest.main()
