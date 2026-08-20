import tempfile
import unittest
from pathlib import Path

from qy_channel_profile.io import read_jsonl, write_jsonl
from qy_channel_profile.sampling import (
    build_blind_annotation_pilot,
    build_deterministic_country_coverage_sample,
    build_deterministic_holdout,
)


class SamplingTest(unittest.TestCase):
    def test_holdout_is_deterministic_agent_blind_and_source_ordered(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            rows = [
                {
                    "snapshot": {
                        "channel": {"channel_id": f"UC{index:022d}"},
                        "contents": [],
                        "as_of": "2026-08-09T00:00:00Z",
                    },
                    "agent_reference": {"country": "Mexico" if index % 2 else "Brazil"},
                }
                for index in range(50)
            ]
            write_jsonl(source, rows)
            first = root / "first.jsonl"
            second = root / "second.jsonl"
            first_manifest = build_deterministic_holdout(source, first, sample_size=10, seed="fixed")
            second_manifest = build_deterministic_holdout(source, second, sample_size=10, seed="fixed")
            first_rows = list(read_jsonl(first))
            second_rows = list(read_jsonl(second))
            self.assertEqual(first_rows, second_rows)
            self.assertEqual(first_manifest["selected_channel_ids_sha256"], second_manifest["selected_channel_ids_sha256"])
            self.assertFalse(first_manifest["selection_uses_agent_values"])
            source_positions = {
                row["snapshot"]["channel"]["channel_id"]: index for index, row in enumerate(rows)
            }
            selected_positions = [
                source_positions[row["snapshot"]["channel"]["channel_id"]] for row in first_rows
            ]
            self.assertEqual(selected_positions, sorted(selected_positions))

    def test_country_coverage_sample_is_agent_blind_and_covers_public_groups(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            countries = ("Brazil", "Mexico", "Japan", "United States")
            rows = [
                {
                    "snapshot": {
                        "channel": {
                            "channel_id": f"UC{index:022d}",
                            "country_canonical_name": countries[index % len(countries)],
                        },
                        "contents": [],
                        "as_of": "2026-08-09T00:00:00Z",
                    },
                    "agent_reference": {"country": "Agent value must stay opaque"},
                }
                for index in range(24)
            ]
            write_jsonl(source, rows)
            output = root / "coverage.jsonl"
            manifest = build_deterministic_country_coverage_sample(
                source,
                output,
                sample_size=8,
                seed="fixed-coverage",
            )
            selected = list(read_jsonl(output))
            selected_countries = {
                row["snapshot"]["channel"]["country_canonical_name"] for row in selected
            }
            self.assertEqual(selected_countries, set(countries))
            self.assertFalse(manifest["selection_uses_agent_values"])
            self.assertFalse(manifest["population_representative"])
            self.assertEqual(manifest["coverage_groups_available"], len(countries))

    def test_annotation_pilot_strips_references_and_writes_independent_templates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            rows = [{
                "input_url": "correlation-only",
                "snapshot": {
                    "channel": {
                        "channel_id": f"UC{index:022d}",
                        "title": f"Software tutorial vlog {index}",
                        "avatar_url": "https://example.invalid/channel.jpg",
                    },
                    "contents": [{
                        "source_content_id": f"v{index}",
                        "content_type": "short" if index % 2 else "video",
                        "title": "AI app tutorial",
                        "thumbnail_url": "https://example.invalid/video.jpg",
                        "comments_first_page": {
                            "comments": [{"author_avatar_url": "https://example.invalid/author.jpg"}],
                        },
                    }],
                    "as_of": "2026-08-09T00:00:00Z",
                    "provenance": {
                        "content_set_boundary": "test",
                        "historical_agent_as_of": "must be stripped",
                    },
                },
                "agent_reference": {"channel_categories": "must not leak"},
                "result": {"facts": {"channel_categories": "must not leak"}},
            } for index in range(30)]
            write_jsonl(source, rows)
            output = root / "pilot.jsonl"

            manifest = build_blind_annotation_pilot(
                source,
                output,
                sample_size=10,
                challenge_fraction=0.5,
                seed="pilot-test",
            )

            selected = list(read_jsonl(output))
            serialized = output.read_text(encoding="utf-8").casefold()
            self.assertEqual(len(selected), 10)
            self.assertNotIn("agent_reference", serialized)
            self.assertNotIn("historical_agent_as_of", serialized)
            self.assertNotIn('"result"', serialized)
            self.assertTrue(all(set(row) == {"blind_id", "snapshot"} for row in selected))
            self.assertTrue(all("avatar_url" not in row["snapshot"]["channel"] for row in selected))
            self.assertTrue(all(
                "thumbnail_url" not in content
                and "author_avatar_url" not in content["comments_first_page"]["comments"][0]
                for row in selected
                for content in row["snapshot"]["contents"]
            ))
            self.assertEqual(manifest["dataset_role"], "annotation_pilot_not_gold_test")
            self.assertEqual(manifest["lineage_status"], "legacy_snapshot_partial")
            self.assertFalse(manifest["selection_uses_agent_values"])
            template_a = list(read_jsonl(manifest["annotator_a_template"]))
            template_b = list(read_jsonl(manifest["annotator_b_template"]))
            self.assertEqual(template_a, template_b)
            self.assertEqual(
                [row["blind_id"] for row in selected],
                [row["blind_id"] for row in template_a],
            )


if __name__ == "__main__":
    unittest.main()
