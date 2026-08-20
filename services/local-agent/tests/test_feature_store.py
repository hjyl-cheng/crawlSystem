import json
import tempfile
import unittest
from pathlib import Path

from qy_channel_profile.feature_store import build_feature_store
from qy_channel_profile.io import write_jsonl


class FeatureStoreTest(unittest.TestCase):
    def test_parquet_store_excludes_agent_reference_columns(self):
        model_path = Path("artifacts/models/external/lid.176.ftz")
        if not model_path.is_file():
            self.skipTest("local FastText artifact is not installed")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            input_path = root / "snapshot.jsonl"
            output_path = root / "features.parquet"
            write_jsonl(input_path, [{
                "snapshot": {
                    "channel": {
                        "channel_id": "UCfeaturestore000000001",
                        "title": "Canal de Cocina",
                        "about_description": "Soy de México y comparto recetas para toda la familia.",
                        "country": "Mexico",
                        "subscriber_count": 1000,
                        "total_view_count": 5000,
                        "total_video_count": 2,
                    },
                    "contents": [{
                        "source_content_id": f"v{index}",
                        "content_type": "video",
                        "title": f"Receta fácil {index}",
                        "description": "Una receta de cocina rápida",
                        "published_at": f"2026-08-0{index + 1}T00:00:00Z",
                        "first_seen_at": f"2026-08-0{index + 1}T01:00:00Z",
                        "view_count": 500,
                    } for index in range(5)],
                    "as_of": "2026-08-09T00:00:00Z",
                },
                "agent_reference": {"must_not_leak": True},
            }])
            manifest = build_feature_store(
                input_path,
                output_path,
                language_model_path=model_path,
            )
            import polars as pl
            frame = pl.read_parquet(output_path)
            self.assertEqual(manifest.row_count, 1)
            self.assertEqual(manifest.lineage_status, "legacy_snapshot_partial")
            self.assertEqual(manifest.source_rows_with_lineage_version, 0)
            self.assertFalse(manifest.agent_reference_columns_present)
            self.assertFalse(any("agent" in name.casefold() for name in frame.columns))
            self.assertEqual(frame["country_label"][0], "Mexico")
            tag_labels = json.loads(frame["tag_labels_json"][0])
            self.assertIn("Cooking Tutorials", tag_labels)
            self.assertNotIn("Storytelling", tag_labels)


if __name__ == "__main__":
    unittest.main()
