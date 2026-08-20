import unittest
from copy import deepcopy
from datetime import datetime, timezone

from qy_channel_profile.contracts import ChannelSnapshot
from qy_channel_profile.errors import SnapshotError
from qy_channel_profile.features import NUMERIC_FEATURE_NAMES, build_channel_features


class FeatureSnapshotTest(unittest.TestCase):
    def test_shared_features_are_deterministic_and_keep_missing_indicators(self):
        value = {
            "channel": {
                "channel_id": "UCfeature0000000000000001",
                "title": "Cocina de Maria",
                "about_description": "Recetas sencillas para toda la familia",
                "subscriber_count": 1000,
                "total_view_count": 50000,
                "total_video_count": 10,
            },
            "contents": [
                {
                    "source_content_id": "v1",
                    "content_type": "video",
                    "title": "Receta de arroz",
                    "description": "Hoy preparamos una receta deliciosa",
                    "published_at": "2026-08-01T00:00:00Z",
                    "first_seen_at": "2026-08-01T01:00:00Z",
                    "view_count": 1000,
                    "like_count": 100,
                    "comment_count": 10,
                },
                {
                    "source_content_id": "v2",
                    "content_type": "short",
                    "title": "Cocina rápida",
                    "published_at": "2026-07-25T00:00:00Z",
                    "first_seen_at": "2026-07-25T01:00:00Z",
                    "view_count": 500,
                },
            ],
            "as_of": "2026-08-09T00:00:00Z",
        }
        snapshot = ChannelSnapshot.from_mapping(value)
        first = build_channel_features(snapshot)
        second = build_channel_features(snapshot)
        self.assertEqual(first.to_record(), second.to_record())
        self.assertEqual(tuple(first.numeric), NUMERIC_FEATURE_NAMES)
        self.assertEqual(first.numeric["short_ratio"], 0.5)
        self.assertEqual(first.numeric["recent_30_count"], 2.0)
        self.assertEqual(first.model_text("creator_gender"), first.channel_text)
        self.assertNotIn("receta de arroz", first.model_text("creator_gender"))
        self.assertIn("joined_at", first.missing)
        self.assertEqual(first.as_of, datetime(2026, 8, 9, tzinfo=timezone.utc))

    def test_explicit_content_type_is_never_overridden_by_duration(self):
        snapshot = ChannelSnapshot.from_mapping({
            "channel": {"channel_id": "UCfeature0000000000000002"},
            "contents": [
                {
                    "source_content_id": "brief-video",
                    "content_type": "video",
                    "duration_seconds": 30,
                },
                {
                    "source_content_id": "long-short",
                    "content_type": "short",
                    "duration_seconds": 180,
                },
                {
                    "source_content_id": "brief-live",
                    "content_type": "live",
                    "duration_seconds": 15,
                },
            ],
            "as_of": "2026-08-09T00:00:00Z",
        })

        features = build_channel_features(snapshot)

        self.assertEqual(features.numeric["short_ratio"], 1 / 3)
        self.assertEqual(features.numeric["longform_ratio"], 1 / 3)
        self.assertEqual(features.numeric["live_ratio"], 1 / 3)

    def test_lineage_changes_are_audited_without_changing_model_features(self):
        value = {
            "channel": {"channel_id": "UCfeature0000000000000004"},
            "contents": [{
                "source_content_id": "v1",
                "content_type": "video",
                "title": "Stable title",
                "description": "Stable description",
                "view_count": 100,
                "description_source": "youtubejs_player",
                "view_count_source": "youtubejs_player",
                "extractor_version": "youtubei.js@1",
            }],
            "as_of": "2026-08-09T00:00:00Z",
            "provenance": {"data_lineage_version": "snapshot-field-lineage-v1"},
        }
        changed = deepcopy(value)
        changed["contents"][0]["description_source"] = "youtube_data_api_snippet"
        changed["contents"][0]["view_count_source"] = "youtube_data_api_statistics"
        changed["contents"][0]["extractor_version"] = "data-api-v1"

        original_snapshot = ChannelSnapshot.from_mapping(value)
        changed_snapshot = ChannelSnapshot.from_mapping(changed)
        original = build_channel_features(original_snapshot)
        updated = build_channel_features(changed_snapshot)

        self.assertEqual(original.full_text, updated.full_text)
        self.assertEqual(original.numeric, updated.numeric)
        self.assertNotEqual(original.hashes["lineage_hash"], updated.hashes["lineage_hash"])
        self.assertNotEqual(original.hashes["snapshot_hash"], updated.hashes["snapshot_hash"])
        for name in (
            "profile_text_hash", "content_text_hash", "channel_stats_hash",
            "content_stats_hash", "comment_text_hash", "comment_stats_hash",
        ):
            self.assertEqual(original.hashes[name], updated.hashes[name])

    def test_content_type_is_required_and_must_be_supported(self):
        base = {
            "channel": {"channel_id": "UCfeature0000000000000003"},
            "contents": [{"source_content_id": "missing-type"}],
            "as_of": "2026-08-09T00:00:00Z",
        }
        with self.assertRaisesRegex(SnapshotError, "content_type is required"):
            ChannelSnapshot.from_mapping(base)

        base["contents"][0]["content_type"] = "clip"
        with self.assertRaisesRegex(SnapshotError, "unsupported content_type"):
            ChannelSnapshot.from_mapping(base)


if __name__ == "__main__":
    unittest.main()
