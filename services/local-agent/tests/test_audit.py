import unittest

from qy_channel_profile.audit import build_snapshot_audit


class SnapshotAuditTest(unittest.TestCase):
    def test_agent_output_and_retained_input_ids_are_counted_separately(self):
        facts = {
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
        }
        row = {
            "snapshot": {
                "channel": {"channel_id": "UC1", "country": "Brazil"},
                "contents": [],
                "as_of": "2026-08-09T00:00:00Z",
                "replay_quality": "approximate_as_of",
                "provenance": {"input_content_ids_retained": 0},
            },
            "agent_reference": {
                "agent_model": "grok",
                "metrics_json": {"audience_profile_agent": facts},
            },
        }
        report = build_snapshot_audit([row])
        self.assertEqual(report["summary"]["complete_agent_profiles"], 1)
        self.assertEqual(report["agent_field_coverage"]["country"], 1)
        self.assertEqual(report["input_content_ids_retained"]["0"], 1)

    def test_field_lineage_coverage_is_reported_separately_from_values(self):
        row = {
            "snapshot": {
                "channel": {
                    "channel_id": "UC2",
                    "country": "Brazil",
                    "country_source": "youtube_about",
                    "channel_extractor": "youtubejs",
                },
                "contents": [{
                    "source_content_id": "v1",
                    "content_type": "video",
                    "content_type_source": "youtube_channel_tab:video",
                    "description": "A description",
                    "description_source": "youtubejs_player",
                    "published_at": "2026-08-01T00:00:00Z",
                    "published_at_source": "youtubejs_player_microformat",
                    "view_count": 100,
                    "view_count_source": "youtubejs_player",
                    "like_count": 5,
                    "extractor_version": "youtubei.js@test",
                    "comments_first_page": {"comments": []},
                }],
                "as_of": "2026-08-09T00:00:00Z",
                "provenance": {
                    "data_lineage_version": "snapshot-field-lineage-v1",
                    "comment_page_source_status": "extractor_not_persisted",
                },
            },
        }

        report = build_snapshot_audit([row])
        coverage = report["snapshot_lineage_coverage"]
        self.assertEqual(coverage["snapshot_rows_with_lineage_version"], 1)
        self.assertEqual(coverage["channel_country_values_with_source"], 1)
        self.assertEqual(coverage["content_description_values_with_source"], 1)
        self.assertEqual(coverage["content_view_count_values_with_source"], 1)
        self.assertEqual(coverage["content_like_count_values"], 1)
        self.assertNotIn("content_like_count_values_with_source", coverage)
        self.assertEqual(coverage["comment_page_rows"], 1)
        self.assertNotIn("comment_page_rows_with_source", coverage)


if __name__ == "__main__":
    unittest.main()
