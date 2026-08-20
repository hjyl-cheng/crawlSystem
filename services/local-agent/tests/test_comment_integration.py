import copy
import unittest

from qy_channel_profile.analyzers import (
    analyze_active_ratio,
    analyze_audience_age_gender,
    analyze_audience_markets,
    analyze_categories,
    analyze_tags,
)
from qy_channel_profile.comment_features import build_comment_evidence
from qy_channel_profile.contracts import AnalysisPolicy, ChannelSnapshot, FieldResult
from qy_channel_profile.features import build_channel_features
from qy_channel_profile.priors import PriorCatalog


def field_result(value, *, strength="strong"):
    return FieldResult(
        value=value,
        source_type="rule_inferred",
        truth_status="estimated",
        evidence_strength=strength,
        model_confidence=0.9,
        evidence_confidence=0.9,
    )


def snapshot_value(*, include_comments=True):
    contents = []
    for video_index in range(5):
        content = {
            "source_content_id": f"video-{video_index}",
            "content_type": "video",
            "title": f"Technology product review episode {video_index}",
            "description": "An English technology review.",
            "published_at": f"2026-08-{10-video_index:02d}T00:00:00Z",
            "first_seen_at": f"2026-08-{10-video_index:02d}T01:00:00Z",
            "view_count": 10_000,
            "like_count": 500,
            "comment_count": 100,
            "duration_seconds": 600,
        }
        if include_comments:
            comments = []
            for comment_index in range(10):
                suffix = video_index * 10 + comment_index
                comments.append({
                    "comment_id": f"comment-{suffix}",
                    "position": comment_index + 1,
                    "text": (
                        "Saludos desde México. Tengo 29 años y soy un hombre; "
                        f"me encanta este análisis de anime y manga número {suffix}."
                    ),
                    "author_name": f"viewer-{suffix}",
                    "author_channel_id": f"UCviewer{suffix:014d}",
                    "author_url": f"https://www.youtube.com/channel/UCviewer{suffix:014d}",
                    "published_at_utc": "2026-08-10T12:00:00Z",
                    "published_text_raw": "1 day ago",
                    "published_at_status": "estimated_relative",
                    "like_count": suffix % 7,
                    "reply_count": int(suffix % 11 == 0),
                    "is_pinned": False,
                    "is_channel_owner": False,
                    "is_verified": False,
                    "is_hearted": suffix % 5 == 0,
                })
            content["comments_first_page"] = {
                "version": 1,
                "collected_at": "2026-08-10T18:00:00Z",
                "sort": "TOP_COMMENTS",
                "total_count": 100,
                "returned_count": len(comments),
                "comments": comments,
            }
        contents.append(content)
    return {
        "channel": {
            "channel_id": "UCcommentsintegration0001",
            "title": "Example Tech",
            "country": "United States",
            "country_code": "US",
            "about_description": "English technology reviews from the United States.",
            "subscriber_count": 100_000,
            "total_view_count": 2_000_000,
            "total_video_count": 100,
        },
        "contents": contents,
        "as_of": "2026-08-11T00:00:00Z",
    }


class CommentIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.catalog = PriorCatalog.load()
        self.snapshot = ChannelSnapshot.from_mapping(snapshot_value())
        self.comments = build_comment_evidence(self.snapshot)
        self.empty_snapshot = ChannelSnapshot.from_mapping(snapshot_value(include_comments=False))
        self.empty_comments = build_comment_evidence(self.empty_snapshot)

    def test_comments_shift_audience_marginals_but_keep_valid_totals(self):
        country = field_result("United States", strength="explicit")
        category = field_result({"level_1": "Tech", "level_2": ["Tech News"]})
        base_region, base_language = analyze_audience_markets(
            {"English": 1.0}, country, self.catalog, AnalysisPolicy.COMPLETE_ESTIMATE,
            snapshot=self.empty_snapshot, category=category, comments=self.empty_comments,
        )
        region, language = analyze_audience_markets(
            {"English": 1.0}, country, self.catalog, AnalysisPolicy.COMPLETE_ESTIMATE,
            snapshot=self.snapshot, category=category, comments=self.comments,
        )
        base_regions = {row["region"]: row["percentage"] for row in base_region.value}
        regions = {row["region"]: row["percentage"] for row in region.value}
        base_languages = {row["language"]: row["percentage"] for row in base_language.value}
        languages = {row["language"]: row["percentage"] for row in language.value}
        self.assertGreater(regions["Mexico"], base_regions.get("Mexico", 0))
        self.assertGreater(languages["Spanish"], base_languages.get("Spanish", 0))
        self.assertEqual(sum(regions.values()), 100)
        self.assertEqual(sum(languages.values()), 100)
        self.assertGreater(region.metadata["comment_region_blend"], 0)
        self.assertGreater(language.metadata["comment_language_blend"], 0)

    def test_comments_shift_age_gender_and_only_bound_active_ratio(self):
        category = field_result({"level_1": "Tech", "level_2": ["Tech News"]})
        base = analyze_audience_age_gender(
            category, self.catalog, AnalysisPolicy.COMPLETE_ESTIMATE,
            snapshot=self.empty_snapshot, comments=self.empty_comments,
        )
        enhanced = analyze_audience_age_gender(
            category, self.catalog, AnalysisPolicy.COMPLETE_ESTIMATE,
            snapshot=self.snapshot, comments=self.comments,
        )
        base_25_34 = next(row for row in base.value if row["age_range"] == "25-34")
        enhanced_25_34 = next(row for row in enhanced.value if row["age_range"] == "25-34")
        self.assertGreater(enhanced_25_34["male"], base_25_34["male"])
        self.assertEqual(
            sum(row[gender] for row in enhanced.value for gender in ("male", "female")),
            100,
        )

        base_active = analyze_active_ratio(
            self.empty_snapshot, self.catalog, AnalysisPolicy.COMPLETE_ESTIMATE,
            comments=self.empty_comments,
        )
        enhanced_active = analyze_active_ratio(
            self.snapshot, self.catalog, AnalysisPolicy.COMPLETE_ESTIMATE,
            comments=self.comments,
        )
        self.assertGreater(enhanced_active.metadata["comment_evidence_weight"], 0)
        self.assertGreaterEqual(enhanced_active.metadata["comment_quality_factor"], 0.9)
        self.assertLessEqual(enhanced_active.metadata["comment_quality_factor"], 1.11)
        self.assertLessEqual(abs(enhanced_active.value - base_active.value), 2)

    def test_comment_topics_cannot_flip_categories_but_can_support_tags(self):
        category_without_comments, _ = analyze_categories(
            self.empty_snapshot,
            AnalysisPolicy.COMPLETE_ESTIMATE,
        )
        category, _ = analyze_categories(
            self.snapshot,
            AnalysisPolicy.COMPLETE_ESTIMATE,
            comments=self.comments,
        )
        tags = analyze_tags(
            self.snapshot,
            category,
            field_result("English"),
            AnalysisPolicy.COMPLETE_ESTIMATE,
            comments=self.comments,
        )
        self.assertEqual(category.value, category_without_comments.value)
        self.assertEqual(category.value["level_1"], "Tech")
        self.assertEqual(category.metadata["comment_support"], 0)
        self.assertIn("Anime", tags.value["tags"])
        self.assertGreater(tags.metadata["comment_support_counts"]["Anime"], 1)

    def test_category_and_tag_model_text_route_comments_differently(self):
        features = build_channel_features(self.snapshot)
        category_text = features.model_text("channel_categories")
        tag_text = features.model_text("channel_tags")

        self.assertNotIn("anime", category_text)
        self.assertIn("anime", tag_text)
        self.assertNotEqual(category_text, tag_text)

    def test_empty_comment_evidence_is_value_compatible_with_no_argument(self):
        country = field_result("United States", strength="explicit")
        category = field_result({"level_1": "Tech", "level_2": ["Tech News"]})
        without = analyze_audience_markets(
            {"English": 1.0}, country, self.catalog, AnalysisPolicy.COMPLETE_ESTIMATE,
            snapshot=self.empty_snapshot, category=category,
        )
        empty = analyze_audience_markets(
            {"English": 1.0}, country, self.catalog, AnalysisPolicy.COMPLETE_ESTIMATE,
            snapshot=self.empty_snapshot, category=category, comments=self.empty_comments,
        )
        self.assertEqual(without[0].value, empty[0].value)
        self.assertEqual(without[1].value, empty[1].value)


if __name__ == "__main__":
    unittest.main()
