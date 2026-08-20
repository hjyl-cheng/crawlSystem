import unittest
from datetime import datetime, timezone

from qy_channel_profile.analyzers import analyze_country, analyze_language, primary_language_hint
from qy_channel_profile.contracts import AnalysisPolicy, ChannelSnapshot
from qy_channel_profile.priors import PriorCatalog
from qy_channel_profile.text_features import TextUnit, detect_languages


def make_snapshot(
    *,
    about: str = "",
    country: str | None = None,
    country_code: str | None = None,
    external_links: list[object] | None = None,
) -> ChannelSnapshot:
    return ChannelSnapshot.from_mapping({
        "channel": {
            "channel_id": "UC1234567890123456789012",
            "title": "Example Channel",
            "about_description": about,
            "country": country,
            "country_code": country_code,
            "external_links": external_links or [],
        },
        "contents": [],
        "as_of": datetime(2026, 8, 9, tzinfo=timezone.utc),
    })


class CountryPrecisionTest(unittest.TestCase):
    def setUp(self):
        self.catalog = PriorCatalog.load()

    def analyze(self, snapshot, languages, policy=AnalysisPolicy.EVIDENCE_FIRST):
        return analyze_country(snapshot, languages, self.catalog, policy)

    def test_explicit_localized_country_is_normalized_to_english(self):
        result = self.analyze(make_snapshot(country="Brasil"), {"Portuguese": 1.0})
        self.assertEqual(result.value, "Brazil")
        self.assertEqual(result.source_type, "observed")

    def test_conflicting_structured_country_fields_are_not_treated_as_fact(self):
        snapshot = make_snapshot(country="Canada", country_code="US")
        evidence_first = self.analyze(snapshot, {"English": 1.0})
        self.assertIsNone(evidence_first.value)
        self.assertEqual(evidence_first.metadata["reason"], "structured_country_fields_conflict")
        complete = self.analyze(snapshot, {"English": 1.0}, AnalysisPolicy.COMPLETE_ESTIMATE)
        self.assertEqual(complete.value, "Canada")
        self.assertEqual(complete.evidence_strength, "weak")
        self.assertTrue(complete.metadata["structured_conflict"])

    def test_shared_language_alone_never_becomes_evidence_first_country(self):
        result = self.analyze(make_snapshot(about="An English-language technology channel."), {"English": 1.0})
        self.assertIsNone(result.value)
        self.assertTrue(result.abstained)

    def test_missing_country_does_not_default_sparse_language_to_english(self):
        snapshot = make_snapshot()
        self.assertIsNone(primary_language_hint(snapshot.channel))
        result = analyze_language(
            detect_languages([]),
            AnalysisPolicy.COMPLETE_ESTIMATE,
            fallback_language=primary_language_hint(snapshot.channel),
        )
        self.assertIsNone(result.value)
        self.assertTrue(result.abstained)

    def test_complete_mode_marks_language_only_choice_as_forced_prior(self):
        result = self.analyze(
            make_snapshot(about="An English-language technology channel."),
            {"English": 1.0},
            AnalysisPolicy.COMPLETE_ESTIMATE,
        )
        self.assertEqual(result.evidence_strength, "prior_only")
        self.assertTrue(result.metadata["language_only"])
        self.assertTrue(result.metadata["forced_for_legacy_completeness"])

    def test_traditional_chinese_script_alone_does_not_choose_taiwan_or_hong_kong(self):
        result = self.analyze(
            make_snapshot(about="這是一個分享科技、生活和旅行內容的繁體中文頻道。"),
            {"Chinese": 1.0},
        )
        self.assertIsNone(result.value)
        self.assertTrue(result.abstained)

    def test_first_person_taiwan_location_is_decisive(self):
        result = self.analyze(
            make_snapshot(about="我們來自台灣，分享科技與生活內容。"),
            {"Chinese": 1.0},
        )
        self.assertEqual(result.value, "Taiwan")
        self.assertEqual(result.evidence_strength, "strong")
        self.assertFalse(result.metadata["language_used_as_country_evidence"])

    def test_hong_kong_phone_and_domain_are_independent_support(self):
        result = self.analyze(
            make_snapshot(
                about="Business enquiries: +852 2345 6789",
                external_links=["https://example.hk/contact"],
            ),
            {"Chinese": 1.0},
        )
        self.assertEqual(result.value, "Hong Kong")
        self.assertEqual(
            result.metadata["evidence_groups"]["Hong Kong"],
            ["country_domain", "phone_country_code"],
        )

    def test_conflicting_country_codes_abstain_in_evidence_first_mode(self):
        result = self.analyze(
            make_snapshot(about="Contact +852 2345 6789 or +886 2 2345 6789"),
            {"Chinese": 1.0},
        )
        self.assertIsNone(result.value)
        self.assertEqual(result.metadata["reason"], "country_public_signals_are_weak_or_conflicted")

    def test_han_without_kana_is_chinese_not_japanese(self):
        evidence = detect_languages([TextUnit("channel_about", "這是一個繁體中文科技頻道", 4.0)])
        self.assertEqual(max(evidence.probabilities, key=evidence.probabilities.get), "Chinese")
        self.assertNotIn("Japanese", evidence.probabilities)

    def test_japanese_kana_keeps_kanji_with_japanese(self):
        evidence = detect_languages([TextUnit("channel_about", "これは日本語の技術チャンネルです", 4.0)])
        self.assertEqual(max(evidence.probabilities, key=evidence.probabilities.get), "Japanese")


if __name__ == "__main__":
    unittest.main()
