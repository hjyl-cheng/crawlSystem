import unittest

from qy_channel_profile.contracts import AnalysisPolicy, FieldResult
from qy_channel_profile.inference import resolve_age, resolve_categories, resolve_gender, resolve_tags
from qy_channel_profile.model_bundle import ModelBundle, ModelPrediction


class TagInferenceTest(unittest.TestCase):
    def test_model_cannot_add_tag_without_snapshot_support(self):
        baseline = FieldResult(
            value={
                "tags": [
                    "Football", "Ball Sports", "Curiosities", "Sports & Outdoors",
                    "Short-form Video", "News Commentary", "Storytelling",
                    "Digital Culture", "Channel Series", "Video Commentary",
                ],
                "top_5_distribution": [
                    {"tag": "Football", "percentage": 30},
                    {"tag": "Ball Sports", "percentage": 25},
                    {"tag": "Curiosities", "percentage": 20},
                    {"tag": "Sports & Outdoors", "percentage": 15},
                    {"tag": "Short-form Video", "percentage": 8},
                    {"tag": "Other", "percentage": 2},
                ],
            },
            source_type="rule_inferred",
            truth_status="estimated",
            evidence_strength="strong",
            model_confidence=0.9,
            evidence_confidence=0.9,
            metadata={
                "supported_tags": [
                    "Football", "Ball Sports", "Curiosities", "Sports & Outdoors",
                    "Short-form Video", "News Commentary", "Storytelling",
                ],
                "fallback_tags": ["Digital Culture", "Channel Series", "Video Commentary"],
            },
        )
        prediction = ModelPrediction(
            value=["Hair & Wigs", "Football"],
            probability=0.999,
            margin=0.001,
            candidates=(
                {"value": "Hair & Wigs", "probability": 0.999},
                {"value": "Football", "probability": 0.99},
            ),
            model_version="test-model",
            metadata={"artifact_id": "channel_tags.multilabel"},
        )
        result = resolve_tags(
            baseline,
            prediction,
            AnalysisPolicy.COMPLETE_ESTIMATE,
            ModelBundle.empty(),
        )
        self.assertNotIn("Hair & Wigs", result.value["tags"])
        self.assertEqual(result.value["tags"][:5], baseline.value["tags"][:5])
        self.assertEqual(
            result.value["top_5_distribution"],
            baseline.value["top_5_distribution"],
        )
        self.assertEqual(result.metadata["model_confirmed_tags"], ["Football"])


class GenderInferenceTest(unittest.TestCase):
    def test_qualified_comment_consensus_is_not_overwritten_by_entity_model(self):
        baseline = FieldResult(
            value="male",
            source_type="public_signal_model",
            truth_status="estimated",
            evidence_strength="weak",
            model_confidence=0.78,
            evidence_confidence=0.6,
            metadata={
                "evidence_tier": "B",
                "consensus_gate_passed": True,
                "account_entity_type": "unknown",
                "primary_creator_status": "single_stable",
            },
        )
        entity_prediction = ModelPrediction(
            value="brand_or_team",
            probability=0.99,
            margin=0.98,
            candidates=(
                {"value": "brand_or_team", "probability": 0.99},
                {"value": "single_creator", "probability": 0.01},
            ),
            model_version="imbalanced-entity-model",
            metadata={"artifact_id": "gender.entity_type"},
        )

        result = resolve_gender(
            baseline,
            entity_prediction,
            None,
            AnalysisPolicy.COMPLETE_ESTIMATE,
            ModelBundle.empty(),
        )

        self.assertEqual(result.value, "male")
        self.assertTrue(result.metadata["consensus_gate_passed"])

    def test_entity_model_cannot_override_tier_a_primary_gender(self):
        baseline = FieldResult(
            value="female",
            source_type="rule_inferred",
            truth_status="estimated",
            evidence_strength="weak",
            model_confidence=0.55,
            evidence_confidence=0.4,
            metadata={
                "evidence_tier": "A",
                "account_entity_type": "brand_or_team",
                "primary_creator_status": "single_stable",
                "primary_creator_subject": "Aline Castro",
            },
        )
        entity_prediction = ModelPrediction(
            value="brand_or_team",
            probability=0.99,
            margin=0.98,
            candidates=(
                {"value": "brand_or_team", "probability": 0.99},
                {"value": "single_creator", "probability": 0.01},
            ),
            model_version="imbalanced-entity-model",
            metadata={"artifact_id": "gender.entity_type"},
        )

        result = resolve_gender(
            baseline,
            entity_prediction,
            None,
            AnalysisPolicy.COMPLETE_ESTIMATE,
            ModelBundle.empty(),
        )

        self.assertEqual(result.value, "female")
        self.assertEqual(result.metadata["evidence_tier"], "A")
        self.assertEqual(result.metadata["account_entity_type"], "brand_or_team")


class AgeInferenceTest(unittest.TestCase):
    def test_model_cannot_replace_prior_only_age_with_a_bucket_guess(self):
        baseline = FieldResult(
            value=31,
            source_type="public_prior_estimate",
            truth_status="estimated",
            evidence_strength="prior_only",
            model_confidence=0.04,
            evidence_confidence=0.1,
            metadata={
                "age_interval": [18, 44],
                "evidence_basis": "prior_only",
                "compatibility_point_from_interval": True,
            },
        )
        prediction = ModelPrediction(
            value="25-34",
            probability=0.99,
            margin=0.8,
            candidates=({"value": "25-34", "probability": 0.99},),
            model_version="age-ordinal",
            metadata={"artifact_id": "creator_age.ordinal"},
        )

        result = resolve_age(
            baseline,
            prediction,
            AnalysisPolicy.COMPLETE_ESTIMATE,
            ModelBundle.empty(),
        )

        self.assertEqual(result.value, 31)
        self.assertEqual(result.evidence_strength, "prior_only")
        self.assertEqual(result.metadata["evidence_basis"], "prior_only")


class CategoryInferenceTest(unittest.TestCase):
    def test_confident_rule_level1_is_not_replaced_by_a_different_model(self):
        baseline = FieldResult(
            value={"level_1": "Tech", "level_2": ["Mobile Tech"]},
            source_type="rule_inferred",
            truth_status="estimated",
            evidence_strength="weak",
            model_confidence=0.72,
            evidence_confidence=0.4,
            metadata={"top_score": 6.0},
        )
        level_1 = ModelPrediction(
            value="Gaming",
            probability=0.92,
            margin=0.4,
            candidates=({"value": "Gaming", "probability": 0.92},),
            model_version="cat-l1",
            metadata={"artifact_id": "categories.level1"},
        )
        level_2 = ModelPrediction(
            value=["Mobile Games"],
            probability=0.88,
            margin=0.3,
            candidates=(
                {"value": "Mobile Games", "probability": 0.88},
                {"value": "Action Games", "probability": 0.4},
            ),
            model_version="cat-l2",
            metadata={"artifact_id": "categories.level2"},
        )

        result = resolve_categories(
            baseline,
            level_1,
            level_2,
            AnalysisPolicy.COMPLETE_ESTIMATE,
            ModelBundle.empty(),
        )

        self.assertEqual(result.value["level_1"], "Tech")
        self.assertEqual(result.value["level_2"], ["Mobile Tech"])

    def test_uncategorized_rule_baseline_still_allows_the_model(self):
        baseline = FieldResult(
            value={"level_1": "Uncategorized", "level_2": ["Uncategorized"]},
            source_type="rule_inferred",
            truth_status="estimated",
            evidence_strength="weak",
            model_confidence=0.0,
            evidence_confidence=0.1,
        )
        level_1 = ModelPrediction(
            value="Gaming",
            probability=0.92,
            margin=0.4,
            candidates=({"value": "Gaming", "probability": 0.92},),
            model_version="cat-l1",
            metadata={"artifact_id": "categories.level1"},
        )
        level_2 = ModelPrediction(
            value=["Adventure Games"],
            probability=0.88,
            margin=0.3,
            candidates=({"value": "Adventure Games", "probability": 0.88},),
            model_version="cat-l2",
            metadata={"artifact_id": "categories.level2"},
        )

        result = resolve_categories(
            baseline,
            level_1,
            level_2,
            AnalysisPolicy.COMPLETE_ESTIMATE,
            ModelBundle.empty(),
        )

        self.assertEqual(result.value["level_1"], "Gaming")
        self.assertEqual(result.value["level_2"][0], "Adventure Games")


if __name__ == "__main__":
    unittest.main()
