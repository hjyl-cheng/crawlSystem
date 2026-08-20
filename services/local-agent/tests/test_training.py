import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from qy_channel_profile.language_id import file_sha256
from qy_channel_profile.errors import TrainingDataError
from qy_channel_profile.feature_store import FEATURE_STORE_SCHEMA_VERSION
from qy_channel_profile.features import FEATURE_SCHEMA_VERSION
from qy_channel_profile.model_bundle import ModelBundle
from qy_channel_profile.training import (
    CATEGORY_TEXT_TRANSFORM_VERSION,
    ChannelProfileModelBuilder,
    ModelBuildPlan,
    _category_text_transformer,
)
from qy_channel_profile.training import _validation_split


class ModelBuilderTest(unittest.TestCase):
    def test_category_text_transform_masks_rule_triggers_but_keeps_context(self):
        transformed = _category_text_transformer().fit_transform([
            "Roblox gameplay speedrun with a difficult boss battle",
        ])[0]

        self.assertNotIn("roblox", transformed)
        self.assertNotIn("gameplay", transformed)
        self.assertIn("speedrun with a difficult boss battle", transformed)

    def test_equal_timestamp_rows_use_deterministic_hash_split(self):
        rows = [
            {
                "channel_id": f"UC{index:022d}",
                "as_of": "2026-08-11T00:00:00Z",
            }
            for index in range(100)
        ]
        first = _validation_split(rows)
        second = _validation_split(list(reversed(rows)))

        first_ids = [
            {rows[index]["channel_id"] for index in indices}
            for indices in first[:3]
        ]
        reversed_rows = list(reversed(rows))
        second_ids = [
            {reversed_rows[index]["channel_id"] for index in indices}
            for indices in second[:3]
        ]
        self.assertEqual(first[3], "channel_unique_hash_70_15_15")
        self.assertEqual(first_ids, second_ids)

    def test_builder_rejects_legacy_feature_store_manifest_before_loading_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            feature_path = root / "features.parquet"
            language_model = root / "lid.176.ftz"
            feature_path.write_bytes(b"not-read")
            language_model.write_bytes(b"not-read")
            feature_path.with_suffix(".parquet.manifest.json").write_text(
                json.dumps({
                    "schema_version": "qy-channel-profile-feature-store-v3",
                    "feature_schema_version": FEATURE_SCHEMA_VERSION,
                    "agent_reference_columns_present": False,
                    "output_sha256": file_sha256(feature_path),
                    "prior_catalog_version": "synthetic-prior-v1",
                    "prior_catalog_hash": "sha256:" + "0" * 64,
                }),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                TrainingDataError,
                "feature store manifest schema is incompatible",
            ):
                ChannelProfileModelBuilder().build(ModelBuildPlan(
                    feature_store_path=feature_path,
                    output_directory=root / "bundles",
                    bundle_version="test-v1",
                    language_model_path=language_model,
                ))

    def test_builder_rejects_current_manifest_without_lineage_status(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            feature_path = root / "features.parquet"
            language_model = root / "lid.176.ftz"
            feature_path.write_bytes(b"not-read")
            language_model.write_bytes(b"not-read")
            feature_path.with_suffix(".parquet.manifest.json").write_text(
                json.dumps({
                    "schema_version": FEATURE_STORE_SCHEMA_VERSION,
                    "feature_schema_version": FEATURE_SCHEMA_VERSION,
                }),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                TrainingDataError,
                "feature store lineage status is required",
            ):
                ChannelProfileModelBuilder().build(ModelBuildPlan(
                    feature_store_path=feature_path,
                    output_directory=root / "bundles",
                    bundle_version="test-v1",
                    language_model_path=language_model,
                ))

    def test_builder_trains_without_agent_columns_and_packages_immutable_bundle(self):
        language_model = Path("artifacts/models/external/lid.176.ftz").resolve()
        if not language_model.is_file():
            self.skipTest("local FastText artifact is not installed")
        import polars as pl

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            feature_path = root / "features.parquet"
            rows = []
            start = datetime(2025, 1, 1, tzinfo=timezone.utc)
            for index in range(120):
                mexico = index % 2 == 0
                team = index % 3 == 0
                country = "Mexico" if mexico else "Spain"
                category = "Gaming" if mexico else "Food"
                text = (
                    f"soy de mexico canal roblox gameplay creador numero {index}"
                    if mexico else
                    f"soy de españa canal recetas cocina creadora numero {index}"
                )
                gender = "brand_team" if team else ("male" if mexico else "female")
                rows.append({
                    "channel_id": f"UC{index:022d}",
                    "as_of": (start + timedelta(days=index)).isoformat().replace("+00:00", "Z"),
                    "channel_text": text,
                    "content_text": text,
                    "full_text": text,
                    "identity_text": text,
                    "category_text": text,
                    "tag_text": text,
                    "country_label": country,
                    "gender_label": gender,
                    "age_label": 22 if mexico else 32,
                    "category_level1_label": category,
                    "category_level2_labels_json": json.dumps(
                        ["Casual Games"] if mexico else ["Cooking Tutorials"]
                    ),
                    "tag_labels_json": json.dumps(
                        ["Roblox", "Gameplay"] if mexico else ["Food", "Cooking Tutorials"]
                    ),
                })
            pl.DataFrame(rows).write_parquet(feature_path)
            manifest = {
                "schema_version": FEATURE_STORE_SCHEMA_VERSION,
                "feature_schema_version": FEATURE_SCHEMA_VERSION,
                "agent_reference_columns_present": False,
                "output_sha256": file_sha256(feature_path),
                "label_policy": "synthetic-explicit-test-no-agent",
                "lineage_status": "complete",
                "source_rows_with_lineage_version": len(rows),
                "prior_catalog_version": "synthetic-prior-v1",
                "prior_catalog_hash": "sha256:" + "0" * 64,
            }
            feature_path.with_suffix(".parquet.manifest.json").write_text(
                json.dumps(manifest), encoding="utf-8"
            )
            result = ChannelProfileModelBuilder().build(ModelBuildPlan(
                feature_store_path=feature_path,
                output_directory=root / "bundles",
                bundle_version="test-v1",
                language_model_path=language_model,
                minimum_total_samples=40,
                minimum_class_samples=5,
                minimum_country_classes=2,
                minimum_multilabel_samples=5,
                minimum_age_samples=40,
                target_evidence_precision=0.7,
            ))
            bundle = ModelBundle.load(result.manifest_path)
            prediction = bundle.predict_text(
                "country.text",
                "soy de mexico y este es mi canal de roblox gameplay",
            )
            self.assertIsNotNone(prediction)
            self.assertEqual(prediction.value, "Mexico")
            self.assertFalse(result.manifest.build_metadata["agent_reference_used_for_training"])
            self.assertEqual(
                result.manifest.build_metadata["feature_store_lineage_status"],
                "complete",
            )
            self.assertTrue((result.bundle_path / "artifacts/lid.176.ftz").is_file())
            category = bundle.descriptor("categories.level1")
            self.assertIsNotNone(category)
            self.assertEqual(
                category.metadata["text_transform_version"],
                CATEGORY_TEXT_TRANSFORM_VERSION,
            )
            category_policy = bundle.decision_policy("channel_categories")
            self.assertGreater(category_policy.complete_min_probability, 0.0)
            self.assertTrue(
                category_policy.metadata["complete_mode_uses_validation_threshold"]
            )
            with self.assertRaises(Exception):
                ChannelProfileModelBuilder().build(ModelBuildPlan(
                    feature_store_path=feature_path,
                    output_directory=root / "bundles",
                    bundle_version="test-v1",
                    language_model_path=language_model,
                    minimum_total_samples=40,
                    minimum_class_samples=5,
                    minimum_country_classes=2,
                    minimum_multilabel_samples=5,
                    minimum_age_samples=40,
                ))


if __name__ == "__main__":
    unittest.main()
