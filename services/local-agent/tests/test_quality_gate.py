import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from qy_channel_profile.language_id import file_sha256
from qy_channel_profile.model_bundle import (
    ArtifactDescriptor,
    GoldEvaluationDescriptor,
    MetricConfidenceInterval,
    ModelBundle,
    ModelBundleManifest,
)
from qy_channel_profile.quality_gate import build_deployment_bundle


class QualityGateTest(unittest.TestCase):
    @staticmethod
    def _gold(
        *,
        precision: float,
        lower: float,
        kappa: float | None = None,
        rows: int = 200,
    ):
        metrics = {"macro_f1": precision, "precision": precision}
        intervals = {
            "macro_f1": MetricConfidenceInterval(
                lower=lower,
                upper=min(1.0, precision + 0.03),
                method="bootstrap_channel",
            ),
            "precision": MetricConfidenceInterval(
                lower=lower,
                upper=min(1.0, precision + 0.03),
                method="bootstrap_channel",
            ),
        }
        if kappa is not None:
            metrics["cohen_kappa"] = kappa
            intervals["cohen_kappa"] = MetricConfidenceInterval(
                lower=kappa - 0.04,
                upper=min(1.0, kappa + 0.04),
                method="bootstrap_channel",
            )
        return GoldEvaluationDescriptor(
            truth_status="verified_manual",
            gold_dataset_hash="sha256:" + "1" * 64,
            annotation_guideline_version="qy-annotation-v1",
            gold_test_rows=rows,
            primary_metric="macro_f1",
            precision_metric="precision",
            gold_metrics=metrics,
            subgroup_metrics={"language": {"reported": True}},
            confidence_intervals=intervals,
        )

    def test_gate_rejects_high_risk_artifacts_without_independent_gold(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            artifacts_dir = source / "artifacts"
            artifacts_dir.mkdir(parents=True)
            descriptors = []
            specs = [
                ("creator_language.fasttext", "creator_language", "fasttext_language", {}),
                ("categories.level1", "channel_categories", "sklearn_text_classifier", {"validation_evidence_precision": 0.95}),
                ("categories.level2", "channel_categories", "sklearn_multilabel_classifier", {"validation_micro_precision": 0.80}),
                ("channel_tags.multilabel", "channel_tags", "sklearn_multilabel_classifier", {"validation_micro_precision": 0.60}),
                ("gender.entity_type", "creator_gender", "sklearn_text_classifier", {"validation_evidence_precision": 0.90}),
            ]
            for index, (artifact_id, field, kind, metrics) in enumerate(specs):
                path = artifacts_dir / f"artifact-{index}.bin"
                path.write_bytes(f"artifact-{index}".encode())
                descriptors.append(ArtifactDescriptor(
                    artifact_id=artifact_id,
                    field=field,
                    kind=kind,
                    relative_path=f"artifacts/{path.name}",
                    sha256=file_sha256(path),
                    training_source="unit test",
                    label_quality="synthetic_test_only",
                    metrics=metrics,
                ))
            manifest = ModelBundleManifest(
                bundle_version="source-v1",
                created_at=datetime.now(timezone.utc),
                artifacts=tuple(descriptors),
                field_status={},
            )
            manifest_path = source / "manifest.json"
            manifest_path.write_text(manifest.model_dump_json(), encoding="utf-8")

            result = build_deployment_bundle(
                source,
                root / "deployments",
                bundle_version="deployment-v1",
                target_precision=0.85,
            )
            active = {artifact.artifact_id for artifact in result.manifest.artifacts}
            self.assertEqual(active, {"creator_language.fasttext"})
            self.assertEqual(result.manifest.output_label_language, "English")
            self.assertEqual(
                result.manifest.field_status["channel_categories"],
                "fallback_only_candidate_models_below_gate",
            )
            self.assertEqual(
                result.manifest.field_status["creator_gender"],
                "fallback_only_candidate_models_below_gate",
            )
            excluded = {
                item["artifact_id"]: item["reason"]
                for item in result.manifest.build_metadata["excluded_candidate_artifacts"]
            }
            self.assertIn("independent gold-test evaluation", excluded["gender.entity_type"])
            ModelBundle.load(result.manifest_path)

    def test_gate_uses_gold_confidence_lower_bound_and_preserves_chains(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            artifacts_dir = source / "artifacts"
            artifacts_dir.mkdir(parents=True)
            descriptors = []
            specs = [
                ("categories.level1", "channel_categories", "sklearn_text_classifier", 0.93),
                ("categories.level2", "channel_categories", "sklearn_multilabel_classifier", 0.91),
                ("channel_tags.multilabel", "channel_tags", "sklearn_multilabel_classifier", 0.90),
                ("gender.entity_type", "creator_gender", "sklearn_text_classifier", 0.92),
                ("gender.single_creator", "creator_gender", "sklearn_text_classifier", 0.91),
            ]
            for index, (artifact_id, field, kind, precision) in enumerate(specs):
                path = artifacts_dir / f"artifact-{index}.bin"
                path.write_bytes(f"artifact-{index}".encode())
                descriptors.append(ArtifactDescriptor(
                    artifact_id=artifact_id,
                    field=field,
                    kind=kind,
                    relative_path=f"artifacts/{path.name}",
                    sha256=file_sha256(path),
                    training_source="unit test weak labels",
                    label_quality="high_precision_weak_supervision",
                    metrics={"validation_micro_precision": 0.99},
                    gold_evaluation=self._gold(
                        precision=precision,
                        lower=precision - 0.04,
                        kappa=0.82 if artifact_id in {"categories.level1", "gender.entity_type"} else None,
                    ),
                ))
            manifest = ModelBundleManifest(
                bundle_version="source-gold-v1",
                created_at=datetime.now(timezone.utc),
                artifacts=tuple(descriptors),
                field_status={},
            )
            manifest_path = source / "manifest.json"
            manifest_path.write_text(manifest.model_dump_json(), encoding="utf-8")

            result = build_deployment_bundle(
                manifest_path,
                root / "deployments",
                bundle_version="deployment-gold-v1",
                target_precision=0.85,
            )

            active = {artifact.artifact_id for artifact in result.manifest.artifacts}
            self.assertEqual(active, {artifact_id for artifact_id, *_ in specs})
            self.assertEqual(
                result.manifest.build_metadata["deployment_quality_gate_version"],
                "deployment-quality-gate-v2-gold-aware",
            )
            ModelBundle.load(result.manifest_path)

    def test_gate_rejects_point_estimate_when_gold_lower_bound_misses_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            artifacts_dir = source / "artifacts"
            artifacts_dir.mkdir(parents=True)
            path = artifacts_dir / "tags.bin"
            path.write_bytes(b"tags")
            descriptor = ArtifactDescriptor(
                artifact_id="channel_tags.multilabel",
                field="channel_tags",
                kind="sklearn_multilabel_classifier",
                relative_path="artifacts/tags.bin",
                sha256=file_sha256(path),
                training_source="unit test",
                label_quality="high_precision_weak_supervision",
                metrics={"validation_micro_precision": 0.99},
                gold_evaluation=self._gold(precision=0.92, lower=0.84),
            )
            manifest = ModelBundleManifest(
                bundle_version="source-low-ci-v1",
                created_at=datetime.now(timezone.utc),
                artifacts=(descriptor,),
                field_status={},
            )
            manifest_path = source / "manifest.json"
            manifest_path.write_text(manifest.model_dump_json(), encoding="utf-8")

            result = build_deployment_bundle(
                manifest_path,
                root / "deployments",
                bundle_version="deployment-low-ci-v1",
                target_precision=0.85,
            )

            self.assertEqual(result.manifest.artifacts, ())
            reason = result.manifest.build_metadata["excluded_candidate_artifacts"][0]["reason"]
            self.assertIn("confidence lower bound 0.840000", reason)

    def test_gate_rejects_undersized_gold_test(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            artifacts_dir = source / "artifacts"
            artifacts_dir.mkdir(parents=True)
            path = artifacts_dir / "tags.bin"
            path.write_bytes(b"tags")
            descriptor = ArtifactDescriptor(
                artifact_id="channel_tags.multilabel",
                field="channel_tags",
                kind="sklearn_multilabel_classifier",
                relative_path="artifacts/tags.bin",
                sha256=file_sha256(path),
                training_source="unit test",
                label_quality="verified_manual",
                metrics={},
                gold_evaluation=self._gold(precision=0.95, lower=0.90, rows=199),
            )
            manifest = ModelBundleManifest(
                bundle_version="source-small-gold-v1",
                created_at=datetime.now(timezone.utc),
                artifacts=(descriptor,),
                field_status={},
            )
            manifest_path = source / "manifest.json"
            manifest_path.write_text(manifest.model_dump_json(), encoding="utf-8")

            result = build_deployment_bundle(
                manifest_path,
                root / "deployments",
                bundle_version="deployment-small-gold-v1",
            )

            reason = result.manifest.build_metadata["excluded_candidate_artifacts"][0]["reason"]
            self.assertEqual(reason, "gold test rows 199 are below 200")

    def test_gate_rejects_kappa_when_confidence_lower_bound_misses_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            artifacts_dir = source / "artifacts"
            artifacts_dir.mkdir(parents=True)
            path = artifacts_dir / "entity.bin"
            path.write_bytes(b"entity")
            descriptor = ArtifactDescriptor(
                artifact_id="gender.entity_type",
                field="creator_gender",
                kind="sklearn_text_classifier",
                relative_path="artifacts/entity.bin",
                sha256=file_sha256(path),
                training_source="unit test",
                label_quality="verified_manual",
                metrics={},
                gold_evaluation=self._gold(precision=0.95, lower=0.90, kappa=0.72),
            )
            manifest = ModelBundleManifest(
                bundle_version="source-low-kappa-ci-v1",
                created_at=datetime.now(timezone.utc),
                artifacts=(descriptor,),
                field_status={},
            )
            manifest_path = source / "manifest.json"
            manifest_path.write_text(manifest.model_dump_json(), encoding="utf-8")

            result = build_deployment_bundle(
                manifest_path,
                root / "deployments",
                bundle_version="deployment-low-kappa-ci-v1",
                minimum_kappa=0.70,
            )

            reason = result.manifest.build_metadata["excluded_candidate_artifacts"][0]["reason"]
            self.assertIn("kappa confidence lower bound 0.680000", reason)


if __name__ == "__main__":
    unittest.main()
