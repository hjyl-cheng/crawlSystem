from __future__ import annotations

import json
import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .errors import ModelBundleError
from .model_bundle import ArtifactDescriptor, ModelBundle, ModelBundleManifest


QUALITY_GATE_VERSION = "deployment-quality-gate-v2-gold-aware"
GOLD_REQUIRED_FIELDS = frozenset({"channel_categories", "channel_tags", "creator_gender"})
KAPPA_REQUIRED_ARTIFACTS = frozenset({"categories.level1", "gender.entity_type"})


@dataclass(frozen=True)
class DeploymentBundleResult:
    bundle_path: Path
    manifest_path: Path
    manifest: ModelBundleManifest


def _weak_validation_precision(artifact: ArtifactDescriptor) -> float | None:
    for key in ("validation_micro_precision", "validation_evidence_precision"):
        value = artifact.metrics.get(key)
        if value is not None:
            return float(value)
    return None


def _deployment_precision(artifact: ArtifactDescriptor) -> tuple[float | None, str]:
    if artifact.field in GOLD_REQUIRED_FIELDS:
        gold = artifact.gold_evaluation
        if gold is None:
            return None, "independent gold-test evaluation is required for this field"
        interval = gold.confidence_intervals[gold.precision_metric]
        return interval.lower, (
            f"gold {gold.precision_metric} {interval.confidence_level:.0%} confidence lower bound"
        )
    return _weak_validation_precision(artifact), "validation precision"


def _initial_decisions(
    artifacts: tuple[ArtifactDescriptor, ...],
    target_precision: float,
    minimum_kappa: float,
    minimum_gold_test_rows: int,
) -> tuple[set[str], dict[str, str]]:
    active: set[str] = set()
    excluded: dict[str, str] = {}
    for artifact in artifacts:
        if artifact.status == "disabled":
            excluded[artifact.artifact_id] = "source artifact is disabled"
            continue
        if artifact.kind == "fasttext_language":
            active.add(artifact.artifact_id)
            continue
        precision, precision_source = _deployment_precision(artifact)
        gold = artifact.gold_evaluation
        if artifact.artifact_id in KAPPA_REQUIRED_ARTIFACTS and gold is not None:
            kappa_interval = gold.confidence_intervals.get("cohen_kappa")
            kappa = kappa_interval.lower if kappa_interval is not None else None
            kappa_source = "gold Cohen kappa confidence lower bound"
        else:
            kappa = artifact.metrics.get("cohen_kappa")
            kappa_source = "Cohen kappa"
        if precision is None:
            excluded[artifact.artifact_id] = precision_source
        elif (
            artifact.field in GOLD_REQUIRED_FIELDS
            and gold is not None
            and gold.gold_test_rows < minimum_gold_test_rows
        ):
            excluded[artifact.artifact_id] = (
                f"gold test rows {gold.gold_test_rows} are below {minimum_gold_test_rows}"
            )
        elif artifact.artifact_id in KAPPA_REQUIRED_ARTIFACTS and kappa is None:
            excluded[artifact.artifact_id] = (
                "independent gold-test Cohen kappa confidence interval is required"
            )
        elif precision < target_precision:
            excluded[artifact.artifact_id] = (
                f"{precision_source} {precision:.6f} is below {target_precision:.6f}"
            )
        elif kappa is not None and float(kappa) < minimum_kappa:
            excluded[artifact.artifact_id] = (
                f"{kappa_source} {float(kappa):.6f} is below {minimum_kappa:.6f}"
            )
        else:
            active.add(artifact.artifact_id)

    # A hierarchical category prediction is only usable when both levels pass.
    category_chain = {"categories.level1", "categories.level2"}
    if active.intersection(category_chain) and not category_chain.issubset(active):
        for artifact_id in category_chain:
            if artifact_id in active:
                active.remove(artifact_id)
                excluded[artifact_id] = "category hierarchy is incomplete after quality gating"
    # A male/female classifier is unusable until the entity classifier has
    # established that the account represents a single creator. The entity
    # classifier may still be deployed alone to identify brand/team accounts.
    if "gender.single_creator" in active and "gender.entity_type" not in active:
        active.remove("gender.single_creator")
        excluded["gender.single_creator"] = "gender entity chain is incomplete after quality gating"
    return active, excluded


def build_deployment_bundle(
    source_manifest_path: str | Path,
    output_directory: str | Path,
    *,
    bundle_version: str,
    target_precision: float = 0.85,
    minimum_kappa: float = 0.70,
    minimum_gold_test_rows: int = 200,
) -> DeploymentBundleResult:
    if not 0.5 <= target_precision <= 1.0:
        raise ValueError("target_precision must be in [0.5, 1.0]")
    if not 0.0 <= minimum_kappa <= 1.0:
        raise ValueError("minimum_kappa must be in [0, 1]")
    if minimum_gold_test_rows < 1:
        raise ValueError("minimum_gold_test_rows must be positive")
    source_path = Path(source_manifest_path).expanduser().resolve()
    if source_path.is_dir():
        source_path = source_path / "manifest.json"
    source = ModelBundle.load(source_path)
    if source.root is None:
        raise ModelBundleError("deployment bundle requires a filesystem-backed source bundle")
    target = Path(output_directory).expanduser().resolve() / bundle_version
    if target.exists():
        raise ModelBundleError(f"immutable bundle path already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.parent / f".{bundle_version}.tmp-{uuid.uuid4().hex}"
    temporary.mkdir(parents=True)

    active_ids, excluded = _initial_decisions(
        source.manifest.artifacts,
        target_precision,
        minimum_kappa,
        minimum_gold_test_rows,
    )
    active_artifacts = tuple(
        artifact.model_copy(update={"status": "active"})
        for artifact in source.manifest.artifacts
        if artifact.artifact_id in active_ids
    )
    try:
        for artifact in active_artifacts:
            source_path = source.root / artifact.relative_path
            target_path = temporary / artifact.relative_path
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, target_path)

        field_status = dict(source.manifest.field_status)
        if "categories.level1" not in active_ids or "categories.level2" not in active_ids:
            field_status["channel_categories"] = "fallback_only_candidate_models_below_gate"
        if "channel_tags.multilabel" not in active_ids:
            field_status["channel_tags"] = "fallback_only_candidate_model_below_gate"
        if "country.text" not in active_ids:
            field_status["country"] = "fallback_only_candidate_model_below_gate"
        if "creator_age.ordinal" not in active_ids:
            field_status["creator_age_range"] = "fallback_only_candidate_model_below_gate"
        if "gender.entity_type" in active_ids and "gender.single_creator" not in active_ids:
            field_status["creator_gender"] = "partial_brand_team_model_plus_fallback"
        elif not {"gender.entity_type", "gender.single_creator"}.issubset(active_ids):
            field_status["creator_gender"] = "fallback_only_candidate_models_below_gate"

        active_fields = {artifact.field for artifact in active_artifacts}
        policies = {
            field: policy
            for field, policy in source.manifest.decision_policies.items()
            if field in active_fields
        }
        excluded_details: list[dict[str, Any]] = []
        descriptors = {artifact.artifact_id: artifact for artifact in source.manifest.artifacts}
        for artifact_id, reason in sorted(excluded.items()):
            artifact = descriptors[artifact_id]
            excluded_details.append({
                "artifact_id": artifact_id,
                "field": artifact.field,
                "reason": reason,
                "metrics": artifact.metrics,
            })

        build_metadata = dict(source.manifest.build_metadata)
        build_metadata.update({
            "deployment_quality_gate_version": QUALITY_GATE_VERSION,
            "deployment_source_bundle": source.version,
            "deployment_source_bundle_hash": source.content_hash,
            "deployment_target_precision": target_precision,
            "deployment_minimum_kappa": minimum_kappa,
            "deployment_minimum_gold_test_rows": minimum_gold_test_rows,
            "deployment_gold_required_fields": sorted(GOLD_REQUIRED_FIELDS),
            "deployment_kappa_required_artifacts": sorted(KAPPA_REQUIRED_ARTIFACTS),
            "active_artifact_ids": sorted(active_ids),
            "excluded_candidate_artifacts": excluded_details,
            "agent_reference_used_for_training": False,
            "output_label_language": "English",
        })
        manifest = ModelBundleManifest(
            bundle_version=bundle_version,
            created_at=datetime.now(timezone.utc),
            feature_schema_version=source.manifest.feature_schema_version,
            taxonomy_version=source.manifest.taxonomy_version,
            output_label_language="English",
            compatible_processor_major=source.manifest.compatible_processor_major,
            production_eligible=False,
            training_cutoff=source.manifest.training_cutoff,
            artifacts=active_artifacts,
            field_status=field_status,
            decision_policies=policies,
            dependency_manifest=source.manifest.dependency_manifest,
            build_metadata=build_metadata,
        )
        manifest_path = temporary / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest.model_dump(mode="json"), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.rename(target)
        final_manifest = target / "manifest.json"
        ModelBundle.load(final_manifest)
        return DeploymentBundleResult(target, final_manifest, manifest)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
