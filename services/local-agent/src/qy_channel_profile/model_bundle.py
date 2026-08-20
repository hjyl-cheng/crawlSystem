from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .errors import ModelBundleError
from .features import FEATURE_SCHEMA_VERSION
from .hashing import sha256_json
from .language_id import FastTextLanguageIdentifier, file_sha256
from .taxonomy import TAXONOMY_VERSION


MODEL_BUNDLE_SCHEMA_VERSION = "qy-channel-profile-model-bundle-v1"


class MetricConfidenceInterval(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    confidence_level: float = Field(default=0.95, gt=0.0, lt=1.0)
    lower: float
    upper: float
    method: str = Field(min_length=1)

    @model_validator(mode="after")
    def ordered_bounds(self) -> "MetricConfidenceInterval":
        if not math.isfinite(self.lower) or not math.isfinite(self.upper):
            raise ValueError("confidence interval bounds must be finite")
        if self.lower > self.upper:
            raise ValueError("confidence interval lower bound exceeds upper bound")
        return self


class GoldEvaluationDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset_role: Literal["gold_test"] = "gold_test"
    truth_status: Literal["verified_manual", "explicit_fact", "authorized_analytics"]
    gold_dataset_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    annotation_guideline_version: str = Field(min_length=1)
    gold_test_rows: int = Field(ge=1)
    primary_metric: str = Field(min_length=1)
    precision_metric: str = Field(min_length=1)
    gold_metrics: dict[str, float] = Field(min_length=1)
    subgroup_metrics: dict[str, Any] = Field(min_length=1)
    confidence_intervals: dict[str, MetricConfidenceInterval] = Field(min_length=1)
    agent_reference_used: Literal[False] = False

    @model_validator(mode="after")
    def required_metrics_are_reported(self) -> "GoldEvaluationDescriptor":
        required = {self.primary_metric, self.precision_metric}
        missing_metrics = required.difference(self.gold_metrics)
        if missing_metrics:
            raise ValueError(f"gold metrics are missing: {sorted(missing_metrics)}")
        missing_intervals = required.difference(self.confidence_intervals)
        if missing_intervals:
            raise ValueError(f"gold confidence intervals are missing: {sorted(missing_intervals)}")
        for name, value in self.gold_metrics.items():
            if not math.isfinite(value):
                raise ValueError(f"gold metric {name} must be finite")
        precision = self.gold_metrics[self.precision_metric]
        precision_interval = self.confidence_intervals[self.precision_metric]
        if not 0.0 <= precision <= 1.0:
            raise ValueError("gold precision metric must be in [0, 1]")
        if not 0.0 <= precision_interval.lower <= precision_interval.upper <= 1.0:
            raise ValueError("gold precision confidence interval must be in [0, 1]")
        return self


class ArtifactDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    artifact_id: str = Field(min_length=1)
    field: str = Field(min_length=1)
    kind: Literal[
        "fasttext_language",
        "sklearn_text_classifier",
        "sklearn_multilabel_classifier",
        "ordinal_age_classifier",
        "lightgbm_active_quantiles",
        "distribution_residual_calibrator",
    ]
    relative_path: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    status: Literal["active", "candidate", "disabled"] = "active"
    training_source: str = Field(min_length=1)
    label_quality: Literal[
        "pretrained_public",
        "explicit_fact",
        "verified_manual",
        "high_precision_weak_supervision",
        "authorized_analytics",
        "synthetic_test_only",
    ]
    classes: tuple[str, ...] = ()
    metrics: dict[str, Any] = Field(default_factory=dict)
    gold_evaluation: GoldEvaluationDescriptor | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("relative_path")
    @classmethod
    def relative_artifact_path(cls, value: str) -> str:
        path = Path(value)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("artifact path must be relative and cannot traverse parents")
        return path.as_posix()


class FieldDecisionPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    version: str = Field(min_length=1)
    complete_min_probability: float = Field(default=0.0, ge=0.0, le=1.0)
    evidence_min_probability: float = Field(default=1.0, ge=0.0, le=1.0)
    evidence_min_margin: float = Field(default=0.0, ge=0.0, le=1.0)
    minimum_input_count: int = Field(default=0, ge=0)
    target_precision: float | None = Field(default=None, ge=0.0, le=1.0)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ModelBundleManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[MODEL_BUNDLE_SCHEMA_VERSION] = MODEL_BUNDLE_SCHEMA_VERSION
    bundle_version: str = Field(min_length=1)
    created_at: datetime
    feature_schema_version: str = FEATURE_SCHEMA_VERSION
    taxonomy_version: str = TAXONOMY_VERSION
    output_label_language: Literal["English"] = "English"
    compatible_processor_major: int = Field(default=1, ge=1)
    production_eligible: bool = False
    training_cutoff: datetime | None = None
    artifacts: tuple[ArtifactDescriptor, ...] = ()
    field_status: dict[str, str] = Field(default_factory=dict)
    decision_policies: dict[str, FieldDecisionPolicy] = Field(default_factory=dict)
    dependency_manifest: dict[str, tuple[str, ...]] = Field(default_factory=dict)
    build_metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("created_at", "training_cutoff")
    @classmethod
    def timezone_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("bundle timestamps must be timezone-aware")
        return value.astimezone(timezone.utc) if value else None

    @model_validator(mode="after")
    def unique_artifacts(self) -> "ModelBundleManifest":
        ids = [artifact.artifact_id for artifact in self.artifacts]
        if len(ids) != len(set(ids)):
            raise ValueError("artifact_id values must be unique")
        return self


@dataclass(frozen=True)
class ModelPrediction:
    value: Any
    probability: float
    margin: float
    candidates: tuple[dict[str, Any], ...]
    model_version: str
    metadata: dict[str, Any]


class ModelBundleSource(Protocol):
    def load(self) -> "ModelBundle":
        ...


def _temperature_scale(probabilities: list[float], temperature: float) -> list[float]:
    clean = [max(1e-12, min(1.0, float(value))) for value in probabilities]
    if not clean:
        return []
    temperature = max(0.05, float(temperature))
    logits = [math.log(value) / temperature for value in clean]
    maximum = max(logits)
    exponentials = [math.exp(value - maximum) for value in logits]
    total = sum(exponentials)
    return [value / total for value in exponentials]


class ModelBundle:
    """Validated immutable artifact bundle with lazy model loading."""

    def __init__(self, root: Path | None, manifest: ModelBundleManifest) -> None:
        self.root = root
        self.manifest = manifest
        self._descriptors = MappingProxyType({item.artifact_id: item for item in manifest.artifacts})
        self._loaded: dict[str, Any] = {}

    @classmethod
    def load(cls, manifest_path: str | Path) -> "ModelBundle":
        path = Path(manifest_path).resolve()
        if not path.is_file():
            raise ModelBundleError(f"model bundle manifest not found: {path}")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            manifest = ModelBundleManifest.model_validate(payload)
        except Exception as error:
            raise ModelBundleError(f"invalid model bundle manifest: {error}") from error
        if manifest.feature_schema_version != FEATURE_SCHEMA_VERSION:
            raise ModelBundleError("model bundle feature schema is incompatible")
        if manifest.taxonomy_version != TAXONOMY_VERSION:
            raise ModelBundleError("model bundle taxonomy is incompatible")
        bundle = cls(path.parent, manifest)
        bundle.verify_artifacts()
        return bundle

    @classmethod
    def empty(cls) -> "ModelBundle":
        return cls(
            None,
            ModelBundleManifest(
                bundle_version="no-trained-artifacts",
                created_at=datetime(1970, 1, 1, tzinfo=timezone.utc),
                production_eligible=False,
                field_status={},
            ),
        )

    @property
    def version(self) -> str:
        return self.manifest.bundle_version

    @property
    def production_eligible(self) -> bool:
        return self.manifest.production_eligible

    @property
    def content_hash(self) -> str:
        return sha256_json(self.manifest.model_dump(mode="json"))

    def descriptor(self, artifact_id: str) -> ArtifactDescriptor | None:
        descriptor = self._descriptors.get(artifact_id)
        return descriptor if descriptor and descriptor.status == "active" else None

    def decision_policy(self, field: str) -> FieldDecisionPolicy | None:
        return self.manifest.decision_policies.get(field)

    def _path(self, descriptor: ArtifactDescriptor) -> Path:
        if self.root is None:
            raise ModelBundleError("in-memory empty bundle has no artifacts")
        root = self.root.resolve()
        path = (root / descriptor.relative_path).resolve()
        try:
            path.relative_to(root)
        except ValueError as error:
            raise ModelBundleError("artifact path escapes bundle root") from error
        return path

    def verify_artifacts(self) -> None:
        for descriptor in self.manifest.artifacts:
            if descriptor.status == "disabled":
                continue
            path = self._path(descriptor)
            if not path.is_file():
                raise ModelBundleError(f"artifact missing: {descriptor.artifact_id}")
            if file_sha256(path) != descriptor.sha256:
                raise ModelBundleError(f"artifact hash mismatch: {descriptor.artifact_id}")

    def _load(self, artifact_id: str) -> tuple[ArtifactDescriptor, Any]:
        descriptor = self.descriptor(artifact_id)
        if descriptor is None:
            raise ModelBundleError(f"active artifact is unavailable: {artifact_id}")
        if artifact_id in self._loaded:
            return descriptor, self._loaded[artifact_id]
        path = self._path(descriptor)
        if descriptor.kind == "fasttext_language":
            loaded = FastTextLanguageIdentifier(path, expected_sha256=descriptor.sha256)
        else:
            try:
                import joblib
            except ImportError as error:
                raise ModelBundleError("joblib is required for trained artifacts") from error
            loaded = joblib.load(path)
        self._loaded[artifact_id] = loaded
        return descriptor, loaded

    def language_identifier(self) -> FastTextLanguageIdentifier | None:
        if self.descriptor("creator_language.fasttext") is None:
            return None
        _, identifier = self._load("creator_language.fasttext")
        return identifier

    def predict_text(self, artifact_id: str, text: str, *, top_k: int = 5) -> ModelPrediction | None:
        descriptor = self.descriptor(artifact_id)
        if descriptor is None or not text.strip():
            return None
        descriptor, model = self._load(artifact_id)
        try:
            raw = model.predict_proba([text])[0]
        except Exception as error:
            raise ModelBundleError(f"text artifact prediction failed: {artifact_id}: {error}") from error
        classes = list(descriptor.classes) or [str(value) for value in model.classes_]
        if len(classes) != len(raw):
            raise ModelBundleError(f"class/probability mismatch: {artifact_id}")
        temperature = float(descriptor.metadata.get("temperature", 1.0))
        probabilities = _temperature_scale([float(value) for value in raw], temperature)
        ranked = sorted(zip(classes, probabilities), key=lambda item: (-item[1], item[0]))
        top_value, top_probability = ranked[0]
        runner_up = ranked[1][1] if len(ranked) > 1 else 0.0
        return ModelPrediction(
            value=top_value,
            probability=top_probability,
            margin=top_probability - runner_up,
            candidates=tuple(
                {"value": value, "probability": round(probability, 6)}
                for value, probability in ranked[:top_k]
            ),
            model_version=f"{self.version}:{artifact_id}",
            metadata={"artifact_id": artifact_id, "label_quality": descriptor.label_quality},
        )

    def predict_multilabel(self, artifact_id: str, text: str, *, top_k: int = 10) -> ModelPrediction | None:
        descriptor = self.descriptor(artifact_id)
        if descriptor is None or not text.strip():
            return None
        descriptor, model = self._load(artifact_id)
        try:
            raw = model.predict_proba([text])[0]
        except Exception as error:
            raise ModelBundleError(f"multilabel prediction failed: {artifact_id}: {error}") from error
        classes = list(descriptor.classes)
        if len(classes) != len(raw):
            raise ModelBundleError(f"multilabel class/probability mismatch: {artifact_id}")
        ranked = sorted(zip(classes, (float(value) for value in raw)), key=lambda item: (-item[1], item[0]))
        selected = ranked[:top_k]
        top_probability = selected[0][1] if selected else 0.0
        runner_up = selected[1][1] if len(selected) > 1 else 0.0
        return ModelPrediction(
            value=[value for value, _ in selected],
            probability=top_probability,
            margin=top_probability - runner_up,
            candidates=tuple(
                {"value": value, "probability": round(probability, 6)}
                for value, probability in selected
            ),
            model_version=f"{self.version}:{artifact_id}",
            metadata={"artifact_id": artifact_id, "label_quality": descriptor.label_quality},
        )


class LocalModelBundleSource:
    def __init__(self, manifest_path: str | Path) -> None:
        self.manifest_path = Path(manifest_path)

    def load(self) -> ModelBundle:
        return ModelBundle.load(self.manifest_path)


class InMemoryModelBundleSource:
    def __init__(self, bundle: ModelBundle) -> None:
        self.bundle = bundle

    def load(self) -> ModelBundle:
        return self.bundle
