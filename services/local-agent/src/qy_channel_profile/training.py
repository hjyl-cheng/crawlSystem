from __future__ import annotations

import json
import logging
import math
import shutil
import uuid
import gc
import hashlib
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .analyzers import category_marker_values
from .errors import TrainingDataError
from .feature_store import FEATURE_STORE_SCHEMA_VERSION
from .features import FEATURE_SCHEMA_VERSION
from .language_id import file_sha256
from .ml_models import (
    AGE_BUCKETS,
    OrdinalTextAgeClassifier,
    PhraseMaskingTransformer,
    age_bucket_index,
)
from .model_bundle import (
    ArtifactDescriptor,
    FieldDecisionPolicy,
    ModelBundleManifest,
)
from .io import read_jsonl
from .taxonomy import TAXONOMY_VERSION


TRAINER_VERSION = "agent-free-model-builder-v5-residual-category"
CATEGORY_TEXT_TRANSFORM_VERSION = "category-known-marker-mask-v1"
LOGGER = logging.getLogger(__name__)


class ModelBuildPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    feature_store_path: Path
    output_directory: Path
    bundle_version: str = Field(min_length=1, pattern=r"^[A-Za-z0-9._-]+$")
    language_model_path: Path
    random_seed: int = 20260809
    minimum_total_samples: int = Field(default=200, ge=20)
    minimum_class_samples: int = Field(default=20, ge=2)
    minimum_country_classes: int = Field(default=5, ge=2)
    minimum_multilabel_samples: int = Field(default=20, ge=2)
    minimum_age_samples: int = Field(default=200, ge=20)
    target_evidence_precision: float = Field(default=0.85, ge=0.5, le=1.0)
    maximum_topic_text_characters: int = Field(default=12000, ge=1000, le=60000)
    maximum_identity_text_characters: int = Field(default=16000, ge=1000, le=60000)
    analytics_labels_path: Path | None = None
    holdout_snapshot_path: Path | None = None

    @field_validator(
        "feature_store_path",
        "output_directory",
        "language_model_path",
        "analytics_labels_path",
        "holdout_snapshot_path",
    )
    @classmethod
    def absolute_paths(cls, value: Path | None) -> Path | None:
        return value.expanduser().resolve() if value is not None else None


class ModelBuildResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    bundle_path: Path
    manifest_path: Path
    manifest: ModelBundleManifest


def _text_transformer(*, include_character_features: bool = True) -> Any:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.pipeline import FeatureUnion

    word = TfidfVectorizer(
        analyzer="word",
        ngram_range=(1, 2),
        min_df=2,
        max_df=0.995,
        max_features=40000,
        sublinear_tf=True,
        strip_accents=None,
    )
    if not include_character_features:
        return word
    return FeatureUnion([
        ("word", word),
        ("char", TfidfVectorizer(
            analyzer="char_wb",
            ngram_range=(3, 5),
            min_df=2,
            max_features=40000,
            sublinear_tf=True,
            strip_accents=None,
        )),
    ])


def _classifier(seed: int) -> Any:
    from sklearn.linear_model import SGDClassifier

    return SGDClassifier(
        loss="log_loss",
        penalty="l2",
        alpha=2e-5,
        max_iter=2000,
        tol=1e-4,
        class_weight="balanced",
        random_state=seed,
        average=True,
    )


def _text_pipeline(
    seed: int,
    *,
    include_character_features: bool = True,
    text_transformer: Any | None = None,
) -> Any:
    from sklearn.pipeline import Pipeline

    steps: list[tuple[str, Any]] = []
    if text_transformer is not None:
        steps.append(("text_policy", text_transformer))
    steps.extend([
        ("features", _text_transformer(include_character_features=include_character_features)),
        ("classifier", _classifier(seed)),
    ])
    return Pipeline(steps)


def _category_text_transformer() -> PhraseMaskingTransformer:
    return PhraseMaskingTransformer(
        category_marker_values(),
        version=CATEGORY_TEXT_TRANSFORM_VERSION,
    )


def _validation_split(
    rows: Sequence[dict[str, Any]],
) -> tuple[list[int], list[int], list[int], str]:
    if len(rows) < 20:
        raise TrainingDataError("at least 20 labeled rows are required for a validation split")
    timestamps = {str(row.get("as_of") or "") for row in rows}
    if len(timestamps) >= 3:
        order = sorted(
            range(len(rows)),
            key=lambda index: (
                str(rows[index].get("as_of") or ""),
                str(rows[index].get("channel_id") or ""),
            ),
        )
        strategy = "channel_unique_temporal_70_15_15"
    else:
        order = sorted(
            range(len(rows)),
            key=lambda index: (
                hashlib.sha256(
                    str(rows[index].get("channel_id") or "").encode("utf-8")
                ).digest(),
                str(rows[index].get("channel_id") or ""),
            ),
        )
        strategy = "channel_unique_hash_70_15_15"
    train_end = max(1, int(len(order) * 0.70))
    validation_end = max(train_end + 1, int(len(order) * 0.85))
    validation_end = min(validation_end, len(order) - 1)
    return order[:train_end], order[train_end:validation_end], order[validation_end:], strategy


def _select(rows: Sequence[dict[str, Any]], indices: Sequence[int], key: str) -> list[Any]:
    return [rows[index][key] for index in indices]


def _scale_probabilities(probabilities: np.ndarray, temperature: float) -> np.ndarray:
    values = np.clip(np.asarray(probabilities, dtype=float), 1e-12, 1.0)
    logits = np.log(values) / max(0.05, float(temperature))
    logits -= logits.max(axis=1, keepdims=True)
    exponentials = np.exp(logits)
    return exponentials / exponentials.sum(axis=1, keepdims=True)


def _fit_temperature(probabilities: np.ndarray, labels: Sequence[str], classes: Sequence[str]) -> float:
    class_index = {value: index for index, value in enumerate(classes)}
    targets = np.asarray([class_index[value] for value in labels], dtype=int)
    best = (float("inf"), 1.0)
    for temperature in np.linspace(0.5, 3.0, 51):
        scaled = _scale_probabilities(probabilities, float(temperature))
        loss = -float(np.mean(np.log(np.clip(scaled[np.arange(len(targets)), targets], 1e-12, 1.0))))
        if loss < best[0]:
            best = (loss, float(temperature))
    return best[1]


def _evidence_threshold(
    probabilities: np.ndarray,
    labels: Sequence[str],
    classes: Sequence[str],
    target_precision: float,
) -> tuple[float, float, float]:
    predicted_indices = probabilities.argmax(axis=1)
    confidence = probabilities.max(axis=1)
    predicted = [classes[index] for index in predicted_indices]
    correct = np.asarray([prediction == label for prediction, label in zip(predicted, labels)])
    candidates = sorted(set(float(value) for value in confidence), reverse=True)
    selected = (1.0, 0.0, 0.0)
    for threshold in reversed(candidates):
        mask = confidence >= threshold
        if mask.sum() < max(5, round(len(labels) * 0.05)):
            continue
        precision = float(correct[mask].mean())
        coverage = float(mask.mean())
        if precision >= target_precision and coverage >= selected[2]:
            selected = (threshold, precision, coverage)
    if selected[2] == 0:
        threshold = float(np.quantile(confidence, 0.8))
        mask = confidence >= threshold
        return threshold, float(correct[mask].mean()) if mask.any() else 0.0, float(mask.mean())
    return selected


def _single_label_metrics(labels: Sequence[str], probabilities: np.ndarray, classes: Sequence[str]) -> dict[str, Any]:
    from sklearn.metrics import accuracy_score, cohen_kappa_score, f1_score, log_loss

    predicted = [classes[index] for index in probabilities.argmax(axis=1)]
    return {
        "rows": len(labels),
        "accuracy": round(float(accuracy_score(labels, predicted)), 6),
        "macro_f1": round(float(f1_score(labels, predicted, average="macro", zero_division=0)), 6),
        "cohen_kappa": round(float(cohen_kappa_score(labels, predicted)), 6),
        "log_loss": round(float(log_loss(labels, probabilities, labels=list(classes))), 6),
    }


def _prepare_single_label_rows(
    frame: Any,
    label_column: str,
    *,
    minimum_total: int,
    minimum_class: int,
    minimum_classes: int,
    text_columns: Sequence[str],
    text_builder: Callable[[dict[str, Any]], str],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    labels = [value for value in frame[label_column].to_list() if value is not None]
    counts = Counter(str(value) for value in labels)
    allowed = {label for label, count in counts.items() if count >= minimum_class}
    eligible_count = sum(counts[label] for label in allowed)
    if eligible_count < minimum_total or len(allowed) < minimum_classes:
        raise TrainingDataError(
            f"{label_column} has insufficient eligible labels: rows={eligible_count}, "
            f"classes={len(allowed)}, required_classes={minimum_classes}"
        )
    records = frame.select(["channel_id", "as_of", *text_columns, label_column]).to_dicts()
    rows: list[dict[str, Any]] = []
    for row in records:
        if row.get(label_column) is None or str(row[label_column]) not in allowed:
            continue
        text = text_builder(row)
        if text.strip():
            rows.append({
                "channel_id": row["channel_id"],
                "as_of": row["as_of"],
                "text": text,
                "label": str(row[label_column]),
            })
    if len(rows) < minimum_total:
        raise TrainingDataError(f"{label_column} has only {len(rows)} eligible rows with usable text")
    return rows, dict(counts)


def _train_single_label(
    frame: Any,
    *,
    field: str,
    artifact_id: str,
    label_column: str,
    label_quality: str,
    training_source: str,
    output_path: Path,
    plan: ModelBuildPlan,
    text_builder: Callable[[dict[str, Any]], str],
    minimum_classes: int = 2,
    text_columns: Sequence[str] = ("channel_text", "content_text", "full_text"),
    include_character_features: bool = True,
    text_transformer_factory: Callable[[], Any] | None = None,
    text_transform_version: str | None = None,
) -> tuple[ArtifactDescriptor, FieldDecisionPolicy]:
    import joblib

    rows, label_counts = _prepare_single_label_rows(
        frame,
        label_column,
        minimum_total=plan.minimum_total_samples,
        minimum_class=plan.minimum_class_samples,
        minimum_classes=minimum_classes,
        text_columns=text_columns,
        text_builder=text_builder,
    )
    train, validation, test, split_strategy = _validation_split(rows)
    train_classes = set(_select(rows, train, "label"))
    if len(train_classes) < 2:
        raise TrainingDataError(f"{field} temporal train split has fewer than two classes")
    validation = [index for index in validation if rows[index]["label"] in train_classes]
    test = [index for index in test if rows[index]["label"] in train_classes]
    if not validation or not test:
        raise TrainingDataError(f"{field} temporal validation/test split is empty after class filtering")

    candidate = _text_pipeline(
        plan.random_seed,
        include_character_features=include_character_features,
        text_transformer=text_transformer_factory() if text_transformer_factory else None,
    )
    candidate.fit(_select(rows, train, "text"), _select(rows, train, "label"))
    classes = [str(value) for value in candidate.classes_]
    validation_raw = candidate.predict_proba(_select(rows, validation, "text"))
    temperature = _fit_temperature(validation_raw, _select(rows, validation, "label"), classes)
    validation_probability = _scale_probabilities(validation_raw, temperature)
    threshold, precision, coverage = _evidence_threshold(
        validation_probability,
        _select(rows, validation, "label"),
        classes,
        plan.target_evidence_precision,
    )

    del candidate, validation_raw, validation_probability
    gc.collect()

    evaluation_indices = [*train, *validation]
    evaluation_model = _text_pipeline(
        plan.random_seed,
        include_character_features=include_character_features,
        text_transformer=text_transformer_factory() if text_transformer_factory else None,
    )
    evaluation_model.fit(
        _select(rows, evaluation_indices, "text"),
        _select(rows, evaluation_indices, "label"),
    )
    evaluation_classes = [str(value) for value in evaluation_model.classes_]
    test = [index for index in test if rows[index]["label"] in set(evaluation_classes)]
    test_raw = evaluation_model.predict_proba(_select(rows, test, "text"))
    test_probability = _scale_probabilities(test_raw, temperature)
    metrics = _single_label_metrics(
        _select(rows, test, "label"),
        test_probability,
        evaluation_classes,
    )
    metrics.update({
        "split": split_strategy,
        "train_rows": len(train),
        "validation_rows": len(validation),
        "test_rows": len(test),
        "evaluation_fit_rows": len(evaluation_indices),
        "production_refit_rows": len(rows),
        "validation_evidence_precision": round(precision, 6),
        "validation_evidence_coverage": round(coverage, 6),
        "label_counts": label_counts,
    })
    del evaluation_model, test_raw, test_probability
    gc.collect()

    final_model = _text_pipeline(
        plan.random_seed,
        include_character_features=include_character_features,
        text_transformer=text_transformer_factory() if text_transformer_factory else None,
    )
    final_model.fit(
        _select(rows, range(len(rows)), "text"),
        _select(rows, range(len(rows)), "label"),
    )
    final_classes = [str(value) for value in final_model.classes_]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(final_model, output_path, compress=3)
    descriptor = ArtifactDescriptor(
        artifact_id=artifact_id,
        field=field,
        kind="sklearn_text_classifier",
        relative_path=output_path.name,
        sha256=file_sha256(output_path),
        training_source=training_source,
        label_quality=label_quality,
        classes=tuple(final_classes),
        metrics=metrics,
        metadata={
            "temperature": temperature,
            "trainer_version": TRAINER_VERSION,
            "character_features": include_character_features,
            "text_transform_version": text_transform_version,
        },
    )
    selective_complete = label_quality == "high_precision_weak_supervision"
    policy = FieldDecisionPolicy(
        version=(
            f"{field}-decision-v2-selective-weak-supervision"
            if selective_complete
            else f"{field}-decision-v1"
        ),
        complete_min_probability=round(threshold, 6) if selective_complete else 0.0,
        evidence_min_probability=round(threshold, 6),
        evidence_min_margin=0.05,
        minimum_input_count=1,
        target_precision=plan.target_evidence_precision,
        metadata={
            "validation_precision": round(precision, 6),
            "validation_coverage": round(coverage, 6),
            "complete_mode_uses_validation_threshold": selective_complete,
        },
    )
    return descriptor, policy


def _parse_labels(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return [str(item) for item in parsed if str(item).strip()] if isinstance(parsed, list) else []
    return []


def _multilabel_selection(probabilities: np.ndarray, thresholds: Sequence[float] | float) -> np.ndarray:
    threshold_values = np.asarray(thresholds, dtype=float)
    return probabilities >= threshold_values


def _per_label_thresholds(
    probabilities: np.ndarray,
    target: np.ndarray,
    classes: Sequence[str],
    target_precision: float,
) -> tuple[dict[str, float], dict[str, dict[str, float | int]]]:
    thresholds: dict[str, float] = {}
    diagnostics: dict[str, dict[str, float | int]] = {}
    minimum_predictions = max(5, int(round(probabilities.shape[0] * 0.005)))
    for index, label in enumerate(classes):
        scores = probabilities[:, index]
        truth = target[:, index].astype(bool)
        selected_threshold: float | None = None
        selected_precision = 0.0
        selected_count = 0
        for threshold in sorted(set(float(value) for value in scores)):
            selected = scores >= threshold
            count = int(selected.sum())
            if count < minimum_predictions:
                continue
            precision = float(np.logical_and(selected, truth).sum() / count)
            if precision >= target_precision:
                selected_threshold = threshold
                selected_precision = precision
                selected_count = count
                break
        if selected_threshold is None:
            # 1.000001 deliberately disables an uncalibrated label. The rule
            # baseline still supplies complete-estimate output.
            selected_threshold = 1.000001
        thresholds[str(label)] = round(selected_threshold, 6)
        diagnostics[str(label)] = {
            "threshold": round(selected_threshold, 6),
            "precision": round(selected_precision, 6),
            "selected": selected_count,
            "support": int(truth.sum()),
        }
    return thresholds, diagnostics


def _multilabel_metrics(
    target: np.ndarray,
    probabilities: np.ndarray,
    thresholds: Sequence[float] | float,
) -> dict[str, float]:
    predicted = _multilabel_selection(probabilities, thresholds)
    precisions: list[float] = []
    recalls: list[float] = []
    jaccards: list[float] = []
    for truth, scores, selected in zip(target.astype(bool), probabilities, predicted):
        top5 = np.argsort(scores)[::-1][:5]
        top10 = np.argsort(scores)[::-1][:10]
        precisions.append(float(truth[top5].sum()) / max(1, len(top5)))
        recalls.append(float(truth[top10].sum()) / max(1, int(truth.sum())))
        union = np.logical_or(truth, selected).sum()
        jaccards.append(float(np.logical_and(truth, selected).sum()) / max(1, int(union)))
    return {
        "precision_at_5": round(float(np.mean(precisions)), 6),
        "recall_at_10": round(float(np.mean(recalls)), 6),
        "mean_jaccard": round(float(np.mean(jaccards)), 6),
    }


def _train_multilabel(
    frame: Any,
    *,
    field: str,
    artifact_id: str,
    label_column: str,
    output_path: Path,
    plan: ModelBuildPlan,
    text_column: str = "full_text",
    text_transformer_factory: Callable[[], Any] | None = None,
    text_transform_version: str | None = None,
) -> tuple[ArtifactDescriptor, FieldDecisionPolicy]:
    import joblib
    from sklearn.multiclass import OneVsRestClassifier
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import MultiLabelBinarizer

    records = frame.select(["channel_id", "as_of", text_column, label_column]).to_dicts()
    rows = [
        {
            "channel_id": row["channel_id"],
            "as_of": row["as_of"],
            "text": str(row[text_column] or "")[:plan.maximum_topic_text_characters],
            "labels": _parse_labels(row[label_column]),
        }
        for row in records
        if _parse_labels(row[label_column]) and str(row[text_column] or "").strip()
    ]
    counts = Counter(label for row in rows for label in row["labels"])
    allowed = {label for label, count in counts.items() if count >= plan.minimum_multilabel_samples}
    for row in rows:
        row["labels"] = [label for label in row["labels"] if label in allowed]
    rows = [row for row in rows if row["labels"]]
    if len(rows) < plan.minimum_total_samples or len(allowed) < 2:
        raise TrainingDataError(
            f"{field} has insufficient multilabel data: rows={len(rows)}, labels={len(allowed)}"
        )
    train, validation, test, split_strategy = _validation_split(rows)
    train_labels = {label for index in train for label in rows[index]["labels"]}
    rows = [
        {**row, "labels": [label for label in row["labels"] if label in train_labels]}
        for row in rows
    ]
    validation = [index for index in validation if rows[index]["labels"]]
    test = [index for index in test if rows[index]["labels"]]
    if not validation or not test:
        raise TrainingDataError(f"{field} temporal validation/test split has no known labels")
    binarizer = MultiLabelBinarizer(classes=sorted(train_labels))
    binarizer.fit([[]])

    def create_model() -> Any:
        steps: list[tuple[str, Any]] = []
        if text_transformer_factory is not None:
            steps.append(("text_policy", text_transformer_factory()))
        steps.extend([
            ("features", _text_transformer(include_character_features=False)),
            ("classifier", OneVsRestClassifier(_classifier(plan.random_seed), n_jobs=1)),
        ])
        return Pipeline(steps)

    candidate = create_model()
    candidate.fit(_select(rows, train, "text"), binarizer.transform(_select(rows, train, "labels")))
    validation_probability = np.asarray(candidate.predict_proba(_select(rows, validation, "text")), dtype=float)
    validation_target = binarizer.transform(_select(rows, validation, "labels")).astype(bool)
    classes = tuple(str(value) for value in binarizer.classes_)
    thresholds, threshold_diagnostics = _per_label_thresholds(
        validation_probability,
        validation_target,
        classes,
        plan.target_evidence_precision,
    )
    threshold_vector = np.asarray([thresholds[label] for label in classes], dtype=float)
    selected = _multilabel_selection(validation_probability, threshold_vector)
    validation_precision = float(np.logical_and(selected, validation_target).sum() / max(1, selected.sum()))
    validation_coverage = float(np.mean(selected.any(axis=1)))

    del candidate, validation_probability, validation_target, selected
    gc.collect()

    evaluation_indices = [*train, *validation]
    evaluation_model = create_model()
    evaluation_model.fit(
        _select(rows, evaluation_indices, "text"),
        binarizer.transform(_select(rows, evaluation_indices, "labels")),
    )
    test_probability = np.asarray(evaluation_model.predict_proba(_select(rows, test, "text")), dtype=float)
    test_target = binarizer.transform(_select(rows, test, "labels"))
    metrics = {
        "rows": len(rows),
        "train_rows": len(train),
        "validation_rows": len(validation),
        "test_rows": len(test),
        "evaluation_fit_rows": len(evaluation_indices),
        "production_refit_rows": len(rows),
        "split": split_strategy,
        "validation_micro_precision": round(validation_precision, 6),
        "validation_channel_coverage": round(validation_coverage, 6),
        "enabled_label_count": sum(value <= 1.0 for value in thresholds.values()),
        "label_counts": dict(counts),
        **_multilabel_metrics(test_target, test_probability, threshold_vector),
    }
    del evaluation_model, test_probability, test_target
    gc.collect()

    final_model = create_model()
    final_model.fit(
        _select(rows, range(len(rows)), "text"),
        binarizer.transform(_select(rows, range(len(rows)), "labels")),
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(final_model, output_path, compress=3)
    enabled_thresholds = [value for value in thresholds.values() if value <= 1.0]
    field_threshold = min(enabled_thresholds, default=1.0)
    descriptor = ArtifactDescriptor(
        artifact_id=artifact_id,
        field=field,
        kind="sklearn_multilabel_classifier",
        relative_path=output_path.name,
        sha256=file_sha256(output_path),
        training_source="high-precision rules from crawler text; no Agent labels",
        label_quality="high_precision_weak_supervision",
        classes=classes,
        status="active" if validation_precision >= plan.target_evidence_precision else "candidate",
        metrics=metrics,
        metadata={
            "threshold": field_threshold,
            "thresholds": thresholds,
            "threshold_diagnostics": threshold_diagnostics,
            "trainer_version": TRAINER_VERSION,
            "character_features": False,
            "maximum_text_characters": plan.maximum_topic_text_characters,
            "text_transform_version": text_transform_version,
        },
    )
    return descriptor, FieldDecisionPolicy(
        version=f"{field}-decision-v1",
        complete_min_probability=0.0,
        evidence_min_probability=round(field_threshold, 6),
        evidence_min_margin=0.0,
        minimum_input_count=1,
        target_precision=plan.target_evidence_precision,
        metadata={
            "validation_precision": round(validation_precision, 6),
            "validation_coverage": round(validation_coverage, 6),
        },
    )


def _train_age(
    frame: Any,
    output_path: Path,
    plan: ModelBuildPlan,
) -> tuple[ArtifactDescriptor, FieldDecisionPolicy]:
    import joblib
    from sklearn.metrics import accuracy_score, mean_absolute_error

    records = frame.select(["channel_id", "as_of", "channel_text", "age_label"]).to_dicts()
    rows = [
        {
            "channel_id": row["channel_id"],
            "as_of": row["as_of"],
            "text": str(row["channel_text"] or "")[:plan.maximum_identity_text_characters],
            "age": int(row["age_label"]),
        }
        for row in records
        if row.get("age_label") is not None and 13 <= int(row["age_label"]) <= 90
    ]
    if len(rows) < plan.minimum_age_samples:
        raise TrainingDataError(f"creator_age_range has only {len(rows)} explicit/strong labels")
    train, validation, test, split_strategy = _validation_split(rows)

    def create() -> OrdinalTextAgeClassifier:
        return OrdinalTextAgeClassifier(
            _text_transformer(include_character_features=True),
            lambda: _classifier(plan.random_seed),
        )

    candidate = create().fit(_select(rows, train, "text"), _select(rows, train, "age"))
    validation_probability = candidate.predict_proba(_select(rows, validation, "text"))
    validation_confidence = validation_probability.max(axis=1)
    validation_bucket = validation_probability.argmax(axis=1)
    actual_bucket = np.asarray([age_bucket_index(age) for age in _select(rows, validation, "age")])
    threshold, precision, coverage = _evidence_threshold(
        validation_probability,
        [AGE_BUCKETS[index] for index in actual_bucket],
        AGE_BUCKETS,
        plan.target_evidence_precision,
    )
    evaluation_indices = [*train, *validation]
    evaluation_model = create().fit(
        _select(rows, evaluation_indices, "text"),
        _select(rows, evaluation_indices, "age"),
    )
    test_probability = evaluation_model.predict_proba(_select(rows, test, "text"))
    predicted_bucket = test_probability.argmax(axis=1)
    actual_ages = _select(rows, test, "age")
    actual_buckets = [age_bucket_index(age) for age in actual_ages]
    centers = [15, 21, 29, 39, 49, 59, 70]
    predicted_ages = [centers[index] for index in predicted_bucket]
    metrics = {
        "rows": len(rows),
        "train_rows": len(train),
        "validation_rows": len(validation),
        "test_rows": len(test),
        "evaluation_fit_rows": len(evaluation_indices),
        "production_refit_rows": len(rows),
        "split": split_strategy,
        "bucket_accuracy": round(float(accuracy_score(actual_buckets, predicted_bucket)), 6),
        "compat_integer_mae": round(float(mean_absolute_error(actual_ages, predicted_ages)), 6),
        "validation_evidence_precision": round(precision, 6),
        "validation_evidence_coverage": round(coverage, 6),
        "validation_mean_confidence": round(float(validation_confidence.mean()), 6),
    }
    del candidate, evaluation_model, validation_probability, test_probability
    gc.collect()

    final_model = create().fit(
        _select(rows, range(len(rows)), "text"),
        _select(rows, range(len(rows)), "age"),
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(final_model, output_path, compress=3)
    return ArtifactDescriptor(
        artifact_id="creator_age.ordinal",
        field="creator_age_range",
        kind="ordinal_age_classifier",
        relative_path=output_path.name,
        sha256=file_sha256(output_path),
        training_source="explicit age or birth-year facts; no Agent labels",
        label_quality="explicit_fact",
        classes=AGE_BUCKETS,
        metrics=metrics,
        metadata={"trainer_version": TRAINER_VERSION},
    ), FieldDecisionPolicy(
        version="creator-age-decision-v1",
        evidence_min_probability=round(threshold, 6),
        evidence_min_margin=0.05,
        minimum_input_count=1,
        target_precision=plan.target_evidence_precision,
    )


class ChannelProfileModelBuilder:
    def build(self, plan: ModelBuildPlan) -> ModelBuildResult:
        try:
            import joblib  # noqa: F401
            import polars as pl
            import sklearn  # noqa: F401
        except ImportError as error:
            raise TrainingDataError("ML build dependencies are not installed") from error
        if not plan.feature_store_path.is_file():
            raise TrainingDataError(f"feature store not found: {plan.feature_store_path}")
        if not plan.language_model_path.is_file():
            raise TrainingDataError(f"language model not found: {plan.language_model_path}")
        feature_manifest_path = plan.feature_store_path.with_suffix(
            plan.feature_store_path.suffix + ".manifest.json"
        )
        if not feature_manifest_path.is_file():
            raise TrainingDataError("feature store manifest is required")
        feature_manifest = json.loads(feature_manifest_path.read_text(encoding="utf-8"))
        if feature_manifest.get("schema_version") != FEATURE_STORE_SCHEMA_VERSION:
            raise TrainingDataError("feature store manifest schema is incompatible")
        if feature_manifest.get("feature_schema_version") != FEATURE_SCHEMA_VERSION:
            raise TrainingDataError("feature store schema is incompatible")
        lineage_status = feature_manifest.get("lineage_status")
        if lineage_status not in {"complete", "legacy_snapshot_partial"}:
            raise TrainingDataError("feature store lineage status is required")
        lineage_rows = feature_manifest.get("source_rows_with_lineage_version")
        if not isinstance(lineage_rows, int) or lineage_rows < 0:
            raise TrainingDataError("feature store lineage row count is required")
        if feature_manifest.get("agent_reference_columns_present") is not False:
            raise TrainingDataError("independent feature store must explicitly exclude Agent reference columns")
        if not feature_manifest.get("prior_catalog_version") or not feature_manifest.get("prior_catalog_hash"):
            raise TrainingDataError("feature store prior catalog provenance is required")
        if file_sha256(plan.feature_store_path) != feature_manifest.get("output_sha256"):
            raise TrainingDataError("feature store hash does not match its manifest")

        frame = pl.read_parquet(plan.feature_store_path)
        if lineage_rows > frame.height:
            raise TrainingDataError("feature store lineage row count exceeds feature rows")
        if lineage_status == "complete" and lineage_rows != frame.height:
            raise TrainingDataError("complete feature store lineage must cover every row")
        if lineage_status == "legacy_snapshot_partial" and lineage_rows == frame.height:
            raise TrainingDataError("partial feature store lineage cannot cover every row")
        if any("agent" in column.casefold() for column in frame.columns):
            raise TrainingDataError("Agent-derived column detected in independent feature store")
        feature_store_rows = int(frame.height)
        LOGGER.info("loaded feature store: %s rows", feature_store_rows)
        holdout_ids: set[str] = set()
        if plan.holdout_snapshot_path is not None:
            if not plan.holdout_snapshot_path.is_file():
                raise TrainingDataError(f"holdout snapshot not found: {plan.holdout_snapshot_path}")
            for envelope in read_jsonl(plan.holdout_snapshot_path):
                snapshot = envelope.get("snapshot") if isinstance(envelope.get("snapshot"), dict) else envelope
                channel = snapshot.get("channel") if isinstance(snapshot, dict) else None
                channel_id = str((channel or {}).get("channel_id") or "").strip()
                if not channel_id:
                    raise TrainingDataError("holdout snapshot contains a row without channel_id")
                holdout_ids.add(channel_id)
            frame = frame.filter(~pl.col("channel_id").is_in(sorted(holdout_ids)))
            if frame.is_empty():
                raise TrainingDataError("holdout exclusion removed every feature-store row")
            LOGGER.info("excluded %s holdout channel IDs; %s training rows remain", len(holdout_ids), frame.height)
        target = plan.output_directory / plan.bundle_version
        if target.exists():
            raise TrainingDataError(f"immutable bundle path already exists: {target}")
        plan.output_directory.mkdir(parents=True, exist_ok=True)
        temporary = plan.output_directory / f".{plan.bundle_version}.tmp-{uuid.uuid4().hex}"
        temporary.mkdir(parents=True)
        artifacts: list[ArtifactDescriptor] = []
        policies: dict[str, FieldDecisionPolicy] = {}
        field_status = {
            "country": "fallback_only",
            "creator_gender": "fallback_only",
            "creator_age_range": "fallback_only",
            "creator_language": "pretrained_public_model",
            "audience_region": "uncalibrated_public_estimate",
            "audience_age_gender": "uncalibrated_public_estimate",
            "audience_language": "uncalibrated_public_estimate",
            "active_subscriber_ratio": "uncalibrated_public_proxy",
            "channel_tags": "fallback_only",
            "channel_categories": "fallback_only",
        }
        skipped: dict[str, str] = {}
        try:
            language_relative = Path("artifacts/lid.176.ftz")
            language_target = temporary / language_relative
            language_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(plan.language_model_path, language_target)
            artifacts.append(ArtifactDescriptor(
                artifact_id="creator_language.fasttext",
                field="creator_language",
                kind="fasttext_language",
                relative_path=language_relative.as_posix(),
                sha256=file_sha256(language_target),
                training_source="Meta FastText language identification lid.176.ftz",
                label_quality="pretrained_public",
                metadata={
                    "upstream_url": "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz",
                    "license": "CC-BY-SA-3.0",
                },
            ))
            LOGGER.info("packaged pretrained language model")

            single_specs = (
                (
                    "country", "country.text", "country_label", "explicit_fact",
                    "crawler explicit YouTube About country; profile text only; no Agent labels",
                    # Raw content can mention a country because it is the topic,
                    # not because the creator operates there. Keep the baseline
                    # identity model on profile text until a dedicated dialect
                    # feature extractor is independently validated.
                    lambda row: str(row["channel_text"] or "")[:plan.maximum_identity_text_characters],
                ),
                (
                    "channel_categories", "categories.level1", "category_level1_label",
                    "high_precision_weak_supervision",
                    "high-precision taxonomy rules with label triggers masked from model text; no Agent labels",
                    lambda row: str(row["category_text"] or "")[:plan.maximum_topic_text_characters],
                ),
            )
            for field, artifact_id, label_column, quality, source, text_builder in single_specs:
                relative = Path(f"artifacts/{artifact_id.replace('.', '_')}.joblib")
                try:
                    LOGGER.info("training %s", artifact_id)
                    descriptor, policy = _train_single_label(
                        frame,
                        field=field,
                        artifact_id=artifact_id,
                        label_column=label_column,
                        label_quality=quality,
                        training_source=source,
                        output_path=temporary / relative,
                        plan=plan,
                        text_builder=text_builder,
                        minimum_classes=plan.minimum_country_classes if field == "country" else 2,
                        text_columns=("channel_text",) if field == "country" else ("category_text",),
                        include_character_features=field == "country",
                        text_transformer_factory=(
                            _category_text_transformer
                            if field == "channel_categories"
                            else None
                        ),
                        text_transform_version=(
                            CATEGORY_TEXT_TRANSFORM_VERSION
                            if field == "channel_categories"
                            else None
                        ),
                    )
                    descriptor = descriptor.model_copy(update={"relative_path": relative.as_posix()})
                    artifacts.append(descriptor)
                    policies[field] = policy
                    field_status[field] = "trained_explicit" if quality == "explicit_fact" else "trained_weak_supervision"
                    LOGGER.info("trained %s: %s", artifact_id, descriptor.metrics)
                except TrainingDataError as error:
                    skipped[artifact_id] = str(error)
                    LOGGER.warning("skipped %s: %s", artifact_id, error)

            multilabel_specs = (
                ("channel_categories", "categories.level2", "category_level2_labels_json", "category_text"),
                ("channel_tags", "channel_tags.multilabel", "tag_labels_json", "tag_text"),
            )
            for field, artifact_id, label_column, text_column in multilabel_specs:
                relative = Path(f"artifacts/{artifact_id.replace('.', '_')}.joblib")
                try:
                    LOGGER.info("training %s", artifact_id)
                    descriptor, policy = _train_multilabel(
                        frame,
                        field=field,
                        artifact_id=artifact_id,
                        label_column=label_column,
                        output_path=temporary / relative,
                        plan=plan,
                        text_column=text_column,
                        text_transformer_factory=(
                            _category_text_transformer
                            if field == "channel_categories"
                            else None
                        ),
                        text_transform_version=(
                            CATEGORY_TEXT_TRANSFORM_VERSION
                            if field == "channel_categories"
                            else None
                        ),
                    )
                    descriptor = descriptor.model_copy(update={"relative_path": relative.as_posix()})
                    artifacts.append(descriptor)
                    # Level 2 has per-label thresholds in its descriptor. The
                    # shared category acceptance decision must retain the L1
                    # selective threshold instead of being overwritten here.
                    if field not in policies:
                        policies[field] = policy
                    field_status[field] = (
                        "trained_weak_supervision"
                        if descriptor.status == "active"
                        else "candidate_below_precision_gate"
                    )
                    LOGGER.info("trained %s: %s", artifact_id, descriptor.metrics)
                except TrainingDataError as error:
                    skipped[artifact_id] = str(error)
                    LOGGER.warning("skipped %s: %s", artifact_id, error)

            try:
                LOGGER.info("training gender.entity_type and gender.single_creator")
                entity_frame = frame.with_columns(
                    pl.when(pl.col("gender_label") == "brand_team")
                    .then(pl.lit("brand_or_team"))
                    .when(pl.col("gender_label").is_in(["male", "female"]))
                    .then(pl.lit("single_creator"))
                    .otherwise(None)
                    .alias("entity_type_label")
                )
                relative = Path("artifacts/gender_entity_type.joblib")
                descriptor, policy = _train_single_label(
                    entity_frame,
                    field="creator_gender",
                    artifact_id="gender.entity_type",
                    label_column="entity_type_label",
                    label_quality="explicit_fact",
                    training_source="explicit self-identification and strong team/entity rules; no Agent labels",
                    output_path=temporary / relative,
                    plan=plan,
                    text_builder=lambda row: str(row["identity_text"] or "")[:plan.maximum_identity_text_characters],
                    text_columns=("identity_text",),
                )
                artifacts.append(descriptor.model_copy(update={"relative_path": relative.as_posix()}))
                policies["creator_gender"] = policy
                relative = Path("artifacts/gender_single_creator.joblib")
                gender_frame = frame.filter(pl.col("gender_label").is_in(["male", "female"]))
                gender_descriptor, _ = _train_single_label(
                    gender_frame,
                    field="creator_gender",
                    artifact_id="gender.single_creator",
                    label_column="gender_label",
                    label_quality="explicit_fact",
                    training_source="explicit first-person gender evidence; no Agent labels",
                    output_path=temporary / relative,
                    plan=plan,
                    text_builder=lambda row: str(row["identity_text"] or "")[:plan.maximum_identity_text_characters],
                    text_columns=("identity_text",),
                )
                artifacts.append(gender_descriptor.model_copy(update={"relative_path": relative.as_posix()}))
                field_status["creator_gender"] = "trained_explicit_two_stage"
                LOGGER.info("trained two-stage creator gender models")
            except TrainingDataError as error:
                skipped["creator_gender.two_stage"] = str(error)
                LOGGER.warning("creator gender remains partially trained/fallback: %s", error)

            try:
                LOGGER.info("training creator_age.ordinal")
                relative = Path("artifacts/creator_age_ordinal.joblib")
                descriptor, policy = _train_age(frame, temporary / relative, plan)
                artifacts.append(descriptor.model_copy(update={"relative_path": relative.as_posix()}))
                policies["creator_age_range"] = policy
                field_status["creator_age_range"] = "trained_explicit_ordinal"
                LOGGER.info("trained creator_age.ordinal: %s", descriptor.metrics)
            except TrainingDataError as error:
                skipped["creator_age.ordinal"] = str(error)
                LOGGER.warning("skipped creator_age.ordinal: %s", error)

            # Analytics calibrators are deliberately activated only when a separate,
            # authorized label file is supplied. Agent references never enter this builder.
            if plan.analytics_labels_path is not None:
                skipped["analytics_calibration"] = (
                    "analytics label adapter is configured but calibration build is handled "
                    "by the dedicated analytics builder"
                )

            manifest = ModelBundleManifest(
                bundle_version=plan.bundle_version,
                created_at=datetime.now(timezone.utc),
                feature_schema_version=FEATURE_SCHEMA_VERSION,
                taxonomy_version=TAXONOMY_VERSION,
                compatible_processor_major=1,
                production_eligible=False,
                training_cutoff=max(
                    (datetime.fromisoformat(str(value).replace("Z", "+00:00")) for value in frame["as_of"]),
                    default=None,
                ),
                artifacts=tuple(artifacts),
                field_status=field_status,
                decision_policies=policies,
                dependency_manifest={
                    "profile_text_hash": (
                        "country", "creator_gender", "creator_age_range", "creator_language",
                        "channel_tags", "channel_categories", "audience_region",
                        "audience_age_gender", "audience_language",
                    ),
                    "content_text_hash": (
                        "creator_gender", "creator_age_range", "creator_language",
                        "channel_tags", "channel_categories", "audience_region",
                        "audience_age_gender", "audience_language",
                    ),
                    "channel_stats_hash": ("active_subscriber_ratio", "audience_age_gender"),
                    "content_stats_hash": ("active_subscriber_ratio", "channel_tags", "audience_age_gender"),
                    "comment_text_hash": (
                        "creator_gender", "creator_age_range", "channel_tags",
                        "audience_region", "audience_age_gender", "audience_language",
                    ),
                    "comment_stats_hash": (
                        "audience_region", "audience_age_gender", "audience_language",
                        "active_subscriber_ratio",
                    ),
                    "media_asset_hash": (),
                    "lineage_hash": (),
                },
                build_metadata={
                    "trainer_version": TRAINER_VERSION,
                    "feature_store_sha256": file_sha256(plan.feature_store_path),
                    "feature_store_rows": feature_store_rows,
                    "prior_catalog_version": feature_manifest["prior_catalog_version"],
                    "prior_catalog_hash": feature_manifest["prior_catalog_hash"],
                    "training_rows_after_holdout": int(frame.height),
                    "holdout_channel_count": len(holdout_ids),
                    "holdout_snapshot_path": str(plan.holdout_snapshot_path) if plan.holdout_snapshot_path else None,
                    "label_policy": feature_manifest.get("label_policy"),
                    "feature_store_lineage_status": feature_manifest.get("lineage_status"),
                    "feature_store_rows_with_lineage_version": feature_manifest.get(
                        "source_rows_with_lineage_version"
                    ),
                    "agent_reference_used_for_training": False,
                    "output_label_language": "English",
                    "skipped_artifacts": skipped,
                    "production_ineligibility_reasons": [
                        "audience distributions are not calibrated against Analytics truth",
                        "active subscriber ratio is not calibrated against subscribed-viewer Analytics",
                        "weak-supervision category/tag models require manual test-set validation",
                        "Top comments are a ranked sample and comment fusion weights are uncalibrated",
                    ],
                },
            )
            manifest_path = temporary / "manifest.json"
            manifest_path.write_text(
                json.dumps(manifest.model_dump(mode="json"), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            temporary.rename(target)
            LOGGER.info("completed immutable model bundle: %s", target)
            final_manifest = target / "manifest.json"
            return ModelBuildResult(bundle_path=target, manifest_path=final_manifest, manifest=manifest)
        except Exception:
            shutil.rmtree(temporary, ignore_errors=True)
            raise
