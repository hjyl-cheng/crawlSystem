from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Sequence

import numpy as np

from .text_features import normalize_text


AGE_BUCKETS = ("13-17", "18-24", "25-34", "35-44", "45-54", "55-64", "65+")
AGE_BUCKET_INTERVALS = (
    (13, 17), (18, 24), (25, 34), (35, 44), (45, 54), (55, 64), (65, 90),
)


class PhraseMaskingTransformer:
    """Sklearn-compatible text transform that removes known label triggers."""

    def __init__(self, phrases: Sequence[str], *, version: str) -> None:
        self.phrases = tuple(str(value) for value in phrases)
        self.version = str(version)
        self._pattern: re.Pattern[str] | None = None

    def get_params(self, deep: bool = True) -> dict[str, Any]:
        return {"phrases": self.phrases, "version": self.version}

    def set_params(self, **params: Any) -> "PhraseMaskingTransformer":
        for name, value in params.items():
            setattr(self, name, tuple(value) if name == "phrases" else value)
        self._pattern = None
        return self

    def fit(self, values: Sequence[str], labels: Sequence[Any] | None = None) -> "PhraseMaskingTransformer":
        self._compile()
        return self

    def transform(self, values: Sequence[str]) -> list[str]:
        pattern = self._compile()
        return [
            normalize_text(pattern.sub(" ", normalize_text(value)))
            for value in values
        ]

    def fit_transform(
        self,
        values: Sequence[str],
        labels: Sequence[Any] | None = None,
        **fit_params: Any,
    ) -> list[str]:
        return self.fit(values, labels).transform(values)

    def _compile(self) -> re.Pattern[str]:
        if self._pattern is None:
            normalized = sorted(
                {normalize_text(value) for value in self.phrases if normalize_text(value)},
                key=lambda value: (-len(value), value),
            )
            if not normalized:
                raise ValueError("phrase masker requires at least one non-empty phrase")
            alternatives = "|".join(re.escape(value) for value in normalized)
            self._pattern = re.compile(rf"(?<!\w)(?:{alternatives})(?!\w)")
        return self._pattern


def age_bucket_index(age: int) -> int:
    for index, (lower, upper) in enumerate(AGE_BUCKET_INTERVALS):
        if lower <= int(age) <= upper:
            return index
    raise ValueError(f"age is outside supported interval: {age}")


class OrdinalTextAgeClassifier:
    """Cumulative-link text classifier with monotonic probability repair."""

    def __init__(self, vectorizer: Any, classifier_factory: Any) -> None:
        self.vectorizer = vectorizer
        self.classifier_factory = classifier_factory
        self.models: list[Any | None] = []
        self.constants: list[float | None] = []
        self.classes_ = np.asarray(AGE_BUCKETS)

    def fit(self, texts: Sequence[str], ages: Sequence[int]) -> "OrdinalTextAgeClassifier":
        if len(texts) != len(ages) or not texts:
            raise ValueError("ordinal age training data is empty or misaligned")
        matrix = self.vectorizer.fit_transform(texts)
        buckets = np.asarray([age_bucket_index(int(age)) for age in ages], dtype=int)
        self.models = []
        self.constants = []
        for boundary in range(len(AGE_BUCKETS) - 1):
            target = (buckets > boundary).astype(int)
            if len(set(target.tolist())) < 2:
                self.models.append(None)
                self.constants.append(float(target[0]))
                continue
            model = self.classifier_factory()
            model.fit(matrix, target)
            self.models.append(model)
            self.constants.append(None)
        self.classifier_factory = None
        return self

    def predict_proba(self, texts: Sequence[str]) -> np.ndarray:
        matrix = self.vectorizer.transform(texts)
        cumulative: list[np.ndarray] = []
        for model, constant in zip(self.models, self.constants):
            if model is None:
                cumulative.append(np.full(matrix.shape[0], float(constant)))
            else:
                class_index = list(model.classes_).index(1)
                cumulative.append(model.predict_proba(matrix)[:, class_index])
        if not cumulative:
            raise ValueError("ordinal age classifier is not fitted")
        q = np.column_stack(cumulative)
        q = np.minimum.accumulate(q, axis=1)
        probabilities = np.empty((q.shape[0], len(AGE_BUCKETS)), dtype=float)
        probabilities[:, 0] = 1.0 - q[:, 0]
        for index in range(1, len(AGE_BUCKETS) - 1):
            probabilities[:, index] = q[:, index - 1] - q[:, index]
        probabilities[:, -1] = q[:, -1]
        probabilities = np.clip(probabilities, 0.0, 1.0)
        totals = probabilities.sum(axis=1, keepdims=True)
        return probabilities / np.where(totals > 0, totals, 1.0)


class DistributionResidualCalibrator:
    """Learns logit residuals over an existing public-signal distribution."""

    def __init__(self, estimator: Any, labels: Sequence[str], epsilon: float = 1e-6) -> None:
        self.estimator = estimator
        self.labels = tuple(labels)
        self.epsilon = float(epsilon)

    def fit(
        self,
        features: Sequence[Sequence[float]],
        public_probabilities: Sequence[Sequence[float]],
        targets: Sequence[Sequence[float]],
    ) -> "DistributionResidualCalibrator":
        public = self._normalized(public_probabilities)
        target = self._normalized(targets)
        residual = np.log(target + self.epsilon) - np.log(public + self.epsilon)
        residual -= residual.mean(axis=1, keepdims=True)
        self.estimator.fit(np.asarray(features, dtype=float), residual)
        return self

    def predict_proba(
        self,
        features: Sequence[Sequence[float]],
        public_probabilities: Sequence[Sequence[float]],
    ) -> np.ndarray:
        public = self._normalized(public_probabilities)
        residual = np.asarray(self.estimator.predict(np.asarray(features, dtype=float)), dtype=float)
        logits = np.log(public + self.epsilon) + residual
        logits -= logits.max(axis=1, keepdims=True)
        values = np.exp(logits)
        return values / values.sum(axis=1, keepdims=True)

    @staticmethod
    def _normalized(values: Sequence[Sequence[float]]) -> np.ndarray:
        array = np.clip(np.asarray(values, dtype=float), 0.0, None)
        if array.ndim != 2 or array.shape[1] == 0:
            raise ValueError("distribution matrix must be two-dimensional and non-empty")
        totals = array.sum(axis=1, keepdims=True)
        if np.any(totals <= 0):
            raise ValueError("distribution rows must have positive mass")
        return array / totals


@dataclass
class ActiveAudienceQuantileModel:
    median_model: Any
    lower_model: Any
    upper_model: Any
    feature_names: tuple[str, ...]

    def predict(self, features: Sequence[Sequence[float]]) -> list[dict[str, float]]:
        matrix = np.asarray(features, dtype=float)
        medians = np.asarray(self.median_model.predict(matrix), dtype=float)
        lowers = np.asarray(self.lower_model.predict(matrix), dtype=float)
        uppers = np.asarray(self.upper_model.predict(matrix), dtype=float)
        result: list[dict[str, float]] = []
        for median_value, lower_value, upper_value in zip(medians, lowers, uppers):
            center = max(0.0, min(100.0, float(median_value)))
            lower = max(0.0, min(center, float(lower_value)))
            upper = min(100.0, max(center, float(upper_value)))
            result.append({"median": center, "lower": lower, "upper": upper})
        return result


def softmax(values: Sequence[float]) -> list[float]:
    clean = [float(value) for value in values]
    maximum = max(clean)
    exponentials = [math.exp(value - maximum) for value in clean]
    total = sum(exponentials)
    return [value / total for value in exponentials]
