from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field
from datetime import date, datetime
import math
from typing import Mapping

from .utc import as_utc


REFERENCE_METHOD_VERSION = "v16-empirical-1"
DEFAULT_MINIMUM_COHORT_SIZE = 20
QUANTILE_PROBABILITIES = tuple(index / 100.0 for index in range(101))


def _unit_interval(value: float, field_name: str) -> float:
    result = float(value)
    if not math.isfinite(result) or not 0.0 <= result <= 1.0:
        raise ValueError(f"{field_name} must be between 0 and 1")
    return result


def subscriber_scale_cohort(subscriber_count: int | None) -> str:
    if subscriber_count is None:
        return "subs:unknown"
    if subscriber_count < 0:
        raise ValueError("subscriber_count cannot be negative")
    if subscriber_count < 1_000:
        return "subs:0-1k"
    if subscriber_count < 10_000:
        return "subs:1k-10k"
    if subscriber_count < 100_000:
        return "subs:10k-100k"
    if subscriber_count < 1_000_000:
        return "subs:100k-1m"
    if subscriber_count < 10_000_000:
        return "subs:1m-10m"
    if subscriber_count < 100_000_000:
        return "subs:10m-100m"
    return "subs:100m+"


@dataclass(frozen=True, slots=True)
class QuantileDistribution:
    as_of_day: date
    cohort_key: str
    feature_name: str
    sample_count: int
    probabilities: tuple[float, ...]
    values: tuple[float, ...]
    method_version: str = REFERENCE_METHOD_VERSION

    def __post_init__(self) -> None:
        if self.sample_count < 0:
            raise ValueError("sample_count cannot be negative")
        if not self.cohort_key or not self.feature_name or not self.method_version:
            raise ValueError("reference distribution identity fields are required")
        if not self.probabilities or len(self.probabilities) != len(self.values):
            raise ValueError("reference probabilities and values must have equal non-zero lengths")
        probabilities = tuple(
            _unit_interval(value, "reference probability") for value in self.probabilities
        )
        values = tuple(float(value) for value in self.values)
        if not all(math.isfinite(value) for value in values):
            raise ValueError("reference values must be finite")
        if tuple(sorted(probabilities)) != probabilities:
            raise ValueError("reference probabilities must be sorted")
        if tuple(sorted(values)) != values:
            raise ValueError("reference values must be sorted")
        object.__setattr__(self, "probabilities", probabilities)
        object.__setattr__(self, "values", values)

    def percentile(self, value: float) -> float:
        target = float(value)
        if not math.isfinite(target):
            raise ValueError("percentile input must be finite")
        left = bisect_left(self.values, target)
        right = bisect_right(self.values, target)
        if left != right:
            return (self.probabilities[left] + self.probabilities[right - 1]) / 2.0
        if left == 0:
            return self.probabilities[0]
        if left == len(self.values):
            return self.probabilities[-1]
        lower_value = self.values[left - 1]
        upper_value = self.values[left]
        if math.isclose(lower_value, upper_value):
            return (self.probabilities[left - 1] + self.probabilities[left]) / 2.0
        fraction = (target - lower_value) / (upper_value - lower_value)
        return self.probabilities[left - 1] + fraction * (
            self.probabilities[left] - self.probabilities[left - 1]
        )


@dataclass(frozen=True, slots=True)
class ReferenceCatalog:
    distributions: tuple[QuantileDistribution, ...]
    minimum_cohort_size: int = DEFAULT_MINIMUM_COHORT_SIZE
    _by_key: Mapping[tuple[str, str], QuantileDistribution] = field(
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        if self.minimum_cohort_size <= 0:
            raise ValueError("minimum_cohort_size must be positive")
        by_key = {
            (item.feature_name, item.cohort_key): item for item in self.distributions
        }
        if len(by_key) != len(self.distributions):
            raise ValueError("reference catalog contains duplicate feature/cohort rows")
        identities = {(item.as_of_day, item.method_version) for item in self.distributions}
        if len(identities) > 1:
            raise ValueError("reference catalog must contain one day and method version")
        object.__setattr__(self, "_by_key", by_key)

    @property
    def version(self) -> str | None:
        if not self.distributions:
            return None
        item = self.distributions[0]
        return f"{item.as_of_day.isoformat()}:{item.method_version}"

    def percentile(
        self,
        feature_name: str,
        cohort_key: str,
        value: int | float | None,
    ) -> float | None:
        if value is None:
            return None
        selected = self._by_key.get((feature_name, cohort_key))
        if selected is None or selected.sample_count < self.minimum_cohort_size:
            selected = self._by_key.get((feature_name, "all"))
        if selected is None or selected.sample_count == 0:
            return None
        return min(1.0, max(0.0, selected.percentile(float(value))))


@dataclass(frozen=True, slots=True)
class CollectionPrioritySignals:
    user_query_demand: float = 0.0
    manual_priority: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "user_query_demand",
            _unit_interval(self.user_query_demand, "user_query_demand"),
        )
        object.__setattr__(
            self,
            "manual_priority",
            _unit_interval(self.manual_priority, "manual_priority"),
        )


@dataclass(frozen=True, slots=True)
class SharedFeatureInputs:
    subscriber_count: int | None = None
    subscriber_velocity_ewma: float | None = None
    view_velocity_ewma: float | None = None
    recent30_video_count: int | None = None
    last_publish_at: datetime | None = None
    about_identity_observed: bool = False
    about_observed: bool = False
    discovery_observed: bool = False
    recent_sampling_observed: bool = False
    agent_observed: bool = False


@dataclass(frozen=True, slots=True)
class SharedFeatureResult:
    subscriber_size_percentile: float | None
    subscriber_growth_percentile: float | None
    view_growth_percentile: float | None
    growth_momentum: float | None
    user_query_demand: float
    data_incompleteness: float
    manual_priority: float
    channel_activity: float
    collection_priority: float
    reference_distribution_version: str | None


def publish_frequency_score(recent_count: int | None) -> float:
    count = recent_count or 0
    if count <= 0:
        return 0.0
    if count == 1:
        return 0.15
    if count <= 4:
        return 0.35
    if count <= 9:
        return 0.60
    if count <= 19:
        return 0.80
    return 1.0


def publish_recency_score(last_publish_at: datetime | None, observed_at: datetime) -> float:
    if last_publish_at is None:
        return 0.05
    observed = as_utc(observed_at, "observed_at")
    published = as_utc(last_publish_at, "last_publish_at")
    age_days = max(0.0, (observed - published).total_seconds() / 86400.0)
    if age_days <= 1:
        return 1.0
    if age_days <= 3:
        return 0.85
    if age_days <= 7:
        return 0.65
    if age_days <= 14:
        return 0.40
    if age_days <= 30:
        return 0.20
    return 0.05


def derive_recent_change_probability(
    *,
    view_change: float | None,
    engagement_change: float | None,
    upload_change: float | None,
    channel_activity: float | None,
) -> float | None:
    if view_change is None and engagement_change is None and upload_change is None:
        return None
    probability = (
        (0.45 * (view_change or 0.0))
        + (0.25 * (engagement_change or 0.0))
        + (0.20 * (upload_change or 0.0))
        + (0.10 * (channel_activity or 0.0))
    )
    return min(1.0, max(0.0, probability))


def derive_shared_features(
    inputs: SharedFeatureInputs,
    *,
    references: ReferenceCatalog,
    signals: CollectionPrioritySignals,
    observed_at: datetime,
) -> SharedFeatureResult:
    cohort = subscriber_scale_cohort(inputs.subscriber_count)
    subscriber_size_percentile = references.percentile(
        "subscriber_count",
        "all",
        inputs.subscriber_count,
    )
    subscriber_growth_percentile = references.percentile(
        "subscriber_velocity_ewma",
        cohort,
        inputs.subscriber_velocity_ewma,
    )
    view_growth_percentile = references.percentile(
        "view_velocity_ewma",
        cohort,
        inputs.view_velocity_ewma,
    )
    growth_momentum = (
        (subscriber_growth_percentile + view_growth_percentile) / 2.0
        if subscriber_growth_percentile is not None and view_growth_percentile is not None
        else None
    )
    activity = (
        (0.40 * publish_frequency_score(inputs.recent30_video_count))
        + (0.25 * publish_recency_score(inputs.last_publish_at, observed_at))
        + (0.35 * (growth_momentum if growth_momentum is not None else 0.5))
    )
    observed_domains = (
        inputs.about_identity_observed,
        inputs.about_observed,
        inputs.discovery_observed,
        inputs.recent_sampling_observed,
        inputs.agent_observed,
    )
    data_incompleteness = 1.0 - (sum(observed_domains) / len(observed_domains))
    collection_priority = (
        (0.30 * (subscriber_size_percentile if subscriber_size_percentile is not None else 0.5))
        + (0.25 * activity)
        + (0.20 * signals.user_query_demand)
        + (0.15 * data_incompleteness)
        + (0.10 * signals.manual_priority)
    )
    return SharedFeatureResult(
        subscriber_size_percentile=subscriber_size_percentile,
        subscriber_growth_percentile=subscriber_growth_percentile,
        view_growth_percentile=view_growth_percentile,
        growth_momentum=growth_momentum,
        user_query_demand=signals.user_query_demand,
        data_incompleteness=data_incompleteness,
        manual_priority=signals.manual_priority,
        channel_activity=min(1.0, max(0.0, activity)),
        collection_priority=min(1.0, max(0.0, collection_priority)),
        reference_distribution_version=references.version,
    )
