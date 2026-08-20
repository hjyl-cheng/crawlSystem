from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from hashlib import sha256
import math
from statistics import median, pstdev

from .events import (
    AboutPayload,
    AgentPayload,
    CrawlerObservationRecorded,
    RESOLVED_STATUSES,
    VideoDiscoveryPayload,
    VideoPayload,
    VideoRecentSamplingPayload,
)
from .shared_features import derive_recent_change_probability
from .utc import as_utc


@dataclass(frozen=True, slots=True)
class ChannelFeatureState:
    last_subscriber_count: int | None = None
    last_subscriber_observed_at: datetime | None = None
    last_total_view_count: int | None = None
    last_total_view_observed_at: datetime | None = None
    last_total_video_count: int | None = None
    last_total_video_observed_at: datetime | None = None
    last_about_observed_at: datetime | None = None
    about_metric_confidence: float | None = None
    subscriber_velocity_ewma: float | None = None
    view_velocity_ewma: float | None = None
    video_count_delta: int | None = None
    subscriber_size_percentile: float | None = None
    subscriber_growth_percentile: float | None = None
    view_growth_percentile: float | None = None
    growth_momentum: float | None = None
    about_stable_since: datetime | None = None
    about_stable_runs: int = 0

    recent_publish_interval_days: tuple[float, ...] = ()
    publish_interval_ewma: float | None = None
    publish_interval_median: float | None = None
    publish_interval_mad: float | None = None
    publish_regularity: float | None = None
    last_publish_at: datetime | None = None
    recent30_video_count: int | None = None
    new_video_empty_runs: int = 0
    last_discovery_observed_at: datetime | None = None
    last_complete_discovery_at: datetime | None = None

    recent_stale_ratio: float | None = None
    recent_view_change_ewma: float | None = None
    recent_engagement_change_ewma: float | None = None
    recent_upload_change_ewma: float | None = None
    recent_change_probability: float | None = None
    recent_sampling_stable_runs: int = 0
    last_recent_sampling_at: datetime | None = None
    last_recent_sample_count: int | None = None

    current_topic_vector: tuple[float, ...] = ()
    current_topic_tokens: tuple[str, ...] = ()
    current_agent_output_hash: str | None = None
    current_agent_evidence_fingerprints: tuple[str, ...] = ()
    current_agent_version_hash: str | None = None
    last_agent_evidence_count: int | None = None
    topic_drift: float | None = None
    evidence_replacement: float | None = None
    recent_content_shift: float | None = None
    agent_version_changed: bool = False
    agent_output_changed: bool = False
    agent_change_score: float | None = None
    agent_topic_vector_source: str | None = None
    agent_confidence: float | None = None
    agent_stable_runs: int = 0
    last_agent_observed_at: datetime | None = None

    user_query_demand: float = 0.0
    data_incompleteness: float = 1.0
    manual_priority: float = 0.0
    collection_priority: float = 0.346875
    channel_activity: float | None = None
    feature_confidence: float = 0.0
    fallback_reason_codes: tuple[str, ...] = ()
    reference_distribution_version: str | None = None
    state_version: int = 0


# Kept as a public alias for the original About-only implementation API.
AboutFeatureState = ChannelFeatureState


@dataclass(frozen=True, slots=True)
class AboutTransition:
    state: ChannelFeatureState
    baseline: bool
    business_state_changed: bool
    resolved_metric_count: int


@dataclass(frozen=True, slots=True)
class DiscoveryTransition:
    state: ChannelFeatureState
    baseline: bool
    business_state_changed: bool
    first_seen_count: int


@dataclass(frozen=True, slots=True)
class RecentSamplingTransition:
    state: ChannelFeatureState
    baseline: bool
    business_state_changed: bool


@dataclass(frozen=True, slots=True)
class VideoTransition:
    state: ChannelFeatureState
    discovery_baseline: bool
    recent_sampling_baseline: bool
    business_state_changed: bool
    discovery_outcome: str
    recent_sampling_outcome: str


@dataclass(frozen=True, slots=True)
class AgentTransition:
    state: ChannelFeatureState
    baseline: bool
    business_state_changed: bool
    output_changed: bool
    evidence_count: int


def _ewma(previous: float | None, current: float, alpha: float) -> float:
    return current if previous is None else (alpha * current) + ((1.0 - alpha) * previous)


def _metric_update(
    *,
    previous_value: int | None,
    previous_at: datetime | None,
    incoming_value: int | None,
    incoming_status: str,
    observed_at: datetime,
) -> tuple[int | None, datetime | None, float | None, bool, bool]:
    if incoming_status not in RESOLVED_STATUSES or incoming_value is None:
        return previous_value, previous_at, None, False, False
    if previous_at is not None and observed_at <= previous_at:
        return previous_value, previous_at, None, False, False
    if previous_value is None or previous_at is None:
        return incoming_value, observed_at, None, True, False
    elapsed_days = (observed_at - previous_at).total_seconds() / 86400.0
    if elapsed_days <= 0:
        return previous_value, previous_at, None, False, False
    velocity = (incoming_value - previous_value) / elapsed_days
    return incoming_value, observed_at, velocity, True, incoming_value != previous_value


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
    age_days = max(0.0, (observed_at - last_publish_at).total_seconds() / 86400.0)
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


def _with_activity(state: ChannelFeatureState, observed_at: datetime) -> ChannelFeatureState:
    growth = state.growth_momentum if state.growth_momentum is not None else 0.5
    activity = (
        (0.40 * publish_frequency_score(state.recent30_video_count))
        + (0.25 * publish_recency_score(state.last_publish_at, observed_at))
        + (0.35 * growth)
    )
    return replace(state, channel_activity=min(1.0, max(0.0, activity)))


def apply_about_event(
    state: ChannelFeatureState,
    event: CrawlerObservationRecorded,
    *,
    velocity_alpha: float = 0.30,
) -> AboutTransition:
    if event.observation_kind != "about" or not isinstance(event.payload, AboutPayload):
        raise ValueError("apply_about_event requires an About event")
    if not 0.0 < velocity_alpha <= 1.0:
        raise ValueError("velocity_alpha must be in (0, 1]")
    if event.outcome == "failed":
        return AboutTransition(
            state=state,
            baseline=state.last_about_observed_at is None,
            business_state_changed=False,
            resolved_metric_count=0,
        )

    payload = event.payload
    subscriber = _metric_update(
        previous_value=state.last_subscriber_count,
        previous_at=state.last_subscriber_observed_at,
        incoming_value=payload.subscriber_count,
        incoming_status=payload.subscriber_count_status,
        observed_at=event.observed_at,
    )
    views = _metric_update(
        previous_value=state.last_total_view_count,
        previous_at=state.last_total_view_observed_at,
        incoming_value=payload.total_view_count,
        incoming_status=payload.total_view_count_status,
        observed_at=event.observed_at,
    )
    videos = _metric_update(
        previous_value=state.last_total_video_count,
        previous_at=state.last_total_video_observed_at,
        incoming_value=payload.total_video_count,
        incoming_status=payload.total_video_count_status,
        observed_at=event.observed_at,
    )
    baseline = state.last_about_observed_at is None
    compared = [metric for metric in (subscriber, views, videos) if metric[2] is not None]
    any_value_changed = any(metric[4] for metric in compared)
    any_applied = any(metric[3] for metric in (subscriber, views, videos))
    if not any_applied:
        return AboutTransition(
            state=state,
            baseline=baseline,
            business_state_changed=False,
            resolved_metric_count=payload.resolved_metric_count,
        )

    stable_runs = state.about_stable_runs
    stable_since = state.about_stable_since
    if compared:
        if any_value_changed:
            stable_runs = 0
            stable_since = event.observed_at
        else:
            stable_runs += 1
            stable_since = stable_since or event.observed_at
    elif baseline:
        stable_since = event.observed_at

    fallback_reasons: list[str] = []
    if baseline:
        fallback_reasons.append("about_baseline")
    if event.outcome == "partial":
        fallback_reasons.append("about_partial")
    if state.subscriber_growth_percentile is None or state.view_growth_percentile is None:
        fallback_reasons.append("growth_reference_unavailable")
    exact_count = sum(
        status == "exact"
        for status in (
            payload.subscriber_count_status,
            payload.total_view_count_status,
            payload.total_video_count_status,
        )
    )
    estimated_count = sum(
        status == "estimated"
        for status in (
            payload.subscriber_count_status,
            payload.total_view_count_status,
            payload.total_video_count_status,
        )
    )
    confidence = min(1.0, (exact_count + (0.75 * estimated_count)) / 3.0)
    growth_momentum = None
    if state.subscriber_growth_percentile is not None and state.view_growth_percentile is not None:
        growth_momentum = (
            state.subscriber_growth_percentile + state.view_growth_percentile
        ) / 2.0

    next_state = replace(
        state,
        last_subscriber_count=subscriber[0],
        last_subscriber_observed_at=subscriber[1],
        last_total_view_count=views[0],
        last_total_view_observed_at=views[1],
        last_total_video_count=videos[0],
        last_total_video_observed_at=videos[1],
        last_about_observed_at=(
            event.observed_at
            if state.last_about_observed_at is None or event.observed_at > state.last_about_observed_at
            else state.last_about_observed_at
        ),
        about_metric_confidence=confidence,
        subscriber_velocity_ewma=(
            _ewma(state.subscriber_velocity_ewma, subscriber[2], velocity_alpha)
            if subscriber[2] is not None
            else state.subscriber_velocity_ewma
        ),
        view_velocity_ewma=(
            _ewma(state.view_velocity_ewma, views[2], velocity_alpha)
            if views[2] is not None
            else state.view_velocity_ewma
        ),
        video_count_delta=(
            videos[0] - state.last_total_video_count
            if videos[2] is not None and state.last_total_video_count is not None
            else None
        ),
        growth_momentum=growth_momentum,
        about_stable_since=stable_since,
        about_stable_runs=stable_runs,
        feature_confidence=confidence,
        fallback_reason_codes=tuple(fallback_reasons),
        state_version=state.state_version + 1,
    )
    next_state = _with_activity(next_state, event.observed_at)
    return AboutTransition(
        state=next_state,
        baseline=baseline,
        business_state_changed=True,
        resolved_metric_count=payload.resolved_metric_count,
    )


def apply_about_stability_evidence(
    previous: ChannelFeatureState,
    current: ChannelFeatureState,
    *,
    observed_at: datetime,
    outcome: str,
    baseline: bool,
    low_growth_percentile: float = 0.35,
) -> ChannelFeatureState:
    if baseline:
        return replace(
            current,
            about_stable_since=as_utc(observed_at, "observed_at"),
            about_stable_runs=0,
        )
    if outcome != "complete":
        return replace(
            current,
            about_stable_since=previous.about_stable_since,
            about_stable_runs=previous.about_stable_runs,
        )
    growth_percentiles = (
        current.subscriber_growth_percentile,
        current.view_growth_percentile,
    )
    if any(value is None for value in growth_percentiles):
        return replace(
            current,
            about_stable_since=previous.about_stable_since,
            about_stable_runs=previous.about_stable_runs,
        )
    low_change = (
        max(float(value) for value in growth_percentiles if value is not None)
        < low_growth_percentile
        and (current.video_count_delta or 0) <= 0
    )
    if not low_change:
        return replace(
            current,
            about_stable_since=as_utc(observed_at, "observed_at"),
            about_stable_runs=0,
        )
    return replace(
        current,
        about_stable_since=(
            previous.about_stable_since or as_utc(observed_at, "observed_at")
        ),
        about_stable_runs=previous.about_stable_runs + 1,
    )


def _parsed_publish_times(
    payload: VideoDiscoveryPayload,
    *,
    after: datetime | None = None,
) -> list[datetime]:
    output: dict[datetime, None] = {}
    for item in payload.first_seen:
        if item.published_at is None:
            continue
        published_at = as_utc(
            datetime.fromisoformat(item.published_at.replace("Z", "+00:00")),
            "published_at",
        )
        if after is None or published_at > after:
            output[published_at] = None
    return sorted(output)


def _interval_statistics(intervals: tuple[float, ...]) -> tuple[float | None, float | None, float | None]:
    if not intervals:
        return None, None, None
    center = float(median(intervals))
    deviations = tuple(abs(value - center) for value in intervals)
    mad = float(median(deviations))
    if mad == 0:
        robust = tuple(value for value in intervals if math.isclose(value, center, abs_tol=1e-9))
    else:
        threshold = 3.5 * 1.4826 * mad
        robust = tuple(value for value in intervals if abs(value - center) <= threshold)
    robust = robust or intervals
    robust_median = float(median(robust))
    robust_mean = sum(robust) / len(robust)
    robust_cv = (pstdev(robust) / robust_mean) if len(robust) > 1 and robust_mean > 0 else 0.0
    regularity = math.exp(-2.5 * robust_cv)
    return robust_median, mad, min(1.0, max(0.0, regularity))


def _apply_discovery_phase(
    state: ChannelFeatureState,
    *,
    observed_at: datetime,
    outcome: str,
    payload: VideoDiscoveryPayload,
    interval_alpha: float = 0.35,
) -> DiscoveryTransition:
    if outcome not in {"complete", "partial"}:
        raise ValueError("Video Discovery phase outcome must be complete or partial")
    if not 0.0 < interval_alpha <= 1.0:
        raise ValueError("interval_alpha must be in (0, 1]")
    baseline = state.last_discovery_observed_at is None
    if (
        state.last_discovery_observed_at is not None
        and observed_at <= state.last_discovery_observed_at
    ):
        return DiscoveryTransition(state, baseline, False, payload.first_seen_count)

    all_published = _parsed_publish_times(payload)
    published = _parsed_publish_times(
        payload,
        after=state.last_complete_discovery_at,
    )
    points = list(published)
    if state.last_publish_at is not None:
        points.append(state.last_publish_at)
    points = sorted(set(points))
    new_intervals = tuple(
        (later - earlier).total_seconds() / 86400.0
        for earlier, later in zip(points, points[1:])
        if later > earlier
    )
    interval_history = (state.recent_publish_interval_days + new_intervals)[-32:]
    interval_ewma = state.publish_interval_ewma
    for value in new_intervals:
        interval_ewma = _ewma(interval_ewma, value, interval_alpha)
    robust_median, mad, regularity = _interval_statistics(interval_history)
    latest_publish = max(
        ([state.last_publish_at] if state.last_publish_at is not None else []) + published,
        default=None,
    )
    empty_runs = state.new_video_empty_runs
    if outcome == "complete":
        empty_runs = 0 if published else empty_runs + 1
    detail_coverage = (
        payload.detail_success_count / payload.first_seen_count
        if payload.first_seen_count > 0
        else 1.0
    )
    confidence = (0.75 if outcome == "complete" else 0.45) + (0.25 * detail_coverage)
    reasons = []
    if baseline:
        reasons.append("discovery_baseline")
    if outcome == "partial":
        reasons.append("discovery_partial")
    if payload.stop_reason == "gap_abandoned_latest_30":
        reasons.append("discovery_gap_abandoned_latest_30")
    if not interval_history:
        reasons.append("publish_interval_unavailable")
    if len(all_published) > len(published):
        reasons.append("backfill_first_seen_ignored")
    next_state = replace(
        state,
        recent_publish_interval_days=interval_history,
        publish_interval_ewma=interval_ewma,
        publish_interval_median=robust_median,
        publish_interval_mad=mad,
        publish_regularity=regularity,
        last_publish_at=latest_publish,
        new_video_empty_runs=empty_runs,
        last_discovery_observed_at=observed_at,
        last_complete_discovery_at=(
            observed_at
            if outcome == "complete"
            else state.last_complete_discovery_at
        ),
        feature_confidence=min(1.0, confidence),
        fallback_reason_codes=tuple(reasons),
        state_version=state.state_version + 1,
    )
    next_state = _with_activity(next_state, observed_at)
    return DiscoveryTransition(next_state, baseline, True, payload.first_seen_count)


def _apply_recent_sampling_phase(
    state: ChannelFeatureState,
    *,
    observed_at: datetime,
    outcome: str,
    payload: VideoRecentSamplingPayload,
    change_alpha: float = 0.40,
) -> RecentSamplingTransition:
    if outcome not in {"complete", "partial", "failed"}:
        raise ValueError("invalid Video Recent Sampling phase outcome")
    if not 0.0 < change_alpha <= 1.0:
        raise ValueError("change_alpha must be in (0, 1]")
    baseline = state.last_recent_sampling_at is None
    if outcome == "failed" or (
        state.last_recent_sampling_at is not None
        and observed_at <= state.last_recent_sampling_at
    ):
        return RecentSamplingTransition(state, baseline, False)

    view_score = (
        payload.view_changed_count / payload.comparable_view_count
        if payload.comparable_view_count > 0
        else None
    )
    engagement_score = (
        payload.engagement_changed_count / payload.success_count
        if payload.success_count > 0
        else 0.0
    )
    upload_score = 0.0
    if state.recent30_video_count is not None:
        upload_score = min(
            1.0,
            abs(payload.recent_count - state.recent30_video_count)
            / max(state.recent30_video_count, 1),
        )
    view_ewma = (
        _ewma(state.recent_view_change_ewma, view_score, change_alpha)
        if view_score is not None
        else state.recent_view_change_ewma
    )
    engagement_ewma = _ewma(
        state.recent_engagement_change_ewma, engagement_score, change_alpha
    )
    upload_ewma = _ewma(state.recent_upload_change_ewma, upload_score, change_alpha)
    changed = (view_score or 0.0) > 0 or engagement_score > 0 or upload_score > 0
    stable_runs = state.recent_sampling_stable_runs
    if baseline or changed:
        stable_runs = 0
    elif view_score is not None or state.recent30_video_count is not None:
        stable_runs += 1
    reasons = []
    if baseline:
        reasons.append("recent_sampling_baseline")
    if outcome == "partial":
        reasons.append("recent_sampling_partial")
    if payload.comparable_view_count == 0:
        reasons.append("comparable_views_unavailable")
    confidence = (
        payload.success_count / payload.selected_count if payload.selected_count > 0 else 1.0
    )
    next_state = replace(
        state,
        recent30_video_count=payload.recent_count,
        recent_stale_ratio=float(payload.stale_ratio),
        recent_view_change_ewma=view_ewma,
        recent_engagement_change_ewma=engagement_ewma,
        recent_upload_change_ewma=upload_ewma,
        recent_sampling_stable_runs=stable_runs,
        last_recent_sampling_at=observed_at,
        last_recent_sample_count=payload.success_count,
        feature_confidence=confidence,
        fallback_reason_codes=tuple(reasons),
        state_version=state.state_version + 1,
    )
    next_state = _with_activity(next_state, observed_at)
    probability = derive_recent_change_probability(
        view_change=view_ewma,
        engagement_change=engagement_ewma,
        upload_change=upload_ewma,
        channel_activity=next_state.channel_activity,
    )
    next_state = replace(next_state, recent_change_probability=probability)
    return RecentSamplingTransition(next_state, baseline, True)


def apply_video_event(
    state: ChannelFeatureState,
    event: CrawlerObservationRecorded,
    *,
    interval_alpha: float = 0.35,
    change_alpha: float = 0.40,
) -> VideoTransition:
    if event.observation_kind != "video" or not isinstance(event.payload, VideoPayload):
        raise ValueError("apply_video_event requires a Video event")
    payload = event.payload
    discovery = _apply_discovery_phase(
        state,
        observed_at=event.observed_at,
        outcome=payload.discovery_outcome,
        payload=payload.discovery,
        interval_alpha=interval_alpha,
    )
    if payload.recent_sampling_outcome == "skipped":
        sampling = RecentSamplingTransition(
            discovery.state,
            discovery.state.last_recent_sampling_at is None,
            False,
        )
    else:
        assert isinstance(payload.recent_sampling, VideoRecentSamplingPayload)
        sampling = _apply_recent_sampling_phase(
            discovery.state,
            observed_at=event.observed_at,
            outcome=payload.recent_sampling_outcome,
            payload=payload.recent_sampling,
            change_alpha=change_alpha,
        )
    reasons = list(discovery.state.fallback_reason_codes)
    reasons.extend(sampling.state.fallback_reason_codes)
    if payload.recent_sampling_outcome == "failed":
        reasons.append("recent_sampling_failed")
    elif payload.recent_sampling_outcome == "skipped":
        reasons.append("recent_sampling_skipped")
    combined_state = replace(
        sampling.state,
        feature_confidence=min(
            discovery.state.feature_confidence,
            sampling.state.feature_confidence,
        ),
        fallback_reason_codes=tuple(dict.fromkeys(reasons)),
        state_version=state.state_version + 1,
    )
    return VideoTransition(
        state=combined_state,
        discovery_baseline=discovery.baseline,
        recent_sampling_baseline=sampling.baseline,
        business_state_changed=(
            discovery.business_state_changed or sampling.business_state_changed
        ),
        discovery_outcome=payload.discovery_outcome,
        recent_sampling_outcome=payload.recent_sampling_outcome,
    )


def _agent_topic_tokens(payload: AgentPayload) -> tuple[tuple[str, ...], str]:
    if payload.topic_tokens:
        return payload.topic_tokens, "crawler_topic_tokens"
    tokens: list[str] = []
    if payload.category_level_1:
        tokens.append(f"l1:{payload.category_level_1.casefold()}")
    tokens.extend(f"l2:{item.casefold()}" for item in payload.category_level_2)
    return tuple(sorted(set(tokens))), "category_vector_fallback"


def _topic_vector(tokens: tuple[str, ...], dimensions: int = 64) -> tuple[float, ...]:
    if not tokens:
        return ()
    values = [0.0] * dimensions
    for token in tokens:
        index = int.from_bytes(sha256(token.encode("utf-8")).digest()[:8], "big") % dimensions
        values[index] += 1.0
    norm = math.sqrt(sum(value * value for value in values))
    return tuple(value / norm for value in values)


def _cosine_distance(previous: tuple[float, ...], current: tuple[float, ...]) -> float | None:
    if not previous or not current or len(previous) != len(current):
        return None
    similarity = sum(left * right for left, right in zip(previous, current, strict=True))
    return min(1.0, max(0.0, 1.0 - similarity))


def _set_change(previous: tuple[str, ...], current: tuple[str, ...]) -> float | None:
    if not previous:
        return None
    previous_set = set(previous)
    current_set = set(current)
    return min(1.0, len(current_set - previous_set) / max(len(previous_set), 1))


def _jaccard_distance(previous: tuple[str, ...], current: tuple[str, ...]) -> float | None:
    if not previous or not current:
        return None
    previous_set = set(previous)
    current_set = set(current)
    union = previous_set | current_set
    return 1.0 - (len(previous_set & current_set) / len(union))


def apply_agent_event(
    state: ChannelFeatureState,
    event: CrawlerObservationRecorded,
) -> AgentTransition:
    if event.observation_kind != "agent" or not isinstance(event.payload, AgentPayload):
        raise ValueError("apply_agent_event requires an Agent event")
    baseline = state.last_agent_observed_at is None
    if event.outcome == "failed" or (
        state.last_agent_observed_at is not None
        and event.observed_at <= state.last_agent_observed_at
    ):
        return AgentTransition(state, baseline, False, False, 0)

    payload = event.payload
    assert payload.output_hash is not None
    topic_tokens, vector_source = _agent_topic_tokens(payload)
    current_vector = _topic_vector(topic_tokens)
    output_changed = (
        state.current_agent_output_hash is not None
        and state.current_agent_output_hash != payload.output_hash
    )
    topic_drift = _cosine_distance(state.current_topic_vector, current_vector)
    evidence_replacement = _set_change(
        state.current_agent_evidence_fingerprints,
        payload.evidence_fingerprints or (),
    )
    if evidence_replacement is None and state.last_agent_evidence_count is not None:
        # Legacy version-1 events did not carry evidence identities.
        evidence_replacement = min(
            1.0,
            abs(payload.evidence_count - state.last_agent_evidence_count)
            / max(state.last_agent_evidence_count, 1),
        )
        vector_source = f"{vector_source}+legacy_evidence_count_fallback"
    recent_content_shift = _jaccard_distance(state.current_topic_tokens, topic_tokens)
    version_changed = (
        state.current_agent_version_hash is not None
        and payload.agent_version_hash is not None
        and state.current_agent_version_hash != payload.agent_version_hash
    )
    agent_change_score = (
        (0.45 * (topic_drift or 0.0))
        + (0.30 * (evidence_replacement or 0.0))
        + (0.15 * (recent_content_shift or 0.0))
        + (0.10 if version_changed else 0.0)
    )
    confidence = (
        (0.25 if payload.category_level_1 else 0.0)
        + (0.25 if payload.category_level_2 else 0.0)
        + (0.20 * min(1.0, payload.tag_count / 10.0))
        + (0.20 * min(1.0, payload.evidence_count / 20.0))
        + (0.10 if payload.active_subscriber_ratio is not None else 0.0)
    )
    reasons = []
    if baseline:
        reasons.append("agent_baseline")
    if not current_vector:
        reasons.append("agent_topic_vector_unavailable")
    if evidence_replacement is None:
        reasons.append("agent_evidence_baseline")
    stable = (
        0
        if baseline or output_changed or version_changed or agent_change_score > 0.0
        else state.agent_stable_runs + 1
    )
    next_state = replace(
        state,
        current_topic_vector=current_vector,
        current_topic_tokens=topic_tokens,
        current_agent_output_hash=payload.output_hash,
        current_agent_evidence_fingerprints=payload.evidence_fingerprints or (),
        current_agent_version_hash=payload.agent_version_hash,
        last_agent_evidence_count=payload.evidence_count,
        topic_drift=topic_drift,
        evidence_replacement=evidence_replacement,
        recent_content_shift=recent_content_shift,
        agent_version_changed=version_changed,
        agent_output_changed=output_changed,
        agent_change_score=min(1.0, agent_change_score),
        agent_topic_vector_source=vector_source,
        agent_confidence=min(1.0, confidence),
        agent_stable_runs=stable,
        last_agent_observed_at=event.observed_at,
        feature_confidence=min(1.0, confidence),
        fallback_reason_codes=tuple(reasons),
        state_version=state.state_version + 1,
    )
    return AgentTransition(
        next_state,
        baseline,
        True,
        output_changed,
        payload.evidence_count,
    )
