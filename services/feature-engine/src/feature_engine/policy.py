from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from hashlib import sha256
import math
from typing import Any

from .clock_window import clock_due_at_for_day
from .state import ChannelFeatureState, publish_frequency_score
from .utc import as_utc


ABOUT_TIER_DAYS = (1, 2, 3, 5, 7, 14, 30, 60, 90, 180)
ABOUT_COLD_START_CADENCE_TIERS = (
    (1.5, 1),
    (2.5, 2),
    (3.5, 3),
    (5.5, 5),
)
ABOUT_LONG_STABILITY_GATES = (
    (21, 3, 14),
    (60, 6, 30),
    (120, 9, 60),
    (180, 12, 90),
    (365, 15, 180),
)
VIDEO_TIER_DAYS = (1, 3, 7, 14, 30, 60, 90)
AGENT_FORWARD_SPREAD_DAYS = {
    14: 0,
    30: 2,
    60: 6,
    90: 14,
    180: 20,
    365: 29,
}
AGENT_FORWARD_SPREAD_VERSION = "agent-forward-spread-1"
AGENT_SEMANTIC_MIN_INTERVAL_DAYS = 60
AGENT_SEMANTIC_MAX_INTERVAL_DAYS = 365
AGENT_SEMANTIC_CURVE_EXPONENT = 1.8
AGENT_SEMANTIC_CURVE_VERSION = "agent-semantic-curve-1"


@dataclass(frozen=True, slots=True)
class AboutPolicyConfig:
    policy_version: str = "v16-rule-2"
    baseline_interval_days: int = 7
    partial_retry_days: int = 3
    stable_min_days_for_long_interval: int = 90
    neutral_growth_percentile: float = 0.50
    video_delta_full_scale: int = 3
    cold_start_priority_floor: float = 0.75
    cold_start_min_recent_video_count: int = 20
    cold_start_max_publish_interval_days: float = 1.5
    cold_start_max_publish_age_days: float = 3.0
    cold_start_tier_one_max_publish_interval_days: float = 1.5
    cold_start_min_reliable_intervals: int = 3
    cold_start_min_subscriber_count: int = 5_000
    cold_start_min_subscriber_percentile: float = 0.50
    cold_start_min_feature_confidence: float = 0.75
    dynamic_baseline_enabled: bool = False
    cadence_baseline_enabled: bool = False


@dataclass(frozen=True, slots=True)
class DiscoveryPolicyConfig:
    policy_version: str = "v16-rule-2"
    fallback_interval_days: int = 7
    partial_retry_days: int = 3
    regularity_threshold: float = 0.65
    silence_decay: float = 0.60
    automatic_min_interval_days: int = 3
    allowed_days: tuple[int, ...] = (1, 3, 7, 14, 30, 60, 90)


@dataclass(frozen=True, slots=True)
class RecentSamplingPolicyConfig:
    policy_version: str = "v16-rule-2"
    fallback_interval_days: int = 14
    partial_retry_days: int = 7


@dataclass(frozen=True, slots=True)
class AgentPolicyConfig:
    policy_version: str = "v16-rule-7"
    baseline_interval_days: int = 180
    partial_retry_days: int = 14
    high_priority_cap_days: int = 90
    version_change_interval_days: int = 14
    dynamic_baseline_enabled: bool = False


@dataclass(frozen=True, slots=True)
class ClockDecision:
    policy_version: str
    due_at: datetime
    due_day: date
    tier_days: int
    reason_codes: tuple[str, ...]
    feature_summary: dict[str, Any]


@dataclass(frozen=True, slots=True)
class VideoRiskCandidate:
    interval_days: int
    reason_codes: tuple[str, ...]
    feature_summary: dict[str, Any]


AboutClockDecision = ClockDecision
VideoClockDecision = ClockDecision
AgentClockDecision = ClockDecision


@dataclass(frozen=True, slots=True)
class RuntimePolicyConfigs:
    about: AboutPolicyConfig
    discovery: DiscoveryPolicyConfig
    recent_sampling: RecentSamplingPolicyConfig
    agent: AgentPolicyConfig


def runtime_policy_configs(policy: Any) -> RuntimePolicyConfigs:
    retry = policy.partial_retry_config
    about = policy.about_config
    discovery = policy.discovery_config
    recent = policy.recent_sampling_config
    agent = policy.agent_config
    discovery_days = tuple(
        value for value in policy.allowed_days if value in VIDEO_TIER_DAYS
    )
    return RuntimePolicyConfigs(
        about=AboutPolicyConfig(
            policy_version=policy.policy_version,
            baseline_interval_days=about.baseline_interval_days,
            partial_retry_days=retry.about_days,
            stable_min_days_for_long_interval=about.stable_min_days_for_long_interval,
            neutral_growth_percentile=float(about.neutral_growth_percentile),
            video_delta_full_scale=about.video_delta_full_scale,
            cold_start_priority_floor=float(about.cold_start_priority_floor),
            cold_start_min_recent_video_count=about.cold_start_min_recent_video_count,
            cold_start_max_publish_interval_days=float(
                about.cold_start_max_publish_interval_days
            ),
            cold_start_max_publish_age_days=float(about.cold_start_max_publish_age_days),
            cold_start_tier_one_max_publish_interval_days=float(
                about.cold_start_tier_one_max_publish_interval_days
            ),
            cold_start_min_reliable_intervals=about.cold_start_min_reliable_intervals,
            cold_start_min_subscriber_count=about.cold_start_min_subscriber_count,
            cold_start_min_subscriber_percentile=float(
                about.cold_start_min_subscriber_percentile
            ),
            cold_start_min_feature_confidence=float(
                about.cold_start_min_feature_confidence
            ),
            dynamic_baseline_enabled=about.dynamic_baseline_enabled,
            cadence_baseline_enabled=about.cadence_baseline_enabled,
        ),
        discovery=DiscoveryPolicyConfig(
            policy_version=policy.policy_version,
            fallback_interval_days=discovery.fallback_interval_days,
            partial_retry_days=retry.discovery_days,
            regularity_threshold=float(discovery.regularity_threshold),
            silence_decay=float(discovery.silence_decay),
            automatic_min_interval_days=discovery.automatic_min_interval_days,
            allowed_days=discovery_days or VIDEO_TIER_DAYS,
        ),
        recent_sampling=RecentSamplingPolicyConfig(
            policy_version=policy.policy_version,
            fallback_interval_days=recent.fallback_interval_days,
            partial_retry_days=retry.recent_sampling_days,
        ),
        agent=AgentPolicyConfig(
            policy_version=policy.policy_version,
            baseline_interval_days=agent.baseline_interval_days,
            partial_retry_days=retry.agent_days,
            high_priority_cap_days=agent.high_priority_cap_days,
            version_change_interval_days=agent.version_change_interval_days,
            dynamic_baseline_enabled=agent.dynamic_baseline_enabled,
        ),
    )


def decide_rebuild_clocks(
    state: ChannelFeatureState,
    *,
    policy: Any,
) -> dict[str, ClockDecision]:
    configs = runtime_policy_configs(policy)
    decisions: dict[str, ClockDecision] = {}
    if state.last_about_observed_at is not None:
        decisions["about"] = decide_about_due(
            state,
            observed_at=state.last_about_observed_at,
            outcome="complete",
            baseline=False,
            config=configs.about,
        )
    video_observed_at = max(
        (
            observed_at
            for observed_at in (
                state.last_discovery_observed_at,
                state.last_recent_sampling_at,
            )
            if observed_at is not None
        ),
        default=None,
    )
    if video_observed_at is not None:
        decisions["video"] = decide_video_due(
            state,
            observed_at=video_observed_at,
            discovery_outcome=(
                "complete" if state.last_discovery_observed_at is not None else "partial"
            ),
            recent_sampling_outcome=(
                "complete" if state.last_recent_sampling_at is not None else "failed"
            ),
            discovery_baseline=state.last_discovery_observed_at is None,
            recent_sampling_baseline=state.last_recent_sampling_at is None,
            discovery_config=configs.discovery,
            recent_sampling_config=configs.recent_sampling,
        )
    if state.last_agent_observed_at is not None:
        decisions["agent"] = decide_agent_due(
            state,
            observed_at=state.last_agent_observed_at,
            outcome="complete",
            baseline=False,
            output_changed=state.agent_output_changed,
            evidence_count=state.last_agent_evidence_count or 0,
            config=configs.agent,
        )
    return decisions


def _stable_days(state: ChannelFeatureState, observed_at: datetime) -> float:
    if state.about_stable_since is None:
        return 0.0
    return max(0.0, (observed_at - state.about_stable_since).total_seconds() / 86400.0)


def recent_publish_active(state: ChannelFeatureState, observed_at: datetime) -> bool:
    if (state.recent30_video_count or 0) > 0:
        return True
    if state.last_publish_at is None or observed_at <= state.last_publish_at:
        return False
    return (observed_at - state.last_publish_at).total_seconds() <= (30 * 86400)


def _about_cold_start_qualifies(
    state: ChannelFeatureState,
    *,
    observed_at: datetime,
    config: AboutPolicyConfig,
) -> tuple[float | None, dict[str, Any]]:
    expected = _expected_publish_interval(state)
    interval_history_count = len(state.recent_publish_interval_days)
    cadence_qualified = (state.recent30_video_count or 0) >= (
        config.cold_start_min_recent_video_count
    ) or (
        expected is not None
        and expected <= config.cold_start_max_publish_interval_days
        and interval_history_count >= config.cold_start_min_reliable_intervals
    )
    publish_age_days = None
    if state.last_publish_at is not None:
        publish_age_days = max(
            0.0,
            (as_utc(observed_at, "observed_at") - state.last_publish_at).total_seconds()
            / 86400.0,
        )
    recency_qualified = (
        publish_age_days is not None
        and publish_age_days <= config.cold_start_max_publish_age_days
    )
    scale_qualified = (
        (state.last_subscriber_count or 0) >= config.cold_start_min_subscriber_count
        or (
            state.subscriber_size_percentile is not None
            and state.subscriber_size_percentile
            >= config.cold_start_min_subscriber_percentile
        )
    )
    about_confidence = (
        state.about_metric_confidence
        if state.about_metric_confidence is not None
        else state.feature_confidence
    )
    confidence_qualified = about_confidence >= config.cold_start_min_feature_confidence
    enabled = config.cold_start_priority_floor > 0
    pressure = _cold_start_pressure(state)
    gates_qualified = (
        enabled
        and cadence_qualified
        and recency_qualified
        and scale_qualified
        and confidence_qualified
    )
    priority_floor = None
    pressure_tier = "ineligible"
    if gates_qualified and not config.dynamic_baseline_enabled:
        priority_floor = config.cold_start_priority_floor
        pressure_tier = "legacy_high"
    elif gates_qualified and pressure["score"] >= 0.75:
        priority_floor = config.cold_start_priority_floor
        pressure_tier = "high"
    elif gates_qualified and pressure["score"] >= 0.45:
        priority_floor = 0.55
        pressure_tier = "active"
    elif gates_qualified:
        pressure_tier = "low"
    return (
        priority_floor,
        {
            "enabled": enabled,
            "cadence_qualified": cadence_qualified,
            "recency_qualified": recency_qualified,
            "scale_qualified": scale_qualified,
            "confidence_qualified": confidence_qualified,
            "recent30_video_count": state.recent30_video_count,
            "expected_publish_interval": round(expected, 6) if expected is not None else None,
            "reliable_interval_count": interval_history_count,
            "last_publish_age_days": (
                round(publish_age_days, 6) if publish_age_days is not None else None
            ),
            "subscriber_count": state.last_subscriber_count,
            "subscriber_size_percentile": state.subscriber_size_percentile,
            "about_metric_confidence": about_confidence,
            "pressure_tier": pressure_tier,
            "pressure": pressure,
        },
    )


def _about_cold_start_cadence_tier(
    state: ChannelFeatureState,
    *,
    observed_at: datetime,
    config: AboutPolicyConfig,
) -> tuple[int | None, dict[str, Any]]:
    expected = _expected_publish_interval(state)
    interval_history_count = len(state.recent_publish_interval_days)
    cadence_interval: float | None = None
    cadence_source = "unavailable"
    if (
        expected is not None
        and expected > 0
        and interval_history_count >= config.cold_start_min_reliable_intervals
    ):
        cadence_interval = float(expected)
        cadence_source = "publish_interval_history"
    elif (state.recent30_video_count or 0) > 0:
        cadence_interval = 30.0 / float(state.recent30_video_count or 1)
        cadence_source = "recent_30_day_count"

    publish_age_days = None
    if state.last_publish_at is not None:
        publish_age_days = max(
            0.0,
            (
                as_utc(observed_at, "observed_at")
                - as_utc(state.last_publish_at, "last_publish_at")
            ).total_seconds()
            / 86400.0,
        )
    about_confidence = (
        state.about_metric_confidence
        if state.about_metric_confidence is not None
        else state.feature_confidence
    )
    recency_qualified = (
        publish_age_days is not None
        and publish_age_days <= config.cold_start_max_publish_age_days
    )
    confidence_qualified = about_confidence >= config.cold_start_min_feature_confidence
    cadence_qualified = (
        config.cadence_baseline_enabled
        and cadence_interval is not None
        and recency_qualified
        and confidence_qualified
    )
    eligible = cadence_qualified
    selected_tier = None
    tier_selection_source = None
    if cadence_qualified:
        selected_tier = 7
        tier_selection_source = cadence_source
        cadence_tiers = (
            (config.cold_start_tier_one_max_publish_interval_days, 1),
            *ABOUT_COLD_START_CADENCE_TIERS[1:],
        )
        for maximum_interval, tier in cadence_tiers:
            if cadence_interval <= maximum_interval:
                selected_tier = tier
                break
    return (
        selected_tier,
        {
            "enabled": config.cadence_baseline_enabled,
            "eligible": eligible,
            "cadence_source": cadence_source,
            "tier_selection_source": tier_selection_source,
            "cadence_interval_days": (
                round(cadence_interval, 6) if cadence_interval is not None else None
            ),
            "expected_publish_interval": (
                round(expected, 6) if expected is not None else None
            ),
            "reliable_interval_count": interval_history_count,
            "recent30_video_count": state.recent30_video_count,
            "last_publish_age_days": (
                round(publish_age_days, 6) if publish_age_days is not None else None
            ),
            "recency_qualified": recency_qualified,
            "tier_one_max_publish_interval_days": (
                config.cold_start_tier_one_max_publish_interval_days
            ),
            "about_metric_confidence": about_confidence,
            "confidence_qualified": confidence_qualified,
            "selected_tier_days": selected_tier,
        },
    )


def _about_priority_tier(priority: float) -> tuple[int, str]:
    for threshold, tier, reason in (
        (0.75, 1, "about_priority_very_high"),
        (0.65, 2, "about_priority_high"),
        (0.55, 3, "about_priority_elevated"),
        (0.45, 5, "about_priority_active"),
        (0.35, 7, "about_priority_medium"),
        (0.25, 14, "about_priority_low"),
        (0.15, 30, "about_priority_very_low"),
        (0.10, 60, "about_priority_minimal"),
        (0.05, 90, "about_priority_dormant"),
    ):
        if priority >= threshold:
            return tier, reason
    return 180, "about_priority_deeply_dormant"


def _about_stability_cap(state: ChannelFeatureState, stable_days: float) -> int:
    cap = 7
    for minimum_days, minimum_runs, tier in ABOUT_LONG_STABILITY_GATES:
        if stable_days >= minimum_days and state.about_stable_runs >= minimum_runs:
            cap = tier
    return cap


def _decision(
    *,
    policy_version: str,
    observed_at: datetime,
    interval: int,
    reasons: list[str],
    summary: dict[str, Any],
) -> ClockDecision:
    observed_day = as_utc(observed_at, "observed_at").date()
    due_day = observed_day + timedelta(days=interval)
    return ClockDecision(
        policy_version=policy_version,
        due_at=clock_due_at_for_day(due_day),
        due_day=due_day,
        tier_days=interval,
        reason_codes=tuple(dict.fromkeys(reasons)),
        feature_summary=summary,
    )


def stable_agent_forward_offset(
    channel_id: str,
    *,
    policy_version: str,
    tier_days: int,
) -> int:
    """Return a deterministic non-negative load-spreading offset for Agent work."""

    normalized_channel_id = channel_id.strip()
    if not normalized_channel_id:
        raise ValueError("channel_id cannot be empty")
    maximum = agent_forward_spread_max_days(tier_days)
    if maximum == 0:
        return 0
    digest = sha256(
        (
            f"{policy_version}:{normalized_channel_id}:{tier_days}:"
            f"{AGENT_FORWARD_SPREAD_VERSION}"
        ).encode()
    ).digest()
    return int.from_bytes(digest[:8], "big") % (maximum + 1)


def agent_forward_spread_max_days(interval_days: int) -> int:
    anchors = tuple(sorted(AGENT_FORWARD_SPREAD_DAYS.items()))
    if interval_days <= anchors[0][0]:
        return anchors[0][1]
    if interval_days >= anchors[-1][0]:
        return anchors[-1][1]
    for (lower_days, lower_spread), (upper_days, upper_spread) in zip(
        anchors,
        anchors[1:],
    ):
        if interval_days <= upper_days:
            position = (interval_days - lower_days) / (upper_days - lower_days)
            return round(lower_spread + position * (upper_spread - lower_spread))
    raise AssertionError("Agent spread anchors do not cover the interval")


def _spread_agent_decision(
    decision: AgentClockDecision,
    *,
    channel_id: str,
) -> AgentClockDecision:
    maximum = agent_forward_spread_max_days(decision.tier_days)
    if maximum == 0:
        return decision
    offset = stable_agent_forward_offset(
        channel_id,
        policy_version=decision.policy_version,
        tier_days=decision.tier_days,
    )
    base_due_day = decision.due_day
    due_day = base_due_day + timedelta(days=offset)
    return replace(
        decision,
        due_at=clock_due_at_for_day(due_day),
        due_day=due_day,
        reason_codes=tuple(
            dict.fromkeys((*decision.reason_codes, "agent_forward_load_spread"))
        ),
        feature_summary={
            **decision.feature_summary,
            "agent_base_due_day": base_due_day.isoformat(),
            "agent_forward_spread_days": offset,
            "agent_forward_spread_max_days": maximum,
            "agent_forward_spread_version": AGENT_FORWARD_SPREAD_VERSION,
        },
    )


def _agent_semantic_change_score(state: ChannelFeatureState) -> float | None:
    values = (
        state.topic_drift,
        state.evidence_replacement,
        state.recent_content_shift,
    )
    comparable = [float(value) for value in values if value is not None]
    if not comparable:
        return None
    return min(1.0, max(0.0, max(comparable)))


def _agent_semantic_interval_days(semantic_change: float) -> int:
    stable_fraction = (1.0 - semantic_change) ** AGENT_SEMANTIC_CURVE_EXPONENT
    interval_range = (
        AGENT_SEMANTIC_MAX_INTERVAL_DAYS - AGENT_SEMANTIC_MIN_INTERVAL_DAYS
    )
    return round(AGENT_SEMANTIC_MIN_INTERVAL_DAYS + interval_range * stable_fraction)


def limit_about_slowdown(
    decision: AboutClockDecision,
    *,
    previous_tier_days: int,
) -> AboutClockDecision:
    if decision.tier_days <= previous_tier_days:
        return decision
    try:
        previous_index = ABOUT_TIER_DAYS.index(previous_tier_days)
    except ValueError:
        return decision
    next_tier = ABOUT_TIER_DAYS[min(previous_index + 1, len(ABOUT_TIER_DAYS) - 1)]
    if decision.tier_days <= next_tier:
        return decision
    observed_day = decision.due_day - timedelta(days=decision.tier_days)
    due_day = observed_day + timedelta(days=next_tier)
    return replace(
        decision,
        due_at=clock_due_at_for_day(due_day),
        due_day=due_day,
        tier_days=next_tier,
        reason_codes=tuple(
            dict.fromkeys((*decision.reason_codes, "about_slowdown_one_tier"))
        ),
        feature_summary={
            **decision.feature_summary,
            "unconstrained_interval_days": decision.tier_days,
            "previous_interval_days": previous_tier_days,
            "slowdown_limited_interval_days": next_tier,
        },
    )


def decide_about_due(
    state: ChannelFeatureState,
    *,
    observed_at: datetime,
    outcome: str,
    baseline: bool,
    config: AboutPolicyConfig = AboutPolicyConfig(),
) -> AboutClockDecision:
    if outcome not in {"complete", "partial"}:
        raise ValueError("About policy only runs for complete or partial observations")
    reasons: list[str] = []
    subscriber_percentile = state.subscriber_growth_percentile
    view_percentile = state.view_growth_percentile
    if subscriber_percentile is None:
        subscriber_percentile = config.neutral_growth_percentile
        reasons.append("subscriber_growth_reference_fallback")
    if view_percentile is None:
        view_percentile = config.neutral_growth_percentile
        reasons.append("view_growth_reference_fallback")
    video_change_score = min(
        1.0,
        max(0.0, float(state.video_count_delta or 0)) / config.video_delta_full_scale,
    )
    priority = (
        (0.40 * subscriber_percentile)
        + (0.35 * view_percentile)
        + (0.20 * video_change_score)
        + (0.05 * state.collection_priority)
    )

    cadence_tier = None
    if config.cadence_baseline_enabled:
        cadence_tier, cold_start_summary = _about_cold_start_cadence_tier(
            state,
            observed_at=observed_at,
            config=config,
        )
        cold_start_floor = None
    else:
        cold_start_floor, cold_start_summary = _about_cold_start_qualifies(
            state,
            observed_at=observed_at,
            config=config,
        )
    cold_start_floor_applied = baseline and cold_start_floor is not None
    if cold_start_floor_applied:
        priority = max(priority, cold_start_floor)
        reasons.append("cold_start_high_activity_scale_floor")
        if config.dynamic_baseline_enabled:
            reasons.append(
                f"cold_start_{cold_start_summary['pressure_tier']}_pressure_floor"
            )

    if baseline and config.cadence_baseline_enabled:
        interval = cadence_tier or config.baseline_interval_days
        reasons.append("about_baseline")
        reasons.append(
            f"about_cold_start_cadence_{interval}d"
            if cadence_tier is not None
            else "about_cold_start_cadence_fallback"
        )
    elif baseline and not cold_start_floor_applied:
        interval = config.baseline_interval_days
        reasons.append("about_baseline")
    else:
        if baseline:
            reasons.append("about_baseline")
        if subscriber_percentile >= 0.90 or view_percentile >= 0.90:
            interval = 1
            reasons.append("growth_percentile_critical")
        elif config.cadence_baseline_enabled:
            interval, priority_reason = _about_priority_tier(priority)
            reasons.append(priority_reason)
        elif priority >= 0.75:
            interval = 1
            reasons.append("about_priority_very_high")
        elif priority >= 0.55:
            interval = 3
            reasons.append("about_priority_high")
        elif priority >= 0.35:
            interval = 7
            reasons.append("about_priority_medium")
        elif priority >= 0.20:
            interval = 14
            reasons.append("about_priority_low")
        else:
            interval = 30
            reasons.append("about_priority_very_low")

    if not baseline and cadence_tier is not None and interval > cadence_tier:
        interval = cadence_tier
        reasons.append(f"about_active_cadence_cap_{cadence_tier}d")

    if (state.video_count_delta or 0) > 0 and recent_publish_active(state, observed_at):
        interval = min(interval, 3)
        reasons.append("video_count_increased")
    stable_days = _stable_days(state, observed_at)
    stability_cap = None
    if config.cadence_baseline_enabled:
        stability_cap = _about_stability_cap(state, stable_days)
        if interval > stability_cap:
            interval = stability_cap
            reasons.append("about_long_interval_stability_cap")
    elif interval > 7 and stable_days < config.stable_min_days_for_long_interval:
        interval = 7
        reasons.append("long_interval_requires_90d_stability")
    if outcome == "partial":
        interval = min(interval, config.partial_retry_days)
        reasons.append("partial_retry_cap")

    return _decision(
        policy_version=config.policy_version,
        observed_at=observed_at,
        interval=interval,
        reasons=reasons,
        summary={
            "about_priority": round(priority, 6),
            "subscriber_growth_percentile": subscriber_percentile,
            "view_growth_percentile": view_percentile,
            "video_change_score": round(video_change_score, 6),
            "stable_days": round(stable_days, 3),
            "stable_runs": state.about_stable_runs,
            "stability_cap_days": stability_cap,
            "feature_confidence": state.feature_confidence,
            "fallback_reason_codes": list(state.fallback_reason_codes),
            "cold_start": cold_start_summary,
        },
    )


def _expected_publish_interval(state: ChannelFeatureState) -> float | None:
    if state.publish_interval_ewma is not None and state.publish_interval_median is not None:
        return (0.60 * state.publish_interval_ewma) + (0.40 * state.publish_interval_median)
    return state.publish_interval_ewma or state.publish_interval_median


def _cold_start_pressure(state: ChannelFeatureState) -> dict[str, Any]:
    expected = _expected_publish_interval(state)
    cadence_score = (
        0.0
        if expected is None
        else 1.0 / (1.0 + max(0.0, float(expected)))
    )
    if state.subscriber_size_percentile is not None:
        subscriber_scale_score = state.subscriber_size_percentile
        subscriber_scale_source = "reference_percentile"
    elif state.last_subscriber_count is not None:
        subscriber_scale_score = min(
            1.0,
            max(0.0, math.log10(state.last_subscriber_count + 1.0) / 6.0),
        )
        subscriber_scale_source = "log_count_fallback"
    else:
        subscriber_scale_score = 0.0
        subscriber_scale_source = "unavailable"
    activity_score = min(1.0, max(0.0, state.channel_activity or 0.0))
    pressure = (
        (0.55 * cadence_score)
        + (0.25 * activity_score)
        + (0.20 * subscriber_scale_score)
    )
    return {
        "score": round(min(1.0, max(0.0, pressure)), 6),
        "cadence_score": round(cadence_score, 6),
        "activity_score": round(activity_score, 6),
        "subscriber_scale_score": round(subscriber_scale_score, 6),
        "subscriber_scale_source": subscriber_scale_source,
        "expected_publish_interval": (
            round(expected, 6) if expected is not None else None
        ),
    }


def _map_tier(raw_days: float, allowed_days: tuple[int, ...]) -> int:
    raw = max(1.0, raw_days)
    for tier in allowed_days:
        if raw <= tier:
            return tier
    return allowed_days[-1]


def evaluate_video_discovery_risk(
    state: ChannelFeatureState,
    *,
    observed_at: datetime,
    outcome: str,
    baseline: bool,
    config: DiscoveryPolicyConfig = DiscoveryPolicyConfig(),
) -> VideoRiskCandidate:
    if outcome not in {"complete", "partial"}:
        raise ValueError("Discovery policy only runs for complete or partial observations")
    reasons: list[str] = []
    expected = _expected_publish_interval(state)
    raw_days: float
    if expected is None or expected <= 0:
        raw_days = float(config.fallback_interval_days)
        reasons.append("publish_interval_fallback")
    elif (
        (state.publish_regularity or 0.0) >= config.regularity_threshold
        and state.last_publish_at is not None
        and (
            state.last_publish_at
            + timedelta(days=expected)
            - timedelta(days=min(7, max(1, math.ceil(expected * 0.20))))
        ) > observed_at
    ):
        lead_days = min(7, max(1, math.ceil(expected * 0.20)))
        predicted = state.last_publish_at + timedelta(days=expected)
        raw_due = predicted - timedelta(days=lead_days)
        raw_days = max(1.0, (raw_due - observed_at).total_seconds() / 86400.0)
        reasons.append("regular_publish_prediction")
    else:
        if (
            (state.publish_regularity or 0.0) >= config.regularity_threshold
            and state.last_publish_at is not None
        ):
            reasons.append("regular_publish_window_elapsed")
        last_publish_age = (
            max(0.0, (observed_at - state.last_publish_at).total_seconds() / 86400.0)
            if state.last_publish_at is not None
            else expected
        )
        silence_excess = max(0.0, (last_publish_age / expected) - 1.0)
        effective_rate = (1.0 / expected) * math.exp(-config.silence_decay * silence_excess)
        if state.collection_priority >= 0.85:
            target_probability = 0.20
            reasons.append("high_collection_priority")
        elif (state.channel_activity or 0.0) >= 0.40:
            target_probability = 0.35
            reasons.append("active_irregular_channel")
        else:
            target_probability = 0.60
            reasons.append("cold_irregular_channel")
        raw_days = -math.log(1.0 - target_probability) / max(effective_rate, 1e-9)
        if state.new_video_empty_runs > 0:
            raw_days *= 1.0 + min(2.0, 0.25 * state.new_video_empty_runs)
            reasons.append("empty_run_backoff")
    interval = _map_tier(raw_days, config.allowed_days)
    if baseline:
        reasons.append("discovery_baseline")
    if outcome == "partial":
        interval = min(interval, config.partial_retry_days)
        reasons.append("partial_retry_cap")
    return VideoRiskCandidate(
        interval_days=interval,
        reason_codes=tuple(dict.fromkeys(reasons)),
        feature_summary={
            "expected_publish_interval": round(expected, 6) if expected is not None else None,
            "publish_regularity": state.publish_regularity,
            "new_video_empty_runs": state.new_video_empty_runs,
            "raw_interval_days": round(raw_days, 6),
            "last_publish_at": state.last_publish_at.isoformat() if state.last_publish_at else None,
        },
    )


def evaluate_video_sampling_risk(
    state: ChannelFeatureState,
    *,
    outcome: str,
    baseline: bool,
    config: RecentSamplingPolicyConfig = RecentSamplingPolicyConfig(),
) -> VideoRiskCandidate:
    if outcome not in {"complete", "partial"}:
        raise ValueError("Recent Sampling policy only runs for complete or partial observations")
    activity = state.channel_activity or 0.0
    frequency = publish_frequency_score(state.recent30_video_count)
    change_probability = state.recent_change_probability or 0.0
    stale_ratio = state.recent_stale_ratio or 0.0
    priority = (
        (0.25 * activity)
        + (0.20 * frequency)
        + (0.25 * change_probability)
        + (0.20 * stale_ratio)
        + (0.10 * state.collection_priority)
    )
    reasons: list[str] = []
    if baseline:
        interval = config.fallback_interval_days
        reasons.append("recent_sampling_baseline")
    elif state.recent30_video_count == 0:
        interval = 60
        reasons.append("recent_pool_empty")
    elif priority >= 0.80:
        interval = 3
        reasons.append("recent_sampling_priority_very_high")
    elif priority >= 0.60:
        interval = 7
        reasons.append("recent_sampling_priority_high")
    elif priority >= 0.40:
        interval = 14
        reasons.append("recent_sampling_priority_medium")
    elif priority >= 0.20:
        interval = 30
        reasons.append("recent_sampling_priority_low")
    else:
        interval = 60
        reasons.append("recent_sampling_priority_very_low")
    if outcome == "partial":
        interval = min(interval, config.partial_retry_days)
        reasons.append("partial_retry_cap")
    return VideoRiskCandidate(
        interval_days=interval,
        reason_codes=tuple(dict.fromkeys(reasons)),
        feature_summary={
            "recent_sampling_priority": round(priority, 6),
            "channel_activity": round(activity, 6),
            "publish_frequency_score": frequency,
            "recent_change_probability": round(change_probability, 6),
            "recent_stale_ratio": round(stale_ratio, 6),
            "recent_sampling_stable_runs": state.recent_sampling_stable_runs,
        },
    )


def decide_video_due(
    state: ChannelFeatureState,
    *,
    observed_at: datetime,
    discovery_outcome: str,
    recent_sampling_outcome: str,
    discovery_baseline: bool,
    recent_sampling_baseline: bool,
    discovery_config: DiscoveryPolicyConfig = DiscoveryPolicyConfig(),
    recent_sampling_config: RecentSamplingPolicyConfig = RecentSamplingPolicyConfig(),
) -> VideoClockDecision:
    if discovery_config.policy_version != recent_sampling_config.policy_version:
        raise ValueError("Video risk policies must use the same policy version")
    discovery = evaluate_video_discovery_risk(
        state,
        observed_at=observed_at,
        outcome=discovery_outcome,
        baseline=discovery_baseline,
        config=discovery_config,
    )
    if recent_sampling_outcome == "skipped":
        sampling = VideoRiskCandidate(
            interval_days=recent_sampling_config.partial_retry_days,
            reason_codes=("recent_sampling_skipped",),
            feature_summary={
                "recent_sampling_skipped": True,
                "recent_sampling_stable_runs": state.recent_sampling_stable_runs,
            },
        )
    elif recent_sampling_outcome == "failed":
        sampling = VideoRiskCandidate(
            interval_days=recent_sampling_config.partial_retry_days,
            reason_codes=("recent_sampling_failed_retry_cap",),
            feature_summary={
                "recent_sampling_failure": True,
                "recent_sampling_stable_runs": state.recent_sampling_stable_runs,
            },
        )
    else:
        sampling = evaluate_video_sampling_risk(
            state,
            outcome=recent_sampling_outcome,
            baseline=recent_sampling_baseline,
            config=recent_sampling_config,
        )
    selected = (
        discovery
        if discovery.interval_days <= sampling.interval_days
        else sampling
    )
    selected_basis = (
        "discovery"
        if discovery.interval_days <= sampling.interval_days
        else "recent_sampling"
    )
    unconstrained_interval = selected.interval_days
    interval = max(unconstrained_interval, discovery_config.automatic_min_interval_days)
    reasons = [
        *discovery.reason_codes,
        *sampling.reason_codes,
        f"video_interval_constrained_by_{selected_basis}",
    ]
    if interval > unconstrained_interval:
        reasons.append("automatic_video_min_interval")
    return _decision(
        policy_version=discovery_config.policy_version,
        observed_at=observed_at,
        interval=interval,
        reasons=reasons,
        summary={
            "selected_risk_basis": selected_basis,
            "unconstrained_interval_days": unconstrained_interval,
            "automatic_min_interval_days": discovery_config.automatic_min_interval_days,
            "discovery_risk": {
                "interval_days": discovery.interval_days,
                "outcome": discovery_outcome,
                "summary": discovery.feature_summary,
            },
            "sampling_risk": {
                "interval_days": sampling.interval_days,
                "outcome": recent_sampling_outcome,
                "summary": sampling.feature_summary,
            },
        },
    )


def decide_agent_due(
    state: ChannelFeatureState,
    *,
    observed_at: datetime,
    outcome: str,
    baseline: bool,
    output_changed: bool,
    evidence_count: int,
    channel_id: str | None = None,
    config: AgentPolicyConfig = AgentPolicyConfig(),
) -> AgentClockDecision:
    if outcome not in {"complete", "partial"}:
        raise ValueError("Agent policy only runs for complete or partial observations")
    cold_start_pressure = _cold_start_pressure(state)
    semantic_change = _agent_semantic_change_score(state)
    reasons: list[str] = []
    if baseline:
        interval = config.baseline_interval_days
        reasons.append("agent_semantic_baseline")
    elif state.agent_version_changed:
        interval = config.baseline_interval_days
        reasons.append("agent_cross_version_baseline")
    elif semantic_change is None:
        interval = config.baseline_interval_days
        reasons.append("agent_semantic_comparison_unavailable")
    else:
        interval = _agent_semantic_interval_days(semantic_change)
        reasons.append("agent_semantic_continuous_interval")
    decision = _decision(
        policy_version=config.policy_version,
        observed_at=observed_at,
        interval=interval,
        reasons=reasons,
        summary={
            "topic_drift": state.topic_drift,
            "evidence_replacement": state.evidence_replacement,
            "recent_content_shift": state.recent_content_shift,
            "version_changed": state.agent_version_changed,
            "agent_change_score": state.agent_change_score,
            "agent_semantic_change_score": semantic_change,
            "agent_semantic_curve_version": AGENT_SEMANTIC_CURVE_VERSION,
            "agent_semantic_curve_exponent": AGENT_SEMANTIC_CURVE_EXPONENT,
            "agent_semantic_min_interval_days": AGENT_SEMANTIC_MIN_INTERVAL_DAYS,
            "agent_semantic_max_interval_days": AGENT_SEMANTIC_MAX_INTERVAL_DAYS,
            "agent_semantic_comparison_available": (
                not baseline
                and not state.agent_version_changed
                and semantic_change is not None
            ),
            "agent_confidence": state.agent_confidence,
            "agent_stable_runs": state.agent_stable_runs,
            "output_changed": output_changed,
            "evidence_count": evidence_count,
            "topic_vector_source": state.agent_topic_vector_source,
            "category_vector_fallback": (
                state.agent_topic_vector_source is not None
                and "category_vector_fallback" in state.agent_topic_vector_source
            ),
            "cold_start_pressure": cold_start_pressure,
        },
    )
    if channel_id is None:
        return decision
    return _spread_agent_decision(decision, channel_id=channel_id)
