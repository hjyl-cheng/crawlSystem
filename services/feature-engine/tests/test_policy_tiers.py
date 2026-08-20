from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from feature_engine.policy import (
    AboutPolicyConfig,
    AgentPolicyConfig,
    DiscoveryPolicyConfig,
    agent_forward_spread_max_days,
    decide_about_due,
    decide_agent_due,
    decide_video_due,
    evaluate_video_discovery_risk,
    evaluate_video_sampling_risk,
    limit_about_slowdown,
    stable_agent_forward_offset,
)
from feature_engine.state import ChannelFeatureState


OBSERVED_AT = datetime(2026, 7, 20, tzinfo=timezone.utc)


class AboutTierTests(unittest.TestCase):
    def state(
        self,
        subscriber: float,
        views: float,
        *,
        video_delta: int = 0,
        collection_priority: float = 0.0,
        stable_days: int = 100,
        recent30_video_count: int | None = None,
    ) -> ChannelFeatureState:
        return ChannelFeatureState(
            subscriber_growth_percentile=subscriber,
            view_growth_percentile=views,
            video_count_delta=video_delta,
            collection_priority=collection_priority,
            about_stable_since=OBSERVED_AT - timedelta(days=stable_days),
            recent30_video_count=recent30_video_count,
        )

    def test_every_about_tier_and_hard_cap(self) -> None:
        cases = (
            ("critical", self.state(0.90, 0.10), "complete", 1),
            ("very-high", self.state(0.89, 0.89, video_delta=3), "complete", 1),
            (
                "high",
                self.state(0.75, 0.65, collection_priority=0.50),
                "complete",
                3,
            ),
            ("medium", self.state(0.50, 0.50), "complete", 7),
            ("low", self.state(0.30, 0.30), "complete", 14),
            ("very-low", self.state(0.10, 0.10), "complete", 30),
            ("stability-cap", self.state(0.10, 0.10, stable_days=10), "complete", 7),
            (
                "video-increase-cap",
                self.state(0.10, 0.10, video_delta=1, recent30_video_count=1),
                "complete",
                3,
            ),
            ("partial-cap", self.state(0.10, 0.10), "partial", 3),
        )
        for name, state, outcome, expected in cases:
            with self.subTest(name=name):
                decision = decide_about_due(
                    state,
                    observed_at=OBSERVED_AT,
                    outcome=outcome,
                    baseline=False,
                )
                self.assertEqual(decision.tier_days, expected)

    def test_high_activity_scaled_baseline_enters_the_one_day_tier(self) -> None:
        decision = decide_about_due(
            ChannelFeatureState(
                last_subscriber_count=9_400,
                recent30_video_count=30,
                recent_publish_interval_days=(0.042, 0.043, 0.041),
                publish_interval_ewma=0.042,
                publish_interval_median=0.042,
                last_publish_at=OBSERVED_AT - timedelta(hours=1),
                feature_confidence=0.916667,
                collection_priority=0.316875,
            ),
            observed_at=OBSERVED_AT,
            outcome="complete",
            baseline=True,
        )

        self.assertEqual(decision.tier_days, 1)
        self.assertEqual(decision.feature_summary["about_priority"], 0.75)
        self.assertIn("cold_start_high_activity_scale_floor", decision.reason_codes)

    def test_daily_publish_cadence_keeps_mature_about_clock_in_one_day_tier(self) -> None:
        decision = decide_about_due(
            ChannelFeatureState(
                subscriber_growth_percentile=0.10,
                view_growth_percentile=0.10,
                recent30_video_count=30,
                recent_publish_interval_days=(1.0, 0.9, 1.0),
                publish_interval_ewma=0.97,
                publish_interval_median=1.0,
                last_publish_at=OBSERVED_AT - timedelta(hours=12),
                about_metric_confidence=1.0,
                about_stable_since=OBSERVED_AT - timedelta(days=400),
                about_stable_runs=20,
            ),
            observed_at=OBSERVED_AT,
            outcome="complete",
            baseline=False,
            config=AboutPolicyConfig(
                policy_version="v16-rule-4",
                cadence_baseline_enabled=True,
            ),
        )

        self.assertEqual(decision.tier_days, 1)
        self.assertIn("about_active_cadence_cap_1d", decision.reason_codes)

    def test_high_activity_small_baseline_keeps_the_seven_day_fallback(self) -> None:
        decision = decide_about_due(
            ChannelFeatureState(
                last_subscriber_count=4_999,
                recent30_video_count=30,
                recent_publish_interval_days=(0.5, 0.5, 0.5),
                publish_interval_ewma=0.5,
                publish_interval_median=0.5,
                last_publish_at=OBSERVED_AT - timedelta(hours=1),
                feature_confidence=1.0,
            ),
            observed_at=OBSERVED_AT,
            outcome="complete",
            baseline=True,
        )

        self.assertEqual(decision.tier_days, 7)
        self.assertNotIn("cold_start_high_activity_scale_floor", decision.reason_codes)

    def test_cold_start_uses_about_metric_confidence_not_a_later_domain(self) -> None:
        decision = decide_about_due(
            ChannelFeatureState(
                last_subscriber_count=9_400,
                recent30_video_count=30,
                recent_publish_interval_days=(0.5, 0.5, 0.5),
                publish_interval_ewma=0.5,
                publish_interval_median=0.5,
                last_publish_at=OBSERVED_AT - timedelta(hours=1),
                about_metric_confidence=0.5,
                feature_confidence=1.0,
            ),
            observed_at=OBSERVED_AT,
            outcome="complete",
            baseline=True,
        )

        self.assertEqual(decision.tier_days, 7)
        self.assertFalse(
            decision.feature_summary["cold_start"]["confidence_qualified"]
        )

    def test_dynamic_baseline_maps_current_pressure_to_different_tiers(self) -> None:
        config = AboutPolicyConfig(
            policy_version="v16-rule-3",
            dynamic_baseline_enabled=True,
        )
        shared = {
            "recent30_video_count": 30,
            "last_publish_at": OBSERVED_AT - timedelta(hours=1),
            "about_metric_confidence": 1.0,
            "channel_activity": 0.825,
            "collection_priority": 0.35625,
        }
        slower = decide_about_due(
            ChannelFeatureState(
                **shared,
                last_subscriber_count=19_000,
                publish_interval_ewma=2.4823,
                publish_interval_median=2.0,
            ),
            observed_at=OBSERVED_AT,
            outcome="complete",
            baseline=True,
            config=config,
        )
        faster = decide_about_due(
            ChannelFeatureState(
                **shared,
                last_subscriber_count=109_000,
                publish_interval_ewma=0.1012,
                publish_interval_median=0.038,
            ),
            observed_at=OBSERVED_AT,
            outcome="complete",
            baseline=True,
            config=config,
        )

        self.assertEqual(slower.tier_days, 3)
        self.assertEqual(faster.tier_days, 1)

    def test_cadence_baseline_uses_the_full_early_about_ladder(self) -> None:
        config = AboutPolicyConfig(
            policy_version="v16-rule-4",
            cadence_baseline_enabled=True,
            cold_start_max_publish_age_days=7.0,
        )
        cases = (
            (1.0, 1),
            (2.289383, 2),
            (3.0, 3),
            (5.0, 5),
            (8.0, 7),
        )
        for publish_interval, expected_tier in cases:
            with self.subTest(publish_interval=publish_interval):
                decision = decide_about_due(
                    ChannelFeatureState(
                        last_subscriber_count=19_000,
                        recent30_video_count=13,
                        recent_publish_interval_days=(
                            publish_interval,
                            publish_interval,
                            publish_interval,
                        ),
                        publish_interval_ewma=publish_interval,
                        publish_interval_median=publish_interval,
                        last_publish_at=OBSERVED_AT - timedelta(days=1),
                        about_metric_confidence=1.0,
                    ),
                    observed_at=OBSERVED_AT,
                    outcome="complete",
                    baseline=True,
                    config=config,
                )

                self.assertEqual(decision.tier_days, expected_tier)
                self.assertEqual(
                    decision.feature_summary["cold_start"]["cadence_source"],
                    "publish_interval_history",
                )

    def test_cadence_baseline_prefers_reliable_intervals_over_recent_count(self) -> None:
        decision = decide_about_due(
            ChannelFeatureState(
                recent30_video_count=30,
                recent_publish_interval_days=(2.0, 2.0, 3.0),
                publish_interval_ewma=2.482305,
                publish_interval_median=2.0,
                last_publish_at=OBSERVED_AT - timedelta(days=1),
                about_metric_confidence=1.0,
            ),
            observed_at=OBSERVED_AT,
            outcome="complete",
            baseline=True,
            config=AboutPolicyConfig(
                policy_version="v16-rule-4",
                cadence_baseline_enabled=True,
                cold_start_max_publish_age_days=7.0,
            ),
        )

        self.assertEqual(decision.tier_days, 2)
        self.assertAlmostEqual(
            decision.feature_summary["cold_start"]["cadence_interval_days"],
            2.289383,
            places=6,
        )

    def test_first_about_tier_requires_at_least_one_video_per_day(self) -> None:
        config = AboutPolicyConfig(
            policy_version="v16-rule-5",
            cadence_baseline_enabled=True,
            cold_start_max_publish_age_days=7.0,
            cold_start_tier_one_max_publish_interval_days=1.0,
        )
        for publish_interval, expected_tier in (
            (1.0, 1),
            (1.000001, 2),
        ):
            with self.subTest(publish_interval=publish_interval):
                decision = decide_about_due(
                    ChannelFeatureState(
                        recent30_video_count=30,
                        recent_publish_interval_days=(
                            publish_interval,
                            publish_interval,
                            publish_interval,
                        ),
                        publish_interval_ewma=publish_interval,
                        publish_interval_median=publish_interval,
                        last_publish_at=OBSERVED_AT - timedelta(days=1),
                        about_metric_confidence=1.0,
                    ),
                    observed_at=OBSERVED_AT,
                    outcome="complete",
                    baseline=True,
                    config=config,
                )

                self.assertEqual(decision.tier_days, expected_tier)
                self.assertEqual(
                    decision.feature_summary["cold_start"]
                    ["tier_one_max_publish_interval_days"],
                    1.0,
                )
                self.assertIn(
                    f"about_cold_start_cadence_{expected_tier}d",
                    decision.reason_codes,
                )

    def test_cadence_baseline_can_infer_two_days_from_the_true_30_day_count(self) -> None:
        decision = decide_about_due(
            ChannelFeatureState(
                recent30_video_count=13,
                last_publish_at=OBSERVED_AT - timedelta(days=1),
                about_metric_confidence=1.0,
            ),
            observed_at=OBSERVED_AT,
            outcome="complete",
            baseline=True,
            config=AboutPolicyConfig(
                policy_version="v16-rule-4",
                cadence_baseline_enabled=True,
                cold_start_max_publish_age_days=7.0,
            ),
        )

        self.assertEqual(decision.tier_days, 2)
        self.assertEqual(
            decision.feature_summary["cold_start"]["cadence_source"],
            "recent_30_day_count",
        )

    def test_mature_policy_can_select_every_about_tier(self) -> None:
        config = AboutPolicyConfig(
            policy_version="v16-rule-4",
            cadence_baseline_enabled=True,
        )
        cases = (
            (0.90, 1),
            (0.87, 2),
            (0.74, 3),
            (0.60, 5),
            (0.47, 7),
            (0.34, 14),
            (0.20, 30),
            (0.134, 60),
            (0.067, 90),
            (0.0, 180),
        )
        for growth_percentile, expected_tier in cases:
            with self.subTest(expected_tier=expected_tier):
                decision = decide_about_due(
                    ChannelFeatureState(
                        subscriber_growth_percentile=growth_percentile,
                        view_growth_percentile=growth_percentile,
                        about_stable_since=OBSERVED_AT - timedelta(days=400),
                        about_stable_runs=20,
                    ),
                    observed_at=OBSERVED_AT,
                    outcome="complete",
                    baseline=False,
                    config=config,
                )

                self.assertEqual(decision.tier_days, expected_tier)

    def test_long_about_tiers_require_both_stable_time_and_runs(self) -> None:
        config = AboutPolicyConfig(
            policy_version="v16-rule-4",
            cadence_baseline_enabled=True,
        )
        cases = (
            (20, 20, 7),
            (21, 2, 7),
            (21, 3, 14),
            (60, 6, 30),
            (120, 9, 60),
            (180, 12, 90),
            (365, 15, 180),
        )
        for stable_days, stable_runs, expected_tier in cases:
            with self.subTest(stable_days=stable_days, stable_runs=stable_runs):
                decision = decide_about_due(
                    ChannelFeatureState(
                        subscriber_growth_percentile=0.0,
                        view_growth_percentile=0.0,
                        about_stable_since=OBSERVED_AT - timedelta(days=stable_days),
                        about_stable_runs=stable_runs,
                    ),
                    observed_at=OBSERVED_AT,
                    outcome="complete",
                    baseline=False,
                    config=config,
                )

                self.assertEqual(decision.tier_days, expected_tier)

    def test_about_slowdown_moves_one_tier_but_acceleration_is_immediate(self) -> None:
        slow_candidate = decide_about_due(
            ChannelFeatureState(
                subscriber_growth_percentile=0.0,
                view_growth_percentile=0.0,
                about_stable_since=OBSERVED_AT - timedelta(days=400),
                about_stable_runs=20,
            ),
            observed_at=OBSERVED_AT,
            outcome="complete",
            baseline=False,
            config=AboutPolicyConfig(
                policy_version="v16-rule-4",
                cadence_baseline_enabled=True,
            ),
        )
        limited = limit_about_slowdown(slow_candidate, previous_tier_days=2)

        self.assertEqual(limited.tier_days, 3)
        self.assertEqual(
            limited.feature_summary["unconstrained_interval_days"],
            180,
        )
        self.assertIn("about_slowdown_one_tier", limited.reason_codes)

        fast_candidate = decide_about_due(
            ChannelFeatureState(
                subscriber_growth_percentile=0.90,
                view_growth_percentile=0.90,
            ),
            observed_at=OBSERVED_AT,
            outcome="complete",
            baseline=False,
            config=AboutPolicyConfig(
                policy_version="v16-rule-4",
                cadence_baseline_enabled=True,
            ),
        )
        accelerated = limit_about_slowdown(fast_candidate, previous_tier_days=30)

        self.assertEqual(accelerated.tier_days, 1)


class VideoDiscoveryRiskTierTests(unittest.TestCase):
    def test_every_discovery_tier_and_cap(self) -> None:
        cases = (
            ("one", 1, "complete", 1),
            ("three", 2, "complete", 3),
            ("seven", 4, "complete", 7),
            ("fourteen", 8, "complete", 14),
            ("thirty", 15, "complete", 30),
            ("sixty", 31, "complete", 60),
            ("ninety", 61, "complete", 90),
            ("maximum", 365, "complete", 90),
            ("partial-cap", 365, "partial", 3),
        )
        for name, raw_days, outcome, expected in cases:
            with self.subTest(name=name):
                candidate = evaluate_video_discovery_risk(
                    ChannelFeatureState(),
                    observed_at=OBSERVED_AT,
                    outcome=outcome,
                    baseline=False,
                    config=DiscoveryPolicyConfig(fallback_interval_days=raw_days),
                )
                self.assertEqual(candidate.interval_days, expected)
                self.assertFalse(hasattr(candidate, "due_day"))


class VideoSamplingRiskTierTests(unittest.TestCase):
    def test_every_recent_sampling_tier_and_cap(self) -> None:
        cases = (
            ("baseline", ChannelFeatureState(), True, "complete", 14),
            (
                "very-high",
                ChannelFeatureState(
                    recent30_video_count=20,
                    channel_activity=1.0,
                    recent_change_probability=1.0,
                    recent_stale_ratio=1.0,
                    collection_priority=1.0,
                ),
                False,
                "complete",
                3,
            ),
            (
                "high",
                ChannelFeatureState(
                    recent30_video_count=10,
                    channel_activity=0.8,
                    recent_change_probability=0.8,
                    recent_stale_ratio=0.2,
                    collection_priority=0.0,
                ),
                False,
                "complete",
                7,
            ),
            (
                "medium",
                ChannelFeatureState(
                    recent30_video_count=5,
                    channel_activity=0.4,
                    recent_change_probability=0.4,
                    recent_stale_ratio=0.4,
                    collection_priority=0.0,
                ),
                False,
                "complete",
                14,
            ),
            (
                "low",
                ChannelFeatureState(
                    recent30_video_count=1,
                    channel_activity=0.2,
                    recent_change_probability=0.2,
                    recent_stale_ratio=0.35,
                    collection_priority=0.0,
                ),
                False,
                "complete",
                30,
            ),
            (
                "very-low",
                ChannelFeatureState(recent30_video_count=1, collection_priority=0.0),
                False,
                "complete",
                60,
            ),
            (
                "empty",
                ChannelFeatureState(recent30_video_count=0),
                False,
                "complete",
                60,
            ),
            (
                "partial-cap",
                ChannelFeatureState(recent30_video_count=1),
                False,
                "partial",
                7,
            ),
        )
        for name, state, baseline, outcome, expected in cases:
            with self.subTest(name=name):
                candidate = evaluate_video_sampling_risk(
                    state,
                    outcome=outcome,
                    baseline=baseline,
                )
                self.assertEqual(candidate.interval_days, expected)
                self.assertFalse(hasattr(candidate, "due_day"))


class VideoTierTests(unittest.TestCase):
    def test_automatic_video_clock_never_uses_the_one_day_risk_tier(self) -> None:
        decision = decide_video_due(
            ChannelFeatureState(
                recent_publish_interval_days=(0.042, 0.043, 0.041),
                publish_interval_ewma=0.042,
                publish_interval_median=0.042,
                publish_regularity=0.99,
                last_publish_at=OBSERVED_AT - timedelta(hours=1),
                recent30_video_count=30,
                channel_activity=0.825,
                recent_stale_ratio=1.0,
            ),
            observed_at=OBSERVED_AT,
            discovery_outcome="partial",
            recent_sampling_outcome="complete",
            discovery_baseline=True,
            recent_sampling_baseline=True,
        )

        self.assertEqual(decision.tier_days, 3)
        self.assertEqual(decision.feature_summary["unconstrained_interval_days"], 1)
        self.assertIn("automatic_video_min_interval", decision.reason_codes)

    def test_single_video_clock_uses_the_shorter_risk_interval(self) -> None:
        decision = decide_video_due(
            ChannelFeatureState(
                recent30_video_count=20,
                channel_activity=1.0,
                recent_change_probability=1.0,
                recent_stale_ratio=1.0,
                collection_priority=1.0,
            ),
            observed_at=OBSERVED_AT,
            discovery_outcome="complete",
            recent_sampling_outcome="complete",
            discovery_baseline=False,
            recent_sampling_baseline=False,
            discovery_config=DiscoveryPolicyConfig(fallback_interval_days=90),
        )
        self.assertEqual(decision.tier_days, 3)
        self.assertEqual(decision.due_day, OBSERVED_AT.date() + timedelta(days=3))
        self.assertEqual(decision.feature_summary["selected_risk_basis"], "recent_sampling")
        self.assertNotIn("due_day", decision.feature_summary["discovery_risk"])
        self.assertNotIn("due_day", decision.feature_summary["sampling_risk"])

    def test_failed_sampling_caps_the_single_video_clock(self) -> None:
        decision = decide_video_due(
            ChannelFeatureState(),
            observed_at=OBSERVED_AT,
            discovery_outcome="complete",
            recent_sampling_outcome="failed",
            discovery_baseline=False,
            recent_sampling_baseline=False,
            discovery_config=DiscoveryPolicyConfig(fallback_interval_days=90),
        )
        self.assertEqual(decision.tier_days, 7)
        self.assertIn("recent_sampling_failed_retry_cap", decision.reason_codes)


class AgentTierTests(unittest.TestCase):
    def test_agent_clock_calculates_a_continuous_semantic_interval(self) -> None:
        cases = (
            ("baseline", ChannelFeatureState(), True, False, 0, "complete", 180),
            (
                "version",
                ChannelFeatureState(
                    agent_version_changed=True,
                    topic_drift=1.0,
                    evidence_replacement=1.0,
                ),
                False,
                True,
                20,
                "complete",
                180,
            ),
            (
                "very-high",
                ChannelFeatureState(topic_drift=0.75),
                False,
                False,
                10,
                "complete",
                85,
            ),
            (
                "high",
                ChannelFeatureState(evidence_replacement=0.40),
                False,
                False,
                15,
                "complete",
                182,
            ),
            (
                "moderate",
                ChannelFeatureState(recent_content_shift=0.15),
                False,
                False,
                5,
                "complete",
                288,
            ),
            (
                "stable",
                ChannelFeatureState(
                    topic_drift=0.14,
                    evidence_replacement=0.05,
                    recent_content_shift=0.10,
                ),
                False,
                False,
                5,
                "complete",
                292,
            ),
            (
                "unavailable",
                ChannelFeatureState(),
                False,
                False,
                5,
                "complete",
                180,
            ),
            (
                "output-hash-only",
                ChannelFeatureState(topic_drift=0.0),
                False,
                True,
                5,
                "complete",
                365,
            ),
            (
                "partial-semantic-change",
                ChannelFeatureState(topic_drift=0.80),
                False,
                False,
                20,
                "partial",
                77,
            ),
        )
        for name, state, baseline, output_changed, evidence, outcome, expected in cases:
            with self.subTest(name=name):
                decision = decide_agent_due(
                    state,
                    observed_at=OBSERVED_AT,
                    outcome=outcome,
                    baseline=baseline,
                    output_changed=output_changed,
                    evidence_count=evidence,
                )
                self.assertEqual(decision.tier_days, expected)

    def test_nearby_semantic_scores_do_not_collapse_into_fixed_tiers(self) -> None:
        intervals = {
            decide_agent_due(
                ChannelFeatureState(topic_drift=score),
                observed_at=OBSERVED_AT,
                outcome="complete",
                baseline=False,
                output_changed=False,
                evidence_count=20,
            ).tier_days
            for score in (0.48, 0.49, 0.50, 0.51, 0.52)
        }

        self.assertGreaterEqual(len(intervals), 4)
        self.assertTrue(intervals.isdisjoint({60, 90, 180, 365}))

    def test_agent_baseline_is_long_even_for_an_active_channel(self) -> None:
        config = AgentPolicyConfig(
            policy_version="v16-rule-3",
            dynamic_baseline_enabled=True,
        )
        active = decide_agent_due(
            ChannelFeatureState(
                last_subscriber_count=19_000,
                publish_interval_ewma=2.4823,
                publish_interval_median=2.0,
                channel_activity=0.825,
            ),
            observed_at=OBSERVED_AT,
            outcome="complete",
            baseline=True,
            output_changed=False,
            evidence_count=10,
            config=config,
        )
        unknown = decide_agent_due(
            ChannelFeatureState(
                last_subscriber_count=860_000,
                channel_activity=0.1875,
            ),
            observed_at=OBSERVED_AT,
            outcome="complete",
            baseline=True,
            output_changed=False,
            evidence_count=10,
            config=config,
        )

        self.assertEqual(active.tier_days, 180)
        self.assertEqual(unknown.tier_days, 180)

    def test_incremental_agent_due_uses_only_forward_stable_spread(self) -> None:
        for tier, state in (
            (85, ChannelFeatureState(topic_drift=0.75)),
            (182, ChannelFeatureState(topic_drift=0.40)),
            (288, ChannelFeatureState(topic_drift=0.15)),
            (365, ChannelFeatureState(topic_drift=0.0)),
        ):
            with self.subTest(tier=tier):
                decision = decide_agent_due(
                    state,
                    observed_at=OBSERVED_AT,
                    outcome="complete",
                    baseline=False,
                    output_changed=False,
                    evidence_count=20,
                    channel_id="UC-agent-spread",
                )
                offset = stable_agent_forward_offset(
                    "UC-agent-spread",
                    policy_version=decision.policy_version,
                    tier_days=tier,
                )

                self.assertEqual(decision.tier_days, tier)
                self.assertGreaterEqual(offset, 0)
                self.assertLessEqual(offset, agent_forward_spread_max_days(tier))
                self.assertEqual(
                    decision.due_day,
                    OBSERVED_AT.date() + timedelta(days=tier + offset),
                )
                self.assertEqual(
                    decision.feature_summary["agent_forward_spread_days"], offset
                )
                self.assertIn("agent_forward_load_spread", decision.reason_codes)

    def test_agent_version_change_restarts_a_long_spread_baseline(self) -> None:
        decision = decide_agent_due(
            ChannelFeatureState(agent_version_changed=True),
            observed_at=OBSERVED_AT,
            outcome="complete",
            baseline=False,
            output_changed=False,
            evidence_count=20,
            channel_id="UC-agent-urgent",
        )

        offset = stable_agent_forward_offset(
            "UC-agent-urgent",
            policy_version=decision.policy_version,
            tier_days=180,
        )
        self.assertEqual(decision.tier_days, 180)
        self.assertEqual(
            decision.due_day,
            OBSERVED_AT.date() + timedelta(days=180 + offset),
        )
        self.assertIn("agent_cross_version_baseline", decision.reason_codes)
        self.assertIn("agent_forward_load_spread", decision.reason_codes)

    def test_agent_forward_spread_is_repeatable_and_distributed(self) -> None:
        offsets = {
            stable_agent_forward_offset(
                f"UC-agent-{index}",
                policy_version="v16-rule-7",
                tier_days=148,
            )
            for index in range(100)
        }

        self.assertGreater(len(offsets), 10)
        maximum = agent_forward_spread_max_days(148)
        self.assertTrue(offsets.issubset(set(range(maximum + 1))))


if __name__ == "__main__":
    unittest.main()
