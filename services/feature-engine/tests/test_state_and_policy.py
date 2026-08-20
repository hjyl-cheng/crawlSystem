from __future__ import annotations

from datetime import date, datetime, timezone
import json
from hashlib import sha256
import unittest
from uuid import uuid4

from feature_engine.events import CrawlerObservationRecorded
from feature_engine.policy import AboutPolicyConfig, decide_about_due
from feature_engine.state import (
    AboutFeatureState,
    apply_about_event,
    apply_about_stability_evidence,
)


def parsed_event(
    *,
    observed_at: str,
    subscriber_count: int | None,
    subscriber_status: str,
    total_view_count: int | None,
    view_status: str,
    total_video_count: int | None,
    video_status: str,
    outcome: str,
    sequence: int,
) -> CrawlerObservationRecorded:
    facts = {
        "subscriber_count": subscriber_count,
        "subscriber_count_status": subscriber_status,
        "total_view_count": total_view_count,
        "total_view_count_status": view_status,
        "total_video_count": total_video_count,
        "total_video_count_status": video_status,
    }
    body = json.dumps(facts, separators=(",", ":"))
    return CrawlerObservationRecorded.from_mapping(
        {
            "event_id": str(uuid4()),
            "event_type": "crawler.observation.recorded",
            "event_version": 1,
            "observation_id": str(uuid4()),
            "channel_id": "UCexample",
            "observation_kind": "about",
            "kind_sequence": sequence,
            "observed_at": observed_at,
            "outcome": outcome,
            "crawler_version": "qy-v16",
            "payload_hash": f"sha256:{sha256(body.encode()).hexdigest()}",
            "payload": facts,
        }
    )


class AboutStateTests(unittest.TestCase):
    def test_first_event_only_establishes_a_baseline(self) -> None:
        transition = apply_about_event(
            AboutFeatureState(),
            parsed_event(
                observed_at="2026-07-20T00:00:00Z",
                subscriber_count=1000,
                subscriber_status="exact",
                total_view_count=10000,
                view_status="exact",
                total_video_count=20,
                video_status="exact",
                outcome="complete",
                sequence=1,
            ),
        )
        self.assertTrue(transition.baseline)
        self.assertIsNone(transition.state.subscriber_velocity_ewma)
        self.assertIsNone(transition.state.view_velocity_ewma)
        self.assertIsNone(transition.state.video_count_delta)
        self.assertEqual(transition.state.about_metric_confidence, 1.0)
        self.assertEqual(transition.state.state_version, 1)

    def test_second_event_computes_daily_velocity_and_ewma(self) -> None:
        first = apply_about_event(
            AboutFeatureState(),
            parsed_event(
                observed_at="2026-07-20T00:00:00Z",
                subscriber_count=1000,
                subscriber_status="exact",
                total_view_count=10000,
                view_status="exact",
                total_video_count=20,
                video_status="exact",
                outcome="complete",
                sequence=1,
            ),
        ).state
        second = apply_about_event(
            first,
            parsed_event(
                observed_at="2026-07-22T00:00:00Z",
                subscriber_count=1200,
                subscriber_status="exact",
                total_view_count=11000,
                view_status="exact",
                total_video_count=22,
                video_status="exact",
                outcome="complete",
                sequence=2,
            ),
        ).state
        self.assertEqual(second.subscriber_velocity_ewma, 100.0)
        self.assertEqual(second.view_velocity_ewma, 500.0)
        self.assertEqual(second.video_count_delta, 2)

    def test_partial_metrics_keep_independent_observation_times(self) -> None:
        state = AboutFeatureState(
            last_subscriber_count=100,
            last_subscriber_observed_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
            last_total_view_count=1000,
            last_total_view_observed_at=datetime(2026, 7, 10, tzinfo=timezone.utc),
            last_about_observed_at=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )
        result = apply_about_event(
            state,
            parsed_event(
                observed_at="2026-07-20T00:00:00Z",
                subscriber_count=290,
                subscriber_status="exact",
                total_view_count=None,
                view_status="unavailable",
                total_video_count=None,
                video_status="unavailable",
                outcome="partial",
                sequence=2,
            ),
        ).state
        self.assertEqual(result.subscriber_velocity_ewma, 10.0)
        self.assertEqual(result.last_total_view_observed_at, datetime(2026, 7, 10, tzinfo=timezone.utc))

    def test_older_source_time_never_regresses_current_state(self) -> None:
        state = AboutFeatureState(
            last_subscriber_count=200,
            last_subscriber_observed_at=datetime(2026, 7, 20, tzinfo=timezone.utc),
            last_about_observed_at=datetime(2026, 7, 20, tzinfo=timezone.utc),
            state_version=3,
        )
        result = apply_about_event(
            state,
            parsed_event(
                observed_at="2026-07-19T00:00:00Z",
                subscriber_count=100,
                subscriber_status="exact",
                total_view_count=None,
                view_status="unavailable",
                total_video_count=None,
                video_status="unavailable",
                outcome="partial",
                sequence=4,
            ),
        ).state
        self.assertEqual(result.last_subscriber_count, 200)
        self.assertEqual(result.state_version, 3)

    def test_missing_video_metric_does_not_reuse_an_old_positive_delta(self) -> None:
        state = AboutFeatureState(
            last_subscriber_count=100,
            last_subscriber_observed_at=datetime(2026, 7, 19, tzinfo=timezone.utc),
            last_total_video_count=20,
            last_total_video_observed_at=datetime(2026, 7, 19, tzinfo=timezone.utc),
            last_about_observed_at=datetime(2026, 7, 19, tzinfo=timezone.utc),
            video_count_delta=2,
        )
        result = apply_about_event(
            state,
            parsed_event(
                observed_at="2026-07-20T00:00:00Z",
                subscriber_count=110,
                subscriber_status="exact",
                total_view_count=None,
                view_status="unavailable",
                total_video_count=None,
                video_status="unavailable",
                outcome="partial",
                sequence=2,
            ),
        ).state
        self.assertIsNone(result.video_count_delta)

    def test_stability_evidence_uses_low_growth_instead_of_exactly_equal_totals(self) -> None:
        stable_since = datetime(2026, 1, 1, tzinfo=timezone.utc)
        previous = AboutFeatureState(
            about_stable_since=stable_since,
            about_stable_runs=4,
        )
        low_growth = apply_about_stability_evidence(
            previous,
            AboutFeatureState(
                subscriber_growth_percentile=0.20,
                view_growth_percentile=0.30,
                video_count_delta=0,
            ),
            observed_at=datetime(2026, 7, 20, tzinfo=timezone.utc),
            outcome="complete",
            baseline=False,
        )

        self.assertEqual(low_growth.about_stable_runs, 5)
        self.assertEqual(low_growth.about_stable_since, stable_since)

        changed = apply_about_stability_evidence(
            low_growth,
            AboutFeatureState(
                subscriber_growth_percentile=0.20,
                view_growth_percentile=0.30,
                video_count_delta=1,
            ),
            observed_at=datetime(2026, 7, 21, tzinfo=timezone.utc),
            outcome="complete",
            baseline=False,
        )

        self.assertEqual(changed.about_stable_runs, 0)
        self.assertEqual(
            changed.about_stable_since,
            datetime(2026, 7, 21, tzinfo=timezone.utc),
        )


class AboutPolicyTests(unittest.TestCase):
    def test_due_day_uses_the_utc_calendar_day(self) -> None:
        decision = decide_about_due(
            AboutFeatureState(),
            observed_at=datetime.fromisoformat("2026-07-21T00:30:00+08:00"),
            outcome="complete",
            baseline=True,
        )

        self.assertEqual(decision.due_day, date(2026, 7, 27))

    def test_baseline_uses_seven_day_fallback(self) -> None:
        observed = datetime(2026, 7, 20, tzinfo=timezone.utc)
        decision = decide_about_due(
            AboutFeatureState(last_about_observed_at=observed),
            observed_at=observed,
            outcome="complete",
            baseline=True,
        )
        self.assertEqual(decision.tier_days, 7)
        self.assertEqual(decision.due_day.isoformat(), "2026-07-27")

    def test_critical_growth_maps_to_one_day(self) -> None:
        observed = datetime(2026, 7, 20, tzinfo=timezone.utc)
        state = AboutFeatureState(
            subscriber_growth_percentile=0.91,
            view_growth_percentile=0.50,
            last_about_observed_at=observed,
        )
        decision = decide_about_due(
            state,
            observed_at=observed,
            outcome="complete",
            baseline=False,
        )
        self.assertEqual(decision.tier_days, 1)

    def test_partial_result_is_capped_at_three_days(self) -> None:
        observed = datetime(2026, 7, 20, tzinfo=timezone.utc)
        state = AboutFeatureState(
            subscriber_growth_percentile=0.10,
            view_growth_percentile=0.10,
            last_about_observed_at=observed,
            about_stable_since=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        decision = decide_about_due(
            state,
            observed_at=observed,
            outcome="partial",
            baseline=False,
            config=AboutPolicyConfig(partial_retry_days=3),
        )
        self.assertEqual(decision.tier_days, 3)
        self.assertIn("partial_retry_cap", decision.reason_codes)

    def test_late_observation_still_uses_its_utc_calendar_day(self) -> None:
        observed = datetime(2026, 7, 20, 23, 50, tzinfo=timezone.utc)
        decision = decide_about_due(
            AboutFeatureState(last_about_observed_at=observed),
            observed_at=observed,
            outcome="complete",
            baseline=True,
        )

        self.assertEqual(
            decision.due_at,
            datetime(2026, 7, 27, 0, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(decision.due_day, date(2026, 7, 27))
        self.assertNotIn("clock_safe_window_shift", decision.reason_codes)


if __name__ == "__main__":
    unittest.main()
