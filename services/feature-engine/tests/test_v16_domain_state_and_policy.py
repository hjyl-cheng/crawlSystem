from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest
from uuid import uuid4

from feature_engine.events import CrawlerObservationRecorded, canonical_payload_hash
from feature_engine.policy import (
    decide_agent_due,
    decide_video_due,
    evaluate_video_discovery_risk,
    evaluate_video_sampling_risk,
)
from feature_engine.state import (
    ChannelFeatureState,
    apply_agent_event,
    apply_video_event,
)


def parsed(
    kind: str,
    payload: dict,
    *,
    outcome: str = "complete",
    observed_at: str = "2026-07-20T12:00:00Z",
    sequence: int = 1,
) -> CrawlerObservationRecorded:
    return CrawlerObservationRecorded.from_mapping(
        {
            "event_id": str(uuid4()),
            "event_type": "crawler.observation.recorded",
            "event_version": 1,
            "observation_id": str(uuid4()),
            "channel_id": "UCrolling",
            "observation_kind": kind,
            "kind_sequence": sequence,
            "observed_at": observed_at,
            "outcome": outcome,
            "crawler_version": "qy-v16",
            "payload_hash": canonical_payload_hash(payload),
            "payload": payload,
        }
    )


def discovery_payload(*, partial: bool = False) -> dict:
    first_seen = [] if partial else [
        {
            "video_id": "newer",
            "position": 1,
            "content_type": "video",
            "published_at": "2026-07-20T10:00:00Z",
            "published_at_precision": "second",
        },
        {
            "video_id": "older",
            "position": 2,
            "content_type": "video",
            "published_at": "2026-07-17T10:00:00Z",
            "published_at_precision": "second",
        },
    ]
    return {
        "pages": 2,
        "items": len(first_seen),
        "anchor_matched": not partial,
        "stop_reason": "max_pages" if partial else "anchor_matched",
        "parse_gap_count": 0,
        "first_seen": first_seen,
        "first_seen_count": len(first_seen),
        "detail_success_count": len(first_seen),
        "detail_failure_count": 0,
    }


def recent_payload(*, partial: bool = False) -> dict:
    return {
        "recent_count": 8,
        "stale_ratio": 0.5,
        "selected_count": 4,
        "success_count": 3 if partial else 4,
        "failure_count": 1 if partial else 0,
        "next_count": 1,
        "comparable_view_count": 3 if partial else 4,
        "view_changed_count": 2,
        "view_delta_total": 250,
        "engagement_changed_count": 1,
    }


def video_payload(
    *,
    discovery: dict | None = None,
    recent_sampling: dict | None = None,
    discovery_outcome: str = "complete",
    recent_sampling_outcome: str = "complete",
) -> dict:
    return {
        "discovery": {
            "outcome": discovery_outcome,
            "payload": discovery or discovery_payload(),
        },
        "recent_sampling": {
            "outcome": recent_sampling_outcome,
            "payload": recent_sampling or recent_payload(),
        },
    }


def catchup_limited_video_payload() -> dict:
    return {
        "discovery": {
            "outcome": "partial",
            "payload": {
                "pages": 2,
                "items": 3,
                "first_page_item_count": 2,
                "catch_up_item_count": 1,
                "anchor_matched": False,
                "stop_reason": "catchup_limit",
                "parse_gap_count": 0,
                "unclosed_video_ids": ["new-1", "new-2", "older-1"],
                "first_seen": [],
                "first_seen_count": 0,
                "detail_success_count": 0,
                "detail_failure_count": 0,
            },
        },
        "recent_sampling": {
            "outcome": "skipped",
            "payload": {"skipped_reason": "discovery_incomplete"},
        },
    }


def gap_abandoned_latest_30_video_payload() -> dict:
    scanned_video_ids = [f"scanned-{index:03d}" for index in range(1, 151)]
    selected_video_ids = scanned_video_ids[:30]
    discovery = discovery_payload()
    discovery.update(
        {
            "items": 30,
            "anchor_matched": False,
            "stop_reason": "gap_abandoned_latest_30",
            "gap_abandonment": {
                "policy_version": "latest-30-on-catchup-limit-v1",
                "source_stop_reason": "catchup_limit",
                "scanned_item_count": 150,
                "first_page_item_count": 100,
                "catch_up_item_count": 50,
                "catch_up_item_limit": 50,
                "selected_item_count": 30,
                "scanned_video_ids": scanned_video_ids,
                "selected_video_ids": selected_video_ids,
                "abandoned_anchor_ids": ["old-anchor-1", "old-anchor-2"],
            },
        }
    )
    discovery["first_seen"][0]["video_id"] = selected_video_ids[0]
    discovery["first_seen"][1]["video_id"] = selected_video_ids[1]
    return video_payload(discovery=discovery)


def decide_video(transition, source):
    return decide_video_due(
        transition.state,
        observed_at=source.observed_at,
        discovery_outcome=transition.discovery_outcome,
        recent_sampling_outcome=transition.recent_sampling_outcome,
        discovery_baseline=transition.discovery_baseline,
        recent_sampling_baseline=transition.recent_sampling_baseline,
    )


def agent_payload(output: str = "a", categories: tuple[str, ...] = ("AI", "Software")) -> dict:
    return {
        "output_hash": f"sha256:{output * 64}",
        "category_level_1": "Technology" if output == "a" else "Entertainment",
        "category_level_2": list(categories),
        "tag_count": 10,
        "evidence_count": 18 if output == "a" else 30,
        "active_subscriber_ratio": 35,
        "fulfilled_plan_count": 1,
    }


def extended_agent_payload(
    *,
    output: str,
    topics: tuple[str, ...],
    evidence: tuple[str, ...],
    version: str,
) -> dict:
    payload = agent_payload(output, topics)
    payload["evidence_count"] = len(evidence)
    payload.update(
        {
            "topic_tokens": list(topics),
            "evidence_fingerprints": [f"sha256:{value * 64}" for value in evidence],
            "agent_version_hash": f"sha256:{version * 64}",
        }
    )
    return payload


class VideoStateAndPolicyTests(unittest.TestCase):
    def test_discovery_builds_publish_intervals_and_predicts_next_check(self) -> None:
        source = parsed("video", video_payload())
        transition = apply_video_event(ChannelFeatureState(), source)
        decision = decide_video(transition, source)
        self.assertEqual(transition.state.recent_publish_interval_days, (3.0,))
        self.assertEqual(transition.state.publish_regularity, 1.0)
        self.assertEqual(decision.tier_days, 3)

    def test_elapsed_regular_publish_window_uses_silence_backoff(self) -> None:
        observed = datetime(2026, 7, 20, tzinfo=timezone.utc)
        candidate = evaluate_video_discovery_risk(
            ChannelFeatureState(
                publish_interval_ewma=7.0,
                publish_interval_median=7.0,
                publish_regularity=0.9,
                last_publish_at=observed - timedelta(days=100),
                new_video_empty_runs=4,
                channel_activity=0.1,
            ),
            observed_at=observed,
            outcome="complete",
            baseline=False,
        )

        self.assertEqual(candidate.interval_days, 90)
        self.assertIn("regular_publish_window_elapsed", candidate.reason_codes)
        self.assertIn("empty_run_backoff", candidate.reason_codes)

    def test_discovery_does_not_claim_empty_run_backoff_after_finding_video(self) -> None:
        observed = datetime(2026, 7, 20, tzinfo=timezone.utc)
        candidate = evaluate_video_discovery_risk(
            ChannelFeatureState(
                publish_interval_ewma=2.0,
                publish_interval_median=1.0,
                publish_regularity=0.2,
                last_publish_at=observed - timedelta(hours=12),
                new_video_empty_runs=0,
                channel_activity=0.8,
            ),
            observed_at=observed,
            outcome="complete",
            baseline=False,
        )

        self.assertNotIn("empty_run_backoff", candidate.reason_codes)

    def test_partial_discovery_is_capped_at_three_days(self) -> None:
        source = parsed(
            "video",
            video_payload(
                discovery=discovery_payload(partial=True),
                discovery_outcome="partial",
            ),
            outcome="partial",
        )
        transition = apply_video_event(ChannelFeatureState(), source)
        decision = decide_video(transition, source)
        self.assertEqual(decision.tier_days, 3)
        self.assertIn("partial_retry_cap", decision.reason_codes)

    def test_catchup_limit_preserves_sampling_state_and_schedules_repair(self) -> None:
        last_sampling_at = datetime(2026, 7, 19, tzinfo=timezone.utc)
        source = parsed(
            "video",
            catchup_limited_video_payload(),
            outcome="partial",
        )

        transition = apply_video_event(
            ChannelFeatureState(
                last_recent_sampling_at=last_sampling_at,
                recent30_video_count=8,
                last_recent_sample_count=4,
            ),
            source,
        )
        decision = decide_video(transition, source)

        self.assertEqual(transition.state.last_recent_sampling_at, last_sampling_at)
        self.assertEqual(transition.state.recent30_video_count, 8)
        self.assertEqual(transition.state.last_recent_sample_count, 4)
        self.assertEqual(transition.recent_sampling_outcome, "skipped")
        self.assertEqual(decision.tier_days, 3)
        self.assertIn("partial_retry_cap", decision.reason_codes)
        self.assertIn("recent_sampling_skipped", decision.reason_codes)

    def test_latest_30_gap_abandonment_advances_bounded_discovery_state(self) -> None:
        source = parsed("video", gap_abandoned_latest_30_video_payload())

        transition = apply_video_event(ChannelFeatureState(), source)

        self.assertEqual(transition.discovery_outcome, "complete")
        self.assertEqual(transition.state.last_complete_discovery_at, source.observed_at)
        self.assertIn(
            "discovery_gap_abandoned_latest_30",
            transition.state.fallback_reason_codes,
        )

    def test_old_first_seen_id_is_treated_as_backfill_not_a_new_publication(self) -> None:
        payload = discovery_payload()
        payload["first_seen"] = [
            {
                "video_id": "old-backfill",
                "position": 1,
                "content_type": "video",
                "published_at": "2026-07-01T10:00:00Z",
                "published_at_precision": "second",
            }
        ]
        payload["items"] = 1
        payload["first_seen_count"] = 1
        payload["detail_success_count"] = 1
        source = parsed("video", video_payload(discovery=payload))
        prior_publish = datetime(2026, 7, 19, tzinfo=timezone.utc)
        transition = apply_video_event(
            ChannelFeatureState(
                last_publish_at=prior_publish,
                last_discovery_observed_at=datetime(2026, 7, 19, tzinfo=timezone.utc),
                last_complete_discovery_at=datetime(2026, 7, 19, tzinfo=timezone.utc),
            ),
            source,
        )
        self.assertEqual(transition.state.last_publish_at, prior_publish)
        self.assertEqual(transition.state.new_video_empty_runs, 1)
        self.assertIn("backfill_first_seen_ignored", transition.state.fallback_reason_codes)

    def test_recent_sampling_updates_change_probability_and_clock(self) -> None:
        source = parsed("video", video_payload(recent_sampling=recent_payload()))
        transition = apply_video_event(
            ChannelFeatureState(recent30_video_count=6), source
        )
        decision = decide_video(transition, source)
        self.assertGreater(transition.state.recent_change_probability or 0, 0)
        self.assertEqual(transition.state.last_recent_sample_count, 4)
        self.assertEqual(decision.tier_days, 3)
        self.assertEqual(decision.feature_summary["selected_risk_basis"], "discovery")

    def test_partial_recent_sampling_is_capped_at_seven_days(self) -> None:
        source = parsed(
            "video",
            video_payload(
                recent_sampling=recent_payload(partial=True),
                recent_sampling_outcome="partial",
            ),
            outcome="partial",
        )
        transition = apply_video_event(ChannelFeatureState(), source)
        decision = decide_video(transition, source)
        self.assertLessEqual(decision.tier_days, 7)

    def test_first_recent_sampling_uses_fourteen_day_baseline(self) -> None:
        source = parsed("video", video_payload(recent_sampling=recent_payload()))
        transition = apply_video_event(ChannelFeatureState(), source)
        candidate = evaluate_video_sampling_risk(
            transition.state,
            outcome=transition.recent_sampling_outcome,
            baseline=transition.recent_sampling_baseline,
        )

        self.assertEqual(candidate.interval_days, 14)
        self.assertIn("recent_sampling_baseline", candidate.reason_codes)

    def test_no_comparable_views_does_not_decay_view_change_ewma(self) -> None:
        payload = recent_payload()
        payload["comparable_view_count"] = 0
        payload["view_changed_count"] = 0
        source = parsed("video", video_payload(recent_sampling=payload))
        transition = apply_video_event(
            ChannelFeatureState(recent30_video_count=8, recent_view_change_ewma=0.8),
            source,
        )
        self.assertEqual(transition.state.recent_view_change_ewma, 0.8)


class AgentStateAndPolicyTests(unittest.TestCase):
    def test_agent_uses_category_vector_and_output_fingerprint(self) -> None:
        first_event = parsed("agent", agent_payload())
        first = apply_agent_event(ChannelFeatureState(), first_event)
        second_event = parsed(
            "agent",
            agent_payload("b", ("Comedy", "Music")),
            observed_at="2026-10-18T12:00:00Z",
            sequence=2,
        )
        second = apply_agent_event(first.state, second_event)
        decision = decide_agent_due(
            second.state,
            observed_at=second_event.observed_at,
            outcome=second_event.outcome,
            baseline=second.baseline,
            output_changed=second.output_changed,
            evidence_count=second.evidence_count,
        )
        self.assertTrue(second.output_changed)
        self.assertIsNotNone(second.state.topic_drift)
        self.assertIsNotNone(second.state.evidence_replacement)
        self.assertGreaterEqual(decision.tier_days, 60)
        self.assertLessEqual(decision.tier_days, 365)

    def test_long_term_stable_agent_maps_to_365_days(self) -> None:
        observed = datetime(2026, 7, 20, tzinfo=timezone.utc)
        decision = decide_agent_due(
            ChannelFeatureState(agent_stable_runs=6, topic_drift=0.0),
            observed_at=observed,
            outcome="complete",
            baseline=False,
            output_changed=False,
            evidence_count=20,
        )
        self.assertEqual(decision.tier_days, 365)

    def test_equal_evidence_counts_still_detect_full_set_replacement(self) -> None:
        first_event = parsed(
            "agent",
            extended_agent_payload(
                output="a",
                topics=("l1:technology", "tag:ai"),
                evidence=("a", "b"),
                version="c",
            ),
        )
        first = apply_agent_event(ChannelFeatureState(), first_event)
        second_event = parsed(
            "agent",
            extended_agent_payload(
                output="b",
                topics=("l1:technology", "tag:ai"),
                evidence=("d", "e"),
                version="c",
            ),
            observed_at="2026-08-20T12:00:00Z",
            sequence=2,
        )
        second = apply_agent_event(first.state, second_event)

        self.assertEqual(second.state.evidence_replacement, 1.0)
        self.assertEqual(second.state.recent_content_shift, 0.0)
        self.assertGreaterEqual(second.state.agent_change_score or 0.0, 0.30)

    def test_agent_version_change_restarts_semantic_comparison_baseline(self) -> None:
        first_event = parsed(
            "agent",
            extended_agent_payload(
                output="a",
                topics=("l1:technology",),
                evidence=("a",),
                version="a",
            ),
        )
        first = apply_agent_event(ChannelFeatureState(), first_event)
        second_event = parsed(
            "agent",
            extended_agent_payload(
                output="a",
                topics=("l1:technology",),
                evidence=("a",),
                version="b",
            ),
            observed_at="2026-08-20T12:00:00Z",
            sequence=2,
        )
        second = apply_agent_event(first.state, second_event)
        decision = decide_agent_due(
            second.state,
            observed_at=second_event.observed_at,
            outcome="complete",
            baseline=False,
            output_changed=second.output_changed,
            evidence_count=second.evidence_count,
        )

        self.assertTrue(second.state.agent_version_changed)
        self.assertEqual(decision.tier_days, 180)
        self.assertEqual(decision.feature_summary["topic_vector_source"], "crawler_topic_tokens")
        self.assertFalse(decision.feature_summary["agent_semantic_comparison_available"])
        self.assertIn("agent_cross_version_baseline", decision.reason_codes)


if __name__ == "__main__":
    unittest.main()
