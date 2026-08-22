from __future__ import annotations

from copy import deepcopy
import unittest
from uuid import uuid4

from feature_engine.events import (
    AgentPayload,
    CrawlerObservationRecorded,
    EventValidationError,
    FailedDomainPayload,
    VideoPayload,
    VideoDiscoveryPayload,
    VideoRecentSamplingPayload,
    canonical_payload_hash,
)


def event(kind: str, payload: dict, *, outcome: str = "complete", sequence: int = 1) -> dict:
    return {
        "event_id": str(uuid4()),
        "event_type": "crawler.observation.recorded",
        "event_version": 1,
        "observation_id": str(uuid4()),
        "channel_id": "UCv16domains",
        "observation_kind": kind,
        "kind_sequence": sequence,
        "observed_at": "2026-07-20T12:00:00.000Z",
        "outcome": outcome,
        "crawler_version": "qy-v16",
        "payload_hash": canonical_payload_hash(payload),
        "payload": payload,
    }


def profile_payload() -> dict:
    fields = ("title", "handle", "avatar_url", "keywords", "available_tabs", "summary")
    changed = {field: False for field in fields}
    changed["title"] = True
    return {
        "field_hashes": {
            field: f"sha256:{str(index + 1) * 64}" for index, field in enumerate(fields)
        },
        "changed": changed,
        "change_score": 0.25,
        "baseline": False,
        "coverage": {"observed_fields": list(fields), "missing_fields": []},
    }


def discovery_payload() -> dict:
    return {
        "pages": 1,
        "items": 3,
        "anchor_matched": True,
        "stop_reason": "anchor_matched",
        "parse_gap_count": 0,
        "first_seen": [
            {
                "video_id": "new-video",
                "position": 1,
                "content_type": "video",
                "published_at": "2026-07-20T10:00:00.000Z",
                "published_at_precision": "second",
            }
        ],
        "first_seen_count": 1,
        "detail_success_count": 1,
        "detail_failure_count": 0,
    }


def recent_payload(*, partial: bool = True) -> dict:
    return {
        "recent_count": 8,
        "stale_ratio": 0.5,
        "selected_count": 4,
        "success_count": 3 if partial else 4,
        "failure_count": 1 if partial else 0,
        "next_count": 1,
        "comparable_view_count": 3,
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
            "payload": recent_sampling or recent_payload(partial=False),
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
    return {
        "discovery": {
            "outcome": "complete",
            "payload": {
                "pages": 2,
                "items": 30,
                "anchor_matched": False,
                "stop_reason": "gap_abandoned_latest_30",
                "parse_gap_count": 0,
                "first_seen": [
                    {
                        "video_id": selected_video_ids[0],
                        "position": 1,
                        "content_type": "video",
                        "published_at": "2026-07-20T10:00:00.000Z",
                        "published_at_precision": "second",
                    }
                ],
                "first_seen_count": 1,
                "detail_success_count": 1,
                "detail_failure_count": 0,
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
            },
        },
        "recent_sampling": {
            "outcome": "complete",
            "payload": recent_payload(partial=False),
        },
    }


def agent_payload() -> dict:
    return {
        "output_hash": f"sha256:{'a' * 64}",
        "category_level_1": "Technology",
        "category_level_2": ["AI", "Software"],
        "tag_count": 10,
        "evidence_count": 18,
        "active_subscriber_ratio": 35,
        "fulfilled_plan_count": 1,
    }


def extended_agent_payload() -> dict:
    payload = agent_payload()
    payload.update(
        {
            "topic_tokens": ["l1:technology", "tag:ai"],
            "evidence_fingerprints": [
                f"sha256:{'b' * 64}",
                f"sha256:{'c' * 64}",
            ],
            "agent_version_hash": f"sha256:{'d' * 64}",
        }
    )
    return payload


class V16DomainEventTests(unittest.TestCase):
    def test_rejects_profile_observation_kind(self) -> None:
        with self.assertRaisesRegex(EventValidationError, "profile"):
            CrawlerObservationRecorded.from_mapping(event("profile", profile_payload()))

    def test_accepts_complete_video_contract(self) -> None:
        parsed = CrawlerObservationRecorded.from_mapping(
            event("video", video_payload())
        )
        self.assertIsInstance(parsed.payload, VideoPayload)
        self.assertIsInstance(parsed.payload.discovery, VideoDiscoveryPayload)
        self.assertIsInstance(parsed.payload.recent_sampling, VideoRecentSamplingPayload)
        self.assertEqual(parsed.payload.discovery.first_seen[0].video_id, "new-video")

    def test_accepts_incremental_video_disposition_ledger(self) -> None:
        discovery = discovery_payload()
        discovery.update(
            {
                "discovered_count": 1,
                "silent_drop_count": 0,
                "silent_drop_video_ids": [],
                "dispositions": [
                    {
                        "video_id": "new-video",
                        "kind": "stored",
                        "reason_code": "content_stored",
                        "retry_class": None,
                    }
                ],
                "recheck_dispositions": [],
                "stored_count": 1,
                "deferred_count": 0,
                "terminal_excluded_count": 0,
                "unresolved_count": 0,
                "unresolved_video_ids": [],
                "recheck_deferred_video_ids": [],
                "recheck_deferred_count": 0,
                "pending_deferred_video_ids": [],
                "pending_deferred_count": 0,
                "blocking_deferred_video_ids": [],
                "recheck_stored_count": 0,
                "recheck_terminal_excluded_count": 0,
            }
        )
        source = event("video", video_payload(discovery=discovery))

        parsed = CrawlerObservationRecorded.from_mapping(source)

        self.assertEqual(parsed.payload.discovery.disposition_ledger["stored_count"], 1)
        self.assertEqual(parsed.as_pending_payload()["payload"], source["payload"])

    def test_rejects_inconsistent_incremental_video_disposition_counts(self) -> None:
        discovery = discovery_payload()
        discovery.update(
            {
                "discovered_count": 1,
                "silent_drop_count": 0,
                "silent_drop_video_ids": [],
                "dispositions": [
                    {
                        "video_id": "new-video",
                        "kind": "stored",
                        "reason_code": "content_stored",
                        "retry_class": None,
                    }
                ],
                "recheck_dispositions": [],
                "stored_count": 0,
                "deferred_count": 0,
                "terminal_excluded_count": 0,
                "unresolved_count": 0,
                "unresolved_video_ids": [],
                "recheck_deferred_video_ids": [],
                "recheck_deferred_count": 0,
                "pending_deferred_video_ids": [],
                "pending_deferred_count": 0,
                "blocking_deferred_video_ids": [],
                "recheck_stored_count": 0,
                "recheck_terminal_excluded_count": 0,
            }
        )

        with self.assertRaisesRegex(EventValidationError, "stored_count"):
            CrawlerObservationRecorded.from_mapping(
                event("video", video_payload(discovery=discovery))
            )

    def test_accepts_deferred_disposition_without_attempted_detail(self) -> None:
        discovery = discovery_payload()
        discovery.update(
            {
                "items": 1,
                "anchor_matched": False,
                "stop_reason": "max_items",
                "first_seen": [],
                "first_seen_count": 0,
                "detail_success_count": 0,
                "detail_failure_count": 0,
                "discovered_count": 1,
                "silent_drop_count": 0,
                "silent_drop_video_ids": [],
                "dispositions": [
                    {
                        "video_id": "deferred-video",
                        "kind": "deferred",
                        "reason_code": "discovery_scan_incomplete",
                        "retry_class": "uploads_scan_retry",
                    }
                ],
                "recheck_dispositions": [],
                "stored_count": 0,
                "deferred_count": 1,
                "terminal_excluded_count": 0,
                "unresolved_count": 1,
                "unresolved_video_ids": ["deferred-video"],
                "recheck_deferred_video_ids": [],
                "recheck_deferred_count": 0,
                "pending_deferred_video_ids": [],
                "pending_deferred_count": 0,
                "blocking_deferred_video_ids": ["deferred-video"],
                "recheck_stored_count": 0,
                "recheck_terminal_excluded_count": 0,
            }
        )

        parsed = CrawlerObservationRecorded.from_mapping(
            event(
                "video",
                video_payload(
                    discovery=discovery,
                    discovery_outcome="partial",
                    recent_sampling_outcome="complete",
                ),
                outcome="partial",
            )
        )

        self.assertEqual(parsed.payload.discovery.disposition_ledger["deferred_count"], 1)

    def test_accepts_deferred_recheck_ledger(self) -> None:
        discovery = discovery_payload()
        discovery.update(
            {
                "items": 1,
                "first_seen": [],
                "first_seen_count": 0,
                "detail_success_count": 0,
                "detail_failure_count": 1,
                "discovered_count": 0,
                "silent_drop_count": 0,
                "silent_drop_video_ids": [],
                "dispositions": [],
                "recheck_dispositions": [
                    {
                        "video_id": "recheck-video",
                        "kind": "deferred",
                        "reason_code": "detail_collection_failed",
                        "retry_class": "player_retry",
                    }
                ],
                "stored_count": 0,
                "deferred_count": 0,
                "terminal_excluded_count": 0,
                "unresolved_count": 0,
                "unresolved_video_ids": [],
                "recheck_deferred_video_ids": ["recheck-video"],
                "recheck_deferred_count": 1,
                "pending_deferred_video_ids": [],
                "pending_deferred_count": 0,
                "blocking_deferred_video_ids": ["recheck-video"],
                "recheck_stored_count": 0,
                "recheck_terminal_excluded_count": 0,
            }
        )

        parsed = CrawlerObservationRecorded.from_mapping(
            event(
                "video",
                video_payload(discovery=discovery, discovery_outcome="partial"),
                outcome="partial",
            )
        )

        self.assertEqual(
            parsed.payload.discovery.disposition_ledger["recheck_deferred_count"],
            1,
        )

    def test_rejects_complete_discovery_with_persisted_blocking_deferred_video(self) -> None:
        discovery = discovery_payload()
        discovery.update(
            {
                "discovered_count": 1,
                "silent_drop_count": 0,
                "silent_drop_video_ids": [],
                "dispositions": [
                    {
                        "video_id": "new-video",
                        "kind": "stored",
                        "reason_code": "content_stored",
                        "retry_class": None,
                    }
                ],
                "recheck_dispositions": [],
                "stored_count": 1,
                "deferred_count": 0,
                "terminal_excluded_count": 0,
                "unresolved_count": 0,
                "unresolved_video_ids": [],
                "recheck_deferred_video_ids": [],
                "recheck_deferred_count": 0,
                "pending_deferred_video_ids": [],
                "pending_deferred_count": 0,
                "blocking_deferred_video_ids": ["persisted-deferred-video"],
                "recheck_stored_count": 0,
                "recheck_terminal_excluded_count": 0,
            }
        )

        with self.assertRaisesRegex(
            EventValidationError,
            "Complete Discovery cannot contain blocking deferred Videos",
        ):
            CrawlerObservationRecorded.from_mapping(
                event("video", video_payload(discovery=discovery))
            )

    def test_accepts_catchup_limited_video_with_skipped_sampling(self) -> None:
        source = event("video", catchup_limited_video_payload(), outcome="partial")

        parsed = CrawlerObservationRecorded.from_mapping(source)

        self.assertEqual(parsed.payload.discovery.stop_reason, "catchup_limit")
        self.assertEqual(
            parsed.payload.discovery.unclosed_video_ids,
            ("new-1", "new-2", "older-1"),
        )
        self.assertEqual(parsed.payload.recent_sampling_outcome, "skipped")
        self.assertEqual(
            parsed.payload.recent_sampling.skipped_reason,
            "discovery_incomplete",
        )
        self.assertEqual(parsed.as_pending_payload()["payload"], source["payload"])

    def test_accepts_complete_latest_30_after_abandoning_a_catchup_gap(self) -> None:
        source = event("video", gap_abandoned_latest_30_video_payload())

        parsed = CrawlerObservationRecorded.from_mapping(source)

        self.assertEqual(
            parsed.payload.discovery.stop_reason,
            "gap_abandoned_latest_30",
        )
        self.assertEqual(
            parsed.payload.discovery.gap_abandonment["selected_item_count"],
            30,
        )
        self.assertEqual(parsed.as_pending_payload()["payload"], source["payload"])

    def test_rejects_latest_30_ids_that_are_not_the_scanned_prefix(self) -> None:
        payload = gap_abandoned_latest_30_video_payload()
        selected = payload["discovery"]["payload"]["gap_abandonment"][
            "selected_video_ids"
        ]
        selected[0], selected[-1] = selected[-1], selected[0]

        with self.assertRaisesRegex(EventValidationError, "scanned prefix"):
            CrawlerObservationRecorded.from_mapping(event("video", payload))

    def test_rejects_gap_abandonment_before_the_catchup_limit(self) -> None:
        payload = gap_abandoned_latest_30_video_payload()
        evidence = payload["discovery"]["payload"]["gap_abandonment"]
        evidence["first_page_item_count"] = 101
        evidence["catch_up_item_count"] = 49

        with self.assertRaisesRegex(EventValidationError, "configured Catch-up limit"):
            CrawlerObservationRecorded.from_mapping(event("video", payload))

    def test_rejects_duplicate_gap_abandonment_video_ids(self) -> None:
        payload = gap_abandoned_latest_30_video_payload()
        scanned = payload["discovery"]["payload"]["gap_abandonment"][
            "scanned_video_ids"
        ]
        scanned[-1] = scanned[-2]

        with self.assertRaisesRegex(EventValidationError, "cannot contain duplicates"):
            CrawlerObservationRecorded.from_mapping(event("video", payload))

    def test_parse_gap_can_report_that_an_anchor_was_seen(self) -> None:
        payload = discovery_payload()
        payload["stop_reason"] = "parse_gap"
        payload["parse_gap_count"] = 1
        source = video_payload(discovery=payload, discovery_outcome="partial")
        parsed = CrawlerObservationRecorded.from_mapping(event("video", source, outcome="partial"))
        self.assertTrue(parsed.payload.discovery.anchor_matched)

    def test_accepts_complete_discovery_after_all_anchor_dates_are_crossed(self) -> None:
        payload = discovery_payload()
        payload["anchor_matched"] = False
        payload["stop_reason"] = "anchor_dates_exhausted"
        source = video_payload(discovery=payload)
        parsed = CrawlerObservationRecorded.from_mapping(event("video", source))
        self.assertFalse(parsed.payload.discovery.anchor_matched)
        self.assertEqual(parsed.payload.discovery.stop_reason, "anchor_dates_exhausted")

    def test_accepts_complete_initial_window_video_proof(self) -> None:
        payload = discovery_payload()
        payload.update(
            {
                "pages": 1,
                "items": 30,
                "anchor_matched": False,
                "stop_reason": "qualified_item_limit",
                "detail_success_count": 30,
                "inspected_count": 30,
                "requested_limit": 30,
                "content_max_age_days": 90,
                "scan_policy_version": None,
                "terminal_condition": "qualified_item_limit",
                "qualified_count": 30,
                "excluded_count": 0,
                "age_boundary_crossed": False,
            }
        )
        source = event("video", video_payload(discovery=payload))

        parsed = CrawlerObservationRecorded.from_mapping(source)

        self.assertEqual(parsed.payload.discovery.stop_reason, "qualified_item_limit")
        pending = parsed.as_pending_payload()
        self.assertEqual(pending["payload"], source["payload"])
        self.assertEqual(pending["payload_hash"], source["payload_hash"])

    def test_accepts_candidate_limit_processed_from_the_crawler_contract(self) -> None:
        payload = discovery_payload()
        payload.update(
            {
                "pages": 1,
                "items": 30,
                "anchor_matched": False,
                "stop_reason": "candidate_limit_processed",
                "detail_success_count": 27,
                "detail_failure_count": 0,
                "inspected_count": 30,
                "requested_limit": 30,
                "content_max_age_days": 90,
                "scan_policy_version": None,
                "terminal_condition": "candidate_limit_processed",
                "qualified_count": 27,
                "excluded_count": 0,
                "age_boundary_crossed": False,
            }
        )
        source = event("video", video_payload(discovery=payload))

        parsed = CrawlerObservationRecorded.from_mapping(source)

        self.assertEqual(
            parsed.payload.discovery.stop_reason,
            "candidate_limit_processed",
        )
        self.assertEqual(parsed.as_pending_payload()["payload"], source["payload"])

    def test_accepts_overlapping_persisted_and_incomplete_detail_counts(self) -> None:
        payload = discovery_payload()
        payload.update(
            {
                "pages": 1,
                "items": 30,
                "anchor_matched": False,
                "stop_reason": "max_items",
                "detail_success_count": 30,
                "detail_failure_count": 23,
                "inspected_count": 30,
                "requested_limit": 30,
                "content_max_age_days": 90,
                "scan_policy_version": None,
                "terminal_condition": None,
                "qualified_count": 1,
                "excluded_count": 29,
                "age_boundary_crossed": False,
            }
        )

        parsed = CrawlerObservationRecorded.from_mapping(
            event(
                "video",
                video_payload(discovery=payload, discovery_outcome="partial"),
                outcome="partial",
            )
        )

        self.assertEqual(parsed.payload.discovery.detail_success_count, 30)
        self.assertEqual(parsed.payload.discovery.detail_failure_count, 23)

    def test_rejects_each_detail_counter_when_it_exceeds_inspected_count(self) -> None:
        for field in ("detail_success_count", "detail_failure_count"):
            with self.subTest(field=field):
                payload = discovery_payload()
                payload.update(
                    {
                        "pages": 1,
                        "items": 30,
                        "anchor_matched": False,
                        "stop_reason": "max_items",
                        "detail_success_count": 30,
                        "detail_failure_count": 0,
                        "inspected_count": 30,
                        "requested_limit": 30,
                        "content_max_age_days": 90,
                        "scan_policy_version": None,
                        "terminal_condition": None,
                        "qualified_count": 1,
                        "excluded_count": 29,
                        "age_boundary_crossed": False,
                    }
                )
                payload[field] = 31

                with self.assertRaisesRegex(
                    EventValidationError,
                    f"{field} cannot exceed inspected_count",
                ):
                    CrawlerObservationRecorded.from_mapping(
                        event(
                            "video",
                            video_payload(discovery=payload, discovery_outcome="partial"),
                            outcome="partial",
                        )
                    )

    def test_rejects_candidate_limit_processed_without_full_scan_proof(self) -> None:
        payload = discovery_payload()
        payload.update(
            {
                "items": 30,
                "anchor_matched": False,
                "stop_reason": "candidate_limit_processed",
                "detail_success_count": 1,
            }
        )

        with self.assertRaisesRegex(EventValidationError, "requires scan proof"):
            CrawlerObservationRecorded.from_mapping(
                event("video", video_payload(discovery=payload))
            )

    def test_accepts_complete_initial_window_age_boundary_proof(self) -> None:
        payload = discovery_payload()
        payload.update(
            {
                "anchor_matched": False,
                "stop_reason": "age_boundary_crossed",
                "detail_success_count": 12,
                "inspected_count": 12,
                "requested_limit": 30,
                "content_max_age_days": 90,
                "scan_policy_version": "initial-window-v1",
                "terminal_condition": "age_boundary_crossed",
                "qualified_count": 1,
                "excluded_count": 11,
                "age_boundary_crossed": True,
            }
        )

        parsed = CrawlerObservationRecorded.from_mapping(
            event("video", video_payload(discovery=payload))
        )

        self.assertEqual(parsed.payload.discovery.stop_reason, "age_boundary_crossed")

    def test_accepts_bounded_first_seen_sample_for_full_scan_proof(self) -> None:
        payload = discovery_payload()
        payload.update(
            {
                "items": 100,
                "anchor_matched": False,
                "stop_reason": "age_boundary_crossed",
                "detail_success_count": 64,
                "detail_failure_count": 0,
                "inspected_count": 100,
                "requested_limit": 100,
                "content_max_age_days": 90,
                "scan_policy_version": "publication-video-window-repair-v1",
                "terminal_condition": "age_boundary_crossed",
                "qualified_count": 17,
                "excluded_count": 47,
                "age_boundary_crossed": True,
            }
        )

        parsed = CrawlerObservationRecorded.from_mapping(
            event("video", video_payload(discovery=payload), sequence=3)
        )

        self.assertEqual(parsed.payload.discovery.first_seen_count, 1)
        self.assertEqual(parsed.payload.discovery.detail_success_count, 64)

    def test_rejects_full_scan_detail_coverage_mismatch(self) -> None:
        payload = discovery_payload()
        payload.update(
            {
                "items": 12,
                "anchor_matched": False,
                "stop_reason": "age_boundary_crossed",
                "detail_success_count": 11,
                "detail_failure_count": 0,
                "inspected_count": 12,
                "requested_limit": 30,
                "content_max_age_days": 90,
                "scan_policy_version": "publication-video-window-repair-v1",
                "terminal_condition": "age_boundary_crossed",
                "qualified_count": 1,
                "excluded_count": 11,
                "age_boundary_crossed": True,
            }
        )

        with self.assertRaisesRegex(
            EventValidationError, "detail success count disagrees with coverage"
        ):
            CrawlerObservationRecorded.from_mapping(
                event("video", video_payload(discovery=payload), sequence=3)
            )

    def test_accepts_unknown_page_count_for_full_scan_proof(self) -> None:
        payload = discovery_payload()
        payload.update(
            {
                "pages": None,
                "items": 100,
                "anchor_matched": False,
                "stop_reason": "age_boundary_crossed",
                "detail_success_count": 44,
                "detail_failure_count": 0,
                "inspected_count": 100,
                "requested_limit": 100,
                "content_max_age_days": 90,
                "scan_policy_version": "publication-video-window-repair-v1",
                "terminal_condition": "age_boundary_crossed",
                "qualified_count": 16,
                "excluded_count": 28,
                "age_boundary_crossed": True,
            }
        )

        parsed = CrawlerObservationRecorded.from_mapping(
            event("video", video_payload(discovery=payload), sequence=5)
        )

        self.assertIsNone(parsed.payload.discovery.pages)

    def test_rejects_unknown_page_count_without_full_scan_proof(self) -> None:
        payload = discovery_payload()
        payload["pages"] = None

        with self.assertRaisesRegex(
            EventValidationError, "pages may be null only with Publication scan proof"
        ):
            CrawlerObservationRecorded.from_mapping(
                event("video", video_payload(discovery=payload))
            )

    def test_rejects_incomplete_initial_window_video_proof(self) -> None:
        payload = discovery_payload()
        payload.update(
            {
                "anchor_matched": False,
                "stop_reason": "qualified_item_limit",
                "terminal_condition": "qualified_item_limit",
                "qualified_count": 30,
            }
        )

        with self.assertRaisesRegex(EventValidationError, "supplied together"):
            CrawlerObservationRecorded.from_mapping(
                event("video", video_payload(discovery=payload))
            )

    def test_rejects_discovery_detail_count_mismatch(self) -> None:
        payload = discovery_payload()
        payload["detail_failure_count"] = 1
        source = video_payload(discovery=payload)
        with self.assertRaisesRegex(EventValidationError, "counts disagree"):
            CrawlerObservationRecorded.from_mapping(event("video", source))

    def test_accepts_partial_recent_sampling_contract(self) -> None:
        source = video_payload(
            recent_sampling=recent_payload(), recent_sampling_outcome="partial"
        )
        parsed = CrawlerObservationRecorded.from_mapping(event("video", source, outcome="partial"))
        self.assertEqual(parsed.payload.recent_sampling.success_count, 3)

    def test_rejects_recent_sampling_outcome_mismatch(self) -> None:
        source = video_payload(
            recent_sampling=recent_payload(), recent_sampling_outcome="complete"
        )
        with self.assertRaisesRegex(EventValidationError, "outcome"):
            CrawlerObservationRecorded.from_mapping(event("video", source, outcome="complete"))

    def test_accepts_terminal_video_failure_without_phase_facts(self) -> None:
        payload = {
            "failure_kind": "channel_removed",
            "attempt_count": 3,
            "removed_reason": "community_guidelines",
        }
        parsed = CrawlerObservationRecorded.from_mapping(
            event("video", payload, outcome="failed")
        )
        self.assertIsInstance(parsed.payload, FailedDomainPayload)
        self.assertEqual(parsed.payload.attempt_count, 3)
        self.assertEqual(parsed.payload.removed_reason, "community_guidelines")

    def test_accepts_complete_and_failed_agent_contracts(self) -> None:
        complete = CrawlerObservationRecorded.from_mapping(event("agent", agent_payload()))
        failed_payload = {"failed_plan_count": 2}
        failed = CrawlerObservationRecorded.from_mapping(
            event("agent", failed_payload, outcome="failed")
        )
        self.assertIsInstance(complete.payload, AgentPayload)
        self.assertEqual(complete.payload.active_subscriber_ratio, 35)
        self.assertEqual(failed.payload.failed_plan_count, 2)

    def test_accepts_bounded_agent_identity_signals(self) -> None:
        source = event("agent", extended_agent_payload(), sequence=2)
        parsed = CrawlerObservationRecorded.from_mapping(source)

        self.assertEqual(parsed.payload.topic_tokens, ("l1:technology", "tag:ai"))
        self.assertEqual(len(parsed.payload.evidence_fingerprints or ()), 2)
        pending = parsed.as_pending_payload()
        self.assertEqual(pending["payload"], source["payload"])
        self.assertEqual(pending["payload_hash"], source["payload_hash"])

    def test_accepts_agent_input_content_evidence(self) -> None:
        payload = extended_agent_payload()
        payload.update(
            {
                "input_content_count": 2,
                "input_content_hash": f"sha256:{'e' * 64}",
            }
        )
        source = event("agent", payload, sequence=2)

        parsed = CrawlerObservationRecorded.from_mapping(source)

        self.assertEqual(parsed.payload.input_content_count, 2)
        pending = parsed.as_pending_payload()
        self.assertEqual(pending["payload"], source["payload"])
        self.assertEqual(pending["payload_hash"], source["payload_hash"])

    def test_rejects_partial_agent_input_content_evidence(self) -> None:
        payload = extended_agent_payload()
        payload["input_content_count"] = 2

        with self.assertRaisesRegex(EventValidationError, "supplied together"):
            CrawlerObservationRecorded.from_mapping(event("agent", payload, sequence=2))

    def test_rejects_partial_agent_identity_signal_bundle(self) -> None:
        payload = agent_payload()
        payload["topic_tokens"] = ["l1:technology"]
        with self.assertRaisesRegex(EventValidationError, "supplied together"):
            CrawlerObservationRecorded.from_mapping(event("agent", payload))

    def test_generic_hash_detects_nested_tampering(self) -> None:
        source = event("agent", agent_payload())
        tampered = deepcopy(source)
        tampered["payload"]["category_level_2"].append("Security")
        with self.assertRaisesRegex(EventValidationError, "payload_hash"):
            CrawlerObservationRecorded.from_mapping(tampered)

    def test_canonical_hash_matches_json_stringify_number_format(self) -> None:
        payload = {"a": 1e-7, "b": 1e-6, "c": 1e20, "d": 1e21, "e": -0.0}
        self.assertEqual(
            canonical_payload_hash(payload),
            "sha256:8b80f2d0e063725ad82f76f2923b8c84107461330c5ed5b9eedbd1e216372867",
        )


if __name__ == "__main__":
    unittest.main()
