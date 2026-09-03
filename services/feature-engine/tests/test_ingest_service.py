from __future__ import annotations

from hashlib import sha256
import json
import unittest
from uuid import uuid4

from feature_engine.applier import ApplyObservationResult, EventConflict, FeatureStateInvariantError
from feature_engine.events import EventValidationError
from feature_engine.ingest_service import FeatureIngestApplication


def event() -> dict:
    facts = {
        "subscriber_count": 1234,
        "subscriber_count_status": "exact",
        "total_view_count": 98765,
        "total_view_count_status": "exact",
        "total_video_count": 42,
        "total_video_count_status": "exact",
    }
    body = json.dumps(facts, separators=(",", ":"))
    return {
        "event_id": str(uuid4()),
        "event_type": "crawler.observation.recorded",
        "event_version": 1,
        "observation_id": str(uuid4()),
        "channel_id": "UCingest",
        "observation_kind": "about",
        "kind_sequence": 1,
        "observed_at": "2026-07-20T00:00:00Z",
        "outcome": "complete",
        "crawler_version": "qy-v16",
        "payload_hash": f"sha256:{sha256(body.encode()).hexdigest()}",
        "payload": facts,
    }


def video_event(
    *,
    activity: bool = False,
    evidence: bool = False,
    incremental: bool = True,
    sampling_outcome: str = "complete",
) -> dict:
    value = event()
    value.update(
        {
            "plan_id": str(uuid4()) if incremental else None,
            "observation_kind": "video",
            "payload": {
                "discovery": {"outcome": "complete", "payload": {}},
                "recent_sampling": {"outcome": sampling_outcome, "payload": {}},
            },
        }
    )
    if activity:
        value["payload"]["activity"] = {"lifecycle_status": "active"}
    if evidence:
        value["payload"]["activity_evidence"] = {"future_field": True}
    return value


class StubApplier:
    def __init__(self, result=None, error=None) -> None:
        self.result = result
        self.error = error
        self.calls: list[dict] = []

    def apply_crawler_observation(self, value: dict):
        self.calls.append(value)
        if self.error:
            raise self.error
        return self.result


class FeatureIngestApplicationTests(unittest.TestCase):
    def request(self, app: FeatureIngestApplication, value: dict, **overrides):
        headers = {
            "authorization": "Bearer secret",
            "content-type": "application/json",
            "idempotency-key": value["event_id"],
            **overrides.pop("headers", {}),
        }
        return app.handle(
            method=overrides.pop("method", "POST"),
            path=overrides.pop("path", "/v1/crawler-observations"),
            headers=headers,
            body=overrides.pop("body", json.dumps(value).encode()),
        )

    def test_applies_an_authenticated_event(self) -> None:
        value = event()
        result = ApplyObservationResult(
            event_id=value["event_id"],
            observation_id=value["observation_id"],
            status="applied",
            duplicate=False,
            last_applied_sequence=1,
        )
        applier = StubApplier(result=result)
        response = self.request(FeatureIngestApplication(applier, token="secret"), value)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["status"], "applied")
        self.assertEqual(len(applier.calls), 1)

    def test_waiting_gap_is_durably_accepted(self) -> None:
        value = event()
        result = ApplyObservationResult(
            event_id=value["event_id"],
            observation_id=value["observation_id"],
            status="waiting_gap",
            duplicate=False,
            last_applied_sequence=0,
        )
        response = self.request(FeatureIngestApplication(StubApplier(result=result), token="secret"), value)
        self.assertEqual(response.status, 202)

    def test_rejects_missing_auth_and_mismatched_idempotency_key(self) -> None:
        value = event()
        app = FeatureIngestApplication(StubApplier(), token="secret")
        unauthorized = self.request(app, value, headers={"authorization": ""})
        mismatch = self.request(app, value, headers={"idempotency-key": "different"})
        self.assertEqual(unauthorized.status, 401)
        self.assertEqual(mismatch.status, 400)

    def test_maps_permanent_and_retryable_failures(self) -> None:
        value = event()
        errors = [
            (EventValidationError("bad event"), 422),
            (EventConflict("identity conflict"), 409),
            (FeatureStateInvariantError("policy missing"), 503),
            (RuntimeError("database down"), 503),
        ]
        for error, expected in errors:
            with self.subTest(error=type(error).__name__):
                response = self.request(
                    FeatureIngestApplication(StubApplier(error=error), token="secret"),
                    value,
                )
                self.assertEqual(response.status, expected)

    def test_health_and_readiness_do_not_require_event_auth(self) -> None:
        app = FeatureIngestApplication(
            StubApplier(),
            token="secret",
            readiness=lambda: {"database": "feature_clock_test"},
        )
        health = app.handle(method="GET", path="/healthz")
        ready = app.handle(method="GET", path="/readyz")
        self.assertEqual(health.status, 200)
        self.assertEqual(ready.body["database"], "feature_clock_test")

    def test_warns_when_incremental_activity_evidence_is_missing(self) -> None:
        value = video_event(activity=True)
        warnings: list[dict] = []
        result = ApplyObservationResult(
            event_id=value["event_id"],
            observation_id=value["observation_id"],
            status="applied",
            duplicate=False,
            last_applied_sequence=1,
        )
        app = FeatureIngestApplication(
            StubApplier(result=result), token="secret", warning_sink=warnings.append
        )

        response = self.request(app, value)

        self.assertEqual(response.status, 200)
        self.assertEqual(
            {warning["warning_code"] for warning in warnings},
            {"activity_without_evidence", "incremental_activity_evidence_missing"},
        )
        self.assertTrue(
            all(warning["event_id"] == value["event_id"] for warning in warnings)
        )

    def test_legacy_activity_without_evidence_only_emits_compatibility_warning(
        self,
    ) -> None:
        value = video_event(activity=True, incremental=False)
        warnings: list[dict] = []
        result = ApplyObservationResult(
            event_id=value["event_id"],
            observation_id=value["observation_id"],
            status="applied",
            duplicate=False,
            last_applied_sequence=1,
        )

        self.request(
            FeatureIngestApplication(
                StubApplier(result=result),
                token="secret",
                warning_sink=warnings.append,
            ),
            value,
        )

        self.assertEqual(
            [warning["warning_code"] for warning in warnings],
            ["activity_without_evidence"],
        )

    def test_skipped_recent_sampling_does_not_require_activity_evidence(self) -> None:
        value = video_event(sampling_outcome="skipped")
        warnings: list[dict] = []
        result = ApplyObservationResult(
            event_id=value["event_id"],
            observation_id=value["observation_id"],
            status="applied",
            duplicate=False,
            last_applied_sequence=1,
        )

        self.request(
            FeatureIngestApplication(
                StubApplier(result=result),
                token="secret",
                warning_sink=warnings.append,
            ),
            value,
        )

        self.assertEqual(warnings, [])

    def test_does_not_warn_for_present_evidence_or_duplicate_delivery(self) -> None:
        present = video_event(activity=True, evidence=True)
        duplicate = video_event(activity=True)
        warnings: list[dict] = []
        present_result = ApplyObservationResult(
            event_id=present["event_id"],
            observation_id=present["observation_id"],
            status="applied",
            duplicate=False,
            last_applied_sequence=1,
        )
        duplicate_result = ApplyObservationResult(
            event_id=duplicate["event_id"],
            observation_id=duplicate["observation_id"],
            status="applied",
            duplicate=True,
            last_applied_sequence=1,
        )

        self.request(
            FeatureIngestApplication(
                StubApplier(result=present_result),
                token="secret",
                warning_sink=warnings.append,
            ),
            present,
        )
        self.request(
            FeatureIngestApplication(
                StubApplier(result=duplicate_result),
                token="secret",
                warning_sink=warnings.append,
            ),
            duplicate,
        )

        self.assertEqual(warnings, [])


if __name__ == "__main__":
    unittest.main()
