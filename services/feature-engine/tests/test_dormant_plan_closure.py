from datetime import datetime, timezone
from types import SimpleNamespace
import unittest

from feature_engine.applier import FeatureObservationApplier
from feature_engine.events import VideoActivityPayload, VideoPayload


class RecordingCursor:
    def __init__(self) -> None:
        self.executions = []
        self.rowcount = 1

    def execute(self, query, params) -> None:
        self.executions.append((" ".join(query.split()), params))


class DormantPlanClosureTests(unittest.TestCase):
    def test_dormant_transition_cancels_the_current_plan_too(self) -> None:
        cursor = RecordingCursor()
        event = SimpleNamespace(
            observation_kind="video",
            event_id="2df201e9-33ef-47d8-b612-2292378beb81",
            channel_id="UC-dormant-current-plan",
            plan_id="7ca232f0-7d07-4795-b2cd-f618d64aaebd",
            payload=VideoPayload(
                discovery_outcome="complete",
                discovery=None,
                recent_sampling_outcome="complete",
                recent_sampling=None,
                activity=VideoActivityPayload(
                    window_days=90,
                    recent_published_content_count=0,
                    lifecycle_status="dormant",
                    dormant_reason="no_published_content_within_90_days",
                    dormant_since=datetime(2026, 8, 10, tzinfo=timezone.utc),
                    dormant_recheck_day="2026-11-07",
                    dormant_cycle=1,
                ),
            ),
        )

        FeatureObservationApplier._apply_video_activity_lifecycle(cursor, event)

        plan_query, plan_params = cursor.executions[1]
        self.assertIn("UPDATE feature_clock.daily_channel_plans", plan_query)
        self.assertNotIn("plan_id<>", plan_query)
        self.assertEqual(plan_params, ("UC-dormant-current-plan",))


if __name__ == "__main__":
    unittest.main()
