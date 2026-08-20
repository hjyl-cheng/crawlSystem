from __future__ import annotations

import unittest

from feature_engine.applier import (
    _logical_plan_outcomes,
    _plan_requests_observation,
)


class ThreeClockPlanTests(unittest.TestCase):
    def test_only_active_clock_kinds_can_be_requested(self) -> None:
        plan = {
            "run_about": True,
            "run_video": False,
            "run_agent": True,
        }

        self.assertTrue(_plan_requests_observation(plan, "about"))
        self.assertFalse(_plan_requests_observation(plan, "video"))
        self.assertTrue(_plan_requests_observation(plan, "agent"))
        self.assertFalse(_plan_requests_observation(plan, "profile"))

    def test_plan_outcomes_are_exactly_about_video_and_agent(self) -> None:
        plan = {
            "run_about": True,
            "run_video": True,
            "run_agent": False,
        }

        self.assertEqual(
            _logical_plan_outcomes(
                plan,
                {
                    "profile": "complete",
                    "about": "complete",
                    "video": "partial",
                    "agent": "complete",
                },
            ),
            {"about": "complete", "video": "partial"},
        )


if __name__ == "__main__":
    unittest.main()
