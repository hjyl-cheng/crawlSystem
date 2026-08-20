from __future__ import annotations

import unittest

from feature_engine.plan_status import reduce_daily_plan_status


class DailyPlanStatusTests(unittest.TestCase):
    def test_latest_requested_outcomes_are_reduced_independently_of_old_status(self) -> None:
        plan = {
            "run_about": True,
            "run_video": True,
            "run_agent": False,
            "status": "failed",
        }

        self.assertEqual(
            reduce_daily_plan_status(plan, {"about": "complete"}).status,
            "running",
        )
        self.assertEqual(
            reduce_daily_plan_status(
                plan,
                {"about": "complete", "video": "partial"},
            ).status,
            "partial",
        )
        self.assertEqual(
            reduce_daily_plan_status(
                plan,
                {"about": "complete", "video": "complete"},
            ).status,
            "succeeded",
        )

    def test_failed_requested_domain_dominates_missing_or_complete_domains(self) -> None:
        decision = reduce_daily_plan_status(
            {"run_about": True, "run_video": True, "run_agent": False},
            {"about": "failed"},
        )

        self.assertEqual(decision.status, "failed")
        self.assertEqual(decision.error_code, "crawler_observation_failed")


if __name__ == "__main__":
    unittest.main()
