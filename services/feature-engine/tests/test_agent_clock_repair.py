from __future__ import annotations

from datetime import date
import unittest

from feature_engine.agent_clock_repair import repaired_agent_due_day


class AgentClockRepairTests(unittest.TestCase):
    def test_repaired_due_day_never_precedes_the_policy_tier(self) -> None:
        observed_day = date(2026, 7, 21)

        due_day, offset = repaired_agent_due_day(
            channel_id="UC-agent-repair",
            policy_version="v16-rule-6",
            observed_day=observed_day,
            tier_days=90,
        )

        self.assertGreaterEqual(due_day, date(2026, 10, 19))
        self.assertLessEqual(due_day, date(2026, 11, 2))
        self.assertEqual((due_day - observed_day).days, 90 + offset)

    def test_urgent_fourteen_day_repair_has_no_spread(self) -> None:
        due_day, offset = repaired_agent_due_day(
            channel_id="UC-agent-urgent",
            policy_version="v16-rule-6",
            observed_day=date(2026, 7, 21),
            tier_days=14,
        )

        self.assertEqual(due_day, date(2026, 8, 4))
        self.assertEqual(offset, 0)

    def test_continuous_interval_repair_keeps_a_bounded_spread(self) -> None:
        observed_day = date(2026, 7, 21)

        due_day, offset = repaired_agent_due_day(
            channel_id="UC-agent-continuous",
            policy_version="v16-rule-7",
            observed_day=observed_day,
            tier_days=148,
        )

        self.assertEqual((due_day - observed_day).days, 148 + offset)
        self.assertGreaterEqual(offset, 0)
        self.assertLessEqual(offset, 18)


if __name__ == "__main__":
    unittest.main()
