from datetime import date, datetime, timezone
import unittest

from feature_engine.agent_clock_semantic_rebase import (
    resolve_rebased_agent_plan,
    semantic_agent_rebase_decision,
)
from feature_engine.policy import AgentPolicyConfig
from feature_engine.state import ChannelFeatureState


class AgentClockSemanticRebaseTests(unittest.TestCase):
    def test_semantic_rebase_uses_v7_continuous_curve_and_forward_spread(self) -> None:
        state = ChannelFeatureState(
            last_agent_observed_at=datetime(2026, 7, 21, 10, tzinfo=timezone.utc),
            topic_drift=0.5,
            evidence_replacement=0.1,
            recent_content_shift=0.2,
            last_agent_evidence_count=10,
        )

        decision = semantic_agent_rebase_decision(
            state,
            channel_id="UC-agent-semantic-rebase",
            config=AgentPolicyConfig(
                policy_version="v16-rule-7",baseline_interval_days=180
            ),
        )

        self.assertEqual(decision.tier_days, 148)
        self.assertGreaterEqual(decision.due_day, date(2026, 12, 16))
        self.assertIn("agent_semantic_continuous_interval", decision.reason_codes)
        self.assertIn("agent_forward_load_spread", decision.reason_codes)

    def test_agent_only_plan_is_cancelled(self) -> None:
        resolution = resolve_rebased_agent_plan(
            {
                "plan_id": "plan-1","channel_id": "UC1","status": "planned",
                "run_about": False,"run_video": False,
            },
            {},
        )

        self.assertEqual(resolution.target_status, "cancelled")

    def test_mixed_plan_succeeds_when_non_agent_evidence_is_complete(self) -> None:
        resolution = resolve_rebased_agent_plan(
            {
                "plan_id": "plan-2","channel_id": "UC2","status": "running",
                "run_about": True,"run_video": True,
            },
            {"about": "complete", "video": "complete"},
        )

        self.assertEqual(resolution.target_status, "succeeded")
        self.assertIsNone(resolution.target_error_code)

    def test_mixed_plan_is_blocked_when_non_agent_evidence_is_missing(self) -> None:
        resolution = resolve_rebased_agent_plan(
            {
                "plan_id": "plan-3","channel_id": "UC3","status": "running",
                "run_about": True,"run_video": True,
            },
            {"about": "complete"},
        )

        self.assertIsNone(resolution.target_status)
        self.assertEqual(resolution.reason, "non_agent_observation_missing")


if __name__ == "__main__":
    unittest.main()
