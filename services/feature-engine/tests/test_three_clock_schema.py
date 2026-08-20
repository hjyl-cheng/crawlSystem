from pathlib import Path
import re
import unittest


SCHEMA = (Path(__file__).resolve().parents[1] / "sql" / "schema.sql").read_text(
    encoding="utf-8"
)


class ThreeClockSchemaTests(unittest.TestCase):
    def test_database_schema_has_no_profile_clock_storage(self) -> None:
        table_definitions = "\n".join(
            re.search(
                rf"CREATE TABLE IF NOT EXISTS feature_clock\.{table} \((.*?)\n\);",
                SCHEMA,
                flags=re.DOTALL,
            ).group(1)
            for table in ("channel_clock_state", "daily_channel_plans")
        )
        forbidden = (
            "profile_due_at",
            "profile_due_day",
            "profile_tier",
            "profile_last_complete_at",
            "run_profile",
        )

        for name in forbidden:
            self.assertNotIn(name, table_definitions)

    def test_policy_and_feature_state_have_no_profile_domain(self) -> None:
        table_definitions = "\n".join(
            re.search(
                rf"CREATE TABLE IF NOT EXISTS feature_clock\.{table} \((.*?)\n\);",
                SCHEMA,
                flags=re.DOTALL,
            ).group(1)
            for table in ("rule_policy_definitions", "channel_feature_state")
        )

        for name in (
            "profile_config",
            "profile_field_hashes",
            "profile_change_score",
            "profile_major_change",
            "profile_change_ewma",
            "profile_stable_runs",
            "last_profile_observed_at",
        ):
            self.assertNotIn(name, table_definitions)

    def test_new_event_and_decision_rows_allow_only_three_kinds(self) -> None:
        self.assertIn(
            "CHECK (observation_kind IN ('about', 'video', 'agent'))",
            SCHEMA,
        )
        self.assertIn(
            "CHECK (clock_kind IN ('about', 'video', 'agent'))",
            SCHEMA,
        )

    def test_latest_seed_activates_the_semantic_agent_policy(self) -> None:
        rule_6_insert = SCHEMA.rfind("  'v16-rule-6',")
        rule_7_insert = SCHEMA.rfind("  'v16-rule-7',")

        self.assertGreater(rule_7_insert, rule_6_insert)
        self.assertIn(
            '"baseline_interval_days":180',
            SCHEMA[rule_7_insert:],
        )
        self.assertIn(
            "WHERE status='active' AND policy_version<>'v16-rule-7'",
            SCHEMA[rule_7_insert:],
        )

    def test_agent_clock_storage_accepts_continuous_long_intervals(self) -> None:
        self.assertIn("agent_tier BETWEEN 60 AND 365", SCHEMA)
        self.assertIn(
            "clock_kind='agent'\n"
            "    AND (tier IN (1, 3, 7, 14, 30) OR tier BETWEEN 60 AND 365)",
            SCHEMA,
        )

    def test_decision_tier_constraint_preserves_legacy_profile_audit(self) -> None:
        constraint_start = SCHEMA.index(
            "ADD CONSTRAINT clock_decision_log_tier_check"
        )
        constraint_end = SCHEMA.index(";", constraint_start)

        self.assertTrue(
            SCHEMA[constraint_start:constraint_end].rstrip().endswith("NOT VALID")
        )


if __name__ == "__main__":
    unittest.main()
