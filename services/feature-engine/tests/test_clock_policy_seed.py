from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory
import unittest

from feature_engine.clock_policy_seed import (
    ClockPolicySeedConflict,
    ClockPolicySeedError,
    DEFAULT_POLICY_FILE,
    load_clock_policy_manifest,
    plan_clock_policy_seed,
)
from feature_engine.policy import runtime_policy_configs


SCHEMA = (Path(__file__).resolve().parents[1] / "sql" / "schema.sql").read_text(
    encoding="utf-8"
)


class ClockPolicyManifestTests(unittest.TestCase):
    def test_manifest_records_the_latest_rule_for_each_clock(self) -> None:
        manifest = load_clock_policy_manifest()
        runtime = runtime_policy_configs(manifest.contract)

        self.assertEqual(manifest.policy_version, "v16-rule-7")
        self.assertEqual(
            manifest.domain_rule_versions,
            {"about": "v16-rule-5", "video": "v16-rule-2", "agent": "v16-rule-7"},
        )
        self.assertEqual(runtime.about.baseline_interval_days, 7)
        self.assertTrue(runtime.about.cadence_baseline_enabled)
        self.assertEqual(runtime.discovery.automatic_min_interval_days, 3)
        self.assertEqual(runtime.recent_sampling.fallback_interval_days, 14)
        self.assertEqual(runtime.agent.baseline_interval_days, 180)

    def test_manifest_matches_the_latest_schema_seed(self) -> None:
        manifest = load_clock_policy_manifest()
        policy = manifest.policy
        latest_seed = SCHEMA[SCHEMA.rfind("INSERT INTO feature_clock.rule_policy_definitions") :]

        self.assertIn("'v16-rule-7'", latest_seed)
        self.assertIn("'2026-08-10T00:00:00Z'", latest_seed)
        self.assertIn("ARRAY[1,2,3,5,7,14,30,60,90,180,365]", latest_seed)
        fields = (
            "about_config",
            "discovery_config",
            "recent_sampling_config",
            "agent_config",
            "partial_retry_config",
        )
        encoded_configs = re.findall(r"'(\{.*?\})'::jsonb", latest_seed)
        self.assertEqual(len(encoded_configs), len(fields))
        for field, encoded in zip(fields, encoded_configs, strict=True):
            self.assertEqual(json.loads(encoded), policy[field])
        self.assertIn("md5('v16-rule-7:2026-08-10')", latest_seed)

    def test_manifest_rejects_tampered_policy_facts(self) -> None:
        source = json.loads(DEFAULT_POLICY_FILE.read_text(encoding="utf-8"))
        source["policy"]["agent_config"]["baseline_interval_days"] = 90
        with TemporaryDirectory() as directory:
            path = Path(directory) / "policy.json"
            path.write_text(json.dumps(source), encoding="utf-8")
            with self.assertRaisesRegex(ClockPolicySeedError, "source_sha256 mismatch"):
                load_clock_policy_manifest(path)

    def test_manifest_rejects_tampered_domain_version_metadata(self) -> None:
        source = json.loads(DEFAULT_POLICY_FILE.read_text(encoding="utf-8"))
        source["domain_rule_versions"]["about"] = "v16-rule-4"
        with TemporaryDirectory() as directory:
            path = Path(directory) / "policy.json"
            path.write_text(json.dumps(source), encoding="utf-8")
            with self.assertRaisesRegex(ClockPolicySeedError, "source_sha256 mismatch"):
                load_clock_policy_manifest(path)


class ClockPolicySeedPlanTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = load_clock_policy_manifest()

    def row(self, *, status: str = "active") -> dict:
        return {**deepcopy(dict(self.manifest.policy)), "status": status}

    def test_empty_database_inserts_and_activates(self) -> None:
        plan = plan_clock_policy_seed([], self.manifest, database="crawler")

        self.assertEqual(plan.action, "insert_and_activate")
        self.assertIsNone(plan.previous_active_policy)

    def test_identical_active_policy_is_idempotent(self) -> None:
        plan = plan_clock_policy_seed(
            [self.row()],
            self.manifest,
            database="crawler",
        )

        self.assertEqual(plan.action, "already_active")
        self.assertEqual(plan.previous_active_policy, "v16-rule-7")

    def test_identical_draft_can_be_activated_after_the_previous_policy(self) -> None:
        previous = self.row(status="active")
        previous["policy_version"] = "v16-rule-6"
        draft = self.row(status="draft")

        plan = plan_clock_policy_seed(
            [previous, draft],
            self.manifest,
            database="crawler",
        )

        self.assertEqual(plan.action, "activate_draft")
        self.assertEqual(plan.previous_active_policy, "v16-rule-6")

    def test_conflicting_stored_policy_fails_closed(self) -> None:
        target = self.row(status="draft")
        target["agent_config"]["baseline_interval_days"] = 90

        with self.assertRaisesRegex(ClockPolicySeedConflict, "differs"):
            plan_clock_policy_seed([target], self.manifest, database="crawler")

    def test_retired_policy_cannot_be_reactivated(self) -> None:
        with self.assertRaisesRegex(ClockPolicySeedConflict, "cannot be reactivated"):
            plan_clock_policy_seed(
                [self.row(status="retired")],
                self.manifest,
                database="crawler",
            )


if __name__ == "__main__":
    unittest.main()
