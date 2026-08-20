from __future__ import annotations

from copy import deepcopy
import unittest

from feature_engine.contracts import (
    ContractValidationError,
    crawler_observation_json_schema,
    validate_active_policy_contract,
)
from feature_engine.policy import runtime_policy_configs


def active_policy() -> dict:
    return {
        "policy_version": "v16-rule-2",
        "allowed_days": [1, 3, 7, 14, 30, 60, 90, 180, 365],
        "about_config": {
            "velocity_ewma_alpha": 0.30,
            "baseline_interval_days": 7,
            "neutral_growth_percentile": 0.50,
            "stable_min_days_for_long_interval": 90,
            "video_delta_full_scale": 3,
            "cold_start_priority_floor": 0.75,
            "cold_start_min_recent_video_count": 20,
            "cold_start_max_publish_interval_days": 1.5,
            "cold_start_max_publish_age_days": 3.0,
            "cold_start_min_reliable_intervals": 3,
            "cold_start_min_subscriber_count": 5000,
            "cold_start_min_subscriber_percentile": 0.50,
            "cold_start_min_feature_confidence": 0.75,
        },
        "discovery_config": {
            "fallback_interval_days": 7,
            "interval_ewma_alpha": 0.35,
            "regularity_threshold": 0.65,
            "silence_decay": 0.60,
            "automatic_min_interval_days": 3,
        },
        "recent_sampling_config": {
            "fallback_interval_days": 14,
            "change_ewma_alpha": 0.40,
        },
        "agent_config": {
            "baseline_interval_days": 90,
            "bootstrap_min_days": 30,
            "bootstrap_max_days": 90,
            "high_priority_cap_days": 90,
        },
        "partial_retry_config": {
            "about_days": 3,
            "discovery_days": 3,
            "recent_sampling_days": 7,
            "agent_days": 14,
        },
        "status": "active",
    }


class CrawlerObservationSchemaTests(unittest.TestCase):
    def test_schema_has_a_discriminator_for_all_observation_kinds(self) -> None:
        schema = crawler_observation_json_schema()

        mapping = schema["discriminator"]["mapping"]
        self.assertEqual(
            set(mapping),
            {"about", "video", "agent"},
        )
        self.assertEqual(len(schema["oneOf"]), 3)


class ActivePolicyContractTests(unittest.TestCase):
    def test_accepts_the_seeded_v16_policy(self) -> None:
        policy = validate_active_policy_contract(active_policy())

        self.assertEqual(policy.policy_version, "v16-rule-2")
        self.assertEqual(policy.partial_retry_config.agent_days, 14)
        self.assertEqual(policy.discovery_config.interval_ewma_alpha, 0.35)
        self.assertEqual(policy.discovery_config.automatic_min_interval_days, 3)
        self.assertEqual(policy.about_config.cold_start_priority_floor, 0.75)

    def test_accepts_dynamic_baseline_policy_switches(self) -> None:
        source = active_policy()
        source["policy_version"] = "v16-rule-3"
        source["about_config"]["dynamic_baseline_enabled"] = True
        source["agent_config"]["dynamic_baseline_enabled"] = True

        policy = validate_active_policy_contract(source)

        self.assertTrue(policy.about_config.dynamic_baseline_enabled)
        self.assertTrue(policy.agent_config.dynamic_baseline_enabled)

    def test_accepts_v16_rule_4_about_tiers(self) -> None:
        source = active_policy()
        source["policy_version"] = "v16-rule-4"
        source["allowed_days"] = [1, 2, 3, 5, 7, 14, 30, 60, 90, 180, 365]
        source["about_config"]["dynamic_baseline_enabled"] = True
        source["about_config"]["cadence_baseline_enabled"] = True
        source["about_config"]["cold_start_max_publish_age_days"] = 7.0
        source["agent_config"]["dynamic_baseline_enabled"] = True

        policy = validate_active_policy_contract(source)

        self.assertTrue(policy.about_config.cadence_baseline_enabled)
        self.assertEqual(policy.allowed_days[:5], (1, 2, 3, 5, 7))
        self.assertEqual(
            runtime_policy_configs(policy).discovery.allowed_days,
            (1, 3, 7, 14, 30, 60, 90),
        )

    def test_accepts_v16_rule_5_daily_publish_first_tier(self) -> None:
        source = active_policy()
        source["policy_version"] = "v16-rule-5"
        source["allowed_days"] = [1, 2, 3, 5, 7, 14, 30, 60, 90, 180, 365]
        source["about_config"]["dynamic_baseline_enabled"] = True
        source["about_config"]["cadence_baseline_enabled"] = True
        source["about_config"]["cold_start_max_publish_age_days"] = 7.0
        source["about_config"][
            "cold_start_tier_one_max_publish_interval_days"
        ] = 1.0
        source["agent_config"]["dynamic_baseline_enabled"] = True

        policy = validate_active_policy_contract(source)
        runtime = runtime_policy_configs(policy)

        self.assertEqual(
            runtime.about.cold_start_tier_one_max_publish_interval_days,
            1.0,
        )

    def test_accepts_v16_rule_6_three_clock_policy(self) -> None:
        source = active_policy()
        source["policy_version"] = "v16-rule-6"
        source["allowed_days"] = [1, 2, 3, 5, 7, 14, 30, 60, 90, 180, 365]
        source["about_config"]["dynamic_baseline_enabled"] = True
        source["about_config"]["cadence_baseline_enabled"] = True
        source["about_config"]["cold_start_max_publish_age_days"] = 7.0
        source["about_config"][
            "cold_start_tier_one_max_publish_interval_days"
        ] = 1.0
        source["agent_config"]["dynamic_baseline_enabled"] = True

        policy = validate_active_policy_contract(source)
        runtime = runtime_policy_configs(policy)

        self.assertEqual(runtime.about.policy_version, "v16-rule-6")

    def test_accepts_v16_rule_7_semantic_agent_clock_policy(self) -> None:
        source = active_policy()
        source["policy_version"] = "v16-rule-7"
        source["allowed_days"] = [1, 2, 3, 5, 7, 14, 30, 60, 90, 180, 365]
        source["about_config"]["dynamic_baseline_enabled"] = True
        source["about_config"]["cadence_baseline_enabled"] = True
        source["about_config"]["cold_start_max_publish_age_days"] = 7.0
        source["about_config"][
            "cold_start_tier_one_max_publish_interval_days"
        ] = 1.0
        source["agent_config"]["baseline_interval_days"] = 180
        source["agent_config"]["dynamic_baseline_enabled"] = True
        source["agent_config"]["version_change_interval_days"] = 14

        policy = validate_active_policy_contract(source)
        runtime = runtime_policy_configs(policy)

        self.assertEqual(runtime.agent.policy_version, "v16-rule-7")
        self.assertEqual(runtime.agent.baseline_interval_days, 180)

    def test_rejects_profile_clock_policy_configuration(self) -> None:
        source = active_policy()
        source["profile_config"] = {"fixed_interval_days": 30}

        with self.assertRaisesRegex(
            ContractValidationError,
            "profile_config is not valid",
        ):
            validate_active_policy_contract(source)

    def test_rejects_profile_clock_retry_configuration(self) -> None:
        source = active_policy()
        source["partial_retry_config"]["profile_days"] = 7

        with self.assertRaisesRegex(
            ContractValidationError,
            "partial_retry.profile_days is not valid",
        ):
            validate_active_policy_contract(source)

    def test_rejects_first_tier_threshold_overlapping_the_two_day_tier(self) -> None:
        source = active_policy()
        source["allowed_days"] = [1, 2, 3, 5, 7, 14, 30, 60, 90, 180, 365]
        source["about_config"]["dynamic_baseline_enabled"] = True
        source["about_config"]["cadence_baseline_enabled"] = True
        source["about_config"][
            "cold_start_tier_one_max_publish_interval_days"
        ] = 2.6

        with self.assertRaisesRegex(
            ContractValidationError,
            "no greater than 2.5",
        ):
            validate_active_policy_contract(source)

    def test_rejects_a_missing_required_policy_value(self) -> None:
        source = active_policy()
        del source["about_config"]["video_delta_full_scale"]

        with self.assertRaisesRegex(ContractValidationError, "video_delta_full_scale"):
            validate_active_policy_contract(source)

    def test_rejects_type_coercion_and_unknown_config(self) -> None:
        source = deepcopy(active_policy())
        source["about_config"]["baseline_interval_days"] = "7"
        source["about_config"]["silent_default"] = True

        with self.assertRaisesRegex(
            ContractValidationError,
            "baseline_interval_days.*integer|silent_default.*unexpected",
        ):
            validate_active_policy_contract(source)

    def test_rejects_unsorted_allowed_days(self) -> None:
        source = active_policy()
        source["allowed_days"] = [1, 7, 3]

        with self.assertRaisesRegex(ContractValidationError, "allowed_days"):
            validate_active_policy_contract(source)

    def test_rejects_an_incomplete_v16_tier_set(self) -> None:
        source = active_policy()
        source["allowed_days"] = [1, 3, 7, 14, 30, 60, 90]

        with self.assertRaisesRegex(ContractValidationError, "V16 tiers"):
            validate_active_policy_contract(source)

    def test_rejects_a_configured_non_tier_interval(self) -> None:
        source = active_policy()
        source["agent_config"]["baseline_interval_days"] = 11

        with self.assertRaisesRegex(ContractValidationError, "configured Clock tiers"):
            validate_active_policy_contract(source)


if __name__ == "__main__":
    unittest.main()
