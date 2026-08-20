import copy
import unittest
from datetime import datetime, timezone

from qy_channel_profile.agent_contract import (
    AGENT_OUTPUT_FIELDS,
    internal_result_to_agent_payload,
    to_agent_payload,
    validate_agent_batch,
    validate_agent_payload,
)
from qy_channel_profile.contracts import AnalysisPolicy, ChannelSnapshot, ProfileAnalysisRequest
from qy_channel_profile.errors import ContractError
from qy_channel_profile.processor import ChannelProfileProcessor
from qy_channel_profile.analyzers import controlled_tag_names

from tests.test_processor import snapshot_value


class AgentContractTest(unittest.TestCase):
    def setUp(self):
        self.snapshot = ChannelSnapshot.from_mapping(snapshot_value())
        self.processor = ChannelProfileProcessor()

    def result(self, input_url: str = " exact input URL "):
        return self.processor.analyze(
            ProfileAnalysisRequest(
                channel_id=self.snapshot.channel_id,
                input_url=input_url,
                as_of=datetime(2026, 8, 9, tzinfo=timezone.utc),
                policy=AnalysisPolicy.COMPLETE_ESTIMATE,
            ),
            self.snapshot,
        )

    def test_adapter_emits_only_strict_fields_in_prompt_order(self):
        payload = to_agent_payload(self.result())
        self.assertEqual(tuple(payload), AGENT_OUTPUT_FIELDS)
        self.assertEqual(payload["input_url"], " exact input URL ")
        self.assertNotIn("confidence", payload)
        self.assertNotIn("channel_id", payload)
        self.assertTrue(set(payload["channel_tags"]["tags"]).issubset(set(controlled_tag_names())))
        validate_agent_payload(payload)

    def test_batch_keeps_duplicate_urls_and_order(self):
        payloads = [
            to_agent_payload(self.result("same")),
            to_agent_payload(self.result("other")),
            to_agent_payload(self.result("same")),
        ]
        self.assertEqual(
            validate_agent_batch(payloads, expected_input_urls=["same", "other", "same"]),
            3,
        )
        with self.assertRaises(ContractError):
            validate_agent_batch(payloads, expected_input_urls=["same", "same", "other"])

    def test_serialized_internal_result_projects_without_inference(self):
        result = self.result("raw URL").to_dict()
        self.assertEqual(internal_result_to_agent_payload(result), to_agent_payload(self.result("raw URL")))

    def test_validator_rejects_each_structural_invariant(self):
        payload = to_agent_payload(self.result())

        invalid = copy.deepcopy(payload)
        invalid["extra"] = True
        with self.assertRaises(ContractError):
            validate_agent_payload(invalid)

        invalid = copy.deepcopy(payload)
        invalid["creator_gender"] = "unknown"
        with self.assertRaises(ContractError):
            validate_agent_payload(invalid)

        invalid = copy.deepcopy(payload)
        invalid["audience_region"][0]["percentage"] += 1
        with self.assertRaises(ContractError):
            validate_agent_payload(invalid)

        invalid = copy.deepcopy(payload)
        invalid["channel_tags"]["tags"][0] = invalid["channel_tags"]["tags"][1]
        with self.assertRaises(ContractError):
            validate_agent_payload(invalid)

        invalid = copy.deepcopy(payload)
        invalid["channel_categories"]["level_2"] = ["Mobile Apps"]
        with self.assertRaises(ContractError):
            validate_agent_payload(invalid)


if __name__ == "__main__":
    unittest.main()
