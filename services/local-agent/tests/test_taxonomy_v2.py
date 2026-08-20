import copy
import unittest

from qy_channel_profile.taxonomy import TAXONOMY_VERSION
from qy_channel_profile.taxonomy_v2 import (
    REQUIRED_FACETS,
    TAXONOMY_V2_DRAFT_VERSION,
    TaxonomyDraftError,
    expanded_legacy_mappings,
    load_taxonomy_v2_draft,
    validate_taxonomy_v2_draft,
)


class TaxonomyV2DraftTest(unittest.TestCase):
    def test_draft_is_valid_complete_and_does_not_replace_v1(self):
        payload = load_taxonomy_v2_draft()

        self.assertEqual(payload["version"], TAXONOMY_V2_DRAFT_VERSION)
        self.assertEqual(set(payload["facets"]), REQUIRED_FACETS)
        self.assertFalse(payload["production_eligible"])
        self.assertEqual(TAXONOMY_VERSION, "qy-taxonomy-v1")
        self.assertEqual(len(expanded_legacy_mappings(payload)), 120)

    def test_draft_keeps_topic_purpose_and_format_orthogonal(self):
        payload = load_taxonomy_v2_draft()
        nodes = {
            node["id"]: facet
            for facet, definition in payload["facets"].items()
            for node in definition["nodes"]
        }

        self.assertEqual(nodes["qy.topic.lifestyle"], "topic")
        self.assertEqual(nodes["qy.purpose.daily_life"], "purpose_genre")
        self.assertEqual(nodes["qy.format.screen_capture"], "format")
        self.assertEqual(nodes["qy.purpose.self_improvement"], "purpose_genre")
        self.assertEqual(nodes["qy.topic.humanities_society.psychology"], "topic")

    def test_validator_rejects_incomplete_legacy_mapping(self):
        payload = copy.deepcopy(load_taxonomy_v2_draft())
        payload["legacy_v1_compatibility"]["branches"].pop()

        with self.assertRaisesRegex(TaxonomyDraftError, "mapping coverage mismatch"):
            validate_taxonomy_v2_draft(payload)

    def test_validator_rejects_cross_facet_parent(self):
        payload = copy.deepcopy(load_taxonomy_v2_draft())
        purpose = payload["facets"]["purpose_genre"]["nodes"][0]
        purpose["parent_id"] = "qy.topic.food"

        with self.assertRaisesRegex(TaxonomyDraftError, "parent in another facet"):
            validate_taxonomy_v2_draft(payload)


if __name__ == "__main__":
    unittest.main()
