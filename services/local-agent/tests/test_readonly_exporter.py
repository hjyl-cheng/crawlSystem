import unittest
from pathlib import Path


class ReadOnlyExporterTest(unittest.TestCase):
    def test_exporter_has_explicit_read_only_transaction_and_no_dml(self):
        source = Path("scripts/qy_readonly_export.cjs").read_text(encoding="utf-8")
        self.assertIn("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY", source)
        self.assertIn("SHOW transaction_read_only", source)
        self.assertIn('option("after-channel-id"', source)
        self.assertIn('booleanOption("include-comments", false)', source)
        self.assertIn('"current-agent-cohort"', source)
        self.assertIn("const historicalReplay = mode === \"agent-reference\"", source)
        self.assertIn("comments_first_page->>'collected_at'", source)
        self.assertIn("comments_first_page->>'collected_at')::timestamptz<=b.as_of", source)
        self.assertIn("c.description_source", source)
        self.assertIn("c.published_at_source", source)
        self.assertIn("c.view_count_source", source)
        self.assertIn("c.extractor_version", source)
        self.assertIn('data_lineage_version: "snapshot-field-lineage-v1"', source)
        self.assertIn('"extractor_not_persisted"', source)
        self.assertIn("jsonb_array_length(b.retained_ids)=0", source)
        self.assertIn("content_type: row.content_type", source)
        self.assertIn("content_type_source: row.content_type_source", source)
        for statement in ("INSERT INTO crawler.", "UPDATE crawler.", "DELETE FROM crawler."):
            self.assertNotIn(statement, source)


if __name__ == "__main__":
    unittest.main()
