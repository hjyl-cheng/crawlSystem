import unittest
from datetime import datetime, timezone

from qy_channel_profile.processor import ChannelProfileProcessor
from qy_channel_profile.errors import ContractError
from qy_channel_profile.runtime import analyze_database_request, snapshot_from_runtime
from qy_channel_profile.snapshot_repository import (
    PostgresSnapshotRepository,
    SnapshotRecord,
)

from tests.test_processor import snapshot_value


CHANNEL_ID = "UC1234567890123456789012"


class _Cursor:
    def __init__(self, rows=None):
        self._rows = list(rows or [])

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _Connection:
    def __init__(self):
        self.statements = []
        self.closed = False

    def execute(self, statement, parameters=None):
        normalized = " ".join(statement.split())
        self.statements.append((normalized, parameters))
        if normalized == "SHOW transaction_read_only":
            return _Cursor([{"transaction_read_only": "on"}])
        if "transaction_timestamp() AS as_of" in normalized:
            return _Cursor([{"as_of": datetime(2026, 8, 14, 9, 0, tzinfo=timezone.utc)}])
        if "FROM crawler.channels" in normalized:
            return _Cursor([{
                "channel_id": CHANNEL_ID,
                "channel_url": "https://www.youtube.com/@example",
                "handle": "@example",
                "title": "Example",
                "country": "Brazil",
                "country_source": "youtube_about",
                "country_code": "BR",
                "country_canonical_name": "Brazil",
                "avatar_url": None,
                "summary": "Canal de receitas",
                "keywords": ["receitas"],
                "about_description": "Receitas brasileiras",
                "joined_at": None,
                "external_links": [],
                "is_verified": False,
                "subscriber_count": 1000,
                "total_view_count": 10000,
                "total_video_count": 1,
                "source_json": {"channel_extractor": "youtubejs"},
                "latest_run_id": "run-current",
            }])
        if "FROM crawler.contents" in normalized:
            return _Cursor([{
                "channel_id": CHANNEL_ID,
                "source_content_id": "video-1",
                "content_type": "video",
                "content_type_source": "youtube_player",
                "title": "Receita",
                "description": "Como cozinhar",
                "description_status": "exact",
                "description_source": "youtube_player",
                "thumbnail_url": None,
                "keywords": [],
                "hashtags": [],
                "published_at": datetime(2026, 8, 13, tzinfo=timezone.utc),
                "published_at_status": "exact",
                "published_at_source": "youtube_player",
                "published_at_precision": "second",
                "first_seen_at": datetime(2026, 8, 13, tzinfo=timezone.utc),
                "view_count": 100,
                "view_count_status": "exact",
                "view_count_source": "youtube_player",
                "like_count": 10,
                "like_count_status": "exact",
                "like_count_source": "youtube_player",
                "comment_count": 2,
                "comment_count_status": "exact",
                "comment_count_source": "youtube_player",
                "comments_disabled": False,
                "duration_seconds": 90,
                "duration_status": "exact",
                "duration_source": "youtube_player",
                "extractor_version": "test",
                "comments_first_page": None,
                "position_rank": 1,
            }])
        return _Cursor()

    def close(self):
        self.closed = True


class _Repository:
    def load_many(self, channel_ids):
        value = snapshot_value()
        value["channel"]["channel_url"] = "https://www.youtube.com/@from-database"
        return {
            CHANNEL_ID: SnapshotRecord(
                snapshot=snapshot_from_runtime(value),
                latest_run_id="run-current",
            ),
        }


class SnapshotRepositoryTest(unittest.TestCase):
    def test_postgres_repository_freezes_one_read_only_snapshot(self):
        connection = _Connection()
        repository = PostgresSnapshotRepository(connection_factory=lambda: connection)

        records = repository.load_many([CHANNEL_ID])

        self.assertEqual(records[CHANNEL_ID].latest_run_id, "run-current")
        self.assertEqual(records[CHANNEL_ID].snapshot.channel_id, CHANNEL_ID)
        self.assertEqual(records[CHANNEL_ID].snapshot.contents[0].source_content_id, "video-1")
        statements = [statement for statement, _ in connection.statements]
        self.assertIn("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY", statements)
        self.assertIn("SHOW transaction_read_only", statements)
        self.assertEqual(statements[-1], "ROLLBACK")
        self.assertTrue(connection.closed)

    def test_database_runtime_accepts_only_channel_ids_and_derives_url(self):
        report = analyze_database_request(
            {"channel_ids": [CHANNEL_ID]},
            ChannelProfileProcessor(),
            _Repository(),
        )

        self.assertEqual(report["status"], "ok")
        result = report["results"][0]
        self.assertEqual(result["channel_id"], CHANNEL_ID)
        self.assertEqual(result["input_url"], "https://www.youtube.com/@from-database")
        self.assertEqual(result["source_latest_run_id"], "run-current")
        self.assertEqual(result["analysis_result"]["channel_id"], CHANNEL_ID)
        self.assertIn("snapshot_hash", result["analysis_result"]["snapshot"])

    def test_database_runtime_reports_missing_channels_without_aborting_batch(self):
        report = analyze_database_request(
            {"channel_ids": [CHANNEL_ID, "UCmissing00000000000000000"]},
            ChannelProfileProcessor(),
            _Repository(),
        )

        self.assertEqual(len(report["results"]), 1)
        self.assertEqual(report["errors"], [{
            "channel_id": "UCmissing00000000000000000",
            "error": "SnapshotNotFound: channel snapshot not found",
        }])

    def test_database_runtime_request_contains_only_channel_ids(self):
        for unexpected in (
            {"input_url": "https://www.youtube.com/@caller-supplied"},
            {"policy": "complete_estimate"},
        ):
            with self.subTest(unexpected=unexpected):
                with self.assertRaisesRegex(ContractError, "accepts only channel_ids"):
                    analyze_database_request(
                        {"channel_ids": [CHANNEL_ID], **unexpected},
                        ChannelProfileProcessor(),
                        _Repository(),
                    )


if __name__ == "__main__":
    unittest.main()
