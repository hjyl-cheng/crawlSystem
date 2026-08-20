from __future__ import annotations

from pathlib import Path
import unittest

from feature_engine.database_topology import validate_shared_feature_database


class CursorStub:
    def __init__(self, responses: list[tuple[object, ...]]) -> None:
        self.responses = list(responses)
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def __enter__(self) -> "CursorStub":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, statement: str, parameters: tuple[object, ...] = ()) -> None:
        self.executed.append((statement, parameters))

    def fetchone(self) -> tuple[object, ...] | None:
        return self.responses.pop(0) if self.responses else None


class ConnectionStub:
    def __init__(self, cursor: CursorStub) -> None:
        self._cursor = cursor

    def __enter__(self) -> "ConnectionStub":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def cursor(self) -> CursorStub:
        return self._cursor


class SharedDatabaseTopologyTests(unittest.TestCase):
    @staticmethod
    def connect_with(*responses: tuple[object, ...]):
        cursor = CursorStub(list(responses))
        return lambda: ConnectionStub(cursor)

    def test_accepts_shared_database_with_schema_only_feature_role(self) -> None:
        connect = self.connect_with(
            ("bullmq_crawler_migration", "feature_user", "UTC", True, True),
            (False,),
            (True,),
            (True,),
        )

        identity = validate_shared_feature_database(
            connect,
            expected_database="bullmq_crawler_migration",
            expected_user="feature_user",
            required_feature_relations=(
                "feature_clock.channel_feature_state",
                "feature_clock.channel_clock_state",
            ),
        )

        self.assertEqual(identity.database, "bullmq_crawler_migration")
        self.assertEqual(identity.user, "feature_user")

    def test_rejects_the_legacy_physically_separate_database(self) -> None:
        connect = self.connect_with(
            ("feature_clock", "feature_user", "UTC", False, False),
        )

        with self.assertRaisesRegex(RuntimeError, "shared Crawler/Feature database"):
            validate_shared_feature_database(
                connect,
                expected_database="bullmq_crawler_migration",
                expected_user="feature_user",
                required_feature_relations=("feature_clock.channel_clock_state",),
            )

    def test_rejects_feature_role_with_crawler_table_read_access(self) -> None:
        connect = self.connect_with(
            ("bullmq_crawler_migration", "feature_user", "UTC", True, True),
            (True,),
        )

        with self.assertRaisesRegex(RuntimeError, "must not read Crawler business tables"):
            validate_shared_feature_database(
                connect,
                expected_database="bullmq_crawler_migration",
                expected_user="feature_user",
                required_feature_relations=("feature_clock.channel_clock_state",),
            )

    def test_rejects_unqualified_required_relations(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "feature_clock-qualified"):
            validate_shared_feature_database(
                self.connect_with(),
                expected_database="bullmq_crawler_migration",
                expected_user="feature_user",
                required_feature_relations=("crawler.channels",),
            )

    def test_fresh_database_bootstrap_keeps_crawler_tables_private(self) -> None:
        script = (
            Path(__file__).resolve().parents[3]
            / "database"
            / "init"
            / "10-crawler.sh"
        ).read_text(encoding="utf-8")
        self.assertNotIn(
            "GRANT SELECT ON ALL TABLES IN SCHEMA crawler TO feature_user",
            script,
        )
        self.assertIn(
            "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA crawler FROM feature_user",
            script,
        )
        self.assertIn(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA crawler\n"
            "  REVOKE ALL ON TABLES FROM feature_user",
            script,
        )


if __name__ == "__main__":
    unittest.main()
