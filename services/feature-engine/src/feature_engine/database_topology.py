from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable


@dataclass(frozen=True, slots=True)
class SharedDatabaseIdentity:
    database: str
    user: str
    timezone: str


def validate_shared_feature_database(
    connect: Callable[[], Any],
    *,
    expected_database: str,
    expected_user: str,
    required_feature_relations: Iterable[str],
) -> SharedDatabaseIdentity:
    """Validate the shared PostgreSQL topology without reading Crawler facts."""

    required = tuple(str(relation).strip() for relation in required_feature_relations)
    if not expected_database.strip():
        raise RuntimeError("expected shared database name is required")
    if not expected_user.strip():
        raise RuntimeError("expected Feature database user is required")
    if not required or any(not relation.startswith("feature_clock.") for relation in required):
        raise RuntimeError("required Feature relations must be feature_clock-qualified")

    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT current_database(),current_user,current_setting('TimeZone'),
                       to_regnamespace('crawler') IS NOT NULL,
                       to_regclass('crawler.channels') IS NOT NULL
                """
            )
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError("shared Crawler/Feature database identity is unavailable")
            database, user, timezone, crawler_ready, channels_ready = row
            if (
                database != expected_database
                or user != expected_user
                or timezone != "UTC"
                or not crawler_ready
                or not channels_ready
            ):
                raise RuntimeError("unexpected or unmigrated shared Crawler/Feature database")

            cursor.execute(
                "SELECT has_table_privilege(current_user,'crawler.channels','SELECT')"
            )
            privilege_row = cursor.fetchone()
            if privilege_row is None or bool(privilege_row[0]):
                raise RuntimeError("Feature database user must not read Crawler business tables")

            for relation in required:
                cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (relation,))
                relation_row = cursor.fetchone()
                if relation_row is None or not bool(relation_row[0]):
                    raise RuntimeError(
                        f"shared Feature schema is missing required relation {relation}"
                    )

    return SharedDatabaseIdentity(
        database=str(database),
        user=str(user),
        timezone=str(timezone),
    )
