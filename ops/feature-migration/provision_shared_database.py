from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg
from psycopg import sql


def required(name: str) -> str:
    value = str(os.environ.get(name) or "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def read_required(path: str) -> str:
    try:
        value = Path(path).read_text(encoding="utf-8").strip()
    except OSError as error:
        raise RuntimeError("cannot read Feature database password file") from error
    if not value:
        raise RuntimeError("Feature database password file is empty")
    return value


def main() -> None:
    host = required("POSTGRES_HOST")
    port = int(os.environ.get("POSTGRES_PORT") or "5432")
    database = required("POSTGRES_DB")
    admin_user = required("POSTGRES_USER")
    admin_password = required("POSTGRES_PASSWORD")
    feature_user = required("FEATURE_DATABASE_USER")
    feature_password = read_required(required("FEATURE_DATABASE_PASSWORD_FILE"))

    with psycopg.connect(
        host=host,
        port=port,
        dbname=database,
        user=admin_user,
        password=admin_password,
        options="-c timezone=UTC",
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (781137211,))
            cursor.execute(
                """
                SELECT current_database(),current_setting('TimeZone'),
                       to_regnamespace('crawler') IS NOT NULL,
                       to_regclass('crawler.channels') IS NOT NULL
                """
            )
            actual_database, timezone, crawler_ready, channels_ready = cursor.fetchone()
            if (
                actual_database != database
                or timezone != "UTC"
                or not crawler_ready
                or not channels_ready
            ):
                raise RuntimeError("refusing to provision an unexpected Crawler database")

            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (feature_user,))
            role_exists = cursor.fetchone() is not None
            role_identifier = sql.Identifier(feature_user)
            password_literal = sql.Literal(feature_password)
            if role_exists:
                cursor.execute(
                    sql.SQL(
                        "ALTER ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                        "NOREPLICATION PASSWORD {} CONNECTION LIMIT 24"
                    ).format(role_identifier, password_literal)
                )
            else:
                cursor.execute(
                    sql.SQL(
                        "CREATE ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                        "NOREPLICATION PASSWORD {} CONNECTION LIMIT 24"
                    ).format(role_identifier, password_literal)
                )

            database_identifier = sql.Identifier(database)
            cursor.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    database_identifier,
                    role_identifier,
                )
            )
            cursor.execute(
                sql.SQL("ALTER ROLE {} IN DATABASE {} SET timezone TO 'UTC'").format(
                    role_identifier,
                    database_identifier,
                )
            )
            cursor.execute("SELECT nspowner::regrole::text FROM pg_namespace WHERE nspname='feature_clock'")
            schema_row = cursor.fetchone()
            if schema_row is None:
                cursor.execute(
                    sql.SQL("CREATE SCHEMA feature_clock AUTHORIZATION {}").format(
                        role_identifier
                    )
                )
            elif schema_row[0] != feature_user:
                raise RuntimeError("existing feature_clock schema has an unexpected owner")

            cursor.execute(
                sql.SQL("REVOKE ALL ON SCHEMA crawler FROM {}").format(role_identifier)
            )
            cursor.execute(
                sql.SQL("GRANT USAGE ON SCHEMA crawler TO {}").format(role_identifier)
            )
            cursor.execute(
                sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA crawler FROM {}").format(
                    role_identifier
                )
            )
            cursor.execute(
                "SELECT has_table_privilege(%s,'crawler.channels','SELECT')",
                (feature_user,),
            )
            if bool(cursor.fetchone()[0]):
                raise RuntimeError("Feature role unexpectedly inherited Crawler table access")

    print(
        json.dumps(
            {
                "event": "shared_feature_database_provisioned",
                "database": database,
                "feature_schema": "feature_clock",
                "feature_user": feature_user,
                "crawler_table_readable": False,
                "timezone": "UTC",
            },
            separators=(",", ":"),
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
