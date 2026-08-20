from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Protocol, Sequence

from .contracts import ChannelSnapshot
from .errors import ContractError, SnapshotError


@dataclass(frozen=True)
class SnapshotRecord:
    """One immutable crawler snapshot and the Channel Run current at that snapshot."""

    snapshot: ChannelSnapshot
    latest_run_id: str | None


class SnapshotRepository(Protocol):
    def load_many(self, channel_ids: Sequence[str]) -> dict[str, SnapshotRecord]:
        """Load one consistent snapshot for each available Channel ID."""


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if value is not None else {}


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _channel_payload(row: dict[str, Any]) -> dict[str, Any]:
    source_json = _json_object(row.get("source_json"))
    return {
        "channel_id": row.get("channel_id"),
        "channel_url": row.get("channel_url"),
        "handle": row.get("handle"),
        "title": row.get("title"),
        "country": row.get("country"),
        "country_source": row.get("country_source"),
        "country_code": row.get("country_code"),
        "country_canonical_name": row.get("country_canonical_name"),
        "avatar_url": row.get("avatar_url"),
        "summary": row.get("summary"),
        "keywords": row.get("keywords") or [],
        "about_description": row.get("about_description"),
        "joined_at": row.get("joined_at"),
        "external_links": row.get("external_links") or [],
        "is_verified": row.get("is_verified"),
        "subscriber_count": row.get("subscriber_count"),
        "total_view_count": row.get("total_view_count"),
        "total_video_count": row.get("total_video_count"),
        "channel_extractor": source_json.get("channel_extractor"),
    }


def _content_payload(row: dict[str, Any], *, include_comments: bool) -> dict[str, Any]:
    payload = {
        "source_content_id": row.get("source_content_id"),
        "content_type": row.get("content_type"),
        "content_type_source": row.get("content_type_source"),
        "title": row.get("title"),
        "description": row.get("description"),
        "description_status": row.get("description_status"),
        "description_source": row.get("description_source"),
        "thumbnail_url": row.get("thumbnail_url"),
        "keywords": row.get("keywords") or [],
        "hashtags": row.get("hashtags") or [],
        "published_at": row.get("published_at"),
        "published_at_status": row.get("published_at_status"),
        "published_at_source": row.get("published_at_source"),
        "published_at_precision": row.get("published_at_precision"),
        "first_seen_at": row.get("first_seen_at"),
        "view_count": row.get("view_count"),
        "view_count_status": row.get("view_count_status"),
        "view_count_source": row.get("view_count_source"),
        "like_count": row.get("like_count"),
        "like_count_status": row.get("like_count_status"),
        "like_count_source": row.get("like_count_source"),
        "comment_count": row.get("comment_count"),
        "comment_count_status": row.get("comment_count_status"),
        "comment_count_source": row.get("comment_count_source"),
        "comments_disabled": row.get("comments_disabled"),
        "duration_seconds": row.get("duration_seconds"),
        "duration_status": row.get("duration_status"),
        "duration_source": row.get("duration_source"),
        "extractor_version": row.get("extractor_version"),
    }
    if include_comments and row.get("comments_first_page") is not None:
        comments = row.get("comments_first_page")
        payload["comments_first_page"] = _json_object(comments) if isinstance(comments, str) else comments
    return payload


def _default_connection() -> Any:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as error:
        raise RuntimeError(
            "PostgresSnapshotRepository requires psycopg; install qy-channel-profile with runtime dependencies"
        ) from error

    connection_url = os.environ.get("LOCAL_PROFILE_DATABASE_URL") or os.environ.get("DATABASE_URL")
    common = {
        "autocommit": True,
        "row_factory": dict_row,
        "application_name": "qy_channel_profile_readonly_runtime",
    }
    if connection_url:
        return psycopg.connect(connection_url, **common)
    return psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "bullmq_crawler"),
        user=os.environ.get("POSTGRES_USER", "bullmq"),
        password=os.environ.get("POSTGRES_PASSWORD", "bullmq"),
        **common,
    )


class PostgresSnapshotRepository:
    """Read current crawler facts under one repeatable, read-only transaction."""

    def __init__(
        self,
        *,
        connection_factory: Callable[[], Any] = _default_connection,
        content_limit: int = 30,
        include_comments: bool = True,
        statement_timeout_seconds: int = 60,
    ) -> None:
        if not 1 <= int(content_limit) <= 100:
            raise ContractError("content_limit must be in [1, 100]")
        if not 1 <= int(statement_timeout_seconds) <= 300:
            raise ContractError("statement_timeout_seconds must be in [1, 300]")
        self._connection_factory = connection_factory
        self.content_limit = int(content_limit)
        self.include_comments = bool(include_comments)
        self.statement_timeout_seconds = int(statement_timeout_seconds)

    @classmethod
    def from_environment(
        cls,
        *,
        content_limit: int = 30,
        include_comments: bool = True,
        statement_timeout_seconds: int = 60,
    ) -> "PostgresSnapshotRepository":
        return cls(
            content_limit=content_limit,
            include_comments=include_comments,
            statement_timeout_seconds=statement_timeout_seconds,
        )

    def load_many(self, channel_ids: Sequence[str]) -> dict[str, SnapshotRecord]:
        requested = list(dict.fromkeys(str(value).strip() for value in channel_ids if str(value).strip()))
        if not requested:
            raise ContractError("channel_ids must contain at least one Channel ID")
        if len(requested) > 50:
            raise ContractError("channel_ids cannot contain more than 50 Channel IDs")

        connection = self._connection_factory()
        transaction_started = False
        try:
            connection.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            transaction_started = True
            connection.execute(f"SET LOCAL statement_timeout='{self.statement_timeout_seconds}s'")
            read_only_row = _mapping(connection.execute("SHOW transaction_read_only").fetchone())
            if read_only_row.get("transaction_read_only") != "on":
                raise SnapshotError("PostgreSQL snapshot transaction is not read-only")
            as_of_row = _mapping(
                connection.execute("SELECT transaction_timestamp() AS as_of").fetchone()
            )
            as_of = as_of_row.get("as_of")
            if not isinstance(as_of, datetime):
                raise SnapshotError("PostgreSQL did not return a valid snapshot timestamp")

            channel_rows = [
                _mapping(row)
                for row in connection.execute(
                    """
                    SELECT channel_id,channel_url,handle,title,country,country_source,
                           country_code,country_canonical_name,avatar_url,summary,keywords,
                           about_description,joined_at,external_links,is_verified,
                           subscriber_count,total_view_count,total_video_count,source_json,
                           latest_run_id
                    FROM crawler.channels
                    WHERE channel_id=ANY(%s::text[])
                      AND status IN ('active','dormant')
                    """,
                    (requested,),
                ).fetchall()
            ]
            content_rows = [
                _mapping(row)
                for row in connection.execute(
                    """
                    WITH ranked AS (
                      SELECT c.channel_id,c.source_content_id,c.content_type,c.content_type_source,
                             c.title,c.description,c.description_status,c.description_source,
                             c.thumbnail_url,c.keywords,c.hashtags,c.published_at,
                             c.published_at_status,c.published_at_source,c.published_at_precision,
                             c.first_seen_at,c.view_count,c.view_count_status,c.view_count_source,
                             c.like_count,c.like_count_status,c.like_count_source,c.comment_count,
                             c.comment_count_status,c.comment_count_source,c.comments_disabled,
                             c.duration_seconds,c.duration_status,c.duration_source,c.extractor_version,
                             c.comments_first_page,
                             row_number() OVER (
                               PARTITION BY c.channel_id
                               ORDER BY c.published_at DESC NULLS LAST,
                                        c.position ASC NULLS LAST,
                                        c.source_content_id
                             ) AS position_rank
                      FROM crawler.contents c
                      WHERE c.channel_id=ANY(%s::text[])
                        AND c.content_type IN ('video','short','live')
                    )
                    SELECT * FROM ranked
                    WHERE position_rank<=%s
                    ORDER BY channel_id,position_rank
                    """,
                    (requested, self.content_limit),
                ).fetchall()
            ]

            contents_by_channel = {channel_id: [] for channel_id in requested}
            for row in content_rows:
                channel_id = str(row.get("channel_id") or "")
                if channel_id in contents_by_channel:
                    contents_by_channel[channel_id].append(
                        _content_payload(row, include_comments=self.include_comments)
                    )

            # Importing here avoids making the repository own snapshot sanitization policy.
            from .runtime import snapshot_from_runtime

            records: dict[str, SnapshotRecord] = {}
            for row in channel_rows:
                channel_id = str(row.get("channel_id") or "")
                snapshot_value = {
                    "channel": _channel_payload(row),
                    "contents": contents_by_channel.get(channel_id, []),
                    "as_of": as_of,
                    "replay_quality": "current_exact",
                    "provenance": {
                        "data_lineage_version": "snapshot-field-lineage-v1",
                        "comment_page_source_status": (
                            "crawler_current" if self.include_comments else "not_requested"
                        ),
                        "content_set_boundary": "repeatable_read_transaction_latest_30",
                        "channel_text_temporality": "current_projection",
                        "content_metric_temporality": "current_projection",
                        "channel_stats_temporality": "current_projection",
                        "comments_included": self.include_comments,
                    },
                }
                records[channel_id] = SnapshotRecord(
                    snapshot=snapshot_from_runtime(snapshot_value),
                    latest_run_id=(str(row.get("latest_run_id")) if row.get("latest_run_id") else None),
                )
            return records
        finally:
            if transaction_started:
                try:
                    connection.execute("ROLLBACK")
                except Exception:
                    pass
            connection.close()
