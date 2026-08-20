from __future__ import annotations

import datetime
import os
import re
from typing import Any


DEFAULT_COMMENT_LIMIT = 20
MAX_COMMENT_LIMIT = 50
ORIGINAL_COMMENT_COUNT_KEY = "_qy_original_comment_count"


def comment_limit_from_env(value: Any = None) -> int:
    raw = value if value is not None else os.environ.get("YTDLP_COMMENT_FIRST_PAGE_MAX")
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        limit = DEFAULT_COMMENT_LIMIT
    return max(1, min(limit, MAX_COMMENT_LIMIT))


def bounded_youtube_extractor_args(
    youtube_args: dict[str, Any] | None,
    limit: Any = None,
) -> dict[str, Any]:
    clean_limit = comment_limit_from_env(limit)
    return {
        **(youtube_args or {}),
        "comment_sort": ["top"],
        # total, parents, replies, replies/thread, depth
        "max_comments": [str(clean_limit), str(clean_limit), "0", "0", "1"],
    }


def install_original_comment_count_capture(ydl: Any) -> None:
    original_post_extract = ydl.post_extract

    def capture(info: dict[str, Any] | None) -> None:
        value = info if isinstance(info, dict) else {}
        original_count = value.get("comment_count")
        already_captured = ORIGINAL_COMMENT_COUNT_KEY in value
        original_post_extract(info)
        if not already_captured and _nonnegative_integer(original_count) is not None:
            value[ORIGINAL_COMMENT_COUNT_KEY] = int(original_count)

    ydl.post_extract = capture


def original_comment_count(info: dict[str, Any] | None) -> int | None:
    if not isinstance(info, dict):
        return None
    return _nonnegative_integer(info.get(ORIGINAL_COMMENT_COUNT_KEY))


def _nonnegative_integer(value: Any) -> int | None:
    if isinstance(value, bool) or value is None or value == "":
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _utc_iso_from_timestamp(value: Any) -> str | None:
    timestamp = _nonnegative_integer(value)
    if timestamp is None:
        return None
    try:
        return (
            datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )
    except (OverflowError, OSError, ValueError):
        return None


def _normalized_collected_at(value: Any = None) -> str:
    if value is None:
        parsed = datetime.datetime.now(datetime.timezone.utc)
    elif isinstance(value, datetime.datetime):
        parsed = value
    else:
        parsed = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.astimezone(datetime.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def normalize_ytdlp_comment_page(
    comments: Any,
    *,
    total_count: Any = None,
    collected_at: Any = None,
) -> dict[str, Any] | None:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for value in comments if isinstance(comments, list) else []:
        if not isinstance(value, dict) or value.get("parent") not in (None, "", "root"):
            continue
        comment_id = str(value.get("id") or "").strip()
        if not comment_id or comment_id in seen:
            continue
        seen.add(comment_id)
        author_id = str(value.get("author_id") or "").strip() or None
        author_url = str(value.get("author_url") or "").strip() or None
        if author_url is None and author_id:
            author_url = f"https://www.youtube.com/channel/{author_id}"
        published_text = str(value.get("_time_text") or "").strip() or None
        published_at = _utc_iso_from_timestamp(value.get("timestamp"))
        rows.append(
            {
                "comment_id": comment_id,
                "position": len(rows) + 1,
                "text": str(value.get("text") or ""),
                "author_name": str(value.get("author") or "").strip() or None,
                "author_channel_id": author_id,
                "author_url": author_url,
                "author_avatar_url": str(value.get("author_thumbnail") or "").strip() or None,
                "published_at_utc": published_at,
                "published_text_raw": published_text,
                "published_at_status": "estimated_relative" if published_at else "unresolved",
                "is_edited": bool(re.search(r"\(\s*edited\s*\)", published_text or "", re.IGNORECASE)),
                "like_count": _nonnegative_integer(value.get("like_count")),
                "reply_count": None,
                "is_pinned": value.get("is_pinned") is True,
                "is_channel_owner": value.get("author_is_uploader") is True,
                "is_verified": value.get("author_is_verified") is True,
                "is_hearted": value.get("is_favorited") is True,
            }
        )

    clean_total = _nonnegative_integer(total_count)
    if not rows and clean_total != 0:
        return None
    if clean_total is not None:
        clean_total = max(clean_total, len(rows))
    return {
        "version": 1,
        "collected_at": _normalized_collected_at(collected_at),
        "sort": "TOP_COMMENTS",
        "total_count": clean_total,
        "returned_count": len(rows),
        "comments": rows,
    }
