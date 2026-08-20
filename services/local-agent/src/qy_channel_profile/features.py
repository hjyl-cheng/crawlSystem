from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import median
from typing import Any, Protocol, Sequence

from .comment_features import (
    COMMENT_NUMERIC_FEATURE_NAMES,
    CommentEvidence,
    build_comment_evidence,
)
from .contracts import ChannelSnapshot
from .creator_evidence import CreatorEvidence, extract_creator_evidence
from .text_features import (
    LanguageEvidence,
    TextUnit,
    corpus,
    detect_languages,
    normalize_text,
    snapshot_text_units,
)


FEATURE_SCHEMA_VERSION = "channel-features-v6-source-content-type-field-routing"


class LanguageIdentifier(Protocol):
    version: str

    def detect(self, units: Sequence[TextUnit]) -> LanguageEvidence:
        ...


def _quantile(values: Sequence[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = max(0.0, min(1.0, probability)) * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _safe_ratio(numerator: float, denominator: float, *, smoothing: float = 1.0) -> float:
    return max(0.0, float(numerator)) / max(smoothing, float(denominator))


def _parse_optional_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _language_entropy(probabilities: dict[str, float]) -> float:
    values = [value for value in probabilities.values() if value > 0]
    if len(values) <= 1:
        return 0.0
    entropy = -sum(value * math.log(value) for value in values)
    return entropy / math.log(len(values))


@dataclass(frozen=True)
class ChannelFeatures:
    channel_id: str
    as_of: datetime
    hashes: dict[str, str]
    channel_text: str
    content_text: str
    full_text: str
    text_units: tuple[TextUnit, ...]
    language: LanguageEvidence
    comments: CommentEvidence
    creator_evidence: CreatorEvidence
    numeric: dict[str, float]
    missing: tuple[str, ...]
    feature_schema_version: str = FEATURE_SCHEMA_VERSION

    def model_text(self, field: str) -> str:
        if field in {"country", "creator_age_range"}:
            return self.channel_text
        if field == "creator_gender":
            return f"{self.channel_text} {self.comments.creator_address_text}".strip()
        if field == "channel_categories":
            return self.full_text
        if field == "channel_tags":
            return f"{self.full_text} {self.comments.topic_text}".strip()
        return self.full_text

    def numeric_vector(self, names: Sequence[str]) -> list[float]:
        return [float(self.numeric.get(name, 0.0)) for name in names]

    def to_record(self) -> dict[str, Any]:
        return {
            "channel_id": self.channel_id,
            "as_of": self.as_of.isoformat().replace("+00:00", "Z"),
            "feature_schema_version": self.feature_schema_version,
            **self.hashes,
            "channel_text": self.channel_text,
            "content_text": self.content_text,
            "full_text": self.full_text,
            "language_probabilities": self.language.probabilities,
            "language_effective_characters": self.language.effective_characters,
            "language_source_count": self.language.source_count,
            "language_top_margin": self.language.top_margin,
            **self.comments.to_record(),
            "numeric": self.numeric,
            "missing": list(self.missing),
        }


BASE_NUMERIC_FEATURE_NAMES = (
    "log_subscribers",
    "log_total_views",
    "log_total_videos",
    "content_count",
    "recent_30_count",
    "recent_90_count",
    "short_ratio",
    "longform_ratio",
    "live_ratio",
    "uploads_per_30_days",
    "upload_interval_median_days",
    "upload_interval_iqr_days",
    "view_median_log",
    "view_q25_log",
    "view_q75_log",
    "view_iqr_log",
    "view_to_subscriber_log_gap",
    "view_top1_share",
    "view_top3_share",
    "like_view_median",
    "comment_view_median",
    "content_text_coverage",
    "channel_age_days",
    "newest_content_age_days",
    "oldest_content_age_days",
    "language_entropy",
    "language_effective_characters_log",
    "subscriber_missing",
    "view_stats_missing",
    "engagement_missing",
    "joined_at_missing",
)
NUMERIC_FEATURE_NAMES = (*BASE_NUMERIC_FEATURE_NAMES, *COMMENT_NUMERIC_FEATURE_NAMES)


def build_channel_features(
    snapshot: ChannelSnapshot,
    language_identifier: LanguageIdentifier | None = None,
) -> ChannelFeatures:
    units = tuple(snapshot_text_units(snapshot))
    language = (
        language_identifier.detect(units)
        if language_identifier is not None
        else detect_languages(units)
    )
    comments = build_comment_evidence(snapshot, language_identifier)
    creator_evidence = extract_creator_evidence(snapshot)
    channel_text = corpus(snapshot, include_content=False, units=units)
    content_units = tuple(unit for unit in units if unit.source.startswith("content_"))
    content_text = " ".join(normalize_text(unit.text) for unit in content_units)
    full_text = f"{channel_text} {content_text}".strip()

    subscriber_count = snapshot.channel.get("subscriber_count")
    total_view_count = snapshot.channel.get("total_view_count")
    total_video_count = snapshot.channel.get("total_video_count")
    subscriber = float(subscriber_count or 0)
    total_views = float(total_view_count or 0)
    total_videos = float(total_video_count or 0)

    content_types = Counter(content.content_type for content in snapshot.contents)
    count = len(snapshot.contents)
    published = sorted(
        content.published_at for content in snapshot.contents if content.published_at is not None
    )
    ages = [
        max(0.0, (snapshot.as_of - value).total_seconds() / 86400.0)
        for value in published
    ]
    recent_30 = sum(age <= 30 for age in ages)
    recent_90 = sum(age <= 90 for age in ages)
    intervals = [
        max(0.0, (right - left).total_seconds() / 86400.0)
        for left, right in zip(published, published[1:])
    ]

    view_values = [float(content.view_count) for content in snapshot.contents if content.view_count is not None]
    log_views = [math.log1p(value) for value in view_values]
    sorted_views = sorted(view_values, reverse=True)
    view_total = sum(sorted_views)
    like_ratios = [
        _safe_ratio(content.like_count or 0, content.view_count or 0, smoothing=100.0)
        for content in snapshot.contents
        if content.like_count is not None and content.view_count is not None
    ]
    comment_ratios = [
        _safe_ratio(content.comment_count or 0, content.view_count or 0, smoothing=100.0)
        for content in snapshot.contents
        if content.comment_count is not None and content.view_count is not None
    ]
    described = sum(bool(normalize_text(content.description)) for content in snapshot.contents)
    joined_at = _parse_optional_datetime(snapshot.channel.get("joined_at"))

    q25 = _quantile(log_views, 0.25)
    q75 = _quantile(log_views, 0.75)
    interval_q25 = _quantile(intervals, 0.25)
    interval_q75 = _quantile(intervals, 0.75)
    view_median = median(log_views) if log_views else 0.0
    numeric = {
        "log_subscribers": math.log1p(subscriber),
        "log_total_views": math.log1p(total_views),
        "log_total_videos": math.log1p(total_videos),
        "content_count": float(count),
        "recent_30_count": float(recent_30),
        "recent_90_count": float(recent_90),
        "short_ratio": _safe_ratio(content_types.get("short", 0), count),
        "longform_ratio": _safe_ratio(content_types.get("video", 0), count),
        "live_ratio": _safe_ratio(content_types.get("live", 0), count),
        "uploads_per_30_days": float(recent_30),
        "upload_interval_median_days": median(intervals) if intervals else 0.0,
        "upload_interval_iqr_days": max(0.0, (interval_q75 or 0.0) - (interval_q25 or 0.0)),
        "view_median_log": view_median,
        "view_q25_log": q25 or 0.0,
        "view_q75_log": q75 or 0.0,
        "view_iqr_log": max(0.0, (q75 or 0.0) - (q25 or 0.0)),
        "view_to_subscriber_log_gap": view_median - math.log1p(subscriber),
        "view_top1_share": _safe_ratio(sum(sorted_views[:1]), view_total),
        "view_top3_share": _safe_ratio(sum(sorted_views[:3]), view_total),
        "like_view_median": median(like_ratios) if like_ratios else 0.0,
        "comment_view_median": median(comment_ratios) if comment_ratios else 0.0,
        "content_text_coverage": _safe_ratio(described, count),
        "channel_age_days": max(0.0, (snapshot.as_of - joined_at).total_seconds() / 86400.0) if joined_at else 0.0,
        "newest_content_age_days": min(ages) if ages else 0.0,
        "oldest_content_age_days": max(ages) if ages else 0.0,
        "language_entropy": _language_entropy(language.probabilities),
        "language_effective_characters_log": math.log1p(language.effective_characters),
        "subscriber_missing": float(subscriber_count is None),
        "view_stats_missing": float(not view_values),
        "engagement_missing": float(not like_ratios and not comment_ratios),
        "joined_at_missing": float(joined_at is None),
        **comments.numeric,
    }
    missing = tuple(
        name for name, flag in (
            ("subscriber_count", subscriber_count is None),
            ("total_view_count", total_view_count is None),
            ("total_video_count", total_video_count is None),
            ("published_at", not published),
            ("view_count", not view_values),
            ("engagement", not like_ratios and not comment_ratios),
            ("joined_at", joined_at is None),
            ("text", not full_text),
            ("comments", not comments.has_comments),
        )
        if flag
    )
    if tuple(numeric) != NUMERIC_FEATURE_NAMES:
        raise AssertionError("numeric feature order drifted")
    return ChannelFeatures(
        channel_id=snapshot.channel_id,
        as_of=snapshot.as_of,
        hashes=snapshot.hashes(),
        channel_text=channel_text,
        content_text=content_text,
        full_text=full_text,
        text_units=units,
        language=language,
        comments=comments,
        creator_evidence=creator_evidence,
        numeric=numeric,
        missing=missing,
    )
