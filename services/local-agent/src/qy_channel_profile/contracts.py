from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from .errors import ContractError, SnapshotError
from .hashing import sha256_json
from .taxonomy import TAXONOMY_VERSION, valid_categories


AGE_RANGES = ("18-24", "25-34", "35-44", "45-54", "55-64", "65+")
CONTENT_TYPES = ("video", "short", "live")
FACT_FIELDS = (
    "country",
    "creator_gender",
    "creator_age_range",
    "creator_language",
    "audience_region",
    "audience_age_gender",
    "audience_language",
    "active_subscriber_ratio",
    "channel_tags",
    "channel_categories",
)


class AnalysisPolicy(str, Enum):
    COMPLETE_ESTIMATE = "complete_estimate"
    EVIDENCE_FIRST = "evidence_first"


def parse_datetime(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    else:
        raise SnapshotError(f"{field_name} must be an ISO timestamp")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class ProfileAnalysisRequest:
    channel_id: str
    input_url: str
    as_of: datetime
    policy: AnalysisPolicy = AnalysisPolicy.COMPLETE_ESTIMATE
    expected_snapshot_hash: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.channel_id, str) or not self.channel_id.strip():
            raise ContractError("request.channel_id is required")
        if not isinstance(self.input_url, str):
            raise ContractError("request.input_url must be a string")
        if not isinstance(self.as_of, datetime) or self.as_of.tzinfo is None:
            raise ContractError("request.as_of must be a timezone-aware datetime")
        try:
            policy = AnalysisPolicy(self.policy)
        except ValueError as error:
            raise ContractError(f"unsupported analysis policy: {self.policy}") from error
        object.__setattr__(self, "policy", policy)
        object.__setattr__(self, "channel_id", self.channel_id.strip())
        object.__setattr__(self, "as_of", self.as_of.astimezone(timezone.utc))


@dataclass(frozen=True)
class CommentRecord:
    comment_id: str
    position: int
    text: str = ""
    author_name: str = ""
    author_channel_id: str = ""
    author_url: str = ""
    author_avatar_url: str = ""
    published_at: datetime | None = None
    published_text_raw: str = ""
    published_at_status: str = "unresolved"
    is_edited: bool = False
    like_count: int = 0
    reply_count: int = 0
    is_pinned: bool = False
    is_channel_owner: bool = False
    is_verified: bool = False
    is_hearted: bool = False

    @classmethod
    def from_mapping(cls, row: dict[str, Any], *, default_position: int) -> "CommentRecord":
        comment_id = str(row.get("comment_id") or "").strip()
        if not comment_id:
            raise SnapshotError("comment.comment_id is required")
        position = _nonnegative_int(row.get("position"))
        if position is None or position <= 0:
            position = default_position
        published = row.get("published_at_utc") or row.get("published_at")
        return cls(
            comment_id=comment_id,
            position=position,
            text=str(row.get("text") or ""),
            author_name=str(row.get("author_name") or row.get("author_display_name") or ""),
            author_channel_id=str(row.get("author_channel_id") or "").strip(),
            author_url=str(row.get("author_url") or ""),
            author_avatar_url=str(row.get("author_avatar_url") or ""),
            published_at=parse_datetime(published, field_name="comment.published_at") if published else None,
            published_text_raw=str(row.get("published_text_raw") or row.get("published_text") or ""),
            published_at_status=str(row.get("published_at_status") or "unresolved"),
            is_edited=row.get("is_edited") is True,
            like_count=_nonnegative_int(row.get("like_count")) or 0,
            reply_count=_nonnegative_int(row.get("reply_count")) or 0,
            is_pinned=row.get("is_pinned") is True,
            is_channel_owner=row.get("is_channel_owner") is True,
            is_verified=row.get("is_verified") is True,
            is_hearted=(
                row.get("is_hearted") is True
                or row.get("is_hearted_by_channel_owner") is True
            ),
        )

    def text_payload(self) -> dict[str, Any]:
        return {
            "id": self.comment_id,
            "text": self.text,
            "author_name": self.author_name,
            "author_channel_id": self.author_channel_id,
        }

    def stats_payload(self) -> dict[str, Any]:
        return {
            "id": self.comment_id,
            "position": self.position,
            "author_url": self.author_url,
            "author_avatar_url": self.author_avatar_url,
            "published_at": self.published_at,
            "published_text_raw": self.published_text_raw,
            "published_at_status": self.published_at_status,
            "is_edited": self.is_edited,
            "likes": self.like_count,
            "replies": self.reply_count,
            "is_pinned": self.is_pinned,
            "is_channel_owner": self.is_channel_owner,
            "is_verified": self.is_verified,
            "is_hearted": self.is_hearted,
        }


@dataclass(frozen=True)
class CommentPageRecord:
    collected_at: datetime
    sort: str
    total_count: int | None
    comments: tuple[CommentRecord, ...]
    version: int = 1

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "CommentPageRecord":
        try:
            version = int(value.get("version"))
        except (TypeError, ValueError) as error:
            raise SnapshotError("comments_first_page.version must be 1") from error
        if version != 1:
            raise SnapshotError("comments_first_page.version must be 1")
        sort = str(value.get("sort") or "").strip()
        if sort != "TOP_COMMENTS":
            raise SnapshotError("comments_first_page.sort must be TOP_COMMENTS")
        collected_at = parse_datetime(
            value.get("collected_at"),
            field_name="comments_first_page.collected_at",
        )
        rows = value.get("comments")
        if not isinstance(rows, list):
            raise SnapshotError("comments_first_page.comments must be a list")
        comments = tuple(
            CommentRecord.from_mapping(row, default_position=index)
            for index, row in enumerate(rows, start=1)
            if isinstance(row, dict)
        )
        if len(comments) != len(rows):
            raise SnapshotError("comments_first_page.comments contains a non-object row")
        returned_count = _nonnegative_int(value.get("returned_count"))
        if returned_count is None or returned_count != len(comments):
            raise SnapshotError("comments_first_page.returned_count does not match comments")
        comment_ids = [comment.comment_id for comment in comments]
        if len(comment_ids) != len(set(comment_ids)):
            raise SnapshotError("comments_first_page contains duplicate comment_id values")
        return cls(
            collected_at=collected_at,
            sort=sort,
            total_count=_nonnegative_int(value.get("total_count")),
            comments=comments,
            version=version,
        )

    @property
    def returned_count(self) -> int:
        return len(self.comments)

    def text_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "sort": self.sort,
            "comments": [comment.text_payload() for comment in self.comments],
        }

    def stats_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "sort": self.sort,
            "collected_at": self.collected_at,
            "total_count": self.total_count,
            "returned_count": self.returned_count,
            "comments": [comment.stats_payload() for comment in self.comments],
        }


@dataclass(frozen=True)
class ContentRecord:
    source_content_id: str
    content_type: str
    content_type_source: str = ""
    title: str = ""
    description: str = ""
    description_status: str = "unresolved"
    description_source: str = ""
    thumbnail_url: str = ""
    keywords: tuple[str, ...] = ()
    hashtags: tuple[str, ...] = ()
    published_at: datetime | None = None
    published_at_status: str = "unresolved"
    published_at_source: str = ""
    published_at_precision: str = "unknown"
    first_seen_at: datetime | None = None
    view_count: int | None = None
    view_count_status: str = "unresolved"
    view_count_source: str = ""
    like_count: int | None = None
    like_count_status: str = "unresolved"
    like_count_source: str = ""
    comment_count: int | None = None
    comment_count_status: str = "unresolved"
    comment_count_source: str = ""
    comments_disabled: bool | None = None
    duration_seconds: int | None = None
    duration_status: str = "unresolved"
    duration_source: str = ""
    extractor_version: str = ""
    comments_first_page: CommentPageRecord | None = None

    @classmethod
    def from_mapping(cls, row: dict[str, Any]) -> "ContentRecord":
        content_id = str(row.get("source_content_id") or row.get("content_id") or "").strip()
        if not content_id:
            raise SnapshotError("content source_content_id is required")
        content_type = str(row.get("content_type") or "").strip()
        if not content_type:
            raise SnapshotError("content content_type is required")
        if content_type not in CONTENT_TYPES:
            raise SnapshotError(f"unsupported content_type: {content_type}")
        published = row.get("published_at")
        first_seen = row.get("first_seen_at")
        return cls(
            source_content_id=content_id,
            content_type=content_type,
            content_type_source=str(row.get("content_type_source") or ""),
            title=str(row.get("title") or ""),
            description=str(row.get("description") or ""),
            description_status=str(row.get("description_status") or "unresolved"),
            description_source=str(row.get("description_source") or ""),
            thumbnail_url=str(row.get("thumbnail_url") or ""),
            keywords=tuple(str(item) for item in (row.get("keywords") or []) if str(item).strip()),
            hashtags=tuple(str(item) for item in (row.get("hashtags") or []) if str(item).strip()),
            published_at=parse_datetime(published, field_name="content.published_at") if published else None,
            published_at_status=str(row.get("published_at_status") or "unresolved"),
            published_at_source=str(row.get("published_at_source") or ""),
            published_at_precision=str(row.get("published_at_precision") or "unknown"),
            first_seen_at=parse_datetime(first_seen, field_name="content.first_seen_at") if first_seen else None,
            view_count=_nonnegative_int(row.get("view_count")),
            view_count_status=str(row.get("view_count_status") or "unresolved"),
            view_count_source=str(row.get("view_count_source") or ""),
            like_count=_nonnegative_int(row.get("like_count")),
            like_count_status=str(row.get("like_count_status") or "unresolved"),
            like_count_source=str(row.get("like_count_source") or ""),
            comment_count=_nonnegative_int(row.get("comment_count")),
            comment_count_status=str(row.get("comment_count_status") or "unresolved"),
            comment_count_source=str(row.get("comment_count_source") or ""),
            comments_disabled=(
                row.get("comments_disabled")
                if isinstance(row.get("comments_disabled"), bool)
                else None
            ),
            duration_seconds=_nonnegative_int(row.get("duration_seconds")),
            duration_status=str(row.get("duration_status") or "unresolved"),
            duration_source=str(row.get("duration_source") or ""),
            extractor_version=str(row.get("extractor_version") or ""),
            comments_first_page=(
                CommentPageRecord.from_mapping(row["comments_first_page"])
                if isinstance(row.get("comments_first_page"), dict)
                else None
            ),
        )

    def text_payload(self) -> dict[str, Any]:
        return {
            "id": self.source_content_id,
            "type": self.content_type,
            "type_source": self.content_type_source,
            "published_at": self.published_at,
            "title": self.title,
            "description": self.description,
            "keywords": self.keywords,
            "hashtags": self.hashtags,
        }

    def stats_payload(self) -> dict[str, Any]:
        return {
            "id": self.source_content_id,
            "views": self.view_count,
            "views_status": self.view_count_status,
            "likes": self.like_count,
            "comments": self.comment_count,
            "comments_status": self.comment_count_status,
            "comments_disabled": self.comments_disabled,
        }

    def lineage_payload(self) -> dict[str, Any]:
        return {
            "id": self.source_content_id,
            "type_source": self.content_type_source,
            "description_status": self.description_status,
            "description_source": self.description_source,
            "published_at_status": self.published_at_status,
            "published_at_source": self.published_at_source,
            "published_at_precision": self.published_at_precision,
            "duration_status": self.duration_status,
            "duration_source": self.duration_source,
            "views_status": self.view_count_status,
            "views_source": self.view_count_source,
            "likes_status": self.like_count_status,
            "likes_source": self.like_count_source,
            "comments_status": self.comment_count_status,
            "comments_source": self.comment_count_source,
            "extractor_version": self.extractor_version,
        }


def _nonnegative_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


@dataclass(frozen=True)
class ChannelSnapshot:
    channel: dict[str, Any]
    contents: tuple[ContentRecord, ...]
    as_of: datetime
    replay_quality: str = "current_exact"
    provenance: dict[str, Any] = field(default_factory=dict)

    @property
    def channel_id(self) -> str:
        return str(self.channel.get("channel_id") or "")

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "ChannelSnapshot":
        channel = value.get("channel")
        if not isinstance(channel, dict) or not str(channel.get("channel_id") or "").strip():
            raise SnapshotError("snapshot.channel.channel_id is required")
        channel = dict(channel)
        for key in ("subscriber_count", "total_view_count", "total_video_count"):
            channel[key] = _nonnegative_int(channel.get(key))
        as_of = parse_datetime(value.get("as_of"), field_name="snapshot.as_of")
        contents = tuple(ContentRecord.from_mapping(row) for row in (value.get("contents") or []))
        content_ids = [content.source_content_id for content in contents]
        if len(content_ids) != len(set(content_ids)):
            raise SnapshotError("snapshot contains duplicate source_content_id values")
        for content in contents:
            if content.published_at and content.published_at > as_of:
                raise SnapshotError(f"content {content.source_content_id} is after snapshot.as_of")
            if content.first_seen_at and content.first_seen_at > as_of:
                raise SnapshotError(f"content {content.source_content_id} was first seen after snapshot.as_of")
            if content.comments_first_page and content.comments_first_page.collected_at > as_of:
                raise SnapshotError(
                    f"content {content.source_content_id} comments were collected after snapshot.as_of"
                )
        return cls(
            channel=channel,
            contents=contents,
            as_of=as_of,
            replay_quality=str(value.get("replay_quality") or "current_exact"),
            provenance=dict(value.get("provenance") or {}),
        )

    def hashes(self) -> dict[str, str]:
        profile_text = {
            key: self.channel.get(key)
            for key in (
                "channel_id", "title", "handle", "summary", "about_description",
                "keywords", "country", "country_code", "country_canonical_name",
                "external_links",
            )
        }
        channel_stats = {
            key: self.channel.get(key)
            for key in (
                "subscriber_count", "total_view_count", "total_video_count", "is_verified",
            )
        }
        ordered_contents = sorted(self.contents, key=lambda item: item.source_content_id)
        content_text = [item.text_payload() for item in ordered_contents]
        content_stats = [item.stats_payload() for item in ordered_contents]
        comment_text = [
            {
                "content_id": item.source_content_id,
                "page": item.comments_first_page.text_payload(),
            }
            for item in ordered_contents
            if item.comments_first_page is not None
        ]
        comment_stats = [
            {
                "content_id": item.source_content_id,
                "page": item.comments_first_page.stats_payload(),
            }
            for item in ordered_contents
            if item.comments_first_page is not None
        ]
        field_lineage = {
            "channel": {
                "country_source": self.channel.get("country_source"),
                "channel_extractor": self.channel.get("channel_extractor"),
            },
            "contents": [item.lineage_payload() for item in ordered_contents],
            "data_lineage_version": self.provenance.get("data_lineage_version"),
            "comment_page_source_status": self.provenance.get("comment_page_source_status"),
        }
        media_assets = {
            "avatar_url": self.channel.get("avatar_url"),
            "thumbnails": sorted(item.thumbnail_url for item in self.contents if item.thumbnail_url),
        }
        hashes = {
            "input_content_hash": sha256_json(sorted(item.source_content_id for item in self.contents)),
            "profile_text_hash": sha256_json(profile_text),
            "content_text_hash": sha256_json(content_text),
            "channel_stats_hash": sha256_json(channel_stats),
            "content_stats_hash": sha256_json(content_stats),
            "comment_text_hash": sha256_json(comment_text),
            "comment_stats_hash": sha256_json(comment_stats),
            "media_asset_hash": sha256_json(media_assets),
            "lineage_hash": sha256_json(field_lineage),
        }
        hashes["snapshot_hash"] = sha256_json({
            **hashes,
            "as_of": self.as_of,
            "quality": self.replay_quality,
            "schema": "channel-snapshot-v4-field-lineage",
        })
        return hashes


@dataclass(frozen=True)
class FieldResult:
    value: Any
    source_type: str
    truth_status: str
    evidence_strength: str
    model_confidence: float
    evidence_confidence: float
    candidates: tuple[dict[str, Any], ...] = ()
    evidence_refs: tuple[str, ...] = ()
    abstained: bool = False
    model_version: str = "deterministic-baseline-v1"
    decision_policy_version: str = "shadow-policy-v1"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "candidates": list(self.candidates),
            "evidence_refs": list(self.evidence_refs),
        }


@dataclass(frozen=True)
class ProfileAnalysisResult:
    channel_id: str
    input_url: str
    analysis_status: str
    snapshot: dict[str, Any]
    facts: dict[str, FieldResult]
    processor: dict[str, str]
    diagnostics: tuple[dict[str, Any], ...] = ()

    def validate(self) -> None:
        if tuple(self.facts) != FACT_FIELDS:
            raise ContractError("result must contain the ten fact fields in contract order")
        for name, result in self.facts.items():
            for confidence_name, confidence in (
                ("model_confidence", result.model_confidence),
                ("evidence_confidence", result.evidence_confidence),
            ):
                if not 0 <= confidence <= 1:
                    raise ContractError(f"{name}.{confidence_name} must be in [0, 1]")
            if result.value is None and not result.abstained:
                raise ContractError(f"{name} has null value without abstaining")
        has_unavailable = any(result.value is None for result in self.facts.values())
        expected_status = "partial_failure" if has_unavailable else "completed_with_estimates"
        if self.analysis_status != expected_status:
            raise ContractError("analysis_status does not match field availability")
        _validate_distributions(self.facts)

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "channel_id": self.channel_id,
            "input_url": self.input_url,
            "analysis_status": self.analysis_status,
            "snapshot": self.snapshot,
            "facts": {name: result.to_dict() for name, result in self.facts.items()},
            "processor": self.processor,
            "diagnostics": list(self.diagnostics),
        }


def _validate_distributions(facts: dict[str, FieldResult]) -> None:
    for name in ("country", "creator_language"):
        value = facts[name].value
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise ContractError(f"{name} must be a non-empty string")
    gender = facts["creator_gender"].value
    if gender is not None and gender not in {"male", "female", "brand_team"}:
        raise ContractError("creator_gender is invalid")
    age = facts["creator_age_range"].value
    if age is not None and (not isinstance(age, int) or isinstance(age, bool) or not 0 <= age <= 120):
        raise ContractError("creator_age_range must be an integer in [0, 120]")
    active = facts["active_subscriber_ratio"].value
    if active is not None and (not isinstance(active, int) or isinstance(active, bool) or not 0 <= active <= 100):
        raise ContractError("active_subscriber_ratio must be an integer in [0, 100]")

    for name, label_key in (("audience_region", "region"), ("audience_language", "language")):
        value = facts[name].value
        if value is None:
            continue
        if not isinstance(value, list) or not value:
            raise ContractError(f"{name} must be a non-empty list")
        labels = [row.get(label_key) for row in value if isinstance(row, dict)]
        if len(labels) != len(value) or any(not isinstance(label, str) or not label for label in labels):
            raise ContractError(f"{name} labels are invalid")
        if len(labels) != len({label.casefold() for label in labels}):
            raise ContractError(f"{name} labels must be unique")
        if sum(int(row["percentage"]) for row in value) != 100:
            raise ContractError(f"{name} must sum to 100")
    regions = facts["audience_region"].value
    if regions is not None and (len(regions) != 6 or regions[-1]["region"] != "Other"):
        raise ContractError("audience_region must contain Top 5 + Other")
    age_gender = facts["audience_age_gender"].value
    if age_gender is not None:
        if [row.get("age_range") for row in age_gender] != list(AGE_RANGES):
            raise ContractError("audience_age_gender age order is invalid")
        if sum(int(row[gender]) for row in age_gender for gender in ("male", "female")) != 100:
            raise ContractError("audience_age_gender must sum to 100")
    tags = facts["channel_tags"].value
    if tags is not None:
        names = tags.get("tags") or []
        distribution = tags.get("top_5_distribution") or []
        if len(names) != 10 or len({str(name).casefold() for name in names}) != 10:
            raise ContractError("channel_tags.tags must contain ten unique tags")
        if len(distribution) != 6 or distribution[-1].get("tag") != "Other":
            raise ContractError("channel_tags.top_5_distribution must contain Top 5 + Other")
        if [row.get("tag") for row in distribution[:5]] != names[:5]:
            raise ContractError("channel tag distribution must correspond to the first five tags")
        if sum(int(row["percentage"]) for row in distribution) != 100:
            raise ContractError("channel_tags.top_5_distribution must sum to 100")
    categories = facts["channel_categories"].value
    if categories is not None and not valid_categories(categories):
        raise ContractError(f"channel_categories is not valid for {TAXONOMY_VERSION}")
