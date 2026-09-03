from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import json
import math
import re
from types import MappingProxyType
from typing import Any, Mapping, TypeAlias
from uuid import UUID

from .contracts import (
    ContractValidationError,
    VIDEO_DISPOSITION_KINDS,
    VIDEO_DISPOSITION_LEDGER_FIELDS,
    VIDEO_DISPOSITION_LEDGER_TRIGGER_FIELDS,
    validate_crawler_observation_contract,
)
from .utc import as_utc


RESOLVED_STATUSES = frozenset({"exact", "estimated"})
METRIC_STATUSES = frozenset({"exact", "estimated", "unavailable", "unresolved"})
OBSERVATION_KINDS = frozenset(
    {"about", "video", "agent"}
)
OUTCOMES = frozenset({"complete", "partial", "failed"})
ABOUT_PAYLOAD_KEYS = frozenset(
    {
        "subscriber_count",
        "subscriber_count_status",
        "total_view_count",
        "total_view_count_status",
        "total_video_count",
        "total_video_count_status",
    }
)
EVENT_REQUIRED_KEYS = frozenset(
    {
        "event_id",
        "event_type",
        "event_version",
        "observation_id",
        "channel_id",
        "observation_kind",
        "kind_sequence",
        "observed_at",
        "outcome",
        "crawler_version",
        "payload_hash",
        "payload",
    }
)
EVENT_OPTIONAL_KEYS = frozenset({"plan_id"})
SHA256_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")


class EventValidationError(ValueError):
    pass


def _required_text(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise EventValidationError(f"{field} must be a string")
    output = value.strip()
    if not output:
        raise EventValidationError(f"{field} is required")
    return output


def _optional_text(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, field)


def _uuid(value: Any, field: str) -> str:
    text = _required_text(value, field)
    try:
        return str(UUID(text))
    except (TypeError, ValueError) as error:
        raise EventValidationError(f"{field} must be a UUID") from error


def _timestamp(value: Any, field: str) -> datetime:
    text = _required_text(value, field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise EventValidationError(f"{field} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise EventValidationError(f"{field} must include a timezone")
    return as_utc(parsed, field)


def _timestamp_text(value: Any, field: str) -> str:
    text = _required_text(value, field)
    _timestamp(text, field)
    return text


def _exact_keys(source: Mapping[str, Any], expected: frozenset[str], label: str) -> None:
    unexpected = set(source) - expected
    missing = expected - set(source)
    if unexpected or missing:
        raise EventValidationError(
            f"{label} keys differ: missing={sorted(missing)}, unexpected={sorted(unexpected)}"
        )


def _required_optional_keys(
    source: Mapping[str, Any],
    *,
    required: frozenset[str],
    optional: frozenset[str],
    label: str,
) -> None:
    unexpected = set(source) - required - optional
    missing = required - set(source)
    if unexpected or missing:
        raise EventValidationError(
            f"{label} keys differ: missing={sorted(missing)}, unexpected={sorted(unexpected)}"
        )


def _boolean(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise EventValidationError(f"{field} must be a boolean")
    return value


def _integer(value: Any, field: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise EventValidationError(f"{field} must be an integer")
    if minimum is not None and value < minimum:
        raise EventValidationError(f"{field} must be at least {minimum}")
    return value


def _number(
    value: Any,
    field: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EventValidationError(f"{field} must be a number")
    if not math.isfinite(float(value)):
        raise EventValidationError(f"{field} must be finite")
    if minimum is not None and value < minimum:
        raise EventValidationError(f"{field} must be at least {minimum}")
    if maximum is not None and value > maximum:
        raise EventValidationError(f"{field} must be at most {maximum}")
    return value


def _string_list(value: Any, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise EventValidationError(f"{field} must be an array")
    output = tuple(_required_text(item, f"{field}[]") for item in value)
    if len(output) != len(set(output)):
        raise EventValidationError(f"{field} cannot contain duplicates")
    return output


def _sha256(value: Any, field: str) -> str:
    text = _required_text(value, field)
    if not SHA256_PATTERN.fullmatch(text):
        raise EventValidationError(f"{field} must be a sha256 digest")
    return text


def _js_number(value: int | float) -> str:
    """Serialize the finite numeric range emitted by JSON.stringify."""
    if isinstance(value, int):
        return str(value)
    if value == 0:
        return "0"
    raw = repr(value).lower()
    absolute = abs(value)
    if 1e-6 <= absolute < 1e21:
        if "e" not in raw:
            return raw[:-2] if raw.endswith(".0") else raw
        mantissa, exponent_text = raw.split("e")
        exponent = int(exponent_text)
        negative = mantissa.startswith("-")
        digits = mantissa.lstrip("-").replace(".", "")
        decimal_position = (1 if "." in mantissa else len(digits)) + exponent
        if decimal_position <= 0:
            output = "0." + ("0" * -decimal_position) + digits
        elif decimal_position >= len(digits):
            output = digits + ("0" * (decimal_position - len(digits)))
        else:
            output = digits[:decimal_position] + "." + digits[decimal_position:]
        return ("-" if negative else "") + output
    if "e" not in raw:
        raw = format(value, ".15e")
    mantissa, exponent_text = raw.split("e")
    if mantissa.endswith(".0"):
        mantissa = mantissa[:-2]
    exponent = int(exponent_text)
    sign = "+" if exponent >= 0 else "-"
    return f"{mantissa}e{sign}{abs(exponent)}"


def canonical_json(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if not math.isfinite(float(value)):
            raise EventValidationError("payload numbers must be finite")
        return _js_number(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, list) or isinstance(value, tuple):
        return "[" + ",".join(canonical_json(item) for item in value) + "]"
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise EventValidationError("payload object keys must be strings")
        return "{" + ",".join(
            f"{json.dumps(key, ensure_ascii=False)}:{canonical_json(value[key])}"
            for key in sorted(value)
        ) + "}"
    raise EventValidationError(f"payload contains unsupported value type: {type(value).__name__}")


def canonical_payload_hash(value: Mapping[str, Any]) -> str:
    body = canonical_json(value)
    return f"sha256:{sha256(body.encode('utf-8')).hexdigest()}"


def _freeze_json(value: Any, field: str) -> Any:
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if not math.isfinite(float(value)):
            raise EventValidationError(f"{field} numbers must be finite")
        return value
    if isinstance(value, list):
        return tuple(_freeze_json(item, f"{field}[]") for item in value)
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise EventValidationError(f"{field} object keys must be strings")
        return MappingProxyType(
            {key: _freeze_json(item, f"{field}.{key}") for key, item in value.items()}
        )
    raise EventValidationError(
        f"{field} contains unsupported value type: {type(value).__name__}"
    )


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _metric(value: Any, status: Any, field: str) -> tuple[int | None, str]:
    normalized_status = _required_text(status, f"{field}_status")
    if normalized_status not in METRIC_STATUSES:
        raise EventValidationError(f"invalid {field}_status: {normalized_status}")
    if value is None:
        normalized_value = None
    elif isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise EventValidationError(f"{field} must be a non-negative integer or null")
    else:
        normalized_value = value
    if (normalized_status in RESOLVED_STATUSES) != (normalized_value is not None):
        raise EventValidationError(f"{field} value and status disagree")
    return normalized_value, normalized_status


@dataclass(frozen=True, slots=True)
class AboutPayload:
    subscriber_count: int | None
    subscriber_count_status: str
    total_view_count: int | None
    total_view_count_status: str
    total_video_count: int | None
    total_video_count_status: str

    @classmethod
    def from_mapping(cls, source: Mapping[str, Any]) -> AboutPayload:
        _exact_keys(source, ABOUT_PAYLOAD_KEYS, "About payload")
        subscribers = _metric(
            source["subscriber_count"], source["subscriber_count_status"], "subscriber_count"
        )
        views = _metric(
            source["total_view_count"], source["total_view_count_status"], "total_view_count"
        )
        videos = _metric(
            source["total_video_count"], source["total_video_count_status"], "total_video_count"
        )
        return cls(
            subscriber_count=subscribers[0],
            subscriber_count_status=subscribers[1],
            total_view_count=views[0],
            total_view_count_status=views[1],
            total_video_count=videos[0],
            total_video_count_status=videos[1],
        )

    @property
    def resolved_metric_count(self) -> int:
        return sum(
            status in RESOLVED_STATUSES
            for status in (
                self.subscriber_count_status,
                self.total_view_count_status,
                self.total_video_count_status,
            )
        )

    def as_facts(self) -> dict[str, int | str | None]:
        # About predates the generic Writer; this insertion order is its hash contract.
        return {
            "subscriber_count": self.subscriber_count,
            "subscriber_count_status": self.subscriber_count_status,
            "total_view_count": self.total_view_count,
            "total_view_count_status": self.total_view_count_status,
            "total_video_count": self.total_video_count,
            "total_video_count_status": self.total_video_count_status,
        }

    def facts_hash(self) -> str:
        body = json.dumps(self.as_facts(), ensure_ascii=False, separators=(",", ":"))
        return f"sha256:{sha256(body.encode('utf-8')).hexdigest()}"


@dataclass(frozen=True, slots=True)
class FailedDomainPayload:
    failure_kind: str
    attempt_count: int
    removed_reason: str | None = None

    @classmethod
    def from_mapping(cls, source: Mapping[str, Any]) -> FailedDomainPayload:
        _required_optional_keys(
            source,
            required=frozenset({"failure_kind", "attempt_count"}),
            optional=frozenset({"removed_reason"}),
            label="failed domain payload",
        )
        failure_kind = _required_text(source["failure_kind"], "failure_kind")
        removed_reason = _optional_text(source.get("removed_reason"), "removed_reason")
        if failure_kind == "channel_removed" and removed_reason is None:
            raise EventValidationError("channel_removed requires removed_reason")
        if failure_kind != "channel_removed" and removed_reason is not None:
            raise EventValidationError("removed_reason requires channel_removed")
        return cls(
            failure_kind=failure_kind,
            attempt_count=_integer(source["attempt_count"], "attempt_count", minimum=1),
            removed_reason=removed_reason,
        )

    def as_facts(self) -> dict[str, Any]:
        facts = {
            "failure_kind": self.failure_kind,
            "attempt_count": self.attempt_count,
        }
        if self.removed_reason is not None:
            facts["removed_reason"] = self.removed_reason
        return facts


@dataclass(frozen=True, slots=True)
class FirstSeenVideo:
    video_id: str
    position: int
    content_type: str
    published_at: str | None
    published_at_precision: str

    @classmethod
    def from_mapping(cls, source: Mapping[str, Any], index: int) -> FirstSeenVideo:
        _exact_keys(
            source,
            frozenset(
                {"video_id", "position", "content_type", "published_at", "published_at_precision"}
            ),
            f"first_seen[{index}]",
        )
        content_type = _required_text(source["content_type"], f"first_seen[{index}].content_type")
        if content_type not in {"video", "short", "live"}:
            raise EventValidationError(f"invalid first_seen[{index}].content_type")
        precision = _required_text(
            source["published_at_precision"], f"first_seen[{index}].published_at_precision"
        )
        if precision not in {"second", "date_only", "unknown"}:
            raise EventValidationError(f"invalid first_seen[{index}].published_at_precision")
        published_at = source["published_at"]
        if published_at is not None:
            published_at = _timestamp_text(published_at, f"first_seen[{index}].published_at")
        elif precision != "unknown":
            raise EventValidationError("missing first_seen published_at requires unknown precision")
        return cls(
            video_id=_required_text(source["video_id"], f"first_seen[{index}].video_id"),
            position=_integer(source["position"], f"first_seen[{index}].position", minimum=1),
            content_type=content_type,
            published_at=published_at,
            published_at_precision=precision,
        )

    def as_facts(self) -> dict[str, Any]:
        return {
            "video_id": self.video_id,
            "position": self.position,
            "content_type": self.content_type,
            "published_at": self.published_at,
            "published_at_precision": self.published_at_precision,
        }


def _video_disposition_entries(value: Any, field: str) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        raise EventValidationError(f"{field} must be an array")
    output: list[dict[str, Any]] = []
    for index, raw in enumerate(value):
        if not isinstance(raw, Mapping):
            raise EventValidationError(f"{field}[{index}] must be an object")
        _exact_keys(
            raw,
            frozenset({"video_id", "kind", "reason_code", "retry_class"}),
            f"{field}[{index}]",
        )
        kind = _required_text(raw["kind"], f"{field}[{index}].kind")
        if kind not in VIDEO_DISPOSITION_KINDS:
            raise EventValidationError(f"invalid {field}[{index}].kind: {kind}")
        retry_class = _optional_text(raw["retry_class"], f"{field}[{index}].retry_class")
        if kind == "stored" and retry_class is not None:
            raise EventValidationError(f"stored {field}[{index}] cannot contain retry_class")
        if kind != "stored" and retry_class is None:
            raise EventValidationError(f"{kind} {field}[{index}] requires retry_class")
        output.append(
            {
                "video_id": _required_text(raw["video_id"], f"{field}[{index}].video_id"),
                "kind": kind,
                "reason_code": _required_text(
                    raw["reason_code"], f"{field}[{index}].reason_code"
                ),
                "retry_class": retry_class,
            }
        )
    ids = [item["video_id"] for item in output]
    if len(ids) != len(set(ids)):
        raise EventValidationError(f"{field} video_id values cannot contain duplicates")
    return tuple(output)


def _video_disposition_ledger(
    source: Mapping[str, Any],
    *,
    first_seen: tuple[FirstSeenVideo, ...],
    detail_success_count: int,
    detail_failure_count: int,
) -> dict[str, Any] | None:
    supplied_trigger_fields = VIDEO_DISPOSITION_LEDGER_TRIGGER_FIELDS & set(source)
    if not supplied_trigger_fields:
        return None
    supplied_fields = VIDEO_DISPOSITION_LEDGER_FIELDS & set(source)
    if supplied_fields != VIDEO_DISPOSITION_LEDGER_FIELDS:
        raise EventValidationError("Video disposition ledger fields must be supplied together")

    dispositions = _video_disposition_entries(source["dispositions"], "dispositions")
    recheck_dispositions = _video_disposition_entries(
        source["recheck_dispositions"], "recheck_dispositions"
    )
    disposition_ids = [item["video_id"] for item in dispositions]
    recheck_ids = [item["video_id"] for item in recheck_dispositions]
    if set(disposition_ids) & set(recheck_ids):
        raise EventValidationError(
            "dispositions and recheck_dispositions cannot overlap"
        )

    list_fields = (
        "silent_drop_video_ids",
        "unresolved_video_ids",
        "recheck_deferred_video_ids",
        "pending_deferred_video_ids",
        "blocking_deferred_video_ids",
    )
    id_lists = {field: _string_list(source[field], field) for field in list_fields}
    if set(id_lists["silent_drop_video_ids"]) & set(disposition_ids):
        raise EventValidationError("silent_drop_video_ids cannot have a disposition")

    counts = {
        field: _integer(source[field], field, minimum=0)
        for field in (
            "discovered_count",
            "silent_drop_count",
            "stored_count",
            "deferred_count",
            "terminal_excluded_count",
            "unresolved_count",
            "recheck_deferred_count",
            "pending_deferred_count",
            "recheck_stored_count",
            "recheck_terminal_excluded_count",
        )
    }
    if counts["silent_drop_count"] != len(id_lists["silent_drop_video_ids"]):
        raise EventValidationError("silent_drop_count must match silent_drop_video_ids")
    if counts["discovered_count"] != len(dispositions) + counts["silent_drop_count"]:
        raise EventValidationError(
            "discovered_count must match dispositions and silent drops"
        )

    disposition_kinds = [item["kind"] for item in dispositions]
    recheck_kinds = [item["kind"] for item in recheck_dispositions]
    expected_counts = {
        "stored_count": disposition_kinds.count("stored"),
        "deferred_count": disposition_kinds.count("deferred"),
        "terminal_excluded_count": disposition_kinds.count("terminal_excluded"),
        "recheck_stored_count": recheck_kinds.count("stored"),
        "recheck_deferred_count": recheck_kinds.count("deferred"),
        "recheck_terminal_excluded_count": recheck_kinds.count("terminal_excluded"),
    }
    for field, expected in expected_counts.items():
        if counts[field] != expected:
            raise EventValidationError(f"{field} must match disposition entries")

    recheck_deferred_ids = {
        item["video_id"] for item in recheck_dispositions if item["kind"] == "deferred"
    }
    if set(id_lists["recheck_deferred_video_ids"]) != recheck_deferred_ids:
        raise EventValidationError(
            "recheck_deferred_video_ids must match deferred recheck dispositions"
        )
    if counts["recheck_deferred_count"] != len(id_lists["recheck_deferred_video_ids"]):
        raise EventValidationError(
            "recheck_deferred_count must match recheck_deferred_video_ids"
        )
    if counts["pending_deferred_count"] != len(id_lists["pending_deferred_video_ids"]):
        raise EventValidationError(
            "pending_deferred_count must match pending_deferred_video_ids"
        )

    newly_deferred_ids = {
        item["video_id"] for item in dispositions if item["kind"] == "deferred"
    }
    expected_unresolved_ids = newly_deferred_ids | set(
        id_lists["pending_deferred_video_ids"]
    )
    if set(id_lists["unresolved_video_ids"]) != expected_unresolved_ids:
        raise EventValidationError(
            "unresolved_video_ids must match new and pending deferred Videos"
        )
    if counts["unresolved_count"] != len(id_lists["unresolved_video_ids"]):
        raise EventValidationError("unresolved_count must match unresolved_video_ids")
    required_blocking_ids = expected_unresolved_ids | recheck_deferred_ids
    if not set(id_lists["blocking_deferred_video_ids"]).issuperset(required_blocking_ids):
        raise EventValidationError(
            "blocking_deferred_video_ids must include all deferred Videos"
        )

    stored_ids = {
        item["video_id"]
        for item in (*dispositions, *recheck_dispositions)
        if item["kind"] == "stored"
    }
    first_seen_ids = [item.video_id for item in first_seen]
    if set(first_seen_ids) != stored_ids:
        raise EventValidationError("first_seen Videos must match stored dispositions")
    if len(first_seen) != counts["stored_count"] + counts["recheck_stored_count"]:
        raise EventValidationError(
            "first_seen_count must match stored disposition counts"
        )
    detail_count = detail_success_count + detail_failure_count
    if len(first_seen) > detail_success_count:
        raise EventValidationError("stored Videos require successful Detail evidence")
    if detail_count > len(dispositions) + len(recheck_dispositions):
        raise EventValidationError("Detail counts cannot exceed disposition entries")

    return {
        "discovered_count": counts["discovered_count"],
        "silent_drop_count": counts["silent_drop_count"],
        "silent_drop_video_ids": list(id_lists["silent_drop_video_ids"]),
        "dispositions": [dict(item) for item in dispositions],
        "recheck_dispositions": [dict(item) for item in recheck_dispositions],
        "stored_count": counts["stored_count"],
        "deferred_count": counts["deferred_count"],
        "terminal_excluded_count": counts["terminal_excluded_count"],
        "unresolved_count": counts["unresolved_count"],
        "unresolved_video_ids": list(id_lists["unresolved_video_ids"]),
        "recheck_deferred_video_ids": list(id_lists["recheck_deferred_video_ids"]),
        "recheck_deferred_count": counts["recheck_deferred_count"],
        "pending_deferred_video_ids": list(id_lists["pending_deferred_video_ids"]),
        "pending_deferred_count": counts["pending_deferred_count"],
        "blocking_deferred_video_ids": list(id_lists["blocking_deferred_video_ids"]),
        "recheck_stored_count": counts["recheck_stored_count"],
        "recheck_terminal_excluded_count": counts["recheck_terminal_excluded_count"],
    }


@dataclass(frozen=True, slots=True)
class VideoDiscoveryPayload:
    pages: int | None
    items: int
    anchor_matched: bool
    stop_reason: str
    parse_gap_count: int
    first_seen: tuple[FirstSeenVideo, ...]
    first_seen_count: int
    detail_success_count: int
    detail_failure_count: int
    unresolved_count: int | None = None
    unresolved_video_ids: tuple[str, ...] | None = None
    publication_scan_proof: dict[str, Any] | None = None
    first_page_item_count: int | None = None
    catch_up_item_count: int | None = None
    unclosed_video_ids: tuple[str, ...] | None = None
    gap_abandonment: dict[str, Any] | None = None
    disposition_ledger: dict[str, Any] | None = None

    @classmethod
    def from_mapping(cls, source: Mapping[str, Any], *, outcome: str) -> VideoDiscoveryPayload:
        base_keys = frozenset(
            {
                "pages",
                "items",
                "anchor_matched",
                "stop_reason",
                "parse_gap_count",
                "first_seen",
                "first_seen_count",
                "detail_success_count",
                "detail_failure_count",
            }
        )
        proof_keys = frozenset(
            {
                "inspected_count",
                "requested_limit",
                "content_max_age_days",
                "scan_policy_version",
                "terminal_condition",
                "qualified_count",
                "excluded_count",
                "age_boundary_crossed",
            }
        )
        incomplete_scan_keys = frozenset(
            {
                "first_page_item_count",
                "catch_up_item_count",
                "unclosed_video_ids",
            }
        )
        unresolved_keys = frozenset({"unresolved_count", "unresolved_video_ids"})
        gap_abandonment_keys = frozenset({"gap_abandonment"})
        _required_optional_keys(
            source,
            required=base_keys,
            optional=(
                proof_keys
                | incomplete_scan_keys
                | unresolved_keys
                | gap_abandonment_keys
                | VIDEO_DISPOSITION_LEDGER_TRIGGER_FIELDS
            ),
            label="Video Discovery payload",
        )
        supplied_proof = proof_keys & set(source)
        if supplied_proof and supplied_proof != proof_keys:
            raise EventValidationError("Publication scan proof fields must be supplied together")
        supplied_incomplete_scan = incomplete_scan_keys & set(source)
        if supplied_incomplete_scan and supplied_incomplete_scan != incomplete_scan_keys:
            raise EventValidationError(
                "Incomplete scan evidence fields must be supplied together"
            )
        supplied_unresolved = unresolved_keys & set(source)
        if supplied_unresolved and supplied_unresolved != unresolved_keys:
            raise EventValidationError(
                "Unresolved Video evidence fields must be supplied together"
            )
        raw_first_seen = source["first_seen"]
        if not isinstance(raw_first_seen, list):
            raise EventValidationError("first_seen must be an array")
        if not all(isinstance(item, Mapping) for item in raw_first_seen):
            raise EventValidationError("first_seen entries must be objects")
        first_seen = tuple(
            FirstSeenVideo.from_mapping(item, index) for index, item in enumerate(raw_first_seen)
        )
        if len({item.video_id for item in first_seen}) != len(first_seen):
            raise EventValidationError("first_seen video_id values must be unique")
        pages = (
            _integer(source["pages"], "pages", minimum=0)
            if source["pages"] is not None
            else None
        )
        if pages is None and not supplied_proof:
            raise EventValidationError("pages may be null only with Publication scan proof")
        items = _integer(source["items"], "items", minimum=0)
        anchor_matched = _boolean(source["anchor_matched"], "anchor_matched")
        stop_reason = _required_text(source["stop_reason"], "stop_reason")
        if stop_reason not in {
            "anchor_matched",
            "anchor_dates_exhausted",
            "list_end",
            "max_items",
            "max_pages",
            "pagination_error",
            "parse_gap",
            "catchup_limit",
            "gap_abandoned_latest_30",
            "qualified_item_limit",
            "age_boundary_crossed",
            "candidate_limit_processed",
        }:
            raise EventValidationError(f"invalid Discovery stop_reason: {stop_reason}")
        parse_gaps = _integer(source["parse_gap_count"], "parse_gap_count", minimum=0)
        first_seen_count = _integer(source["first_seen_count"], "first_seen_count", minimum=0)
        successes = _integer(source["detail_success_count"], "detail_success_count", minimum=0)
        failures = _integer(source["detail_failure_count"], "detail_failure_count", minimum=0)
        unresolved_count = None
        unresolved_video_ids = None
        if supplied_unresolved:
            unresolved_count = _integer(
                source["unresolved_count"], "unresolved_count", minimum=0
            )
            raw_unresolved_video_ids = source["unresolved_video_ids"]
            if not isinstance(raw_unresolved_video_ids, list):
                raise EventValidationError("unresolved_video_ids must be an array")
            unresolved_video_ids = tuple(
                _required_text(value, f"unresolved_video_ids[{index}]")
                for index, value in enumerate(raw_unresolved_video_ids)
            )
            if len(set(unresolved_video_ids)) != len(unresolved_video_ids):
                raise EventValidationError("unresolved_video_ids cannot contain duplicates")
            if unresolved_count != len(unresolved_video_ids):
                raise EventValidationError(
                    "unresolved_count must match unresolved_video_ids"
                )
        effective_unresolved_count = unresolved_count or 0
        disposition_ledger = _video_disposition_ledger(
            source,
            first_seen=first_seen,
            detail_success_count=successes,
            detail_failure_count=failures,
        )
        if first_seen_count != len(first_seen):
            raise EventValidationError("first_seen_count must match first_seen entries")
        if supplied_proof:
            if first_seen_count > successes:
                raise EventValidationError(
                    "Publication first_seen sample cannot exceed successful details"
                )
        elif (
            disposition_ledger is None
            and successes + failures != first_seen_count + effective_unresolved_count
        ):
            raise EventValidationError("Discovery first_seen/detail counts disagree")
        if disposition_ledger is None and first_seen_count > items:
            raise EventValidationError("first_seen_count cannot exceed items")
        if (
            disposition_ledger is None
            and first_seen_count + effective_unresolved_count > items
        ):
            raise EventValidationError(
                "resolved and unresolved first-seen counts cannot exceed items"
            )
        if stop_reason == "anchor_matched" and not anchor_matched:
            raise EventValidationError("anchor_matched disagrees with stop_reason")
        if anchor_matched and stop_reason not in {"anchor_matched", "parse_gap"}:
            raise EventValidationError("anchor_matched disagrees with stop_reason")
        first_page_item_count = None
        catch_up_item_count = None
        unclosed_video_ids = None
        if supplied_incomplete_scan:
            first_page_item_count = _integer(
                source["first_page_item_count"], "first_page_item_count", minimum=0
            )
            catch_up_item_count = _integer(
                source["catch_up_item_count"], "catch_up_item_count", minimum=0
            )
            raw_unclosed_video_ids = source["unclosed_video_ids"]
            if not isinstance(raw_unclosed_video_ids, list):
                raise EventValidationError("unclosed_video_ids must be an array")
            unclosed_video_ids = tuple(
                _required_text(value, f"unclosed_video_ids[{index}]")
                for index, value in enumerate(raw_unclosed_video_ids)
            )
            if len(set(unclosed_video_ids)) != len(unclosed_video_ids):
                raise EventValidationError("unclosed_video_ids must be unique")
            if len(unclosed_video_ids) != items:
                raise EventValidationError("unclosed_video_ids must match items")
            if first_page_item_count + catch_up_item_count != items:
                raise EventValidationError("Incomplete scan item counts must match items")
            if (
                first_seen_count != 0
                or effective_unresolved_count != 0
                or successes != 0
                or failures != 0
            ):
                raise EventValidationError(
                    "Incomplete scan evidence cannot contain committed Video facts"
                )
        if stop_reason == "catchup_limit":
            if not supplied_incomplete_scan:
                raise EventValidationError(
                    "catchup_limit requires incomplete scan evidence"
                )
            if anchor_matched:
                raise EventValidationError("catchup_limit cannot report an Anchor match")
        gap_abandonment = None
        if "gap_abandonment" in source:
            raw_gap_abandonment = source["gap_abandonment"]
            if not isinstance(raw_gap_abandonment, Mapping):
                raise EventValidationError("gap_abandonment must be an object")
            _exact_keys(
                raw_gap_abandonment,
                frozenset(
                    {
                        "policy_version",
                        "source_stop_reason",
                        "scanned_item_count",
                        "first_page_item_count",
                        "catch_up_item_count",
                        "catch_up_item_limit",
                        "selected_item_count",
                        "scanned_video_ids",
                        "selected_video_ids",
                        "abandoned_anchor_ids",
                    }
                ),
                "gap_abandonment",
            )
            policy_version = _required_text(
                raw_gap_abandonment["policy_version"], "gap_abandonment.policy_version"
            )
            source_stop_reason = _required_text(
                raw_gap_abandonment["source_stop_reason"],
                "gap_abandonment.source_stop_reason",
            )
            scanned_item_count = _integer(
                raw_gap_abandonment["scanned_item_count"],
                "gap_abandonment.scanned_item_count",
                minimum=1,
            )
            first_page_item_count = _integer(
                raw_gap_abandonment["first_page_item_count"],
                "gap_abandonment.first_page_item_count",
                minimum=0,
            )
            catch_up_item_count = _integer(
                raw_gap_abandonment["catch_up_item_count"],
                "gap_abandonment.catch_up_item_count",
                minimum=1,
            )
            catch_up_item_limit = _integer(
                raw_gap_abandonment["catch_up_item_limit"],
                "gap_abandonment.catch_up_item_limit",
                minimum=1,
            )
            selected_item_count = _integer(
                raw_gap_abandonment["selected_item_count"],
                "gap_abandonment.selected_item_count",
                minimum=1,
            )
            scanned_video_ids = _string_list(
                raw_gap_abandonment["scanned_video_ids"],
                "gap_abandonment.scanned_video_ids",
            )
            selected_video_ids = _string_list(
                raw_gap_abandonment["selected_video_ids"],
                "gap_abandonment.selected_video_ids",
            )
            abandoned_anchor_ids = _string_list(
                raw_gap_abandonment["abandoned_anchor_ids"],
                "gap_abandonment.abandoned_anchor_ids",
            )
            if policy_version != "latest-30-on-catchup-limit-v1":
                raise EventValidationError("unsupported gap_abandonment policy_version")
            if source_stop_reason != "catchup_limit":
                raise EventValidationError("gap_abandonment must originate from catchup_limit")
            if scanned_item_count != len(scanned_video_ids):
                raise EventValidationError(
                    "gap_abandonment scanned_item_count must match scanned_video_ids"
                )
            if first_page_item_count + catch_up_item_count != scanned_item_count:
                raise EventValidationError(
                    "gap_abandonment scan counts must match scanned_item_count"
                )
            if catch_up_item_count != catch_up_item_limit:
                raise EventValidationError(
                    "gap_abandonment must reach the configured Catch-up limit"
                )
            if len(set(scanned_video_ids)) != len(scanned_video_ids):
                raise EventValidationError(
                    "gap_abandonment scanned_video_ids cannot contain duplicates"
                )
            if len(set(selected_video_ids)) != len(selected_video_ids):
                raise EventValidationError(
                    "gap_abandonment selected_video_ids cannot contain duplicates"
                )
            if len(set(abandoned_anchor_ids)) != len(abandoned_anchor_ids):
                raise EventValidationError(
                    "gap_abandonment abandoned_anchor_ids cannot contain duplicates"
                )
            if selected_item_count != 30 or selected_item_count != len(selected_video_ids):
                raise EventValidationError(
                    "gap_abandonment selected_item_count must be 30 and match selected_video_ids"
                )
            if selected_video_ids != scanned_video_ids[:selected_item_count]:
                raise EventValidationError(
                    "gap_abandonment selected_video_ids must be the scanned prefix"
                )
            gap_abandonment = {
                "policy_version": policy_version,
                "source_stop_reason": source_stop_reason,
                "scanned_item_count": scanned_item_count,
                "first_page_item_count": first_page_item_count,
                "catch_up_item_count": catch_up_item_count,
                "catch_up_item_limit": catch_up_item_limit,
                "selected_item_count": selected_item_count,
                "scanned_video_ids": list(scanned_video_ids),
                "selected_video_ids": list(selected_video_ids),
                "abandoned_anchor_ids": list(abandoned_anchor_ids),
            }
        if stop_reason == "gap_abandoned_latest_30":
            if gap_abandonment is None:
                raise EventValidationError(
                    "gap_abandoned_latest_30 requires gap_abandonment proof"
                )
            if anchor_matched:
                raise EventValidationError(
                    "gap_abandoned_latest_30 cannot report an Anchor match"
                )
            if items != gap_abandonment["selected_item_count"]:
                raise EventValidationError(
                    "gap_abandoned_latest_30 items must match selected_item_count"
                )
            selected_ids = set(gap_abandonment["selected_video_ids"])
            recheck_stored_ids = {
                item["video_id"]
                for item in (
                    disposition_ledger["recheck_dispositions"]
                    if disposition_ledger is not None
                    else []
                )
                if item["kind"] == "stored"
            }
            if any(
                item.video_id not in selected_ids | recheck_stored_ids
                for item in first_seen
            ):
                raise EventValidationError(
                    "gap_abandoned_latest_30 first_seen must come from selected Videos"
                )
        elif gap_abandonment is not None:
            raise EventValidationError(
                "gap_abandonment proof requires gap_abandoned_latest_30"
            )
        publication_scan_proof = None
        if supplied_proof:
            publication_scan_proof = {
                "inspected_count": (
                    _integer(source["inspected_count"], "inspected_count", minimum=0)
                    if source["inspected_count"] is not None
                    else None
                ),
                "requested_limit": (
                    _integer(source["requested_limit"], "requested_limit", minimum=0)
                    if source["requested_limit"] is not None
                    else None
                ),
                "content_max_age_days": (
                    _integer(source["content_max_age_days"], "content_max_age_days", minimum=0)
                    if source["content_max_age_days"] is not None
                    else None
                ),
                "scan_policy_version": _optional_text(
                    source["scan_policy_version"], "scan_policy_version"
                ),
                "terminal_condition": _optional_text(
                    source["terminal_condition"], "terminal_condition"
                ),
                "qualified_count": (
                    _integer(source["qualified_count"], "qualified_count", minimum=0)
                    if source["qualified_count"] is not None
                    else None
                ),
                "excluded_count": (
                    _integer(source["excluded_count"], "excluded_count", minimum=0)
                    if source["excluded_count"] is not None
                    else None
                ),
                "age_boundary_crossed": _boolean(
                    source["age_boundary_crossed"], "age_boundary_crossed"
                ),
            }
            inspected_count = publication_scan_proof["inspected_count"]
            terminal_condition = publication_scan_proof["terminal_condition"]
            qualified_count = publication_scan_proof["qualified_count"]
            excluded_count = publication_scan_proof["excluded_count"]
            if inspected_count is not None and successes > inspected_count:
                raise EventValidationError(
                    "detail_success_count cannot exceed inspected_count"
                )
            if inspected_count is not None and failures > inspected_count:
                raise EventValidationError(
                    "detail_failure_count cannot exceed inspected_count"
                )
            if (
                qualified_count is not None
                and excluded_count is not None
                and successes != qualified_count + excluded_count
            ):
                raise EventValidationError(
                    "Publication detail success count disagrees with coverage"
                )
            if stop_reason == "qualified_item_limit" and (
                terminal_condition != "qualified_item_limit"
                or qualified_count is None
                or qualified_count < 30
            ):
                raise EventValidationError(
                    "qualified_item_limit requires at least 30 qualified items"
                )
            if stop_reason == "age_boundary_crossed" and (
                terminal_condition != "age_boundary_crossed"
                or publication_scan_proof["age_boundary_crossed"] is not True
                or publication_scan_proof["content_max_age_days"] != 90
            ):
                raise EventValidationError(
                    "age_boundary_crossed requires the 90-day boundary proof"
                )
            if stop_reason == "candidate_limit_processed" and (
                terminal_condition != "candidate_limit_processed"
                or items != 30
                or publication_scan_proof["requested_limit"] != 30
                or inspected_count is None
                or inspected_count < 30
                or publication_scan_proof["content_max_age_days"] != 90
                or failures != 0
            ):
                raise EventValidationError(
                    "candidate_limit_processed requires 30 inspected candidates without Detail failures"
                )
            if stop_reason == "list_end" and terminal_condition not in {None, "list_end"}:
                raise EventValidationError("list_end disagrees with terminal_condition")
        elif stop_reason in {
            "qualified_item_limit",
            "age_boundary_crossed",
            "candidate_limit_processed",
        }:
            raise EventValidationError("Publication completion reason requires scan proof")
        blocking_deferred_video_ids = (
            disposition_ledger["blocking_deferred_video_ids"]
            if disposition_ledger is not None
            else []
        )
        complete = stop_reason in {
            "anchor_matched",
            "anchor_dates_exhausted",
            "list_end",
            "gap_abandoned_latest_30",
            "qualified_item_limit",
            "age_boundary_crossed",
            "candidate_limit_processed",
        } and parse_gaps == 0 and effective_unresolved_count == 0 and not blocking_deferred_video_ids
        if outcome not in {"complete", "partial"} or (outcome == "complete") != complete:
            raise EventValidationError("Discovery outcome disagrees with scan coverage")
        return cls(
            pages,
            items,
            anchor_matched,
            stop_reason,
            parse_gaps,
            first_seen,
            first_seen_count,
            successes,
            failures,
            unresolved_count,
            unresolved_video_ids,
            publication_scan_proof,
            first_page_item_count,
            catch_up_item_count,
            unclosed_video_ids,
            gap_abandonment,
            disposition_ledger,
        )

    def as_facts(self) -> dict[str, Any]:
        facts = {
            "pages": self.pages,
            "items": self.items,
            "anchor_matched": self.anchor_matched,
            "stop_reason": self.stop_reason,
            "parse_gap_count": self.parse_gap_count,
            "first_seen": [item.as_facts() for item in self.first_seen],
            "first_seen_count": self.first_seen_count,
            "detail_success_count": self.detail_success_count,
            "detail_failure_count": self.detail_failure_count,
        }
        if self.unresolved_count is not None and self.disposition_ledger is None:
            facts.update(
                {
                    "unresolved_count": self.unresolved_count,
                    "unresolved_video_ids": list(self.unresolved_video_ids or ()),
                }
            )
        if self.publication_scan_proof is not None:
            facts.update(self.publication_scan_proof)
        if self.unclosed_video_ids is not None:
            facts.update(
                {
                    "first_page_item_count": self.first_page_item_count,
                    "catch_up_item_count": self.catch_up_item_count,
                    "unclosed_video_ids": list(self.unclosed_video_ids),
                }
            )
        if self.gap_abandonment is not None:
            facts["gap_abandonment"] = self.gap_abandonment
        if self.disposition_ledger is not None:
            facts.update(self.disposition_ledger)
        return facts


@dataclass(frozen=True, slots=True)
class VideoRecentSamplingPayload:
    recent_count: int
    stale_ratio: int | float
    selected_count: int
    success_count: int
    failure_count: int
    next_count: int
    comparable_view_count: int
    view_changed_count: int
    view_delta_total: int
    engagement_changed_count: int

    @classmethod
    def from_mapping(
        cls, source: Mapping[str, Any], *, outcome: str
    ) -> VideoRecentSamplingPayload:
        keys = frozenset(
            {
                "recent_count",
                "stale_ratio",
                "selected_count",
                "success_count",
                "failure_count",
                "next_count",
                "comparable_view_count",
                "view_changed_count",
                "view_delta_total",
                "engagement_changed_count",
            }
        )
        _exact_keys(source, keys, "Video Recent Sampling payload")
        recent = _integer(source["recent_count"], "recent_count", minimum=0)
        stale = _number(source["stale_ratio"], "stale_ratio", minimum=0.0, maximum=1.0)
        selected = _integer(source["selected_count"], "selected_count", minimum=0)
        success = _integer(source["success_count"], "success_count", minimum=0)
        failure = _integer(source["failure_count"], "failure_count", minimum=0)
        next_count = _integer(source["next_count"], "next_count", minimum=0)
        comparable = _integer(source["comparable_view_count"], "comparable_view_count", minimum=0)
        view_changed = _integer(source["view_changed_count"], "view_changed_count", minimum=0)
        view_delta = _integer(source["view_delta_total"], "view_delta_total")
        engagement = _integer(
            source["engagement_changed_count"], "engagement_changed_count", minimum=0
        )
        if selected > recent or success + failure != selected:
            raise EventValidationError("Recent Sampling selected/success/failure counts disagree")
        if next_count > selected or comparable > success or view_changed > comparable or engagement > success:
            raise EventValidationError("Recent Sampling coverage counts disagree")
        expected_outcome = "complete" if failure == 0 else "partial" if success > 0 else "failed"
        if outcome != expected_outcome:
            raise EventValidationError("Recent Sampling outcome disagrees with result counts")
        return cls(
            recent,
            stale,
            selected,
            success,
            failure,
            next_count,
            comparable,
            view_changed,
            view_delta,
            engagement,
        )

    def as_facts(self) -> dict[str, Any]:
        return {
            "recent_count": self.recent_count,
            "stale_ratio": self.stale_ratio,
            "selected_count": self.selected_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "next_count": self.next_count,
            "comparable_view_count": self.comparable_view_count,
            "view_changed_count": self.view_changed_count,
            "view_delta_total": self.view_delta_total,
            "engagement_changed_count": self.engagement_changed_count,
        }


@dataclass(frozen=True, slots=True)
class VideoRecentSamplingSkippedPayload:
    skipped_reason: str

    @classmethod
    def from_mapping(
        cls, source: Mapping[str, Any]
    ) -> VideoRecentSamplingSkippedPayload:
        _exact_keys(
            source,
            frozenset({"skipped_reason"}),
            "skipped Video Recent Sampling payload",
        )
        skipped_reason = _required_text(source["skipped_reason"], "skipped_reason")
        if skipped_reason != "discovery_incomplete":
            raise EventValidationError("invalid Video Recent Sampling skipped_reason")
        return cls(skipped_reason=skipped_reason)

    def as_facts(self) -> dict[str, Any]:
        return {"skipped_reason": self.skipped_reason}


@dataclass(frozen=True, slots=True)
class VideoActivityPayload:
    window_days: int
    recent_published_content_count: int
    lifecycle_status: str
    dormant_reason: str | None
    dormant_since: datetime | None
    dormant_recheck_day: str | None
    dormant_cycle: int

    @classmethod
    def from_mapping(cls, source: Mapping[str, Any]) -> VideoActivityPayload:
        keys = frozenset(
            {
                "window_days",
                "recent_published_content_count",
                "lifecycle_status",
                "dormant_reason",
                "dormant_since",
                "dormant_recheck_day",
                "dormant_cycle",
            }
        )
        _exact_keys(source, keys, "Video activity payload")
        status = _required_text(source["lifecycle_status"], "activity.lifecycle_status")
        if status not in {"active", "dormant"}:
            raise EventValidationError("invalid Video activity lifecycle_status")
        window_days = _integer(source["window_days"], "activity.window_days", minimum=1)
        if window_days != 90:
            raise EventValidationError("Video activity window_days must be 90")
        recent_count = _integer(
            source["recent_published_content_count"],
            "activity.recent_published_content_count",
            minimum=0,
        )
        dormant_reason = _optional_text(source["dormant_reason"], "activity.dormant_reason")
        dormant_since = (
            _timestamp(source["dormant_since"], "activity.dormant_since")
            if source["dormant_since"] is not None
            else None
        )
        recheck_day = _optional_text(
            source["dormant_recheck_day"], "activity.dormant_recheck_day"
        )
        if recheck_day is not None:
            try:
                parsed_day = datetime.fromisoformat(f"{recheck_day}T00:00:00+00:00")
            except ValueError as error:
                raise EventValidationError(
                    "activity.dormant_recheck_day must be an ISO date"
                ) from error
            if parsed_day.date().isoformat() != recheck_day:
                raise EventValidationError("activity.dormant_recheck_day must be an ISO date")
        cycle = _integer(source["dormant_cycle"], "activity.dormant_cycle", minimum=0)
        if status == "dormant":
            if (
                dormant_reason != "no_published_content_within_90_days"
                or dormant_since is None
                or recheck_day is None
                or cycle == 0
                or recent_count != 0
            ):
                raise EventValidationError("dormant Video activity fields disagree")
        elif dormant_reason is not None or dormant_since is not None or recheck_day is not None or cycle:
            raise EventValidationError("active Video activity cannot contain dormant fields")
        return cls(
            window_days=window_days,
            recent_published_content_count=recent_count,
            lifecycle_status=status,
            dormant_reason=dormant_reason,
            dormant_since=dormant_since,
            dormant_recheck_day=recheck_day,
            dormant_cycle=cycle,
        )

    def as_facts(self) -> dict[str, Any]:
        return {
            "window_days": self.window_days,
            "recent_published_content_count": self.recent_published_content_count,
            "lifecycle_status": self.lifecycle_status,
            "dormant_reason": self.dormant_reason,
            "dormant_since": (
                self.dormant_since.isoformat().replace("+00:00", "Z")
                if self.dormant_since is not None
                else None
            ),
            "dormant_recheck_day": self.dormant_recheck_day,
            "dormant_cycle": self.dormant_cycle,
        }


@dataclass(frozen=True, slots=True)
class VideoActivityEvidencePayload:
    raw: Mapping[str, Any]

    @classmethod
    def from_mapping(
        cls, source: Mapping[str, Any]
    ) -> VideoActivityEvidencePayload:
        if not isinstance(source, Mapping):
            raise EventValidationError("Video activity_evidence must be an object")
        frozen = _freeze_json(source, "activity_evidence")
        assert isinstance(frozen, Mapping)
        return cls(raw=frozen)

    def as_facts(self) -> dict[str, Any]:
        return _thaw_json(self.raw)


@dataclass(frozen=True, slots=True)
class VideoPayload:
    discovery_outcome: str
    discovery: VideoDiscoveryPayload
    recent_sampling_outcome: str
    recent_sampling: VideoRecentSamplingPayload | VideoRecentSamplingSkippedPayload
    activity_evidence: VideoActivityEvidencePayload | None = None
    activity: VideoActivityPayload | None = None

    @classmethod
    def from_mapping(cls, source: Mapping[str, Any], *, outcome: str) -> VideoPayload:
        _required_optional_keys(
            source,
            required=frozenset({"discovery", "recent_sampling"}),
            optional=frozenset({"activity_evidence", "activity"}),
            label="Video payload",
        )
        raw_discovery = source["discovery"]
        raw_sampling = source["recent_sampling"]
        if not isinstance(raw_discovery, Mapping) or not isinstance(raw_sampling, Mapping):
            raise EventValidationError("Video phases must be objects")
        phase_keys = frozenset({"outcome", "payload"})
        _exact_keys(raw_discovery, phase_keys, "Video Discovery phase")
        _exact_keys(raw_sampling, phase_keys, "Video Recent Sampling phase")
        discovery_outcome = _required_text(
            raw_discovery["outcome"], "discovery.outcome"
        )
        sampling_outcome = _required_text(
            raw_sampling["outcome"], "recent_sampling.outcome"
        )
        if discovery_outcome not in {"complete", "partial"}:
            raise EventValidationError("Video Discovery outcome must be complete or partial")
        if sampling_outcome not in OUTCOMES | {"skipped"}:
            raise EventValidationError("invalid Video Recent Sampling outcome")
        if sampling_outcome == "skipped" and discovery_outcome != "partial":
            raise EventValidationError(
                "Skipped Video Recent Sampling requires partial Discovery"
            )
        discovery_payload = raw_discovery["payload"]
        sampling_payload = raw_sampling["payload"]
        if not isinstance(discovery_payload, Mapping) or not isinstance(
            sampling_payload, Mapping
        ):
            raise EventValidationError("Video phase payloads must be objects")
        discovery = VideoDiscoveryPayload.from_mapping(
            discovery_payload, outcome=discovery_outcome
        )
        recent_sampling = (
            VideoRecentSamplingSkippedPayload.from_mapping(sampling_payload)
            if sampling_outcome == "skipped"
            else VideoRecentSamplingPayload.from_mapping(
                sampling_payload, outcome=sampling_outcome
            )
        )
        activity_evidence_present = "activity_evidence" in source
        raw_activity_evidence = source.get("activity_evidence")
        if activity_evidence_present and raw_activity_evidence is None:
            raise EventValidationError(
                "Video activity_evidence must be an object when supplied"
            )
        if activity_evidence_present and not isinstance(raw_activity_evidence, Mapping):
            raise EventValidationError("Video activity_evidence must be an object")
        activity_evidence = (
            VideoActivityEvidencePayload.from_mapping(raw_activity_evidence)
            if activity_evidence_present and isinstance(raw_activity_evidence, Mapping)
            else None
        )
        raw_activity = source.get("activity")
        if raw_activity is not None and not isinstance(raw_activity, Mapping):
            raise EventValidationError("Video activity must be an object")
        activity = (
            VideoActivityPayload.from_mapping(raw_activity)
            if raw_activity is not None
            else None
        )
        expected_outcome = (
            "complete"
            if discovery_outcome == "complete" and sampling_outcome == "complete"
            else "partial"
        )
        if outcome != expected_outcome:
            raise EventValidationError("Video outcome disagrees with phase outcomes")
        return cls(
            discovery_outcome=discovery_outcome,
            discovery=discovery,
            recent_sampling_outcome=sampling_outcome,
            recent_sampling=recent_sampling,
            activity_evidence=activity_evidence,
            activity=activity,
        )

    def as_facts(self) -> dict[str, Any]:
        facts = {
            "discovery": {
                "outcome": self.discovery_outcome,
                "payload": self.discovery.as_facts(),
            },
            "recent_sampling": {
                "outcome": self.recent_sampling_outcome,
                "payload": self.recent_sampling.as_facts(),
            },
        }
        if self.activity_evidence is not None:
            facts["activity_evidence"] = self.activity_evidence.as_facts()
        if self.activity is not None:
            facts["activity"] = self.activity.as_facts()
        return facts


@dataclass(frozen=True, slots=True)
class AgentPayload:
    output_hash: str | None = None
    category_level_1: str | None = None
    category_level_2: tuple[str, ...] = ()
    tag_count: int = 0
    evidence_count: int = 0
    active_subscriber_ratio: int | None = None
    fulfilled_plan_count: int | None = None
    failed_plan_count: int | None = None
    topic_tokens: tuple[str, ...] | None = None
    evidence_fingerprints: tuple[str, ...] | None = None
    agent_version_hash: str | None = None
    input_content_count: int | None = None
    input_content_hash: str | None = None

    @classmethod
    def from_mapping(cls, source: Mapping[str, Any], *, outcome: str) -> AgentPayload:
        if outcome == "failed":
            _exact_keys(source, frozenset({"failed_plan_count"}), "failed Agent payload")
            return cls(
                failed_plan_count=_integer(
                    source["failed_plan_count"], "failed_plan_count", minimum=1
                )
            )
        if outcome != "complete":
            raise EventValidationError("partial Agent observations are not part of event version 1")
        required = frozenset(
            {
                "output_hash",
                "category_level_1",
                "category_level_2",
                "tag_count",
                "evidence_count",
                "active_subscriber_ratio",
                "fulfilled_plan_count",
            }
        )
        extended = frozenset(
            {"topic_tokens", "evidence_fingerprints", "agent_version_hash"}
        )
        input_evidence = frozenset({"input_content_count", "input_content_hash"})
        _required_optional_keys(
            source,
            required=required,
            optional=extended | input_evidence,
            label="Agent payload",
        )
        extended_present = extended & set(source)
        if extended_present and extended_present != extended:
            raise EventValidationError("Agent extended signals must be supplied together")
        input_evidence_present = input_evidence & set(source)
        if input_evidence_present and input_evidence_present != input_evidence:
            raise EventValidationError("Agent input content evidence must be supplied together")
        ratio = source["active_subscriber_ratio"]
        if ratio is not None:
            ratio = _integer(ratio, "active_subscriber_ratio", minimum=0)
            if ratio > 100:
                raise EventValidationError("active_subscriber_ratio must be at most 100")
        topic_tokens = (
            _string_list(source["topic_tokens"], "topic_tokens")
            if extended_present
            else None
        )
        evidence_fingerprints = (
            _string_list(source["evidence_fingerprints"], "evidence_fingerprints")
            if extended_present
            else None
        )
        if topic_tokens is not None and len(topic_tokens) > 128:
            raise EventValidationError("topic_tokens cannot contain more than 128 values")
        if evidence_fingerprints is not None:
            if len(evidence_fingerprints) > 256:
                raise EventValidationError(
                    "evidence_fingerprints cannot contain more than 256 values"
                )
            evidence_fingerprints = tuple(
                _sha256(value, "evidence_fingerprints[]")
                for value in evidence_fingerprints
            )
        evidence_count = _integer(source["evidence_count"], "evidence_count", minimum=0)
        if evidence_fingerprints is not None and len(evidence_fingerprints) > evidence_count:
            raise EventValidationError("evidence fingerprints cannot exceed evidence_count")
        return cls(
            output_hash=_sha256(source["output_hash"], "output_hash"),
            category_level_1=_optional_text(source["category_level_1"], "category_level_1"),
            category_level_2=_string_list(source["category_level_2"], "category_level_2"),
            tag_count=_integer(source["tag_count"], "tag_count", minimum=0),
            evidence_count=evidence_count,
            active_subscriber_ratio=ratio,
            fulfilled_plan_count=_integer(
                source["fulfilled_plan_count"], "fulfilled_plan_count", minimum=1
            ),
            topic_tokens=topic_tokens,
            evidence_fingerprints=evidence_fingerprints,
            agent_version_hash=(
                _sha256(source["agent_version_hash"], "agent_version_hash")
                if extended_present
                else None
            ),
            input_content_count=(
                _integer(source["input_content_count"], "input_content_count", minimum=0)
                if input_evidence_present
                else None
            ),
            input_content_hash=(
                _sha256(source["input_content_hash"], "input_content_hash")
                if input_evidence_present
                else None
            ),
        )

    def as_facts(self) -> dict[str, Any]:
        if self.failed_plan_count is not None:
            return {"failed_plan_count": self.failed_plan_count}
        facts = {
            "output_hash": self.output_hash,
            "category_level_1": self.category_level_1,
            "category_level_2": list(self.category_level_2),
            "tag_count": self.tag_count,
            "evidence_count": self.evidence_count,
            "active_subscriber_ratio": self.active_subscriber_ratio,
            "fulfilled_plan_count": self.fulfilled_plan_count,
        }
        if self.topic_tokens is not None:
            facts.update(
                {
                    "topic_tokens": list(self.topic_tokens),
                    "evidence_fingerprints": list(self.evidence_fingerprints or ()),
                    "agent_version_hash": self.agent_version_hash,
                }
            )
        if self.input_content_count is not None:
            facts.update(
                {
                    "input_content_count": self.input_content_count,
                    "input_content_hash": self.input_content_hash,
                }
            )
        return facts


ObservationPayload: TypeAlias = (
    AboutPayload
    | VideoPayload
    | AgentPayload
    | FailedDomainPayload
)


@dataclass(frozen=True, slots=True)
class CrawlerObservationRecorded:
    event_id: str
    observation_id: str
    plan_id: str | None
    channel_id: str
    observation_kind: str
    kind_sequence: int
    observed_at: datetime
    outcome: str
    event_version: int
    payload_hash: str
    crawler_version: str | None
    payload: ObservationPayload

    @classmethod
    def from_mapping(cls, source: Mapping[str, Any]) -> CrawlerObservationRecorded:
        try:
            validate_crawler_observation_contract(source)
        except ContractValidationError as error:
            raise EventValidationError(str(error)) from error
        _required_optional_keys(
            source,
            required=EVENT_REQUIRED_KEYS,
            optional=EVENT_OPTIONAL_KEYS,
            label="Crawler event",
        )
        if source.get("event_type") != "crawler.observation.recorded":
            raise EventValidationError("unsupported event_type")
        if source.get("event_version") != 1:
            raise EventValidationError("unsupported event_version")
        observation_kind = _required_text(source.get("observation_kind"), "observation_kind")
        if observation_kind not in OBSERVATION_KINDS:
            raise EventValidationError(f"unsupported observation_kind: {observation_kind}")
        outcome = _required_text(source.get("outcome"), "outcome")
        if outcome not in OUTCOMES:
            raise EventValidationError(f"invalid outcome: {outcome}")
        sequence = _integer(source.get("kind_sequence"), "kind_sequence", minimum=1)
        raw_payload = source.get("payload")
        if not isinstance(raw_payload, Mapping):
            raise EventValidationError("payload must be an object")

        if observation_kind == "about":
            if outcome == "failed" and "failure_kind" in raw_payload:
                payload = FailedDomainPayload.from_mapping(raw_payload)
                expected_hash = canonical_payload_hash(raw_payload)
            else:
                payload = AboutPayload.from_mapping(raw_payload)
                assert isinstance(payload, AboutPayload)
                if outcome == "complete" and payload.resolved_metric_count != 3:
                    raise EventValidationError("complete About events require all three metrics")
                if outcome == "partial" and payload.resolved_metric_count == 3:
                    raise EventValidationError("partial About events cannot resolve all three metrics")
                if outcome == "failed" and payload.resolved_metric_count != 0:
                    raise EventValidationError("failed About events cannot contain resolved metrics")
                expected_hash = payload.facts_hash()
        else:
            expected_hash = canonical_payload_hash(raw_payload)
            if outcome == "failed" and observation_kind == "video":
                payload = FailedDomainPayload.from_mapping(raw_payload)
            elif observation_kind == "video":
                payload = VideoPayload.from_mapping(raw_payload, outcome=outcome)
            else:
                payload = AgentPayload.from_mapping(raw_payload, outcome=outcome)

        payload_hash = _sha256(source.get("payload_hash"), "payload_hash")
        if payload_hash != expected_hash:
            raise EventValidationError("payload_hash does not match payload")
        return cls(
            event_id=_uuid(source.get("event_id"), "event_id"),
            observation_id=_uuid(source.get("observation_id"), "observation_id"),
            plan_id=(
                _uuid(source.get("plan_id"), "plan_id")
                if source.get("plan_id") is not None
                else None
            ),
            channel_id=_required_text(source.get("channel_id"), "channel_id"),
            observation_kind=observation_kind,
            kind_sequence=sequence,
            observed_at=_timestamp(source.get("observed_at"), "observed_at"),
            outcome=outcome,
            event_version=1,
            payload_hash=payload_hash,
            crawler_version=_optional_text(source.get("crawler_version"), "crawler_version"),
            payload=payload,
        )

    def as_pending_payload(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": "crawler.observation.recorded",
            "event_version": self.event_version,
            "observation_id": self.observation_id,
            "plan_id": self.plan_id,
            "channel_id": self.channel_id,
            "observation_kind": self.observation_kind,
            "kind_sequence": self.kind_sequence,
            "observed_at": self.observed_at.isoformat().replace("+00:00", "Z"),
            "outcome": self.outcome,
            "crawler_version": self.crawler_version,
            "payload_hash": self.payload_hash,
            "payload": self.payload.as_facts(),
        }
