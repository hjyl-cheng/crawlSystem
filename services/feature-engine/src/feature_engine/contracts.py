from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Any, Literal, Mapping, Self, TypeAlias
from uuid import UUID

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)


class ContractValidationError(ValueError):
    pass


def _uuid_text(value: str) -> str:
    try:
        UUID(value)
    except (TypeError, ValueError) as error:
        raise ValueError("must be a UUID") from error
    return value


def _timestamp_text(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError("must include a timezone")
    return value


def _date_text(value: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise ValueError("must be an ISO-8601 date") from error
    if parsed.isoformat() != value:
        raise ValueError("must be an ISO-8601 date")
    return value


NonEmptyText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]
UuidText = Annotated[NonEmptyText, AfterValidator(_uuid_text)]
TimestampText = Annotated[NonEmptyText, AfterValidator(_timestamp_text)]
DateText = Annotated[NonEmptyText, AfterValidator(_date_text)]
Sha256Text = Annotated[
    str,
    StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$"),
]
NonNegativeInt = Annotated[int, Field(ge=0)]
PositiveInt = Annotated[int, Field(gt=0)]
UnitNumber = Annotated[int | float, Field(ge=0.0, le=1.0)]
PositiveUnitNumber = Annotated[int | float, Field(gt=0.0, le=1.0)]
NonNegativeNumber = Annotated[int | float, Field(ge=0.0)]

MetricStatus: TypeAlias = Literal["exact", "estimated", "unavailable", "unresolved"]
Outcome: TypeAlias = Literal["complete", "partial", "failed"]
V16_LEGACY_ALLOWED_DAYS = (1, 3, 7, 14, 30, 60, 90, 180, 365)
V16_ALLOWED_DAYS = (1, 2, 3, 5, 7, 14, 30, 60, 90, 180, 365)
VIDEO_DISPOSITION_KINDS = frozenset({"stored", "deferred", "terminal_excluded"})
VIDEO_DISPOSITION_LEDGER_FIELDS = frozenset(
    {
        "discovered_count",
        "silent_drop_count",
        "silent_drop_video_ids",
        "dispositions",
        "recheck_dispositions",
        "stored_count",
        "deferred_count",
        "terminal_excluded_count",
        "unresolved_count",
        "unresolved_video_ids",
        "recheck_deferred_video_ids",
        "recheck_deferred_count",
        "pending_deferred_video_ids",
        "pending_deferred_count",
        "blocking_deferred_video_ids",
        "recheck_stored_count",
        "recheck_terminal_excluded_count",
    }
)
VIDEO_DISPOSITION_LEDGER_TRIGGER_FIELDS = VIDEO_DISPOSITION_LEDGER_FIELDS - {
    "unresolved_count",
    "unresolved_video_ids",
}


class _ContractModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        frozen=True,
        allow_inf_nan=False,
    )


class _AboutPayloadContract(_ContractModel):
    subscriber_count: NonNegativeInt | None
    subscriber_count_status: MetricStatus
    total_view_count: NonNegativeInt | None
    total_view_count_status: MetricStatus
    total_video_count: NonNegativeInt | None
    total_video_count_status: MetricStatus


class _FailedDomainPayloadContract(_ContractModel):
    failure_kind: NonEmptyText
    attempt_count: PositiveInt
    removed_reason: NonEmptyText | None = None

    @model_validator(mode="after")
    def validate_removed_reason(self) -> Self:
        if self.failure_kind == "channel_removed" and self.removed_reason is None:
            raise ValueError("channel_removed requires removed_reason")
        if self.failure_kind != "channel_removed" and self.removed_reason is not None:
            raise ValueError("removed_reason requires channel_removed")
        return self


class _FirstSeenVideoContract(_ContractModel):
    video_id: NonEmptyText
    position: PositiveInt
    content_type: Literal["video", "short", "live"]
    published_at: TimestampText | None
    published_at_precision: Literal["second", "date_only", "unknown"]


class _VideoDispositionEvidenceContract(_ContractModel):
    video_id: NonEmptyText
    kind: Literal["stored", "deferred", "terminal_excluded"]
    reason_code: NonEmptyText
    retry_class: NonEmptyText | None

    @model_validator(mode="after")
    def validate_retry_class(self) -> Self:
        if self.kind == "stored" and self.retry_class is not None:
            raise ValueError("stored disposition cannot contain retry_class")
        if self.kind != "stored" and self.retry_class is None:
            raise ValueError(f"{self.kind} disposition requires retry_class")
        return self


class _VideoGapAbandonmentContract(_ContractModel):
    policy_version: Literal["latest-30-on-catchup-limit-v1"]
    source_stop_reason: Literal["catchup_limit"]
    scanned_item_count: PositiveInt
    first_page_item_count: NonNegativeInt
    catch_up_item_count: PositiveInt
    catch_up_item_limit: PositiveInt
    selected_item_count: Literal[30]
    scanned_video_ids: list[NonEmptyText]
    selected_video_ids: list[NonEmptyText]
    abandoned_anchor_ids: list[NonEmptyText]

    @model_validator(mode="after")
    def validate_latest_30_prefix(self) -> Self:
        if len(set(self.scanned_video_ids)) != len(self.scanned_video_ids):
            raise ValueError("gap_abandonment scanned_video_ids cannot contain duplicates")
        if len(set(self.selected_video_ids)) != len(self.selected_video_ids):
            raise ValueError("gap_abandonment selected_video_ids cannot contain duplicates")
        if len(set(self.abandoned_anchor_ids)) != len(self.abandoned_anchor_ids):
            raise ValueError("gap_abandonment abandoned_anchor_ids cannot contain duplicates")
        if len(self.scanned_video_ids) != self.scanned_item_count:
            raise ValueError("gap_abandonment scanned_item_count must match scanned_video_ids")
        if self.first_page_item_count + self.catch_up_item_count != self.scanned_item_count:
            raise ValueError("gap_abandonment scan counts must match scanned_item_count")
        if self.catch_up_item_count != self.catch_up_item_limit:
            raise ValueError("gap_abandonment must reach the configured Catch-up limit")
        if len(self.selected_video_ids) != self.selected_item_count:
            raise ValueError("gap_abandonment selected_item_count must match selected_video_ids")
        if self.selected_video_ids != self.scanned_video_ids[: self.selected_item_count]:
            raise ValueError("gap_abandonment selected_video_ids must be the scanned prefix")
        return self


class _VideoDiscoveryPayloadContract(_ContractModel):
    pages: NonNegativeInt | None
    items: NonNegativeInt
    anchor_matched: bool
    stop_reason: Literal[
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
    ]
    parse_gap_count: NonNegativeInt
    first_seen: list[_FirstSeenVideoContract]
    first_seen_count: NonNegativeInt
    detail_success_count: NonNegativeInt
    detail_failure_count: NonNegativeInt
    unresolved_count: NonNegativeInt | None = None
    unresolved_video_ids: list[NonEmptyText] | None = None
    discovered_count: NonNegativeInt | None = None
    silent_drop_count: NonNegativeInt | None = None
    silent_drop_video_ids: list[NonEmptyText] | None = None
    dispositions: list[_VideoDispositionEvidenceContract] | None = None
    recheck_dispositions: list[_VideoDispositionEvidenceContract] | None = None
    stored_count: NonNegativeInt | None = None
    deferred_count: NonNegativeInt | None = None
    terminal_excluded_count: NonNegativeInt | None = None
    recheck_deferred_video_ids: list[NonEmptyText] | None = None
    recheck_deferred_count: NonNegativeInt | None = None
    pending_deferred_video_ids: list[NonEmptyText] | None = None
    pending_deferred_count: NonNegativeInt | None = None
    blocking_deferred_video_ids: list[NonEmptyText] | None = None
    recheck_stored_count: NonNegativeInt | None = None
    recheck_terminal_excluded_count: NonNegativeInt | None = None
    inspected_count: NonNegativeInt | None = None
    requested_limit: NonNegativeInt | None = None
    content_max_age_days: NonNegativeInt | None = None
    scan_policy_version: NonEmptyText | None = None
    terminal_condition: Literal[
        "qualified_item_limit",
        "age_boundary_crossed",
        "list_end",
        "candidate_limit_processed",
    ] | None = None
    qualified_count: NonNegativeInt | None = None
    excluded_count: NonNegativeInt | None = None
    age_boundary_crossed: bool | None = None
    first_page_item_count: NonNegativeInt | None = None
    catch_up_item_count: NonNegativeInt | None = None
    unclosed_video_ids: list[NonEmptyText] | None = None
    gap_abandonment: _VideoGapAbandonmentContract | None = None

    @model_validator(mode="after")
    def validate_publication_scan_proof(self) -> Self:
        proof_fields = {
            "inspected_count",
            "requested_limit",
            "content_max_age_days",
            "scan_policy_version",
            "terminal_condition",
            "qualified_count",
            "excluded_count",
            "age_boundary_crossed",
        }
        supplied = proof_fields & self.model_fields_set
        if supplied and supplied != proof_fields:
            raise ValueError("Publication scan proof fields must be supplied together")
        unresolved_fields = {"unresolved_count", "unresolved_video_ids"}
        supplied_unresolved = unresolved_fields & self.model_fields_set
        if supplied_unresolved and supplied_unresolved != unresolved_fields:
            raise ValueError("Unresolved Video evidence fields must be supplied together")
        unresolved_count = self.unresolved_count or 0
        unresolved_video_ids = self.unresolved_video_ids or []
        if len(set(unresolved_video_ids)) != len(unresolved_video_ids):
            raise ValueError("unresolved_video_ids cannot contain duplicates")
        if unresolved_count != len(unresolved_video_ids):
            raise ValueError("unresolved_count must match unresolved_video_ids")
        supplied_ledger = VIDEO_DISPOSITION_LEDGER_TRIGGER_FIELDS & self.model_fields_set
        if supplied_ledger:
            supplied_ledger_fields = VIDEO_DISPOSITION_LEDGER_FIELDS & self.model_fields_set
            if supplied_ledger_fields != VIDEO_DISPOSITION_LEDGER_FIELDS:
                raise ValueError("Video disposition ledger fields must be supplied together")
            missing_values = [
                field
                for field in VIDEO_DISPOSITION_LEDGER_FIELDS
                if getattr(self, field) is None
            ]
            if missing_values:
                raise ValueError(
                    "Video disposition ledger fields cannot be null: "
                    + ", ".join(sorted(missing_values))
                )
            self._validate_disposition_ledger()
        incomplete_scan_fields = {
            "first_page_item_count",
            "catch_up_item_count",
            "unclosed_video_ids",
        }
        supplied_incomplete_scan = incomplete_scan_fields & self.model_fields_set
        if supplied_incomplete_scan and supplied_incomplete_scan != incomplete_scan_fields:
            raise ValueError("Incomplete scan evidence fields must be supplied together")
        if self.pages is None and not supplied:
            raise ValueError("pages may be null only with Publication scan proof")
        if self.first_seen_count != len(self.first_seen):
            raise ValueError("first_seen_count must match first_seen entries")
        if not supplied_ledger and self.first_seen_count > self.items:
            raise ValueError("first_seen_count cannot exceed items")
        if (
            not supplied_ledger
            and self.first_seen_count + unresolved_count > self.items
        ):
            raise ValueError("resolved and unresolved first-seen counts cannot exceed items")
        detail_count = self.detail_success_count + self.detail_failure_count
        if (
            not supplied
            and not supplied_ledger
            and detail_count != self.first_seen_count + unresolved_count
        ):
            raise ValueError("Discovery first_seen/detail counts disagree")
        if self.stop_reason in {
            "qualified_item_limit",
            "age_boundary_crossed",
            "candidate_limit_processed",
        } and not supplied:
            raise ValueError("Publication completion reason requires scan proof")
        if self.stop_reason == "catchup_limit" and not supplied_incomplete_scan:
            raise ValueError("catchup_limit requires incomplete scan evidence")
        if supplied_incomplete_scan:
            assert self.unclosed_video_ids is not None
            assert self.first_page_item_count is not None
            assert self.catch_up_item_count is not None
            if len(set(self.unclosed_video_ids)) != len(self.unclosed_video_ids):
                raise ValueError("unclosed_video_ids cannot contain duplicates")
            if len(self.unclosed_video_ids) != self.items:
                raise ValueError("unclosed_video_ids must match items")
            if self.first_page_item_count + self.catch_up_item_count != self.items:
                raise ValueError("Incomplete scan item counts must match items")
            if self.first_seen_count != 0 or unresolved_count != 0 or detail_count != 0:
                raise ValueError("Incomplete scan evidence cannot contain committed Video facts")
        if self.stop_reason == "catchup_limit" and self.anchor_matched:
            raise ValueError("catchup_limit cannot report an Anchor match")
        if self.stop_reason == "gap_abandoned_latest_30":
            if self.gap_abandonment is None:
                raise ValueError("gap_abandoned_latest_30 requires gap_abandonment proof")
            if self.anchor_matched:
                raise ValueError("gap_abandoned_latest_30 cannot report an Anchor match")
            if self.items != self.gap_abandonment.selected_item_count:
                raise ValueError("gap_abandoned_latest_30 items must match selected_item_count")
            selected_ids = set(self.gap_abandonment.selected_video_ids)
            recheck_stored_ids = {
                item.video_id
                for item in (self.recheck_dispositions or [])
                if item.kind == "stored"
            }
            if any(
                item.video_id not in selected_ids | recheck_stored_ids
                for item in self.first_seen
            ):
                raise ValueError("gap_abandoned_latest_30 first_seen must come from selected Videos")
        elif self.gap_abandonment is not None:
            raise ValueError("gap_abandonment proof requires gap_abandoned_latest_30")
        if not supplied:
            return self
        if self.first_seen_count > self.detail_success_count:
            raise ValueError("Publication first_seen sample cannot exceed successful details")
        if (
            self.inspected_count is not None
            and self.detail_success_count > self.inspected_count
        ):
            raise ValueError("detail_success_count cannot exceed inspected_count")
        if (
            self.inspected_count is not None
            and self.detail_failure_count > self.inspected_count
        ):
            raise ValueError("detail_failure_count cannot exceed inspected_count")
        if (
            self.qualified_count is not None
            and self.excluded_count is not None
            and self.detail_success_count != self.qualified_count + self.excluded_count
        ):
            raise ValueError("Publication detail success count disagrees with coverage")
        if self.stop_reason == "qualified_item_limit" and (
            self.terminal_condition != "qualified_item_limit"
            or self.qualified_count is None
            or self.qualified_count < 30
        ):
            raise ValueError("qualified_item_limit requires at least 30 qualified items")
        if self.stop_reason == "age_boundary_crossed" and (
            self.terminal_condition != "age_boundary_crossed"
            or self.age_boundary_crossed is not True
            or self.content_max_age_days != 90
        ):
            raise ValueError("age_boundary_crossed requires the 90-day boundary proof")
        if self.stop_reason == "candidate_limit_processed" and (
            self.terminal_condition != "candidate_limit_processed"
            or self.items != 30
            or self.requested_limit != 30
            or self.inspected_count is None
            or self.inspected_count < 30
            or self.content_max_age_days != 90
            or self.detail_failure_count != 0
        ):
            raise ValueError(
                "candidate_limit_processed requires 30 inspected candidates without Detail failures"
            )
        if self.stop_reason == "list_end" and self.terminal_condition not in {None, "list_end"}:
            raise ValueError("list_end disagrees with terminal_condition")
        return self

    def _validate_disposition_ledger(self) -> None:
        dispositions = self.dispositions or []
        recheck_dispositions = self.recheck_dispositions or []
        silent_drop_video_ids = self.silent_drop_video_ids or []
        pending_deferred_video_ids = self.pending_deferred_video_ids or []
        recheck_deferred_video_ids = self.recheck_deferred_video_ids or []
        blocking_deferred_video_ids = self.blocking_deferred_video_ids or []
        disposition_ids = [item.video_id for item in dispositions]
        recheck_ids = [item.video_id for item in recheck_dispositions]
        unique_lists = {
            "dispositions video_id": disposition_ids,
            "recheck_dispositions video_id": recheck_ids,
            "silent_drop_video_ids": silent_drop_video_ids,
            "pending_deferred_video_ids": pending_deferred_video_ids,
            "recheck_deferred_video_ids": recheck_deferred_video_ids,
            "blocking_deferred_video_ids": blocking_deferred_video_ids,
        }
        for label, values in unique_lists.items():
            if len(values) != len(set(values)):
                raise ValueError(f"{label} cannot contain duplicates")
        if set(disposition_ids) & set(recheck_ids):
            raise ValueError("dispositions and recheck_dispositions cannot overlap")
        if set(silent_drop_video_ids) & set(disposition_ids):
            raise ValueError("silent_drop_video_ids cannot have a disposition")
        if self.silent_drop_count != len(silent_drop_video_ids):
            raise ValueError("silent_drop_count must match silent_drop_video_ids")
        if self.discovered_count != len(dispositions) + len(silent_drop_video_ids):
            raise ValueError("discovered_count must match dispositions and silent drops")

        disposition_kinds = [item.kind for item in dispositions]
        recheck_kinds = [item.kind for item in recheck_dispositions]
        expected_counts = {
            "stored_count": disposition_kinds.count("stored"),
            "deferred_count": disposition_kinds.count("deferred"),
            "terminal_excluded_count": disposition_kinds.count("terminal_excluded"),
            "recheck_stored_count": recheck_kinds.count("stored"),
            "recheck_deferred_count": recheck_kinds.count("deferred"),
            "recheck_terminal_excluded_count": recheck_kinds.count("terminal_excluded"),
        }
        for field, expected in expected_counts.items():
            if getattr(self, field) != expected:
                raise ValueError(f"{field} must match disposition entries")

        recheck_deferred_ids = {
            item.video_id for item in recheck_dispositions if item.kind == "deferred"
        }
        if set(recheck_deferred_video_ids) != recheck_deferred_ids:
            raise ValueError(
                "recheck_deferred_video_ids must match deferred recheck dispositions"
            )
        if self.pending_deferred_count != len(pending_deferred_video_ids):
            raise ValueError(
                "pending_deferred_count must match pending_deferred_video_ids"
            )
        if self.recheck_deferred_count != len(recheck_deferred_video_ids):
            raise ValueError(
                "recheck_deferred_count must match recheck_deferred_video_ids"
            )
        newly_deferred_ids = {
            item.video_id for item in dispositions if item.kind == "deferred"
        }
        expected_unresolved_ids = newly_deferred_ids | set(pending_deferred_video_ids)
        if set(self.unresolved_video_ids or []) != expected_unresolved_ids:
            raise ValueError(
                "unresolved_video_ids must match new and pending deferred Videos"
            )
        required_blocking_ids = expected_unresolved_ids | recheck_deferred_ids
        if not set(blocking_deferred_video_ids).issuperset(required_blocking_ids):
            raise ValueError(
                "blocking_deferred_video_ids must include all deferred Videos"
            )

        stored_ids = {
            item.video_id
            for item in (*dispositions, *recheck_dispositions)
            if item.kind == "stored"
        }
        first_seen_ids = [item.video_id for item in self.first_seen]
        if len(first_seen_ids) != len(set(first_seen_ids)):
            raise ValueError("first_seen video_id values must be unique")
        if set(first_seen_ids) != stored_ids:
            raise ValueError("first_seen Videos must match stored dispositions")
        if self.first_seen_count != (self.stored_count or 0) + (self.recheck_stored_count or 0):
            raise ValueError("first_seen_count must match stored disposition counts")
        detail_count = self.detail_success_count + self.detail_failure_count
        if self.first_seen_count > self.detail_success_count:
            raise ValueError("stored Videos require successful Detail evidence")
        if detail_count > len(dispositions) + len(recheck_dispositions):
            raise ValueError("Detail counts cannot exceed disposition entries")


class _VideoRecentSamplingPayloadContract(_ContractModel):
    recent_count: NonNegativeInt
    stale_ratio: UnitNumber
    selected_count: NonNegativeInt
    success_count: NonNegativeInt
    failure_count: NonNegativeInt
    next_count: NonNegativeInt
    comparable_view_count: NonNegativeInt
    view_changed_count: NonNegativeInt
    view_delta_total: int
    engagement_changed_count: NonNegativeInt


class _VideoDiscoveryPhaseContract(_ContractModel):
    outcome: Literal["complete", "partial"]
    payload: _VideoDiscoveryPayloadContract


class _VideoRecentSamplingCollectedPhaseContract(_ContractModel):
    outcome: Outcome
    payload: _VideoRecentSamplingPayloadContract


class _VideoRecentSamplingSkippedPayloadContract(_ContractModel):
    skipped_reason: Literal["discovery_incomplete"]


class _VideoRecentSamplingSkippedPhaseContract(_ContractModel):
    outcome: Literal["skipped"]
    payload: _VideoRecentSamplingSkippedPayloadContract


_VideoRecentSamplingPhaseContract: TypeAlias = Annotated[
    _VideoRecentSamplingCollectedPhaseContract
    | _VideoRecentSamplingSkippedPhaseContract,
    Field(discriminator="outcome"),
]


class _VideoActivityContract(_ContractModel):
    window_days: Literal[90]
    recent_published_content_count: NonNegativeInt
    lifecycle_status: Literal["active", "dormant"]
    dormant_reason: Literal["no_published_content_within_90_days"] | None
    dormant_since: TimestampText | None
    dormant_recheck_day: DateText | None
    dormant_cycle: NonNegativeInt

    @model_validator(mode="after")
    def validate_lifecycle_fields(self) -> Self:
        dormant_fields = (
            self.dormant_reason,
            self.dormant_since,
            self.dormant_recheck_day,
        )
        if self.lifecycle_status == "dormant":
            if any(value is None for value in dormant_fields) or self.dormant_cycle <= 0:
                raise ValueError("dormant activity requires reason, since, recheck day, and cycle")
            if self.recent_published_content_count != 0:
                raise ValueError("dormant activity cannot contain recent published content")
        elif any(value is not None for value in dormant_fields) or self.dormant_cycle != 0:
            raise ValueError("active activity cannot contain dormant lifecycle fields")
        return self


class _VideoPayloadContract(_ContractModel):
    discovery: _VideoDiscoveryPhaseContract
    recent_sampling: _VideoRecentSamplingPhaseContract
    activity: _VideoActivityContract | None = None

    @model_validator(mode="after")
    def validate_skipped_sampling(self) -> Self:
        if (
            self.discovery.outcome == "complete"
            and (self.discovery.payload.unresolved_count or 0) != 0
        ):
            raise ValueError("Complete Discovery cannot contain unresolved Videos")
        if (
            self.discovery.outcome == "complete"
            and self.discovery.payload.blocking_deferred_video_ids
        ):
            raise ValueError("Complete Discovery cannot contain blocking deferred Videos")
        if (
            isinstance(self.recent_sampling, _VideoRecentSamplingSkippedPhaseContract)
            and self.discovery.outcome != "partial"
        ):
            raise ValueError("Skipped Recent Sampling requires partial Discovery")
        return self


class _AgentCompletePayloadContract(_ContractModel):
    output_hash: Sha256Text
    category_level_1: NonEmptyText | None
    category_level_2: list[NonEmptyText]
    tag_count: NonNegativeInt
    evidence_count: NonNegativeInt
    active_subscriber_ratio: Annotated[int, Field(ge=0, le=100)] | None
    fulfilled_plan_count: PositiveInt
    topic_tokens: Annotated[list[NonEmptyText], Field(max_length=128)] | None = None
    evidence_fingerprints: Annotated[list[Sha256Text], Field(max_length=256)] | None = None
    agent_version_hash: Sha256Text | None = None
    input_content_count: NonNegativeInt | None = None
    input_content_hash: Sha256Text | None = None

    @model_validator(mode="after")
    def validate_extended_signals(self) -> Self:
        signals = (
            self.topic_tokens,
            self.evidence_fingerprints,
            self.agent_version_hash,
        )
        if any(value is not None for value in signals) and not all(
            value is not None for value in signals
        ):
            raise ValueError("Agent extended signals must be supplied together")
        if self.topic_tokens is not None and len(set(self.topic_tokens)) != len(self.topic_tokens):
            raise ValueError("topic_tokens cannot contain duplicates")
        if self.evidence_fingerprints is not None:
            if len(set(self.evidence_fingerprints)) != len(self.evidence_fingerprints):
                raise ValueError("evidence_fingerprints cannot contain duplicates")
            if len(self.evidence_fingerprints) > self.evidence_count:
                raise ValueError("evidence fingerprints cannot exceed evidence_count")
        input_fields = {"input_content_count", "input_content_hash"}
        supplied_input_fields = input_fields & self.model_fields_set
        if supplied_input_fields and supplied_input_fields != input_fields:
            raise ValueError("Agent input content evidence must be supplied together")
        return self


class _AgentFailedPayloadContract(_ContractModel):
    failed_plan_count: PositiveInt


class _ObservationEnvelopeContract(_ContractModel):
    event_id: UuidText
    event_type: Literal["crawler.observation.recorded"]
    event_version: Literal[1]
    observation_id: UuidText
    plan_id: UuidText | None = None
    channel_id: NonEmptyText
    kind_sequence: PositiveInt
    observed_at: TimestampText
    outcome: Outcome
    crawler_version: NonEmptyText | None
    payload_hash: Sha256Text


class _AboutObservationContract(_ObservationEnvelopeContract):
    observation_kind: Literal["about"]
    payload: _AboutPayloadContract | _FailedDomainPayloadContract


class _VideoObservationContract(_ObservationEnvelopeContract):
    observation_kind: Literal["video"]
    payload: _VideoPayloadContract | _FailedDomainPayloadContract


class _AgentObservationContract(_ObservationEnvelopeContract):
    observation_kind: Literal["agent"]
    payload: _AgentCompletePayloadContract | _AgentFailedPayloadContract


CrawlerObservationContract: TypeAlias = Annotated[
    _AboutObservationContract
    | _VideoObservationContract
    | _AgentObservationContract,
    Field(discriminator="observation_kind"),
]
_CRAWLER_OBSERVATION_ADAPTER = TypeAdapter(CrawlerObservationContract)


class AboutPolicyContract(_ContractModel):
    velocity_ewma_alpha: PositiveUnitNumber
    baseline_interval_days: PositiveInt
    neutral_growth_percentile: UnitNumber
    stable_min_days_for_long_interval: PositiveInt
    video_delta_full_scale: PositiveInt
    cold_start_priority_floor: UnitNumber = 0.0
    cold_start_min_recent_video_count: NonNegativeInt = 0
    cold_start_max_publish_interval_days: NonNegativeNumber = 0.0
    cold_start_max_publish_age_days: NonNegativeNumber = 0.0
    cold_start_tier_one_max_publish_interval_days: NonNegativeNumber = 1.5
    cold_start_min_reliable_intervals: NonNegativeInt = 0
    cold_start_min_subscriber_count: NonNegativeInt = 0
    cold_start_min_subscriber_percentile: UnitNumber = 0.0
    cold_start_min_feature_confidence: UnitNumber = 0.0
    dynamic_baseline_enabled: bool = False
    cadence_baseline_enabled: bool = False

    @model_validator(mode="after")
    def validate_cold_start_gate(self) -> Self:
        if self.cold_start_priority_floor > 0 and (
            self.cold_start_max_publish_interval_days <= 0
            or self.cold_start_max_publish_age_days <= 0
            or self.cold_start_min_reliable_intervals <= 0
        ):
            raise ValueError("enabled About cold-start gate requires positive activity limits")
        if self.cadence_baseline_enabled and not self.dynamic_baseline_enabled:
            raise ValueError("cadence_baseline_enabled requires dynamic_baseline_enabled")
        if not 0 < self.cold_start_tier_one_max_publish_interval_days <= 2.5:
            raise ValueError(
                "cold_start_tier_one_max_publish_interval_days must be "
                "greater than 0 and no greater than 2.5"
            )
        return self


class DiscoveryPolicyContract(_ContractModel):
    fallback_interval_days: PositiveInt
    interval_ewma_alpha: PositiveUnitNumber
    regularity_threshold: UnitNumber
    silence_decay: NonNegativeNumber
    automatic_min_interval_days: PositiveInt = 1


class RecentSamplingPolicyContract(_ContractModel):
    fallback_interval_days: PositiveInt
    change_ewma_alpha: PositiveUnitNumber


class AgentPolicyContract(_ContractModel):
    baseline_interval_days: PositiveInt
    bootstrap_min_days: PositiveInt
    bootstrap_max_days: PositiveInt
    high_priority_cap_days: PositiveInt
    version_change_interval_days: PositiveInt = 14
    dynamic_baseline_enabled: bool = False

    @model_validator(mode="after")
    def validate_bootstrap_range(self) -> Self:
        if self.bootstrap_min_days > self.bootstrap_max_days:
            raise ValueError("bootstrap_min_days cannot exceed bootstrap_max_days")
        return self


class PartialRetryPolicyContract(_ContractModel):
    about_days: PositiveInt
    discovery_days: PositiveInt
    recent_sampling_days: PositiveInt
    agent_days: PositiveInt


class ActivePolicyContract(BaseModel):
    model_config = ConfigDict(
        extra="ignore",
        strict=True,
        frozen=True,
        allow_inf_nan=False,
    )

    policy_version: NonEmptyText
    allowed_days: tuple[PositiveInt, ...]
    about_config: AboutPolicyContract
    discovery_config: DiscoveryPolicyContract
    recent_sampling_config: RecentSamplingPolicyContract
    agent_config: AgentPolicyContract
    partial_retry_config: PartialRetryPolicyContract

    @model_validator(mode="before")
    @classmethod
    def reject_profile_clock_configuration(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        if "profile_config" in value:
            raise ValueError("profile_config is not valid in the three-Clock policy")
        retry = value.get("partial_retry_config")
        if isinstance(retry, Mapping) and "profile_days" in retry:
            raise ValueError(
                "partial_retry.profile_days is not valid in the three-Clock policy"
            )
        return value

    @field_validator("allowed_days", mode="before")
    @classmethod
    def normalize_allowed_days(cls, value: Any) -> Any:
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def validate_allowed_days(self) -> Self:
        if not self.allowed_days:
            raise ValueError("allowed_days cannot be empty")
        if tuple(sorted(set(self.allowed_days))) != self.allowed_days:
            raise ValueError("allowed_days must be strictly increasing and unique")
        valid_tier_sets = (V16_LEGACY_ALLOWED_DAYS, V16_ALLOWED_DAYS)
        if self.allowed_days not in valid_tier_sets:
            raise ValueError(
                "allowed_days must equal one of the supported V16 tiers: "
                f"{V16_LEGACY_ALLOWED_DAYS} or {V16_ALLOWED_DAYS}"
            )
        if (
            self.about_config.cadence_baseline_enabled
            and self.allowed_days != V16_ALLOWED_DAYS
        ):
            raise ValueError(
                f"cadence About policy requires the V16 tiers {V16_ALLOWED_DAYS}"
            )
        configured_tiers = {
            "about.baseline_interval_days": self.about_config.baseline_interval_days,
            "discovery.fallback_interval_days": self.discovery_config.fallback_interval_days,
            "discovery.automatic_min_interval_days": (
                self.discovery_config.automatic_min_interval_days
            ),
            "recent_sampling.fallback_interval_days": (
                self.recent_sampling_config.fallback_interval_days
            ),
            "agent.baseline_interval_days": self.agent_config.baseline_interval_days,
            "agent.high_priority_cap_days": self.agent_config.high_priority_cap_days,
            "agent.version_change_interval_days": (
                self.agent_config.version_change_interval_days
            ),
            "partial_retry.about_days": self.partial_retry_config.about_days,
            "partial_retry.discovery_days": self.partial_retry_config.discovery_days,
            "partial_retry.recent_sampling_days": (
                self.partial_retry_config.recent_sampling_days
            ),
            "partial_retry.agent_days": self.partial_retry_config.agent_days,
        }
        invalid = [name for name, value in configured_tiers.items() if value not in self.allowed_days]
        if invalid:
            raise ValueError(f"configured Clock tiers are not allowed: {', '.join(invalid)}")
        if self.discovery_config.fallback_interval_days > 90:
            raise ValueError("discovery.fallback_interval_days cannot exceed 90")
        if not any(
            self.agent_config.bootstrap_min_days
            <= tier
            <= self.agent_config.bootstrap_max_days
            for tier in self.allowed_days
        ):
            raise ValueError("agent bootstrap range contains no allowed tier")
        return self


def _format_validation_error(error: ValidationError) -> str:
    messages: list[str] = []
    for issue in error.errors(
        include_url=False,
        include_context=False,
        include_input=False,
    ):
        location = ".".join(str(part) for part in issue["loc"])
        message = "unexpected field" if issue["type"] == "extra_forbidden" else issue["msg"]
        messages.append(f"{location}: {message}" if location else message)
    return "; ".join(messages)


def validate_crawler_observation_contract(source: Mapping[str, Any]) -> None:
    if not isinstance(source, Mapping):
        raise ContractValidationError("Crawler event must be an object")
    try:
        _CRAWLER_OBSERVATION_ADAPTER.validate_python(dict(source), strict=True)
    except ValidationError as error:
        raise ContractValidationError(_format_validation_error(error)) from error


def crawler_observation_json_schema() -> dict[str, Any]:
    return _CRAWLER_OBSERVATION_ADAPTER.json_schema()


def validate_active_policy_contract(source: Mapping[str, Any]) -> ActivePolicyContract:
    if not isinstance(source, Mapping):
        raise ContractValidationError("Active policy must be an object")
    try:
        return ActivePolicyContract.model_validate(dict(source), strict=True)
    except ValidationError as error:
        raise ContractValidationError(_format_validation_error(error)) from error


def main() -> None:
    import json

    print(json.dumps(crawler_observation_json_schema(), ensure_ascii=False, indent=2))
