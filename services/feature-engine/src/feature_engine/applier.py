from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from hashlib import sha256
import json
from typing import Any, Callable, Mapping
from uuid import uuid4

from .clock_window import clock_due_at_for_day
from .contracts import (
    ActivePolicyContract,
    ContractValidationError,
    validate_active_policy_contract,
)
from .events import (
    AboutPayload,
    CrawlerObservationRecorded,
    FailedDomainPayload,
    VideoPayload,
)
from .policy import (
    decide_about_due,
    decide_agent_due,
    decide_video_due,
    limit_about_slowdown,
    recent_publish_active,
    runtime_policy_configs,
)
from .plan_status import logical_plan_outcomes, reduce_daily_plan_status
from .reference_data import load_collection_priority_signals, load_reference_catalog
from .shared_features import (
    SharedFeatureInputs,
    derive_recent_change_probability,
    derive_shared_features,
)
from .state import (
    ChannelFeatureState,
    apply_about_event,
    apply_about_stability_evidence,
    apply_agent_event,
    apply_video_event,
)
from .utc import as_utc, utc_day


class EventConflict(RuntimeError):
    pass


class FeatureStateInvariantError(RuntimeError):
    pass


def _plan_requests_observation(plan: Mapping[str, Any], observation_kind: str) -> bool:
    if observation_kind not in {"about", "video", "agent"}:
        return False
    return bool(plan.get(f"run_{observation_kind}"))


def _logical_plan_outcomes(
    plan: Mapping[str, Any], outcomes: Mapping[str, str]
) -> dict[str, str]:
    return logical_plan_outcomes(plan, outcomes)


@dataclass(frozen=True, slots=True)
class ApplyObservationResult:
    event_id: str
    observation_id: str
    status: str
    duplicate: bool
    last_applied_sequence: int
    drained_event_ids: tuple[str, ...] = ()


FEATURE_STATE_COLUMNS = (
    "last_subscriber_count",
    "last_subscriber_observed_at",
    "last_total_view_count",
    "last_total_view_observed_at",
    "last_total_video_count",
    "last_total_video_observed_at",
    "last_about_observed_at",
    "about_metric_confidence",
    "subscriber_velocity_ewma",
    "view_velocity_ewma",
    "video_count_delta",
    "subscriber_size_percentile",
    "subscriber_growth_percentile",
    "view_growth_percentile",
    "growth_momentum",
    "about_stable_since",
    "about_stable_runs",
    "recent_publish_interval_days",
    "publish_interval_ewma",
    "publish_interval_median",
    "publish_interval_mad",
    "publish_regularity",
    "last_publish_at",
    "recent30_video_count",
    "new_video_empty_runs",
    "last_discovery_observed_at",
    "last_complete_discovery_at",
    "recent_stale_ratio",
    "recent_view_change_ewma",
    "recent_engagement_change_ewma",
    "recent_upload_change_ewma",
    "recent_change_probability",
    "recent_sampling_stable_runs",
    "last_recent_sampling_at",
    "last_recent_sample_count",
    "current_topic_vector",
    "current_topic_tokens",
    "current_agent_output_hash",
    "current_agent_evidence_fingerprints",
    "current_agent_version_hash",
    "last_agent_evidence_count",
    "topic_drift",
    "evidence_replacement",
    "recent_content_shift",
    "agent_version_changed",
    "agent_output_changed",
    "agent_change_score",
    "agent_topic_vector_source",
    "agent_confidence",
    "agent_stable_runs",
    "last_agent_observed_at",
    "user_query_demand",
    "data_incompleteness",
    "manual_priority",
    "collection_priority",
    "channel_activity",
    "feature_confidence",
    "fallback_reason_codes",
    "reference_distribution_version",
    "state_version",
)
ARRAY_STATE_COLUMNS = frozenset(
    {
        "recent_publish_interval_days",
        "current_topic_vector",
        "current_topic_tokens",
        "current_agent_evidence_fingerprints",
        "fallback_reason_codes",
    }
)
JSON_STATE_COLUMNS: frozenset[str] = frozenset()
POLICY_CONFIG_FIELDS = (
    "about_config",
    "discovery_config",
    "recent_sampling_config",
    "agent_config",
    "partial_retry_config",
)


def _column_name(description: Any) -> str:
    return description.name if hasattr(description, "name") else description[0]


def _row(cursor: Any) -> dict[str, Any] | None:
    value = cursor.fetchone()
    if value is None:
        return None
    if isinstance(value, Mapping):
        return dict(value)
    return {
        _column_name(description): item
        for description, item in zip(cursor.description, value, strict=True)
    }


def _rows(cursor: Any) -> list[dict[str, Any]]:
    values = cursor.fetchall()
    columns = [_column_name(description) for description in cursor.description]
    return [
        dict(value) if isinstance(value, Mapping) else dict(zip(columns, value, strict=True))
        for value in values
    ]


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _pick_tier(channel_id: str, label: str, choices: tuple[int, ...]) -> int:
    digest = sha256(f"{channel_id}:{label}".encode()).digest()
    return choices[int.from_bytes(digest[:8], "big") % len(choices)]


def _dispatch_slot(channel_id: str, slots: int = 1024) -> int:
    digest = sha256(f"{channel_id}:dispatch".encode()).digest()
    return int.from_bytes(digest[:8], "big") % slots


def _clock_kind(observation_kind: str) -> str:
    return observation_kind


class FeatureObservationApplier:
    """Apply one Crawler event and every newly unblocked successor atomically."""

    def __init__(self, connection_factory: Callable[[], Any]) -> None:
        self._connection_factory = connection_factory

    def apply_crawler_observation(
        self, event: CrawlerObservationRecorded | Mapping[str, Any]
    ) -> ApplyObservationResult:
        parsed = (
            event
            if isinstance(event, CrawlerObservationRecorded)
            else CrawlerObservationRecorded.from_mapping(event)
        )
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    self._lock_channel(cursor, parsed.channel_id)
                    duplicate = self._claim_event(cursor, parsed)
                    if duplicate is not None:
                        return duplicate
                    checkpoint = self._lock_checkpoint(cursor, parsed)
                    last_sequence = int(checkpoint["last_applied_sequence"])
                    if parsed.kind_sequence <= last_sequence:
                        self._reject_stale(cursor, parsed)
                        return ApplyObservationResult(
                            event_id=parsed.event_id,
                            observation_id=parsed.observation_id,
                            status="rejected",
                            duplicate=False,
                            last_applied_sequence=last_sequence,
                        )
                    if parsed.kind_sequence > last_sequence + 1:
                        self._mark_waiting_gap(cursor, parsed)
                        return ApplyObservationResult(
                            event_id=parsed.event_id,
                            observation_id=parsed.observation_id,
                            status="waiting_gap",
                            duplicate=False,
                            last_applied_sequence=last_sequence,
                        )

                    self._apply_next(cursor, parsed)
                    drained: list[str] = []
                    last_sequence = parsed.kind_sequence
                    while True:
                        waiting = self._next_waiting_event(
                            cursor,
                            channel_id=parsed.channel_id,
                            observation_kind=parsed.observation_kind,
                            sequence=last_sequence + 1,
                        )
                        if waiting is None:
                            break
                        self._apply_next(cursor, waiting)
                        drained.append(waiting.event_id)
                        last_sequence = waiting.kind_sequence
                    return ApplyObservationResult(
                        event_id=parsed.event_id,
                        observation_id=parsed.observation_id,
                        status="applied",
                        duplicate=False,
                        last_applied_sequence=last_sequence,
                        drained_event_ids=tuple(drained),
                    )
        finally:
            connection.close()

    @staticmethod
    def _lock_channel(cursor: Any, channel_id: str) -> None:
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"feature-observation:{channel_id}",),
        )

    def _claim_event(
        self, cursor: Any, event: CrawlerObservationRecorded
    ) -> ApplyObservationResult | None:
        cursor.execute(
            """
            INSERT INTO feature_clock.crawler_event_inbox (
              event_id,observation_id,event_type,event_version,channel_id,
              observation_kind,kind_sequence,plan_id,observed_at,outcome,payload_hash,
              status,pending_payload_json
            ) VALUES (
              %s,%s,'crawler.observation.recorded',%s,%s,%s,%s,%s,%s,%s,%s,
              'received',%s::jsonb
            )
            ON CONFLICT DO NOTHING
            RETURNING event_id
            """,
            (
                event.event_id,
                event.observation_id,
                event.event_version,
                event.channel_id,
                event.observation_kind,
                event.kind_sequence,
                event.plan_id,
                event.observed_at,
                event.outcome,
                event.payload_hash,
                _json(event.as_pending_payload()),
            ),
        )
        if _row(cursor) is not None:
            return None

        cursor.execute(
            """
            SELECT event_id,observation_id,event_version,plan_id,channel_id,observation_kind,
                   kind_sequence,observed_at,outcome,payload_hash,status
            FROM feature_clock.crawler_event_inbox
            WHERE event_id=%s
               OR observation_id=%s
               OR (channel_id=%s AND observation_kind=%s AND kind_sequence=%s)
            FOR UPDATE
            """,
            (
                event.event_id,
                event.observation_id,
                event.channel_id,
                event.observation_kind,
                event.kind_sequence,
            ),
        )
        conflicts = _rows(cursor)
        for existing in conflicts:
            if (
                str(existing["event_id"]) == event.event_id
                and str(existing["observation_id"]) == event.observation_id
                and int(existing["event_version"]) == event.event_version
                and (
                    str(existing["plan_id"]) if existing["plan_id"] is not None else None
                ) == event.plan_id
                and existing["channel_id"] == event.channel_id
                and existing["observation_kind"] == event.observation_kind
                and int(existing["kind_sequence"]) == event.kind_sequence
                and existing["observed_at"] == event.observed_at
                and existing["outcome"] == event.outcome
                and existing["payload_hash"] == event.payload_hash
            ):
                checkpoint = self._checkpoint(cursor, event.channel_id, event.observation_kind)
                return ApplyObservationResult(
                    event_id=event.event_id,
                    observation_id=event.observation_id,
                    status=existing["status"],
                    duplicate=True,
                    last_applied_sequence=int(
                        checkpoint["last_applied_sequence"] if checkpoint else 0
                    ),
                )
        raise EventConflict(
            f"event, observation, or sequence identity was reused with different facts: {event.event_id}"
        )

    def _checkpoint(
        self, cursor: Any, channel_id: str, observation_kind: str
    ) -> dict[str, Any] | None:
        cursor.execute(
            """
            SELECT *
            FROM feature_clock.channel_observation_checkpoints
            WHERE channel_id=%s AND observation_kind=%s
            """,
            (channel_id, observation_kind),
        )
        return _row(cursor)

    def _lock_checkpoint(
        self, cursor: Any, event: CrawlerObservationRecorded
    ) -> dict[str, Any]:
        cursor.execute(
            """
            INSERT INTO feature_clock.channel_observation_checkpoints (
              channel_id,observation_kind
            ) VALUES (%s,%s)
            ON CONFLICT (channel_id,observation_kind) DO NOTHING
            """,
            (event.channel_id, event.observation_kind),
        )
        cursor.execute(
            """
            SELECT *
            FROM feature_clock.channel_observation_checkpoints
            WHERE channel_id=%s AND observation_kind=%s
            FOR UPDATE
            """,
            (event.channel_id, event.observation_kind),
        )
        checkpoint = _row(cursor)
        if checkpoint is None:
            raise FeatureStateInvariantError("checkpoint row was not created")
        return checkpoint

    def _reject_stale(self, cursor: Any, event: CrawlerObservationRecorded) -> None:
        cursor.execute(
            """
            UPDATE feature_clock.crawler_event_inbox
            SET status='rejected',pending_payload_json=NULL,
                error_code='stale_sequence',
                error_message='sequence was already applied'
            WHERE event_id=%s
            """,
            (event.event_id,),
        )

    def _mark_waiting_gap(self, cursor: Any, event: CrawlerObservationRecorded) -> None:
        cursor.execute(
            """
            UPDATE feature_clock.crawler_event_inbox
            SET status='waiting_gap',error_code='sequence_gap',
                error_message='waiting for an earlier sequence'
            WHERE event_id=%s
            """,
            (event.event_id,),
        )

    def _next_waiting_event(
        self,
        cursor: Any,
        *,
        channel_id: str,
        observation_kind: str,
        sequence: int,
    ) -> CrawlerObservationRecorded | None:
        cursor.execute(
            """
            SELECT pending_payload_json
            FROM feature_clock.crawler_event_inbox
            WHERE channel_id=%s AND observation_kind=%s
              AND kind_sequence=%s AND status='waiting_gap'
            FOR UPDATE
            """,
            (channel_id, observation_kind, sequence),
        )
        value = _row(cursor)
        if value is None:
            return None
        payload = value["pending_payload_json"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return CrawlerObservationRecorded.from_mapping(payload)

    def _active_policy(self, cursor: Any) -> ActivePolicyContract:
        cursor.execute(
            """
            SELECT *
            FROM feature_clock.rule_policy_definitions
            WHERE status='active' AND effective_from <= now()
            ORDER BY effective_from DESC
            LIMIT 1
            FOR SHARE
            """
        )
        policy = _row(cursor)
        if policy is None:
            raise FeatureStateInvariantError("no active rule policy is effective for the event")
        try:
            for field in POLICY_CONFIG_FIELDS:
                if isinstance(policy.get(field), str):
                    policy[field] = json.loads(policy[field])
            return validate_active_policy_contract(policy)
        except (ContractValidationError, json.JSONDecodeError, TypeError) as error:
            raise FeatureStateInvariantError("active rule policy configuration is invalid") from error

    def _load_feature_state(self, cursor: Any, channel_id: str) -> ChannelFeatureState:
        cursor.execute(
            """
            SELECT *
            FROM feature_clock.channel_feature_state
            WHERE channel_id=%s
            FOR UPDATE
            """,
            (channel_id,),
        )
        row = _row(cursor)
        if row is None:
            return ChannelFeatureState()
        values: dict[str, Any] = {}
        for column in FEATURE_STATE_COLUMNS:
            value = row.get(column)
            if column in JSON_STATE_COLUMNS:
                if isinstance(value, str):
                    value = json.loads(value)
                value = dict(value or {})
            elif column in ARRAY_STATE_COLUMNS:
                value = tuple(value or ())
            values[column] = value
        return ChannelFeatureState(**values)

    def _save_feature_state(
        self, cursor: Any, channel_id: str, state: ChannelFeatureState
    ) -> None:
        columns = ",".join(FEATURE_STATE_COLUMNS)
        placeholders = ",".join(
            "%s::jsonb" if column in JSON_STATE_COLUMNS else "%s"
            for column in FEATURE_STATE_COLUMNS
        )
        assignments = ",".join(
            f"{column}=EXCLUDED.{column}" for column in FEATURE_STATE_COLUMNS
        )
        values: list[Any] = []
        for column in FEATURE_STATE_COLUMNS:
            value = getattr(state, column)
            if column in JSON_STATE_COLUMNS:
                value = _json(value or {})
            elif column in ARRAY_STATE_COLUMNS:
                value = list(value or ())
            values.append(value)
        cursor.execute(
            f"""
            INSERT INTO feature_clock.channel_feature_state (
              channel_id,{columns},updated_at
            ) VALUES (%s,{placeholders},now())
            ON CONFLICT (channel_id) DO UPDATE
            SET {assignments},updated_at=now()
            """,
            (channel_id, *values),
        )

    def _enrich_shared_state(
        self,
        cursor: Any,
        *,
        channel_id: str,
        state: ChannelFeatureState,
        observed_at: Any,
    ) -> ChannelFeatureState:
        references = load_reference_catalog(
            cursor,
            as_of_day=utc_day(observed_at, "observed_at"),
        )
        signals = load_collection_priority_signals(
            cursor,
            channel_id=channel_id,
            observed_at=observed_at,
        )
        shared = derive_shared_features(
            SharedFeatureInputs(
                subscriber_count=state.last_subscriber_count,
                subscriber_velocity_ewma=state.subscriber_velocity_ewma,
                view_velocity_ewma=state.view_velocity_ewma,
                recent30_video_count=state.recent30_video_count,
                last_publish_at=state.last_publish_at,
                about_identity_observed=state.last_about_observed_at is not None,
                about_observed=state.last_about_observed_at is not None,
                discovery_observed=state.last_discovery_observed_at is not None,
                recent_sampling_observed=state.last_recent_sampling_at is not None,
                agent_observed=state.last_agent_observed_at is not None,
            ),
            references=references,
            signals=signals,
            observed_at=observed_at,
        )
        fallback_reasons = list(state.fallback_reason_codes)
        if references.version is None:
            fallback_reasons.append("reference_distribution_unavailable")
        return replace(
            state,
            subscriber_size_percentile=shared.subscriber_size_percentile,
            subscriber_growth_percentile=shared.subscriber_growth_percentile,
            view_growth_percentile=shared.view_growth_percentile,
            growth_momentum=shared.growth_momentum,
            user_query_demand=shared.user_query_demand,
            data_incompleteness=shared.data_incompleteness,
            manual_priority=shared.manual_priority,
            collection_priority=shared.collection_priority,
            channel_activity=shared.channel_activity,
            recent_change_probability=derive_recent_change_probability(
                view_change=state.recent_view_change_ewma,
                engagement_change=state.recent_engagement_change_ewma,
                upload_change=state.recent_upload_change_ewma,
                channel_activity=shared.channel_activity,
            ),
            fallback_reason_codes=tuple(dict.fromkeys(fallback_reasons)),
            reference_distribution_version=shared.reference_distribution_version,
        )

    def _load_clock(self, cursor: Any, channel_id: str) -> dict[str, Any] | None:
        cursor.execute(
            "SELECT * FROM feature_clock.channel_clock_state WHERE channel_id=%s FOR UPDATE",
            (channel_id,),
        )
        return _row(cursor)

    def _apply_next(self, cursor: Any, event: CrawlerObservationRecorded) -> None:
        self._validate_plan_event(cursor, event)
        if (
            event.outcome == "failed"
            and isinstance(event.payload, FailedDomainPayload)
            and event.payload.failure_kind == "channel_removed"
        ):
            self._apply_removed_channel(cursor, event)
        elif event.outcome != "failed":
            self._apply_business_state(cursor, event)
        cursor.execute(
            """
            UPDATE feature_clock.channel_observation_checkpoints
            SET last_applied_sequence=%s,last_observation_id=%s,
                last_observed_at=%s,last_payload_hash=%s,updated_at=now()
            WHERE channel_id=%s AND observation_kind=%s
            """,
            (
                event.kind_sequence,
                event.observation_id,
                event.observed_at,
                event.payload_hash,
                event.channel_id,
                event.observation_kind,
            ),
        )
        cursor.execute(
            """
            UPDATE feature_clock.crawler_event_inbox
            SET status='applied',pending_payload_json=NULL,applied_at=now(),
                error_code=NULL,error_message=NULL
            WHERE event_id=%s
            """,
            (event.event_id,),
        )
        self._update_plan_status(cursor, event)

    @staticmethod
    def _apply_removed_channel(cursor: Any, event: CrawlerObservationRecorded) -> None:
        payload = event.payload
        if not isinstance(payload, FailedDomainPayload) or payload.removed_reason is None:
            raise FeatureStateInvariantError("removed Channel event lacks terminal evidence")
        cursor.execute(
            """
            UPDATE feature_clock.channel_clock_state
            SET lifecycle_status='removed',
                removed_reason=COALESCE(removed_reason,%s),
                removed_at=COALESCE(removed_at,%s),
                removed_source_event_id=COALESCE(removed_source_event_id,%s),
                clock_version=CASE
                  WHEN lifecycle_status='active' THEN clock_version+1
                  ELSE clock_version
                END,
                updated_at=now()
            WHERE channel_id=%s
            """,
            (
                payload.removed_reason,
                event.observed_at,
                event.event_id,
                event.channel_id,
            ),
        )
        cursor.execute(
            """
            UPDATE feature_clock.daily_channel_plans
            SET status='cancelled',error_code='channel_removed',
                lease_owner=NULL,lease_expires_at=NULL,
                finished_at=COALESCE(finished_at,now()),
                completed_at=COALESCE(completed_at,now()),updated_at=now()
            WHERE channel_id=%s
              AND status IN ('planned','dispatching','dispatched','running')
              AND (%s::uuid IS NULL OR plan_id<>%s::uuid)
            """,
            (event.channel_id, event.plan_id, event.plan_id),
        )

    def _validate_plan_event(self, cursor: Any, event: CrawlerObservationRecorded) -> None:
        if event.plan_id is None:
            return
        cursor.execute(
            """
            SELECT channel_id,run_about,run_video,run_agent,status
            FROM feature_clock.daily_channel_plans
            WHERE plan_id=%s
            FOR UPDATE
            """,
            (event.plan_id,),
        )
        plan = _row(cursor)
        if plan is None:
            raise EventConflict(f"unknown plan_id: {event.plan_id}")
        if plan["channel_id"] != event.channel_id:
            raise EventConflict("Plan and Observation channel_id differ")
        if not _plan_requests_observation(plan, event.observation_kind):
            raise EventConflict(
                f"Plan {event.plan_id} did not request {event.observation_kind}"
            )

    def _update_plan_status(self, cursor: Any, event: CrawlerObservationRecorded) -> None:
        if event.plan_id is None:
            return
        cursor.execute(
            """
            SELECT run_about,run_video,run_agent,status
            FROM feature_clock.daily_channel_plans
            WHERE plan_id=%s
            FOR UPDATE
            """,
            (event.plan_id,),
        )
        plan = _row(cursor)
        if plan is None:
            raise EventConflict(f"unknown plan_id: {event.plan_id}")
        if plan["status"] in {"succeeded", "cancelled"}:
            return

        cursor.execute(
            """
            SELECT DISTINCT ON (observation_kind) observation_kind,outcome
            FROM feature_clock.crawler_event_inbox
            WHERE plan_id=%s AND status='applied'
            ORDER BY observation_kind,kind_sequence DESC
            """,
            (event.plan_id,),
        )
        outcomes = {row["observation_kind"]: row["outcome"] for row in _rows(cursor)}
        decision = reduce_daily_plan_status(plan, outcomes)
        status = decision.status
        error_code = decision.error_code
        cursor.execute(
            """
            UPDATE feature_clock.daily_channel_plans
            SET status=%s,error_code=%s,
                started_at=COALESCE(started_at,%s),
                finished_at=CASE
                  WHEN %s IN ('succeeded','partial','failed')
                    THEN GREATEST(COALESCE(finished_at,%s),%s)
                  ELSE finished_at
                END,
                completed_at=CASE
                  WHEN %s IN ('succeeded','partial','failed') THEN now()
                  ELSE NULL
                END,
                lease_owner=CASE
                  WHEN %s IN ('succeeded','partial','failed') THEN NULL
                  ELSE lease_owner
                END,
                lease_expires_at=CASE
                  WHEN %s IN ('succeeded','partial','failed') THEN NULL
                  ELSE lease_expires_at
                END,
                updated_at=now()
            WHERE plan_id=%s
              AND status IN (
                'planned','dispatching','dispatched','running','partial','failed'
              )
            """,
            (
                status,error_code,event.observed_at,status,event.observed_at,
                event.observed_at,
                status,status,status,event.plan_id,
            ),
        )

    def _apply_business_state(self, cursor: Any, event: CrawlerObservationRecorded) -> None:
        policy = self._active_policy(cursor)
        configs = runtime_policy_configs(policy)
        previous_state = self._load_feature_state(cursor, event.channel_id)

        if event.observation_kind == "about":
            config_data = policy.about_config
            transition = apply_about_event(
                previous_state,
                event,
                velocity_alpha=float(config_data.velocity_ewma_alpha),
            )
            assert isinstance(event.payload, AboutPayload)
            should_decide = True
            if transition.business_state_changed:
                transition = replace(
                    transition,
                    state=self._enrich_shared_state(
                        cursor,
                        channel_id=event.channel_id,
                        state=transition.state,
                        observed_at=event.observed_at,
                    ),
                )
                if configs.about.cadence_baseline_enabled:
                    transition = replace(
                        transition,
                        state=apply_about_stability_evidence(
                            previous_state,
                            transition.state,
                            observed_at=event.observed_at,
                            outcome=event.outcome,
                            baseline=transition.baseline,
                        ),
                    )
            decision = decide_about_due(
                transition.state,
                observed_at=event.observed_at,
                outcome=event.outcome,
                baseline=transition.baseline,
                config=configs.about,
            ) if should_decide else None
        elif event.observation_kind == "video":
            transition = apply_video_event(
                previous_state,
                event,
                interval_alpha=float(policy.discovery_config.interval_ewma_alpha),
                change_alpha=float(policy.recent_sampling_config.change_ewma_alpha),
            )
            should_decide = True
            if transition.business_state_changed:
                transition = replace(
                    transition,
                    state=self._enrich_shared_state(
                        cursor,
                        channel_id=event.channel_id,
                        state=transition.state,
                        observed_at=event.observed_at,
                    ),
                )
            decision = decide_video_due(
                transition.state,
                observed_at=event.observed_at,
                discovery_outcome=transition.discovery_outcome,
                recent_sampling_outcome=transition.recent_sampling_outcome,
                discovery_baseline=transition.discovery_baseline,
                recent_sampling_baseline=transition.recent_sampling_baseline,
                discovery_config=configs.discovery,
                recent_sampling_config=configs.recent_sampling,
            ) if should_decide else None
        else:
            transition = apply_agent_event(previous_state, event)
            should_decide = True
            if transition.business_state_changed:
                transition = replace(
                    transition,
                    state=self._enrich_shared_state(
                        cursor,
                        channel_id=event.channel_id,
                        state=transition.state,
                        observed_at=event.observed_at,
                    ),
                )
            decision = decide_agent_due(
                transition.state,
                observed_at=event.observed_at,
                outcome=event.outcome,
                baseline=transition.baseline,
                output_changed=transition.output_changed,
                evidence_count=transition.evidence_count,
                channel_id=event.channel_id,
                config=configs.agent,
            ) if should_decide else None

        if not should_decide or decision is None:
            return
        self._save_feature_state(cursor, event.channel_id, transition.state)
        channel_clock = self._load_clock(cursor, event.channel_id)
        if channel_clock is None:
            self._bootstrap_clocks(
                cursor,
                event=event,
                state=transition.state,
                decision=decision,
                policy=policy,
            )
            self._apply_video_activity_lifecycle(cursor, event)
            return
        if (
            event.observation_kind == "about"
            and not transition.baseline
            and configs.about.cadence_baseline_enabled
        ):
            decision = limit_about_slowdown(
                decision,
                previous_tier_days=int(channel_clock["about_tier"]),
            )
        baseline_recalculations = self._recalculate_first_cross_domain_clocks(
            cursor,
            state=transition.state,
            channel_clock=channel_clock,
            event=event,
            configs=configs,
        )
        self._update_clocks(
            cursor,
            event=event,
            state=transition.state,
            decision=decision,
            channel_clock=channel_clock,
            policy_version=policy.policy_version,
            automatic_video_min_interval_days=(
                configs.discovery.automatic_min_interval_days
            ),
            baseline_recalculations=baseline_recalculations,
        )
        self._apply_video_activity_lifecycle(cursor, event)

    @staticmethod
    def _apply_video_activity_lifecycle(
        cursor: Any, event: CrawlerObservationRecorded
    ) -> None:
        if event.observation_kind != "video" or not isinstance(event.payload, VideoPayload):
            return
        activity = event.payload.activity
        if activity is None:
            return
        if activity.lifecycle_status == "dormant":
            cursor.execute(
                """
                UPDATE feature_clock.channel_clock_state
                SET lifecycle_status='dormant',
                    dormant_reason=%s,dormant_since=%s,dormant_recheck_day=%s::date,
                    dormant_cycle=%s,dormant_source_event_id=%s,
                    clock_version=clock_version+1,updated_at=now()
                WHERE channel_id=%s AND lifecycle_status<>'removed'
                """,
                (
                    activity.dormant_reason,
                    activity.dormant_since,
                    activity.dormant_recheck_day,
                    activity.dormant_cycle,
                    event.event_id,
                    event.channel_id,
                ),
            )
            if cursor.rowcount != 1:
                raise FeatureStateInvariantError(
                    "dormant lifecycle transition could not update the Channel Clock"
                )
            cursor.execute(
                """
                UPDATE feature_clock.daily_channel_plans
                SET status='cancelled',error_code='channel_dormant',
                    lease_owner=NULL,lease_expires_at=NULL,
                    finished_at=COALESCE(finished_at,now()),
                    completed_at=COALESCE(completed_at,now()),updated_at=now()
                WHERE channel_id=%s
                  AND status IN ('planned','dispatching','dispatched','running')
                """,
                (event.channel_id,),
            )
            return

        cursor.execute(
            """
            UPDATE feature_clock.channel_clock_state
            SET lifecycle_status='active',
                dormant_reason=NULL,dormant_since=NULL,dormant_recheck_day=NULL,
                dormant_cycle=0,dormant_source_event_id=NULL,
                clock_version=clock_version+1,updated_at=now()
            WHERE channel_id=%s AND lifecycle_status<>'removed'
            """,
            (event.channel_id,),
        )
        if cursor.rowcount != 1:
            raise FeatureStateInvariantError(
                "active lifecycle transition could not update the Channel Clock"
            )

    def _recalculate_first_cross_domain_clocks(
        self,
        cursor: Any,
        *,
        state: ChannelFeatureState,
        channel_clock: Mapping[str, Any],
        event: CrawlerObservationRecorded,
        configs: Any,
    ) -> dict[str, Any]:
        recalculations: dict[str, Any] = {}
        observed_at_by_kind = {
            "about": state.last_about_observed_at,
            "agent": state.last_agent_observed_at,
        }
        for kind, observed_at in observed_at_by_kind.items():
            if kind == event.observation_kind or observed_at is None:
                continue
            if (
                kind == "about"
                and not configs.about.dynamic_baseline_enabled
                and event.observation_kind != "video"
            ):
                continue
            if kind == "agent" and not configs.agent.dynamic_baseline_enabled:
                continue
            checkpoint = self._checkpoint(cursor, event.channel_id, kind)
            if checkpoint is None or int(checkpoint["last_applied_sequence"]) != 1:
                continue

            last_complete_at = channel_clock.get(f"{kind}_last_complete_at")
            outcome = (
                "complete"
                if last_complete_at is not None
                and as_utc(last_complete_at, f"{kind}_last_complete_at")
                == as_utc(observed_at, f"last_{kind}_observed_at")
                else "partial"
            )
            if kind == "about":
                recalculated = decide_about_due(
                    state,
                    observed_at=observed_at,
                    outcome=outcome,
                    baseline=True,
                    config=configs.about,
                )
            else:
                recalculated = decide_agent_due(
                    state,
                    observed_at=observed_at,
                    outcome=outcome,
                    baseline=True,
                    output_changed=state.agent_output_changed,
                    evidence_count=state.last_agent_evidence_count or 0,
                    channel_id=event.channel_id,
                    config=configs.agent,
                )
            if recalculated.due_day >= channel_clock[f"{kind}_due_day"]:
                continue

            specific_reason = f"{kind}_cold_start_recalculation"
            extra_summary: dict[str, Any] = {}
            if kind == "about" and event.observation_kind == "video":
                specific_reason = "video_activity_recalculation"
                extra_summary = {
                    "recalculated_after_video_observation": True,
                    "trigger_video_observed_at": as_utc(
                        event.observed_at, "event.observed_at"
                    ).isoformat(),
                }
            recalculations[kind] = replace(
                recalculated,
                reason_codes=tuple(
                    dict.fromkeys(
                        (
                            *recalculated.reason_codes,
                            specific_reason,
                            "cross_domain_cold_start_recalculation",
                        )
                    )
                ),
                feature_summary={
                    **recalculated.feature_summary,
                    **extra_summary,
                    "recalculated_after_observation_kind": event.observation_kind,
                    "trigger_observed_at": as_utc(
                        event.observed_at, "event.observed_at"
                    ).isoformat(),
                },
            )
        return recalculations

    def _bootstrap_clocks(
        self,
        cursor: Any,
        *,
        event: CrawlerObservationRecorded,
        state: ChannelFeatureState,
        decision: Any,
        policy: ActivePolicyContract,
    ) -> None:
        base_at = as_utc(event.observed_at, "event.observed_at")
        agent_choices = tuple(
            tier
            for tier in policy.allowed_days
            if policy.agent_config.bootstrap_min_days
            <= tier
            <= policy.agent_config.bootstrap_max_days
        )
        tiers = {
            "about": (
                policy.about_config.baseline_interval_days
                if policy.about_config.dynamic_baseline_enabled
                else _pick_tier(event.channel_id, "about-bootstrap", (1, 3, 7))
            ),
            "video": 7,
            "agent": (
                policy.agent_config.baseline_interval_days
                if policy.agent_config.dynamic_baseline_enabled
                else _pick_tier(event.channel_id, "agent-bootstrap", agent_choices)
            ),
        }
        due_day = {
            kind: base_at.date() + timedelta(days=tier)
            for kind, tier in tiers.items()
        }
        due_at = {kind: clock_due_at_for_day(value) for kind, value in due_day.items()}
        tiers[event.observation_kind] = decision.tier_days
        due_day[event.observation_kind] = decision.due_day
        due_at[event.observation_kind] = decision.due_at

        hint_pulled = False
        if (
            event.observation_kind == "about"
            and (state.video_count_delta or 0) > 0
            and recent_publish_active(state, event.observed_at)
        ):
            hint_days = int(policy.discovery_config.automatic_min_interval_days)
            hinted_due_day = base_at.date() + timedelta(days=hint_days)
            hinted_due_at = clock_due_at_for_day(hinted_due_day)
            if hinted_due_day < due_day["video"]:
                due_day["video"] = hinted_due_day
                due_at["video"] = hinted_due_at
                tiers["video"] = hint_days
                hint_pulled = True

        next_run_at = min(due_at["about"],due_at["video"],due_at["agent"])
        next_run_day = min(due_day[kind] for kind in ("about", "video", "agent"))
        cursor.execute(
            """
            INSERT INTO feature_clock.channel_clock_state (
              channel_id,about_due_at,about_due_day,about_tier,about_last_complete_at,
              video_due_at,video_due_day,video_tier,video_last_complete_at,video_last_outcome,
              agent_due_at,agent_due_day,agent_tier,agent_mode,agent_last_complete_at,
              channel_next_run_at,channel_next_run_day,dispatch_slot,estimated_request_cost,
              policy_version,feature_state_version,clock_version
            ) VALUES (
              %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
              %s,%s,%s,'basic',%s,%s,%s,%s,0,%s,%s,1
            )
            """,
            (
                event.channel_id,
                due_at["about"],
                due_day["about"],
                tiers["about"],
                event.observed_at
                if event.observation_kind == "about" and event.outcome == "complete"
                else None,
                due_at["video"],
                due_day["video"],
                tiers["video"],
                event.observed_at
                if event.observation_kind == "video" and event.outcome == "complete"
                else None,
                event.outcome if event.observation_kind == "video" else None,
                due_at["agent"],
                due_day["agent"],
                tiers["agent"],
                event.observed_at
                if event.observation_kind == "agent" and event.outcome == "complete"
                else None,
                next_run_at,
                next_run_day,
                _dispatch_slot(event.channel_id),
                policy.policy_version,
                state.state_version,
            ),
        )
        self._insert_decision(
            cursor,
            event=event,
            clock_kind=_clock_kind(event.observation_kind),
            decision_mode="bootstrap",
            previous_due_at=None,
            decided_due_at=decision.due_at,
            tier=decision.tier_days,
            reason_codes=decision.reason_codes,
            feature_state_version=state.state_version,
            feature_summary=decision.feature_summary,
            policy_version=policy.policy_version,
            reference_distribution_version=state.reference_distribution_version,
            clock_before=0,
            clock_after=1,
        )
        if hint_pulled:
            self._insert_decision(
                cursor,
                event=event,
                clock_kind="video",
                decision_mode="bootstrap",
                previous_due_at=None,
                decided_due_at=due_at["video"],
                tier=tiers["video"],
                reason_codes=(
                    "about_video_count_hint",
                    "recent_publish_active",
                ),
                feature_state_version=state.state_version,
                feature_summary={
                    "video_count_delta": state.video_count_delta,
                    "hint_only": True,
                    "hinted_due_day": hinted_due_day.isoformat(),
                    "automatic_video_min_interval_days": hint_days,
                },
                policy_version=policy.policy_version,
                reference_distribution_version=state.reference_distribution_version,
                clock_before=0,
                clock_after=1,
            )

    def _update_clocks(
        self,
        cursor: Any,
        *,
        event: CrawlerObservationRecorded,
        state: ChannelFeatureState,
        decision: Any,
        channel_clock: dict[str, Any],
        policy_version: str,
        automatic_video_min_interval_days: int,
        baseline_recalculations: Mapping[str, Any] | None = None,
    ) -> None:
        channel_before = int(channel_clock["clock_version"])
        channel_after = channel_before + 1

        about_due_at = as_utc(channel_clock["about_due_at"], "about_due_at")
        about_due_day = channel_clock["about_due_day"]
        about_tier = int(channel_clock["about_tier"])
        video_due_at = as_utc(channel_clock["video_due_at"], "video_due_at")
        video_due_day = channel_clock["video_due_day"]
        video_tier = int(channel_clock["video_tier"])
        agent_due_at = as_utc(channel_clock["agent_due_at"], "agent_due_at")
        agent_due_day = channel_clock["agent_due_day"]
        agent_tier = int(channel_clock["agent_tier"])
        previous_video_due_at = video_due_at
        previous_due_at_by_kind = {
            "about": about_due_at,
            "video": video_due_at,
            "agent": agent_due_at,
        }
        hint_pulled = False

        if event.observation_kind == "about":
            previous_due_at = about_due_at
            about_due_at, about_due_day, about_tier = (
                decision.due_at, decision.due_day, decision.tier_days
            )
            if (state.video_count_delta or 0) > 0 and recent_publish_active(
                state, event.observed_at
            ):
                hinted_due_day = (
                    as_utc(event.observed_at, "event.observed_at").date()
                    + timedelta(days=automatic_video_min_interval_days)
                )
                hinted_due_at = clock_due_at_for_day(hinted_due_day)
                if hinted_due_day < video_due_day:
                    video_due_at, video_due_day, video_tier = (
                        hinted_due_at,
                        hinted_due_day,
                        automatic_video_min_interval_days,
                    )
                    hint_pulled = True
        elif event.observation_kind == "video":
            previous_due_at = video_due_at
            video_due_at, video_due_day, video_tier = (
                decision.due_at, decision.due_day, decision.tier_days
            )
        else:
            previous_due_at = agent_due_at
            agent_due_at, agent_due_day, agent_tier = (
                decision.due_at, decision.due_day, decision.tier_days
            )

        for kind, recalculation in (baseline_recalculations or {}).items():
            if kind == "about":
                about_due_at, about_due_day, about_tier = (
                    recalculation.due_at,
                    recalculation.due_day,
                    recalculation.tier_days,
                )
            elif kind == "agent":
                agent_due_at, agent_due_day, agent_tier = (
                    recalculation.due_at,
                    recalculation.due_day,
                    recalculation.tier_days,
                )
            else:
                raise FeatureStateInvariantError(
                    f"unsupported cold-start recalculation kind: {kind}"
                )

        next_run_at = min(about_due_at, video_due_at, agent_due_at)
        next_run_day = min(about_due_day, video_due_day, agent_due_day)
        cursor.execute(
            """
            UPDATE feature_clock.channel_clock_state
            SET about_due_at=%s,about_due_day=%s,about_tier=%s,
                about_last_complete_at=CASE
                  WHEN %s='about' AND %s='complete' THEN %s ELSE about_last_complete_at END,
                video_due_at=%s,video_due_day=%s,video_tier=%s,
                video_last_complete_at=CASE
                  WHEN %s='video' AND %s='complete' THEN %s ELSE video_last_complete_at END,
                video_last_outcome=CASE
                  WHEN %s='video' THEN %s ELSE video_last_outcome END,
                agent_due_at=%s,agent_due_day=%s,agent_tier=%s,
                agent_last_complete_at=CASE
                  WHEN %s='agent' AND %s='complete' THEN %s ELSE agent_last_complete_at END,
                channel_next_run_at=%s,channel_next_run_day=%s,
                policy_version=%s,feature_state_version=%s,
                clock_version=%s,updated_at=now()
            WHERE channel_id=%s AND clock_version=%s
            """,
            (
                about_due_at,
                about_due_day,
                about_tier,
                event.observation_kind,
                event.outcome,
                event.observed_at,
                video_due_at,
                video_due_day,
                video_tier,
                event.observation_kind,
                event.outcome,
                event.observed_at,
                event.observation_kind,
                event.outcome,
                agent_due_at,
                agent_due_day,
                agent_tier,
                event.observation_kind,
                event.outcome,
                event.observed_at,
                next_run_at,
                next_run_day,
                policy_version,
                state.state_version,
                channel_after,
                event.channel_id,
                channel_before,
            ),
        )
        if cursor.rowcount != 1:
            raise FeatureStateInvariantError("channel clock optimistic update failed")

        self._insert_decision(
            cursor,
            event=event,
            clock_kind=_clock_kind(event.observation_kind),
            decision_mode="post_run",
            previous_due_at=previous_due_at,
            decided_due_at=decision.due_at,
            tier=decision.tier_days,
            reason_codes=decision.reason_codes,
            feature_state_version=state.state_version,
            feature_summary=decision.feature_summary,
            policy_version=policy_version,
            reference_distribution_version=state.reference_distribution_version,
            clock_before=channel_before,
            clock_after=channel_after,
        )
        if hint_pulled:
            self._insert_decision(
                cursor,
                event=event,
                clock_kind="video",
                decision_mode="post_run",
                previous_due_at=previous_video_due_at,
                decided_due_at=video_due_at,
                tier=video_tier,
                reason_codes=(
                    "about_video_count_hint",
                    "recent_publish_active",
                ),
                feature_state_version=state.state_version,
                feature_summary={
                    "video_count_delta": state.video_count_delta,
                    "hint_only": True,
                    "hinted_due_day": hinted_due_day.isoformat(),
                    "automatic_video_min_interval_days": (
                        automatic_video_min_interval_days
                    ),
                },
                policy_version=policy_version,
                reference_distribution_version=state.reference_distribution_version,
                clock_before=channel_before,
                clock_after=channel_after,
            )
        for kind, recalculation in (baseline_recalculations or {}).items():
            self._insert_decision(
                cursor,
                event=event,
                clock_kind=kind,
                decision_mode="post_run",
                previous_due_at=previous_due_at_by_kind[kind],
                decided_due_at=recalculation.due_at,
                tier=recalculation.tier_days,
                reason_codes=recalculation.reason_codes,
                feature_state_version=state.state_version,
                feature_summary=recalculation.feature_summary,
                policy_version=policy_version,
                reference_distribution_version=state.reference_distribution_version,
                clock_before=channel_before,
                clock_after=channel_after,
            )

    def _insert_decision(
        self,
        cursor: Any,
        *,
        event: CrawlerObservationRecorded,
        clock_kind: str,
        decision_mode: str,
        previous_due_at: datetime | None,
        decided_due_at: datetime,
        tier: int,
        reason_codes: tuple[str, ...],
        feature_state_version: int,
        feature_summary: Mapping[str, Any],
        policy_version: str,
        reference_distribution_version: str | None,
        clock_before: int,
        clock_after: int,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO feature_clock.clock_decision_log (
              decision_id,channel_id,clock_kind,trigger_event_id,trigger_observation_id,
              decision_mode,previous_due_at,previous_due_day,decided_due_at,decided_due_day,
              tier,reason_codes,
              feature_state_version,feature_summary_json,policy_version,
              reference_distribution_version,clock_version_before,clock_version_after
            ) VALUES (
              %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s
            )
            """,
            (
                str(uuid4()),
                event.channel_id,
                clock_kind,
                event.event_id,
                event.observation_id,
                decision_mode,
                previous_due_at,
                previous_due_at.date() if previous_due_at is not None else None,
                decided_due_at,
                decided_due_at.date(),
                tier,
                list(reason_codes),
                feature_state_version,
                _json(dict(feature_summary)),
                policy_version,
                reference_distribution_version,
                clock_before,
                clock_after,
            ),
        )
