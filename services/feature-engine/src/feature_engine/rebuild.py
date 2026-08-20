from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime
from hashlib import sha256
import json
from typing import Any, Callable, Mapping
from uuid import uuid4

from .applier import ARRAY_STATE_COLUMNS, FEATURE_STATE_COLUMNS, JSON_STATE_COLUMNS
from .clock_window import clock_due_at_for_day
from .contracts import (
    ActivePolicyContract,
    ContractValidationError,
    validate_active_policy_contract,
)
from .policy import ClockDecision, decide_rebuild_clocks
from .recalculation import (
    RECALCULATION_COORDINATION_LOCK_ID,
    RECALCULATION_RESUME_LOCK_ID,
    UNRESOLVED_RECALCULATION_STATUSES,
)
from .state import ChannelFeatureState


class PolicyRebuildError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class PolicyRebuildResult:
    recalculation_id: str
    policy_version: str
    status: str
    processed_channels: int
    failed_channels: int
    succeeded_shards: int
    failed_shards: int


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


def _source_version(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("source_baseline_version cannot be empty")
    return normalized


def _state_from_row(row: Mapping[str, Any]) -> ChannelFeatureState:
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


def _policy_from_row(row: Mapping[str, Any]) -> ActivePolicyContract:
    values = dict(row)
    for field in (
        "about_config",
        "discovery_config",
        "recent_sampling_config",
        "agent_config",
        "partial_retry_config",
    ):
        if isinstance(values.get(field), str):
            values[field] = json.loads(values[field])
    try:
        return validate_active_policy_contract(values)
    except (ContractValidationError, json.JSONDecodeError, TypeError) as error:
        raise PolicyRebuildError("rule policy configuration is invalid") from error


def _active_policy(cursor: Any, policy_version: str | None = None) -> ActivePolicyContract:
    if policy_version is None:
        cursor.execute(
            """
            SELECT *
            FROM feature_clock.rule_policy_definitions
            WHERE status='active' AND effective_from<=now()
            ORDER BY effective_from DESC
            LIMIT 1
            FOR SHARE
            """
        )
    else:
        cursor.execute(
            """
            SELECT *
            FROM feature_clock.rule_policy_definitions
            WHERE policy_version=%s AND status='active' AND effective_from<=now()
            FOR SHARE
            """,
            (policy_version,),
        )
    row = _row(cursor)
    if row is None:
        raise PolicyRebuildError("no requested active rule policy is effective")
    return _policy_from_row(row)


def _preserved_decision(
    *,
    policy_version: str,
    due_at: datetime,
    due_day: date,
    tier: int,
    reason: str,
    summary: Mapping[str, Any] | None = None,
) -> ClockDecision:
    return ClockDecision(
        policy_version=policy_version,
        due_at=clock_due_at_for_day(due_day),
        due_day=due_day,
        tier_days=tier,
        reason_codes=(reason,),
        feature_summary=dict(summary or {}),
    )


def _not_delayed(
    decision: ClockDecision,
    *,
    previous_due_at: datetime,
    previous_due_day: date,
    previous_tier: int,
) -> ClockDecision:
    if decision.due_day <= previous_due_day:
        return decision
    reasons = [*decision.reason_codes, "existing_due_not_delayed"]
    summary = {
        **decision.feature_summary,
        "policy_candidate_due_day": decision.due_day.isoformat(),
        "existing_due_day": previous_due_day.isoformat(),
    }
    return replace(
        decision,
        due_at=clock_due_at_for_day(previous_due_day),
        due_day=previous_due_day,
        tier_days=previous_tier,
        reason_codes=tuple(dict.fromkeys(reasons)),
        feature_summary=summary,
    )


class FeaturePolicyRebuilder:
    """Rebuild every Clock resumably from current rolling Feature State."""

    def __init__(self, connection_factory: Callable[[], Any]) -> None:
        self._connection_factory = connection_factory

    def start(
        self,
        *,
        shard_count: int = 128,
        policy_version: str | None = None,
        source_baseline_version: str | None = None,
    ) -> str:
        if shard_count <= 0:
            raise ValueError("shard_count must be positive")
        source_baseline_version = _source_version(source_baseline_version)
        recalculation_id = str(uuid4())
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        "SELECT pg_advisory_xact_lock(%s)",
                        (RECALCULATION_COORDINATION_LOCK_ID,),
                    )
                    cursor.execute(
                        """
                        SELECT recalculation_id,status
                        FROM feature_clock.recalculation_runs
                        WHERE status=ANY(%s)
                        ORDER BY created_at,recalculation_id
                        LIMIT 1
                        FOR UPDATE
                        """,
                        (list(UNRESOLVED_RECALCULATION_STATUSES),),
                    )
                    unresolved = _row(cursor)
                    if unresolved is not None:
                        raise PolicyRebuildError(
                            "unresolved Feature recalculation "
                            f"{unresolved['recalculation_id']} ({unresolved['status']}) "
                            "must be resumed or cancelled before starting another"
                        )
                    policy = _active_policy(cursor, policy_version)
                    checksum_body = {
                        "mode": "policy_rebuild",
                        "policy_version": policy.policy_version,
                        "source_baseline_version": source_baseline_version,
                        "shard_count": shard_count,
                    }
                    checksum = f"sha256:{sha256(_json(checksum_body).encode()).hexdigest()}"
                    cursor.execute(
                        """
                        INSERT INTO feature_clock.recalculation_runs (
                          recalculation_id,mode,policy_version,source_baseline_version,
                          shard_count,status,started_at,checksum
                        ) VALUES (%s,'policy_rebuild',%s,%s,%s,'running',now(),%s)
                        """,
                        (
                            recalculation_id,
                            policy.policy_version,
                            source_baseline_version,
                            shard_count,
                            checksum,
                        ),
                    )
                    cursor.executemany(
                        """
                        INSERT INTO feature_clock.recalculation_shards (
                          recalculation_id,shard_id,status
                        ) VALUES (%s,%s,'pending')
                        """,
                        ((recalculation_id, shard_id) for shard_id in range(shard_count)),
                    )
            return recalculation_id
        finally:
            connection.close()

    def fail(self, recalculation_id: str) -> None:
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        """
                        UPDATE feature_clock.recalculation_runs
                        SET status='failed',completed_at=now(),updated_at=now()
                        WHERE recalculation_id=%s AND status IN ('pending','running','partial')
                        """,
                        (recalculation_id,),
                    )
        finally:
            connection.close()

    def rebuild(
        self,
        *,
        shard_count: int = 128,
        batch_size: int = 500,
        policy_version: str | None = None,
        source_baseline_version: str | None = None,
        worker_id: str = "feature-policy-rebuild",
    ) -> PolicyRebuildResult:
        recalculation_id = self.start(
            shard_count=shard_count,
            policy_version=policy_version,
            source_baseline_version=source_baseline_version,
        )
        return self.resume(
            recalculation_id,
            batch_size=batch_size,
            source_baseline_version=source_baseline_version,
            worker_id=worker_id,
        )

    def resume(
        self,
        recalculation_id: str,
        *,
        batch_size: int = 500,
        source_baseline_version: str | None = None,
        worker_id: str = "feature-policy-rebuild",
    ) -> PolicyRebuildResult:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        source_baseline_version = _source_version(source_baseline_version)
        execution_connection = self._acquire_execution_lock()
        try:
            run = self._prepare_run(
                recalculation_id,
                source_baseline_version=source_baseline_version,
            )
            if run["status"] == "succeeded":
                return self._finish_run(recalculation_id)
            try:
                self._validate_clock_coverage()
                for shard_id in range(int(run["shard_count"])):
                    self._process_shard(
                        recalculation_id,
                        shard_id=shard_id,
                        shard_count=int(run["shard_count"]),
                        policy_version=str(run["policy_version"]),
                        source_baseline_version=(
                            str(run["source_baseline_version"])
                            if run["source_baseline_version"] is not None
                            else None
                        ),
                        batch_size=batch_size,
                        worker_id=worker_id,
                    )
            except Exception:
                self.fail(recalculation_id)
                raise
            return self._finish_run(recalculation_id)
        finally:
            execution_connection.close()

    def _acquire_execution_lock(self) -> Any:
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        "SELECT pg_try_advisory_lock(%s) AS acquired",
                        (RECALCULATION_RESUME_LOCK_ID,),
                    )
                    lock = _row(cursor)
                    if lock is None or not bool(lock["acquired"]):
                        raise PolicyRebuildError(
                            "another Feature recalculation worker is already running"
                        )
            return connection
        except Exception:
            connection.close()
            raise

    def _prepare_run(
        self,
        recalculation_id: str,
        *,
        source_baseline_version: str | None,
    ) -> dict[str, Any]:
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        """
                        SELECT * FROM feature_clock.recalculation_runs
                        WHERE recalculation_id=%s AND mode='policy_rebuild'
                        FOR UPDATE
                        """,
                        (recalculation_id,),
                    )
                    run = _row(cursor)
                    if run is None:
                        raise PolicyRebuildError("policy rebuild run does not exist")
                    if run["status"] == "cancelled":
                        raise PolicyRebuildError("cancelled policy rebuild cannot be resumed")
                    if run["status"] == "succeeded":
                        return run
                    _active_policy(cursor, str(run["policy_version"]))
                    stored_source = run["source_baseline_version"]
                    if (
                        source_baseline_version is not None
                        and stored_source is not None
                        and source_baseline_version != stored_source
                    ):
                        raise PolicyRebuildError(
                            "source_baseline_version cannot change when resuming a Run"
                        )
                    if source_baseline_version is not None and stored_source is None:
                        cursor.execute(
                            """
                            SELECT EXISTS (
                              SELECT 1
                              FROM feature_clock.recalculation_shards
                              WHERE recalculation_id=%s
                                AND (processed_rows>0 OR cursor IS NOT NULL OR status='succeeded')
                            ) AS progressed
                            """,
                            (recalculation_id,),
                        )
                        progress = _row(cursor)
                        if progress is not None and bool(progress["progressed"]):
                            raise PolicyRebuildError(
                                "source_baseline_version cannot be set after shard progress"
                            )
                    updated_source = (
                        source_baseline_version
                        if source_baseline_version is not None
                        else stored_source
                    )
                    checksum_body = {
                        "mode": "policy_rebuild",
                        "policy_version": str(run["policy_version"]),
                        "source_baseline_version": updated_source,
                        "shard_count": int(run["shard_count"]),
                    }
                    cursor.execute(
                        """
                        UPDATE feature_clock.recalculation_runs
                        SET status='running',source_baseline_version=COALESCE(%s,source_baseline_version),
                            checksum=%s,completed_at=NULL,updated_at=now()
                        WHERE recalculation_id=%s
                        """,
                        (
                            source_baseline_version,
                            f"sha256:{sha256(_json(checksum_body).encode()).hexdigest()}",
                            recalculation_id,
                        ),
                    )
                    run["status"] = "running"
                    if source_baseline_version is not None:
                        run["source_baseline_version"] = source_baseline_version
                    return run
        finally:
            connection.close()

    def _validate_clock_coverage(self) -> None:
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        """
                        SELECT count(*)::bigint AS missing_count
                        FROM feature_clock.channel_feature_state state
                        LEFT JOIN feature_clock.channel_clock_state channel_clock
                          USING (channel_id)
                        WHERE channel_clock.channel_id IS NULL
                        """
                    )
                    coverage = _row(cursor)
                    missing = int(coverage["missing_count"] if coverage is not None else 0)
                    if missing:
                        raise PolicyRebuildError(
                            f"{missing} Feature State rows do not have a Channel Clock row"
                        )
        finally:
            connection.close()

    def _process_shard(
        self,
        recalculation_id: str,
        *,
        shard_id: int,
        shard_count: int,
        policy_version: str,
        source_baseline_version: str | None,
        batch_size: int,
        worker_id: str,
    ) -> None:
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        """
                        UPDATE feature_clock.recalculation_shards
                        SET status='running',lease_owner=%s,
                            lease_expires_at=now()+interval '15 minutes',
                            started_at=COALESCE(started_at,now()),completed_at=NULL,
                            last_error=NULL,updated_at=now()
                        WHERE recalculation_id=%s AND shard_id=%s
                          AND (
                            status IN ('pending','failed')
                            OR (status='running' AND lease_expires_at<=now())
                          )
                        RETURNING cursor,status
                        """,
                        (worker_id, recalculation_id, shard_id),
                    )
                    claimed = _row(cursor)
                    if claimed is None:
                        return

            while True:
                with connection.transaction():
                    with connection.cursor() as cursor:
                        cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                        cursor.execute(
                            """
                            SELECT cursor
                            FROM feature_clock.recalculation_shards
                            WHERE recalculation_id=%s AND shard_id=%s
                              AND status='running' AND lease_owner=%s
                            FOR UPDATE
                            """,
                            (recalculation_id, shard_id, worker_id),
                        )
                        shard = _row(cursor)
                        if shard is None:
                            raise PolicyRebuildError("policy rebuild shard lease was lost")
                        last_channel_id = shard["cursor"]
                        policy = _active_policy(cursor, policy_version)
                        state_columns = ",".join(
                            f"state.{column}" for column in FEATURE_STATE_COLUMNS
                        )
                        cursor.execute(
                            f"""
                            SELECT state.channel_id,{state_columns},
                                   channel_clock.about_due_at AS clock_about_due_at,
                                   channel_clock.about_due_day AS clock_about_due_day,
                                   channel_clock.about_tier AS clock_about_tier,
                                   channel_clock.video_due_at AS clock_video_due_at,
                                   channel_clock.video_due_day AS clock_video_due_day,
                                   channel_clock.video_tier AS clock_video_tier,
                                   channel_clock.agent_due_at AS clock_agent_due_at,
                                   channel_clock.agent_due_day AS clock_agent_due_day,
                                   channel_clock.agent_tier AS clock_agent_tier,
                                   channel_clock.clock_version AS channel_clock_version
                            FROM feature_clock.channel_feature_state state
                            JOIN feature_clock.channel_clock_state channel_clock USING (channel_id)
                            WHERE ((hashtextextended(state.channel_id,0) & 9223372036854775807)
                                   %% %s)=%s
                              AND channel_clock.lifecycle_status='active'
                              AND (%s::text IS NULL OR state.channel_id>%s)
                            ORDER BY state.channel_id
                            FOR UPDATE OF state,channel_clock
                            LIMIT %s
                            """,
                            (
                                shard_count,
                                shard_id,
                                last_channel_id,
                                last_channel_id,
                                batch_size,
                            ),
                        )
                        batch = _rows(cursor)
                        if not batch:
                            checksum_body = {
                                "recalculation_id": recalculation_id,
                                "shard_id": shard_id,
                                "cursor": last_channel_id,
                            }
                            cursor.execute(
                                """
                                UPDATE feature_clock.recalculation_shards
                                SET status='succeeded',failed_rows=0,lease_owner=NULL,
                                    lease_expires_at=NULL,completed_at=now(),
                                    last_error=NULL,checksum=%s,updated_at=now()
                                WHERE recalculation_id=%s AND shard_id=%s
                                  AND status='running' AND lease_owner=%s
                                """,
                                (
                                    f"sha256:{sha256(_json(checksum_body).encode()).hexdigest()}",
                                    recalculation_id,
                                    shard_id,
                                    worker_id,
                                ),
                            )
                            if cursor.rowcount != 1:
                                raise PolicyRebuildError("policy rebuild shard lease was lost")
                            return
                        for row in batch:
                            self._rebuild_clock_row(
                                cursor,
                                row=row,
                                policy=policy,
                                audit_context={
                                    "recalculation_id": recalculation_id,
                                    "source_baseline_version": source_baseline_version,
                                },
                            )
                        last_channel_id = str(batch[-1]["channel_id"])
                        cursor.execute(
                            """
                            UPDATE feature_clock.recalculation_shards
                            SET cursor=%s,processed_rows=processed_rows+%s,failed_rows=0,
                                lease_expires_at=now()+interval '15 minutes',updated_at=now()
                            WHERE recalculation_id=%s AND shard_id=%s
                              AND status='running' AND lease_owner=%s
                            """,
                            (
                                last_channel_id,
                                len(batch),
                                recalculation_id,
                                shard_id,
                                worker_id,
                            ),
                        )
                        if cursor.rowcount != 1:
                            raise PolicyRebuildError("policy rebuild shard lease was lost")
        except Exception as error:
            self._mark_shard_failed(recalculation_id, shard_id, worker_id, error)
            return
        finally:
            connection.close()

    def _mark_shard_failed(
        self,
        recalculation_id: str,
        shard_id: int,
        worker_id: str,
        error: Exception,
    ) -> None:
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        """
                        UPDATE feature_clock.recalculation_shards
                        SET status='failed',failed_rows=1,lease_owner=NULL,
                            lease_expires_at=NULL,completed_at=now(),last_error=%s,
                            updated_at=now()
                        WHERE recalculation_id=%s AND shard_id=%s
                          AND status='running' AND lease_owner=%s
                        """,
                        (str(error)[:2000], recalculation_id, shard_id, worker_id),
                    )
        finally:
            connection.close()

    @staticmethod
    def _rebuild_clock_row(
        cursor: Any,
        *,
        row: Mapping[str, Any],
        policy: ActivePolicyContract,
        decision_mode: str = "policy_rebuild",
        audit_reason: str = "policy_rebuild",
        audit_context: Mapping[str, Any] | None = None,
    ) -> None:
        if decision_mode not in {"policy_rebuild", "repair"}:
            raise ValueError("recalculation decision_mode must be policy_rebuild or repair")
        state = _state_from_row(row)
        calculated = decide_rebuild_clocks(state, policy=policy)
        previous = {
            "about": (
                row["clock_about_due_at"],row["clock_about_due_day"],
                int(row["clock_about_tier"]),
            ),
            "video": (
                row["clock_video_due_at"],row["clock_video_due_day"],
                int(row["clock_video_tier"]),
            ),
            "agent": (
                row["clock_agent_due_at"],row["clock_agent_due_day"],
                int(row["clock_agent_tier"]),
            ),
        }
        decisions: dict[str, ClockDecision] = {}
        for kind, (previous_due_at, previous_due_day, previous_tier) in previous.items():
            candidate = calculated.get(kind)
            if candidate is None:
                candidate = _preserved_decision(
                    policy_version=policy.policy_version,
                    due_at=previous_due_at,
                    due_day=previous_due_day,
                    tier=previous_tier,
                    reason="no_reliable_feature_state_preserved",
                )
            decisions[kind] = _not_delayed(
                candidate,
                previous_due_at=previous_due_at,
                previous_due_day=previous_due_day,
                previous_tier=previous_tier,
            )

        video = decisions["video"]
        about = decisions["about"]
        agent = decisions["agent"]
        next_run_at = min(about.due_at, video.due_at, agent.due_at)
        next_run_day = min(about.due_day, video.due_day, agent.due_day)
        channel_before = int(row["channel_clock_version"])
        channel_after = channel_before + 1
        cursor.execute(
            """
            UPDATE feature_clock.channel_clock_state
            SET about_due_at=%s,about_due_day=%s,about_tier=%s,
                video_due_at=%s,video_due_day=%s,video_tier=%s,
                agent_due_at=%s,agent_due_day=%s,agent_tier=%s,
                channel_next_run_at=%s,channel_next_run_day=%s,policy_version=%s,
                feature_state_version=%s,clock_version=%s,updated_at=now()
            WHERE channel_id=%s AND clock_version=%s
            """,
            (
                about.due_at,
                about.due_day,
                about.tier_days,
                video.due_at,
                video.due_day,
                video.tier_days,
                agent.due_at,
                agent.due_day,
                agent.tier_days,
                next_run_at,
                next_run_day,
                policy.policy_version,
                state.state_version,
                channel_after,
                row["channel_id"],
                channel_before,
            ),
        )
        if cursor.rowcount != 1:
            raise PolicyRebuildError("channel clock optimistic rebuild failed")

        for kind, decision in decisions.items():
            cursor.execute(
                """
                INSERT INTO feature_clock.clock_decision_log (
                  decision_id,channel_id,clock_kind,decision_mode,
                  previous_due_at,previous_due_day,decided_due_at,decided_due_day,
                  tier,reason_codes,
                  feature_state_version,feature_summary_json,policy_version,
                  reference_distribution_version,clock_version_before,clock_version_after
                ) VALUES (
                  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s
                )
                """,
                (
                    str(uuid4()),
                    row["channel_id"],
                    kind,
                    decision_mode,
                    previous[kind][0],
                    previous[kind][1],
                    decision.due_at,
                    decision.due_day,
                    decision.tier_days,
                    list(dict.fromkeys((*decision.reason_codes, audit_reason))),
                    state.state_version,
                    _json(
                        {
                            **decision.feature_summary,
                            **({"recalculation_context": dict(audit_context)} if audit_context else {}),
                        }
                    ),
                    policy.policy_version,
                    state.reference_distribution_version,
                    channel_before,
                    channel_after,
                ),
            )

    def _finish_run(self, recalculation_id: str) -> PolicyRebuildResult:
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        """
                        SELECT policy_version,status
                        FROM feature_clock.recalculation_runs
                        WHERE recalculation_id=%s
                        FOR UPDATE
                        """,
                        (recalculation_id,),
                    )
                    run = _row(cursor)
                    if run is None:
                        raise PolicyRebuildError("policy rebuild run does not exist")
                    cursor.execute(
                        """
                        SELECT count(*)::int AS total_shards,
                               COALESCE(sum(shard.processed_rows),0)::bigint AS processed,
                               COALESCE(sum(shard.failed_rows),0)::bigint AS failed,
                               count(*) FILTER (WHERE shard.status='succeeded')::int AS succeeded_shards,
                               count(*) FILTER (WHERE shard.status<>'succeeded')::int AS failed_shards
                        FROM feature_clock.recalculation_shards shard
                        WHERE shard.recalculation_id=%s
                        """,
                        (recalculation_id,),
                    )
                    totals = _row(cursor)
                    if totals is None or int(totals["total_shards"]) == 0:
                        raise PolicyRebuildError("policy rebuild run has no shards")
                    failed_shards = int(totals["failed_shards"])
                    processed = int(totals["processed"])
                    if run["status"] in {"succeeded", "cancelled"}:
                        status = str(run["status"])
                    else:
                        status = (
                            "succeeded"
                            if failed_shards == 0
                            else "partial" if processed else "failed"
                        )
                        cursor.execute(
                            """
                            UPDATE feature_clock.recalculation_runs
                            SET status=%s,processed_channels=%s,failed_channels=%s,
                                completed_at=now(),updated_at=now()
                            WHERE recalculation_id=%s AND status<>'cancelled'
                            """,
                            (status, processed, int(totals["failed"]), recalculation_id),
                        )
                    return PolicyRebuildResult(
                        recalculation_id=recalculation_id,
                        policy_version=str(run["policy_version"]),
                        status=status,
                        processed_channels=processed,
                        failed_channels=int(totals["failed"]),
                        succeeded_shards=int(totals["succeeded_shards"]),
                        failed_shards=failed_shards,
                    )
        finally:
            connection.close()


def recalculate_locked_clock_row(
    cursor: Any,
    *,
    row: Mapping[str, Any],
    policy: ActivePolicyContract,
    decision_mode: str,
    audit_reason: str,
    audit_context: Mapping[str, Any] | None = None,
) -> None:
    """Recalculate a caller-locked Channel and preserve every earlier Due Day."""

    FeaturePolicyRebuilder._rebuild_clock_row(
        cursor,
        row=row,
        policy=policy,
        decision_mode=decision_mode,
        audit_reason=audit_reason,
        audit_context=audit_context,
    )
