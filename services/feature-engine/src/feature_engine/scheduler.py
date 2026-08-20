from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
import math
from typing import Any, Callable, Mapping
from uuid import UUID, uuid5

from .clock_window import clock_window_bounds
from .recalculation import (
    RECALCULATION_COORDINATION_LOCK_ID,
    UNRESOLVED_RECALCULATION_STATUSES,
)
from .utc import as_utc


PLAN_NAMESPACE = UUID("8d5b40e0-1aa8-4d0a-bcc6-9c38bcaf5e6f")
INCREMENTAL_QUEUE = "youtube-channel-incremental"


class SchedulerConfigurationError(ValueError):
    pass


class ClockStateInvariantError(RuntimeError):
    pass


def _required_text(value: Any, field: str) -> str:
    output = str(value or "").strip()
    if not output:
        raise SchedulerConfigurationError(f"{field} is required")
    return output


def _non_negative_integer(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise SchedulerConfigurationError(f"{field} must be a non-negative integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as error:
        raise SchedulerConfigurationError(f"{field} must be a non-negative integer") from error
    if parsed < 0 or parsed != value:
        raise SchedulerConfigurationError(f"{field} must be a non-negative integer")
    return parsed


def _positive_integer(value: Any, field: str) -> int:
    parsed = _non_negative_integer(value, field)
    if parsed == 0:
        raise SchedulerConfigurationError(f"{field} must be positive")
    return parsed


def _day(value: Any, field: str) -> date:
    if isinstance(value, datetime):
        raise ClockStateInvariantError(f"{field} must be a date, not a timestamp")
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as error:
        raise ClockStateInvariantError(f"{field} must be an ISO date") from error


@dataclass(frozen=True, slots=True)
class DailyPlanConfig:
    planner_config_version: str
    capacity_version: str
    queue_name: str = INCREMENTAL_QUEUE
    capacity_factor: float = 1.0
    player_cap: int = 20
    next_cap: int = 8

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "planner_config_version",
            _required_text(self.planner_config_version, "planner_config_version"),
        )
        object.__setattr__(
            self,
            "capacity_version",
            _required_text(self.capacity_version, "capacity_version"),
        )
        object.__setattr__(self, "queue_name", _required_text(self.queue_name, "queue_name"))
        if self.queue_name != INCREMENTAL_QUEUE:
            raise SchedulerConfigurationError(
                f"queue_name must be {INCREMENTAL_QUEUE}; existing Full queues are forbidden"
            )
        if isinstance(self.capacity_factor, bool):
            raise SchedulerConfigurationError("capacity_factor must be between 0 and 1")
        try:
            factor = float(self.capacity_factor)
        except (TypeError, ValueError) as error:
            raise SchedulerConfigurationError("capacity_factor must be between 0 and 1") from error
        if not math.isfinite(factor) or not 0 <= factor <= 1:
            raise SchedulerConfigurationError("capacity_factor must be between 0 and 1")
        object.__setattr__(self, "capacity_factor", factor)
        object.__setattr__(self, "player_cap", _non_negative_integer(self.player_cap, "player_cap"))
        object.__setattr__(self, "next_cap", _non_negative_integer(self.next_cap, "next_cap"))


@dataclass(frozen=True, slots=True)
class PlannedDispatch:
    plan_id: str
    plan_mode: str
    queue_name: str
    plan_day: date
    channel_id: str
    due_day: date
    due_at: datetime
    eligible_at: datetime
    run_about: bool
    run_video: bool
    run_agent: bool
    dispatch_slot: int
    capacity_factor: float
    player_cap: int
    next_cap: int
    estimated_request_cost: int
    source_clock_version: int
    policy_version: str
    planner_config_version: str
    capacity_version: str


@dataclass(frozen=True, slots=True)
class SchedulerBatchResult:
    plan_day: date
    selected: int
    created: tuple[PlannedDispatch, ...]
    capacity_exhausted: bool = False


@dataclass(frozen=True, slots=True)
class SchedulerRunResult:
    plan_day: date
    planned: int
    batches: int
    capacity_exhausted: bool = False


@dataclass(frozen=True, slots=True)
class _PlanRequirements:
    plan_mode: str
    due_day: date
    due_at: datetime
    run_about: bool
    run_video: bool
    run_agent: bool


def _plan_requirements(clock: Mapping[str, Any], *, plan_day: date) -> _PlanRequirements:
    target_day = _day(plan_day, "plan_day")
    lifecycle_status = str(clock.get("lifecycle_status", "active"))
    if lifecycle_status == "removed":
        raise ClockStateInvariantError("removed Channel cannot produce a Daily Plan")
    if lifecycle_status == "dormant":
        recheck_day = _day(clock.get("dormant_recheck_day"), "dormant_recheck_day")
        if recheck_day > target_day:
            raise ClockStateInvariantError("dormant Channel is not due during plan_day")
        if bool(clock.get("same_day_video_covered", False)):
            raise ClockStateInvariantError("a due dormant Channel has already been probed")
        return _PlanRequirements(
            plan_mode="dormant_probe",
            due_day=recheck_day,
            due_at=datetime.combine(recheck_day, time.min, tzinfo=timezone.utc),
            run_about=False,
            run_video=True,
            run_agent=False,
        )
    if lifecycle_status != "active":
        raise ClockStateInvariantError(f"unsupported Channel lifecycle: {lifecycle_status}")
    due_days = {
        field: _day(clock.get(field), field)
        for field in (
            "about_due_day",
            "video_due_day",
            "agent_due_day",
            "channel_next_run_day",
        )
    }
    expected_channel_due_day = min(
        due_days["about_due_day"],
        due_days["video_due_day"],
        due_days["agent_due_day"],
    )
    if due_days["channel_next_run_day"] != expected_channel_due_day:
        raise ClockStateInvariantError("channel_next_run_day is not the minimum Clock due day")
    if expected_channel_due_day > target_day:
        raise ClockStateInvariantError("channel is not due during plan_day")

    selected = {
        kind: (
            due_days[f"{kind}_due_day"] <= target_day
            and not bool(clock.get(f"same_day_{kind}_covered", False))
        )
        for kind in ("about", "video", "agent")
    }
    if not any(selected.values()):
        raise ClockStateInvariantError("a due Channel produced an empty uncovered task mask")
    selected_due_day = min(
        due_days[f"{kind}_due_day"] for kind, included in selected.items() if included
    )
    return _PlanRequirements(
        plan_mode="standard",
        due_day=selected_due_day,
        due_at=datetime.combine(selected_due_day, time.min, tzinfo=timezone.utc),
        run_about=selected["about"],
        run_video=selected["video"],
        run_agent=selected["agent"],
    )


def build_daily_plan(
    clock: Mapping[str, Any],
    *,
    plan_day: date,
    config: DailyPlanConfig,
) -> PlannedDispatch:
    target_day = _day(plan_day, "plan_day")
    channel_id = _required_text(clock.get("channel_id"), "channel_id")
    policy_version = _required_text(clock.get("policy_version"), "policy_version")

    requirements = _plan_requirements(clock, plan_day=target_day)
    eligible_at, _window_end = clock_window_bounds(target_day)

    source_clock_version = _positive_integer(clock.get("clock_version"), "clock_version")
    dispatch_slot = _non_negative_integer(clock.get("dispatch_slot"), "dispatch_slot")
    estimated_request_cost = _non_negative_integer(
        clock.get("estimated_request_cost", 0), "estimated_request_cost"
    )
    same_day_plan_count = _non_negative_integer(
        clock.get("same_day_plan_count", 0) or 0,
        "same_day_plan_count",
    )
    plan_sequence = same_day_plan_count + 1
    identity = f"{target_day.isoformat()}:{channel_id}"
    if plan_sequence > 1:
        identity = f"{identity}:{plan_sequence}"
    plan_id = str(uuid5(PLAN_NAMESPACE, identity))
    return PlannedDispatch(
        plan_id=plan_id,
        plan_mode=requirements.plan_mode,
        queue_name=config.queue_name,
        plan_day=target_day,
        channel_id=channel_id,
        due_day=requirements.due_day,
        due_at=requirements.due_at,
        eligible_at=eligible_at,
        run_about=requirements.run_about,
        run_video=requirements.run_video,
        run_agent=requirements.run_agent,
        dispatch_slot=dispatch_slot,
        capacity_factor=float(config.capacity_factor),
        player_cap=int(config.player_cap),
        next_cap=int(config.next_cap),
        estimated_request_cost=estimated_request_cost,
        source_clock_version=source_clock_version,
        policy_version=policy_version,
        planner_config_version=config.planner_config_version,
        capacity_version=config.capacity_version,
    )


def _column_name(description: Any) -> str:
    return description.name if hasattr(description, "name") else description[0]


def _rows(cursor: Any) -> list[dict[str, Any]]:
    values = cursor.fetchall()
    columns = [_column_name(description) for description in cursor.description]
    return [
        dict(value) if isinstance(value, Mapping) else dict(zip(columns, value, strict=True))
        for value in values
    ]


class DailyScheduler:
    """Freeze date-due Clock rows into eligible Daily Plans."""

    def __init__(self, connection_factory: Callable[[], Any]) -> None:
        self._connection_factory = connection_factory

    def run_batch(
        self,
        *,
        plan_day: date,
        config: DailyPlanConfig,
        batch_size: int = 100,
        channel_id: str | None = None,
        now: datetime | None = None,
    ) -> SchedulerBatchResult:
        target_day = _day(plan_day, "plan_day")
        limit = _positive_integer(batch_size, "batch_size")
        target_channel_id = (
            _required_text(channel_id, "channel_id") if channel_id is not None else None
        )
        if now is not None:
            as_utc(now, "now")
        connection = self._connection_factory()
        try:
            return self._run_batch_on_connection(
                connection,
                target_day=target_day,
                config=config,
                limit=limit,
                channel_id=target_channel_id,
            )
        finally:
            connection.close()

    def run_day(
        self,
        *,
        plan_day: date,
        config: DailyPlanConfig,
        batch_size: int = 100,
        max_plans: int = 100_000,
        channel_id: str | None = None,
        now: datetime | None = None,
    ) -> SchedulerRunResult:
        """Plan one UTC day while excluding Rebuild starts across every batch."""

        target_day = _day(plan_day, "plan_day")
        limit = _positive_integer(batch_size, "batch_size")
        maximum = _positive_integer(max_plans, "max_plans")
        target_channel_id = (
            _required_text(channel_id, "channel_id") if channel_id is not None else None
        )
        if now is not None:
            as_utc(now, "now")
        lock_connection = self._connection_factory()
        work_connection = None
        try:
            # Keep one transaction pinned for the full run. This preserves the
            # cross-batch exclusion guarantee under PgBouncer transaction pooling.
            with lock_connection.transaction():
                with lock_connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        "SELECT pg_advisory_xact_lock(%s)",
                        (RECALCULATION_COORDINATION_LOCK_ID,),
                    )
                work_connection = self._connection_factory()
                planned = 0
                batches = 0
                capacity_exhausted = False
                while planned < maximum:
                    result = self._run_batch_on_connection(
                        work_connection,
                        target_day=target_day,
                        config=config,
                        limit=min(limit, maximum - planned),
                        channel_id=target_channel_id,
                        coordination_locked=True,
                    )
                    batches += 1
                    planned += len(result.created)
                    if result.selected == 0:
                        break
                    if result.capacity_exhausted:
                        capacity_exhausted = True
                    if not result.created:
                        if result.capacity_exhausted:
                            break
                        raise ClockStateInvariantError(
                            "Scheduler selected rows but created no Plan"
                        )
                return SchedulerRunResult(
                    plan_day=target_day,
                    planned=planned,
                    batches=batches,
                    capacity_exhausted=capacity_exhausted,
                )
        finally:
            if work_connection is not None:
                work_connection.close()
            lock_connection.close()

    @staticmethod
    def _run_batch_on_connection(
        connection: Any,
        *,
        target_day: date,
        config: DailyPlanConfig,
        limit: int,
        channel_id: str | None,
        coordination_locked: bool = False,
    ) -> SchedulerBatchResult:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                if not coordination_locked:
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
                        """,
                        (list(UNRESOLVED_RECALCULATION_STATUSES),),
                    )
                unresolved = cursor.fetchone()
                if unresolved is not None:
                    recalculation_id, status = (
                        (unresolved.get("recalculation_id"), unresolved.get("status"))
                        if isinstance(unresolved, Mapping)
                        else (unresolved[0], unresolved[1])
                    )
                    raise ClockStateInvariantError(
                        "Feature recalculation "
                        f"{recalculation_id} ({status}) is unresolved; scheduling is blocked"
                    )
                cursor.execute(
                        """
                        WITH bounds AS (
                          SELECT %s::date AS target_day
                        )
                        SELECT c.channel_id,c.lifecycle_status,
                               c.dormant_recheck_day,
                               c.about_due_at,c.about_due_day,
                               c.video_due_at,c.video_due_day,
                               c.agent_due_at,c.agent_due_day,
                               c.channel_next_run_at,c.channel_next_run_day,
                               c.dispatch_slot,c.estimated_request_cost,c.policy_version,
                               c.clock_version,
                               same_day.plan_count AS same_day_plan_count,
                               same_day.about_covered AS same_day_about_covered,
                               same_day.video_covered AS same_day_video_covered,
                               same_day.agent_covered AS same_day_agent_covered
                        FROM feature_clock.channel_clock_state c
                        CROSS JOIN bounds
                        CROSS JOIN LATERAL (
                          SELECT
                            count(*)::int AS plan_count,
                            COALESCE(
                              bool_or(p.run_about) FILTER (WHERE p.status<>'cancelled'),
                              false
                            ) AS about_covered,
                            COALESCE(
                              bool_or(p.run_video) FILTER (WHERE p.status<>'cancelled'),
                              false
                            ) AS video_covered,
                            COALESCE(
                              bool_or(p.run_agent) FILTER (WHERE p.status<>'cancelled'),
                              false
                            ) AS agent_covered
                          FROM feature_clock.daily_channel_plans p
                          WHERE p.channel_id=c.channel_id
                            AND p.plan_day=bounds.target_day
                        ) same_day
                        WHERE c.lifecycle_status IN ('active','dormant')
                          AND (
                            (c.lifecycle_status='active'
                             AND c.channel_next_run_day <= bounds.target_day)
                            OR (c.lifecycle_status='dormant'
                                AND c.dormant_recheck_day <= bounds.target_day)
                          )
                          AND (%s::text IS NULL OR c.channel_id=%s::text)
                          AND NOT EXISTS (
                            SELECT 1
                            FROM feature_clock.daily_channel_plans p
                            WHERE p.channel_id=c.channel_id
                              AND (
                                p.status IN ('dispatching','dispatched','running')
                                OR (
                                  p.status='planned'
                                  AND (
                                    p.scheduled_at IS NOT NULL
                                    OR p.plan_day >= bounds.target_day
                                  )
                                )
                              )
                          )
                          AND (
                            (
                              c.lifecycle_status='active'
                              AND (
                                (c.about_due_day <= bounds.target_day AND NOT same_day.about_covered)
                                OR (c.video_due_day <= bounds.target_day AND NOT same_day.video_covered)
                                OR (c.agent_due_day <= bounds.target_day AND NOT same_day.agent_covered)
                              )
                            )
                            OR (
                              c.lifecycle_status='dormant'
                              AND NOT same_day.video_covered
                            )
                          )
                        ORDER BY CASE
                                   WHEN c.lifecycle_status='dormant' THEN c.dormant_recheck_day
                                   ELSE c.channel_next_run_day
                                 END,
                                 c.dispatch_slot,c.channel_id
                        FOR UPDATE OF c SKIP LOCKED
                        LIMIT %s
                        """,
                        (
                            target_day,
                            channel_id,
                            channel_id,
                            limit,
                        ),
                    )
                candidates = _rows(cursor)
                created: list[PlannedDispatch] = []
                for candidate in candidates:
                    plan = build_daily_plan(
                        candidate,
                        plan_day=target_day,
                        config=config,
                    )
                    cursor.execute(
                        """
                        UPDATE feature_clock.daily_channel_plans old
                        SET status='cancelled',error_code='superseded_by_daily_plan',
                            completed_at=now(),updated_at=now()
                        WHERE old.channel_id=%s
                          AND old.plan_day < %s
                          AND old.status='planned'
                          AND old.scheduled_at IS NULL
                          AND NOT EXISTS (
                            SELECT 1 FROM feature_clock.dispatch_outbox outbox
                            WHERE outbox.plan_id=old.plan_id
                          )
                        """,
                        (plan.channel_id, target_day),
                    )
                    cursor.execute(
                            """
                            INSERT INTO feature_clock.daily_channel_plans (
                              plan_id,plan_mode,plan_day,channel_id,due_day,due_at,eligible_at,
                              run_about,run_video,run_agent,
                              dispatch_slot,
                              capacity_factor,player_cap,next_cap,estimated_request_cost,
                              source_clock_version,policy_version,
                              planner_config_version,capacity_version,status
                            ) VALUES (
                              %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                              %s,%s,%s,%s,%s,%s,%s,%s,%s,'planned'
                            )
                            ON CONFLICT DO NOTHING
                            RETURNING plan_id
                            """,
                            (
                                plan.plan_id,
                                plan.plan_mode,
                                plan.plan_day,
                                plan.channel_id,
                                plan.due_day,
                                plan.due_at,
                                plan.eligible_at,
                                plan.run_about,
                                plan.run_video,
                                plan.run_agent,
                                plan.dispatch_slot,
                                plan.capacity_factor,
                                plan.player_cap,
                                plan.next_cap,
                                plan.estimated_request_cost,
                                plan.source_clock_version,
                                plan.policy_version,
                                plan.planner_config_version,
                                plan.capacity_version,
                            ),
                        )
                    if cursor.fetchone() is None:
                        continue
                    created.append(plan)
                return SchedulerBatchResult(
                    plan_day=target_day,
                    selected=len(candidates),
                    created=tuple(created),
                    capacity_exhausted=False,
                )
