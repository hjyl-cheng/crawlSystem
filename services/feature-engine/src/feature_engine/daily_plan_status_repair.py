from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timezone
from hashlib import sha256
import argparse
import json
import os
from pathlib import Path
from typing import Any, Callable, Mapping
from uuid import uuid4

from .database_topology import validate_shared_feature_database
from .plan_status import reduce_daily_plan_status
from .runtime_environment import required_environment


REPORT_FORMAT = "qy-daily-plan-status-repair-v1"


@dataclass(frozen=True, slots=True)
class LatestPlanOutcome:
    observation_kind: str
    kind_sequence: int
    outcome: str
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class DailyPlanStatusRepairItem:
    plan_id: str
    plan_day: date
    channel_id: str
    current_status: str
    current_error_code: str | None
    target_status: str
    target_error_code: str | None
    latest_observed_at: datetime | None
    latest_outcomes: tuple[LatestPlanOutcome, ...]
    classification: str
    repairable: bool
    reason: str


@dataclass(frozen=True, slots=True)
class DailyPlanStatusRepairReport:
    format: str
    generated_at: datetime
    from_day: date
    to_day: date
    examined_plans: int
    candidate_plans: int
    current_failed: int
    current_partial: int
    repairable_plans: int
    consistent_plans: int
    blocked_plans: int
    can_apply: bool
    confirmation: str | None
    applied: bool
    repaired_plans: int
    repair_batch_id: str | None
    items: tuple[DailyPlanStatusRepairItem, ...]


def _column_name(description: Any) -> str:
    return description.name if hasattr(description, "name") else description[0]


def _rows(cursor: Any) -> list[dict[str, Any]]:
    columns = [_column_name(description) for description in cursor.description]
    return [
        dict(value) if isinstance(value, Mapping) else dict(zip(columns, value, strict=True))
        for value in cursor.fetchall()
    ]


def _iso(value: date | datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _confirmation_payload(
    *, from_day: date, to_day: date, items: tuple[DailyPlanStatusRepairItem, ...]
) -> dict[str, Any]:
    return {
        "action": "recover_daily_plan_statuses_from_latest_applied_observations",
        "format": REPORT_FORMAT,
        "from_day": from_day.isoformat(),
        "to_day": to_day.isoformat(),
        "items": [
            {
                "plan_id": item.plan_id,
                "plan_day": item.plan_day.isoformat(),
                "channel_id": item.channel_id,
                "current_status": item.current_status,
                "current_error_code": item.current_error_code,
                "target_status": item.target_status,
                "target_error_code": item.target_error_code,
                "latest_observed_at": _iso(item.latest_observed_at),
                "latest_outcomes": [
                    {
                        "observation_kind": outcome.observation_kind,
                        "kind_sequence": outcome.kind_sequence,
                        "outcome": outcome.outcome,
                        "observed_at": outcome.observed_at.isoformat(),
                    }
                    for outcome in item.latest_outcomes
                ],
            }
            for item in items
        ],
    }


def daily_plan_status_repair_confirmation(
    *, from_day: date, to_day: date, items: tuple[DailyPlanStatusRepairItem, ...]
) -> str:
    body = json.dumps(
        _confirmation_payload(from_day=from_day, to_day=to_day, items=items),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"sha256:{sha256(body.encode('utf-8')).hexdigest()}"


def _load_repair_items(
    cursor: Any,
    *,
    from_day: date,
    to_day: date,
    lock: bool,
) -> tuple[DailyPlanStatusRepairItem, ...]:
    lock_clause = "FOR UPDATE OF plan" if lock else ""
    cursor.execute(
        f"""
        SELECT plan.plan_id::text AS plan_id,plan.plan_day,plan.channel_id,
               plan.run_about,plan.run_video,plan.run_agent,
               plan.status AS current_status,
               plan.error_code AS current_error_code,
               latest.observation_kind,latest.kind_sequence,
               latest.outcome,latest.observed_at
        FROM feature_clock.daily_channel_plans plan
        LEFT JOIN LATERAL (
          SELECT DISTINCT ON (inbox.observation_kind)
                 inbox.observation_kind,inbox.kind_sequence,
                 inbox.outcome,inbox.observed_at
          FROM feature_clock.crawler_event_inbox inbox
          WHERE inbox.plan_id=plan.plan_id AND inbox.status='applied'
          ORDER BY inbox.observation_kind,inbox.kind_sequence DESC
        ) latest ON true
        WHERE plan.plan_day BETWEEN %s AND %s
          AND plan.status IN ('failed','partial')
        ORDER BY plan.plan_id,latest.observation_kind
        {lock_clause}
        """,
        (from_day, to_day),
    )
    grouped: dict[str, dict[str, Any]] = {}
    for row in _rows(cursor):
        plan_id = str(row["plan_id"])
        current = grouped.setdefault(
            plan_id,
            {
                "plan": row,
                "outcomes": [],
            },
        )
        if row["observation_kind"] is not None:
            current["outcomes"].append(
                LatestPlanOutcome(
                    observation_kind=str(row["observation_kind"]),
                    kind_sequence=int(row["kind_sequence"]),
                    outcome=str(row["outcome"]),
                    observed_at=row["observed_at"],
                )
            )

    items: list[DailyPlanStatusRepairItem] = []
    for plan_id in sorted(grouped):
        source = grouped[plan_id]
        plan = source["plan"]
        latest_outcomes = tuple(source["outcomes"])
        outcomes = {
            outcome.observation_kind: outcome.outcome for outcome in latest_outcomes
        }
        decision = reduce_daily_plan_status(plan, outcomes)
        current_status = str(plan["current_status"])
        consistent = decision.status == current_status
        repairable = not consistent and decision.status == "succeeded"
        classification = (
            "consistent" if consistent else "repairable" if repairable else "blocked"
        )
        if consistent:
            reason = "current_status_matches_latest_requested_observations"
        elif repairable:
            reason = "latest_requested_observations_complete"
        else:
            reason = f"latest_requested_observations_reduce_to_{decision.status}"
        items.append(
            DailyPlanStatusRepairItem(
                plan_id=plan_id,
                plan_day=plan["plan_day"],
                channel_id=str(plan["channel_id"]),
                current_status=current_status,
                current_error_code=plan["current_error_code"],
                target_status=decision.status,
                target_error_code=decision.error_code,
                latest_observed_at=max(
                    (outcome.observed_at for outcome in latest_outcomes),
                    default=None,
                ),
                latest_outcomes=latest_outcomes,
                classification=classification,
                repairable=repairable,
                reason=reason,
            )
        )
    return tuple(items)


def _build_report(
    *,
    from_day: date,
    to_day: date,
    items: tuple[DailyPlanStatusRepairItem, ...],
    generated_at: datetime,
) -> DailyPlanStatusRepairReport:
    repair_items = tuple(item for item in items if item.repairable)
    consistent = sum(item.classification == "consistent" for item in items)
    blocked = sum(item.classification == "blocked" for item in items)
    candidate_plans = len(repair_items) + blocked
    can_apply = bool(repair_items) and blocked == 0
    confirmation = (
        daily_plan_status_repair_confirmation(
            from_day=from_day,
            to_day=to_day,
            items=repair_items,
        )
        if can_apply
        else None
    )
    return DailyPlanStatusRepairReport(
        format=REPORT_FORMAT,
        generated_at=generated_at,
        from_day=from_day,
        to_day=to_day,
        examined_plans=len(items),
        candidate_plans=candidate_plans,
        current_failed=sum(item.current_status == "failed" for item in items),
        current_partial=sum(item.current_status == "partial" for item in items),
        repairable_plans=len(repair_items),
        consistent_plans=consistent,
        blocked_plans=blocked,
        can_apply=can_apply,
        confirmation=confirmation,
        applied=False,
        repaired_plans=0,
        repair_batch_id=None,
        items=items,
    )


def repair_daily_plan_statuses(
    connection_factory: Callable[[], Any],
    *,
    from_day: date,
    to_day: date,
    apply: bool,
    confirm: str | None = None,
    operator: str | None = None,
    reason: str | None = None,
    now: datetime | None = None,
) -> DailyPlanStatusRepairReport:
    if from_day > to_day:
        raise ValueError("from_day cannot be after to_day")
    if apply and not confirm:
        raise ValueError("apply requires the confirmation from a current dry run")
    normalized_operator = str(operator or "").strip()
    normalized_reason = str(reason or "").strip()
    if apply and (not normalized_operator or not normalized_reason):
        raise ValueError("apply requires non-empty operator and reason")

    generated_at = now or datetime.now(timezone.utc)
    connection = connection_factory()
    try:
        with connection.transaction():
            with connection.cursor() as cursor:
                if apply:
                    cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                items = _load_repair_items(
                    cursor,
                    from_day=from_day,
                    to_day=to_day,
                    lock=apply,
                )
                report = _build_report(
                    from_day=from_day,
                    to_day=to_day,
                    items=items,
                    generated_at=generated_at,
                )
                if not apply:
                    return report
                if not report.can_apply:
                    raise RuntimeError(
                        "refusing Daily Plan repair: every candidate must reduce to succeeded"
                    )
                if confirm != report.confirmation:
                    raise RuntimeError(
                        "refusing Daily Plan repair: confirmation does not match current evidence"
                    )

                repair_batch_id = str(uuid4())
                repair_items = tuple(item for item in items if item.repairable)
                for item in repair_items:
                    assert item.latest_observed_at is not None
                    cursor.execute(
                        """
                        UPDATE feature_clock.daily_channel_plans
                        SET status='succeeded',error_code=NULL,
                            finished_at=GREATEST(
                              COALESCE(finished_at,%s),%s
                            ),
                            completed_at=now(),lease_owner=NULL,
                            lease_expires_at=NULL,updated_at=now()
                        WHERE plan_id=%s
                          AND status=%s
                          AND error_code IS NOT DISTINCT FROM %s
                        """,
                        (
                            item.latest_observed_at,
                            item.latest_observed_at,
                            item.plan_id,
                            item.current_status,
                            item.current_error_code,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise RuntimeError(
                            f"Daily Plan repair lost its optimistic lock: {item.plan_id}"
                        )
                    cursor.execute(
                        """
                        INSERT INTO feature_clock.daily_plan_status_repair_audit (
                          audit_id,repair_batch_id,plan_id,plan_day,channel_id,
                          previous_status,previous_error_code,repaired_status,
                          latest_outcomes_json,confirmation,operator,reason,repaired_at
                        ) VALUES (
                          %s,%s,%s,%s,%s,%s,%s,'succeeded',%s::jsonb,
                          %s,%s,%s,now()
                        )
                        """,
                        (
                            str(uuid4()),
                            repair_batch_id,
                            item.plan_id,
                            item.plan_day,
                            item.channel_id,
                            item.current_status,
                            item.current_error_code,
                            json.dumps(
                                [
                                    {
                                        "observation_kind": outcome.observation_kind,
                                        "kind_sequence": outcome.kind_sequence,
                                        "outcome": outcome.outcome,
                                        "observed_at": outcome.observed_at.isoformat(),
                                    }
                                    for outcome in item.latest_outcomes
                                ],
                                separators=(",", ":"),
                                sort_keys=True,
                            ),
                            report.confirmation,
                            normalized_operator,
                            normalized_reason,
                        ),
                    )
                return replace(
                    report,
                    applied=True,
                    repaired_plans=len(repair_items),
                    repair_batch_id=repair_batch_id,
                )
    finally:
        connection.close()


def report_as_json(report: DailyPlanStatusRepairReport) -> dict[str, Any]:
    return asdict(report)


def _emit(report: DailyPlanStatusRepairReport, output: str | None) -> None:
    body = f"{json.dumps(report_as_json(report), default=_iso, indent=2, sort_keys=True)}\n"
    if output:
        with Path(output).open("x", encoding="utf-8", newline="") as stream:
            stream.write(body)
    else:
        print(body, end="")


def _day(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from error


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit and recover stale failed/partial Daily Plan statuses."
    )
    parser.add_argument("--from-day", required=True, type=_day)
    parser.add_argument("--to-day", required=True, type=_day)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--operator")
    parser.add_argument("--reason")
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.apply and not args.confirm:
        parser.error("--apply requires --confirm")
    if not args.apply and args.confirm:
        parser.error("--confirm requires --apply")

    import psycopg

    database_url = required_environment(os.environ, "FEATURE_DATABASE_URL")
    expected_database = required_environment(os.environ, "EXPECTED_FEATURE_DATABASE")
    expected_user = required_environment(os.environ, "EXPECTED_FEATURE_DATABASE_USER")

    def connect() -> Any:
        return psycopg.connect(database_url, options="-c timezone=UTC")

    validate_shared_feature_database(
        connect,
        expected_database=expected_database,
        expected_user=expected_user,
        required_feature_relations=(
            "feature_clock.daily_channel_plans",
            "feature_clock.crawler_event_inbox",
            "feature_clock.daily_plan_status_repair_audit",
        ),
    )
    report = repair_daily_plan_statuses(
        connect,
        from_day=args.from_day,
        to_day=args.to_day,
        apply=args.apply,
        confirm=args.confirm,
        operator=args.operator,
        reason=args.reason,
    )
    _emit(report, args.output)


if __name__ == "__main__":
    main()
