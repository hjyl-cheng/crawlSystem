from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
import os
from typing import Any, Mapping

from .database_topology import validate_shared_feature_database
from .scheduler import DailyPlanConfig, DailyScheduler
from .runtime_environment import required_environment


def _required_environment(name: str) -> str:
    return required_environment(os.environ, name)


def _plan_day(*, now: datetime | None = None) -> date:
    utc_today = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).date()
    value = str(os.environ.get("SCHEDULER_PLAN_DAY") or "").strip()
    target_day = date.fromisoformat(value) if value else utc_today
    if target_day > utc_today + timedelta(days=1):
        raise RuntimeError(
            "SCHEDULER_PLAN_DAY cannot be more than one UTC day ahead"
        )
    return target_day


def main() -> None:
    import psycopg

    database_url = _required_environment("FEATURE_DATABASE_URL")
    expected_database = _required_environment("EXPECTED_FEATURE_DATABASE")
    expected_user = _required_environment("EXPECTED_FEATURE_DATABASE_USER")
    config = DailyPlanConfig(
        planner_config_version=_required_environment("SCHEDULER_PLANNER_CONFIG_VERSION"),
        capacity_version=_required_environment("SCHEDULER_CAPACITY_VERSION"),
        queue_name=str(
            os.environ.get("SCHEDULER_QUEUE_NAME") or "youtube-channel-incremental"
        ).strip(),
        capacity_factor=float(os.environ.get("SCHEDULER_CAPACITY_FACTOR") or "1"),
        player_cap=int(os.environ.get("SCHEDULER_PLAYER_CAP") or "20"),
        next_cap=int(os.environ.get("SCHEDULER_NEXT_CAP") or "8"),
    )
    batch_size = int(os.environ.get("SCHEDULER_BATCH_SIZE") or "100")
    maximum = int(os.environ.get("SCHEDULER_MAX_PLANS") or "100000")
    if maximum <= 0:
        raise RuntimeError("SCHEDULER_MAX_PLANS must be positive")
    channel_id = str(os.environ.get("SCHEDULER_CHANNEL_ID") or "").strip() or None
    if channel_id is not None and maximum != 1:
        raise RuntimeError("SCHEDULER_CHANNEL_ID requires SCHEDULER_MAX_PLANS=1")

    def connect() -> Any:
        return psycopg.connect(database_url, options="-c timezone=UTC")

    validate_shared_feature_database(
        connect,
        expected_database=expected_database,
        expected_user=expected_user,
        required_feature_relations=(
            "feature_clock.daily_channel_plans",
            "feature_clock.dispatch_outbox",
        ),
    )

    target_day = _plan_day()
    scheduler = DailyScheduler(connect)
    result = scheduler.run_day(
        plan_day=target_day,
        config=config,
        batch_size=batch_size,
        max_plans=maximum,
        channel_id=channel_id,
    )
    print(
        json.dumps(
            {
                "event": "feature_schedule_day_completed",
                "database": expected_database,
                "plan_day": target_day.isoformat(),
                "planned": result.planned,
                "batches": result.batches,
                "queue": config.queue_name,
                "planner_config_version": config.planner_config_version,
                "capacity_version": config.capacity_version,
                "capacity_exhausted": result.capacity_exhausted,
                "channel_id": channel_id,
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
