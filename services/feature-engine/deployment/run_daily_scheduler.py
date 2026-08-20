from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
import signal
import subprocess
import threading


STOPPING = threading.Event()


def bounded_integer(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name) or default)
    except ValueError as error:
        raise RuntimeError(f"{name} must be an integer") from error
    if value < minimum or value > maximum:
        raise RuntimeError(f"{name} must be between {minimum} and {maximum}")
    return value


def next_window_open(now: datetime, hour: int, minute: int) -> datetime:
    current = now.astimezone(timezone.utc)
    target = current.replace(hour=hour, minute=minute, second=0, microsecond=0)
    window_end = current.replace(hour=21, minute=30, second=0, microsecond=0)
    if current >= window_end:
        target += timedelta(days=1)
    return max(current, target)


def inside_dispatch_window(now: datetime) -> bool:
    current = now.astimezone(timezone.utc)
    start = current.replace(hour=0, minute=30, second=0, microsecond=0)
    end = current.replace(hour=21, minute=30, second=0, microsecond=0)
    return start <= current < end


def scheduler_environment() -> dict[str, str]:
    environment = dict(os.environ)
    if not str(environment.get("SCHEDULER_PLAN_DAY") or "").strip():
        environment.pop("SCHEDULER_PLAN_DAY", None)
    return environment


def log(event: str, **details: object) -> None:
    print(json.dumps({"event": event, **details}, separators=(",", ":")), flush=True)


def stop(_signum: int, _frame: object) -> None:
    STOPPING.set()


def main() -> None:
    hour = bounded_integer("SCHEDULER_DAILY_HOUR_UTC", 0, 0, 23)
    minute = bounded_integer("SCHEDULER_DAILY_MINUTE_UTC", 30, 0, 59)
    retry_seconds = bounded_integer("SCHEDULER_RETRY_SECONDS", 300, 10, 3600)
    refresh_seconds = bounded_integer("SCHEDULER_REFRESH_SECONDS", 300, 10, 3600)
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    log(
        "feature_daily_planner_ready",
        hour_utc=hour,
        minute_utc=minute,
        refresh_seconds=refresh_seconds,
    )

    while not STOPPING.is_set():
        now = datetime.now(timezone.utc)
        if not inside_dispatch_window(now):
            target = next_window_open(now, hour, minute)
            wait_seconds = max(1, int((target - now).total_seconds()))
            log("feature_daily_planner_waiting_for_window", next_run_at=target.isoformat())
            STOPPING.wait(wait_seconds)
            continue
        started_at = datetime.now(timezone.utc)
        result = subprocess.run(
            ["feature-schedule-day"],
            env=scheduler_environment(),
            check=False,
        )
        if result.returncode != 0:
            log(
                "feature_daily_scheduler_failed",
                return_code=result.returncode,
                retry_seconds=retry_seconds,
            )
            STOPPING.wait(retry_seconds)
            continue

        log(
            "feature_daily_planner_refresh_wait",
            completed_at=datetime.now(timezone.utc).isoformat(),
            elapsed_seconds=round((datetime.now(timezone.utc) - started_at).total_seconds(), 3),
            refresh_seconds=refresh_seconds,
        )
        STOPPING.wait(refresh_seconds)


if __name__ == "__main__":
    main()

