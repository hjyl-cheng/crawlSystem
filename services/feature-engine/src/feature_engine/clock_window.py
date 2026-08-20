from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

from .utc import as_utc


CLOCK_WINDOW_START = time(0, 30)
CLOCK_WINDOW_END = time(21, 30)
CLOCK_WINDOW_START_OFFSET = timedelta(minutes=30)
CLOCK_WINDOW_END_OFFSET = timedelta(hours=21, minutes=30)
CLOCK_WINDOW_DURATION = CLOCK_WINDOW_END_OFFSET - CLOCK_WINDOW_START_OFFSET
CLOCK_WINDOW_EARLY_SHIFT = CLOCK_WINDOW_START_OFFSET
CLOCK_WINDOW_LATE_SHIFT = (
    timedelta(days=1) - CLOCK_WINDOW_END_OFFSET + CLOCK_WINDOW_START_OFFSET
)


def clock_window_bounds(value: date) -> tuple[datetime, datetime]:
    start = datetime.combine(value, CLOCK_WINDOW_START, tzinfo=timezone.utc)
    end = datetime.combine(value, CLOCK_WINDOW_END, tzinfo=timezone.utc)
    return start, end


def clock_due_at_for_day(value: date) -> datetime:
    """Return the legacy timestamp representation of an authoritative UTC due day."""

    if not isinstance(value, date) or isinstance(value, datetime):
        raise ValueError("due_day must be a date")
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def clock_due_at_in_window(value: datetime) -> bool:
    normalized = as_utc(value, "due_at")
    offset = normalized - datetime.combine(
        normalized.date(), time.min, tzinfo=timezone.utc
    )
    return CLOCK_WINDOW_START_OFFSET <= offset < CLOCK_WINDOW_END_OFFSET


def normalize_clock_due_at(value: datetime) -> datetime:
    """Move a UTC Clock forward into [00:30, 21:30) without hiding its phase."""

    normalized = as_utc(value, "due_at")
    offset = normalized - datetime.combine(
        normalized.date(), time.min, tzinfo=timezone.utc
    )
    if offset < CLOCK_WINDOW_START_OFFSET:
        return normalized + CLOCK_WINDOW_EARLY_SHIFT
    if offset >= CLOCK_WINDOW_END_OFFSET:
        return normalized + CLOCK_WINDOW_LATE_SHIFT
    return normalized
