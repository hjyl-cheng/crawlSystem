from __future__ import annotations

from datetime import date, datetime, timezone


def as_utc(value: datetime, field: str = "timestamp") -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return value.astimezone(timezone.utc)


def utc_day(value: datetime, field: str = "timestamp") -> date:
    return as_utc(value, field).date()
