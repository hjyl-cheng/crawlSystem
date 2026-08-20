from datetime import date, datetime, timezone
import os
import unittest
from unittest.mock import patch

from feature_engine.scheduler_service import _plan_day


class SchedulerServicePlanDayTests(unittest.TestCase):
    def test_defaults_to_the_current_utc_day(self) -> None:
        now = datetime(2026, 7, 21, 23, 30, tzinfo=timezone.utc)

        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_plan_day(now=now).isoformat(), "2026-07-21")

    def test_accepts_today_and_past_days_for_idempotent_replay(self) -> None:
        now = datetime(2026, 7, 21, 1, 0, tzinfo=timezone.utc)

        for value in ("2026-07-21", "2026-07-20"):
            with self.subTest(value=value), patch.dict(
                os.environ,
                {"SCHEDULER_PLAN_DAY": value},
                clear=True,
            ):
                self.assertEqual(_plan_day(now=now).isoformat(), value)

    def test_accepts_tomorrow_but_rejects_a_later_utc_plan_day(self) -> None:
        now = datetime(2026, 7, 21, 23, 59, tzinfo=timezone.utc)

        with patch.dict(
            os.environ,
            {"SCHEDULER_PLAN_DAY": "2026-07-22"},
            clear=True,
        ):
            self.assertEqual(_plan_day(now=now), date(2026, 7, 22))

        with patch.dict(
            os.environ,
            {"SCHEDULER_PLAN_DAY": "2026-07-23"},
            clear=True,
        ), self.assertRaisesRegex(RuntimeError, "one UTC day ahead"):
            _plan_day(now=now)


if __name__ == "__main__":
    unittest.main()
