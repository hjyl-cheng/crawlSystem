from __future__ import annotations

from datetime import date, datetime, timezone
import unittest

from feature_engine.clock_window import (
    clock_due_at_in_window,
    clock_window_bounds,
    normalize_clock_due_at,
)


class ClockWindowTests(unittest.TestCase):
    def test_window_is_utc_0030_until_2130(self) -> None:
        start, end = clock_window_bounds(date(2026, 7, 20))

        self.assertEqual(start, datetime(2026, 7, 20, 0, 30, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 7, 20, 21, 30, tzinfo=timezone.utc))
        self.assertTrue(clock_due_at_in_window(start))
        self.assertTrue(
            clock_due_at_in_window(
                datetime(2026, 7, 20, 21, 29, 59, tzinfo=timezone.utc)
            )
        )
        self.assertFalse(
            clock_due_at_in_window(
                datetime(2026, 7, 20, 0, 29, 59, tzinfo=timezone.utc)
            )
        )
        self.assertFalse(clock_due_at_in_window(end))

    def test_early_clock_moves_forward_without_collapsing_minutes(self) -> None:
        ideal = datetime(2026, 7, 20, 0, 10, 25, tzinfo=timezone.utc)

        self.assertEqual(
            normalize_clock_due_at(ideal),
            datetime(2026, 7, 20, 0, 40, 25, tzinfo=timezone.utc),
        )

    def test_late_clock_rolls_forward_without_shortening_the_tier(self) -> None:
        ideal = datetime(2026, 7, 20, 23, 50, 25, tzinfo=timezone.utc)

        self.assertEqual(
            normalize_clock_due_at(ideal),
            datetime(2026, 7, 21, 2, 50, 25, tzinfo=timezone.utc),
        )

    def test_offset_input_is_normalized_using_utc_time(self) -> None:
        ideal = datetime.fromisoformat("2026-07-21T05:00:00+08:00")

        self.assertEqual(
            normalize_clock_due_at(ideal),
            datetime(2026, 7, 20, 21, tzinfo=timezone.utc),
        )


if __name__ == "__main__":
    unittest.main()
