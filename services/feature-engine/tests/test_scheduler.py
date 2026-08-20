from __future__ import annotations

from datetime import date, datetime, timezone
import unittest

from feature_engine.scheduler import (
    ClockStateInvariantError,
    DailyPlanConfig,
    DailyScheduler,
    SchedulerConfigurationError,
    build_daily_plan,
)


class _Context:
    def __init__(self, value):
        self.value = value

    def __enter__(self):
        return self.value

    def __exit__(self, *_args):
        return False


class _Cursor:
    def __init__(self, *, unresolved=False) -> None:
        self.description = []
        self.queries: list[tuple[str, object]] = []
        self._row = None
        self.unresolved = unresolved

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=None) -> None:
        normalized = " ".join(str(query).split())
        self.queries.append((normalized, params))
        self._row = (
            ("00000000-0000-0000-0000-000000000001", "running")
            if self.unresolved and "FROM feature_clock.recalculation_runs" in normalized
            else None
        )

    def fetchone(self):
        return self._row

    def fetchall(self):
        return []


class _Connection:
    def __init__(self, *, unresolved=False) -> None:
        self.cursor_instance = _Cursor(unresolved=unresolved)
        self.closed = False

    def transaction(self):
        return _Context(self)

    def cursor(self):
        return _Context(self.cursor_instance)

    def close(self) -> None:
        self.closed = True


def clock(**overrides):
    value = {
        "channel_id": "UCscheduler",
        "about_due_at": datetime(2026, 7, 20, 20, 59, tzinfo=timezone.utc),
        "about_due_day": date(2026, 7, 20),
        "video_due_at": datetime(2026, 7, 19, 10, tzinfo=timezone.utc),
        "video_due_day": date(2026, 7, 19),
        "agent_due_at": datetime(2026, 8, 1, 9, tzinfo=timezone.utc),
        "agent_due_day": date(2026, 8, 1),
        "channel_next_run_at": datetime(2026, 7, 19, 10, tzinfo=timezone.utc),
        "channel_next_run_day": date(2026, 7, 19),
        "dispatch_slot": 42,
        "estimated_request_cost": 9,
        "policy_version": "v16-rule-1",
        "clock_version": 7,
    }
    value.update(overrides)
    return value


def config(**overrides):
    values = {
        "planner_config_version": "date-plan-1",
        "capacity_version": "capacity-2026-07-20",
        "capacity_factor": 0.75,
        "player_cap": 20,
        "next_cap": 8,
    }
    values.update(overrides)
    return DailyPlanConfig(**values)


class DailyPlanTests(unittest.TestCase):
    def test_due_days_create_an_unassigned_plan(self) -> None:
        plan = build_daily_plan(
            clock(),
            plan_day=date(2026, 7, 20),
            config=config(),
        )

        self.assertEqual(plan.due_day, date(2026, 7, 19))
        self.assertEqual(plan.due_at, datetime(2026, 7, 19, tzinfo=timezone.utc))
        self.assertEqual(plan.eligible_at, datetime(2026, 7, 20, 0, 30, tzinfo=timezone.utc))
        self.assertEqual(
            (plan.run_about, plan.run_video, plan.run_agent),
            (True, True, False),
        )
        self.assertFalse(hasattr(plan, "scheduled_at"))
        self.assertFalse(hasattr(plan, "payload"))

    def test_precise_compatibility_time_does_not_block_a_due_day(self) -> None:
        plan = build_daily_plan(
            clock(
                video_due_day=date(2026, 7, 21),
                channel_next_run_day=date(2026, 7, 20),
                channel_next_run_at=datetime(2026, 7, 20, 20, 59, tzinfo=timezone.utc),
            ),
            plan_day=date(2026, 7, 20),
            config=config(),
        )

        self.assertTrue(plan.run_about)
        self.assertFalse(plan.run_video)
        self.assertEqual(plan.eligible_at.hour, 0)
        self.assertEqual(plan.eligible_at.minute, 30)

    def test_overdue_and_today_domains_are_coalesced(self) -> None:
        plan = build_daily_plan(
            clock(
                about_due_day=date(2026, 7, 19),
                video_due_day=date(2026, 7, 21),
                channel_next_run_day=date(2026, 7, 19),
            ),
            plan_day=date(2026, 7, 20),
            config=config(),
        )

        self.assertEqual(
            (plan.run_about, plan.run_video, plan.run_agent),
            (True, False, False),
        )

    def test_same_day_supplement_excludes_domains_already_planned(self) -> None:
        first = build_daily_plan(
            clock(),
            plan_day=date(2026, 7, 20),
            config=config(),
        )
        supplement = build_daily_plan(
            clock(
                same_day_plan_count=1,
                same_day_video_covered=True,
            ),
            plan_day=date(2026, 7, 20),
            config=config(),
        )

        self.assertEqual(supplement.due_day, date(2026, 7, 20))
        self.assertEqual(
            (
                supplement.run_about,
                supplement.run_video,
                supplement.run_agent,
            ),
            (True, False, False),
        )
        self.assertNotEqual(supplement.plan_id, first.plan_id)

    def test_channel_next_run_day_is_authoritative_and_validated(self) -> None:
        with self.assertRaisesRegex(ClockStateInvariantError, "channel_next_run_day"):
            build_daily_plan(
                clock(channel_next_run_day=date(2026, 7, 20)),
                plan_day=date(2026, 7, 20),
                config=config(),
            )

    def test_removed_channel_cannot_build_a_daily_plan(self) -> None:
        with self.assertRaisesRegex(ClockStateInvariantError, "removed Channel"):
            build_daily_plan(
                clock(lifecycle_status="removed"),
                plan_day=date(2026, 7, 20),
                config=config(),
            )

    def test_plan_identity_is_stable_for_day_and_channel(self) -> None:
        first = build_daily_plan(clock(), plan_day=date(2026, 7, 20), config=config())
        second = build_daily_plan(clock(), plan_day=date(2026, 7, 20), config=config())
        self.assertEqual(first.plan_id, second.plan_id)

    def test_capacity_factor_is_bounded(self) -> None:
        for factor in (-0.1, 1.1, float("nan")):
            with self.subTest(factor=factor), self.assertRaises(SchedulerConfigurationError):
                config(capacity_factor=factor)

    def test_existing_full_crawler_queue_is_forbidden(self) -> None:
        with self.assertRaisesRegex(SchedulerConfigurationError, "Full queues are forbidden"):
            config(queue_name="youtube-channel-crawl")

    def test_scheduler_blocks_on_unresolved_recalculation(self) -> None:
        connection = _Connection(unresolved=True)
        scheduler = DailyScheduler(lambda: connection)

        with self.assertRaisesRegex(ClockStateInvariantError, "is unresolved"):
            scheduler.run_batch(
                plan_day=date(2026, 7, 20),
                config=config(),
                batch_size=1,
            )
        self.assertTrue(connection.closed)

    def test_scheduler_selects_by_due_day_and_canary_channel(self) -> None:
        connection = _Connection()
        result = DailyScheduler(lambda: connection).run_batch(
            plan_day=date(2026, 7, 22),
            config=config(),
            batch_size=1,
            channel_id="UC-canary",
        )

        self.assertEqual(result.selected, 0)
        candidate_query, params = next(
            item
            for item in connection.cursor_instance.queries
            if "FROM feature_clock.channel_clock_state" in item[0]
        )
        self.assertIn("c.channel_next_run_day <= bounds.target_day", candidate_query)
        self.assertIn("c.lifecycle_status='active'", candidate_query)
        self.assertNotIn("c.profile_due_day <=", candidate_query)
        self.assertNotIn("same_day.profile_covered", candidate_query)
        self.assertNotIn("c.channel_next_run_at <", candidate_query)
        self.assertEqual(
            params,
            (
                date(2026, 7, 22),
                "UC-canary",
                "UC-canary",
                1,
            ),
        )

    def test_full_day_holds_transaction_coordination_lock(self) -> None:
        lock_connection = _Connection()
        work_connection = _Connection(unresolved=True)
        connections = iter((lock_connection, work_connection))
        scheduler = DailyScheduler(lambda: next(connections))
        with self.assertRaisesRegex(ClockStateInvariantError, "is unresolved"):
            scheduler.run_day(
                plan_day=date(2026, 7, 20),
                config=config(),
                batch_size=1,
                max_plans=1,
            )
        lock_queries = [
            query for query, _params in lock_connection.cursor_instance.queries
        ]
        work_queries = [
            query for query, _params in work_connection.cursor_instance.queries
        ]
        self.assertTrue(any("pg_advisory_xact_lock(" in query for query in lock_queries))
        self.assertFalse(any("pg_advisory_xact_lock(" in query for query in work_queries))
        self.assertFalse(any("pg_advisory_unlock(" in query for query in lock_queries))
        self.assertTrue(lock_connection.closed)
        self.assertTrue(work_connection.closed)


if __name__ == "__main__":
    unittest.main()
