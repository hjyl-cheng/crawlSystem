from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
import os
from threading import Barrier, BrokenBarrierError
import unittest
from uuid import uuid4

from feature_engine.applier import FeatureObservationApplier
from feature_engine.daily_plan_status_repair import repair_daily_plan_statuses
from feature_engine.events import AboutPayload, canonical_payload_hash
from feature_engine.scheduler import DailyPlanConfig, DailyScheduler


POSTGRES_TEST_URL = os.environ.get("FEATURE_ENGINE_POSTGRES_TEST_URL")


def observation_event(
    *,
    channel_id: str,
    observation_kind: str,
    payload: dict,
    observed_at: str,
    sequence: int = 1,
    outcome: str = "complete",
) -> dict:
    payload_hash = (
        AboutPayload.from_mapping(payload).facts_hash()
        if observation_kind == "about" and "failure_kind" not in payload
        else canonical_payload_hash(payload)
    )
    return {
        "event_id": str(uuid4()),
        "event_type": "crawler.observation.recorded",
        "event_version": 1,
        "observation_id": str(uuid4()),
        "channel_id": channel_id,
        "observation_kind": observation_kind,
        "kind_sequence": sequence,
        "observed_at": observed_at,
        "outcome": outcome,
        "crawler_version": "qy-v16-postgres-test",
        "payload_hash": payload_hash,
        "payload": payload,
    }


def high_activity_video_payload(observed_at: datetime) -> dict:
    first_seen = [
        {
            "video_id": f"daily-video-{index:02d}",
            "position": index + 1,
            "content_type": "video",
            "published_at": (
                observed_at - timedelta(hours=index + 1)
            ).isoformat().replace("+00:00", "Z"),
            "published_at_precision": "second",
        }
        for index in range(30)
    ]
    return {
        "discovery": {
            "outcome": "complete",
            "payload": {
                "pages": 1,
                "items": 30,
                "anchor_matched": False,
                "stop_reason": "list_end",
                "parse_gap_count": 0,
                "first_seen": first_seen,
                "first_seen_count": 30,
                "detail_success_count": 30,
                "detail_failure_count": 0,
            },
        },
        "recent_sampling": {
            "outcome": "complete",
            "payload": {
                "recent_count": 30,
                "stale_ratio": 0,
                "selected_count": 0,
                "success_count": 0,
                "failure_count": 0,
                "next_count": 0,
                "comparable_view_count": 0,
                "view_changed_count": 0,
                "view_delta_total": 0,
                "engagement_changed_count": 0,
            },
        },
    }


def catchup_limited_video_payload() -> dict:
    return {
        "discovery": {
            "outcome": "partial",
            "payload": {
                "pages": 2,
                "items": 3,
                "first_page_item_count": 2,
                "catch_up_item_count": 1,
                "anchor_matched": False,
                "stop_reason": "catchup_limit",
                "parse_gap_count": 0,
                "unclosed_video_ids": ["new-1", "new-2", "older-1"],
                "first_seen": [],
                "first_seen_count": 0,
                "detail_success_count": 0,
                "detail_failure_count": 0,
            },
        },
        "recent_sampling": {
            "outcome": "skipped",
            "payload": {"skipped_reason": "discovery_incomplete"},
        },
    }


def complete_agent_payload() -> dict:
    return {
        "output_hash": f"sha256:{'a' * 64}",
        "category_level_1": "news",
        "category_level_2": ["daily"],
        "tag_count": 10,
        "evidence_count": 10,
        "active_subscriber_ratio": 50,
        "fulfilled_plan_count": 1,
    }


class BarrierCursor:
    def __init__(self, cursor, state_barrier: Barrier) -> None:
        self._cursor = cursor
        self._state_barrier = state_barrier

    def __enter__(self):
        self._cursor.__enter__()
        return self

    def __exit__(self, *args):
        return self._cursor.__exit__(*args)

    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def execute(self, query, params=None):
        normalized = " ".join(str(query).split())
        if (
            "FROM feature_clock.channel_feature_state" in normalized
            and "FOR UPDATE" in normalized
        ):
            try:
                self._state_barrier.wait(timeout=0.5)
            except BrokenBarrierError:
                pass
        return self._cursor.execute(query, params)


class BarrierConnection:
    def __init__(self, connection, state_barrier: Barrier) -> None:
        self._connection = connection
        self._state_barrier = state_barrier

    def transaction(self):
        return self._connection.transaction()

    def cursor(self, *args, **kwargs):
        return BarrierCursor(
            self._connection.cursor(*args, **kwargs),
            self._state_barrier,
        )

    def close(self):
        return self._connection.close()


@unittest.skipUnless(POSTGRES_TEST_URL, "FEATURE_ENGINE_POSTGRES_TEST_URL is not set")
class FeatureEnginePostgresIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        import psycopg

        self.psycopg = psycopg
        self.channel_id = f"UCfeatureintegration{uuid4().hex}"
        self.channel_ids = [self.channel_id]

    def connect(self):
        return self.psycopg.connect(POSTGRES_TEST_URL, options="-c timezone=UTC")

    def test_database_exposes_exactly_three_clock_domains(self) -> None:
        with self.connect() as connection:
            clock_columns = {
                row[0]
                for row in connection.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema='feature_clock'
                      AND table_name='channel_clock_state'
                    """
                ).fetchall()
            }
            plan_mask_columns = {
                row[0]
                for row in connection.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema='feature_clock'
                      AND table_name='daily_channel_plans'
                      AND column_name LIKE 'run_%'
                    """
                ).fetchall()
            }

        self.assertTrue(
            {
                "about_due_at",
                "about_due_day",
                "about_tier",
                "video_due_at",
                "video_due_day",
                "video_tier",
                "agent_due_at",
                "agent_due_day",
                "agent_tier",
            }.issubset(clock_columns)
        )
        self.assertTrue(
            {
                "profile_due_at",
                "profile_due_day",
                "profile_tier",
                "profile_last_complete_at",
            }.isdisjoint(clock_columns)
        )
        self.assertEqual(plan_mask_columns, {"run_about", "run_video", "run_agent"})

    def tearDown(self) -> None:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                for table in (
                    "daily_plan_status_repair_audit",
                    "daily_channel_plans",
                    "clock_decision_log",
                    "crawler_event_inbox",
                    "channel_observation_checkpoints",
                    "channel_clock_state",
                    "channel_feature_state",
                ):
                    cursor.execute(
                        f"DELETE FROM feature_clock.{table} WHERE channel_id=ANY(%s)",
                        (self.channel_ids,),
                    )
                cursor.execute(
                    "DELETE FROM crawler.channels WHERE channel_id=ANY(%s)",
                    (self.channel_ids,),
                )

    def seed_clock(
        self,
        *,
        channel_id: str,
        due_at: datetime,
        estimated_request_cost: int = 0,
    ) -> None:
        later_at = due_at + timedelta(days=1)
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO feature_clock.channel_clock_state (
                      channel_id,
                      about_due_at,about_due_day,about_tier,
                      video_due_at,video_due_day,video_tier,
                      agent_due_at,agent_due_day,agent_tier,
                      channel_next_run_at,channel_next_run_day,
                      dispatch_slot,estimated_request_cost,policy_version,
                      feature_state_version,clock_version
                    ) VALUES (
                      %s,%s,%s,1,%s,%s,1,%s,%s,1,
                      %s,%s,0,%s,'v16-rule-1',0,1
                    )
                    """,
                    (
                        channel_id,
                        due_at,
                        due_at.date(),
                        later_at,
                        later_at.date(),
                        later_at,
                        later_at.date(),
                        due_at,
                        due_at.date(),
                        estimated_request_cost,
                    ),
                )

    def test_about_observation_creates_a_date_due_unassigned_plan(self) -> None:
        payload = {
            "subscriber_count": 1_000,
            "subscriber_count_status": "exact",
            "total_view_count": 50_000,
            "total_view_count_status": "exact",
            "total_video_count": 20,
            "total_video_count_status": "exact",
        }
        event = {
            "event_id": str(uuid4()),
            "event_type": "crawler.observation.recorded",
            "event_version": 1,
            "observation_id": str(uuid4()),
            "channel_id": self.channel_id,
            "observation_kind": "about",
            "kind_sequence": 1,
            "observed_at": "2026-07-20T13:25:40Z",
            "outcome": "complete",
            "crawler_version": "qy-v16-postgres-test",
            "payload_hash": AboutPayload.from_mapping(payload).facts_hash(),
            "payload": payload,
        }

        applied = FeatureObservationApplier(self.connect).apply_crawler_observation(event)
        scheduled = DailyScheduler(self.connect).run_batch(
            plan_day=date(2026, 7, 27),
            config=DailyPlanConfig(
                planner_config_version="postgres-integration-1",
                capacity_version="postgres-integration-1",
            ),
            channel_id=self.channel_id,
            now=datetime(2026, 7, 27, tzinfo=timezone.utc),
        )

        self.assertEqual(applied.status, "applied")
        self.assertEqual(len(scheduled.created), 1)
        self.assertEqual(
            scheduled.created[0].due_at,
            datetime(2026, 7, 27, 0, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(scheduled.created[0].due_day, date(2026, 7, 27))
        self.assertFalse(hasattr(scheduled.created[0], "scheduled_at"))

    def test_removed_channel_event_permanently_excludes_the_clock(self) -> None:
        due_at = datetime(2026, 7, 23, 0, 30, tzinfo=timezone.utc)
        self.seed_clock(channel_id=self.channel_id, due_at=due_at)
        payload = {
            "failure_kind": "channel_removed",
            "attempt_count": 1,
            "removed_reason": "community_guidelines",
        }
        removed = observation_event(
            channel_id=self.channel_id,
            observation_kind="about",
            payload=payload,
            observed_at="2026-07-23T03:24:09Z",
            outcome="failed",
        )

        applied = FeatureObservationApplier(self.connect).apply_crawler_observation(removed)
        scheduled = DailyScheduler(self.connect).run_batch(
            plan_day=due_at.date(),
            config=DailyPlanConfig(
                planner_config_version="postgres-integration-1",
                capacity_version="postgres-integration-1",
            ),
            channel_id=self.channel_id,
            now=due_at,
        )
        with self.connect() as connection:
            lifecycle = connection.execute(
                """
                SELECT lifecycle_status,removed_reason,removed_at
                FROM feature_clock.channel_clock_state
                WHERE channel_id=%s
                """,
                (self.channel_id,),
            ).fetchone()

        self.assertEqual(applied.status, "applied")
        self.assertEqual(lifecycle[0:2], ("removed", "community_guidelines"))
        self.assertEqual(lifecycle[2], datetime(2026, 7, 23, 3, 24, 9, tzinfo=timezone.utc))
        self.assertEqual(scheduled.created, ())

    def test_full_day_scheduler_holds_transaction_pool_safe_coordination_lock(
        self,
    ) -> None:
        self.seed_clock(
            channel_id=self.channel_id,
            due_at=datetime(2026, 7, 21, 0, 30, tzinfo=timezone.utc),
        )

        result = DailyScheduler(self.connect).run_day(
            plan_day=date(2026, 7, 21),
            config=DailyPlanConfig(
                planner_config_version="postgres-integration-1",
                capacity_version="postgres-integration-1",
            ),
            batch_size=1,
            max_plans=1,
            channel_id=self.channel_id,
            now=datetime(2026, 7, 21, tzinfo=timezone.utc),
        )

        self.assertEqual(result.planned, 1)
        self.assertEqual(result.batches, 1)

        with self.connect() as connection:
            plan = connection.execute(
                """
                SELECT channel_id,status
                FROM feature_clock.daily_channel_plans
                WHERE channel_id=%s
                """,
                (self.channel_id,),
            ).fetchone()
        self.assertEqual(plan, (self.channel_id, "planned"))

    def test_video_activity_recalculates_the_first_about_and_keeps_video_at_three_days(
        self,
    ) -> None:
        about_at = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
        video_at = about_at + timedelta(minutes=1)
        about = observation_event(
            channel_id=self.channel_id,
            observation_kind="about",
            observed_at=about_at.isoformat().replace("+00:00", "Z"),
            payload={
                "subscriber_count": 9_400,
                "subscriber_count_status": "exact",
                "total_view_count": 4_714_978,
                "total_view_count_status": "exact",
                "total_video_count": 8_750,
                "total_video_count_status": "exact",
            },
        )
        video = observation_event(
            channel_id=self.channel_id,
            observation_kind="video",
            observed_at=video_at.isoformat().replace("+00:00", "Z"),
            payload=high_activity_video_payload(video_at),
        )
        applier = FeatureObservationApplier(self.connect)

        applier.apply_crawler_observation(about)
        with self.connect() as connection:
            initial_about_tier = connection.execute(
                "SELECT about_tier FROM feature_clock.channel_clock_state WHERE channel_id=%s",
                (self.channel_id,),
            ).fetchone()[0]
        applier.apply_crawler_observation(video)

        with self.connect() as connection:
            state = connection.execute(
                """
                SELECT last_subscriber_count,about_metric_confidence,
                       recent30_video_count,cardinality(recent_publish_interval_days),
                       state_version
                FROM feature_clock.channel_feature_state
                WHERE channel_id=%s
                """,
                (self.channel_id,),
            ).fetchone()
            clock = connection.execute(
                """
                SELECT about_due_at,about_due_day,about_tier,
                       video_due_at,video_due_day,video_tier,policy_version
                FROM feature_clock.channel_clock_state
                WHERE channel_id=%s
                """,
                (self.channel_id,),
            ).fetchone()
            about_decision = connection.execute(
                """
                SELECT reason_codes,feature_summary_json
                FROM feature_clock.clock_decision_log
                WHERE channel_id=%s AND clock_kind='about'
                ORDER BY decided_at DESC,decision_id DESC
                LIMIT 1
                """,
                (self.channel_id,),
            ).fetchone()

        self.assertEqual(initial_about_tier, 7)
        self.assertEqual(state, (9_400, 1.0, 30, 29, 2))
        self.assertEqual(
            clock[:6],
            (
                datetime(2026, 7, 21, tzinfo=timezone.utc),
                date(2026, 7, 21),
                1,
                datetime(2026, 7, 23, tzinfo=timezone.utc),
                date(2026, 7, 23),
                3,
            ),
        )
        self.assertEqual(clock[6], "v16-rule-7")
        self.assertIn("about_cold_start_cadence_1d", about_decision[0])
        self.assertIn("video_activity_recalculation", about_decision[0])
        self.assertTrue(about_decision[1]["recalculated_after_video_observation"])

    def test_late_video_recalculates_about_but_not_semantic_agent_clock(self) -> None:
        base_at = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
        events = (
            observation_event(
                channel_id=self.channel_id,
                observation_kind="about",
                observed_at=base_at.isoformat().replace("+00:00", "Z"),
                payload={
                    "subscriber_count": 109_000,
                    "subscriber_count_status": "exact",
                    "total_view_count": 10_000_000,
                    "total_view_count_status": "exact",
                    "total_video_count": 1_000,
                    "total_video_count_status": "exact",
                },
            ),
            observation_event(
                channel_id=self.channel_id,
                observation_kind="agent",
                observed_at=(base_at + timedelta(seconds=1)).isoformat().replace(
                    "+00:00", "Z"
                ),
                payload=complete_agent_payload(),
            ),
        )
        applier = FeatureObservationApplier(self.connect)
        for event in events:
            applier.apply_crawler_observation(event)

        with self.connect() as connection:
            before_video = connection.execute(
                """
                SELECT about_tier,agent_tier
                FROM feature_clock.channel_clock_state
                WHERE channel_id=%s
                """,
                (self.channel_id,),
            ).fetchone()

        video_at = base_at + timedelta(seconds=2)
        applier.apply_crawler_observation(
            observation_event(
                channel_id=self.channel_id,
                observation_kind="video",
                observed_at=video_at.isoformat().replace("+00:00", "Z"),
                payload=high_activity_video_payload(video_at),
            )
        )

        with self.connect() as connection:
            clock = connection.execute(
                """
                SELECT about_tier,video_tier,agent_tier,policy_version
                FROM feature_clock.channel_clock_state
                WHERE channel_id=%s
                """,
                (self.channel_id,),
            ).fetchone()
            recalculated_kinds = {
                row[0]
                for row in connection.execute(
                    """
                    SELECT clock_kind
                    FROM feature_clock.clock_decision_log
                    WHERE channel_id=%s
                      AND 'cross_domain_cold_start_recalculation'=ANY(reason_codes)
                    """,
                    (self.channel_id,),
                ).fetchall()
            }

        self.assertEqual(before_video, (7, 180))
        self.assertEqual(clock, (1, 3, 180, "v16-rule-7"))
        self.assertEqual(recalculated_kinds, {"about"})

    def test_about_slowdown_persists_only_the_next_tier(self) -> None:
        first_at = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
        applier = FeatureObservationApplier(self.connect)
        first_payload = {
            "subscriber_count": 10_000,
            "subscriber_count_status": "exact",
            "total_view_count": 1_000_000,
            "total_view_count_status": "exact",
            "total_video_count": 100,
            "total_video_count_status": "exact",
        }
        applier.apply_crawler_observation(
            observation_event(
                channel_id=self.channel_id,
                observation_kind="about",
                observed_at=first_at.isoformat().replace("+00:00", "Z"),
                payload=first_payload,
            )
        )
        forced_due_at = datetime(2026, 7, 21, tzinfo=timezone.utc)
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE feature_clock.channel_clock_state
                SET about_due_at=%s,about_due_day=%s,about_tier=1,
                    channel_next_run_at=LEAST(%s,video_due_at,agent_due_at),
                    channel_next_run_day=LEAST(%s,video_due_day,agent_due_day)
                WHERE channel_id=%s
                """,
                (
                    forced_due_at,
                    forced_due_at.date(),
                    forced_due_at,
                    forced_due_at.date(),
                    self.channel_id,
                ),
            )

        second_payload = {
            **first_payload,
            "total_view_count": 1_000_001,
        }
        applier.apply_crawler_observation(
            observation_event(
                channel_id=self.channel_id,
                observation_kind="about",
                observed_at=(first_at + timedelta(days=1)).isoformat().replace(
                    "+00:00", "Z"
                ),
                payload=second_payload,
                sequence=2,
            )
        )

        with self.connect() as connection:
            clock = connection.execute(
                """
                SELECT about_tier,about_due_day
                FROM feature_clock.channel_clock_state
                WHERE channel_id=%s
                """,
                (self.channel_id,),
            ).fetchone()
            reasons = connection.execute(
                """
                SELECT reason_codes
                FROM feature_clock.clock_decision_log
                WHERE channel_id=%s AND clock_kind='about'
                ORDER BY decided_at DESC,decision_id DESC
                LIMIT 1
                """,
                (self.channel_id,),
            ).fetchone()[0]

        self.assertEqual(clock, (2, date(2026, 7, 23)))
        self.assertIn("about_slowdown_one_tier", reasons)

    def test_legacy_clock_time_no_longer_has_a_safe_window_constraint(self) -> None:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      feature_clock.normalize_clock_due_at(
                        '2026-07-20T00:10:25Z'::timestamptz
                      ),
                      feature_clock.normalize_clock_due_at(
                        '2026-07-20T21:30:00Z'::timestamptz
                      ),
                      feature_clock.normalize_clock_due_at(
                        '2026-07-20T23:50:25Z'::timestamptz
                      )
                    """
                )
                early, boundary, late = cursor.fetchone()

        self.assertEqual(
            early,
            datetime(2026, 7, 20, 0, 40, 25, tzinfo=timezone.utc),
        )
        self.assertEqual(
            boundary,
            datetime(2026, 7, 21, 0, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(
            late,
            datetime(2026, 7, 21, 2, 50, 25, tzinfo=timezone.utc),
        )

        self.seed_clock(
            channel_id=self.channel_id,
            due_at=datetime(2026, 7, 20, 23, 50, tzinfo=timezone.utc),
        )

    def test_dispatch_window_constraint_preserves_legacy_terminal_history(self) -> None:
        insert_sql = """
            INSERT INTO feature_clock.daily_channel_plans (
              plan_id,plan_day,channel_id,due_day,due_at,eligible_at,scheduled_at,
              execution_deadline_at,
              run_about,run_video,run_agent,dispatch_slot,
              capacity_factor,player_cap,next_cap,estimated_request_cost,
              source_clock_version,policy_version,planner_config_version,
              capacity_version,status
            ) VALUES (
              %s,'2026-07-20',%s,'2026-07-20','2026-07-20T00:00:00Z',
              '2026-07-20T00:30:00Z','2026-07-20T23:27:00Z',
              '2026-07-21T23:27:00Z',
              false,true,false,0,1,20,8,1,1,'v16-rule-1',
              'postgres-integration-1','postgres-integration-1',%s
            )
        """
        with self.connect() as connection:
            connection.execute(insert_sql, (str(uuid4()), self.channel_id, "succeeded"))

        with self.assertRaises(self.psycopg.errors.CheckViolation):
            with self.connect() as connection:
                connection.execute(insert_sql, (str(uuid4()), self.channel_id, "planned"))

    def test_scheduler_does_not_apply_static_day_capacity(self) -> None:
        due_at = datetime(2026, 7, 28, 0, 30, tzinfo=timezone.utc)
        channel_ids = [f"UCcapacity{uuid4().hex}" for _ in range(3)]
        self.channel_ids.extend(channel_ids)
        for channel_id in channel_ids:
            self.seed_clock(
                channel_id=channel_id,
                due_at=due_at,
                estimated_request_cost=6,
            )

        result = DailyScheduler(self.connect).run_batch(
            plan_day=date(2026, 7, 28),
            config=DailyPlanConfig(
                planner_config_version="postgres-integration-1",
                capacity_version="postgres-integration-1",
            ),
            batch_size=10,
            now=due_at,
        )

        self.assertEqual(len(result.created), 3)
        self.assertTrue(all(not hasattr(plan, "scheduled_at") for plan in result.created))
        self.assertFalse(result.capacity_exhausted)

    def test_scheduler_does_not_roll_an_overdue_clock_forward(self) -> None:
        due_at = datetime(2026, 7, 20, 13, 25, 40, tzinfo=timezone.utc)
        self.seed_clock(channel_id=self.channel_id, due_at=due_at)

        result = DailyScheduler(self.connect).run_batch(
            plan_day=date(2026, 7, 22),
            config=DailyPlanConfig(
                planner_config_version="postgres-integration-1",
                capacity_version="postgres-integration-1",
            ),
            channel_id=self.channel_id,
            now=datetime(2026, 7, 22, 0, 30, tzinfo=timezone.utc),
        )

        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT about_due_at,channel_next_run_at,clock_version
                    FROM feature_clock.channel_clock_state
                    WHERE channel_id=%s
                    """,
                    (self.channel_id,),
                )
                stored_due_at, stored_next_run_at, clock_version = cursor.fetchone()

        self.assertEqual(len(result.created), 1)
        self.assertEqual(result.created[0].due_day, due_at.date())
        self.assertEqual(
            result.created[0].due_at,
            datetime.combine(due_at.date(), datetime.min.time(), tzinfo=timezone.utc),
        )
        self.assertEqual(stored_due_at, due_at)
        self.assertEqual(stored_next_run_at, due_at)
        self.assertEqual(clock_version, 1)

    def test_scheduler_creates_same_day_supplement_for_newly_due_domain(self) -> None:
        plan_day = date(2026, 7, 23)
        due_at = datetime(2026, 7, 23, 1, 0, tzinfo=timezone.utc)
        first_plan_id = str(uuid4())
        self.seed_clock(channel_id=self.channel_id, due_at=due_at)
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE feature_clock.channel_clock_state
                    SET about_due_at=%s,about_due_day=%s,
                        channel_next_run_at=%s,channel_next_run_day=%s,
                        clock_version=2
                    WHERE channel_id=%s
                    """,
                    (due_at, plan_day, due_at, plan_day, self.channel_id),
                )
                cursor.execute(
                    """
                    INSERT INTO feature_clock.daily_channel_plans (
                      plan_id,plan_day,channel_id,due_day,due_at,eligible_at,
                      run_about,run_video,run_agent,dispatch_slot,
                      capacity_factor,player_cap,next_cap,estimated_request_cost,
                      source_clock_version,policy_version,planner_config_version,
                      capacity_version,status,completed_at
                    ) VALUES (
                      %s,%s,%s,%s,%s,%s,
                      false,true,false,0,
                      1,20,8,1,1,'v16-rule-1','same-day-first','same-day-first',
                      'succeeded',now()
                    )
                    """,
                    (
                        first_plan_id,
                        plan_day,
                        self.channel_id,
                        plan_day,
                        due_at,
                        due_at,
                    ),
                )

        result = DailyScheduler(self.connect).run_batch(
            plan_day=plan_day,
            config=DailyPlanConfig(
                planner_config_version="postgres-integration-1",
                capacity_version="postgres-integration-1",
            ),
            channel_id=self.channel_id,
            now=datetime(2026, 7, 23, 2, tzinfo=timezone.utc),
        )

        with self.connect() as connection:
            plans = connection.execute(
                """
                SELECT run_about,run_video,run_agent,status
                FROM feature_clock.daily_channel_plans
                WHERE channel_id=%s AND plan_day=%s
                ORDER BY created_at,plan_id
                """,
                (self.channel_id, plan_day),
            ).fetchall()

        self.assertEqual(len(result.created), 1)
        self.assertNotEqual(result.created[0].plan_id, first_plan_id)
        self.assertEqual(
            plans,
            [
                (False, True, False, "succeeded"),
                (True, False, False, "planned"),
            ],
        )

    def test_scheduler_replaces_stale_unassigned_plan_with_coalesced_plan(self) -> None:
        old_plan_id = str(uuid4())
        self.seed_clock(
            channel_id=self.channel_id,
            due_at=datetime(2026, 7, 20, 18, 15, tzinfo=timezone.utc),
        )
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE feature_clock.channel_clock_state
                    SET about_due_at='2026-07-22T19:45:00Z',about_due_day='2026-07-22',
                        video_due_at='2026-07-23T08:00:00Z',video_due_day='2026-07-23',
                        agent_due_at='2026-07-23T09:00:00Z',agent_due_day='2026-07-23',
                        channel_next_run_at='2026-07-22T19:45:00Z',
                        channel_next_run_day='2026-07-22'
                    WHERE channel_id=%s
                    """,
                    (self.channel_id,),
                )
                cursor.execute(
                    """
                    INSERT INTO feature_clock.daily_channel_plans (
                      plan_id,plan_day,channel_id,due_day,due_at,eligible_at,
                      run_about,run_video,run_agent,dispatch_slot,
                      capacity_factor,player_cap,next_cap,estimated_request_cost,
                      source_clock_version,policy_version,planner_config_version,
                      capacity_version,status
                    ) VALUES (
                      %s,'2026-07-21',%s,'2026-07-20','2026-07-20T00:00:00Z',
                      '2026-07-21T00:30:00Z',true,false,false,0,
                      1,20,8,1,1,'v16-rule-1','stale-plan-1','stale-capacity-1','planned'
                    )
                    """,
                    (old_plan_id, self.channel_id),
                )

        result = DailyScheduler(self.connect).run_batch(
            plan_day=date(2026, 7, 22),
            config=DailyPlanConfig(
                planner_config_version="postgres-integration-1",
                capacity_version="postgres-integration-1",
            ),
            channel_id=self.channel_id,
            now=datetime(2026, 7, 22, 10, tzinfo=timezone.utc),
        )

        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT plan_day,status,error_code,scheduled_at,
                           run_about,run_video,run_agent
                    FROM feature_clock.daily_channel_plans
                    WHERE channel_id=%s
                    ORDER BY plan_day
                    """,
                    (self.channel_id,),
                )
                plans = cursor.fetchall()
                cursor.execute(
                    """
                    SELECT count(*)
                    FROM feature_clock.dispatch_outbox outbox
                    JOIN feature_clock.daily_channel_plans plan USING (plan_id)
                    WHERE plan.channel_id=%s
                    """,
                    (self.channel_id,),
                )
                outbox_count = cursor.fetchone()[0]

        self.assertEqual(len(result.created), 1)
        self.assertEqual(result.created[0].due_day, date(2026, 7, 22))
        self.assertTrue(result.created[0].run_about)
        self.assertFalse(result.created[0].run_video)
        self.assertFalse(result.created[0].run_agent)
        self.assertEqual(
            plans,
            [
                (
                    date(2026, 7, 21),
                    "cancelled",
                    "superseded_by_daily_plan",
                    None,
                    True,
                    False,
                    False,
                ),
                (
                    date(2026, 7, 22),
                    "planned",
                    None,
                    None,
                    True,
                    False,
                    False,
                ),
            ],
        )
        self.assertEqual(outbox_count, 0)

    def test_about_only_plan_closes_after_one_about_observation(self) -> None:
        plan_id = str(uuid4())
        observed_at = "2026-07-20T13:25:40Z"
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO feature_clock.daily_channel_plans (
                      plan_id,plan_day,channel_id,due_day,due_at,eligible_at,scheduled_at,
                      execution_deadline_at,
                      run_about,run_video,run_agent,dispatch_slot,
                      capacity_factor,player_cap,next_cap,estimated_request_cost,
                      source_clock_version,policy_version,planner_config_version,
                      capacity_version,status
                    ) VALUES (
                      %s,'2026-07-20',%s,'2026-07-20',%s,%s,%s,
                      %s::timestamptz + interval '1 day',
                      true,false,false,0,1,20,8,1,1,'v16-rule-1',
                      'postgres-integration-1','postgres-integration-1','dispatched'
                    )
                    """,
                    (
                        plan_id,self.channel_id,observed_at,observed_at,
                        observed_at,observed_at,
                    ),
                )

        about_payload = {
            "subscriber_count": 1_000,
            "subscriber_count_status": "exact",
            "total_view_count": 50_000,
            "total_view_count_status": "exact",
            "total_video_count": 20,
            "total_video_count_status": "exact",
        }
        about_event = {
            "event_id": str(uuid4()),
            "event_type": "crawler.observation.recorded",
            "event_version": 1,
            "observation_id": str(uuid4()),
            "plan_id": plan_id,
            "channel_id": self.channel_id,
            "observation_kind": "about",
            "kind_sequence": 1,
            "observed_at": observed_at,
            "outcome": "complete",
            "crawler_version": "qy-v16-postgres-test",
            "payload_hash": AboutPayload.from_mapping(about_payload).facts_hash(),
            "payload": about_payload,
        }
        scheduler_config = DailyPlanConfig(
            planner_config_version="postgres-integration-1",
            capacity_version="postgres-integration-1",
        )
        applier = FeatureObservationApplier(self.connect)

        about_result = applier.apply_crawler_observation(about_event)
        released = DailyScheduler(self.connect).run_batch(
            plan_day=date(2026, 7, 27),
            config=scheduler_config,
            channel_id=self.channel_id,
            now=datetime(2026, 7, 27, tzinfo=timezone.utc),
        )
        with self.connect() as connection:
            plan_status = connection.execute(
                "SELECT status FROM feature_clock.daily_channel_plans WHERE plan_id=%s",
                (plan_id,),
            ).fetchone()[0]

        self.assertEqual(about_result.status, "applied")
        self.assertEqual(plan_status, "succeeded")
        self.assertEqual(len(released.created), 1)

    def test_later_complete_about_observation_recovers_failed_or_partial_plan(self) -> None:
        about_payload = {
            "subscriber_count": 1_000,
            "subscriber_count_status": "exact",
            "total_view_count": 50_000,
            "total_view_count_status": "exact",
            "total_video_count": 20,
            "total_video_count_status": "exact",
        }
        partial_about_payload = {
            **about_payload,
            "subscriber_count": None,
            "subscriber_count_status": "unavailable",
        }
        applier = FeatureObservationApplier(self.connect)

        for initial_outcome in ("failed", "partial"):
            with self.subTest(initial_outcome=initial_outcome):
                channel_id = f"UCplanrecovery{uuid4().hex}"
                self.channel_ids.append(channel_id)
                plan_id = str(uuid4())
                with self.connect() as connection:
                    connection.execute(
                        """
                        INSERT INTO feature_clock.daily_channel_plans (
                          plan_id,plan_day,channel_id,due_day,due_at,eligible_at,
                          scheduled_at,execution_deadline_at,
                          run_about,run_video,run_agent,dispatch_slot,
                          capacity_factor,player_cap,next_cap,estimated_request_cost,
                          source_clock_version,policy_version,planner_config_version,
                          capacity_version,status
                        ) VALUES (
                          %s,'2026-08-03',%s,'2026-08-03','2026-08-03T00:00:00Z',
                          '2026-08-03T00:30:00Z','2026-08-03T00:40:00Z',
                          '2026-08-04T00:40:00Z',true,false,false,0,
                          1,20,8,1,1,'v16-rule-6',
                          'postgres-integration-1','postgres-integration-1','running'
                        )
                        """,
                        (plan_id, channel_id),
                    )

                first_payload = (
                    {"failure_kind": "transient_upstream", "attempt_count": 1}
                    if initial_outcome == "failed"
                    else partial_about_payload
                )
                first = observation_event(
                    channel_id=channel_id,
                    observation_kind="about",
                    payload=first_payload,
                    observed_at="2026-08-03T00:41:00Z",
                    sequence=1,
                    outcome=initial_outcome,
                )
                first["plan_id"] = plan_id
                second = observation_event(
                    channel_id=channel_id,
                    observation_kind="about",
                    payload=about_payload,
                    observed_at="2026-08-03T00:46:00Z",
                    sequence=2,
                    outcome="complete",
                )
                second["plan_id"] = plan_id

                self.assertEqual(applier.apply_crawler_observation(first).status, "applied")
                with self.connect() as connection:
                    initial_status = connection.execute(
                        """
                        SELECT status,error_code
                        FROM feature_clock.daily_channel_plans
                        WHERE plan_id=%s
                        """,
                        (plan_id,),
                    ).fetchone()
                self.assertEqual(
                    initial_status,
                    (
                        initial_outcome,
                        f"crawler_observation_{initial_outcome}",
                    ),
                )

                self.assertEqual(applier.apply_crawler_observation(second).status, "applied")
                with self.connect() as connection:
                    plan = connection.execute(
                        """
                        SELECT status,error_code,finished_at,completed_at
                        FROM feature_clock.daily_channel_plans
                        WHERE plan_id=%s
                        """,
                        (plan_id,),
                    ).fetchone()
                    outcomes = connection.execute(
                        """
                        SELECT kind_sequence,outcome,status
                        FROM feature_clock.crawler_event_inbox
                        WHERE plan_id=%s
                        ORDER BY kind_sequence
                        """,
                        (plan_id,),
                    ).fetchall()

                self.assertEqual(plan[0:2], ("succeeded", None))
                self.assertEqual(
                    plan[2],
                    datetime(2026, 8, 3, 0, 46, tzinfo=timezone.utc),
                )
                self.assertIsNotNone(plan[3])
                self.assertEqual(
                    outcomes,
                    [(1, initial_outcome, "applied"), (2, "complete", "applied")],
                )

    def test_guarded_repair_recovers_only_confirmed_stale_daily_plan(self) -> None:
        channel_id = f"UCplanrepair{uuid4().hex}"
        consistent_channel_id = f"UCplanconsistent{uuid4().hex}"
        self.channel_ids.extend((channel_id, consistent_channel_id))
        plan_id = str(uuid4())
        consistent_plan_id = str(uuid4())
        plan_insert = """
            INSERT INTO feature_clock.daily_channel_plans (
              plan_id,plan_day,channel_id,due_day,due_at,eligible_at,
              scheduled_at,execution_deadline_at,
              run_about,run_video,run_agent,dispatch_slot,
              capacity_factor,player_cap,next_cap,estimated_request_cost,
              source_clock_version,policy_version,planner_config_version,
              capacity_version,status
            ) VALUES (
              %s,'2030-08-03',%s,'2030-08-03','2030-08-03T00:00:00Z',
              '2030-08-03T00:30:00Z','2030-08-03T00:40:00Z',
              '2030-08-04T00:40:00Z',true,false,false,0,
              1,20,8,1,1,'v16-rule-6',
              'postgres-integration-1','postgres-integration-1','running'
            )
        """
        with self.connect() as connection:
            connection.execute(plan_insert, (plan_id, channel_id))
            connection.execute(
                plan_insert,
                (consistent_plan_id, consistent_channel_id),
            )

        failed = observation_event(
            channel_id=channel_id,
            observation_kind="about",
            payload={"failure_kind": "transient_upstream", "attempt_count": 1},
            observed_at="2030-08-03T01:00:00Z",
            sequence=1,
            outcome="failed",
        )
        failed["plan_id"] = plan_id
        complete_payload = {
            "subscriber_count": 1_000,
            "subscriber_count_status": "exact",
            "total_view_count": 50_000,
            "total_view_count_status": "exact",
            "total_video_count": 20,
            "total_video_count_status": "exact",
        }
        complete = observation_event(
            channel_id=channel_id,
            observation_kind="about",
            payload=complete_payload,
            observed_at="2030-08-03T01:05:00Z",
            sequence=2,
            outcome="complete",
        )
        complete["plan_id"] = plan_id
        partial_payload = {
            **complete_payload,
            "subscriber_count": None,
            "subscriber_count_status": "unavailable",
        }
        consistent_partial = observation_event(
            channel_id=consistent_channel_id,
            observation_kind="about",
            payload=partial_payload,
            observed_at="2030-08-03T01:02:00Z",
            sequence=1,
            outcome="partial",
        )
        consistent_partial["plan_id"] = consistent_plan_id
        applier = FeatureObservationApplier(self.connect)
        self.assertEqual(applier.apply_crawler_observation(failed).status, "applied")
        self.assertEqual(applier.apply_crawler_observation(complete).status, "applied")
        self.assertEqual(
            applier.apply_crawler_observation(consistent_partial).status,
            "applied",
        )

        with self.connect() as connection:
            connection.execute(
                """
                UPDATE feature_clock.daily_channel_plans
                SET status='failed',error_code='crawler_observation_failed',
                    finished_at='2030-08-03T01:00:00Z'
                WHERE plan_id=%s
                """,
                (plan_id,),
            )

        report = repair_daily_plan_statuses(
            self.connect,
            from_day=date(2030, 8, 3),
            to_day=date(2030, 8, 3),
            apply=False,
            now=datetime(2026, 8, 3, 3, tzinfo=timezone.utc),
        )
        self.assertEqual(report.examined_plans, 2)
        self.assertEqual(report.candidate_plans, 1)
        self.assertEqual(report.repairable_plans, 1)
        self.assertEqual(report.consistent_plans, 1)
        self.assertEqual(report.blocked_plans, 0)
        self.assertTrue(report.can_apply)
        self.assertIsNotNone(report.confirmation)
        repaired_item = next(item for item in report.items if item.plan_id == plan_id)
        consistent_item = next(
            item for item in report.items if item.plan_id == consistent_plan_id
        )
        self.assertEqual(repaired_item.classification, "repairable")
        self.assertEqual(repaired_item.target_status, "succeeded")
        self.assertEqual(consistent_item.classification, "consistent")
        self.assertEqual(consistent_item.target_status, "partial")

        with self.assertRaisesRegex(RuntimeError, "confirmation does not match"):
            repair_daily_plan_statuses(
                self.connect,
                from_day=date(2030, 8, 3),
                to_day=date(2030, 8, 3),
                apply=True,
                confirm=f"sha256:{'0' * 64}",
                operator="integration-test",
                reason="verify guarded stale Plan recovery",
            )

        applied = repair_daily_plan_statuses(
            self.connect,
            from_day=date(2030, 8, 3),
            to_day=date(2030, 8, 3),
            apply=True,
            confirm=report.confirmation,
            operator="integration-test",
            reason="verify guarded stale Plan recovery",
        )
        with self.connect() as connection:
            plan = connection.execute(
                """
                SELECT status,error_code,finished_at
                FROM feature_clock.daily_channel_plans
                WHERE plan_id=%s
                """,
                (plan_id,),
            ).fetchone()
            audit = connection.execute(
                """
                SELECT previous_status,repaired_status,confirmation,
                       latest_outcomes_json,operator,reason
                FROM feature_clock.daily_plan_status_repair_audit
                WHERE plan_id=%s
                """,
                (plan_id,),
            ).fetchone()
            consistent_plan = connection.execute(
                """
                SELECT status,error_code
                FROM feature_clock.daily_channel_plans
                WHERE plan_id=%s
                """,
                (consistent_plan_id,),
            ).fetchone()
            consistent_audit_count = connection.execute(
                """
                SELECT count(*)
                FROM feature_clock.daily_plan_status_repair_audit
                WHERE plan_id=%s
                """,
                (consistent_plan_id,),
            ).fetchone()[0]

        self.assertTrue(applied.applied)
        self.assertEqual(applied.repaired_plans, 1)
        self.assertIsNotNone(applied.repair_batch_id)
        self.assertEqual(
            plan,
            (
                "succeeded",
                None,
                datetime(2030, 8, 3, 1, 5, tzinfo=timezone.utc),
            ),
        )
        self.assertEqual(audit[0:3], ("failed", "succeeded", report.confirmation))
        self.assertEqual(
            audit[3],
            [
                {
                    "observation_kind": "about",
                    "kind_sequence": 2,
                    "outcome": "complete",
                    "observed_at": "2030-08-03T01:05:00+00:00",
                }
            ],
        )
        self.assertEqual(audit[4:6], ("integration-test", "verify guarded stale Plan recovery"))
        self.assertEqual(
            consistent_plan,
            ("partial", "crawler_observation_partial"),
        )
        self.assertEqual(consistent_audit_count, 0)

    def test_catchup_limited_video_closes_plan_without_advancing_complete_state(self) -> None:
        plan_id = str(uuid4())
        observed_at = "2026-07-31T00:38:18Z"
        previous_complete_at = datetime(2026, 7, 28, tzinfo=timezone.utc)
        due_at = datetime(2026, 7, 31, 0, 30, tzinfo=timezone.utc)
        self.seed_clock(channel_id=self.channel_id, due_at=due_at)
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE feature_clock.channel_clock_state
                    SET video_due_at=%s,video_due_day=%s,video_tier=3,
                        video_last_complete_at=%s,video_last_outcome='complete'
                    WHERE channel_id=%s
                    """,
                    (due_at, due_at.date(), previous_complete_at, self.channel_id),
                )
                cursor.execute(
                    """
                    INSERT INTO feature_clock.channel_feature_state (
                      channel_id,last_discovery_observed_at,last_complete_discovery_at,
                      last_recent_sampling_at,recent30_video_count,last_recent_sample_count,
                      feature_confidence,state_version
                    ) VALUES (%s,%s,%s,%s,8,4,1,4)
                    """,
                    (
                        self.channel_id,
                        previous_complete_at,
                        previous_complete_at,
                        previous_complete_at,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO feature_clock.daily_channel_plans (
                      plan_id,plan_day,channel_id,due_day,due_at,eligible_at,scheduled_at,
                      execution_deadline_at,run_about,run_video,run_agent,dispatch_slot,
                      capacity_factor,player_cap,next_cap,estimated_request_cost,
                      source_clock_version,policy_version,planner_config_version,
                      capacity_version,status
                    ) VALUES (
                      %s,'2026-07-31',%s,'2026-07-31',%s,%s,%s,
                      %s::timestamptz + interval '1 day',
                      false,true,false,0,1,20,8,1,1,'v16-rule-1',
                      'postgres-integration-1','postgres-integration-1','running'
                    )
                    """,
                    (plan_id, self.channel_id, due_at, due_at, due_at, due_at),
                )

        event = observation_event(
            channel_id=self.channel_id,
            observation_kind="video",
            payload=catchup_limited_video_payload(),
            observed_at=observed_at,
            outcome="partial",
        )
        event["plan_id"] = plan_id

        result = FeatureObservationApplier(self.connect).apply_crawler_observation(event)

        with self.connect() as connection:
            plan = connection.execute(
                """
                SELECT status,error_code,finished_at IS NOT NULL
                FROM feature_clock.daily_channel_plans
                WHERE plan_id=%s
                """,
                (plan_id,),
            ).fetchone()
            inbox = connection.execute(
                """
                SELECT outcome,status
                FROM feature_clock.crawler_event_inbox
                WHERE event_id=%s
                """,
                (event["event_id"],),
            ).fetchone()
            feature_state = connection.execute(
                """
                SELECT last_discovery_observed_at,last_complete_discovery_at,
                       last_recent_sampling_at,recent30_video_count,last_recent_sample_count
                FROM feature_clock.channel_feature_state
                WHERE channel_id=%s
                """,
                (self.channel_id,),
            ).fetchone()
            clock = connection.execute(
                """
                SELECT video_due_day,video_tier,video_last_complete_at,video_last_outcome
                FROM feature_clock.channel_clock_state
                WHERE channel_id=%s
                """,
                (self.channel_id,),
            ).fetchone()

        self.assertEqual(result.status, "applied")
        self.assertEqual(plan, ("partial", "crawler_observation_partial", True))
        self.assertEqual(inbox, ("partial", "applied"))
        self.assertEqual(
            feature_state,
            (
                datetime(2026, 7, 31, 0, 38, 18, tzinfo=timezone.utc),
                previous_complete_at,
                previous_complete_at,
                8,
                4,
            ),
        )
        self.assertEqual(
            clock,
            (date(2026, 8, 3), 3, previous_complete_at, "partial"),
        )

if __name__ == "__main__":
    unittest.main()
