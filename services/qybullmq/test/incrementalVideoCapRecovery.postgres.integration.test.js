import assert from "node:assert/strict";
import test from "node:test";
import pg from "pg";

import {
  BUG035_RECOVERY_OPERATION_ID,
  classifyBug035RecoveryTarget,
  loadBug035RecoveryTargets,
  prepareBug035RecoveryTargets,
} from "../src/incrementalVideoCapRecovery.js";

const { Pool } = pg;
const connectionString = process.env.POSTGRES_TEST_URL;

test("BUG-035 recovery selects and reopens only its exact terminal Partial Run", {
  skip: !connectionString,
}, async () => {
  const pool = new Pool({ connectionString, max: 2 });
  const planId = "11111111-1111-4111-8111-111111111111";
  const runId = `incremental:${planId}`;
  const channelId = "UCbug035pg";
  const jobId = "incremental__UCbug035pg__20260817__clock_1__111111111111";
  const observationId = "22222222-2222-4222-8222-222222222222";
  try {
    await pool.query("DROP SCHEMA IF EXISTS crawler CASCADE");
    await pool.query("CREATE SCHEMA crawler");
    await pool.query(`
      CREATE TABLE crawler.channel_runs (
        run_id text PRIMARY KEY,
        channel_id text NOT NULL,
        plan_id uuid NOT NULL,
        plan_day date NOT NULL,
        status text NOT NULL,
        detail_status text NOT NULL,
        result_json jsonb NOT NULL,
        error_message text,
        finished_at timestamptz,
        updated_at timestamptz NOT NULL DEFAULT now()
      );
      CREATE TABLE crawler.crawl_observations (
        observation_id uuid PRIMARY KEY,
        run_id text NOT NULL,
        plan_day date NOT NULL,
        observation_kind text NOT NULL,
        outcome_reason_code text NOT NULL,
        kind_sequence bigint NOT NULL,
        result_summary_json jsonb NOT NULL
      );
      CREATE TABLE crawler.crawler_outbox (
        observation_id uuid PRIMARY KEY
      );
    `);
    await pool.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,plan_id,plan_day,status,detail_status,result_json,finished_at
       ) VALUES ($1,$2,$3,$4,'done','done',$5::jsonb,now())`,
      [
        runId,
        channelId,
        planId,
        "2026-08-17",
        JSON.stringify({
          job_id: jobId,
          domains: {
            about: { status: "not_due" },
            video: { status: "partial", observation_id: observationId },
            agent: { status: "not_due" },
          },
        }),
      ],
    );
    await pool.query(
      `INSERT INTO crawler.crawl_observations (
         observation_id,run_id,plan_day,observation_kind,outcome_reason_code,
         kind_sequence,result_summary_json
       ) VALUES ($1,$2,$3,'video','video_cycle_partial_complete',7,$4::jsonb)`,
      [
        observationId,
        runId,
        "2026-08-17",
        JSON.stringify({ discovery: { first_seen_count: 30, unresolved_count: 10 } }),
      ],
    );
    await pool.query(
      "INSERT INTO crawler.crawler_outbox (observation_id) VALUES ($1)",
      [observationId],
    );

    const [target] = await loadBug035RecoveryTargets(pool.query.bind(pool), {
      planDay: "2026-08-17",
    });
    assert.equal(target.run_id, runId);
    assert.equal(target.affected_count, 40);
    assert.equal(target.recovery_marker, null);

    const job = {
      id: jobId,
      name: "channel.incremental.plan",
      data: {
        job_id: jobId,
        plan_id: planId,
        channel_id: channelId,
        task_mask: { video: true },
      },
      attemptsMade: 1,
      async getState() { return "completed"; },
    };
    const inspection = await classifyBug035RecoveryTarget(target, job);
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await prepareBug035RecoveryTargets(client, [inspection], {
        now: new Date("2026-08-17T03:00:00.000Z"),
      });
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }

    const stored = await pool.query(
      `SELECT status,detail_status,finished_at,
              result_json #>> '{domains,video,status}' AS video_status,
              result_json #> $2::text[] AS marker
         FROM crawler.channel_runs
        WHERE run_id=$1`,
      [runId, ["controlled_recoveries", BUG035_RECOVERY_OPERATION_ID]],
    );
    assert.equal(stored.rows[0].status, "queued");
    assert.equal(stored.rows[0].detail_status, "pending");
    assert.equal(stored.rows[0].finished_at, null);
    assert.equal(stored.rows[0].video_status, "pending");
    assert.equal(stored.rows[0].marker.attempts_made_before, 1);
    assert.equal(stored.rows[0].marker.source_observation_id, observationId);

    const [resumed] = await loadBug035RecoveryTargets(pool.query.bind(pool), {
      planDay: "2026-08-17",
    });
    const classified = await classifyBug035RecoveryTarget(resumed, job);
    assert.equal(classified.action, "retry_prepared");

    const auditRows = await pool.query(
      "SELECT count(*)::int AS count FROM crawler.crawl_observations",
    );
    const outboxRows = await pool.query(
      "SELECT count(*)::int AS count FROM crawler.crawler_outbox",
    );
    assert.equal(auditRows.rows[0].count, 1);
    assert.equal(outboxRows.rows[0].count, 1);
  } finally {
    await pool.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    await pool.end();
  }
});
