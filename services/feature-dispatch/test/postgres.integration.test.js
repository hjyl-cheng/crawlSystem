import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import {
  DispatchOutboxPublisher,
  PostgresDispatchOutboxStore,
} from "../src/dispatchOutboxPublisher.js";
import { PostgresDynamicDispatchStore } from "../src/dynamicDispatcher.js";
import { dispatchPayloadHash } from "../src/dispatchTransport.js";

const { Pool } = pg;
const integrationUrl = process.env.FEATURE_DISPATCH_POSTGRES_TEST_URL;

function fixture(channelId, scheduledAt) {
  const planId = randomUUID();
  const dispatchEventId = randomUUID();
  const suffix = randomUUID().replaceAll("-", "");
  const payload = {
    schema_version: 5,
    dispatch_generation: 1,
    job_id: `incremental__${channelId}__clock_1__${suffix}`,
    plan_id: planId,
    plan_mode: "standard",
    plan_day: scheduledAt.toISOString().slice(0, 10),
    scheduled_at: scheduledAt.toISOString(),
    channel_id: channelId,
    task_mask: { about: true, video: false, agent: false },
    capacity: { factor: 1, player_cap: 20, next_cap: 8, version: "postgres-test-1" },
    clock_version: 1,
    policy_version: "v16-rule-1",
    planner_config_version: "postgres-test-1",
  };
  return {
    planId,
    dispatchEventId,
    channelId,
    scheduledAt,
    payload,
    payloadHash: dispatchPayloadHash(payload),
  };
}

async function insertFixture(pool, value) {
  await pool.query(
    `INSERT INTO feature_clock.daily_channel_plans (
       plan_id,plan_day,channel_id,due_day,due_at,eligible_at,scheduled_at,
       execution_deadline_at,
       run_about,run_video,run_agent,dispatch_slot,
       capacity_factor,player_cap,next_cap,estimated_request_cost,
       source_clock_version,policy_version,planner_config_version,capacity_version,status
     ) VALUES (
       $1,$2,$3,$2,$4::timestamptz,$4::timestamptz,$4::timestamptz,
       $4::timestamptz + interval '1 day',
       true,false,false,0,1,20,8,1,1,'v16-rule-1','postgres-test-1',
       'postgres-test-1','planned'
     )`,
    [value.planId, value.payload.plan_day, value.channelId, value.scheduledAt],
  );
  await pool.query(
    `INSERT INTO feature_clock.dispatch_outbox (
       dispatch_event_id,plan_id,job_id,queue_name,payload_json,payload_hash,
       status,next_attempt_at
     ) VALUES ($1,$2,$3,'youtube-channel-incremental',$4,$5,'pending',now()-interval '1 minute')`,
    [
      value.dispatchEventId,
      value.planId,
      value.payload.job_id,
      value.payload,
      value.payloadHash,
    ],
  );
}

test("Publisher releases only Plans whose exact scheduled_at has arrived", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 4 });
  const databaseNow = (await pool.query("SELECT now() AS now")).rows[0].now;
  const due = fixture(
    `UCdispatchdue${randomUUID().replaceAll("-", "")}`,
    new Date(databaseNow.getTime() - 60_000),
  );
  const future = fixture(
    `UCdispatchfuture${randomUUID().replaceAll("-", "")}`,
    new Date(databaseNow.getTime() + 3_600_000),
  );
  const withTransaction = async (action) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await action(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  };
  const jobs = new Map();
  const queue = {
    name: "youtube-channel-incremental",
    async getJob(jobId) { return jobs.get(jobId) || null; },
    async add(_name, data, options) {
      const job = { id: options.jobId, data };
      jobs.set(job.id, job);
      return job;
    },
  };

  try {
    await insertFixture(pool, due);
    await insertFixture(pool, future);
    const publisher = new DispatchOutboxPublisher({
      store: new PostgresDispatchOutboxStore({
        query: (sql, params) => pool.query(sql, params),
        withTransaction,
      }),
      queue,
      leaseOwner: `postgres-test:${randomUUID()}`,
      releaseBatchSize: 10,
      maxInFlight: 10,
      logger: { info() {}, error() {} },
    });

    const first = await publisher.runOnce();
    const second = await publisher.runOnce();

    assert.deepEqual(first, {
      claimed: 1,
      published: 1,
      retried: 0,
      dead_lettered: 0,
      lease_lost: 0,
    });
    assert.equal(second.claimed, 0);
    assert.equal(jobs.has(due.payload.job_id), true);
    assert.equal(jobs.has(future.payload.job_id), false);
  } finally {
    await pool.query(
      "DELETE FROM feature_clock.daily_channel_plans WHERE plan_id=ANY($1::uuid[])",
      [[due.planId, future.planId]],
    ).catch(() => {});
    await pool.end();
  }
});

test("Dynamic Dispatcher assigns time and Outbox in one transaction", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 4 });
  const planId = randomUUID();
  const channelId = `UCdynamic${randomUUID().replaceAll("-", "")}`;
  const releasedAt = new Date("2026-07-22T06:00:00Z");
  const withTransaction = async (action) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await action(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  };

  try {
    await pool.query(
      `INSERT INTO feature_clock.daily_channel_plans (
         plan_id,plan_day,channel_id,due_day,due_at,eligible_at,
         run_about,run_video,run_agent,dispatch_slot,
         capacity_factor,player_cap,next_cap,estimated_request_cost,
         source_clock_version,policy_version,planner_config_version,capacity_version,status
       ) VALUES (
         $1,'2026-07-22',$2,'2026-07-20','2026-07-20T00:00:00Z',
         '2026-07-22T00:30:00Z',true,false,false,17,
         1,20,8,1,1,'v16-rule-1','postgres-date-plan-1','postgres-capacity-1','planned'
       )`,
      [planId, channelId],
    );
    const store = new PostgresDynamicDispatchStore({ withTransaction });
    const staged = await store.stageBatch({
      releasedAt,
      totalLimit: 1,
      agentOnlyLimit: 0,
      agentPendingTarget: 30,
      executionTimeoutMinutes: 60,
    });
    const result = await pool.query(
      `SELECT plan.scheduled_at,plan.execution_deadline_at,plan.dispatched_at,
              outbox.payload_json,outbox.status
       FROM feature_clock.daily_channel_plans plan
       JOIN feature_clock.dispatch_outbox outbox ON outbox.plan_id=plan.plan_id
       WHERE plan.plan_id=$1`,
      [planId],
    );

    assert.equal(staged.staged, 1);
    assert.equal(result.rowCount, 1);
    assert.equal(result.rows[0].scheduled_at.toISOString(), releasedAt.toISOString());
    assert.equal(
      result.rows[0].execution_deadline_at.toISOString(),
      "2026-07-22T07:00:00.000Z",
    );
    assert.equal(result.rows[0].dispatched_at, null);
    assert.equal(result.rows[0].payload_json.scheduled_at, releasedAt.toISOString());
    assert.equal(result.rows[0].status, "pending");
  } finally {
    await pool.query(
      "DELETE FROM feature_clock.daily_channel_plans WHERE plan_id=$1",
      [planId],
    ).catch(() => {});
    await pool.end();
  }
});
