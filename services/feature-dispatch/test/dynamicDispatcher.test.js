import assert from "node:assert/strict";
import test from "node:test";
import {
  BullMqCapacityProbe,
  DynamicDispatcher,
  PostgresDynamicDispatchStore,
  buildDispatchEnvelope,
  computeDispatchBudget,
  insideUtcDispatchWindow,
  queuePressure,
  uuidV5,
} from "../src/dynamicDispatcher.js";
import { validateDispatchOutboxRow } from "../src/dispatchTransport.js";

function counts(values = {}) {
  return {
    waiting: 0,
    active: 0,
    delayed: 0,
    prioritized: 0,
    paused: 0,
    "waiting-children": 0,
    ...values,
  };
}

function telemetry(overrides = {}) {
  return {
    incremental: { counts: counts({ waiting: 3, active: 2 }), workers: 20, paused: false },
    channel_crawl: { counts: counts({ waiting: 8, active: 2 }), workers: 20, paused: false },
    agent_incremental: { counts: counts(), workers: 2, paused: false },
    proxy_channel_ready: 18,
    ...overrides,
  };
}

function plan(overrides = {}) {
  return {
    plan_id: "4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d",
    plan_day: "2026-07-22",
    channel_id: "UCtest",
    run_about: true,
    run_video: false,
    run_agent: false,
    capacity_factor: 1,
    player_cap: 20,
    next_cap: 8,
    capacity_version: "capacity-1",
    source_clock_version: 7,
    policy_version: "v16-rule-1",
    planner_config_version: "date-plan-1",
    ...overrides,
  };
}

test("UTC dispatch window is half-open", () => {
  assert.equal(insideUtcDispatchWindow("2026-07-22T00:29:59Z"), false);
  assert.equal(insideUtcDispatchWindow("2026-07-22T00:30:00Z"), true);
  assert.equal(insideUtcDispatchWindow("2026-07-22T21:29:59Z"), true);
  assert.equal(insideUtcDispatchWindow("2026-07-22T21:30:00Z"), false);
});

test("capacity budget accounts for shared Channel work and reserves Agent headroom", () => {
  const result = computeDispatchBudget(telemetry(), {
    releaseBatchSize: 50,
    bufferPerWorker: 2,
    maximumQueueBuffer: 100,
    agentShare: 0.25,
    agentBatchSize: 30,
  });

  assert.equal(queuePressure(telemetry().incremental.counts), 5);
  assert.equal(result.channel_capacity, 18);
  assert.equal(result.channel_target, 36);
  assert.equal(result.incremental_target, 26);
  assert.equal(result.total_limit, 21);
  assert.equal(result.agent_only_limit, 5);
  assert.equal(result.agent_pending_target, 60);
});

test("large Full Crawl backlog cannot starve due incremental work", () => {
  const result = computeDispatchBudget(telemetry({
    incremental: { counts: counts(), workers: 20, paused: false },
    channel_crawl: {
      counts: counts({ waiting: 100_000, active: 18 }),
      workers: 20,
      paused: false,
    },
  }), {
    releaseBatchSize: 50,
    bufferPerWorker: 2,
    minimumIncrementalShare: 0.25,
  });

  assert.equal(result.channel_target, 36);
  assert.equal(result.competing_channel_pressure, 100_018);
  assert.equal(result.incremental_target, 9);
  assert.equal(result.total_limit, 9);
});

test("paused or zero-worker incremental queue releases nothing", () => {
  assert.equal(computeDispatchBudget(telemetry({
    incremental: { counts: counts(), workers: 20, paused: true },
  })).total_limit, 0);
  assert.equal(computeDispatchBudget(telemetry({
    incremental: { counts: counts(), workers: 0, paused: false },
  })).total_limit, 0);
});

test("capacity probe reads all queues and the proxy Channel role", async () => {
  const queue = (name, workers) => ({
    name,
    async getJobCounts() { return counts({ active: 1 }); },
    async getWorkersCount() { return workers; },
    async getGlobalConcurrency() { return workers - 1; },
    async isPaused() { return false; },
  });
  const probe = new BullMqCapacityProbe({
    incrementalQueue: queue("youtube-channel-incremental", 5),
    channelCrawlQueue: queue("youtube-channel-crawl", 5),
    agentIncrementalQueue: queue("youtube-agent-incremental", 2),
    proxyCapacityUrl: "http://rota-core:8001/api/v1/proxy-control/",
    fetchImpl: async (url) => ({
      ok: true,
      async json() { return { roles: { channel: { ready: 4 } } }; },
      url,
    }),
  });

  const sampled = await probe.sample();
  assert.equal(sampled.incremental.workers, 5);
  assert.equal(sampled.incremental.global_concurrency, 4);
  assert.equal(sampled.proxy_channel_ready, 4);
});

test("release envelope receives scheduled_at only at dispatch time", () => {
  const releasedAt = new Date("2026-07-22T02:03:04.567Z");
  const envelope = buildDispatchEnvelope(plan(), releasedAt);
  const row = {
    ...envelope,
    payload_json: envelope.payload,
    plan_scheduled_at: releasedAt,
  };

  assert.equal(envelope.payload.scheduled_at, releasedAt.toISOString());
  assert.equal(envelope.payload.schema_version, 5);
  assert.deepEqual(envelope.payload.task_mask, {
    about: true,
    video: false,
    agent: false,
  });
  assert.equal(validateDispatchOutboxRow(row).channel_id, "UCtest");
  assert.equal(
    uuidV5("336343f8-4684-4468-8cb2-2042ad252eb7", plan().plan_id),
    envelope.dispatch_event_id,
  );
});

test("an empty three-Clock Plan cannot create a dispatch", () => {
  assert.throws(() => buildDispatchEnvelope(plan({
    run_about: false,
  }), new Date("2026-07-22T02:03:04.567Z")), /no active About, Video, or Agent task/);
});

test("PostgreSQL staging reserves Agent-only work then fills regular capacity", async () => {
  const releaseAt = new Date("2026-07-22T02:03:04Z");
  const agentPlan = plan({
    plan_id: "29a8bada-8116-4fe2-8e79-5c86940852d7",
    channel_id: "UCagent",
    run_about: false,
    run_agent: true,
  });
  const regularPlan = plan();
  const inserts = [];
  const client = {
    async query(sql, params = []) {
      if (sql.includes("pg_advisory_xact_lock")) return { rows: [] };
      if (sql.includes("count(*)::int AS total")) {
        return { rows: [{ total: 0, agent_only: 0 }] };
      }
      if (sql.includes("count(*)::int AS count")) return { rows: [{ count: 0 }] };
      if (sql.includes("SELECT *") && sql.includes("AND run_agent AND NOT")) {
        return { rows: [agentPlan] };
      }
      if (sql.includes("SELECT *") && sql.includes("run_about OR run_video")) {
        return { rows: [regularPlan] };
      }
      if (sql.includes("UPDATE feature_clock.daily_channel_plans")) {
        assert.equal(params[1], releaseAt);
        return { rows: [{ plan_id: params[0] }], rowCount: 1 };
      }
      if (sql.includes("INSERT INTO feature_clock.dispatch_outbox")) {
        inserts.push(params);
        return { rows: [], rowCount: 1 };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };
  const store = new PostgresDynamicDispatchStore({
    withTransaction: async (action) => action(client),
  });

  const result = await store.stageBatch({
    releasedAt: releaseAt,
    totalLimit: 3,
    agentOnlyLimit: 1,
    agentPendingTarget: 30,
    executionTimeoutMinutes: 60,
  });

  assert.deepEqual(result, {
    staged: 2,
    channel: 1,
    agent_only: 1,
    pending_outbox: 0,
  });
  assert.equal(inserts.length, 2);
  assert.equal(JSON.parse(inserts[0][4]).scheduled_at, releaseAt.toISOString());
});

test("Dynamic Dispatcher does no telemetry or database work outside the safe window", async () => {
  let called = false;
  const dispatcher = new DynamicDispatcher({
    probe: { async sample() { called = true; } },
    store: { async stageBatch() { called = true; } },
    publisher: {
      async runOnce({ batchSize }) {
        assert.equal(batchSize, 0);
        return { claimed: 0 };
      },
    },
    now: () => new Date("2026-07-22T22:00:00Z"),
  });

  const result = await dispatcher.runOnce();
  assert.equal(result.window_open, false);
  assert.equal(called, false);
});
