import assert from "node:assert/strict";
import test from "node:test";
import {
  DispatchOutboxPublisher,
  PostgresDispatchOutboxStore,
  dispatchRetryDelayMs,
} from "../src/dispatchOutboxPublisher.js";
import { dispatchPayloadHash } from "../src/dispatchTransport.js";

function row() {
  const payload = {
    schema_version: 4,
    job_id: "incremental__UCtest__20260720__clock_7__5d62c032cbbc",
    plan_id: "4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d",
    plan_day: "2026-07-20",
    scheduled_at: "2026-07-20T13:25:40.000Z",
    channel_id: "UCtest",
    plan_mode: "standard",
    task_mask: { about: true, video: false, agent: false },
    capacity: { factor: 1, player_cap: 20, next_cap: 8, version: "capacity-1" },
    clock_version: 7,
    policy_version: "v16-rule-1",
    planner_config_version: "video-plan-1",
  };
  return {
    dispatch_event_id: "9e043d70-74f0-5e4e-a4c7-a2ac1591f356",
    plan_id: payload.plan_id,
    job_id: payload.job_id,
    queue_name: "youtube-channel-incremental",
    payload_json: payload,
    payload_hash: dispatchPayloadHash(payload),
    attempts: 1,
    plan_scheduled_at: new Date(payload.scheduled_at),
  };
}

function logger() {
  return { info() {}, error() {} };
}

test("Outbox Publisher marks a Plan only after BullMQ accepts it", async () => {
  const value = row();
  const calls = [];
  const store = {
    async claimBatch() { return [value]; },
    async markPublished(input) { calls.push(["published", input]); return true; },
    async markFailed(input) { calls.push(["failed", input]); return "pending"; },
  };
  const queue = {
    name: value.queue_name,
    async getJob() { return null; },
    async add(_name, data, options) { return { id: options.jobId, data }; },
  };
  const summary = await new DispatchOutboxPublisher({
    store, queue, leaseOwner: "publisher-1", logger: logger(),
  }).runOnce();
  assert.equal(summary.published, 1);
  assert.equal(calls[0][0], "published");
  assert.equal(calls.some(([name]) => name === "failed"), false);
});

test("Outbox Publisher retries transient delivery and dead-letters corrupt Plans", async () => {
  const transient = row();
  const corrupt = { ...row(), payload_hash: "sha256:wrong" };
  const statuses = [];
  const store = {
    batches: [[transient], [corrupt]],
    async claimBatch() { return this.batches.shift(); },
    async markPublished() { return true; },
    async markFailed(input) {
      statuses.push(input.deadLetter);
      return input.deadLetter ? "dead_letter" : "pending";
    },
  };
  const queue = {
    name: transient.queue_name,
    async getJob() { throw new Error("redis unavailable"); },
  };
  const publisher = new DispatchOutboxPublisher({
    store, queue, leaseOwner: "publisher-1", logger: logger(),
  });
  assert.equal((await publisher.runOnce()).retried, 1);
  assert.equal((await publisher.runOnce()).dead_lettered, 1);
  assert.deepEqual(statuses, [false, true]);
});

test("Dispatch retry delay is bounded and deterministic with injected random", () => {
  assert.equal(dispatchRetryDelayMs(1, { random: () => 0.5 }), 5000);
  assert.equal(dispatchRetryDelayMs(20, { random: () => 0.5 }), 900000);
});

test("PostgreSQL claim enforces the release window and due-day ordering", async () => {
  const queries = [];
  const client = {
    async query(sql, params = []) {
      queries.push([sql, params]);
      return { rows: [], rowCount: 0 };
    },
  };
  const store = new PostgresDispatchOutboxStore({
    query: async () => ({ rows: [] }),
    withTransaction: async (action) => action(client),
  });
  await store.claimBatch({
    leaseOwner: "publisher-1", batchSize: 10, leaseSeconds: 60,
  });
  const [claimSql, claimParams] = queries.find(([sql]) => sql.includes("WITH claimable"));
  assert.match(claimSql, /plan\.scheduled_at<=now\(\)/);
  assert.match(claimSql, /TIME '00:30'/);
  assert.match(claimSql, /TIME '21:30'/);
  assert.match(claimSql, /ORDER BY plan\.due_day,plan\.dispatch_slot,plan\.channel_id/);
  assert.equal(claimParams[0], 10);
});
