import assert from "node:assert/strict";
import test from "node:test";
import {
  IncrementalAgentBacklog,
  IncrementalAgentBatcher,
} from "../src/incrementalAgentBacklog.js";

test("Agent backlog sends the known Plan tail without waiting for the quiet window", async () => {
  const queries = [];
  const rows = [
    { channel_id: "UCtail1", plan_id: "plan-tail-1", run_id: "run-tail-1" },
    { channel_id: "UCtail2", plan_id: "plan-tail-2", run_id: "run-tail-2" },
  ];
  const client = {
    async query(sql) {
      queries.push(sql);
      if (sql.includes("pg_advisory_xact_lock")) return { rows: [] };
      if (sql.includes("AS unregistered_plan_count")) {
        return {
          rows: [{
            pending_channel_count: 2,
            oldest_pending_at: "2026-07-20T12:29:59.000Z",
            newest_pending_at: "2026-07-20T12:29:59.000Z",
            unregistered_plan_count: 0,
          }],
        };
      }
      if (sql.includes("WITH first_per_channel")) return { rows };
      if (sql.includes("UPDATE crawler.channels")) return { rows: [] };
      throw new Error(`unexpected query: ${sql}`);
    },
  };
  const backlog = new IncrementalAgentBacklog({
    withTransaction: async (work) => work(client),
  });

  const claimed = await backlog.claimBatch({
    batchId: "tail-batch",
    batchSize: 30,
    tailQuietMs: 15 * 60 * 1000,
    now: new Date("2026-07-20T12:30:00.000Z"),
  });

  assert.equal(claimed.decision.reason, "planned_tail_ready");
  assert.equal(claimed.requests.length, 2);
  const summaryQuery = queries.find((sql) => sql.includes("AS unregistered_plan_count"));
  assert.match(summaryQuery, /feature_clock\.daily_channel_plans/);
  assert.match(summaryQuery, /registered\.plan_id=plan\.plan_id/);
});

test("Incremental Agent Batcher sends only the Channels claimed from Clock requests", async () => {
  const added = [];
  const requests = Array.from({ length: 30 }, (_, index) => ({
    channel_id: `UC${index}`,
    plan_ids: [`00000000-0000-4000-8000-${String(index).padStart(12, "0")}`],
    run_ids: [`incremental:${index}`],
  }));
  const backlog = {
    async claimBatch() {
      return {
        decision: { dispatch: true, limit: 30, partial: false, reason: "full_batch" },
        requests,
      };
    },
    async releaseBatch() { throw new Error("must not release a published batch"); },
  };
  const queue = {
    async add(name, data, options) { added.push({ name, data, options }); },
  };
  const batcher = new IncrementalAgentBatcher({
    backlog,
    queue,
    agentConfigId: 7,
    now: () => new Date("2026-07-20T12:00:00.000Z"),
  });

  await batcher.runOnce();
  assert.equal(added.length, 1);
  assert.equal(added[0].name, "agent-profile-batch");
  assert.deepEqual(added[0].data.channel_ids, requests.map((item) => item.channel_id));
  assert.equal(added[0].data.force_refresh, true);
  assert.equal(added[0].data.agent_config_id, 7);
  assert.equal(added[0].data.incremental_agent_requests.length, 30);
});

test("Incremental Agent Batcher releases only its claimed batch when BullMQ rejects it", async () => {
  const releases = [];
  const backlog = {
    async claimBatch({ batchId }) {
      return {
        decision: { dispatch: true, limit: 1, partial: true, reason: "tail_quiet_window_elapsed" },
        requests: [{ channel_id: "UCtail", plan_ids: ["plan-tail"], run_ids: ["run-tail"] }],
        batchId,
      };
    },
    async releaseBatch(batchId, error) { releases.push({ batchId, error }); },
  };
  const batcher = new IncrementalAgentBatcher({
    backlog,
    queue: { async add() { throw new Error("redis unavailable"); } },
    now: () => new Date("2026-07-20T12:00:00.000Z"),
  });

  await assert.rejects(batcher.runOnce(), /redis unavailable/);
  assert.equal(releases.length, 1);
  assert.match(releases[0].batchId, /^incremental-agent:/);
});
