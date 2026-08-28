import assert from "node:assert/strict";
import test from "node:test";
import { IncrementalChannelRunner } from "../src/incrementalChannelRunner.js";

function plan(masks = {}) {
  const taskMask = {
    about: false,
    video: false,
    agent: false,
    ...masks,
  };
  return {
    schema_version: 5,
    dispatch_generation: 1,
    job_id: "incremental__UCtest__20260720__clock_7__5d62c032cbbc",
    plan_id: "4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d",
    plan_mode: "standard",
    plan_day: "2026-07-20",
    scheduled_at: "2026-07-20T13:25:40.000Z",
    channel_id: "UCtest",
    task_mask: taskMask,
    capacity: { factor: 1, player_cap: 20, next_cap: 8, version: "capacity-1" },
    clock_version: 7,
    policy_version: "v16-rule-1",
    planner_config_version: "video-plan-1",
  };
}

function job(data) {
  return {
    id: data.job_id,
    name: "channel.incremental.plan",
    queueName: "youtube-channel-incremental",
    data,
  };
}

function storeFixture(data) {
  const calls = [];
  const domainResults = [];
  return {
    calls,
    domainResults,
    async claim() {
      return {
        created: true,
        resumed: false,
        terminal: false,
        run: {
          run_id: `incremental:${data.plan_id}`,
          status: "running",
          result_json: {
            domains: Object.fromEntries(Object.entries(data.task_mask).map(([domain, due]) => [
              domain,
              { status: due ? "pending" : "not_due" },
            ])),
          },
        },
      };
    },
    async markDomain(runId, domain, status, result) {
      calls.push(["domain", domain, status]);
      domainResults.push({ domain, status, result });
    },
    async finish(runId, options) { calls.push(["finish", options.waitingForAgent]); },
    async fail() { calls.push(["fail"]); },
  };
}

test("About Plan performs one About execution in one Channel session", async () => {
  const data = plan({ about: true });
  const runStore = storeFixture(data);
  let opened = 0;
  const snapshots = [];
  const runner = new IncrementalChannelRunner({
    runStore,
    agentBacklog: {},
    withTransaction: async (action) => action({}),
    openChannel: async (channelId, options) => {
      opened += 1;
      assert.equal(channelId, data.channel_id);
      assert.equal(options.includeAbout, true);
      return { token: "same-session" };
    },
    about: async ({ getChannelSnapshot }) => {
      snapshots.push(await getChannelSnapshot());
      return { outcome: "complete", observation_id: "about-observation" };
    },
  });

  const result = await runner.execute(job(data));
  assert.equal(opened, 1);
  assert.equal(snapshots.length, 1);
  assert.equal(snapshots[0].token, "same-session");
  assert.deepEqual(result.executed_domains, ["about"]);
  assert.equal(result.status, "done");
});

test("Agent-only Plan registers one Channel without opening a YouTube session", async () => {
  const data = plan({ agent: true });
  const runStore = storeFixture(data);
  let opened = 0;
  const registered = [];
  const runner = new IncrementalChannelRunner({
    runStore,
    agentBacklog: {
      async register(value) { registered.push(value); return { created: true, request: {} }; },
    },
    withTransaction: async (action) => action({}),
    openChannel: async () => { opened += 1; return {}; },
  });

  const result = await runner.execute(job(data));
  assert.equal(opened, 0);
  assert.equal(registered.length, 1);
  assert.equal(registered[0].plan.channel_id, "UCtest");
  assert.equal(result.status, "waiting_agent");
  assert.deepEqual(result.executed_domains, ["agent"]);
});

test("a Profile task key is rejected before a Run is claimed", async () => {
  const valid = plan({ about: true });
  const data = {
    ...valid,
    task_mask: { profile: true, ...valid.task_mask },
  };
  let claimed = false;
  const runner = new IncrementalChannelRunner({
    runStore: { async claim() { claimed = true; } },
    agentBacklog: {},
    withTransaction: async (action) => action({}),
  });

  await assert.rejects(runner.execute(job(data)), /task_mask keys differ from the contract/);
  assert.equal(claimed, false);
});

test("Video executor receives the Crawler query dependency", async () => {
  const data = plan({ video: true });
  const runStore = storeFixture(data);
  const crawlerQuery = async () => ({ rows: [] });
  let receivedQuery = null;
  let channelOptions = null;
  const runner = new IncrementalChannelRunner({
    runStore,
    agentBacklog: {},
    query: crawlerQuery,
    withTransaction: async (action) => action({}),
    openChannel: async (channelId, options) => {
      assert.equal(channelId, data.channel_id);
      channelOptions = options;
      return { token: "same-session" };
    },
    video: async (context) => {
      receivedQuery = context.query;
      await context.getChannelSnapshot();
      return { outcome: "complete" };
    },
  });

  const result = await runner.execute(job(data));
  assert.equal(receivedQuery, crawlerQuery);
  assert.deepEqual(channelOptions, { includeAbout: false });
  assert.equal(result.session_opened, true);
  assert.deepEqual(result.executed_domains, ["video"]);
});

test("a deferred Video reservation cleanup keeps the Run successful and records the warning", async () => {
  const data = plan({ video: true });
  const runStore = storeFixture(data);
  const runner = new IncrementalChannelRunner({
    runStore,
    agentBacklog: {},
    query: async () => ({ rows: [] }),
    withTransaction: async (action) => action({}),
    openChannel: async () => ({}),
    video: async () => ({
      outcome: "complete",
      observation_id: "video-observation",
      reservation_cleanup_deferred: true,
    }),
  });

  const result = await runner.execute(job(data));

  assert.equal(result.status, "done");
  assert.deepEqual(runStore.calls, [
    ["domain", "video", "running"],
    ["domain", "video", "complete"],
    ["finish", false],
  ]);
  assert.deepEqual(runStore.domainResults.at(-1), {
    domain: "video",
    status: "complete",
    result: {
      outcome: "complete",
      observation_id: "video-observation",
      event_id: null,
      kind_sequence: null,
      duplicate: false,
      reservation_cleanup_deferred: true,
    },
  });
});

test("a failed domain Observation fails the incremental Run instead of marking it complete", async () => {
  const data = plan({ about: true });
  const runStore = storeFixture(data);
  const runner = new IncrementalChannelRunner({
    runStore,
    agentBacklog: {},
    withTransaction: async (action) => action({}),
    openChannel: async () => ({ token: "same-session" }),
    about: async () => ({ outcome: "failed", observation_id: "failed-about-observation" }),
  });

  await assert.rejects(
    runner.execute(job(data)),
    /incremental about returned a failed Observation/,
  );
  assert.deepEqual(runStore.calls, [
    ["domain", "about", "running"],
    ["domain", "about", "failed"],
    ["fail"],
  ]);
});

test("a Video transition to dormant skips an Agent due in the same Plan", async () => {
  const data = plan({ video: true, agent: true });
  const runStore = storeFixture(data);
  let agentCalls = 0;
  const runner = new IncrementalChannelRunner({
    runStore,
    agentBacklog: {},
    withTransaction: async (action) => action({}),
    openChannel: async () => ({}),
    video: async () => ({ outcome: "complete", lifecycle_status: "dormant" }),
    agent: async () => { agentCalls += 1; return { queued: true }; },
  });

  const result = await runner.execute(job(data));
  assert.equal(agentCalls, 0);
  assert.equal(result.status, "done");
  assert.deepEqual(runStore.calls.slice(-2), [
    ["domain", "agent", "skipped"],
    ["finish", false],
  ]);
});
