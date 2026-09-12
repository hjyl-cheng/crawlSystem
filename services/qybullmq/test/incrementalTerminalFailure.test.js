import assert from "node:assert/strict";
import test from "node:test";
import {
  recordIncrementalTerminalFailure,
} from "../src/incrementalTerminalFailure.js";

for (const [code, reason] of [
  ["CONTENT_DETAIL_EXECUTION_FENCE_STALE", "execution_superseded"],
  ["VIDEO_EXECUTION_RECOVERY_PENDING", "execution_recovery_pending"],
]) test(`queue terminal callback cannot fail the current Plan for ${code}`, async () => {
  const result = await recordIncrementalTerminalFailure({
    job: { id: plan().job_id, name: "channel.incremental.plan", queueName: "youtube-channel-incremental", data: plan() },
    error: Object.assign(new Error(code), { code }),
    attempts: 10, maxAttempts: 1, permanent: true,
    withTransaction: async () => assert.fail("old execution cannot write the current Plan"),
  });
  assert.deepEqual(result, { recorded: false, reason });
});

function plan(overrides = {}) {
  const base = {
    schema_version: 5,
    dispatch_generation: 1,
    job_id: "incremental__UCfailed__20260723__clock_6__terminal",
    plan_id: "4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d",
    plan_mode: "standard",
    plan_day: "2026-07-23",
    channel_id: "UCfailed",
    task_mask: { about: false, video: true, agent: false },
    capacity: { factor: 1, player_cap: 20, next_cap: 8, version: "capacity-v1" },
    scheduled_at: "2026-07-23T01:00:00.000Z",
    clock_version: 6,
    policy_version: "v16-rule-6",
    planner_config_version: "video-plan-1",
  };
  return { ...base, ...overrides };
}

test("terminal incremental failure emits one failed Observation for the active domain", async () => {
  const calls = [];
  const removals = [];
  const client = {
    async query(statement) {
      assert.match(statement, /FROM crawler\.channel_runs/);
      return {
        rows: [{
          run_id: `incremental:${plan().plan_id}`,
          finished_at: "2026-07-23T01:02:03.000Z",
          result_json: {
            domains: {
              about: { status: "not_due" },
              video: { status: "failed" },
              agent: { status: "not_due" },
            },
          },
        }],
      };
    },
  };
  const result = await recordIncrementalTerminalFailure({
    job: {
      id: plan().job_id,
      name: "channel.incremental.plan",
      queueName: "youtube-channel-incremental",
      data: plan(),
    },
    error: new Error("This channel was removed because it violated our Community Guidelines."),
    attempts: 3,
    withTransaction: async (action) => action(client),
    markRemoved: async (_client, command) => removals.push(command),
    recordGeneric: async (_client, command) => {
      calls.push(command);
      return { outcome: "failed", observation_id: "failure-observation" };
    },
  });

  assert.equal(result.domain, "video");
  assert.equal(removals.length, 1);
  assert.equal(removals[0].terminal.removed_reason, "community_guidelines");
  assert.equal(calls.length, 1);
  assert.equal(calls[0].observationKind, "video");
  assert.equal(calls[0].observedAt, "2026-07-23T01:02:03.000Z");
  const prepared = await calls[0].prepare({});
  assert.equal(prepared.outcome, "failed");
  assert.deepEqual(prepared.payload, {
    failure_kind: "channel_removed",
    attempt_count: 3,
    removed_reason: "community_guidelines",
  });
});

test("terminal incremental failure is not emitted while the domain can still retry", async () => {
  const result = await recordIncrementalTerminalFailure({
    job: {
      id: plan().job_id,
      name: "channel.incremental.plan",
      queueName: "youtube-channel-incremental",
      data: plan(),
    },
    error: new Error("temporary timeout"),
    attempts: 2,
    maxAttempts: 3,
    withTransaction: async () => assert.fail("database must not be touched"),
  });
  assert.deepEqual(result, { recorded: false, reason: "retry_pending" });
});

test("explicit Channel removal is persisted on the first failed attempt", async () => {
  const calls = [];
  let preparedFailure = null;
  const client = {
    async query(statement) {
      calls.push(String(statement));
      if (String(statement).includes("FROM crawler.channel_runs")) {
        return {
          rows: [{
            run_id: `incremental:${plan().plan_id}`,
            finished_at: "2026-07-23T01:02:03.000Z",
            result_json: { domains: { video: { status: "failed" } } },
          }],
        };
      }
      return { rowCount: 1, rows: [] };
    },
  };

  const result = await recordIncrementalTerminalFailure({
    job: {
      id: plan().job_id,
      name: "channel.incremental.plan",
      queueName: "youtube-channel-incremental",
      data: plan(),
    },
    error: new Error("This channel does not exist."),
    attempts: 1,
    maxAttempts: 5,
    withTransaction: async (action) => action(client),
    recordGeneric: async (_client, command) => {
      preparedFailure = await command.prepare({});
      return { outcome: "failed", observation_id: "failure-observation" };
    },
  });

  assert.equal(result.recorded, true);
  assert.equal(result.failure_kind, "channel_removed");
  assert.equal(result.removed_reason, "channel_not_found");
  assert.equal(calls.some((call) => (
    typeof call === "string" && call.includes("UPDATE crawler.channels")
  )), true);
  assert.deepEqual(preparedFailure, {
    outcome: "failed",
    outcomeReasonCode: "video_channel_removed",
    resultSummary: {
      attempt_count: 1,
      failure_kind: "channel_removed",
      removed_reason: "channel_not_found",
    },
    payload: {
      failure_kind: "channel_removed",
      attempt_count: 1,
      removed_reason: "channel_not_found",
    },
    errorClass: "Error",
    errorMessage: "This channel does not exist.",
  });
});

test("terminal About failure uses the generic failed-domain payload", async () => {
  const aboutPlan = plan({
    task_mask: { about: true, video: false, agent: false },
  });
  const calls = [];
  const client = {
    async query() {
      return {
        rows: [{
          run_id: `incremental:${aboutPlan.plan_id}`,
          finished_at: "2026-07-23T01:02:03.000Z",
          result_json: { domains: { about: { status: "failed" } } },
        }],
      };
    },
  };

  const result = await recordIncrementalTerminalFailure({
    job: {
      id: aboutPlan.job_id,
      name: "channel.incremental.plan",
      queueName: "youtube-channel-incremental",
      data: aboutPlan,
    },
    error: new Error("temporary timeout"),
    attempts: 3,
    maxAttempts: 3,
    withTransaction: async (action) => action(client),
    recordGeneric: async (_client, command) => {
      calls.push(command);
      return { outcome: "failed", observation_id: "about-failure" };
    },
    recordAbout: async () => assert.fail("metric writer must not handle failed About"),
  });

  assert.equal(result.domain, "about");
  assert.equal(calls.length, 1);
  assert.equal(calls[0].observationKind, "about");
  const prepared = await calls[0].prepare({});
  assert.deepEqual(prepared.payload, {
    failure_kind: "timeout",
    attempt_count: 3,
  });
});

test("a Profile task key is rejected before a terminal failure is recorded", async () => {
  const validPlan = plan({
    task_mask: { about: true, video: false, agent: false },
  });
  const invalidPlan = {
    ...validPlan,
    task_mask: { profile: true, ...validPlan.task_mask },
  };
  let touchedDatabase = false;

  await assert.rejects(recordIncrementalTerminalFailure({
    job: {
      id: invalidPlan.job_id,
      name: "channel.incremental.plan",
      queueName: "youtube-channel-incremental",
      data: invalidPlan,
    },
    error: new Error("temporary timeout"),
    attempts: 3,
    maxAttempts: 3,
    withTransaction: async () => { touchedDatabase = true; },
  }), /task_mask keys differ from the contract/);

  assert.equal(touchedDatabase, false);
});

test("terminal Agent failure uses the bounded failed batch payload", async () => {
  const agentPlan = plan({
    task_mask: { about: false, video: false, agent: true },
  });
  const calls = [];
  const client = {
    async query() {
      return {
        rows: [{
          run_id: `incremental:${agentPlan.plan_id}`,
          finished_at: "2026-07-23T01:02:03.000Z",
          result_json: { domains: { agent: { status: "failed" } } },
        }],
      };
    },
  };

  const result = await recordIncrementalTerminalFailure({
    job: {
      id: agentPlan.job_id,
      name: "channel.incremental.plan",
      queueName: "youtube-channel-incremental",
      data: agentPlan,
    },
    error: new Error("agent upstream timeout"),
    attempts: 3,
    maxAttempts: 3,
    withTransaction: async (action) => action(client),
    recordGeneric: async (_client, command) => {
      calls.push(command);
      return { outcome: "failed", observation_id: "agent-failure" };
    },
  });

  assert.equal(result.domain, "agent");
  assert.equal(calls.length, 1);
  const prepared = await calls[0].prepare({});
  assert.equal(prepared.outcome, "failed");
  assert.deepEqual(prepared.payload, { failed_plan_count: 1 });
});
