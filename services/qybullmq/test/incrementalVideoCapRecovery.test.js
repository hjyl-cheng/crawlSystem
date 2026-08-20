import assert from "node:assert/strict";
import test from "node:test";

import {
  BUG035_RECOVERY_OPERATION_ID,
  classifyBug035RecoveryTarget,
  dispatchBug035RecoveryTarget,
  prepareBug035RecoveryTargets,
} from "../src/incrementalVideoCapRecovery.js";

function target(overrides = {}) {
  return {
    run_id: "incremental:11111111-1111-4111-8111-111111111111",
    channel_id: "UCbug035",
    plan_id: "11111111-1111-4111-8111-111111111111",
    plan_day: "2026-08-17",
    run_status: "done",
    detail_status: "done",
    video_status: "partial",
    job_id: "incremental__UCbug035__20260817__clock_1__111111111111",
    source_observation_id: "22222222-2222-4222-8222-222222222222",
    source_kind_sequence: 7,
    first_seen_count: 30,
    unresolved_count: 10,
    affected_count: 40,
    previous_video_domain: {
      status: "partial",
      observation_id: "22222222-2222-4222-8222-222222222222",
    },
    recovery_marker: null,
    ...overrides,
  };
}

function jobFixture(source = target(), overrides = {}) {
  const retryStates = [];
  const job = {
    id: source.job_id,
    name: "channel.incremental.plan",
    data: {
      job_id: source.job_id,
      plan_id: source.plan_id,
      channel_id: source.channel_id,
      task_mask: { about: false, video: true, agent: false },
    },
    attemptsMade: 1,
    async getState() { return "completed"; },
    async retry(state) { retryStates.push(state); },
    ...overrides,
  };
  return { job, retryStates };
}

test("a terminal BUG-035 Run is prepared before its completed BullMQ Job is retried", async () => {
  const source = target();
  const { job } = jobFixture(source);
  const inspection = await classifyBug035RecoveryTarget(source, job);

  assert.equal(inspection.action, "prepare_and_retry");
  assert.equal(inspection.attempts_made_before, 1);

  const calls = [];
  const prepared = await prepareBug035RecoveryTargets({
    async query(sql, params) {
      calls.push({ sql, params });
      if (sql.includes("pg_advisory_xact_lock")) return { rowCount: 1, rows: [{}] };
      return { rowCount: 1, rows: [{ run_id: source.run_id }] };
    },
  }, [inspection], {
    now: new Date("2026-08-17T03:00:00.000Z"),
  });

  assert.deepEqual(prepared, [source.run_id]);
  const update = calls.find(({ sql }) => sql.includes("UPDATE crawler.channel_runs"));
  assert.ok(update);
  assert.match(update.sql, /status='queued'/);
  assert.match(update.sql, /detail_status='pending'/);
  assert.match(update.sql, /finished_at=NULL/);
  assert.equal(update.params[4], BUG035_RECOVERY_OPERATION_ID);
  const pendingDomain = JSON.parse(update.params[6]);
  assert.equal(pendingDomain.status, "pending");
  const marker = JSON.parse(update.params[7]);
  assert.equal(marker.attempts_made_before, 1);
  assert.deepEqual(marker.previous_video_domain, source.previous_video_domain);
});

test("a prepared recovery retries the same completed Job without replacing its Plan", async () => {
  const source = target({
    run_status: "queued",
    detail_status: "pending",
    video_status: "pending",
    recovery_marker: {
      operation_id: BUG035_RECOVERY_OPERATION_ID,
      run_id: "incremental:11111111-1111-4111-8111-111111111111",
      channel_id: "UCbug035",
      plan_id: "11111111-1111-4111-8111-111111111111",
      job_id: "incremental__UCbug035__20260817__clock_1__111111111111",
      attempts_made_before: 1,
    },
  });
  const { job, retryStates } = jobFixture(source);
  const inspection = await classifyBug035RecoveryTarget(source, job);

  assert.equal(inspection.action, "retry_prepared");
  const result = await dispatchBug035RecoveryTarget({
    async getJob(id) {
      assert.equal(id, source.job_id);
      return job;
    },
  }, inspection);

  assert.equal(result.action, "retried_completed");
  assert.deepEqual(retryStates, ["completed"]);
  assert.equal(job.data.plan_id, source.plan_id);
  assert.equal(job.data.job_id, source.job_id);
});

test("a recovery whose attempt was already consumed is not dispatched twice", async () => {
  const source = target({
    run_status: "done",
    detail_status: "done",
    video_status: "complete",
    recovery_marker: {
      operation_id: BUG035_RECOVERY_OPERATION_ID,
      run_id: "incremental:11111111-1111-4111-8111-111111111111",
      channel_id: "UCbug035",
      plan_id: "11111111-1111-4111-8111-111111111111",
      job_id: "incremental__UCbug035__20260817__clock_1__111111111111",
      attempts_made_before: 1,
    },
  });
  const { job, retryStates } = jobFixture(source, { attemptsMade: 2 });
  const inspection = await classifyBug035RecoveryTarget(source, job);

  assert.equal(inspection.action, "recovery_completed");
  const result = await dispatchBug035RecoveryTarget({
    async getJob() { return job; },
  }, inspection);

  assert.equal(result.action, "already_consumed");
  assert.deepEqual(retryStates, []);
});

test("a conflicting completed Job fails closed before the Run is reopened", async () => {
  const source = target();
  const { job } = jobFixture(source, {
    data: {
      job_id: source.job_id,
      plan_id: source.plan_id,
      channel_id: "UCother",
      task_mask: { video: true },
    },
  });

  await assert.rejects(
    classifyBug035RecoveryTarget(source, job),
    /conflicting channel_id/,
  );
});
