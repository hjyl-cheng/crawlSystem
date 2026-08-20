import assert from "node:assert/strict";
import test from "node:test";

import {
  INCREMENTAL_CHALLENGE_RECOVERY_CHANNEL_IDS,
  INCREMENTAL_CHALLENGE_RECOVERY_OPERATION_ID,
  INCREMENTAL_CHALLENGE_TARGET_SQL,
  classifyIncrementalChallengeRecoveryTarget,
  dispatchIncrementalChallengeRecoveryTarget,
  prepareIncrementalChallengeRecoveryTargets,
} from "../src/incrementalChallengeRecovery.js";

function target(overrides = {}) {
  return {
    run_id: "incremental:e55313a2-99cd-5e7a-92fe-5d1349832570",
    channel_id: INCREMENTAL_CHALLENGE_RECOVERY_CHANNEL_IDS[0],
    plan_id: "e55313a2-99cd-5e7a-92fe-5d1349832570",
    plan_day: "2026-08-17",
    run_status: "done",
    detail_status: "done",
    publication_finalized_at: null,
    video_status: "partial",
    job_id: "incremental__UC2bZgihqibFD_vhaYEXQZFg__20260817__clock_20__d4080bd3f80d",
    source_observation_id: "06109359-403c-4b55-881b-4a4ccc56905f",
    source_kind_sequence: 6,
    source_reason_code: "video_cycle_partial_partial",
    detail_failure_count: 27,
    challenge_decision_count: 2,
    challenge_attempt_count: 2,
    network_identity_count: 1,
    route_generation_count: 1,
    previous_video_domain: {
      status: "partial",
      observation_id: "06109359-403c-4b55-881b-4a4ccc56905f",
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
    attemptsMade: 2,
    async getState() { return "completed"; },
    async retry(state) { retryStates.push(state); },
    ...overrides,
  };
  return { job, retryStates };
}

test("the selector requires persisted challenge evidence and only the fixed target channels", () => {
  assert.match(INCREMENTAL_CHALLENGE_TARGET_SQL, /failure_decision->>'kind'='youtube_challenge'/);
  assert.match(INCREMENTAL_CHALLENGE_TARGET_SQL, /run\.channel_id=ANY\(\$2::text\[\]\)/);
  assert.equal(INCREMENTAL_CHALLENGE_RECOVERY_CHANNEL_IDS.length, 5);
});

test("a terminal challenge Partial is prepared before its original Job is retried", async () => {
  const source = target();
  const { job } = jobFixture(source);
  const inspection = await classifyIncrementalChallengeRecoveryTarget(source, job);

  assert.equal(inspection.action, "prepare_and_retry");
  assert.equal(inspection.attempts_made_before, 2);

  const calls = [];
  const prepared = await prepareIncrementalChallengeRecoveryTargets({
    async query(sql, params) {
      calls.push({ sql, params });
      if (sql.includes("pg_advisory_xact_lock")) return { rowCount: 1, rows: [{}] };
      return { rowCount: 1, rows: [{ run_id: source.run_id }] };
    },
  }, [inspection], {
    now: new Date("2026-08-17T04:00:00.000Z"),
  });

  assert.deepEqual(prepared, [source.run_id]);
  const update = calls.find(({ sql }) => sql.includes("UPDATE crawler.channel_runs"));
  assert.ok(update);
  assert.match(update.sql, /status='queued'/);
  assert.match(update.sql, /detail_status='pending'/);
  assert.match(update.sql, /publication_finalized_at IS NULL/);
  assert.equal(update.params[4], INCREMENTAL_CHALLENGE_RECOVERY_OPERATION_ID);
  const marker = JSON.parse(update.params[7]);
  assert.equal(marker.challenge_decision_count, 2);
  assert.equal(marker.source_observation_id, source.source_observation_id);
  assert.equal(marker.previous_video_domain.status, "partial");
});

test("a prepared recovery retries the same completed Job and Plan", async () => {
  const source = target({
    run_status: "queued",
    detail_status: "pending",
    video_status: "pending",
    recovery_marker: {
      operation_id: INCREMENTAL_CHALLENGE_RECOVERY_OPERATION_ID,
      run_id: "incremental:e55313a2-99cd-5e7a-92fe-5d1349832570",
      channel_id: INCREMENTAL_CHALLENGE_RECOVERY_CHANNEL_IDS[0],
      plan_id: "e55313a2-99cd-5e7a-92fe-5d1349832570",
      job_id: "incremental__UC2bZgihqibFD_vhaYEXQZFg__20260817__clock_20__d4080bd3f80d",
      attempts_made_before: 2,
    },
  });
  const { job, retryStates } = jobFixture(source);
  const inspection = await classifyIncrementalChallengeRecoveryTarget(source, job);

  assert.equal(inspection.action, "retry_prepared");
  const result = await dispatchIncrementalChallengeRecoveryTarget({
    async getJob(id) {
      assert.equal(id, source.job_id);
      return job;
    },
  }, inspection);

  assert.equal(result.action, "retried_completed");
  assert.deepEqual(retryStates, ["completed"]);
  assert.equal(job.data.plan_id, source.plan_id);
});

test("unexpected channels and missing challenge evidence fail closed", async () => {
  const unexpected = target({ channel_id: "UCunexpected" });
  await assert.rejects(
    classifyIncrementalChallengeRecoveryTarget(unexpected, jobFixture(unexpected).job),
    /unexpected Incremental challenge recovery channel/,
  );

  const noEvidence = target({ challenge_decision_count: 0 });
  await assert.rejects(
    classifyIncrementalChallengeRecoveryTarget(noEvidence, jobFixture(noEvidence).job),
    /challenge_decision_count must be positive/,
  );
});

test("a finalized Run cannot be reopened", async () => {
  const source = target({ publication_finalized_at: "2026-08-17T03:30:00.000Z" });
  await assert.rejects(
    classifyIncrementalChallengeRecoveryTarget(source, jobFixture(source).job),
    /already finalized/,
  );
});
