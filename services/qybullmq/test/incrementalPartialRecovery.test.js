import assert from "node:assert/strict";
import test from "node:test";

import {
  INCREMENTAL_PARTIAL_RECOVERY_OPERATION_ID,
  INCREMENTAL_PARTIAL_TARGET_SQL,
  classifyIncrementalPartialRecoveryTarget,
  dispatchIncrementalPartialRecoveryTarget,
  prepareIncrementalPartialRecoveryTargets,
} from "../src/incrementalPartialRecovery.js";

function target(overrides = {}) {
  return {
    run_id: "incremental:e55313a2-99cd-5e7a-92fe-5d1349832570",
    channel_id: "UCpartialRecovery",
    plan_id: "e55313a2-99cd-5e7a-92fe-5d1349832570",
    plan_day: "2026-08-17",
    run_status: "done",
    detail_status: "done",
    publication_finalized_at: null,
    video_status: "partial",
    job_id: "incremental__UCpartialRecovery__20260817__clock_20__d4080bd3f80d",
    source_observation_id: "06109359-403c-4b55-881b-4a4ccc56905f",
    source_kind_sequence: 6,
    source_reason_code: "video_cycle_partial_complete",
    unresolved_count: 7,
    discovery_failure_count: 0,
    sampling_failure_count: 0,
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

test("the selector uses only each channel's latest terminal Video Partial", () => {
  assert.match(INCREMENTAL_PARTIAL_TARGET_SQL, /DISTINCT ON \(observation\.channel_id\)/);
  assert.match(INCREMENTAL_PARTIAL_TARGET_SQL, /latest_video\.outcome='partial'/);
  assert.match(INCREMENTAL_PARTIAL_TARGET_SQL, /outcome_reason_code=ANY\(\$3::text\[\]\)/);
  assert.match(INCREMENTAL_PARTIAL_TARGET_SQL, /\$4::text\[\] IS NULL/);
});

test("a terminal Video Partial is prepared before its original Job is retried", async () => {
  const source = target();
  const { job } = jobFixture(source);
  const inspection = await classifyIncrementalPartialRecoveryTarget(source, job);

  assert.equal(inspection.action, "prepare_and_retry");
  assert.equal(inspection.attempts_made_before, 2);

  const calls = [];
  const prepared = await prepareIncrementalPartialRecoveryTargets({
    async query(sql, params) {
      calls.push({ sql, params });
      if (sql.includes("pg_advisory_xact_lock")) return { rowCount: 1, rows: [{}] };
      return { rowCount: 1, rows: [{ run_id: source.run_id }] };
    },
  }, [inspection], {
    now: new Date("2026-08-17T05:00:00.000Z"),
  });

  assert.deepEqual(prepared, [source.run_id]);
  const update = calls.find(({ sql }) => sql.includes("UPDATE crawler.channel_runs"));
  assert.ok(update);
  assert.match(update.sql, /status='queued'/);
  assert.match(update.sql, /detail_status='pending'/);
  assert.match(update.sql, /publication_finalized_at IS NULL/);
  assert.match(update.sql, /newer\.kind_sequence>source\.kind_sequence/);
  assert.equal(update.params[4], INCREMENTAL_PARTIAL_RECOVERY_OPERATION_ID);
  const marker = JSON.parse(update.params[7]);
  assert.equal(marker.source_reason_code, source.source_reason_code);
  assert.equal(marker.source_observation_id, source.source_observation_id);
});

test("a prepared recovery retries the same completed Job without resetting attempts", async () => {
  const source = target({
    run_status: "queued",
    detail_status: "pending",
    video_status: "pending",
    recovery_marker: {
      operation_id: INCREMENTAL_PARTIAL_RECOVERY_OPERATION_ID,
      run_id: "incremental:e55313a2-99cd-5e7a-92fe-5d1349832570",
      channel_id: "UCpartialRecovery",
      plan_id: "e55313a2-99cd-5e7a-92fe-5d1349832570",
      job_id: "incremental__UCpartialRecovery__20260817__clock_20__d4080bd3f80d",
      attempts_made_before: 2,
    },
  });
  const { job, retryStates } = jobFixture(source);
  const inspection = await classifyIncrementalPartialRecoveryTarget(source, job);

  assert.equal(inspection.action, "retry_prepared");
  const result = await dispatchIncrementalPartialRecoveryTarget({
    async getJob(id) {
      assert.equal(id, source.job_id);
      return job;
    },
  }, inspection);

  assert.equal(result.action, "retried_completed");
  assert.deepEqual(retryStates, ["completed"]);
  assert.equal(job.attemptsMade, 2);
});

test("a finalized Run and an unsupported Partial reason fail closed", async () => {
  const finalized = target({ publication_finalized_at: "2026-08-17T04:30:00.000Z" });
  await assert.rejects(
    classifyIncrementalPartialRecoveryTarget(finalized, jobFixture(finalized).job),
    /already finalized/,
  );

  const unsupported = target({ source_reason_code: "video_cycle_unknown" });
  await assert.rejects(
    classifyIncrementalPartialRecoveryTarget(unsupported, jobFixture(unsupported).job),
    /unsupported Video Partial reason/,
  );
});
