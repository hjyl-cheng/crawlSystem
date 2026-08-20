import assert from "node:assert/strict";
import test from "node:test";

import {
  MIGRATION_BUSINESS_RUN_INTENT_RECOVERY_OPERATION_ID,
  MIGRATION_BUSINESS_RUN_INTENT_TARGET_SQL,
  classifyMigrationBusinessRunIntentRecoveryTarget,
  dispatchMigrationBusinessRunIntentRecoveryTarget,
  prepareMigrationBusinessRunIntentRecoveryTargets,
  recoverMigrationBusinessRunIntentFailures,
} from "../src/migrationBusinessRunIntentRecovery.js";
import { MIGRATION_PROXY_CONTROL_RECOVERY_OPERATION_ID } from "../src/migrationProxyControlRecovery.js";

const batchId = "ytdlp-comments-migration-1000-20260818-v1";
const candidateId = 30999;
const channelId = "UC67I0ZT05l5MTjn1K7bBlTQ";
const jobId = `channel-snapshot__${batchId}__${channelId}`;
const businessRunKey = `full-candidate:${candidateId}`;
const businessRunId = "run:302e6f8e-e871-471d-bcca-394a3275c8df";

function originalMarker() {
  return {
    operation_id: MIGRATION_PROXY_CONTROL_RECOVERY_OPERATION_ID,
    candidate_id: candidateId,
    dispatch_batch_id: batchId,
    channel_id: channelId,
    job_id: jobId,
    attempts_made_before: 3,
  };
}

function compatibilityMarker() {
  return {
    operation_id: MIGRATION_BUSINESS_RUN_INTENT_RECOVERY_OPERATION_ID,
    candidate_id: candidateId,
    dispatch_batch_id: batchId,
    channel_id: channelId,
    job_id: jobId,
    attempts_made_before: 4,
    binding_business_run_key: businessRunKey,
    binding_business_run_id: businessRunId,
    binding_intent_hash: "sha256:old-intent",
  };
}

function target(overrides = {}) {
  return {
    candidate_id: String(candidateId),
    dispatch_batch_id: batchId,
    pipeline_cycle_id: batchId,
    channel_id: channelId,
    channel_url: `https://www.youtube.com/channel/${channelId}`,
    status: "failed",
    snapshot_attempts: 0,
    error_message: `${businessRunKey.replace("full-candidate", "BUSINESS_RUN_KEY_CONFLICT: full-candidate")}: BUSINESS_RUN_KEY_CONFLICT`,
    validation_started_at: null,
    validation_finished_at: "2026-08-18T08:01:13.719Z",
    original_recovery_marker: originalMarker(),
    compatibility_recovery_marker: null,
    run_count: 0,
    binding_business_run_key: businessRunKey,
    binding_business_run_id: businessRunId,
    binding_status: "reserved",
    binding_terminal_reason: null,
    binding_run_kind: "full",
    binding_channel_id: channelId,
    binding_candidate_id: String(candidateId),
    binding_intent_hash: "sha256:old-intent",
    binding_intent_json: {
      intent: {
        job_name: "channel-snapshot",
        crawl_mode: "full",
      },
    },
    ...overrides,
  };
}

function jobFixture(source = target(), overrides = {}) {
  const retried = [];
  const job = {
    id: jobId,
    name: "channel-snapshot",
    data: {
      candidate_id: candidateId,
      dispatch_batch_id: batchId,
      pipeline_cycle_id: batchId,
      channel_id: channelId,
      business_run_key: businessRunKey,
      run_id: businessRunId,
    },
    attemptsMade: 4,
    attemptsStarted: 4,
    failedReason: `BUSINESS_RUN_KEY_CONFLICT: ${businessRunKey}`,
    async getState() { return "failed"; },
    async retry(state) { retried.push(state); },
    ...overrides,
  };
  return { job, retried, source };
}

test("the compatibility selector requires the original recovery, old binding, and no Run", () => {
  assert.match(MIGRATION_BUSINESS_RUN_INTENT_TARGET_SQL, /dispatch_batch_id=\$1/);
  assert.match(MIGRATION_BUSINESS_RUN_INTENT_TARGET_SQL, /source_json #> \$2::text\[\]/);
  assert.match(MIGRATION_BUSINESS_RUN_INTENT_TARGET_SQL, /source_json #> \$3::text\[\]/);
  assert.match(MIGRATION_BUSINESS_RUN_INTENT_TARGET_SQL, /business_run_bindings/);
  assert.match(MIGRATION_BUSINESS_RUN_INTENT_TARGET_SQL, /NOT EXISTS/);
});

test("the exact old-Intent conflict is classified for one audited retry", async () => {
  const source = target();
  const inspection = await classifyMigrationBusinessRunIntentRecoveryTarget(
    source,
    jobFixture(source).job,
  );

  assert.equal(inspection.action, "prepare_and_retry");
  assert.equal(inspection.candidate_id, candidateId);
  assert.equal(inspection.attempts_made_before, 4);
  assert.equal(inspection.binding_business_run_id, businessRunId);
});

test("preparation changes only the Candidate state and appends a second audit marker", async () => {
  const source = target();
  const inspection = await classifyMigrationBusinessRunIntentRecoveryTarget(
    source,
    jobFixture(source).job,
  );
  const calls = [];
  const client = {
    async query(sql, params) {
      calls.push({ sql, params });
      if (sql.includes("FROM crawler.settings")) {
        return { rows: [{ value_json: { status: "stopped" } }], rowCount: 1 };
      }
      if (sql.includes("UPDATE crawler.channel_candidates")) {
        return { rows: [{ candidate_id: candidateId }], rowCount: 1 };
      }
      return { rows: [{}], rowCount: 1 };
    },
  };

  const prepared = await prepareMigrationBusinessRunIntentRecoveryTargets(
    client,
    [inspection],
    { batchId, now: new Date("2026-08-18T09:00:00.000Z") },
  );

  assert.deepEqual(prepared, [candidateId]);
  assert.equal(calls.some(({ sql }) => /UPDATE crawler\.business_run_bindings/.test(sql)), false);
  const update = calls.find(({ sql }) => sql.includes("UPDATE crawler.channel_candidates"));
  assert.match(update.sql, /status='queued'/);
  assert.match(update.sql, /NOT EXISTS/);
  assert.equal(update.params[0], candidateId);
  assert.equal(update.params[1], batchId);
  assert.equal(update.params[3], MIGRATION_BUSINESS_RUN_INTENT_RECOVERY_OPERATION_ID);
  const marker = JSON.parse(update.params[4]);
  assert.equal(marker.attempts_made_before, 4);
  assert.equal(marker.binding_business_run_id, businessRunId);
  assert.equal(marker.binding_intent_hash, "sha256:old-intent");
});

test("a prepared compatibility recovery retries the same exhausted Job", async () => {
  const source = target({
    status: "queued",
    error_message: null,
    compatibility_recovery_marker: compatibilityMarker(),
  });
  const { job, retried } = jobFixture(source);
  const inspection = await classifyMigrationBusinessRunIntentRecoveryTarget(source, job);
  assert.equal(inspection.action, "retry_prepared");

  const result = await dispatchMigrationBusinessRunIntentRecoveryTarget({
    async getJob(id) {
      assert.equal(id, jobId);
      return job;
    },
  }, inspection);

  assert.equal(result.action, "retried_failed");
  assert.deepEqual(retried, ["failed"]);
});

test("a completed factual skip accepts the matching terminal Binding", async () => {
  const source = target({
    status: "rejected",
    error_message: null,
    binding_status: "terminal",
    binding_terminal_reason: "channel_unavailable",
    compatibility_recovery_marker: compatibilityMarker(),
  });
  const completedJob = jobFixture(source, {
    attemptsMade: 5,
    attemptsStarted: 5,
    failedReason: undefined,
    returnvalue: {
      ok: true,
      skipped: true,
      skip_reason: "channel_unavailable",
      candidate_id: candidateId,
      channel_id: channelId,
    },
    async getState() { return "completed"; },
  }).job;

  const inspection = await classifyMigrationBusinessRunIntentRecoveryTarget(
    source,
    completedJob,
  );
  assert.equal(inspection.action, "recovery_completed");

  const mismatchedReasonJob = {
    ...completedJob,
    returnvalue: { ...completedJob.returnvalue, skip_reason: "another_reason" },
  };
  await assert.rejects(
    classifyMigrationBusinessRunIntentRecoveryTarget(source, mismatchedReasonJob),
    /terminal reason/,
  );
});

test("new-format binding, an existing Run, or a mismatched Job fails closed", async () => {
  await assert.rejects(
    classifyMigrationBusinessRunIntentRecoveryTarget(target({
      binding_intent_json: {
        intent: { checkpoint_target_run_id: null },
      },
    }), jobFixture().job),
    /old-format Binding/,
  );
  await assert.rejects(
    classifyMigrationBusinessRunIntentRecoveryTarget(
      target({ run_count: 1 }),
      jobFixture().job,
    ),
    /not an original compatibility failure/,
  );
  const mismatchedJob = jobFixture().job;
  mismatchedJob.data.business_run_key = "full-candidate:99999";
  await assert.rejects(
    classifyMigrationBusinessRunIntentRecoveryTarget(
      target(),
      mismatchedJob,
    ),
    /conflicting business_run_key/,
  );
});

test("the expected count gate prevents any compatibility recovery write", async () => {
  let transactionCalls = 0;
  const source = target();
  await assert.rejects(
    recoverMigrationBusinessRunIntentFailures({
      async query() { return { rows: [source] }; },
      async withTransaction() { transactionCalls += 1; },
      queue: { async getJob() { return jobFixture(source).job; } },
      batchId,
      expectedCount: 2,
      apply: true,
      dispatchLimit: 1,
    }),
    /target count changed: expected 2, got 1/,
  );
  assert.equal(transactionCalls, 0);
});
