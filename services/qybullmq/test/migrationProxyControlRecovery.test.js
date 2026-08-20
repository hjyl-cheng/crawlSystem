import assert from "node:assert/strict";
import test from "node:test";

import {
  MIGRATION_PROXY_CONTROL_FAILURE,
  MIGRATION_PROXY_CONTROL_RECOVERY_OPERATION_ID,
  MIGRATION_PROXY_CONTROL_TARGET_SQL,
  assertMigrationRecoverySchedulerInactive,
  classifyMigrationProxyControlRecoveryTarget,
  dispatchMigrationProxyControlRecoveryTarget,
  migrationProxyControlDispatchCapacity,
  prepareMigrationProxyControlRecoveryTargets,
  recoverMigrationProxyControlFailures,
} from "../src/migrationProxyControlRecovery.js";

const batchId = "ytdlp-comments-migration-1000-20260818-v1";

function target(overrides = {}) {
  return {
    candidate_id: "30962",
    dispatch_batch_id: batchId,
    pipeline_cycle_id: batchId,
    channel_id: "UCLU6yeXTYNTaNOq7LUGzDcw",
    channel_url: "https://www.youtube.com/channel/UCLU6yeXTYNTaNOq7LUGzDcw",
    priority: 100,
    status: "failed",
    snapshot_attempts: 0,
    error_message: MIGRATION_PROXY_CONTROL_FAILURE,
    validation_started_at: null,
    validation_finished_at: "2026-08-18T03:35:09.246Z",
    recovery_marker: null,
    run_count: 0,
    ...overrides,
  };
}

function jobFixture(source = target(), overrides = {}) {
  const retried = [];
  const job = {
    id: `channel-snapshot__${batchId}__${source.channel_id}`,
    name: "channel-snapshot",
    data: {
      candidate_id: Number(source.candidate_id),
      dispatch_batch_id: source.dispatch_batch_id,
      pipeline_cycle_id: source.pipeline_cycle_id,
      channel_id: source.channel_id,
    },
    attemptsMade: 3,
    failedReason: "proxy control request failed",
    async getState() { return "failed"; },
    async retry(state) { retried.push(state); },
    ...overrides,
  };
  return { job, retried };
}

test("the selector is limited to exact pre-Run BUG-043 failures and persisted markers", () => {
  assert.match(MIGRATION_PROXY_CONTROL_TARGET_SQL, /candidate\.dispatch_batch_id=\$1/);
  assert.match(MIGRATION_PROXY_CONTROL_TARGET_SQL, /candidate\.error_message=\$2/);
  assert.match(MIGRATION_PROXY_CONTROL_TARGET_SQL, /candidate\.snapshot_attempts=0/);
  assert.match(MIGRATION_PROXY_CONTROL_TARGET_SQL, /candidate\.validation_started_at IS NULL/);
  assert.match(MIGRATION_PROXY_CONTROL_TARGET_SQL, /NOT EXISTS/);
  assert.match(MIGRATION_PROXY_CONTROL_TARGET_SQL, /source_json #> \$3::text\[\]/);
});

test("an original failed Candidate and Job are classified for preparation", async () => {
  const source = target();
  const inspection = await classifyMigrationProxyControlRecoveryTarget(
    source,
    jobFixture(source).job,
  );
  assert.equal(inspection.action, "prepare_and_retry");
  assert.equal(inspection.candidate_id, 30962);
  assert.equal(inspection.attempts_made_before, 3);
});

test("preparation preserves Candidate and batch identity and records the recovery", async () => {
  const source = target();
  const inspection = await classifyMigrationProxyControlRecoveryTarget(
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
        return { rows: [{ candidate_id: source.candidate_id }], rowCount: 1 };
      }
      return { rows: [{}], rowCount: 1 };
    },
  };
  const prepared = await prepareMigrationProxyControlRecoveryTargets(client, [inspection], {
    batchId,
    now: new Date("2026-08-18T04:30:00.000Z"),
  });

  assert.deepEqual(prepared, [30962]);
  const update = calls.find(({ sql }) => sql.includes("UPDATE crawler.channel_candidates"));
  assert.match(update.sql, /status='queued'/);
  assert.match(update.sql, /candidate\.dispatch_batch_id=\$2/);
  assert.match(update.sql, /NOT EXISTS/);
  assert.equal(update.params[0], 30962);
  assert.equal(update.params[1], batchId);
  assert.equal(update.params[3], MIGRATION_PROXY_CONTROL_RECOVERY_OPERATION_ID);
  const marker = JSON.parse(update.params[4]);
  assert.equal(marker.candidate_id, 30962);
  assert.equal(marker.dispatch_batch_id, batchId);
  assert.equal(marker.attempts_made_before, 3);
});

test("a prepared recovery retries the same failed BullMQ Job", async () => {
  const source = target({
    status: "queued",
    error_message: null,
    recovery_marker: {
      operation_id: MIGRATION_PROXY_CONTROL_RECOVERY_OPERATION_ID,
      candidate_id: 30962,
      dispatch_batch_id: batchId,
      channel_id: "UCLU6yeXTYNTaNOq7LUGzDcw",
      job_id: `channel-snapshot__${batchId}__UCLU6yeXTYNTaNOq7LUGzDcw`,
      attempts_made_before: 3,
    },
  });
  const { job, retried } = jobFixture(source);
  const inspection = await classifyMigrationProxyControlRecoveryTarget(source, job);
  assert.equal(inspection.action, "retry_prepared");

  const result = await dispatchMigrationProxyControlRecoveryTarget({
    async getJob(id) {
      assert.equal(id, job.id);
      return job;
    },
  }, inspection);
  assert.equal(result.action, "retried_failed");
  assert.deepEqual(retried, ["failed"]);
});

test("wrong error, existing Run, conflicting Job, and active Scheduler fail closed", async () => {
  await assert.rejects(
    classifyMigrationProxyControlRecoveryTarget(
      target({ error_message: "another error" }),
      jobFixture().job,
    ),
    /not an original BUG-043 failure/,
  );
  await assert.rejects(
    classifyMigrationProxyControlRecoveryTarget(
      target({ run_count: 1 }),
      jobFixture().job,
    ),
    /not an original BUG-043 failure/,
  );
  await assert.rejects(
    classifyMigrationProxyControlRecoveryTarget(
      target(),
      jobFixture(target(), { id: "another-job" }).job,
    ),
    /conflicting job_id/,
  );
  await assert.rejects(
    assertMigrationRecoverySchedulerInactive({
      async query() {
        return { rows: [{ value_json: { status: "finishing", pipeline_cycle_id: batchId } }] };
      },
    }, batchId),
    /requires an inactive Scheduler/,
  );
});

test("the expected target count gate fails before any recovery write", async () => {
  let transactionCalls = 0;
  const source = target();
  await assert.rejects(
    recoverMigrationProxyControlFailures({
      async query() { return { rows: [source] }; },
      async withTransaction() { transactionCalls += 1; },
      queue: {
        async getJob() { return jobFixture(source).job; },
      },
      batchId,
      expectedCount: 2,
      apply: true,
      dispatchLimit: 1,
    }),
    /target count changed: expected 2, got 1/,
  );
  assert.equal(transactionCalls, 0);
});

test("paused Channel Jobs consume the BUG-043 recovery high-water budget", () => {
  assert.deepEqual(migrationProxyControlDispatchCapacity({
    waiting: 0,
    active: 0,
    delayed: 0,
    prioritized: 0,
    "waiting-children": 0,
    paused: 20,
  }, 20), {
    pressure: 20,
    dispatchLimit: 0,
  });
  assert.deepEqual(migrationProxyControlDispatchCapacity({
    active: 6,
    paused: 3,
  }, 20), {
    pressure: 9,
    dispatchLimit: 11,
  });
});
