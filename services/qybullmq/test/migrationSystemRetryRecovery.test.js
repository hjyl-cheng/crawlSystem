import assert from "node:assert/strict";
import test from "node:test";

import {
  claimMigrationSystemRetryAgentJobFence,
  lockGenericFinalizeAgainstMigrationSystemRetry,
  lockMigrationSystemRetryFinalizeJobFence,
  lockMigrationSystemRetryAgentJobFence,
  migrationSystemRetryFinalizeJobFence,
  migrationSystemRetryAgentJobFence,
  MigrationSystemRetryRecoveryReconciler,
  representedMigrationSystemRetryAgentJob,
} from "../src/migrationSystemRetryRecovery.js";
import { finalizeDispatchRevision } from "../src/finalizePolicy.js";
import { queuesByRole } from "../src/queues.js";

function recoveryAgentJob(overrides = {}) {
  return {
    id: "migration-system-retry-agent:19:g2:run-19",
    name: "agent-profile-batch",
    attemptsStarted: 1,
    data: {
      migration_system_retry_id: 19,
      recovery_agent_job_epoch: 0,
      candidate_id: 482,
      dispatch_generation: 2,
      dispatch_batch_id: "batch-19",
      run_id: "run-19",
      channel_ids: ["UC19"],
    },
    ...overrides,
  };
}

function finalizeState(overrides = {}) {
  return {
    channel_id: "UC19",
    latest_run_id: "run-19",
    channel_status: "active",
    agent_status: "done",
    channel_updated_at: new Date("2026-08-30T10:00:00.000Z"),
    detail_status: "done",
    expected_content_count: 1,
    pipeline_cycle_id: "batch-19",
    run_final_repair: null,
    candidate_count: 1,
    candidate_updated_at: new Date("2026-08-30T10:01:00.000Z"),
    content_count: 1,
    content_updated_at: new Date("2026-08-30T10:02:00.000Z"),
    agent_updated_at: new Date("2026-08-30T10:03:00.000Z"),
    ...overrides,
  };
}

function recoveryFinalizeJob(state = finalizeState(), overrides = {}) {
  return {
    id: "finalize:run-19:revision",
    name: "finalize-channel",
    data: {
      migration_system_retry_id: 19,
      candidate_id: 482,
      dispatch_generation: 2,
      dispatch_batch_id: "batch-19",
      pipeline_cycle_id: "batch-19",
      run_id: "run-19",
      channel_id: "UC19",
      source_revision: finalizeDispatchRevision(state),
    },
    ...overrides,
  };
}

test("migration recovery Agent Fence binds logical and physical Worker identity", async () => {
  const fence = migrationSystemRetryAgentJobFence(recoveryAgentJob());
  assert.deepEqual(fence, {
    systemRetryId: 19,
    candidateId: 482,
    dispatchGeneration: 2,
    dispatchBatchId: "batch-19",
    runId: "run-19",
    channelId: "UC19",
    jobId: "migration-system-retry-agent:19:g2:run-19",
    jobAttempt: 1,
    jobEpoch: 0,
  });

  const statements = [];
  const accepted = await lockMigrationSystemRetryAgentJobFence({
    async query(sql, params) {
      statements.push({ sql: String(sql), params });
      return {
        rowCount: 1,
        rows: [{
          system_retry_id: "19",
          recovery_agent_job_epoch: "0",
          recovery_agent_active_job_id: "migration-system-retry-agent:19:g2:run-19",
          recovery_agent_active_job_attempt: "1",
        }],
      };
    },
  }, fence);

  assert.equal(accepted, true);
  assert.equal(statements.length, 4);
  assert.match(statements[0].sql, /migration-system-retry-lock:candidate[\s\S]*FOR UPDATE OF candidate/);
  assert.match(statements[1].sql, /migration-system-retry-lock:retry[\s\S]*FOR UPDATE OF retry/);
  assert.match(statements[1].sql, /retry\.recovery_agent_job_epoch=\$6/);
  assert.match(statements[1].sql, /retry\.recovery_agent_active_job_id=\$7/);
  assert.match(statements[1].sql, /retry\.recovery_agent_active_job_attempt=\$8/);
  assert.match(statements[2].sql, /migration-system-retry-lock:run[\s\S]*FOR UPDATE OF run/);
  assert.match(statements[3].sql, /migration-system-retry-lock:channel[\s\S]*FOR UPDATE OF channel/);
  assert.deepEqual(statements[1].params, [
    19,
    482,
    2,
    "batch-19",
    "run-19",
    0,
    "migration-system-retry-agent:19:g2:run-19",
    1,
  ]);
});

test("a legacy Recovery Agent Job is epoch zero only", () => {
  const legacy = recoveryAgentJob();
  delete legacy.data.recovery_agent_job_epoch;
  assert.equal(migrationSystemRetryAgentJobFence(legacy).jobEpoch, 0);
});

test("an invalid Recovery Agent epoch cannot represent epoch zero", () => {
  const expected = recoveryAgentJob();
  const legacy = recoveryAgentJob();
  delete legacy.data.recovery_agent_job_epoch;
  assert.equal(representedMigrationSystemRetryAgentJob(legacy, expected), true);
  assert.equal(representedMigrationSystemRetryAgentJob(recoveryAgentJob({
    data: {
      ...recoveryAgentJob().data,
      recovery_agent_job_epoch: "invalid",
    },
  }), expected), false);
  assert.equal(representedMigrationSystemRetryAgentJob(recoveryAgentJob({
    data: {
      ...recoveryAgentJob().data,
      recovery_agent_job_epoch: -1,
    },
  }), expected), false);
});

test("migration recovery Agent claim permits only monotonic same-Job takeover", async () => {
  let statement;
  const claimed = await claimMigrationSystemRetryAgentJobFence({
    async query(sql, params) {
      statement = { sql: String(sql), params };
      return { rowCount: 1, rows: [{ system_retry_id: "19" }] };
    },
  }, migrationSystemRetryAgentJobFence(recoveryAgentJob({ attemptsStarted: 2 })));

  assert.equal(claimed, true);
  assert.match(statement.sql, /recovery_agent_active_job_id IS NULL/);
  assert.match(statement.sql, /recovery_agent_active_job_id=\$7/);
  assert.match(statement.sql, /recovery_agent_active_job_attempt<=\$8/);
  assert.match(statement.sql, /SET recovery_agent_active_job_id=\$7/);
  assert.deepEqual(statement.params.slice(-3), [
    "migration-system-retry-agent:19:g2:run-19",
    2,
    0,
  ]);
});

test("migration recovery Agent Jobs reject incomplete execution identity", () => {
  assert.throws(
    () => migrationSystemRetryAgentJobFence(recoveryAgentJob({
      data: {
        ...recoveryAgentJob().data,
        run_id: null,
      },
    })),
    /identity is incomplete/,
  );
  assert.throws(
    () => migrationSystemRetryAgentJobFence(recoveryAgentJob({ attemptsStarted: 0 })),
    /identity is incomplete/,
  );
  assert.throws(
    () => migrationSystemRetryAgentJobFence(recoveryAgentJob({ id: null })),
    /identity is incomplete/,
  );
  assert.throws(
    () => migrationSystemRetryAgentJobFence(recoveryAgentJob({
      data: {
        ...recoveryAgentJob().data,
        recovery_agent_job_epoch: -1,
      },
    })),
    /identity is incomplete/,
  );
  assert.throws(
    () => migrationSystemRetryAgentJobFence(recoveryAgentJob({
      data: {
        ...recoveryAgentJob().data,
        channel_ids: ["UC19", "UC20"],
      },
    })),
    /identity is incomplete/,
  );
  assert.equal(migrationSystemRetryAgentJobFence({ data: {} }), null);
});

test("migration recovery Finalize Fence rejects a newer persisted source revision", async () => {
  const state = finalizeState();
  const fence = migrationSystemRetryFinalizeJobFence(recoveryFinalizeJob(state));
  assert.deepEqual(fence, {
    systemRetryId: 19,
    candidateId: 482,
    dispatchGeneration: 2,
    dispatchBatchId: "batch-19",
    runId: "run-19",
    channelId: "UC19",
    sourceRevision: finalizeDispatchRevision(state),
  });

  const statements = [];
  const client = {
    async query(sql, params) {
      statements.push({ sql: String(sql), params });
      return {
        rowCount: 1,
        rows: [{
          ...state,
          system_retry_id: "19",
          candidate_id: "482",
          retry_dispatch_generation: "2",
          failed_dispatch_batch_id: "batch-19",
          recovery_run_id: "run-19",
        }],
      };
    },
  };
  assert.equal(await lockMigrationSystemRetryFinalizeJobFence(client, fence), true);
  assert.equal(statements.length, 5);
  assert.match(statements[0].sql, /migration-system-retry-lock:candidate[\s\S]*FOR UPDATE OF candidate/);
  assert.match(statements[1].sql, /migration-system-retry-lock:retry[\s\S]*FOR UPDATE OF retry/);
  assert.match(statements[2].sql, /migration-system-retry-lock:run[\s\S]*FOR UPDATE OF run/);
  assert.match(statements[3].sql, /migration-system-retry-lock:channel[\s\S]*FOR UPDATE OF channel/);
  assert.match(statements[4].sql, /max\(source_candidate\.updated_at\)/);
  assert.match(statements[4].sql, /max\(COALESCE\(content\.last_enriched_at,content\.last_seen_at\)\)/);
  assert.doesNotMatch(statements[4].sql, /FOR UPDATE/);
  assert.deepEqual(statements[4].params, [19, 482, 2, "batch-19", "run-19", "UC19"]);

  client.query = async () => ({
    rowCount: 1,
    rows: [{
      ...state,
      candidate_updated_at: new Date("2026-08-30T10:04:00.000Z"),
    }],
  });
  assert.equal(await lockMigrationSystemRetryFinalizeJobFence(client, fence), false);
});

test("migration recovery Finalize Jobs reject incomplete execution identity", () => {
  assert.throws(
    () => migrationSystemRetryFinalizeJobFence(recoveryFinalizeJob(finalizeState(), {
      data: { ...recoveryFinalizeJob().data, candidate_id: null },
    })),
    /identity is incomplete/,
  );
  assert.equal(migrationSystemRetryFinalizeJobFence({ data: {} }), null);
});

test("generic Finalize locks its Candidate and rejects an active migration retry", async () => {
  const statements = [];
  const accepted = await lockGenericFinalizeAgainstMigrationSystemRetry({
    async query(sql, params) {
      statements.push({ sql: String(sql), params });
      if (statements.length === 1) {
        return {
          rowCount: 1,
          rows: [{
            candidate_id: "482",
            dispatch_batch_id: "batch-19",
            snapshot_dispatch_generation: "2",
          }],
        };
      }
      return { rowCount: 1, rows: [{ system_retry_id: "19" }] };
    },
  }, { channelId: "UC19", runId: "run-19" });

  assert.equal(accepted, false);
  assert.match(statements[0].sql, /FOR UPDATE OF candidate/);
  assert.deepEqual(statements[0].params, ["UC19", "run-19"]);
  assert.match(statements[1].sql, /status IN \('retrying','pending','dispatched'\)/);
  assert.match(statements[1].sql, /retry_dispatch_generation/);
  assert.deepEqual(statements[1].params, [482, "batch-19", 2]);
});

test("terminal Job cleanup adopts the same Job when it becomes active during remove", async () => {
  let state = "completed";
  let addCalls = 0;
  const existing = {
    id: "job-19",
    name: "agent-profile-batch",
    data: { token: "same" },
    getState: async () => state,
    remove: async () => {
      state = "active";
      throw new Error("job is locked by another worker");
    },
  };
  const queue = {
    getJob: async () => existing,
    add: async () => {
      addCalls += 1;
      return existing;
    },
  };
  const reconciler = new MigrationSystemRetryRecoveryReconciler({
    query: async () => ({ rows: [] }),
    withTransaction: async (action) => action({ query: async () => ({ rows: [] }) }),
    queues: { [queuesByRole.agentBatch]: queue },
  });

  const result = await reconciler.ensureQueueJob(
    queuesByRole.agentBatch,
    { id: "job-19", name: "agent-profile-batch", data: { token: "same" } },
    (job, expected) => job.name === expected.name && job.data.token === expected.data.token,
  );

  assert.equal(result.represented, true);
  assert.equal(result.state, "active");
  assert.equal(result.conflict, false);
  assert.equal(addCalls, 0);
});

test("terminal generic Job identity is replaced by the fenced recovery Job", async () => {
  let existing = {
    id: "content-detail:run-19",
    name: "content-detail-batch",
    data: { migration_system_retry_id: null },
    getState: async () => "completed",
    remove: async () => { existing = null; },
  };
  let addCalls = 0;
  const queue = {
    getJob: async () => existing,
    add: async (name, data, { jobId }) => {
      addCalls += 1;
      existing = { id: jobId, name, data, getState: async () => "waiting" };
      return existing;
    },
  };
  const reconciler = new MigrationSystemRetryRecoveryReconciler({
    query: async () => ({ rows: [] }),
    withTransaction: async (action) => action({ query: async () => ({ rows: [] }) }),
    queues: { [queuesByRole.contentDetail]: queue },
  });
  const expected = {
    id: "content-detail:run-19",
    name: "content-detail-batch",
    data: { migration_system_retry_id: 19 },
  };

  const result = await reconciler.ensureQueueJob(
    queuesByRole.contentDetail,
    expected,
    (job, recovery) => (
      job.name === recovery.name
      && job.data.migration_system_retry_id === recovery.data.migration_system_retry_id
    ),
  );

  assert.equal(result.created, true);
  assert.equal(result.terminalRequeued, true);
  assert.equal(result.conflict, false);
  assert.equal(addCalls, 1);
  assert.deepEqual(existing.data, expected.data);
});

test("a represented failed or completed Content Detail Job waits for durable replay evidence", async () => {
  for (const terminalState of ["failed", "completed"]) {
    let removed = false;
    let addCalls = 0;
    let observedState = null;
    const existing = {
      id: "content-detail:run-19",
      name: "content-detail-batch",
      data: { migration_system_retry_id: 19, content_detail_job_epoch: 0 },
      getState: async () => terminalState,
      remove: async () => { removed = true; },
    };
    const queue = {
      getJob: async () => existing,
      add: async () => { addCalls += 1; },
    };
    const reconciler = new MigrationSystemRetryRecoveryReconciler({
      query: async () => ({ rows: [] }),
      withTransaction: async (action) => action({ query: async () => ({ rows: [] }) }),
      queues: { [queuesByRole.contentDetail]: queue },
    });
    const expected = {
      id: existing.id,
      name: existing.name,
      data: { ...existing.data },
    };

    const result = await reconciler.ensureQueueJob(
      queuesByRole.contentDetail,
      expected,
      (job, recovery) => (
        job.name === recovery.name
        && job.data.migration_system_retry_id === recovery.data.migration_system_retry_id
        && job.data.content_detail_job_epoch === recovery.data.content_detail_job_epoch
      ),
      {
        allowTerminalRequeue: async ({ state }) => {
          observedState = state;
          return false;
        },
      },
    );

    assert.equal(observedState, terminalState);
    assert.equal(result.terminalBlocked, true);
    assert.equal(result.conflict, false);
    assert.equal(removed, false);
    assert.equal(addCalls, 0);
  }
});

test("active and legacy recovery scans are both selected under a permanent active backlog", async () => {
  const selectedKinds = [];
  const reconciler = new MigrationSystemRetryRecoveryReconciler({
    query: async (_sql, params) => ({
      rows: [{
        system_retry_id: params[3] ? "10" : "20",
        status: params[3] ? "dispatched" : "resolved",
        resolution: params[3] ? null : "job_completed",
      }],
    }),
    withTransaction: async (action) => action({ query: async () => ({ rows: [] }) }),
    queues: {},
  });

  for (let index = 0; index < 5; index += 1) {
    const rows = await reconciler.loadRecoveries(1);
    selectedKinds.push(rows[0].status === "resolved" ? "legacy" : "active");
  }

  assert.deepEqual(selectedKinds, ["active", "active", "active", "active", "legacy"]);
});
