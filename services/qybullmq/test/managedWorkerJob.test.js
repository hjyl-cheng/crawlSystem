import assert from "node:assert/strict";
import test from "node:test";
import { applyFailureRetryDecision, queuesByRole } from "../src/queues.js";
import { ProxyIdentityChangedError } from "../src/channelExecutionContext.js";
import {
  RotaBusinessRunBudgetExhaustedError,
  RotaExecutionBudgetExhaustedError,
  RotaSlotDeferredError,
} from "../src/rotaSlotAdapter.js";
import {
  activeChannelCandidateAttemptFence,
  channelCandidateFailureDisposition,
  classifyRetryableSystemFailure,
  clearChannelCandidateJobAttempt,
  failedChannelCandidateAttemptFence,
  markChannelCandidateJobAttemptActive,
  processManagedWorkerJob,
  recordChannelCandidateJobFailure,
  resolveMigrationSystemRetryItems,
  retryableSystemFailureDecision,
  settleChannelCandidateJobFailure,
} from "../src/managedWorkerJob.js";

function channelJob() {
  return {
    id: "channel-job-01",
    queueName: queuesByRole.channelCrawl,
    data: { candidate_id: 1, run_id: "run-01" },
  };
}

test("an Execution budget error consumes the current BullMQ attempt", async () => {
  const expected = new RotaExecutionBudgetExhaustedError();
  let deferred = false;
  let terminated = false;
  await assert.rejects(
    processManagedWorkerJob({
      job: channelJob(),
      token: "bullmq-lock-token",
      execute: async () => { throw expected; },
      terminateBusinessRun: async () => { terminated = true; },
      deferForSlotPause: async () => { deferred = true; },
    }),
    (error) => error === expected,
  );
  assert.equal(deferred, false);
  assert.equal(terminated, false);
});

test("a Business Run budget error is sent to terminal recovery", async () => {
  const budgetError = new RotaBusinessRunBudgetExhaustedError();
  const terminalError = Object.assign(new Error("terminal"), { code: "BUSINESS_RUN_BUDGET_EXHAUSTED" });
  let deferred = false;
  let terminated = false;
  await assert.rejects(
    processManagedWorkerJob({
      job: channelJob(),
      token: "bullmq-lock-token",
      execute: async () => { throw budgetError; },
      terminateBusinessRun: async (job, error) => {
        assert.equal(job.id, "channel-job-01");
        assert.equal(error, budgetError);
        terminated = true;
        throw terminalError;
      },
      deferForSlotPause: async () => { deferred = true; },
    }),
    (error) => error === terminalError,
  );
  assert.equal(terminated, true);
  assert.equal(deferred, false);
});

test("no Reserve remains a BullMQ delay without consuming an attempt", async () => {
  const deferredError = new RotaSlotDeferredError("no_reserve", { retryAfterMs: 2500 });
  const calls = [];
  const result = await processManagedWorkerJob({
    job: channelJob(),
    token: "bullmq-lock-token",
    execute: async () => { throw deferredError; },
    terminateBusinessRun: async () => assert.fail("Business Run must not terminate"),
    deferForSlotPause: async (job, token, options) => {
      calls.push({ job, token, options });
      return { delayed: true };
    },
  });
  assert.deepEqual(result, { delayed: true });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].job.id, "channel-job-01");
  assert.equal(calls[0].token, "bullmq-lock-token");
  assert.deepEqual(calls[0].options, { delayMs: 2500 });
});

test("a temporarily unavailable Route remains a BullMQ delay without consuming an attempt", async () => {
  const deferredError = new RotaSlotDeferredError("route_not_ready");
  let terminated = false;
  let delayed = false;
  const result = await processManagedWorkerJob({
    job: channelJob(),
    token: "bullmq-lock-token",
    execute: async () => { throw deferredError; },
    terminateBusinessRun: async () => { terminated = true; },
    deferForSlotPause: async (_job, _token, { delayMs }) => {
      delayed = true;
      assert.equal(delayMs, 5000);
      return { delayed: true };
    },
  });

  assert.deepEqual(result, { delayed: true });
  assert.equal(delayed, true);
  assert.equal(terminated, false);
});

test("the failed listener separates Business Run exhaustion from Route system failure", () => {
  assert.equal(channelCandidateFailureDisposition({
    error: new RotaBusinessRunBudgetExhaustedError(),
    attemptsMade: 1,
    maxAttempts: 3,
  }), "preserve");
  assert.equal(channelCandidateFailureDisposition({
    error: new RotaExecutionBudgetExhaustedError(),
    attemptsMade: 1,
    maxAttempts: 3,
  }), "retryable_system_failure");
  assert.equal(channelCandidateFailureDisposition({
    error: new Error("last attempt failed"),
    attemptsMade: 3,
    maxAttempts: 3,
  }), "failed");
});

test("real Worker control errors are classified without spending channel business budget", () => {
  const cases = [
    ["EXECUTION_ROUTE_BUDGET_EXHAUSTED", "route"],
    ["POLICY_UNAVAILABLE", "route"],
    ["MANAGED_POLICY_UNAVAILABLE", "route"],
    ["BUSINESS_RUN_KEY_CONFLICT", "identity"],
    ["MANAGED_JOB_INTENT_CONFLICT", "identity"],
    ["MIGRATION_RETRY_INTENT_CONFLICT", "identity"],
    ["BUSINESS_RUN_BUDGET_RECOVERY_FAILED", "fence"],
  ];

  for (const [code, category] of cases) {
    const error = Object.assign(new Error(code), { code });
    assert.equal(classifyRetryableSystemFailure(error)?.category, category, code);
    assert.equal(channelCandidateFailureDisposition({
      error,
      attemptsMade: 3,
      maxAttempts: 3,
      permanentFailure: true,
    }), "retryable_system_failure", code);
  }
});

test("a control-plane failure is retryable_system_failure even on the last BullMQ attempt", () => {
  const error = Object.assign(new Error("Rota Lease expired"), {
    name: "ProxyControlRequestError",
    code: "LEASE_GONE",
    status: 410,
    retryable: false,
  });

  assert.equal(channelCandidateFailureDisposition({
    error,
    attemptsMade: 3,
    maxAttempts: 3,
    permanentFailure: true,
  }), "retryable_system_failure");
});

test("a control-plane failure cannot be discarded before the failed listener classifies it", () => {
  const error = Object.assign(new Error("invalid status from fingerprint target"), {
    code: "FINGERPRINT_INVALID_TARGET_STATUS",
    status: 502,
  });
  let discarded = false;
  const decision = retryableSystemFailureDecision(error);

  assert.equal(decision.kind, "retryable_system_failure");
  assert.equal(decision.retry_mode, "system_retry");
  assert.equal(decision.evidence.category, "fingerprint_gateway");
  assert.deepEqual(applyFailureRetryDecision({
    discard() { discarded = true; },
  }, decision), {
    retry: true,
    retry_mode: "system_retry",
    requires_new_identity: false,
  });
  assert.equal(discarded, false);
});

test("proxy identity drift is a structured retryable system failure", () => {
  const error = new ProxyIdentityChangedError(
    { proxy_id: 7, proxy_address_hash: "before" },
    { proxy_id: 7, proxy_address_hash: "after" },
  );

  assert.equal(error.code, "PROXY_IDENTITY_CHANGED");
  assert.deepEqual(classifyRetryableSystemFailure(error), {
    failure_type: "retryable_system_failure",
    category: "identity",
    code: "PROXY_IDENTITY_CHANGED",
    name: "ProxyIdentityChangedError",
    message: "proxy identity changed during channel attempt: expected 7, received 7",
    status: null,
    retryable: true,
  });
  assert.equal(channelCandidateFailureDisposition({
    error,
    attemptsMade: 3,
    maxAttempts: 3,
    permanentFailure: true,
  }), "retryable_system_failure");
});

test("Candidate attempt Fences use the active and failed BullMQ attempt clocks", () => {
  const job = {
    id: "channel-job-01",
    attemptsMade: 2,
    data: { candidate_id: 42, dispatch_generation: 7 },
  };

  assert.deepEqual(activeChannelCandidateAttemptFence(job), {
    candidateId: 42,
    dispatchGeneration: 7,
    jobId: "channel-job-01",
    bullmqAttempt: 3,
  });
  assert.deepEqual(failedChannelCandidateAttemptFence(job), {
    candidateId: 42,
    dispatchGeneration: 7,
    jobId: "channel-job-01",
    bullmqAttempt: 2,
  });
});

test("a Candidate attempt claim is monotonic within one dispatch generation", async () => {
  let statement = null;
  const updated = await markChannelCandidateJobAttemptActive(async (sql, params) => {
    statement = { sql, params };
    return { rowCount: 1, rows: [{}] };
  }, {
    id: "channel-job-01",
    attemptsMade: 1,
    data: { candidate_id: 42, dispatch_generation: 7 },
  });

  assert.equal(updated, true);
  assert.match(statement.sql, /snapshot_active_job_attempt<=\$3/);
  assert.match(statement.sql, /snapshot_dispatch_generation=\$4/);
  assert.match(statement.sql, /status IN \('discovered','queued','validating','accepted'\)/);
  assert.deepEqual(statement.params, [42, "channel-job-01", 2, 7]);
});

test("a completed Job only clears its own active Candidate attempt", async () => {
  let statement = null;
  const cleared = await clearChannelCandidateJobAttempt(async (sql, params) => {
    statement = { sql, params };
    return { rowCount: 1, rows: [{}] };
  }, {
    id: "channel-job-01",
    attemptsMade: 2,
    data: { candidate_id: 42, dispatch_generation: 7 },
  });

  assert.equal(cleared, true);
  assert.match(statement.sql, /snapshot_dispatch_generation=\$2/);
  assert.match(statement.sql, /snapshot_active_job_id=\$3/);
  assert.match(statement.sql, /snapshot_active_job_attempt=\$4/);
  assert.deepEqual(statement.params, [42, 7, "channel-job-01", 2]);
});

test("a completed Job resolves only its active Migration system retry item", async () => {
  let statement = null;
  const resolved = await resolveMigrationSystemRetryItems(async (sql, params) => {
    statement = { sql, params };
    return { rowCount: 1, rows: [{ system_retry_id: 19 }] };
  }, {
    id: "channel-job-01",
    attemptsMade: 2,
    data: { candidate_id: 42, dispatch_generation: 7 },
  });

  assert.equal(resolved, 1);
  assert.match(statement.sql, /status IN \('retrying','pending','dispatched'\)/);
  assert.match(statement.sql, /failed_dispatch_generation=\$2/);
  assert.match(statement.sql, /retry_dispatch_generation=\$2/);
  assert.deepEqual(statement.params, [42, 7, "channel-job-01", "job_completed"]);
});

test("a preserved terminal failure still releases its own Candidate attempt Fence", async () => {
  const statements = [];
  const result = await settleChannelCandidateJobFailure(async (sql, params) => {
    statements.push({ sql, params });
    return { rowCount: 1, rows: [{ candidate_id: 42 }] };
  }, {
    id: "channel-job-01",
    attemptsMade: 2,
    data: { candidate_id: 42, dispatch_generation: 7 },
  }, {
    disposition: "preserve",
    message: "Business Run terminal state is already persisted",
  });

  assert.deepEqual(result, { recorded: false, fenceCleared: true });
  assert.equal(statements.length, 1);
  assert.match(statements[0].sql, /snapshot_active_job_id=\$3/);
  assert.deepEqual(statements[0].params, [42, 7, "channel-job-01", 2]);
});

test("a Candidate failed event is fenced by terminal state, generation and newer BullMQ attempt", async () => {
  let statement = null;
  const updated = await recordChannelCandidateJobFailure(async (sql, params) => {
    statement = { sql, params };
    return { rowCount: 0, rows: [] };
  }, {
    id: "channel-job-01",
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 2,
    data: { candidate_id: 42, dispatch_generation: 7 },
  }, {
    disposition: "queued",
    message: "late failure",
    snapshotPatch: { failure_kind: "unknown" },
  });

  assert.equal(updated, false);
  assert.match(statement.sql, /snapshot_dispatch_generation=\$5/);
  assert.match(
    statement.sql,
    /snapshot_json=\(COALESCE\(snapshot_json,'\{\}'::jsonb\)[\s\S]*- 'failure_type' - 'system_failure'\)[\s\S]*\|\| \$4::jsonb/,
  );
  assert.match(statement.sql, /status IN \('discovered','queued','validating'\)/);
  assert.match(statement.sql, /snapshot_active_job_id=\$6/);
  assert.match(statement.sql, /snapshot_active_job_attempt=\$7/);
  assert.deepEqual(statement.params, [
    42,
    "queued",
    "late failure",
    JSON.stringify({ failure_kind: "unknown" }),
    7,
    "channel-job-01",
    2,
  ]);
});

test("a legal terminal failure preserves the Candidate attempt Fence for atomic G+1 allocation", async () => {
  let statement = null;
  const result = await settleChannelCandidateJobFailure(async (sql, params) => {
    statement = { sql, params };
    return {
      rowCount: 1,
      rows: [{
        candidate_id: 482,
        status: "failed",
        snapshot_dispatch_generation: 1,
        snapshot_active_job_id: "channel-snapshot__legacy-results-canary__UC0Noar__g1",
        snapshot_active_job_attempt: 1,
        system_retry_id: 801,
      }],
    };
  }, {
    id: "channel-snapshot__legacy-results-canary__UC0Noar__g1",
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 1,
    data: {
      candidate_id: 482,
      dispatch_generation: 1,
      dispatch_batch_id: "legacy-results-canary",
    },
  }, {
    disposition: "failed",
    message: "retryable system failure reached a terminal BullMQ state",
  });

  assert.deepEqual(result, {
    recorded: true,
    fenceCleared: false,
  });
  assert.doesNotMatch(
    statement.sql,
    /SET[\s\S]*snapshot_active_job_id=NULL[\s\S]*WHERE/,
  );
  assert.match(statement.sql, /snapshot_active_job_id=\$6/);
  assert.match(statement.sql, /snapshot_active_job_attempt=\$7/);
});

test("retryable_system_failure restores Candidate business budget under the exact attempt Fence", async () => {
  const error = Object.assign(new Error("Rota Lease expired"), {
    name: "ProxyControlRequestError",
    code: "LEASE_GONE",
    status: 410,
    retryable: false,
  });
  let statement = null;
  const result = await settleChannelCandidateJobFailure(async (sql, params) => {
    statement = { sql, params };
    return {
      rowCount: 1,
      rows: [{
        candidate_id: 482,
        status: "failed",
        snapshot_dispatch_generation: 1,
        snapshot_active_job_id: "channel-snapshot__legacy-results-canary__UC0Noar__g1",
        snapshot_active_job_attempt: 1,
        system_retry_id: 801,
      }],
    };
  }, {
    id: "channel-snapshot__legacy-results-canary__UC0Noar__g1",
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 1,
    data: {
      candidate_id: 482,
      dispatch_generation: 1,
      dispatch_batch_id: "legacy-results-canary",
    },
  }, {
    disposition: "retryable_system_failure",
    message: error.message,
    error,
    systemFailureTerminal: true,
  });

  assert.deepEqual(result, {
    recorded: true,
    fenceCleared: false,
    systemRetryRecorded: true,
  });
  assert.match(
    statement.sql,
    /snapshot_attempts=CASE[\s\S]*WHEN status='validating' THEN GREATEST\(snapshot_attempts-1,0\)/,
  );
  assert.match(statement.sql, /snapshot_active_job_id=\$5/);
  assert.match(statement.sql, /snapshot_active_job_attempt=\$6/);
  assert.match(statement.sql, /INSERT INTO crawler\.migration_system_retry_items/);
  assert.match(
    statement.sql,
    /migration_intent_id,candidate_id,failed_dispatch_batch_id,failed_dispatch_generation/,
  );
  assert.match(statement.sql, /UPDATE crawler\.migration_channel_intents/);
  assert.match(
    statement.sql,
    /ON CONFLICT \([\s\S]*migration_intent_id,failed_dispatch_generation,failed_job_id,failed_job_attempt[\s\S]*\) DO UPDATE/,
  );
  const evidence = JSON.parse(statement.params[2]);
  assert.equal(evidence.failure_type, "retryable_system_failure");
  assert.equal(evidence.system_failure.code, "LEASE_GONE");
  assert.equal(evidence.system_failure.category, "lease");
  assert.equal(evidence.system_failure.retryable, true);
  assert.equal(statement.params[7], "legacy-results-canary");
});

test("a system failure after Candidate acceptance preserves the business terminal and retry Fence", async () => {
  const error = Object.assign(new Error("Rota Lease changed after admission"), {
    code: "LEASE_CONFLICT",
    status: 409,
  });
  let statement = null;
  const result = await settleChannelCandidateJobFailure(async (sql, params) => {
    statement = { sql, params };
    return {
      rowCount: 1,
      rows: [{
        candidate_id: 482,
        status: "accepted",
        snapshot_dispatch_generation: 1,
        snapshot_active_job_id: "channel-snapshot__legacy-results-canary__UC0Noar__g1",
        snapshot_active_job_attempt: 3,
        system_retry_id: 801,
      }],
    };
  }, {
    id: "channel-snapshot__legacy-results-canary__UC0Noar__g1",
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 3,
    data: {
      candidate_id: 482,
      dispatch_generation: 1,
      dispatch_batch_id: "legacy-results-canary",
    },
  }, {
    disposition: "retryable_system_failure",
    message: error.message,
    error,
    systemFailureTerminal: true,
  });

  assert.deepEqual(result, {
    recorded: true,
    fenceCleared: false,
    systemRetryRecorded: true,
  });
  assert.match(
    statement.sql,
    /status=CASE\s+WHEN status='accepted' THEN 'accepted'/,
  );
  assert.match(
    statement.sql,
    /candidate\.status IN \('discovered','queued','validating','accepted'\)/,
  );
  assert.match(
    statement.sql,
    /WHEN status='accepted' THEN validation_finished_at/,
  );
  assert.match(statement.sql, /INSERT INTO crawler\.migration_system_retry_items/);
  assert.match(statement.sql, /CASE WHEN \$7::boolean THEN 'pending' ELSE 'retrying' END/);
  assert.match(
    statement.sql,
    /AND NOT \(\s*retry\.migration_intent_id=intent\.migration_intent_id[\s\S]*retry\.failed_job_attempt=intent\.snapshot_active_job_attempt\s*\)/,
  );
});

test("an accepted non-Migration Candidate releases its Fence after terminal system failure", async () => {
  const error = Object.assign(new Error("Rota Lease changed after ordinary admission"), {
    code: "LEASE_CONFLICT",
    status: 409,
  });
  let statement = null;
  const result = await settleChannelCandidateJobFailure(async (sql, params) => {
    statement = { sql, params };
    return {
      rowCount: 1,
      rows: [{
        candidate_id: 900,
        status: "accepted",
        snapshot_dispatch_generation: 1,
        snapshot_active_job_id: null,
        snapshot_active_job_attempt: null,
        system_retry_id: null,
      }],
    };
  }, {
    id: "channel-snapshot__query-batch__UCordinary__g1",
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 3,
    data: {
      candidate_id: 900,
      dispatch_generation: 1,
      dispatch_batch_id: "query-batch",
    },
  }, {
    disposition: "retryable_system_failure",
    message: error.message,
    error,
    systemFailureTerminal: true,
  });

  assert.deepEqual(result, {
    recorded: true,
    fenceCleared: true,
    systemRetryRecorded: false,
  });
  assert.match(
    statement.sql,
    /matching_intent AS \([\s\S]*FROM crawler\.migration_channel_intents[\s\S]*target_candidate_id=\$1/,
  );
  assert.match(
    statement.sql,
    /snapshot_active_job_id=CASE[\s\S]*status='accepted'[\s\S]*NOT EXISTS \(SELECT 1 FROM matching_intent\)[\s\S]*THEN NULL/,
  );
  assert.match(
    statement.sql,
    /snapshot_active_job_attempt=CASE[\s\S]*status='accepted'[\s\S]*NOT EXISTS \(SELECT 1 FROM matching_intent\)[\s\S]*THEN NULL/,
  );
});
