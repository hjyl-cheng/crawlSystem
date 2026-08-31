import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";

import {
  BusinessRunBudgetRecoveryError,
  terminateExhaustedBusinessRun,
} from "../src/businessRunBudgetRecovery.js";
import { UnrecoverableError } from "bullmq";

const { Pool } = pg;
const integrationUrl = process.env.INCREMENTAL_POSTGRES_TEST_URL;

async function transaction(pool, action) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await action(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function fixture(pool, {
  materializedRun = false,
  runStatus = "running",
  dispatchGeneration = 1,
  candidateStatus = "queued",
  activeJobAttempt = 1,
} = {}) {
  const suffix = randomUUID();
  const batchId = `budget-recovery:${suffix}`;
  const channelId = `UC${suffix.replaceAll("-", "").slice(0, 22)}`;
  const runId = `run:budget:${suffix}`;
  const businessRunKey = `full-candidate:budget:${suffix}`;
  const jobId = `budget-job:${suffix}`;
  const candidateId = await transaction(pool, async (client) => {
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,result_json
       ) VALUES ($1,$1,'failed','{}'::jsonb)`,
      [batchId],
    );
    await client.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,status)
       VALUES ($1,$2,'active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    const candidate = await client.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt
       ) VALUES ($1,$1,$2,$3,$4,$5,$6,$7)
       RETURNING candidate_id`,
      [
        batchId,
        channelId,
        `https://www.youtube.com/channel/${channelId}`,
        candidateStatus,
        dispatchGeneration,
        jobId,
        activeJobAttempt,
      ],
    );
    const id = Number(candidate.rows[0].candidate_id);
    await client.query(
      `INSERT INTO crawler.business_run_bindings (
         business_run_key,business_run_id,intent_hash,intent_json,
         identity_policy_id,identity_policy_version,identity_policy_hash,
         run_kind,channel_id,candidate_id,status,materialized_at
       ) VALUES ($1,$2,'sha256:test','{}'::jsonb,
                 'channel-sticky-v1',1,'sha256:policy','full',$3,$4,$5,
                 CASE WHEN $5='materialized' THEN now() ELSE NULL END)`,
      [
        businessRunKey,
        runId,
        channelId,
        id,
        materializedRun ? "materialized" : "reserved",
      ],
    );
    if (materializedRun) {
      await client.query(
        `INSERT INTO crawler.channel_runs (
           run_id,channel_id,status,detail_status,crawl_mode
         ) VALUES ($1,$2,$3,$4,'full')`,
        [runId, channelId, runStatus, runStatus === "done" ? "done" : "running"],
      );
    }
    return id;
  });
  return {
    activeJobAttempt,
    batchId,
    businessRunKey,
    candidateId,
    channelId,
    dispatchGeneration,
    jobId,
    runId,
  };
}

async function cleanup(pool, value) {
  await transaction(pool, async (client) => {
    await client.query(
      "DELETE FROM crawler.migration_system_retry_items WHERE candidate_id=$1",
      [value.candidateId],
    );
    await client.query(
      "DELETE FROM crawler.migration_channel_intents WHERE target_candidate_id=$1",
      [value.candidateId],
    );
    await client.query(
      "DELETE FROM crawler.business_run_bindings WHERE business_run_key=$1",
      [value.businessRunKey],
    );
    await client.query("DELETE FROM crawler.channel_runs WHERE run_id=$1", [value.runId]);
    await client.query(
      "DELETE FROM crawler.query_dispatch_batches WHERE dispatch_batch_id=$1",
      [value.batchId],
    );
    await client.query("DELETE FROM crawler.channels WHERE channel_id=$1", [value.channelId]);
  });
}

function budgetJob(value) {
  return {
    id: value.jobId,
    queueName: "youtube-channel-crawl",
    name: "channel-crawl",
    attemptsStarted: value.activeJobAttempt,
    data: {
      business_run_key: value.businessRunKey,
      candidate_id: value.candidateId,
      channel_id: value.channelId,
      dispatch_generation: value.dispatchGeneration,
    },
  };
}

async function addDispatchedSystemRetry(pool, value) {
  return transaction(pool, async (client) => {
    const intent = await client.query(
      `INSERT INTO crawler.migration_channel_intents (
         source_id,source_database,source_database_oid,source_candidate_id,
         channel_id,source_snapshot,snapshot_sha256,target_candidate_id,
         first_dispatch_batch_id,dispatch_attempts,last_dispatch_at
       ) VALUES (
         $1,current_database(),
         (SELECT oid FROM pg_database WHERE datname=current_database()),$2,
         $3,'{}'::jsonb,repeat('a',64),$2,$4,$5,now()
       ) RETURNING migration_intent_id`,
      [
        `budget-recovery:${value.candidateId}`,
        value.candidateId,
        value.channelId,
        value.batchId,
        value.dispatchGeneration,
      ],
    );
    const retry = await client.query(
      `INSERT INTO crawler.migration_system_retry_items (
         migration_intent_id,candidate_id,failed_dispatch_batch_id,
         failed_dispatch_generation,failed_job_id,failed_job_attempt,
         failure_code,failure_category,failure_evidence,status,
         retry_dispatch_generation,dispatched_at
       ) VALUES ($1,$2,$3,$4,$5,1,'LEASE_CONFLICT','lease','{}'::jsonb,
                 'dispatched',$6,now())
       RETURNING system_retry_id`,
      [
        Number(intent.rows[0].migration_intent_id),
        value.candidateId,
        value.batchId,
        value.dispatchGeneration - 1,
        `${value.jobId}:g${value.dispatchGeneration - 1}`,
        value.dispatchGeneration,
      ],
    );
    return Number(retry.rows[0].system_retry_id);
  });
}

test("a reserved Binding without a Channel Run commits one terminal budget state", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  let value = null;
  try {
    value = await fixture(pool);
    let terminal = null;
    try {
      await terminateExhaustedBusinessRun(
        (action) => transaction(pool, action),
        budgetJob(value),
        { code: "BUSINESS_RUN_BUDGET_EXHAUSTED" },
      );
    } catch (error) {
      terminal = error;
    }
    assert.ok(terminal instanceof UnrecoverableError);
    assert.equal(terminal.code, "BUSINESS_RUN_BUDGET_EXHAUSTED");
    assert.equal(terminal.recovery.run_materialized, false);

    const state = await pool.query(
      `SELECT candidate.status AS candidate_status,
              candidate.validation_finished_at IS NOT NULL AS candidate_finished,
              candidate.snapshot_json->'proxy_control'->>'status' AS evidence_status,
              binding.status AS binding_status,binding.terminal_reason,
              (SELECT count(*)::int FROM crawler.channel_runs run WHERE run.run_id=$2) AS run_count
       FROM crawler.channel_candidates candidate
       JOIN crawler.business_run_bindings binding
         ON binding.candidate_id=candidate.candidate_id
       WHERE candidate.candidate_id=$1`,
      [value.candidateId, value.runId],
    );
    assert.deepEqual(state.rows[0], {
      candidate_status: "failed",
      candidate_finished: true,
      evidence_status: "business_run_budget_exhausted",
      binding_status: "terminal",
      terminal_reason: "proxy_control_business_run_budget_exhausted",
      run_count: 0,
    });
  } finally {
    if (value) await cleanup(pool, value).catch(() => {});
    await pool.end();
  }
});

test("a materialized running Channel Run commits the same atomic terminal budget state", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  let value = null;
  try {
    value = await fixture(pool, { materializedRun: true, runStatus: "running" });
    let terminal = null;
    try {
      await terminateExhaustedBusinessRun(
        (action) => transaction(pool, action),
        { ...budgetJob(value), data: { ...budgetJob(value).data, run_id: value.runId } },
        { code: "BUSINESS_RUN_BUDGET_EXHAUSTED" },
      );
    } catch (error) {
      terminal = error;
    }
    assert.ok(terminal instanceof UnrecoverableError);
    assert.equal(terminal.recovery.run_materialized, true);

    const state = await pool.query(
      `SELECT run.status AS run_status,run.detail_status,
              run.result_json->'proxy_control'->>'status' AS run_evidence_status,
              candidate.status AS candidate_status,
              candidate.snapshot_json->'proxy_control'->>'status' AS candidate_evidence_status,
              binding.status AS binding_status,binding.terminal_reason
       FROM crawler.channel_runs run
       JOIN crawler.business_run_bindings binding ON binding.business_run_id=run.run_id
       JOIN crawler.channel_candidates candidate ON candidate.candidate_id=binding.candidate_id
       WHERE run.run_id=$1`,
      [value.runId],
    );
    assert.deepEqual(state.rows[0], {
      run_status: "failed",
      detail_status: "failed",
      run_evidence_status: "business_run_budget_exhausted",
      candidate_status: "failed",
      candidate_evidence_status: "business_run_budget_exhausted",
      binding_status: "terminal",
      terminal_reason: "proxy_control_business_run_budget_exhausted",
    });
  } finally {
    if (value) await cleanup(pool, value).catch(() => {});
    await pool.end();
  }
});

test("G+1 budget exhaustion preserves an accepted Candidate and resolves its dispatched retry", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  let value = null;
  try {
    value = await fixture(pool, {
      materializedRun: true,
      runStatus: "running",
      dispatchGeneration: 2,
      candidateStatus: "accepted",
    });
    const systemRetryId = await addDispatchedSystemRetry(pool, value);

    let terminal = null;
    try {
      await terminateExhaustedBusinessRun(
        (action) => transaction(pool, action),
        { ...budgetJob(value), data: { ...budgetJob(value).data, run_id: value.runId } },
        { code: "BUSINESS_RUN_BUDGET_EXHAUSTED" },
      );
    } catch (error) {
      terminal = error;
    }
    assert.ok(terminal instanceof UnrecoverableError);
    assert.equal(terminal.recovery.retry_resolved, 1);

    const state = await pool.query(
      `SELECT candidate.status AS candidate_status,
              candidate.snapshot_active_job_id,candidate.snapshot_active_job_attempt,
              run.status AS run_status,binding.status AS binding_status,
              retry.status AS retry_status,retry.resolution
       FROM crawler.channel_candidates candidate
       JOIN crawler.business_run_bindings binding
         ON binding.candidate_id=candidate.candidate_id
       JOIN crawler.channel_runs run ON run.run_id=binding.business_run_id
       JOIN crawler.migration_system_retry_items retry
         ON retry.candidate_id=candidate.candidate_id
       WHERE candidate.candidate_id=$1 AND retry.system_retry_id=$2`,
      [value.candidateId, systemRetryId],
    );
    assert.deepEqual(state.rows[0], {
      candidate_status: "accepted",
      snapshot_active_job_id: null,
      snapshot_active_job_attempt: null,
      run_status: "failed",
      binding_status: "terminal",
      retry_status: "resolved",
      resolution: "retry_job_terminal_business_run_budget_exhausted",
    });
  } finally {
    if (value) await cleanup(pool, value).catch(() => {});
    await pool.end();
  }
});

test("a stalled budget failure cannot terminate or resolve the newer Candidate activation", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  let value = null;
  try {
    value = await fixture(pool, {
      materializedRun: true,
      runStatus: "running",
      dispatchGeneration: 2,
      activeJobAttempt: 2,
    });
    const systemRetryId = await addDispatchedSystemRetry(pool, value);

    await assert.rejects(
      terminateExhaustedBusinessRun(
        (action) => transaction(pool, action),
        {
          ...budgetJob(value),
          attemptsStarted: 1,
          data: { ...budgetJob(value).data, run_id: value.runId },
        },
        { code: "BUSINESS_RUN_BUDGET_EXHAUSTED" },
      ),
      (error) => error instanceof BusinessRunBudgetRecoveryError
        && /Candidate activation changed/.test(error.message),
    );

    const state = await pool.query(
      `SELECT candidate.status AS candidate_status,
              candidate.snapshot_active_job_id,candidate.snapshot_active_job_attempt,
              run.status AS run_status,binding.status AS binding_status,
              retry.status AS retry_status,retry.resolution
       FROM crawler.channel_candidates candidate
       JOIN crawler.business_run_bindings binding
         ON binding.candidate_id=candidate.candidate_id
       JOIN crawler.channel_runs run ON run.run_id=binding.business_run_id
       JOIN crawler.migration_system_retry_items retry
         ON retry.candidate_id=candidate.candidate_id
       WHERE candidate.candidate_id=$1 AND retry.system_retry_id=$2`,
      [value.candidateId, systemRetryId],
    );
    assert.deepEqual(state.rows[0], {
      candidate_status: "queued",
      snapshot_active_job_id: value.jobId,
      snapshot_active_job_attempt: 2,
      run_status: "running",
      binding_status: "materialized",
      retry_status: "dispatched",
      resolution: null,
    });
  } finally {
    if (value) await cleanup(pool, value).catch(() => {});
    await pool.end();
  }
});

test("a completed Channel Run is not downgraded by a late budget error", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  let value = null;
  try {
    value = await fixture(pool, { materializedRun: true, runStatus: "done" });
    let failure = null;
    try {
      await terminateExhaustedBusinessRun(
        (action) => transaction(pool, action),
        { ...budgetJob(value), data: { ...budgetJob(value).data, run_id: value.runId } },
        { code: "BUSINESS_RUN_BUDGET_EXHAUSTED" },
      );
    } catch (error) {
      failure = error;
    }
    assert.ok(failure instanceof BusinessRunBudgetRecoveryError);

    const state = await pool.query(
      `SELECT run.status AS run_status,run.detail_status,
              candidate.status AS candidate_status,binding.status AS binding_status
       FROM crawler.channel_runs run
       JOIN crawler.business_run_bindings binding ON binding.business_run_id=run.run_id
       JOIN crawler.channel_candidates candidate ON candidate.candidate_id=binding.candidate_id
       WHERE run.run_id=$1`,
      [value.runId],
    );
    assert.deepEqual(state.rows[0], {
      run_status: "done",
      detail_status: "done",
      candidate_status: "queued",
      binding_status: "materialized",
    });
  } finally {
    if (value) await cleanup(pool, value).catch(() => {});
    await pool.end();
  }
});

test("a concurrent newer Candidate generation makes the old budget transaction roll back", {
  skip: !integrationUrl,
  timeout: 30_000,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  let value = null;
  let releaseCandidateRead;
  const candidateReadReleased = new Promise((resolve) => {
    releaseCandidateRead = resolve;
  });
  let candidateReadReached;
  const candidateReadBarrier = new Promise((resolve) => {
    candidateReadReached = resolve;
  });
  try {
    value = await fixture(pool, {
      materializedRun: true,
      runStatus: "running",
      dispatchGeneration: 5,
    });
    const staleJob = budgetJob(value);
    const recovery = terminateExhaustedBusinessRun(
      (action) => transaction(pool, (client) => action({
        async query(sql, params) {
          if (/FROM crawler\.channel_candidates/.test(sql) && /FOR UPDATE/.test(sql)) {
            candidateReadReached();
            await candidateReadReleased;
          }
          return client.query(sql, params);
        },
      })),
      { ...staleJob, data: { ...staleJob.data, run_id: value.runId } },
      { code: "BUSINESS_RUN_BUDGET_EXHAUSTED" },
    );

    await candidateReadBarrier;
    await pool.query(
      `UPDATE crawler.channel_candidates
       SET snapshot_dispatch_generation=6,status='queued',updated_at=now()
       WHERE candidate_id=$1`,
      [value.candidateId],
    );
    releaseCandidateRead();

    await assert.rejects(
      recovery,
      (error) => error instanceof BusinessRunBudgetRecoveryError
        && /expected 5, got 6/.test(error.message),
    );

    const state = await pool.query(
      `SELECT candidate.status AS candidate_status,
              candidate.snapshot_dispatch_generation::text AS candidate_generation,
              binding.status AS binding_status,binding.terminal_reason,
              run.status AS run_status,run.detail_status
       FROM crawler.channel_candidates candidate
       JOIN crawler.business_run_bindings binding
         ON binding.candidate_id=candidate.candidate_id
       JOIN crawler.channel_runs run ON run.run_id=binding.business_run_id
       WHERE candidate.candidate_id=$1`,
      [value.candidateId],
    );
    assert.deepEqual(state.rows[0], {
      candidate_status: "queued",
      candidate_generation: "6",
      binding_status: "materialized",
      terminal_reason: null,
      run_status: "running",
      detail_status: "running",
    });
  } finally {
    releaseCandidateRead?.();
    if (value) await cleanup(pool, value).catch(() => {});
    await pool.end();
  }
});
