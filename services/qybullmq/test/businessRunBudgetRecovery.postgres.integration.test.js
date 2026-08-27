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

async function fixture(pool, { materializedRun = false, runStatus = "running" } = {}) {
  const suffix = randomUUID();
  const batchId = `budget-recovery:${suffix}`;
  const channelId = `UC${suffix.replaceAll("-", "").slice(0, 22)}`;
  const runId = `run:budget:${suffix}`;
  const businessRunKey = `full-candidate:budget:${suffix}`;
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
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status
       ) VALUES ($1,$1,$2,$3,'queued')
       RETURNING candidate_id`,
      [batchId, channelId, `https://www.youtube.com/channel/${channelId}`],
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
  return { batchId, businessRunKey, candidateId, channelId, runId };
}

async function cleanup(pool, value) {
  await transaction(pool, async (client) => {
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
    id: `budget-job:${value.candidateId}`,
    queueName: "youtube-channel-crawl",
    name: "channel-crawl",
    data: {
      business_run_key: value.businessRunKey,
      candidate_id: value.candidateId,
      channel_id: value.channelId,
    },
  };
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
