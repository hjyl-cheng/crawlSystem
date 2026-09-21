#!/usr/bin/env node

import process from "node:process";
import { closeDb, query, withTransaction } from "../src/db.js";
import { verifyCrawlerWriterDatabase } from "../src/databaseIdentity.js";

const DEFAULT_BATCH_ID = "migration-19eff406-4ec3-442e-a448-882b14caae69";

function parseArgs(argv) {
  const options = { batchId: DEFAULT_BATCH_ID, apply: false, expectedCount: null };
  for (const arg of argv) {
    if (arg === "--apply") options.apply = true;
    else if (arg.startsWith("--batch-id=")) options.batchId = arg.slice(11).trim();
    else if (arg.startsWith("--expected-count=")) {
      const value = Number(arg.slice(17));
      if (!Number.isSafeInteger(value) || value < 0) {
        throw new TypeError("--expected-count must be a non-negative integer");
      }
      options.expectedCount = value;
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }
  if (!options.batchId) throw new TypeError("--batch-id is required");
  if (options.apply && options.expectedCount == null) {
    throw new TypeError("--expected-count is required with --apply");
  }
  return options;
}

const ORPHAN_QUERY = `
WITH latest AS MATERIALIZED (
  SELECT DISTINCT ON (item.channel_id)
         item.channel_id,item.candidate_id,item.state,
         candidate.status AS candidate_status,
         candidate.snapshot_active_job_id,
         candidate.snapshot_active_job_attempt,
         candidate.snapshot_dispatch_generation,
         run.run_id,run.status AS run_status,run.finished_at,
         run.error_message,COALESCE(run.result_json,'{}'::jsonb) AS result_json,
         candidate.dispatch_batch_id
  FROM crawler.migration_control_items item
  JOIN crawler.channel_candidates candidate
    ON candidate.candidate_id=item.candidate_id
  JOIN LATERAL (
    SELECT * FROM crawler.channel_runs
    WHERE candidate_id=candidate.candidate_id
    ORDER BY created_at DESC,run_id DESC LIMIT 1
  ) run ON true
  WHERE item.batch_id=$1
    AND item.state='started'
    AND run.status='failed'
  ORDER BY item.channel_id,run.created_at DESC
), classified AS (
  SELECT latest.*,
    EXISTS (
      SELECT 1 FROM crawler.migration_system_retry_items retry
      WHERE retry.candidate_id=latest.candidate_id
        AND retry.status IN ('pending','retrying','dispatched')
    ) AS active_retry,
    EXISTS (
      SELECT 1 FROM crawler.migration_system_retry_items retry
      WHERE retry.candidate_id=latest.candidate_id
        AND retry.status='resolved'
        AND retry.resolution='job_completed'
    ) AS reopenable_retry,
    EXISTS (
      SELECT 1 FROM crawler.migration_system_retry_items retry
      WHERE retry.candidate_id=latest.candidate_id
        AND retry.failed_dispatch_generation=latest.snapshot_dispatch_generation
        AND retry.failed_job_id=latest.result_json->>'job_id'
        AND retry.status='resolved'
        AND retry.resolution IN (
          'retry_job_terminal_business_failure',
          'retry_job_terminal_business_run_budget_exhausted',
          'recovery_terminal_business_outcome'
        )
    ) AS resolved_terminal_retry,
    (COALESCE(latest.result_json ? 'content_detail_recovery_terminal',false)
      OR COALESCE(latest.result_json ? 'parser_contract_error',false)
      OR COALESCE(latest.result_json ? 'channel_run_terminal_failure',false)
      OR COALESCE((latest.result_json->'proxy_control'->>'status')='business_run_budget_exhausted',false)
      OR COALESCE(latest.result_json ? 'terminal_channel',false)) AS has_terminal_evidence
  FROM latest
)
SELECT *,
  CASE
    WHEN snapshot_active_job_id IS NOT NULL THEN 'active_fence'
    WHEN active_retry THEN 'active_retry'
    WHEN reopenable_retry THEN 'reopenable_retry'
    WHEN has_terminal_evidence OR resolved_terminal_retry THEN 'already_evidenced'
    WHEN candidate_status='accepted'
      AND finished_at < now() - interval '30 minutes'
      AND error_message IS NOT NULL THEN 'eligible_terminal_backfill'
    ELSE 'manual_review'
  END AS repair_class
FROM classified
ORDER BY channel_id`;

const BACKFILL_SQL = `
WITH classified AS MATERIALIZED (${ORPHAN_QUERY}), target AS MATERIALIZED (
  SELECT channel_id,candidate_id,run_id FROM classified
  WHERE repair_class='eligible_terminal_backfill'
)
UPDATE crawler.channel_runs run
SET result_json=COALESCE(run.result_json,'{}'::jsonb)
      || jsonb_build_object(
           'channel_run_terminal_failure',jsonb_build_object(
             'source','legacy_orphan_reconciliation',
             'reason','failed_run_without_active_recovery',
             'reconciled_at',now(),
             'error_message',left(run.error_message,2000)
           )
         ),
    updated_at=now()
FROM target
WHERE run.run_id=target.run_id
  AND run.status='failed'
RETURNING target.channel_id,target.candidate_id,run.run_id`;

async function main() {
  const options = parseArgs(process.argv.slice(2));
  await verifyCrawlerWriterDatabase(query);
  const rows = (await query(ORPHAN_QUERY, [options.batchId])).rows;
  const summary = rows.reduce((counts, row) => {
    counts[row.repair_class] = (counts[row.repair_class] ?? 0) + 1;
    return counts;
  }, {});
  console.log(JSON.stringify({
    event: "migration_orphaned_run_audit",
    batch_id: options.batchId,
    total_failed_runs: rows.length,
    classes: summary,
    apply: options.apply,
  }));
  if (!options.apply) {
    for (const row of rows) {
      console.log(JSON.stringify({
        channel_id: row.channel_id,
        candidate_id: row.candidate_id,
        run_id: row.run_id,
        candidate_status: row.candidate_status,
        repair_class: row.repair_class,
        error_message: row.error_message,
      }));
    }
    return;
  }
  const result = await withTransaction(async client => {
    await client.query("SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='30s'");
    await client.query("SELECT value_json FROM crawler.settings WHERE setting_key='query_scheduler' FOR UPDATE");
    const batch = (await client.query(
      "SELECT status FROM crawler.migration_control_batches WHERE batch_id=$1 FOR UPDATE",
      [options.batchId],
    )).rows[0];
    if (!batch || ['completed', 'ended'].includes(batch.status)) {
      throw new Error('batch is missing or already closed');
    }
    await client.query(`SELECT candidate.candidate_id
      FROM crawler.migration_control_items item
      JOIN crawler.channel_candidates candidate ON candidate.candidate_id=item.candidate_id
      WHERE item.batch_id=$1 AND item.state='started'
      ORDER BY candidate.candidate_id FOR UPDATE OF candidate`, [options.batchId]);
    const lockedRows = (await client.query(ORPHAN_QUERY, [options.batchId])).rows;
    const eligible = lockedRows.filter(row => row.repair_class === 'eligible_terminal_backfill');
    if (eligible.length !== options.expectedCount) {
      throw new Error(`eligible count changed: expected ${options.expectedCount}, found ${eligible.length}`);
    }
    const updated = await client.query(BACKFILL_SQL, [options.batchId]);
    const expectedIds = eligible.map(row => row.run_id).sort();
    const actualIds = updated.rows.map(row => row.run_id).sort();
    if (updated.rowCount !== options.expectedCount
        || JSON.stringify(actualIds) !== JSON.stringify(expectedIds)) {
      throw new Error('backfill target set changed; transaction rolled back');
    }
    return updated;
  });
  console.log(JSON.stringify({
    event: "migration_orphaned_run_backfill",
    batch_id: options.batchId,
    updated: result.rowCount,
  }));
}

try {
  await main();
} finally {
  await closeDb();
}
