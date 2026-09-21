import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {fullCrawlSlotUnsettled} from './fullCrawlCenterRecovery.js';

const files = ['fullCrawlSchema.sql', 'fullCrawlBusinessSchema.sql', 'fullCrawlTransportSchema.sql', 'workerCountSchema.sql'];
const baseTables = ['remote_ingestion.node_deployments', 'remote_ingestion.worker_connections',
  'remote_ingestion.transport_receipts', 'remote_ingestion.network_bindings', 'remote_ingestion.youtube_sessions',
  'crawler.channel_execution_attempts', 'crawler.channel_candidates', 'crawler.business_run_bindings'];

export async function assertFullCrawlReleaseSchema(query) {
  for (const table of baseTables) {
    if (!(await query('SELECT to_regclass($1) AS relation', [table])).rows[0].relation) throw new Error('FULL_CRAWL_BASE_SCHEMA_REQUIRED');
  }
  for (const sql of [
    'SELECT connection_identity,rota_fence,settings_snapshot,reference_at,admission_started_at FROM remote_ingestion.full_crawl_executions LIMIT 0',
    'SELECT input_hash,target_hash,applied_at FROM remote_ingestion.full_crawl_stages LIMIT 0',
    'SELECT result_hash,applied_result FROM remote_ingestion.full_crawl_detail_reservations LIMIT 0',
    'SELECT payload_bytes,part_count,applied_result FROM remote_ingestion.full_crawl_result_batches LIMIT 0',
    'SELECT payload,part_hash FROM remote_ingestion.full_crawl_result_parts LIMIT 0',
    'SELECT retirement_id,retired_at FROM remote_ingestion.worker_connections LIMIT 0',
  ]) await query(sql);
  const triggers = await query(`SELECT tgname FROM pg_trigger WHERE NOT tgisinternal AND tgenabled IN ('O','A')
    AND ((tgrelid='crawler.channel_execution_attempts'::regclass AND tgname='full_crawl_attempt_insert_lock')
      OR (tgrelid='remote_ingestion.full_crawl_stages'::regclass AND tgname='remote_full_stage_notify'))`);
  if (triggers.rowCount !== 2) throw new Error('FULL_CRAWL_SCHEMA_TRIGGERS_REQUIRED');
}

export async function fullCrawlRollbackReadiness(pool) {
  const rows = (await pool.query("SELECT * FROM remote_ingestion.worker_connections WHERE mode='full_crawl_collect'")).rows;
  const blockers = [];
  for (const row of rows) {
    const intake = (await pool.query(`SELECT 1 FROM remote_ingestion.node_intake_requests
      WHERE node_id=$1 AND $2=ANY(selected_slots)`, [row.node_id, row.slot])).rowCount > 0;
    if (row.enabled || row.activation_requested || intake || await fullCrawlSlotUnsettled(pool, row)) {
      blockers.push({node_id: row.node_id, slot: row.slot, reason: 'intake_or_execution_unsettled'});
    }
  }
  const pending = await pool.query(`SELECT count(*)::int AS count FROM remote_ingestion.full_crawl_result_batches WHERE state<>'applied'`);
  if (pending.rows[0].count) blockers.push({reason: 'retained_results_need_reconciliation', count: pending.rows[0].count});
  const continuations = await pool.query(`SELECT count(*)::int AS count FROM remote_ingestion.tasks t
    WHERE t.capability='youtube.full-crawl.v1' AND t.state='received'
      AND NOT EXISTS(SELECT 1 FROM crawler.channel_runs r WHERE r.run_id=t.input->>'run_id'
        AND r.result_json#>>'{full_crawl,fetch,status}'='complete')`);
  if (continuations.rows[0].count) blockers.push({reason: 'business_continuations_pending', count: continuations.rows[0].count});
  return {ready: blockers.length === 0, blockers, schema_action: 'retain', evidence_action: 'retain'};
}

export async function applyFullCrawlReleaseSchema(pool) {
  const sources = await Promise.all(files.map(async file => ({file, sql: await readFile(new URL(file, import.meta.url), 'utf8')})));
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query("SET LOCAL lock_timeout='3s'");
    await client.query("SET LOCAL statement_timeout='15s'");
    await client.query('SELECT pg_advisory_xact_lock(781137981)');
    for (const {sql} of sources) await client.query(sql);
    await assertFullCrawlReleaseSchema(client.query.bind(client));
    await client.query('COMMIT');
    return sources.map(({file, sql}) => ({file, sha256: createHash('sha256').update(sql).digest('hex')}));
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally { client.release(); }
}
