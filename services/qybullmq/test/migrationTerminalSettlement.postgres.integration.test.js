import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import {maintainMigrationControl} from '../src/migrationBatchControl.js';

test('terminal evidence settles the latest failed Run without closing recovering or newer work', {
  skip: !process.env.MIGRATION_TERMINAL_TEST_URL,
}, async () => {
  const url = process.env.MIGRATION_TERMINAL_TEST_URL;
  assert.equal(new URL(url).pathname, '/migration_terminal_settlement_test');
  const pool = new pg.Pool({connectionString: url});
  let sql;
  const batch = {batch_id: 'b', status: 'running'};
  const client = {query: async text => {
    if (text.includes('FROM crawler.settings')) return {rows: []};
    if (text.includes('FROM crawler.migration_control_batches')) return {rows: [batch]};
    if (text.includes('WITH started AS MATERIALIZED')) {sql = text; return {rows: []};}
    if (text.includes('SELECT count(*) FILTER')) return {rows: [{started: 1}]};
    if (text.includes('FROM publication.outbox')) return {rows: [{count: 0}]};
    throw new Error(text);
  }};
  await maintainMigrationControl({query: async () => ({rows: [batch]}), withTransaction: fn => fn(client)});
  try {
    await pool.query(`DROP SCHEMA IF EXISTS crawler CASCADE; CREATE SCHEMA crawler;
      CREATE TABLE crawler.migration_control_items(batch_id text,channel_id text,candidate_id bigint,ordinal bigint,state text,outcome text,finished_at timestamptz);
      CREATE TABLE crawler.channel_candidates(candidate_id bigint,status text,snapshot_attempts int,snapshot_json jsonb,snapshot_dispatch_generation bigint,snapshot_active_job_id text,snapshot_active_job_attempt int,dispatch_batch_id text);
      CREATE TABLE crawler.channel_runs(candidate_id bigint,status text,publication_finalized_status text,result_json jsonb,created_at timestamptz);
      CREATE TABLE crawler.migration_system_retry_items(candidate_id bigint,failed_dispatch_batch_id text,status text,failed_dispatch_generation bigint,failed_job_id text,failed_job_attempt int,failure_code text,resolution text);
      INSERT INTO crawler.migration_control_items SELECT 'b','channel-'||n,n,n,'started',NULL,NULL FROM generate_series(1,8) n;
      INSERT INTO crawler.channel_candidates SELECT n,'accepted',1,'{}',1,NULL,NULL,'b' FROM generate_series(1,8) n;
      INSERT INTO crawler.channel_runs SELECT n,'failed',NULL,'{"job_id":"current"}',now() FROM generate_series(1,8) n;
      UPDATE crawler.channel_runs SET result_json=result_json||'{"channel_run_terminal_failure":{}}' WHERE candidate_id IN (1,5,6,7);
      UPDATE crawler.channel_runs SET status='waiting_detail' WHERE candidate_id=2;
      INSERT INTO crawler.channel_runs VALUES (7,'waiting_detail',NULL,'{}',now()+interval '1 second');
      UPDATE crawler.channel_runs SET result_json=result_json||'{"proxy_control":{"status":"business_run_budget_exhausted"}}' WHERE candidate_id=8;
      INSERT INTO crawler.migration_system_retry_items SELECT n,'b','resolved',1,CASE WHEN n=3 THEN 'old-job' ELSE 'current' END,1,NULL,'retry_job_terminal_business_failure' FROM generate_series(2,4) n;
      INSERT INTO crawler.migration_system_retry_items VALUES(5,'b','retrying',1,'current',1,NULL,NULL);
      UPDATE crawler.channel_candidates SET snapshot_active_job_id='current',snapshot_active_job_attempt=1 WHERE candidate_id=6;
    `);
    await pool.query(sql, ['b', 6, 0, null]);
    const rows = (await pool.query('SELECT candidate_id::int,state,outcome FROM crawler.migration_control_items ORDER BY candidate_id')).rows;
    assert.deepEqual(rows.filter(r => r.state === 'terminal').map(r => r.candidate_id), [1,4,8]);
    assert.ok(rows.filter(r => r.state === 'terminal').every(r => r.outcome === 'failed'));
    assert.deepEqual(rows.filter(r => r.state === 'started').map(r => r.candidate_id), [2,3,5,6,7]);

    // Qualification counts and final Run outcomes differ when an accepted
    // Candidate fails later. Completion must not count that channel twice.
    await pool.query(`
      TRUNCATE crawler.migration_control_items,crawler.channel_candidates,crawler.channel_runs,crawler.migration_system_retry_items;
      ALTER TABLE crawler.migration_control_items ADD COLUMN started_at timestamptz;
      CREATE TABLE crawler.settings(setting_key text,value_json jsonb,updated_at timestamptz);
      INSERT INTO crawler.settings VALUES('query_scheduler','{"pipeline_cycle_id":"b","status":"finishing"}',now());
      CREATE TABLE crawler.migration_control_batches(batch_id text,status text,version int,total_count int,created_at timestamptz,updated_at timestamptz,paused_at timestamptz,finished_at timestamptz);
      INSERT INTO crawler.migration_control_batches VALUES('b','running',7,3,now(),now(),NULL,NULL);
      CREATE TABLE crawler.query_dispatch_batches(dispatch_batch_id text,status text,total_channel_count int,discovered_candidate_count int,accepted_channel_count int,rejected_channel_count int,failed_channel_count int,result_json jsonb,updated_at timestamptz,finished_at timestamptz,
        CHECK(accepted_channel_count+rejected_channel_count+failed_channel_count<=total_channel_count));
      INSERT INTO crawler.query_dispatch_batches VALUES('b','validation_closed',3,3,2,0,1,'{}',now(),NULL);
      CREATE SCHEMA IF NOT EXISTS publication;
      CREATE TABLE IF NOT EXISTS publication.revision(revision_id text,channel_id text);
      CREATE TABLE IF NOT EXISTS publication.outbox(revision_id text,created_at timestamptz,status text);
      INSERT INTO crawler.migration_control_items SELECT 'b','channel-'||n,n,n,'started',NULL,NULL,now() FROM generate_series(1,3) n;
      INSERT INTO crawler.channel_candidates SELECT n,CASE WHEN n=3 THEN 'failed' ELSE 'accepted' END,6,'{}',1,NULL,NULL,'b' FROM generate_series(1,3) n;
      INSERT INTO crawler.channel_runs VALUES(1,'failed',NULL,'{"channel_run_terminal_failure":{}}',now()),(2,'done','ready_auto','{}',now());
    `);
    const query = pool.query.bind(pool);
    const withTransaction = async fn => {
      const c = await pool.connect();
      try { await c.query('BEGIN'); const result = await fn(c); await c.query('COMMIT'); return result; }
      catch (error) { await c.query('ROLLBACK'); throw error; }
      finally { c.release(); }
    };
    await maintainMigrationControl({query, withTransaction, queue: {add() { assert.fail('must not dispatch'); }}});
    assert.equal((await query('SELECT status FROM crawler.migration_control_batches')).rows[0].status, 'completed');
    const counts = (await query('SELECT * FROM crawler.query_dispatch_batches')).rows[0];
    assert.equal(counts.total_channel_count, 3);
    assert.equal(counts.accepted_channel_count, 2);
    assert.equal(counts.failed_channel_count, 1);
    assert.equal(counts.result_json.terminal_failed_count, 2);
  } finally { await pool.end(); }
});
