import test from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';
import { loadIncrementalBudgetPage } from '../src/incrementalBudgetReconciliation.js';
import { INCREMENTAL_QUEUE, incrementalRunId } from '../src/incrementalPlan.js';

test('budget reconciliation pages past deferred records without losing timestamp precision', {
  skip: !process.env.REMOTE_NODE_TEST_DATABASE_URL,
}, async () => {
  const pool = new pg.Pool({ connectionString: process.env.REMOTE_NODE_TEST_DATABASE_URL, max: 1 });
  try {
    await assertIsolatedRemoteDatabase(pool);
    await pool.query('BEGIN');
    await pool.query(`DROP SCHEMA IF EXISTS crawler CASCADE; DROP SCHEMA IF EXISTS feature_clock CASCADE;
      CREATE SCHEMA crawler; CREATE SCHEMA feature_clock;
      CREATE TABLE crawler.task_events(job_id text,queue_name text,created_at timestamptz,status text,error_message text);
      CREATE TABLE feature_clock.dispatch_outbox(plan_id uuid,job_id text,payload_json jsonb);
      CREATE TABLE crawler.channel_runs(run_id text PRIMARY KEY,plan_id uuid,channel_id text,crawl_mode text,status text,started_at timestamptz);
      CREATE TABLE crawler.business_run_bindings(business_run_id text,status text);
      CREATE TABLE crawler.channel_execution_attempts(channel_id text,business_run_id text,job_attempt integer);`);
    // Reverse UUID order for the second microsecond to expose JS Date rounding.
    const plans = [3, 4, 1].map(n => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`);
    const ids = plans.map(incrementalRunId);
    for (let i = 0; i < ids.length; i++) {
      await pool.query(`INSERT INTO crawler.channel_runs VALUES($1,$2,'channel','incremental','failed',$3)`,
        [ids[i], plans[i], i < 2 ? '2026-09-24T08:00:00.000001Z' : '2026-09-24T08:00:00.000002Z']);
      await pool.query(`INSERT INTO crawler.task_events VALUES($1,$2,'2026-09-24T08:01:00Z','failed','Rota Business Run budget exhausted')`, [ids[i], INCREMENTAL_QUEUE]);
      await pool.query(`INSERT INTO feature_clock.dispatch_outbox VALUES($1,$2,'{}');`, [plans[i], ids[i]]);
      await pool.query(`INSERT INTO crawler.business_run_bindings VALUES($1,'materialized')`, [ids[i]]);
    }
    const options = { since: '2026-09-24T08:00:00Z', until: '2026-09-24T09:00:00Z', limit: 1 };
    const seen = [];
    let cursor = null;
    do {
      const page = await loadIncrementalBudgetPage(pool, { ...options, cursor });
      seen.push(...page.candidates.map(row => row.run_id));
      cursor = page.next_cursor;
      assert.ok(seen.length <= 3, 'unchanged deferred records must not repeat');
    } while (cursor);
    assert.deepEqual(seen, ids);
    const first = await loadIncrementalBudgetPage(pool, options);
    assert.equal(first.candidates[0].run_id, ids[0], 'new pass revisits deferred records');
    await pool.query(`DELETE FROM crawler.channel_runs WHERE run_id=$1`, [ids[0]]);
    const next = await loadIncrementalBudgetPage(pool, { ...options, cursor: first.next_cursor });
    assert.equal(next.candidates[0].run_id, ids[1], 'cursor survives removal of the preceding record');
    await assert.rejects(loadIncrementalBudgetPage(pool, { ...options, until: '2026-09-24T10:00:00Z', cursor: first.next_cursor }), /mismatched window/);
    await assert.rejects(loadIncrementalBudgetPage(pool, { ...options, cursor: 'invalid' }), /invalid.*cursor/);
  } finally {
    try { await pool.query('ROLLBACK'); } finally { await pool.end(); }
  }
});
