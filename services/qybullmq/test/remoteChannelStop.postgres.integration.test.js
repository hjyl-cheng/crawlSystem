import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import pg from 'pg';
import {RemoteNodeStore} from '../src/remoteNodes/store.js';
import {RemoteChannelExecutionStore} from '../src/remoteNodes/channelExecutionStore.js';

const url = process.env.REMOTE_STOP_TEST_DATABASE_URL;

test('remote stop remains bounded and preserves attempt ownership with sparse live tasks', {skip: !url}, async t => {
  const pool = new pg.Pool({connectionString: url, max: 4});
  t.after(() => pool.end());
  assert.equal((await pool.query('SELECT current_database() AS name')).rows[0].name, 'remote_stop_test');
  await pool.query(await readFile(new URL('../src/remoteNodes/schema.sql', import.meta.url), 'utf8'));
  await pool.query('TRUNCATE remote_ingestion.nodes,remote_ingestion.tasks CASCADE');
  const realStore = new RemoteNodeStore({pool});
  const execution = new RemoteChannelExecutionStore({channelStore: {store: realStore}, profileSecret: 'isolated-stop-test'});
  const add = async (state, attempt = 'original') => {
    const id = randomUUID();
    await pool.query(`INSERT INTO remote_ingestion.tasks(task_id,work_key,capability,input,context,state,coordinator_until)
      VALUES($1::uuid,$1::text,'fixture','{}',jsonb_build_object('execution_attempt_id',$2::text),$3,now()+interval '1 minute')`, [id, attempt, state]);
    return id;
  };
  const row = async id => (await pool.query('SELECT state,context,last_error,coordinator_until FROM remote_ingestion.tasks WHERE task_id=$1', [id])).rows[0];

  await t.test('only pending or leased tasks owned by the original attempt are failed', async () => {
    for (const state of ['pending', 'leased', 'received', 'applied', 'failed', 'cancelled']) {
      const id = await add(state);
      const before = await row(id);
      await execution.stop({taskId: id, attemptId: 'original'}, {code: 'FIXTURE_STOP'});
      const after = await row(id);
      if (['pending', 'leased'].includes(state)) {
        assert.equal(after.state, 'failed');
        assert.equal(after.last_error, 'FIXTURE_STOP');
        assert.equal(after.coordinator_until, null);
      } else assert.deepEqual(after, before);
    }
    const replaced = await add('pending', 'newer');
    const before = await row(replaced);
    await execution.stop({taskId: replaced, attemptId: 'original'});
    assert.deepEqual(await row(replaced), before);
    await execution.stop({taskId: randomUUID(), attemptId: 'missing'});
  });

  await t.test('a concurrent newer attempt survives an old stop waiting for the row lock', async () => {
    const id = await add('pending');
    const owner = await pool.connect();
    try {
      await owner.query('BEGIN');
      await owner.query(`UPDATE remote_ingestion.tasks SET context='{"execution_attempt_id":"newer"}' WHERE task_id=$1`, [id]);
      const stopping = execution.stop({taskId: id, attemptId: 'original'}, {code: 'OLD_STOP'});
      // Queue the stop behind the owner before making the newer attempt visible.
      for (let i = 0; ; i++) {
        const waiting = (await pool.query(`SELECT 1 FROM pg_stat_activity WHERE datname=current_database()
          AND wait_event_type='Lock' AND query LIKE '%remote_ingestion.tasks%' AND pid<>pg_backend_pid()`)).rowCount;
        if (waiting) break;
        assert.ok(i < 200, 'stop must reach the row lock');
        await new Promise(resolve => setTimeout(resolve, 5));
      }
      await owner.query('COMMIT');
      await stopping;
      assert.equal((await row(id)).state, 'pending');
      assert.equal((await row(id)).context.execution_attempt_id, 'newer');
    } finally { await owner.query('ROLLBACK'); owner.release(); }
  });

  await t.test('empty live-task statistics cannot turn a point stop into a history scan', async () => {
    await pool.query('TRUNCATE remote_ingestion.tasks CASCADE');
    await pool.query(`INSERT INTO remote_ingestion.tasks(task_id,work_key,capability,input,context,state)
      SELECT md5(n::text)::uuid,n::text,'fixture','{}','{"execution_attempt_id":"history"}','pending'
      FROM generate_series(1,20000) n`);
    // A long snapshot retains old index entries after every task has completed.
    const snapshot = await pool.connect();
    await snapshot.query('BEGIN ISOLATION LEVEL REPEATABLE READ');
    await snapshot.query('SELECT count(*) FROM remote_ingestion.tasks');
    try {
      await pool.query("UPDATE remote_ingestion.tasks SET state='applied'");
      await pool.query('ANALYZE remote_ingestion.tasks');
      const client = await pool.connect();
      const plans = [];
      const query = async (sql, values) => {
        if (/\b(?:SELECT|UPDATE)\b[\s\S]*remote_ingestion\.tasks/i.test(sql)) {
          const explained = await client.query('EXPLAIN (FORMAT JSON) ' + sql, values);
          plans.push(explained.rows[0]['QUERY PLAN'][0].Plan);
        }
        return client.query(sql, values);
      };
      try {
        const store = new RemoteNodeStore({pool: {query, connect: async () => ({query, release() {}})}});
        const tracked = new RemoteChannelExecutionStore({channelStore: {store}, profileSecret: 'isolated-stop-test'});
        await tracked.stop({taskId: randomUUID(), attemptId: 'absent'});
        assert.ok(plans.length > 0);
        const scans = plan => [plan, ...(plan.Plans || []).flatMap(scans)];
        for (const plan of plans) {
          const taskScans = scans(plan).filter(p => p['Relation Name'] === 'tasks' && p['Node Type'].includes('Scan'));
          assert.ok(taskScans.length > 0);
          for (const scan of taskScans) assert.match(scan['Index Cond'] || '', /task_id =/,
            'stopping one task must use its key, even when a partial live-task index appears empty');
        }
      } finally { client.release(); }
    } finally { await snapshot.query('ROLLBACK'); snapshot.release(); }
  });
});
