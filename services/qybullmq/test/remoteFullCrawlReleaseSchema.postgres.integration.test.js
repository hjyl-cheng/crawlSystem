import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import pg from 'pg';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';
import {applyFullCrawlReleaseSchema, fullCrawlRollbackReadiness} from '../src/remoteNodes/fullCrawlReleaseSchema.js';

const url = process.env.REMOTE_NODE_TEST_DATABASE_URL;
test('explicit release migration is repeatable, short-lock, preserves old workers and blocks rollback with pending API evidence',
  {skip: !url, timeout: 30000}, async t => {
  const pool = new pg.Pool({connectionString: url, max: 3});
  await assertIsolatedRemoteDatabase(pool);
  const node = randomUUID(), task = randomUUID(), deployment = randomUUID();
  t.after(async () => {
    await pool.query('DELETE FROM remote_ingestion.tasks WHERE task_id=$1', [task]);
    for (const table of ['worker_connections', 'network_slots', 'nodes']) await pool.query(`DELETE FROM remote_ingestion.${table} WHERE node_id=$1`, [node]);
    await pool.end();
  });
  await pool.query("INSERT INTO remote_ingestion.nodes(node_id,token_hash,capabilities,max_leases) VALUES($1,$2,ARRAY['youtube.channel-plan.v1'],1)", [node, node.replaceAll('-', '').repeat(2)]);
  await pool.query("INSERT INTO remote_ingestion.network_slots(node_id,slot,rota_worker_id) VALUES($1,'incremental-1',$2)", [node, node]);
  await pool.query(`INSERT INTO remote_ingestion.worker_connections(node_id,slot,deployment_id,config_hash,role,mode,enabled,instance_id)
    VALUES($1,'incremental-1',$2,$3,'incremental','incremental_collect',true,$4)`, [node, deployment, 'a'.repeat(64), randomUUID()]);
  const before = (await pool.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1', [node])).rows;
  const first = await applyFullCrawlReleaseSchema(pool);
  assert.equal(first.length, 4);
  assert.deepEqual(await applyFullCrawlReleaseSchema(pool), first);
  assert.deepEqual((await pool.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1', [node])).rows, before);
  const blocker = await pool.connect();
  try {
    await blocker.query('SELECT pg_advisory_lock(781137981)');
    await assert.rejects(applyFullCrawlReleaseSchema(pool), {code: '55P03'});
  } finally {await blocker.query('SELECT pg_advisory_unlock(781137981)'); blocker.release();}
  await pool.query(`INSERT INTO remote_ingestion.tasks(task_id,work_key,capability,input,context,state,last_error,applied_result)
    VALUES($1,$2,'youtube.full-crawl.v1',$3,'{}','received','VIDEO_API_PENDING',$4)`,
    [task, 'p5-api-' + task, {run_id: 'pending-fixture-' + task}, {request_id: 'fixture-api-' + task}]);
  assert.ok((await fullCrawlRollbackReadiness(pool)).blockers.some(row => row.reason === 'business_continuations_pending'));
  assert.equal((await pool.query('SELECT applied_result FROM remote_ingestion.tasks WHERE task_id=$1', [task])).rows[0].applied_result.request_id, 'fixture-api-' + task);
  const child = spawn(process.execPath, ['scripts/fullCrawlReleaseSchema.mjs', '--check'], {
    env: {...process.env, FULL_CRAWL_RELEASE_DATABASE_URL: url, FULL_CRAWL_RELEASE_EXPECTED_DATABASE: 'wrong_database'},
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  let error = ''; child.stderr.on('data', bytes => {error += bytes;});
  assert.equal((await once(child, 'exit'))[0], 1); assert.match(error, /FULL_CRAWL_RELEASE_DATABASE_MISMATCH/);
});
