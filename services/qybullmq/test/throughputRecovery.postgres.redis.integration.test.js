import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import pg from 'pg';
import { Queue } from 'bullmq';
import { createFinalizeRecoveryScan } from '../src/finalizeRecoveryScan.js';
import { createFinalizeChangeRecovery } from '../src/finalizeChangeRecovery.js';
import { loadFinalizeRecoveryCandidates } from '../src/finalizeRecoveryPolicy.js';
import { readFinalizeSource } from '../src/finalizeSourceFence.js';
import { PUBLICATION_WRITER_VERSION } from '../src/publicationWriterVersion.js';

const databaseUrl = process.env.THROUGHPUT_TEST_DATABASE_URL;
const redisPort = Number(process.env.THROUGHPUT_TEST_REDIS_PORT);
test('bounded recovery and durable source generations survive Redis failures, concurrent updates and incomplete data', { skip: !databaseUrl || !redisPort }, async t => {
  assert.equal(new URL(databaseUrl).pathname, '/throughput_recovery_test');
  const pool = new pg.Pool({ connectionString: databaseUrl, options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}` });
  const query = pool.query.bind(pool);
  const withTransaction = async action => {
    const c = await pool.connect();
    try { await c.query('BEGIN'); const value = await action(c); await c.query('COMMIT'); return value; }
    catch (error) { await c.query('ROLLBACK'); throw error; }
    finally { c.release(); }
  };
  const queue = new Queue('finalize-test', { prefix: `throughput-${Date.now()}`, connection: { host: '127.0.0.1', port: redisPort } });
  t.after(async () => { await queue.obliterate({ force: true }); await queue.close(); await pool.end(); });
  await query('DROP SCHEMA IF EXISTS publication CASCADE; DROP SCHEMA IF EXISTS crawler CASCADE');
  await query(await readFile(new URL('../src/schema.sql', import.meta.url), 'utf8'));
  await query(await readFile(new URL('../src/throughputRecoverySchema.sql', import.meta.url), 'utf8'));
  for (const id of ['a', 'b', 'c', 'd', 'e']) {
    await query(`INSERT INTO crawler.channels(channel_id,channel_url,agent_status) VALUES($1,$1,'done')`, [id]);
    await query(`INSERT INTO crawler.channel_runs(run_id,channel_id,status,detail_status) VALUES($1,$2,'waiting_agent','done')`, [`run-${id}`, id]);
    await query(`UPDATE crawler.channels SET latest_run_id=$2 WHERE channel_id=$1`, [id, `run-${id}`]);
    await query(`INSERT INTO crawler.agent_profiles(channel_id,input_url) VALUES($1,$1)`, [id]);
  }
  await query("UPDATE crawler.channel_runs SET detail_status='queued' WHERE channel_id='b'");
  const expected = (await loadFinalizeRecoveryCandidates(query)).map(r => r.channel_id).sort();
  assert.deepEqual(expected, ['a', 'c', 'd', 'e']);

  await t.test('failed dispatch keeps cursor, full sweep matches old eligibility and pending detail stays out', async () => {
    const failScan = createFinalizeRecoveryScan({ query, withTransaction, queue: { add: async () => { throw new Error('redis unavailable'); } }, pageSize: 2 });
    await assert.rejects(failScan(), /redis unavailable/);
    assert.equal((await query('SELECT after_channel_id FROM crawler.finalize_recovery_scan')).rows[0].after_channel_id, '');
    const scan = createFinalizeRecoveryScan({ query, withTransaction, queue, pageSize: 2 });
    const reports = [await scan(), await scan(), await scan()];
    assert.deepEqual(reports.map(r => r.examined), [2, 2, 1]);
    assert.equal(reports.at(-1).wrapped, true);
    assert.deepEqual((await queue.getJobs(['waiting'])).map(j => j.data.channel_id).sort(), expected);
    assert.equal((await query('SELECT completed_rounds FROM crawler.finalize_recovery_scan')).rows[0].completed_rounds, '1');
  });

  await t.test('source changes roll back atomically and finalization bookkeeping does not self-trigger', async () => {
    const generation = async () => (await query("SELECT requested_generation FROM crawler.finalize_recovery_requests WHERE channel_id='a'")).rows[0].requested_generation;
    const before = await generation();
    await assert.rejects(withTransaction(async c => {
      await c.query("UPDATE crawler.channels SET title='rolled back' WHERE channel_id='a'");
      throw new Error('rollback');
    }), /rollback/);
    assert.equal(await generation(), before);
    await query("UPDATE crawler.channel_runs SET status='done',publication_finalized_at=now(),publication_finalized_status='ready_auto',updated_at=now() WHERE channel_id='a'");
    assert.equal(await generation(), before);
    await query("UPDATE crawler.channel_runs SET result_json=result_json||'{\"source_observation\":\"new\"}'::jsonb WHERE channel_id='a'");
    assert.ok(Number(await generation()) > Number(before));
  });

  await t.test('generation changing while a job is dispatched remains pending; enqueue is not completion', async () => {
    const recovery = createFinalizeChangeRecovery({ query, withTransaction, queue: {
      add: async (...args) => {
        await query('UPDATE crawler.channels SET title=COALESCE(title,\'\')||\'changed\',updated_at=now() WHERE channel_id=$1', [args[1].channel_id]);
        return queue.add(...args);
      }, getJob: queue.getJob.bind(queue),
    } });
    const result = await recovery();
    assert.equal(result.dispatched, 4);
    const row = (await query("SELECT * FROM crawler.finalize_recovery_requests WHERE channel_id='a'")).rows[0];
    assert.ok(Number(row.requested_generation) > Number(row.dispatched_generation));
    assert.equal(row.handled_generation, '0');
    assert.equal(row.lease_token, null);
    const pending = (await query("SELECT * FROM crawler.finalize_recovery_requests WHERE channel_id='b'")).rows[0];
    assert.equal(pending.dispatched_job_id, null);
    assert.equal(pending.handled_generation, pending.requested_generation);
    assert.equal(pending.last_decision, 'not_ready');
    assert.equal((await query("SELECT publication_finalized_at FROM crawler.channel_runs WHERE run_id='run-b'")).rows[0].publication_finalized_at, null);
  });

  await t.test('only a current finalized source is acknowledged; later source change is rediscovered', async () => {
    const source = await readFinalizeSource(query, { channelId: 'a', runId: 'run-a' });
    await query("INSERT INTO crawler.finalized_profiles(channel_id,run_id,status,updated_at,quality_json) VALUES ('a','run-a','ready_auto',now()+interval '1 second',$1)", [{ source_revision: source.sourceRevision }]);
    await query("UPDATE crawler.finalize_recovery_requests SET next_check_at=now() WHERE channel_id='a'");
    await createFinalizeChangeRecovery({ query, withTransaction, queue })();
    let row = (await query("SELECT * FROM crawler.finalize_recovery_requests WHERE channel_id='a'")).rows[0];
    assert.equal(row.handled_generation, row.requested_generation);
    await query("UPDATE crawler.agent_profiles SET updated_at=now()+interval '2 seconds' WHERE channel_id='a'");
    row = (await query("SELECT * FROM crawler.finalize_recovery_requests WHERE channel_id='a'")).rows[0];
    assert.ok(Number(row.requested_generation) > Number(row.handled_generation));
    assert.equal((await createFinalizeChangeRecovery({ query, withTransaction, queue })()).dispatched >= 1, true);
  });

  await t.test('an unchanged generation already in the queue is followed without rescanning video data', async () => {
    await query("UPDATE crawler.finalize_recovery_requests SET next_check_at=now()+interval '1 hour'");
    await query("UPDATE crawler.finalize_recovery_requests SET next_check_at=now() WHERE channel_id='e'");
    await createFinalizeChangeRecovery({ query, withTransaction, queue })();
    await query("UPDATE crawler.finalize_recovery_requests SET next_check_at=now() WHERE channel_id='e'");
    let sourceChecks = 0;
    await createFinalizeChangeRecovery({ query: (sql, args) => {
      if (sql.includes('finalize-recovery:candidates')) sourceChecks += 1;
      return query(sql, args);
    }, withTransaction, queue })();
    assert.equal(sourceChecks, 0, 'successful dispatch is not another source scan');
  });

  await t.test('an incomplete generation waits for a source change, and becomes eligible when detail finishes', async () => {
    await query("UPDATE crawler.finalize_recovery_requests SET next_check_at=now()+interval '1 hour'");
    const before = (await query("SELECT * FROM crawler.finalize_recovery_requests WHERE channel_id='b'")).rows[0];
    assert.equal(before.handled_generation, before.requested_generation);
    await query("UPDATE crawler.channel_runs SET detail_status='done' WHERE run_id='run-b'");
    assert.equal((await createFinalizeChangeRecovery({ query, withTransaction, queue })()).dispatched, 1);
    const after = (await query("SELECT * FROM crawler.finalize_recovery_requests WHERE channel_id='b'")).rows[0];
    assert.ok(Number(after.requested_generation) > Number(before.requested_generation));
    assert.equal(after.last_decision, 'dispatched');
    assert.equal((await query("SELECT publication_finalized_at FROM crawler.channel_runs WHERE run_id='run-b'")).rows[0].publication_finalized_at, null);
  });

  await t.test('the audit recreates a missing durable intent without depending on Redis', async () => {
    await query("DELETE FROM crawler.finalize_recovery_requests WHERE channel_id='c'");
    const scan = createFinalizeRecoveryScan({ query, withTransaction,
      queue: { add: () => assert.fail('event audit only records durable intent') }, pageSize: 5, registerChanges: true });
    assert.ok((await scan()).registered > 0);
    const intent = (await query("SELECT * FROM crawler.finalize_recovery_requests WHERE channel_id='c'")).rows[0];
    assert.equal(intent.requested_generation, '1');
    assert.equal(intent.handled_generation, '0');
  });
  await t.test('an expensive page retries the same cursor with a smaller page after timeout', async () => {
    await query("UPDATE crawler.finalize_recovery_scan SET after_channel_id='',upper_channel_id=NULL,lease_token=NULL,lease_until=NULL");
    let timeout = true;
    const scan = createFinalizeRecoveryScan({ query: async (sql, args) => {
      if (timeout && sql.includes('finalize-recovery:candidates')) {
        timeout = false;
        throw Object.assign(new Error('synthetic statement timeout'), { code: '57014' });
      }
      return query(sql, args);
    }, withTransaction, queue, pageSize: 2, registerChanges: true });
    await assert.rejects(scan, /synthetic statement timeout/);
    assert.equal((await query("SELECT after_channel_id FROM crawler.finalize_recovery_scan WHERE scope='global'")).rows[0].after_channel_id, '');
    assert.equal((await scan()).examined, 1);
  });
});
