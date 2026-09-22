import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import pg from 'pg';
import { Queue, Worker } from 'bullmq';
import { createMigrationQueueControl, migrationQueueNames, applyFencedQueueState } from '../src/migrationQueueControl.js';
import { createControllerWorkLoops } from '../src/controllerWorkLoops.js';

const url = process.env.MIGRATION_QUEUE_TEST_DATABASE_URL;
const port = Number(process.env.MIGRATION_QUEUE_TEST_REDIS_PORT);
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const until = async check => {
  const end = Date.now() + 5000;
  while (!await check()) { assert.ok(Date.now() < end, 'queue control did not converge'); await new Promise(r => setTimeout(r, 20)); }
};

test('migration queue state survives stale ticks, interrupted control and independent recovery', { skip: !url || !port }, async t => {
  assert.equal(new URL(url).pathname, '/migration_queue_test');
  const pool = new pg.Pool({ connectionString: url, max: 5 });
  const query = pool.query.bind(pool);
  const tx = async action => { const c = await pool.connect(); try { await c.query('BEGIN'); const v = await action(c); await c.query('COMMIT'); return v; }
    catch (e) { await c.query('ROLLBACK').catch(() => {}); throw e; } finally { c.release(); } };
  const connection = { host: '127.0.0.1', port, maxRetriesPerRequest: null };
  const prefix = `migration-queues-${Date.now()}`;
  const queues = Object.fromEntries([...migrationQueueNames].map(name => [name, new Queue(name, { connection, prefix })]));
  t.after(async () => { await Promise.all(Object.values(queues).map(async q => { await q.obliterate({ force: true }); await q.close(); })); await pool.end(); });
  await query(`CREATE SCHEMA IF NOT EXISTS crawler;
    CREATE TABLE IF NOT EXISTS crawler.settings(setting_key text PRIMARY KEY,value_json jsonb NOT NULL,updated_at timestamptz DEFAULT now());
    CREATE TABLE IF NOT EXISTS crawler.migration_control_batches(batch_id text PRIMARY KEY,status text,version bigint);
    CREATE TABLE IF NOT EXISTS crawler.youtube_api_detail_requests(status text);
    CREATE SEQUENCE IF NOT EXISTS crawler.migration_queue_control_revision;
    TRUNCATE crawler.settings,crawler.migration_control_batches,crawler.youtube_api_detail_requests;`);
  let ready = 10, recovery = [], fail = false;
  const control = createMigrationQueueControl({ withTransaction: tx, queues,
    loadPressure: async () => { if (fail) throw new Error('temporary outage'); return { at: Date.now(), channelReady: ready, detailReady: ready, detailBacklog: 0 }; },
    recoveryQueues: () => recovery });
  const state = async (status, schedulerStatus = 'finishing', id = 'new') => tx(async c => {
    await c.query(`INSERT INTO crawler.settings VALUES('query_scheduler',$1,now()) ON CONFLICT(setting_key)
      DO UPDATE SET value_json=EXCLUDED.value_json,updated_at=now()`, [JSON.stringify({ status: schedulerStatus, pipeline_cycle_id: id, stop_reason: 'pipeline_complete' })]);
    await c.query(`INSERT INTO crawler.migration_control_batches VALUES($1,$2,1) ON CONFLICT(batch_id)
      DO UPDATE SET status=$2,version=crawler.migration_control_batches.version+1`, [id, status]);
  });
  const crawl = queues['youtube-channel-crawl'], agent = queues['youtube-agent-batch'];
  await t.test('production controller late stopped decision cannot undo new batch recovery', async () => {
    const source = await readFile(new URL('../src/controller.js', import.meta.url), 'utf8');
    const body = source.slice(source.indexOf('async function setPaused('), source.indexOf('\nfunction hasQueueBacklog'));
    const setPaused = new Function('queues', 'migrationQueueControl', 'migrationBatchControlEnabled', `${body}; return setPaused;`)(queues, control, () => true);
    await state('completed', 'stopped', 'old');
    await control.reconcile();
    const oldSnapshot = { status: 'stopped', pipeline_cycle_id: 'old' };
    const gate = deferred();
    const oldTick = gate.promise.then(() => setPaused(crawl.name, true, 'query_scheduler_stopped', [], oldSnapshot));
    await state('running');
    await control.request();
    const result = await control.reconcile();
    assert.equal(result.batch_id, 'new');
    assert.equal(await crawl.isPaused(), false);
    assert.equal(await agent.isPaused(), false);
    gate.resolve(); await oldTick;
    assert.equal(await crawl.isPaused(), false);
  });
  await t.test('priority and delayed jobs remain consumable after fenced resume', async () => {
    await crawl.pause();
    const received = new Set();
    const worker = new Worker(crawl.name, async job => received.add(job.name), { connection, prefix });
    try {
      await crawl.add('priority', {}, { priority: 1 });
      await crawl.add('delayed', {}, { delay: 100 });
      await control.reconcile();
      await until(() => received.size === 2);
    } finally { await worker.close(); }
  });
  await t.test('Redis rejects delayed commands after newer revision, even across database rollback', async () => {
    let oldRevision;
    await assert.rejects(tx(async c => {
      await c.query("SELECT value_json FROM crawler.settings WHERE setting_key='query_scheduler' FOR UPDATE");
      oldRevision = (await c.query("SELECT nextval('crawler.migration_queue_control_revision')::text AS n")).rows[0].n;
      throw new Error('connection lost');
    }), /connection lost/);
    const newer = await control.reconcile();
    assert.ok(BigInt(newer.revision) > BigInt(oldRevision));
    assert.equal(await applyFencedQueueState(crawl, { paused: true, revision: oldRevision, reason: 'late' }), -1);
    assert.equal(await crawl.isPaused(), false);
  });
  await t.test('pause/resume same batch, drain, manual stop and genuine pressure remain distinct', async () => {
    await state('paused'); await control.reconcile();
    assert.equal(await crawl.isPaused(), true); assert.equal(await agent.isPaused(), true);
    await state('running'); await control.reconcile(); assert.equal(await crawl.isPaused(), false);
    ready = 0; await control.reconcile(); assert.equal(await crawl.isPaused(), true);
    assert.equal(await agent.isPaused(), false);
    ready = 10; await control.reconcile(); assert.equal(await crawl.isPaused(), false);
    await state('pausing'); await control.reconcile(); assert.equal(await agent.isPaused(), false);
    await state('running', 'paused'); await control.reconcile(); assert.equal(await crawl.isPaused(), true);
    await state('completed', 'stopped'); await control.reconcile(); assert.equal(await agent.isPaused(), false);
    assert.equal(await crawl.isPaused(), false);
    recovery = [agent.name]; await control.reconcile(); assert.equal(await agent.isPaused(), false);
    assert.equal(await crawl.isPaused(), false); recovery = [];
  });
  await t.test('failed immediate recovery is pending; independent loop recovers while main loop is blocked', async () => {
    await state('running'); await crawl.pause(); await agent.pause(); fail = true;
    assert.equal((await control.request()).pending, true);
    await assert.rejects(control.reconcile(), /temporary outage/);
    fail = false;
    const gate = deferred(); let mainDone = false;
    const loops = createControllerWorkLoops({ tasks: {
      slow_main: { intervalMs: 10000, run: async () => { await gate.promise; mainDone = true; } },
      queue_control: { intervalMs: 100, run: () => control.reconcile() },
    } });
    loops.start();
    try { await until(async () => !await crawl.isPaused()); assert.equal(mainDone, false); }
    finally { gate.resolve(); await loops.shutdown(); }
  });
  await t.test('a newer manual pause wins while an API recovery waits for pressure', async () => {
    const loaded = deferred(), release = deferred();
    const request = createMigrationQueueControl({ withTransaction: tx, queues, loadPressure: async () => {
      loaded.resolve(); await release.promise;
      return { at: Date.now(), channelReady: 10, detailReady: 10, detailBacklog: 0 };
    } });
    const pending = request.reconcile(); await loaded.promise;
    await state('paused'); release.resolve();
    assert.equal((await pending).batch_status, 'paused');
    assert.equal(await crawl.isPaused(), true);
  });
  await t.test('committed batch survives a partially applied Redis failure and restart repairs all queues', async () => {
    await state('running');
    const original = agent.isPaused.bind(agent); let once = true;
    agent.isPaused = async () => { if (once) { once = false; throw new Error('lost acknowledgement'); } return original(); };
    try { await assert.rejects(control.reconcile(), /lost acknowledgement/); }
    finally { agent.isPaused = original; }
    const restarted = createMigrationQueueControl({ withTransaction: tx, queues,
      loadPressure: async () => ({ at: Date.now(), channelReady: 10, detailReady: 10, detailBacklog: 0 }) });
    await restarted.reconcile();
    assert.equal(await crawl.isPaused(), false); assert.equal(await agent.isPaused(), false);
    const stored = (await query("SELECT value_json FROM crawler.settings WHERE setting_key='migration_queue_control'")).rows[0].value_json;
    assert.equal(stored.batch_id, 'new'); assert.equal(stored.queues[agent.name].paused, false);
  });
  await t.test('queued API jobs keep their consumer running; disabled fallback is respected', async () => {
    const api = queues['youtube-data-api-batch'];
    await api.add('api', {}); await control.reconcile(); assert.equal(await api.isPaused(), false);
    await query(`INSERT INTO crawler.settings(setting_key,value_json) VALUES('youtube_api','{"fallback_mode":"disabled"}')
      ON CONFLICT(setting_key) DO UPDATE SET value_json=EXCLUDED.value_json`);
    await control.reconcile(); assert.equal(await api.isPaused(), true);
  });
});
