import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import pg from 'pg';
import { Queue, Worker } from 'bullmq';
import { createControllerWorkLoops } from '../src/controllerWorkLoops.js';
import { createMigrationControlBatch, prepareMigrationControlList, refillMigrationControl, maintainMigrationControl, startControlledMigrationChannel, controlMigrationBatch } from '../src/migrationBatchControl.js';
import { sourceSnapshotHash } from '../src/migrationSource.js';
import { PUBLICATION_WRITER_VERSION } from '../src/publicationWriterVersion.js';
import { createMigrationProgressReader, sampleMigrationThroughput } from '../src/migrationThroughput.js';

const url = process.env.THROUGHPUT_CONTROLLER_TEST_DATABASE_URL;
const port = Number(process.env.THROUGHPUT_TEST_REDIS_PORT);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check) {
  const deadline = Date.now() + 45000;
  while (!await check()) {
    assert.ok(Date.now() < deadline, 'controller intake did not make progress');
    await sleep(50);
  }
}

test('40 concurrent real consumers keep receiving frozen migration jobs during a 120-second SQL stall; pause fences new admission', { skip: !url || !port }, async t => {
  assert.equal(new URL(url).pathname, '/throughput_controller_test');
  const pool = new pg.Pool({ connectionString: url, max: 45, options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}` });
  const query = pool.query.bind(pool);
  const withTransaction = async action => {
    const c = await pool.connect();
    try { await c.query('BEGIN'); const r = await action(c); await c.query('COMMIT'); return r; }
    catch (error) { await c.query('ROLLBACK'); throw error; }
    finally { c.release(); }
  };
  const connection = { host: '127.0.0.1', port, maxRetriesPerRequest: null };
  const prefix = `controller-throughput-${Date.now()}`;
  const queue = new Queue('crawl', { connection, prefix });
  const blocker = new pg.Client({ connectionString: url });
  await blocker.connect();
  let worker, loops, blockedQuery;
  const errors = [];
  t.after(async () => {
    await query('SELECT pg_cancel_backend($1)', [blocker.processID]);
    await blockedQuery?.catch(() => {});
    await loops?.shutdown();
    await worker?.close();
    await queue.obliterate({ force: true });
    await queue.close();
    await blocker.end();
    await pool.end();
  });
  await query('DROP SCHEMA IF EXISTS publication CASCADE; DROP SCHEMA IF EXISTS crawler CASCADE');
  await query(await readFile(new URL('../src/schema.sql', import.meta.url), 'utf8'));
  await query(await readFile(new URL('../src/migrationInventorySchema.sql', import.meta.url), 'utf8'));
  const source = 'throughput-source';
  const sync = '00000000-0000-0000-0000-000000000001';
  await query(`INSERT INTO crawler.migration_channel_inventory_syncs(source_id,source_database,source_database_oid,status,sync_token)
    VALUES($1,'legacy_test',42,'ready',$2)`, [source, sync]);
  await query(`INSERT INTO crawler.migration_channel_inventory(source_id,source_candidate_id,channel_id,channel_url,source_candidate_status,sync_token)
    SELECT $1,n,'UCthroughput'||n,'https://youtube.com/channel/UCthroughput'||n,'discovered',$2 FROM generate_series(1,250)n`, [source, sync]);
  const batch = await createMigrationControlBatch({ withTransaction, selection: 'all', sourceId: source });
  await prepareMigrationControlList({ withTransaction, batchId: batch.batch_id });
  for (let n = 1; n <= 250; n++) {
    const snapshot = { source_id: source, source_database: 'legacy_test', source_database_oid: '42',
      source_candidate_id: String(n), source_candidate_status: 'discovered', channel_id: `UCthroughput${n}`,
      channel_url: `https://youtube.com/channel/UCthroughput${n}`, title: `channel ${n}`, priority: 100, source_json: {}, snapshot_json: {} };
    await query('UPDATE crawler.migration_control_items SET snapshot_json=$3 WHERE batch_id=$1 AND channel_id=$2',
      [batch.batch_id, snapshot.channel_id, { ...snapshot, snapshot_sha256: sourceSnapshotHash(snapshot) }]);
  }
  const admitted = new Set();
  worker = new Worker('crawl', async job => {
    const result = await startControlledMigrationChannel({ query, withTransaction, batchId: batch.batch_id,
      channelId: job.data.channel_id, executionJobId: job.id });
    if (result.started) admitted.add(job.data.channel_id);
    else job.opts.removeOnComplete = true;
    await sleep(20);
  }, { connection, prefix, concurrency: 40 });
  worker.on('error', error => errors.push(error.message));
  worker.on('failed', (_job, error) => errors.push(error.message));
  let maintenanceFinished = false;
  loops = createControllerWorkLoops({
    tasks: {
      blocked_finalize: { intervalMs: 300000, run: async () => {
        blockedQuery = blocker.query('SELECT pg_sleep(120)');
        await blockedQuery;
        maintenanceFinished = true;
      } },
      refill: { intervalMs: 100, run: () => refillMigrationControl({ query, withTransaction, queue }) },
      settlement: { intervalMs: 500, run: () => maintainMigrationControl({ query, withTransaction, settlementPageSize: 20 }) },
    },
    onError: ({ name, error }) => { if (name !== 'blocked_finalize') errors.push(error.message); },
  });
  loops.start();
  await until(() => admitted.size >= 120);
  assert.equal(maintenanceFinished, false);
  const version = (await query('SELECT version FROM crawler.migration_control_batches WHERE batch_id=$1', [batch.batch_id])).rows[0].version;
  await controlMigrationBatch({ withTransaction, batchId: batch.batch_id, action: 'pause', version });
  const countStarted = async () => Number((await query("SELECT count(*) FROM crawler.migration_control_items WHERE batch_id=$1 AND state='started'", [batch.batch_id])).rows[0].count);
  const atPause = await countStarted();
  await sleep(1000);
  assert.equal(await countStarted(), atPause);
  const pausedVersion = (await query('SELECT version FROM crawler.migration_control_batches WHERE batch_id=$1', [batch.batch_id])).rows[0].version;
  await controlMigrationBatch({ withTransaction, batchId: batch.batch_id, action: 'resume', version: pausedVersion });
  await until(() => admitted.size === 250);
  assert.equal(maintenanceFinished, false);
  assert.deepEqual(errors, []);
  await query('SELECT pg_cancel_backend($1)', [blocker.processID]);
  await loops.shutdown();
  await worker.close();

  await t.test('the actual Controller entrypoint starts and drains all independent cycles', async () => {
    const child = spawn(process.execPath, ['src/controller.js'], {
      cwd: new URL('..', import.meta.url),
      env: { ...process.env, DATABASE_URL: url, DATABASE_URL_FILE: '', EXPECTED_CRAWLER_DATABASE: 'throughput_controller_test',
        FORBIDDEN_CRAWLER_DATABASE: 'forbidden', SKIP_SCHEMA_MIGRATION: 'true', POSTGRES_POOL_MIN: '0',
        REDIS_HOST: '127.0.0.1', REDIS_PORT: String(port), REDIS_PASSWORD: '', BULLMQ_PREFIX: prefix,
        ROTA_WORKLOAD_SCOPE_EXPECTED: 'qy-production', ROTA_PROXY_CONTROL_URL: '', ROTA_PROXY_CONTROL_TOKEN: '',
        QUERY_METADATA_AUTO_CYCLE_ENABLED: 'false', CHANNEL_CANDIDATE_DISPATCH_ENABLED: 'false',
        CONTENT_ENRICH_DISPATCH_ENABLED: 'false', MIGRATION_BATCH_CONTROL_ENABLED: 'true',
        CONTROLLER_THROUGHPUT_ENABLED: 'true', FINALIZE_CHANGE_RECOVERY_ENABLED: 'true',
        YOUTUBE_DATA_API_FALLBACK_MODE: 'emergency', CONTROLLER_INTERVAL_MS: '300000' },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let output = '';
    child.stdout.on('data', data => { output += data; });
    child.stderr.on('data', data => { output += data; });
    const exited = new Promise(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
    try {
      await until(() => ['migration_metrics', 'migration_intake', 'migration_settlement', 'video_api', 'finalize_scan', 'finalize_changes']
        .every(name => output.includes(`"name":"${name}"`)));
      assert.doesNotMatch(output, /controller_work_cycle_failed/);
    } catch (error) { throw new Error(`${error.message}\n${output.slice(-8000)}`); }
    finally {
      child.kill('SIGTERM');
      const timer = setTimeout(() => child.kill('SIGKILL'), 15000);
      const result = await exited;
      clearTimeout(timer);
      assert.deepEqual(result, { code: 0, signal: null }, output.slice(-4000));
    }
  });
  await t.test('page polling reads indexed samples with live batch controls, without recounting the inventory', async () => {
    const statements = [];
    const read = createMigrationProgressReader((sql, args) => { statements.push(sql); return query(sql, args); });
    const progress = await read();
    assert.equal(progress.active.batch_id, batch.batch_id);
    assert.equal(progress.active.counts.started, 250);
    assert.equal(progress.active.rolling_rates.minutes_15, null);
    assert.ok(statements.every(sql => !sql.includes('FROM crawler.migration_control_items')));
  });
  await t.test('fetch completion counts a channel once across retries before final settlement', async () => {
    const item = (await query('SELECT * FROM crawler.migration_control_items WHERE batch_id=$1 ORDER BY ordinal LIMIT 1', [batch.batch_id])).rows[0];
    await query('INSERT INTO crawler.channels(channel_id,channel_url) VALUES($1,$2) ON CONFLICT DO NOTHING', [item.channel_id, `https://youtube.com/channel/${item.channel_id}`]);
    await query(`INSERT INTO crawler.channel_runs(run_id,channel_id,candidate_id,detail_status)
      VALUES('metrics-run-1',$1,$2,'done'),('metrics-run-2',$1,$2,'done')`, [item.channel_id,item.candidate_id]);
    await sampleMigrationThroughput(query);
    const progress = await createMigrationProgressReader(query)();
    assert.equal(progress.active.counts.fetch_completed, 1);
    assert.equal(progress.active.counts.started, 250);
    assert.equal(progress.active.counts.success ?? 0, 0);
  });
});
