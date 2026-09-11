import assert from 'node:assert/strict';
import { randomBytes, randomUUID } from 'node:crypto';
import { readFile, mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import pg from 'pg';
import { RemoteNodeStore } from '../src/remoteNodes/store.js';
import { createRemoteNodeGateway } from '../src/remoteNodes/gateway.js';
import { createRemoteNodeClient } from '../src/remoteNodes/client.js';
import { RemoteNodeExecutor } from '../src/remoteNodes/executor.js';
import { RemoteResultSpool } from '../src/remoteNodes/spool.js';
import { encodeResult, RemoteProtocolError } from '../src/remoteNodes/protocol.js';
import { createRemoteAboutExtractor, ABOUT_CAPABILITY } from '../src/remoteNodes/aboutExtractor.js';
import { createRemoteAboutIngestion } from '../src/remoteNodes/aboutIngestion.js';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';
import { PUBLICATION_WRITER_VERSION } from '../src/publicationWriterVersion.js';
import { runRemoteResultProcessor } from '../src/remoteNodes/processor.js';

const databaseUrl = process.env.REMOTE_NODE_TEST_DATABASE_URL;

test('remote node ingestion with isolated PostgreSQL, HTTP and durable spool', {
  skip: !databaseUrl,
  timeout: 120000,
}, async (t) => {
  const pool = new pg.Pool({ connectionString: databaseUrl, max: 4, connectionTimeoutMillis: 5000,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}` });
  const guard = await pool.connect();
  t.after(async () => { guard.release(); await pool.end(); });
  await assertIsolatedRemoteDatabase(pool);
  await guard.query('SELECT pg_advisory_lock(781137981)');
  await pool.query(await readFile(new URL('../src/schema.sql', import.meta.url), 'utf8'));
  await pool.query(await readFile(new URL('../src/remoteNodes/schema.sql', import.meta.url), 'utf8'));
  await pool.query(`CREATE TABLE IF NOT EXISTS remote_ingestion.test_business_writes (
    task_id UUID PRIMARY KEY, applications INTEGER NOT NULL DEFAULT 1)`);
  const store = new RemoteNodeStore({ pool, retrySeconds: 1 });
  const server = createRemoteNodeGateway({ store });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => { server.close(resolve); server.closeAllConnections(); }));
  const gatewayUrl = `http://127.0.0.1:${server.address().port}`;
  const capability = 'test.fetch.v1';
  async function node(maxLeases = 1, capabilities = [capability]) {
    const nodeId = randomUUID();
    const token = randomBytes(32).toString('hex');
    await store.registerNode({ nodeId, token, capabilities, maxLeases });
    return { nodeId, token, client: createRemoteNodeClient({ url: gatewayUrl, token, allowLoopbackHttp: true }) };
  }
  async function enqueue(overrides = {}) {
    return store.enqueue({ workKey: randomUUID(), capability, input: { channel_id: 'UCtest' }, context: {}, ...overrides });
  }
  async function result(lease, overrides = {}) {
    return encodeResult({ version: 1, batch_id: randomUUID(), generation: lease.generation,
      outcome: 'success', data: { title: 'test' }, ...overrides });
  }
  async function apply(client, { task }) {
    await client.query(`INSERT INTO remote_ingestion.test_business_writes(task_id) VALUES($1)
      ON CONFLICT(task_id) DO UPDATE SET applications=test_business_writes.applications+1`, [task.task_id]);
    return { written: true };
  }
  async function expire(taskId) {
    await pool.query("UPDATE remote_ingestion.tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE task_id=$1", [taskId]);
  }
  async function readyAgain(taskId) {
    await pool.query("UPDATE remote_ingestion.tasks SET next_process_at=clock_timestamp() WHERE task_id=$1", [taskId]);
  }
  async function state(taskId) {
    return (await pool.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1', [taskId])).rows[0];
  }
  async function reset() {
    await pool.query(`TRUNCATE remote_ingestion.channel_commands,remote_ingestion.receipts,remote_ingestion.claims,remote_ingestion.tasks,
      remote_ingestion.nodes,remote_ingestion.test_business_writes CASCADE`);
  }
  async function withSpool(action) {
    const directory = await mkdtemp(join(tmpdir(), 'remote-node-spool-test-'));
    try { return await action(new RemoteResultSpool({ directory }), directory); }
    finally { await rm(directory, { recursive: true, force: true }); }
  }

  await t.test('authenticated HTTPS policy, capability scoping, and hidden central context', async () => {
    await reset();
    const a = await node();
    assert.throws(() => createRemoteNodeClient({ url: 'http://example.test', token: a.token }), /HTTPS/);
    await assert.rejects(createRemoteNodeClient({ url: gatewayUrl, allowLoopbackHttp: true, token: 'x'.repeat(64) }).claim(randomUUID()),
      { code: 'UNAUTHORIZED' });
    await enqueue({ capability: 'not.allowed' });
    assert.equal(await a.client.claim(randomUUID()), null);
    const taskId = await enqueue({ context: { run_id: 'central-only', private_value: 'never-send' } });
    const lease = await a.client.claim(randomUUID());
    assert.equal(lease.task_id, taskId);
    assert.equal(JSON.stringify(lease).includes('never-send'), false);
    await store.setNodeState(a.nodeId, 'disabled');
    await assert.rejects(a.client.heartbeat(lease), { code: 'UNAUTHORIZED' });
  });

  await t.test('work key deduplicates identical work and rejects a changed command', async () => {
    await reset();
    const workKey = randomUUID();
    const taskId = await enqueue({ workKey });
    assert.equal(await enqueue({ workKey }), taskId);
    await assert.rejects(enqueue({ workKey, input: { changed: true } }), { code: 'WORK_KEY_CONFLICT' });
  });

  await t.test('concurrent claim requests cannot exceed node capacity; replay recovers a lost claim response', async () => {
    await reset();
    const a = await node(2);
    for (let i = 0; i < 4; i++) await enqueue();
    const claims = Array.from({ length: 5 }, () => randomUUID());
    const leases = await Promise.all(claims.map((id) => a.client.claim(id)));
    assert.equal(leases.filter(Boolean).length, 2);
    const i = leases.findIndex(Boolean);
    assert.equal((await a.client.claim(claims[i])).task_id, leases[i].task_id);
    assert.equal((await a.client.claim(claims[i])).generation, leases[i].generation);
  });

  await t.test('expired or foreign executions cannot heartbeat or write, and old claim IDs cannot create new work', async () => {
    await reset();
    const a = await node(); const b = await node();
    const taskId = await enqueue();
    const claimId = randomUUID();
    const old = await a.client.claim(claimId);
    await assert.rejects(b.client.heartbeat(old), { code: 'STALE_LEASE' });
    await assert.rejects(b.client.upload(taskId, await result(old)), { code: 'STALE_LEASE' });
    await expire(taskId);
    await assert.rejects(a.client.heartbeat(old), { code: 'STALE_LEASE' });
    const current = await b.client.claim(randomUUID());
    assert.equal(current.generation, old.generation + 1);
    await assert.rejects(a.client.claim(claimId), { code: 'CLAIM_EXPIRED' });
    await assert.rejects(a.client.upload(taskId, await result(old)), { code: 'STALE_LEASE' });
    await b.client.upload(taskId, await result(current));
    await expire(taskId);
    assert.equal(await a.client.claim(randomUUID()), null);
  });

  await t.test('durable receipt deduplicates uploads, conflicts and cross-node reads are rejected', async () => {
    await reset();
    const a = await node(); const b = await node();
    const taskId = await enqueue();
    const lease = await a.client.claim(randomUUID());
    const batchId = randomUUID();
    const bytes = await result(lease, { batch_id: batchId });
    const acks = await Promise.all([a.client.upload(taskId, bytes), a.client.upload(taskId, bytes)]);
    assert.ok(acks.every((ack) => ack.status === 'received' && ack.durable));
    await assert.rejects(a.client.upload(taskId, await result(lease, { batch_id: batchId, data: { changed: true } })), { code: 'BATCH_CONFLICT' });
    await assert.rejects(b.client.upload(taskId, bytes), { code: 'BATCH_CONFLICT' });
    await assert.rejects(b.client.receipt(batchId), { code: 'NOT_FOUND' });
    assert.equal((await a.client.receipt(batchId)).status, 'received');
    await store.processOne({ [capability]: apply });
    assert.equal((await a.client.upload(taskId, bytes)).status, 'applied');
    assert.equal(await store.processOne({ [capability]: apply }), null);
    assert.equal((await pool.query('SELECT applications FROM remote_ingestion.test_business_writes')).rows[0].applications, 1);
  });

  await t.test('business write then failure rolls back business rows; a later retry applies once', async () => {
    await reset();
    const a = await node(); const taskId = await enqueue();
    const lease = await a.client.claim(randomUUID());
    await a.client.upload(taskId, await result(lease));
    const first = await store.processOne({ [capability]: async (...args) => {
      await apply(...args); throw new Error('simulate crash after business write');
    } });
    assert.equal(first.retry, true);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM remote_ingestion.test_business_writes')).rows[0].n, 0);
    assert.equal((await state(taskId)).state, 'received');
    await readyAgain(taskId);
    assert.equal((await store.processOne({ [capability]: apply })).status, 'applied');
    assert.equal((await state(taskId)).process_attempts, 2);
  });

  await t.test('database COMMIT succeeds but caller loses confirmation: no second business application', async () => {
    await reset();
    const a = await node(); const taskId = await enqueue();
    const lease = await a.client.claim(randomUUID());
    await a.client.upload(taskId, await result(lease));
    const lostResponsePool = { connect: async () => {
      const client = await pool.connect();
      return { release: () => client.release(), query: async (...args) => {
        const output = await client.query(...args);
        if (args[0] === 'COMMIT') throw new Error('simulated lost COMMIT response');
        return output;
      } };
    } };
    await assert.rejects(new RemoteNodeStore({ pool: lostResponsePool }).processOne({ [capability]: apply }), /lost COMMIT/);
    assert.equal((await state(taskId)).state, 'applied');
    assert.equal(await store.processOne({ [capability]: apply }), null);
    assert.equal((await pool.query('SELECT applications FROM remote_ingestion.test_business_writes')).rows[0].applications, 1);
  });

  await t.test('concurrent processors lock different receipts; one receipt has only one business writer', async () => {
    await reset();
    const a = await node(2);
    for (let i = 0; i < 2; i++) {
      const taskId = await enqueue(); const lease = await a.client.claim(randomUUID());
      await a.client.upload(taskId, await result(lease));
    }
    const results = await Promise.all(Array.from({ length: 4 }, () => store.processOne({ [capability]: apply })));
    assert.equal(results.filter(Boolean).length, 2);
    assert.ok((await pool.query('SELECT applications FROM remote_ingestion.test_business_writes')).rows.every((row) => row.applications === 1));
  });

  await t.test('separate processor stops intake and commits its in-flight transaction before exiting', async () => {
    await reset();
    const a = await node(); const taskId = await enqueue();
    const lease = await a.client.claim(randomUUID());
    await a.client.upload(taskId, await result(lease));
    const controller = new AbortController();
    await runRemoteResultProcessor({ store, signal: controller.signal, handlers: { [capability]: async (...args) => {
      controller.abort();
      return apply(...args);
    } } });
    assert.equal((await state(taskId)).state, 'applied');
  });

  await t.test('bounded processing retries preserve the failed receipt and never mark it applied', async () => {
    await reset();
    const a = await node(); const taskId = await enqueue();
    const lease = await a.client.claim(randomUUID());
    await a.client.upload(taskId, await result(lease));
    const limited = new RemoteNodeStore({ pool, maxProcessAttempts: 2 });
    const bad = { [capability]: async () => { throw new Error('temporary downstream transaction failure'); } };
    await limited.processOne(bad); await readyAgain(taskId);
    assert.equal((await limited.processOne(bad)).status, 'failed');
    assert.equal((await state(taskId)).applied_at, null);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM remote_ingestion.receipts')).rows[0].n, 1);
    assert.equal(await limited.processOne(bad), null);
  });

  await t.test('remote extraction failures remain failures, and execution lease retries are bounded', async () => {
    await reset();
    const a = await node(); const taskId = await enqueue();
    const lease = await a.client.claim(randomUUID());
    await a.client.upload(taskId, await result(lease, { outcome: 'failure', error: { code: 'PARSER_MISSING_FIELD' } }));
    assert.equal((await store.processOne({ [capability]: apply })).status, 'failed');
    assert.equal((await state(taskId)).last_error, 'REMOTE:PARSER_MISSING_FIELD');
    const abandonedId = await enqueue();
    for (let i = 0; i < 3; i++) { await a.client.claim(randomUUID()); await expire(abandonedId); }
    assert.equal(await a.client.claim(randomUUID()), null);
    assert.equal((await state(abandonedId)).last_error, 'EXECUTION_LEASES_EXHAUSTED');
  });

  await t.test('draining finishes leased work, central cancellation blocks application, backlog stops new claims', async () => {
    await reset();
    const a = await node(2); const taskId = await enqueue();
    const lease = await a.client.claim(randomUUID());
    await enqueue(); await store.setNodeState(a.nodeId, 'draining');
    assert.equal(await a.client.claim(randomUUID()), null);
    await a.client.heartbeat(lease);
    const ack = await a.client.upload(taskId, await result(lease));
    assert.equal(ack.status, 'received');
    await store.setNodeState(a.nodeId, 'active');
    assert.equal(await new RemoteNodeStore({ pool, maxBacklog: 1 }).claim(a.nodeId, randomUUID()), null);
    await store.cancel(taskId);
    assert.equal(await store.processOne({ [capability]: apply }), null);
    assert.equal((await a.client.receipt(ack.batch_id)).status, 'cancelled');
  });

  await t.test('lost HTTP receipt survives executor restart and uploads without another extraction', async () => {
    await reset();
    const a = await node(); await enqueue();
    await withSpool(async (spool) => {
      let extractions = 0;
      const faulty = { ...a.client, upload: async (...args) => { await a.client.upload(...args); throw new Error('response lost'); } };
      const execute = async () => { extractions++; return { title: 'durable payload' }; };
      await assert.rejects(new RemoteNodeExecutor({ client: faulty, spool, execute }).runOnce(), /response lost/);
      assert.ok(await spool.read('pending.json'));
      assert.equal(await new RemoteNodeExecutor({ client: a.client, spool, execute }).runOnce(), 'uploaded');
      assert.equal(extractions, 1);
      assert.equal(await spool.read('pending.json'), null);
      assert.equal(await spool.read('claim.json'), null);
    });
  });

  await t.test('lost claim HTTP response resumes the same lease after executor restart', async () => {
    await reset();
    const a = await node(); const taskId = await enqueue();
    await withSpool(async (spool) => {
      const faulty = { ...a.client, claim: async (...args) => { await a.client.claim(...args); throw new Error('claim response lost'); } };
      const execute = async () => ({ title: 'resumed' });
      await assert.rejects(new RemoteNodeExecutor({ client: faulty, spool, execute }).runOnce(), /claim response lost/);
      assert.equal(await new RemoteNodeExecutor({ client: a.client, spool, execute }).runOnce(), 'uploaded');
      assert.equal((await state(taskId)).generation, 1);
    });
  });

  await t.test('stale upload remains blocked on disk; graceful stop finishes an active extraction', async () => {
    await reset();
    const a = await node(); await enqueue();
    await withSpool(async (spool, directory) => {
      const executor = new RemoteNodeExecutor({ client: a.client, spool, execute: async (lease) => {
        await expire(lease.task_id); return { title: 'late' };
      } });
      await assert.rejects(executor.runOnce(), { code: 'STALE_LEASE' });
      assert.ok((await readdir(directory)).some((name) => name.endsWith('.blocked')));
      assert.equal(await executor.runOnce(), 'blocked');
    });
    await reset();
    const b = await node(); const taskId = await enqueue();
    await withSpool(async (spool) => {
      let executor;
      executor = new RemoteNodeExecutor({ client: b.client, spool, execute: async () => {
        executor.stop(); return { title: 'finished before stop' };
      } });
      assert.equal(await executor.runOnce(), 'uploaded');
      assert.equal((await state(taskId)).state, 'received');
      assert.equal(await executor.runOnce(), 'stopped');
    });
  });

  await t.test('gzip bomb, invalid bodies and oversized compressed uploads are rejected', async () => {
    await reset();
    const a = await node(); const taskId = await enqueue();
    const lease = await a.client.claim(randomUUID());
    await assert.rejects(a.client.upload(taskId, Buffer.from('not gzip')), { code: 'INVALID_COMPRESSED_RESULT' });
    await assert.rejects(a.client.upload(taskId, Buffer.alloc(1048577)), { code: 'BODY_TOO_LARGE' });
    const { gzipSync } = await import('node:zlib');
    await assert.rejects(a.client.upload(taskId, gzipSync(Buffer.alloc(5 * 1024 * 1024))), { code: 'INVALID_COMPRESSED_RESULT' });
    assert.equal((await state(taskId)).state, 'leased');
    await assert.rejects(result(lease, { data: { text: 'x'.repeat(5 * 1024 * 1024) } }), { code: 'RESULT_TOO_LARGE' });
  });

  await t.test('About goes through HTTP/spool and the existing writer into actual crawler observations and Current', async () => {
    await reset();
    const channelId = `UC${randomUUID().replaceAll('-', '').slice(0, 22)}`;
    const runId = `remote-test:${randomUUID()}`;
    await pool.query(`INSERT INTO crawler.channels(channel_id,channel_url,title,status)
      VALUES($1,$2,'Before remote About','active')`, [channelId, `https://www.youtube.com/channel/${channelId}`]);
    await pool.query(`INSERT INTO crawler.channel_runs(run_id,channel_id,status,crawl_mode)
      VALUES($1,$2,'running','incremental')`, [runId, channelId]);
    const a = await node(1, [ABOUT_CAPABILITY]);
    const taskId = await enqueue({ capability: ABOUT_CAPABILITY, input: { channel_id: channelId }, context: {
      run_id: runId, execution_attempt_id: 'isolated-attempt-1', trigger_reason: 'manual', crawler_version: 'remote-isolation-test',
    } });
    const snapshot = {
      about_requested: true, about_observed: true, about_error: null,
      metadata: { channel_id: channelId, title: 'Remote About verified', handle: '@remote-test',
        description: 'YouTubeJS snapshot fixture', keywords: ['code'], available_tabs: ['videos'],
        external_links: [], external_links_status: 'observed', country: 'Brazil',
        subscriber_count_text: '1,234 subscribers', subscriber_count_source: 'youtube_about',
        video_count_text: '42 videos', video_count_source: 'youtube_about',
        view_count_text: '98,765 views', view_count_source: 'youtube_about' },
      raw: { engine: 'youtubei.js@17.2.0', request_counts: { get_channel: 1, get_about: 1 } },
    };
    const execute = createRemoteAboutExtractor({ openChannel: async () => snapshot,
      withSession: async (_lease, _options, action) => action() });
    await withSpool(async (spool) => {
      assert.equal(await new RemoteNodeExecutor({ client: a.client, spool, execute }).runOnce(), 'uploaded');
    });
    const handlers = createRemoteAboutIngestion({ assertBusinessFence: async (client, task) => {
      const run = (await client.query('SELECT * FROM crawler.channel_runs WHERE run_id=$1 FOR UPDATE', [task.context.run_id])).rows[0];
      if (run?.status !== 'running' || run.channel_id !== task.input.channel_id) throw new RemoteProtocolError('BUSINESS_FENCE_STALE');
    } });
    const applied = await store.processOne(handlers);
    if (applied.status !== 'applied') assert.fail(`About apply failed: ${(await state(taskId)).last_error}`);
    const current = (await pool.query('SELECT title,handle FROM crawler.channels WHERE channel_id=$1', [channelId])).rows[0];
    assert.equal(current.title, 'Remote About verified');
    assert.equal(current.handle, '@remote-test');
    const metrics = (await pool.query(`SELECT subscriber_count,total_view_count,total_video_count
      FROM crawler.channel_about_metric_snapshots WHERE channel_id=$1`, [channelId])).rows;
    assert.equal(metrics.length, 1);
    assert.deepEqual(Object.values(metrics[0]).map(Number), [1234, 98765, 42]);
    assert.equal((await state(taskId)).applied_result.outcome, 'complete');
    assert.equal(await store.processOne(handlers), null);

    // A valid network lease cannot override a superseded central business run.
    const staleTaskId = await enqueue({ capability: ABOUT_CAPABILITY, input: { channel_id: channelId }, context: {
      run_id: runId, execution_attempt_id: 'isolated-attempt-2', trigger_reason: 'manual', crawler_version: 'remote-isolation-test',
    } });
    const staleLease = await a.client.claim(randomUUID());
    await a.client.upload(staleTaskId, await result(staleLease, { data: { observed_at: new Date().toISOString(), snapshot } }));
    await pool.query("UPDATE crawler.channel_runs SET status='failed' WHERE run_id=$1", [runId]);
    assert.equal((await store.processOne(handlers)).status, 'failed');
    assert.equal((await state(staleTaskId)).last_error, 'BUSINESS_FENCE_STALE');
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM crawler.crawl_observations WHERE channel_id=$1', [channelId])).rows[0].n, 1);

    const forgedTaskId = await enqueue({ capability: ABOUT_CAPABILITY, input: { channel_id: channelId }, context: {
      run_id: runId, execution_attempt_id: 'isolated-attempt-3', trigger_reason: 'manual', crawler_version: 'remote-isolation-test',
    } });
    const forgedLease = await a.client.claim(randomUUID());
    await a.client.upload(forgedTaskId, await result(forgedLease, { data: {
      observed_at: new Date().toISOString(), snapshot: { ...snapshot, metadata: { ...snapshot.metadata, channel_id: 'UCwrong-channel' } },
    } }));
    assert.equal((await store.processOne(handlers)).status, 'failed');
    assert.equal((await state(forgedTaskId)).last_error, 'INVALID_ABOUT_RESULT');
  });
});
