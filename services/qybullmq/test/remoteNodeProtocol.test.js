import assert from 'node:assert/strict';
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { RemoteNodeExecutor } from '../src/remoteNodes/executor.js';
import { RemoteResultSpool } from '../src/remoteNodes/spool.js';
import { decodeResult, encodeResult, MAX_GZIP_BYTES } from '../src/remoteNodes/protocol.js';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';
import { createRemoteAboutIngestion } from '../src/remoteNodes/aboutIngestion.js';

test('isolated runner refuses other databases and About ingestion requires a business fence', async () => {
  await assert.rejects(assertIsolatedRemoteDatabase({ query: async () => ({ rows: [{ name: 'crawler' }] }) }), /requires database/);
  assert.throws(() => createRemoteAboutIngestion({}), /business fence required/);
});

test('disk budget and blocked evidence stop new work without deleting evidence', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'remote-spool-cap-'));
  try {
    const spool = new RemoteResultSpool({ directory, maxBytes: 2 * MAX_GZIP_BYTES });
    await spool.init();
    assert.equal(await spool.writable(), true);
    await writeFile(join(directory, 'orphan.tmp'), Buffer.alloc(MAX_GZIP_BYTES));
    let claims = 0;
    const executor = new RemoteNodeExecutor({ spool, client: { claim: async () => claims++ }, execute: async () => ({}) });
    assert.equal(await executor.runOnce(), 'blocked');
    assert.equal(claims, 0);
    assert.equal((await readFile(join(directory, 'orphan.tmp'))).length, MAX_GZIP_BYTES);
    await rm(join(directory, 'orphan.tmp'));
    await spool.save('pending.json', Buffer.from('{}'));
    await spool.block();
    assert.equal(await spool.writable(), false);
    assert.ok((await readdir(directory)).some((name) => name.endsWith('.blocked')));
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('concurrent network and result writes cannot jointly exceed the spool budget', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'remote-spool-concurrent-'));
  try {
    const spool = new RemoteResultSpool({ directory, maxBytes: 2 * MAX_GZIP_BYTES }); await spool.init();
    const bytes = Buffer.alloc(Math.ceil(MAX_GZIP_BYTES * 1.2));
    const results = await Promise.allSettled([spool.save('network.json', bytes), spool.save('pending.json', bytes)]);
    assert.equal(results.filter(r => r.status === 'fulfilled').length, 1);
    assert.equal(results.find(r => r.status === 'rejected').reason.message, 'SPOOL_FULL');
    assert.ok(await spool.usage() <= spool.maxBytes);
    await spool.remove('network.json'); await spool.remove('pending.json');
    // A rejected space check must not poison the serialization chain.
    await spool.save('claim.json', Buffer.from('{}'));
    assert.deepEqual(await spool.read('claim.json'), {});
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('renewal space checks do not race with claim cleanup', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'remote-spool-cleanup-'));
  try {
    const spool = new RemoteResultSpool({ directory }); await spool.init();
    await spool.save('claim.json', Buffer.from('{}'));
    const results = await Promise.allSettled(Array.from({ length: 30 }, (_, n) => (
      n % 3 === 0 ? spool.remove('claim.json') : n % 3 === 1
        ? spool.save('network.json', Buffer.from(JSON.stringify({ renewal: n }))) : spool.save('claim.json', Buffer.from('{}'))
    )));
    for (const result of results) if (result.status === 'rejected') throw result.reason;
    assert.deepEqual(await spool.read('network.json'), { renewal: 28 });
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('an oversized extraction becomes an explicit failed result and never a successful truncated payload', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'remote-spool-size-'));
  try {
    let uploaded;
    const client = {
      claim: async () => ({ task_id: randomUUID(), generation: 1, heartbeat_ms: 30000 }),
      heartbeat: async () => ({}),
      upload: async (_taskId, bytes) => {
        uploaded = (await decodeResult(bytes)).value;
        return { durable: true, status: 'received', batch_id: uploaded.batch_id };
      },
    };
    const executor = new RemoteNodeExecutor({ client, spool: new RemoteResultSpool({ directory }),
      execute: async () => ({ tooMuch: 'x'.repeat(5 * 1024 * 1024) }) });
    assert.equal(await executor.runOnce(), 'uploaded');
    assert.equal(uploaded.outcome, 'failure');
    assert.equal(uploaded.error.code, 'RESULT_TOO_LARGE');
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('a temporary upload failure or invalid acknowledgement never deletes pending results', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'remote-spool-ack-'));
  try {
    const spool = new RemoteResultSpool({ directory });
    await spool.init();
    const payload = await encodeResult({ version: 1, batch_id: randomUUID(), generation: 1, outcome: 'success', data: { count: 1 } });
    await spool.save('pending.json', Buffer.from(JSON.stringify({ task_id: randomUUID(), payload: payload.toString('base64') })));
    const executor = new RemoteNodeExecutor({ spool, client: { upload: async () => ({ durable: false, status: 'received' }) } });
    await assert.rejects(executor.runOnce(), { code: 'INVALID_RECEIPT' });
    assert.ok(await spool.read('pending.json'));
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('wire format rejects ambiguous batch IDs and preserves failure evidence', async () => {
  const value = { version: 1, batch_id: randomUUID(), generation: 1, outcome: 'failure', error: { code: 'COMMENTS_PARSE_ERROR' } };
  assert.deepEqual((await decodeResult(await encodeResult(value))).value, value);
  await assert.rejects(decodeResult(await encodeResult({ ...value, batch_id: value.batch_id.toUpperCase() })), { code: 'NON_CANONICAL_ID' });
});
