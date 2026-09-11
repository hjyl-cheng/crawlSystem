import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, symlink, chmod, rm } from 'node:fs/promises';
import { tmpdir, uptime } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { RemoteWorkerConnection } from '../src/remoteNodes/workerConnection.js';
import { parseWorkerConfig, readNodeFile } from '../src/remoteNodes/workerConfig.js';
import { checkNodeConnectionHealth } from '../src/remoteNodes/nodeConnectionRuntime.js';

function config(patch = {}) {
  return { version: 1, mode: 'connect_only', node_id: randomUUID(), deployment_id: randomUUID(), slot: 'incremental-1', role: 'incremental', gateway_url: 'https://center.example/remote-node', ...patch };
}
test('config rejects active collection, embedded credentials and unsafe connection URLs', () => {
  assert.match(parseWorkerConfig(Buffer.from(JSON.stringify(config()))).config_hash, /^[a-f0-9]{64}$/);
  for (const patch of [{ mode: 'active' }, { role: 'fullcrawl' }, { token: 'embedded' }, { gateway_url: 'http://center.example' },
    { gateway_url: 'https://user:secret@center.example' }, { gateway_url: 'https://center.example?token=secret' },
    { slot: '../escape' }, { slot: undefined }, { slot: 123 }]) {
    assert.throws(() => parseWorkerConfig(Buffer.from(JSON.stringify(config(patch)))));
  }
});

test('secret files are bounded, reject symlinks and require restricted permissions', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'remote-worker-secret-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const path = join(directory, 'token');
  await writeFile(path, 'fixture-secret', { mode: 0o600 });
  assert.equal((await readNodeFile(path, { secret: true })).toString(), 'fixture-secret');
  await symlink(path, join(directory, 'link'));
  await assert.rejects(() => readNodeFile(join(directory, 'link'), { secret: true }));
  await chmod(path, 0o644);
  await assert.rejects(() => readNodeFile(path, { secret: true }), /NODE_FILE_INVALID/);
  await assert.rejects(() => readNodeFile(path, { maxBytes: 2 }), /NODE_FILE_INVALID/);
});

test('connection retries preserve instance identity and never claim channel work', async () => {
  const stop = new AbortController(); const calls = []; const reports = [];
  const runner = new RemoteWorkerConnection({ config: parseWorkerConfig(Buffer.from(JSON.stringify(config()))), intervalMs: 50,
    localRota: { boot: async () => ({ boot_id: 'b'.repeat(48) }) },
    client: { async workerHeartbeat(value) {
      calls.push(value);
      if (calls.length === 1) throw new Error('temporary network failure');
      return { ...value, state: 'connected_waiting_activation', ready_for_tasks: false, server_time: new Date().toISOString(), connected_until: new Date(Date.now()+45000).toISOString() };
    }, claim() { assert.fail('connection verification must not claim work'); } },
    report: async status => { reports.push(status); if (status.state === 'connected_waiting_activation') stop.abort(); },
  });
  await runner.run({ signal: stop.signal });
  assert.deepEqual(reports.map(item => item.state), ['disconnected', 'connected_waiting_activation', 'stopped']);
  assert.equal(calls[0].instance_id, calls[1].instance_id);
  assert.ok(reports.every(item => item.ready_for_tasks === false));
});

test('wrong identity or a center that unexpectedly enables work cannot make the node ready', async () => {
  for (const patch of [{ node_id: randomUUID() }, { ready_for_tasks: true }, { config_hash: 'different' }]) {
    const runner = new RemoteWorkerConnection({ config: config(), localRota: { boot: async () => ({ boot_id: 'a'.repeat(48) }) },
      client: { workerHeartbeat: async value => ({ ...value, state: 'connected_waiting_activation', ready_for_tasks: false, ...patch }) } });
    await assert.rejects(() => runner.probe(), /NODE_CONNECTION_RECEIPT_MISMATCH/);
  }
});

test('expired health and stopped processes fail Docker health checks', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'remote-worker-health-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const path = join(directory, 'health.json');
  const state = { version: 1, state: 'connected_waiting_activation', ready_for_tasks: false, valid_until_uptime: uptime()+30 };
  await writeFile(path, JSON.stringify(state));
  await checkNodeConnectionHealth(path);
  for (const patch of [{ valid_until_uptime: uptime()-1 }, { state: 'stopped' }, { ready_for_tasks: true }]) {
    await writeFile(path, JSON.stringify({ ...state, ...patch }));
    await assert.rejects(() => checkNodeConnectionHealth(path), /NODE_CONNECTION_UNHEALTHY/);
  }
});
