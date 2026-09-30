import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import { retryableRotaFailure } from '../src/managedWorkerExecution.js';
import { currentProxyIdentity } from '../src/proxyIdentity.js';
import { classifyYoutubeError } from '../src/youtube.js';
import { annotateYoutubeFailure } from '../src/youtubeFailurePolicy.js';
import { fromChannelWire, toChannelWire } from '../src/remoteNodes/channelWire.js';
import {
  collectingSlotValid,
  collectingWorkload,
  dedicatedWorkloadForCapability,
  DISCOVER_WORKLOAD,
  FULL_CRAWL_WORKLOAD,
} from '../src/remoteNodes/collectingWorkload.js';
import {
  assertDiscoverRequest,
  discoverAttemptId,
  discoverBusinessRunId,
  discoverPageFromWire,
  discoverTaskInput,
  DISCOVER_PAGE_OPERATION,
} from '../src/remoteNodes/discoverPageContract.js';
import {
  createDiscoverRouteRuntime,
  executeDiscoverCommand,
  RemoteDiscoverExecutor,
} from '../src/remoteNodes/discoverNode.js';
import { RemoteManagedDiscoverRuntime } from '../src/remoteNodes/managedDiscoverRuntime.js';
import { decodeResult } from '../src/remoteNodes/protocol.js';
import { RemoteResultSpool } from '../src/remoteNodes/spool.js';

const initial = { kind: 'initial', queryText: 'receitas', language: 'pt', country: 'BR', ytConfig: null, continuation: null };
const page = { kind: 'initial', url: 'https://www.youtube.com/results?search_query=receitas', status: 200,
  rawText: '<html>fixture</html>', rawContentType: 'text/html; charset=utf-8' };

test('Discover is a dedicated workload with its own slots and Rota role', () => {
  assert.equal(collectingWorkload('discover_collect'), DISCOVER_WORKLOAD);
  assert.equal(DISCOVER_WORKLOAD.rotaRole, 'discover');
  assert.equal(FULL_CRAWL_WORKLOAD.rotaRole, 'channel');
  assert.equal(collectingWorkload('incremental_collect').rotaRole, 'channel');
  assert.equal(dedicatedWorkloadForCapability('youtube.discover-page.v1'), DISCOVER_WORKLOAD);
  assert.equal(dedicatedWorkloadForCapability('youtube.incremental.plan.v1'), null);
  assert.equal(collectingSlotValid(DISCOVER_WORKLOAD, 'discover-1'), true);
  assert.equal(collectingSlotValid(DISCOVER_WORKLOAD, 'discover-0'), false);
  assert.equal(collectingSlotValid(DISCOVER_WORKLOAD, 'incremental-1'), false);
  assert.equal(collectingSlotValid(collectingWorkload('incremental_collect'), 'discover-2'), false);
  assert.equal(collectingSlotValid(collectingWorkload('incremental_collect'), 'incremental-2'), true);
});

test('Discover contract freezes the request and validates the returned page', () => {
  assert.equal(discoverBusinessRunId('p1'), 'discover-page:p1');
  assert.equal(discoverAttemptId('t1'), 'discover-attempt:t1');
  assert.deepEqual(discoverTaskInput({ pageId: 'p1', pageIntentHash: 'h', request: initial }),
    { page_id: 'p1', page_intent_hash: 'h', request: initial });
  const continuation = { ...initial, kind: 'continuation', continuation: 'tok', ytConfig: { apiKey: 'k', context: {} } };
  assert.equal(assertDiscoverRequest(continuation), continuation);
  for (const bad of [
    { ...initial, kind: 'channel' },
    { ...initial, continuation: 'tok' },
    { ...continuation, ytConfig: { context: {} } },
    { ...initial, extra: true },
    { ...initial, queryText: '' },
  ]) assert.throws(() => assertDiscoverRequest(bad), /INVALID_DISCOVER_REQUEST/);

  assert.deepEqual(discoverPageFromWire(page, initial), page);
  for (const bad of [
    { ...page, kind: 'continuation' },
    { ...page, url: 'https://evil.example/results' },
    { ...page, url: 'http://www.youtube.com/results' },
    { ...page, status: 99 },
    { ...page, rawText: 'x'.repeat(3 * 1024 * 1024 + 1) },
    { ...page, extra: 1 },
  ]) assert.throws(() => discoverPageFromWire(bad, initial), /INVALID_DISCOVER_RESULT/);
});

function fakeRemote({ outcome = { outcome: 'success', data: page } } = {}) {
  const calls = [];
  const lease = { task_id: randomUUID(), generation: 1 };
  const pages = {
    admit: async value => { calls.push(['admit', value]); return { taskId: lease.task_id, attemptId: value.attemptId }; },
    waitClaim: async () => { calls.push(['waitClaim']); return lease; },
    request: async () => { calls.push(['request']); return 'command-1'; },
    awaitResult: async () => { calls.push(['awaitResult']); return outcome; },
    complete: async (_lease, result) => { calls.push(['complete', result]); },
    stop: async (admission, error) => { calls.push(['stop', error?.code ?? null]); },
    bindings: async () => [],
  };
  const binding = { binding_id: randomUUID(), identity: { egress_country: '' } };
  const routes = {
    bind: async (nodeId, boundLease, slot, fence) => { calls.push(['bind', fence.business_run_id]); return binding; },
    bindingForExecution: async () => binding,
    requestStop: async () => { calls.push(['requestStop']); },
    waitQuiesced: async () => ({ active_managed_requests: 0 }),
  };
  const runtime = new RemoteManagedDiscoverRuntime({ pages, routes, nodeId: randomUUID(), slot: 'discover-1' });
  const policy = { id: 'qy-br-discover-anonymous-v1', version: 1, hash: 'sha256:x', role: 'discover' };
  const assignment = { role: 'discover', identity_policy_id: policy.id, identity_policy_version: 1, identity_policy_hash: policy.hash,
    worker_id: 'rota-worker', slot_name: 'discover-slot', lease_id: 'lease', route_generation: 3 };
  const rotaTask = { task_id: randomUUID(), job_execution_id: 'exec-1' };
  const prepared = { businessRunId: 'discover-page:page-1', page: { page_id: 'page-1', page_intent_hash: 'hash-1' } };
  return { calls, runtime, policy, assignment, rotaTask, prepared };
}

test('remote runtime sends one request through admission, claim and route binding', async () => {
  const f = fakeRemote();
  const handle = await f.runtime.acquire({ assignment: f.assignment, policy: f.policy, task: f.rotaTask, prepared: f.prepared });
  await assert.rejects(f.runtime.requestPage(initial), /REMOTE_DISCOVER_EXECUTION_REQUIRED/);
  const result = await handle.execute({}, () => f.runtime.requestPage(initial));
  assert.deepEqual(result, page);
  assert.deepEqual(f.calls.map(([name]) => name), ['admit', 'waitClaim', 'bind', 'request', 'awaitResult', 'complete']);
  const admitted = f.calls[0][1];
  assert.equal(admitted.attemptId, `discover-attempt:${f.rotaTask.task_id}`);
  assert.equal(admitted.rotaWorkerId, 'rota-worker');
  assert.equal(admitted.pageId, 'page-1');
  assert.equal(f.calls[2][1], 'discover-page:page-1');
  assert.deepEqual(f.calls[5][1], { command_id: 'command-1', status: 200 });

  // Quiesce is idempotent and waits for the node's route release.
  const quiet = await f.runtime.quiesce(handle);
  assert.deepEqual(quiet, { active_managed_requests: 0 });
  assert.equal(await f.runtime.checkpoint(handle), quiet);
  assert.equal(f.calls.filter(([name]) => name === 'stop').length, 1);
  assert.equal(f.calls.filter(([name]) => name === 'requestStop').length, 1);
});

test('a node failure is rebuilt with the evidence Rota classifies like a local failure', async () => {
  // A proxy timeout from the managed fetch is a retryable route failure.
  const local = annotateYoutubeFailure(Object.assign(new Error('timeout 20000ms'), { code: 'UND_ERR_CONNECT_TIMEOUT' }), {
    source: 'youtube_fetch_transport', targetUrl: page.url, client: 'WEB',
  });
  local.cause = Object.assign(new Error('Connect Timeout Error'), { code: 'UND_ERR_CONNECT_TIMEOUT' });
  assert.equal(retryableRotaFailure(local)?.observation, 'proxy_transport');
  const wire = { ...toChannelWire(local), code: local.code };
  const f = fakeRemote({ outcome: { outcome: 'failure', error: JSON.parse(JSON.stringify(wire)) } });
  const handle = await f.runtime.acquire({ assignment: f.assignment, policy: f.policy, task: f.rotaTask, prepared: f.prepared });
  const error = await handle.execute({}, () => f.runtime.requestPage(initial)).then(() => null, value => value);
  assert.ok(error instanceof Error);
  assert.equal(error.message, 'timeout 20000ms');
  assert.deepEqual(retryableRotaFailure(error), retryableRotaFailure(local));
  assert.equal(classifyYoutubeError(error), classifyYoutubeError(local));
  assert.equal(f.calls.some(([name]) => name === 'complete'), false);
  await f.runtime.retire(handle);
  assert.deepEqual(f.calls.find(([name]) => name === 'stop'), ['stop', 'UND_ERR_CONNECT_TIMEOUT']);

  const localChallenge = annotateYoutubeFailure(Object.assign(new Error("Sign in to confirm you're not a bot"), { status: 200 }),
    { status: 200, body: "Sign in to confirm you're not a bot", source: 'youtube_text' });
  const challenge = fromChannelWire(JSON.parse(JSON.stringify({ ...toChannelWire(localChallenge), code: 'Error' })));
  assert.equal(retryableRotaFailure(localChallenge)?.observation, 'youtube_challenge');
  assert.deepEqual(retryableRotaFailure(challenge), retryableRotaFailure(localChallenge));
  assert.equal(classifyYoutubeError(challenge), 'ip_blocked_or_rate_limited');
});

test('remote runtime rejects a non-Discover Rota assignment', async () => {
  const f = fakeRemote();
  await assert.rejects(f.runtime.acquire({ assignment: { ...f.assignment, role: 'channel' }, policy: f.policy,
    task: f.rotaTask, prepared: f.prepared }), /conflicts with its Identity Policy/);
});

test('node route runtime never sends without a granted route', async () => {
  const closed = [];
  const dispatcher = { close: async () => { closed.push(true); } };
  const withRuntime = createDiscoverRouteRuntime({ createDispatcher: url => { assert.equal(url, 'http://proxy.invalid:1'); return dispatcher; } });
  await assert.rejects(withRuntime({ slot: 'discover-1' }, async () => 1), /DISCOVER_ROUTE_REQUIRED/);
  const seen = await withRuntime({ proxyUrl: 'http://proxy.invalid:1', slot: 'discover-1' }, () => currentProxyIdentity().dispatcher);
  assert.equal(seen, dispatcher);
  assert.equal(closed.length, 1);

  const command = { command_id: randomUUID(), operation: DISCOVER_PAGE_OPERATION, input: initial };
  await assert.rejects(executeDiscoverCommand(command, { request: async () => page }), /DISCOVER_ROUTE_REQUIRED/);
  await assert.rejects(executeDiscoverCommand({ ...command, operation: 'open_channel' }), /UNKNOWN_DISCOVER_OPERATION/);
  const value = await withRuntime({ proxyUrl: 'http://proxy.invalid:1', slot: 'discover-1' },
    () => executeDiscoverCommand(command, { request: async input => ({ ...page, echoed: input.queryText }) }));
  assert.equal(value.echoed, 'receitas');
});

async function executorHarness(t, { execute, polls, upload }) {
  const directory = await mkdtemp(join(tmpdir(), 'discover-spool-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const spool = new RemoteResultSpool({ directory });
  const lease = { task_id: randomUUID(), generation: 2, capability: 'youtube.discover-page.v1', worker_slot: 'discover-1', heartbeat_ms: 60000 };
  const uploads = [];
  const pollQueue = [...polls];
  const client = {
    transport: 'nats',
    claim: async () => lease,
    heartbeat: async () => ({}),
    discoverPoll: async () => pollQueue.shift() ?? { status: 'applied', commands: [] },
    uploadDiscover: async (value, bytes) => {
      const { value: decoded } = await decodeResult(bytes);
      uploads.push({ lease: value, decoded });
      if (upload) return upload(decoded);
      return { durable: true, batch_id: decoded.batch_id, command_id: decoded.command_id, status: 'received' };
    },
  };
  const withRuntime = createDiscoverRouteRuntime({ createDispatcher: () => ({ close: async () => {} }) });
  const networkSession = { slot: 'discover-1', recover: async () => {},
    run: (runLease, { signal }, invoke) => withRuntime({ proxyUrl: 'http://proxy.invalid:1', slot: 'discover-1', signal },
      () => invoke({ signal })) };
  const executor = new RemoteDiscoverExecutor({ client, spool, networkSession, execute });
  return { executor, uploads, spool, lease };
}

test('node executor runs the single command, spools the result and ends when the center closes the task', async t => {
  const command = { command_id: randomUUID(), operation: DISCOVER_PAGE_OPERATION, input: initial };
  const h = await executorHarness(t, {
    execute: value => executeDiscoverCommand(value, { request: async () => page }),
    polls: [{ status: 'leased', commands: [command] }, { status: 'applied', commands: [] }],
  });
  assert.equal(await h.executor.runOnce(), 'applied');
  assert.equal(h.uploads.length, 1);
  assert.equal(h.uploads[0].lease.task_id, h.lease.task_id);
  assert.equal(h.uploads[0].decoded.outcome, 'success');
  assert.equal(h.uploads[0].decoded.command_id, command.command_id);
  assert.equal(h.uploads[0].decoded.generation, 2);
  assert.deepEqual(h.uploads[0].decoded.data, page);
  assert.equal(await h.spool.read('pending.json'), null);
  assert.equal(await h.spool.read('claim.json'), null);
});

test('node executor returns a request failure with its evidence', async t => {
  const command = { command_id: randomUUID(), operation: DISCOVER_PAGE_OPERATION, input: initial };
  const h = await executorHarness(t, {
    execute: async () => {
      throw annotateYoutubeFailure(Object.assign(new Error('fetch failed'), { code: 'UND_ERR_SOCKET' }), {
        source: 'youtube_fetch_transport', targetUrl: page.url, client: 'WEB',
      });
    },
    polls: [{ status: 'leased', commands: [command] }, { status: 'failed', commands: [] }],
  });
  assert.equal(await h.executor.runOnce(), 'failed');
  const failure = h.uploads[0].decoded;
  assert.equal(failure.outcome, 'failure');
  assert.equal(failure.error.code, 'UND_ERR_SOCKET');
  assert.equal(failure.error.youtube_failure_evidence.source, 'youtube_fetch_transport');
});

test('a stale upload is archived instead of blocking the slot', async t => {
  const command = { command_id: randomUUID(), operation: DISCOVER_PAGE_OPERATION, input: initial };
  const h = await executorHarness(t, {
    execute: async () => page,
    polls: [{ status: 'leased', commands: [command] }, { status: 'failed', commands: [] }],
    upload: async () => { throw Object.assign(new Error('STALE_LEASE'), { code: 'STALE_LEASE', status: 409 }); },
  });
  assert.equal(await h.executor.runOnce(), 'failed');
  assert.equal(await h.spool.read('pending.json'), null);
  assert.equal(await h.spool.writable(), true);
});

test('node executor reports idle when the center offers no page', async t => {
  const h = await executorHarness(t, { execute: async () => page, polls: [] });
  h.executor.client.claim = async () => null;
  assert.equal(await h.executor.runOnce(), 'idle');
  assert.equal(await h.spool.read('claim.json'), null);
});

test('center assembles one Discover supervisor with explicit Discover factories', async () => {
  const { RemoteCenterExecutionSupervisor } = await import('../src/remoteNodes/centerExecutionSupervisor.js');
  const { createDiscoverCenterRuntime } = await import('../src/remoteNodes/discoverCenterRuntime.js');
  const { generateKeyPairSync, randomBytes } = await import('node:crypto');
  const base = { store: { pool: { query: async () => ({ rows: [] }) } }, guardPool: { connect: async () => ({}) },
    connection: {}, prefix: 'bull', dashboardManaged: true };
  assert.throws(() => new RemoteCenterExecutionSupervisor({ ...base, mode: 'discover_collect' }), /explicit full-crawl runtime|explicit/);

  const closed = [];
  class FakeQueue { constructor(name) { this.name = name; } async close() { closed.push(this.name); } }
  const runtime = createDiscoverCenterRuntime({ ...base, activation: {}, rotaClient: {}, resolvedPolicy: { policy: {} },
    privateKey: generateKeyPairSync('ed25519').privateKey.export({ type: 'pkcs8', format: 'pem' }), secretKey: randomBytes(32),
    readRotaRoute: async () => ({}), putRawObject: async () => {}, language: 'pt-BR', country: 'BR', QueueClass: FakeQueue });
  assert.equal(runtime.supervisor.workload, DISCOVER_WORKLOAD);
  assert.equal(runtime.supervisor.workload.rotaRole, 'discover');
  assert.equal(runtime.supervisor.settleHandoffs, false);
  assert.equal(typeof runtime.execution.verifyExecution, 'function');
  assert.equal(typeof runtime.execution.assertTask, 'function');
  await runtime.close();
  assert.deepEqual(closed.sort(), ['youtube-channel-crawl', 'youtube-discover-page']);
});

test('node configuration accepts only a Discover role in a Discover slot', async () => {
  const { parseWorkerConfig } = await import('../src/remoteNodes/workerConfig.js');
  const config = { version: 1, mode: 'discover_collect', role: 'discover', node_id: randomUUID(), slot: 'discover-3',
    deployment_id: randomUUID(), gateway_url: 'https://center.example/node-execution/v1' };
  const bytes = value => Buffer.from(JSON.stringify(value));
  assert.equal(parseWorkerConfig(bytes(config), { mode: 'discover_collect' }).slot, 'discover-3');
  for (const bad of [{ ...config, role: 'incremental' }, { ...config, slot: 'incremental-3' }, { ...config, mode: 'incremental_collect' }]) {
    assert.throws(() => parseWorkerConfig(bytes(bad), { mode: 'discover_collect' }), /NODE_CONFIG_INVALID/);
  }
  assert.throws(() => parseWorkerConfig(bytes(config), { mode: 'incremental_collect' }), /NODE_CONFIG_INVALID/);
});

test('every schema file that redefines a shared constraint uses the same definition', async () => {
  const { readdir, readFile } = await import('node:fs/promises');
  const directory = new URL('../src/remoteNodes/', import.meta.url);
  const definitions = new Map();
  for (const file of (await readdir(directory)).filter(name => name.endsWith('.sql'))) {
    const sql = await readFile(new URL(file, directory), 'utf8');
    for (const match of sql.matchAll(/ADD CONSTRAINT (\w+)\s+(CHECK\s*\([\s\S]*?\));/g)) {
      const list = definitions.get(match[1]) ?? [];
      list.push({ file, definition: match[2].replace(/\s+/g, ' ').replace(/^CHECK\s*\(/, 'CHECK (').replace(/\(\s/g, '(').replace(/\s\)/g, ')') });
      definitions.set(match[1], list);
    }
  }
  const shared = [...definitions].filter(([, list]) => new Set(list.map(item => item.file)).size > 1);
  assert.ok(shared.length >= 4, 'workload and command constraints are shared');
  for (const [name, list] of shared) {
    assert.equal(new Set(list.map(item => item.definition)).size, 1,
      `${name} differs across ${list.map(item => item.file).join(', ')}`);
  }
});
