import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes, randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import pg from 'pg';

import { PUBLICATION_WRITER_VERSION } from '../src/publicationWriterVersion.js';
import { supervisionLockKey } from '../src/remoteNodes/centerExecutionRecovery.js';
import { RemoteChannelRouteStore } from '../src/remoteNodes/channelRouteStore.js';
import { assertDiscoverSchema } from '../src/remoteNodes/discoverCenterRuntime.js';
import { DISCOVER_PAGE_CAPABILITY, discoverBusinessRunId, discoverTaskFrom } from '../src/remoteNodes/discoverPageContract.js';
import { RemoteDiscoverPageStore } from '../src/remoteNodes/discoverPageStore.js';
import { recoverRemoteDiscoverSlot, remoteDiscoverSlotUnsettled } from '../src/remoteNodes/discoverRecovery.js';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';
import { encodeResult } from '../src/remoteNodes/protocol.js';
import { RemoteNodeStore } from '../src/remoteNodes/store.js';
import { RemoteWorkerActivationStore } from '../src/remoteNodes/workerActivationStore.js';

const url = process.env.REMOTE_NODE_TEST_DATABASE_URL;
const request = { kind: 'initial', queryText: 'receitas', language: 'pt', country: 'BR', ytConfig: null, continuation: null };
const page = { kind: 'initial', url: 'https://www.youtube.com/results?search_query=receitas', status: 200,
  rawText: '<html>fixture</html>', rawContentType: 'text/html; charset=utf-8' };

test('remote Discover transport keeps one fenced delivery per page attempt', { skip: !url, timeout: 180000 }, async t => {
  const pool = new pg.Pool({ connectionString: url, max: 8, options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}` });
  const guard = await pool.connect();
  t.after(async () => { guard.release(); await pool.end(); });
  await assertIsolatedRemoteDatabase(pool);
  await guard.query('SELECT pg_advisory_lock(781137981)');
  await pool.query(await readFile(new URL('../src/schema.sql', import.meta.url), 'utf8'));
  for (const file of ['schema.sql', 'routeSchema.sql', 'workerConnectionSchema.sql', 'workerActivationSchema.sql',
    'wholeChannelSchema.sql', 'fullCrawlSchema.sql', 'discoverSchema.sql']) {
    await pool.query(await readFile(new URL(`../src/remoteNodes/${file}`, import.meta.url), 'utf8'));
  }
  await assertDiscoverSchema(pool.query.bind(pool));
  await pool.query('TRUNCATE remote_ingestion.nodes CASCADE');

  const store = new RemoteNodeStore({ pool });
  const pages = new RemoteDiscoverPageStore({ store });
  const privateKey = generateKeyPairSync('ed25519').privateKey.export({ type: 'pkcs8', format: 'pem' });
  const identity = { workload_scope: 'qy-test', credential_generation: 1, network_identity_key: 'net-1', profile_epoch: 0,
    identity_policy_id: 'qy-br-discover-anonymous-v1', identity_policy_version: 1, identity_policy_hash: 'sha256:x', egress_country: 'BR' };
  const routes = new RemoteChannelRouteStore({ channelStore: pages,
    assertBusinessFence: (client, task) => pages.assertBusinessFence(client, task),
    businessRunId: task => discoverBusinessRunId(discoverTaskFrom(task).page_id),
    readRotaRoute: async fence => ({ ...fence, ...identity, upstream: { host: 'proxy.invalid', port: 1 } }),
    privateKey, secretKey: randomBytes(32) });
  const activation = new RemoteWorkerActivationStore({ store });

  const nodeId = randomUUID();
  await store.registerNode({ nodeId, token: randomBytes(32).toString('hex'), capabilities: [DISCOVER_PAGE_CAPABILITY] });
  await routes.registerSlot(nodeId, 'discover-1', 'rota-worker-1');
  const deploymentId = randomUUID();
  const configHash = 'a'.repeat(64);
  const registered = await activation.register({ nodeId, slot: 'discover-1', deploymentId, configHash,
    role: 'discover', mode: 'discover_collect' });
  assert.equal(registered.role, 'discover');
  const connection = (await pool.query('SELECT activation_requested FROM remote_ingestion.worker_connections WHERE node_id=$1', [nodeId])).rows[0];
  assert.equal(connection.activation_requested, false, 'Discover intake starts closed');

  // Database constraints keep workload namespaces separate.
  await routes.registerSlot(nodeId, 'discover-9', 'rota-worker-9');
  await assert.rejects(pool.query(`INSERT INTO remote_ingestion.worker_connections(node_id,slot,deployment_id,config_hash,role,mode)
    VALUES($1,'discover-9',$2,$3,'incremental','incremental_collect')`, [nodeId, deploymentId, configHash]), /worker_connections_workload_check/);
  await assert.rejects(activation.register({ nodeId, slot: 'discover-9', deploymentId, configHash }), /invalid collecting deployment/);

  // An incremental-capability node cannot join the Discover workload.
  const incrementalNode = randomUUID();
  await store.registerNode({ nodeId: incrementalNode, token: randomBytes(32).toString('hex'), capabilities: ['youtube.incremental.plan.v1'] });
  await routes.registerSlot(incrementalNode, 'discover-1', 'rota-worker-x');
  await assert.rejects(activation.register({ nodeId: incrementalNode, slot: 'discover-1', deploymentId, configHash,
    role: 'discover', mode: 'discover_collect' }), /WORKER_NODE_CAPABILITY_MISMATCH/);

  async function pageIntent() {
    const pageId = `page-${randomUUID()}`;
    await pool.query(`INSERT INTO crawler.query_pages(page_id,query_text,page_no,page_intent_hash,managed_fetch_status,status)
      VALUES($1,'receitas',1,$2,'running','running')`, [pageId, `hash-${pageId}`]);
    return { pageId, pageIntentHash: `hash-${pageId}` };
  }
  const discoverPermission = { capability: DISCOVER_PAGE_CAPABILITY,
    assertTask: (client, task) => pages.assertBusinessFence(client, task) };
  const claim = (slot = 'discover-1', permission = discoverPermission) => store.claim(nodeId, randomUUID(), slot,
    { authorize: async () => ({ allowNew: true, ...permission }) });
  const fenceFor = (lease, pageId, rotaTaskId) => ({ slot_name: 'discover-slot-1', worker_id: 'rota-worker-1', worker_instance_id: 'center-1',
    lease_id: 'lease-1', route_generation: 1, task_id: rotaTaskId, business_run_id: discoverBusinessRunId(pageId), job_execution_id: 'exec-1' });

  // Admission is idempotent per attempt and exclusive per page.
  const first = await pageIntent();
  const admission = await pages.admit({ nodeId, slot: 'discover-1', attemptId: 'discover-attempt:a1', rotaWorkerId: 'rota-worker-1',
    ...first, request });
  const again = await pages.admit({ nodeId, slot: 'discover-1', attemptId: 'discover-attempt:a1', rotaWorkerId: 'rota-worker-1',
    ...first, request });
  assert.equal(again.taskId, admission.taskId);
  await assert.rejects(pages.admit({ nodeId, slot: 'discover-1', attemptId: 'discover-attempt:a2', rotaWorkerId: 'rota-worker-1',
    ...first, request }), /REMOTE_EXECUTION_NOT_SETTLED/);
  await assert.rejects(pages.admit({ nodeId, slot: 'discover-1', attemptId: 'discover-attempt:a3', rotaWorkerId: 'wrong-worker',
    ...(await pageIntent()), request }), /NETWORK_SLOT_CONFLICT/);
  const task = (await pool.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1', [admission.taskId])).rows[0];
  assert.equal(task.capability, DISCOVER_PAGE_CAPABILITY);
  assert.equal(task.target_worker_slot, 'discover-1');
  assert.deepEqual(task.input.request, request);

  // Only a Discover connection with the Discover fence can claim the task.
  assert.equal(await claim('discover-1', {}), null);
  const lease = await claim();
  assert.equal(lease.task_id, admission.taskId);
  assert.equal(lease.capability, DISCOVER_PAGE_CAPABILITY);
  const claimed = await pages.waitClaim(admission, { nodeId, slot: 'discover-1', signal: AbortSignal.timeout(5000) });
  assert.equal(claimed.generation, lease.generation);
  assert.deepEqual(await pages.poll(nodeId, lease), { status: 'leased', commands: [] });

  // The Page Intent is the business fence for both commands and routes.
  // A page that failed and awaits its retry is not the page being fetched now.
  await pool.query("UPDATE crawler.query_pages SET managed_fetch_status='failed' WHERE page_id=$1", [first.pageId]);
  await assert.rejects(pages.request(lease), /DISCOVER_BUSINESS_FENCE_STALE/);
  await pool.query("UPDATE crawler.query_pages SET managed_fetch_status='running' WHERE page_id=$1", [first.pageId]);
  const commandId = await pages.request(lease);
  assert.equal(await pages.request(lease), commandId);
  const polled = await pages.poll(nodeId, lease);
  assert.equal(polled.commands.length, 1);
  assert.equal(polled.commands[0].command_id, commandId);
  assert.equal(polled.commands[0].operation, 'collect_search_page');
  assert.deepEqual(polled.commands[0].input, request);

  const rotaTaskId = randomUUID();
  await assert.rejects(routes.bind(nodeId, lease, 'discover-1', { ...fenceFor(lease, first.pageId, rotaTaskId),
    business_run_id: 'discover-page:other' }), /ROTA_TASK_BINDING_MISMATCH/);
  const binding = await routes.bind(nodeId, lease, 'discover-1', fenceFor(lease, first.pageId, rotaTaskId));
  assert.equal(binding.task_id, lease.task_id);

  // One durable result per command; replay is idempotent, a different body conflicts.
  const envelope = { version: 1, generation: lease.generation, batch_id: randomUUID(), command_id: commandId };
  const bytes = await encodeResult({ ...envelope, outcome: 'success', data: page });
  const receipt = await pages.receive(nodeId, { task_id: lease.task_id }, bytes);
  assert.deepEqual(receipt, { durable: true, batch_id: envelope.batch_id, command_id: commandId, status: 'received' });
  assert.deepEqual(await pages.receive(nodeId, { task_id: lease.task_id }, bytes), receipt);
  await assert.rejects(pages.receive(nodeId, { task_id: lease.task_id },
    await encodeResult({ ...envelope, outcome: 'success', data: { ...page, status: 201 } })), /BATCH_CONFLICT/);
  const value = await pages.awaitResult(lease, commandId, { signal: AbortSignal.timeout(5000) });
  assert.equal(value.outcome, 'success');
  assert.deepEqual(value.data, page);
  await pages.complete(lease, { command_id: commandId, status: 200 });
  assert.deepEqual(await pages.poll(nodeId, lease), { status: 'applied', commands: [] });
  await pages.stop(admission, new Error('late stop'));
  assert.equal((await pool.query('SELECT state FROM remote_ingestion.tasks WHERE task_id=$1', [admission.taskId])).rows[0].state,
    'applied', 'a completed delivery is never failed afterwards');

  // A later attempt may reuse the page task only once the old network is retired.
  await assert.rejects(pages.admit({ nodeId, slot: 'discover-1', attemptId: 'discover-attempt:a4', rotaWorkerId: 'rota-worker-1',
    ...first, request }), /REMOTE_PREVIOUS_NETWORK_NOT_RETIRED/);

  // Recovery after a lost center: an unissued binding is retired immediately,
  // an issued one only requests the node to stop.
  const row = { node_id: nodeId, slot: 'discover-1', mode: 'discover_collect', rota_worker_id: 'rota-worker-1' };
  const recoveryGuard = await pool.connect();
  try {
  await recoveryGuard.query('SELECT pg_advisory_lock(781138012,hashtext($1))', [supervisionLockKey(row)]);
  assert.equal(supervisionLockKey(row), `remote-discover-supervisor:${nodeId}/discover-1`);
  assert.equal(await remoteDiscoverSlotUnsettled(pool, row), true);
  const firstRecovery = await recoverRemoteDiscoverSlot({ guard: recoveryGuard, row, lockKey: supervisionLockKey(row) });
  assert.deepEqual(firstRecovery, { settled: true, closed: 1 });
  const retired = (await pool.query('SELECT state,release_receipt FROM remote_ingestion.network_bindings WHERE binding_id=$1',
    [binding.binding_id])).rows[0];
  assert.equal(retired.state, 'retired');
  assert.deepEqual(retired.release_receipt, { stopped_before_grant: true, in_flight: 0 });

  const second = await pageIntent();
  const secondAdmission = await pages.admit({ nodeId, slot: 'discover-1', attemptId: 'discover-attempt:b1', rotaWorkerId: 'rota-worker-1',
    ...second, request });
  const secondLease = await claim();
  assert.equal(secondLease.task_id, secondAdmission.taskId);
  const secondBinding = await routes.bind(nodeId, secondLease, 'discover-1', fenceFor(secondLease, second.pageId, randomUUID()));
  await pool.query("UPDATE remote_ingestion.network_slots SET grant_until=clock_timestamp()+interval '1 minute' WHERE node_id=$1 AND slot='discover-1'", [nodeId]);
  const secondRecovery = await recoverRemoteDiscoverSlot({ guard: recoveryGuard, row, lockKey: supervisionLockKey(row) });
  assert.deepEqual(secondRecovery, { settled: false, closed: 1 });
  const stopping = (await pool.query('SELECT state,stop_requested FROM remote_ingestion.network_bindings WHERE binding_id=$1',
    [secondBinding.binding_id])).rows[0];
  assert.equal(stopping.state, 'bound');
  assert.equal(stopping.stop_requested, true);
  const interrupted = (await pool.query('SELECT state,last_error FROM remote_ingestion.tasks WHERE task_id=$1', [secondAdmission.taskId])).rows[0];
  assert.deepEqual(interrupted, { state: 'failed', last_error: 'REMOTE_CENTER_INTERRUPTED' });
  await assert.rejects(recoverRemoteDiscoverSlot({ guard: recoveryGuard, row, lockKey: 'remote-discover-supervisor:other' }),
    /REMOTE_RECOVERY_NOT_OWNER/);
  } finally {
    // Session advisory locks end with the session; pool.end waits for this client.
    recoveryGuard.release(true);
  }
});
