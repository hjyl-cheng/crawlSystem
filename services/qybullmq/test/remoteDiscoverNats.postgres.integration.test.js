import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes, randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import pg from 'pg';
import { MockAgent } from 'undici';

import { retryableRotaFailure } from '../src/managedWorkerExecution.js';
import { runWithProxyIdentity } from '../src/proxyIdentity.js';
import { PUBLICATION_WRITER_VERSION } from '../src/publicationWriterVersion.js';
import { classifyYoutubeError, parseDiscoverSearchPage, requestDiscoverSearchPage } from '../src/youtube.js';
import { decideYoutubeFailure } from '../src/youtubeFailurePolicy.js';
import { RemoteChannelPlanStore } from '../src/remoteNodes/channelPlanStore.js';
import { RemoteChannelRouteStore } from '../src/remoteNodes/channelRouteStore.js';
import { DISCOVER_PAGE_CAPABILITY, discoverBusinessRunId, discoverTaskFrom } from '../src/remoteNodes/discoverPageContract.js';
import { RemoteDiscoverPageStore } from '../src/remoteNodes/discoverPageStore.js';
import { createDiscoverRouteRuntime, RemoteDiscoverExecutor } from '../src/remoteNodes/discoverNode.js';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';
import { RemoteManagedDiscoverRuntime } from '../src/remoteNodes/managedDiscoverRuntime.js';
import { createRemoteNatsClient } from '../src/remoteNodes/natsClient.js';
import { startRemoteNatsCenter } from '../src/remoteNodes/natsCenter.js';
import { RemoteResultSpool } from '../src/remoteNodes/spool.js';
import { RemoteNodeStore } from '../src/remoteNodes/store.js';
import { createTransportSignals } from '../src/remoteNodes/transportSignals.js';

const url = process.env.REMOTE_NATS_TEST_URL;
const request = { kind: 'initial', queryText: 'receitas', language: 'pt', country: 'BR', ytConfig: null, continuation: null };
const searchData = { contents: { items: [{ videoRenderer: { videoId: 'v1', title: { runs: [{ text: 'video' }] } } }] } };
const searchHtml = `<html><body><script>var ytInitialData = ${JSON.stringify(searchData)};</script>`
  + '<script>ytcfg.set({"INNERTUBE_API_KEY":"key-1","INNERTUBE_CONTEXT":{"client":{"clientName":"WEB"}}});</script></body></html>';

// The node's route dispatcher is replaced by a mock that answers as YouTube.
function youtubeAgent(body) {
  const agent = new MockAgent();
  agent.disableNetConnect();
  agent.get('https://www.youtube.com').intercept({ path: path => path.startsWith('/results?'), method: 'GET' })
    .reply(200, body, { headers: { 'content-type': 'text/html; charset=utf-8' } });
  return agent;
}

test('remote Discover pages travel over NATS with the local request and failure evidence', { skip: !url, timeout: 120000 }, async t => {
  const pool = new pg.Pool({ connectionString: process.env.REMOTE_NODE_TEST_DATABASE_URL, max: 8,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}` });
  await assertIsolatedRemoteDatabase(pool);
  const guard = await pool.connect();
  await guard.query('SELECT pg_advisory_lock(781137981)');
  let center; let client; let signals;
  const directory = await mkdtemp(join(tmpdir(), 'discover-nats-spool-'));
  t.after(async () => {
    await client?.close(); await center?.close(); await signals?.close();
    guard.release(); await pool.end(); await rm(directory, { recursive: true, force: true });
  });
  await pool.query(await readFile(new URL('../src/schema.sql', import.meta.url), 'utf8'));
  for (const file of ['schema.sql', 'routeSchema.sql', 'natsSchema.sql', 'workerConnectionSchema.sql', 'workerActivationSchema.sql',
    'wholeChannelSchema.sql', 'fullCrawlSchema.sql', 'discoverSchema.sql']) {
    await pool.query(await readFile(new URL(`../src/remoteNodes/${file}`, import.meta.url), 'utf8'));
  }
  await pool.query('TRUNCATE remote_ingestion.nodes CASCADE');

  const store = new RemoteNodeStore({ pool });
  const pages = new RemoteDiscoverPageStore({ store });
  const routes = new RemoteChannelRouteStore({ channelStore: pages,
    assertBusinessFence: (dbClient, task) => pages.assertBusinessFence(dbClient, task),
    businessRunId: task => discoverBusinessRunId(discoverTaskFrom(task).page_id),
    readRotaRoute: async fence => ({ ...fence, workload_scope: 'qy-test', credential_generation: 1, network_identity_key: 'net-1',
      profile_epoch: 0, identity_policy_id: 'qy-br-discover-anonymous-v1', identity_policy_version: 1,
      identity_policy_hash: 'sha256:x', egress_country: 'BR', upstream: { host: 'proxy.invalid', port: 1 } }),
    privateKey: generateKeyPairSync('ed25519').privateKey.export({ type: 'pkcs8', format: 'pem' }), secretKey: randomBytes(32) });
  const nodeId = process.env.REMOTE_NATS_TEST_NODE_ID; const token = process.env.REMOTE_NATS_TEST_TOKEN;
  await store.registerNode({ nodeId, token, capabilities: [DISCOVER_PAGE_CAPABILITY] });
  await routes.registerSlot(nodeId, 'discover-1', 'rota-worker-1');
  // Node route RPCs for a Discover task must reach the Discover route store.
  const routeCalls = { calls: [], grant: async (id, value) => { routeCalls.calls.push(['grant', value.task_id]); return {}; },
    release: async (id, value) => { routeCalls.calls.push(['release', value.task_id]); return {}; },
    abandon: async (id, value) => { routeCalls.calls.push(['abandon', value.task_id]); return {}; } };
  signals = await createTransportSignals({ connectionString: process.env.REMOTE_NODE_TEST_DATABASE_URL });
  pages.transportSignals = signals;
  const tls = { caFile: process.env.REMOTE_NATS_TEST_CA };
  center = await startRemoteNatsCenter({ url, password: process.env.REMOTE_NATS_TEST_PASSWORD, tls, store, signals,
    channelPlans: new RemoteChannelPlanStore({ store }), discoverPages: pages, discoverRoutes: routeCalls,
    workerConnections: { heartbeat: async (_id, value) => value,
      claim: (id, p) => store.claim(id, p.claim_id, p.slot, { authorize: async () => ({ allowNew: true, capability: DISCOVER_PAGE_CAPABILITY,
        assertTask: (dbClient, task) => pages.assertBusinessFence(dbClient, task) }) }) },
    routes: {}, youtubeSessions: {}, resultMaxBytes: 32 * 1024 * 1024 });
  client = await createRemoteNatsClient({ url, token, nodeId, slot: 'discover-1', tls });

  const runtime = new RemoteManagedDiscoverRuntime({ pages, routes, nodeId, slot: 'discover-1', pollMs: 50 });
  const policy = { id: 'qy-br-discover-anonymous-v1', version: 1, hash: 'sha256:x', role: 'discover' };
  const assignment = { role: 'discover', identity_policy_id: policy.id, identity_policy_version: 1, identity_policy_hash: policy.hash,
    worker_id: 'rota-worker-1', worker_instance_id: 'center-1', slot_name: 'discover-slot', lease_id: 'lease-1', route_generation: 1,
    workload_scope: 'qy-test', network_identity_key: 'net-1', profile_epoch: 0, credential_generation: 1, egress_country: 'BR' };

  async function runPage(body) {
    const pageId = `page-${randomUUID()}`;
    await pool.query(`INSERT INTO crawler.query_pages(page_id,query_text,page_no,page_intent_hash,managed_fetch_status,status)
      VALUES($1,'receitas',1,$2,'running','running')`, [pageId, `hash-${pageId}`]);
    const prepared = { businessRunId: discoverBusinessRunId(pageId), page: { page_id: pageId, page_intent_hash: `hash-${pageId}` } };
    const handle = await runtime.acquire({ assignment, policy, task: { task_id: randomUUID(), job_execution_id: 'exec-1' }, prepared });
    const withRuntime = createDiscoverRouteRuntime({ createDispatcher: () => youtubeAgent(body) });
    const executor = new RemoteDiscoverExecutor({ client, spool: new RemoteResultSpool({ directory: join(directory, pageId) }),
      networkSession: { slot: 'discover-1', recover: async () => {},
        run: (_lease, { signal }, invoke) => withRuntime({ proxyUrl: 'http://proxy.invalid:1', slot: 'discover-1', signal },
          () => invoke({ signal })) } });
    const centerSide = handle.execute({}, () => runtime.requestPage(request)).then(value => ({ value }), error => ({ error }));
    // The node finishes when the center closes the delivery; a failure is closed by retire.
    const nodeSide = executor.runOnce();
    const outcome = await centerSide;
    if (outcome.error) await runtime.retire(handle);
    const nodeStatus = await nodeSide;
    const quiet = await runtime.quiesce(handle);
    return { ...outcome, nodeStatus, quiet, handle };
  }

  const success = await runPage(searchHtml);
  assert.equal(success.error, undefined);
  assert.equal(success.nodeStatus, 'applied');
  assert.deepEqual(success.quiet, { active_managed_requests: 0 });
  assert.equal(success.value.kind, 'initial');
  assert.equal(success.value.status, 200);
  assert.equal(success.value.rawText, searchHtml);
  assert.match(success.value.url, /^https:\/\/www\.youtube\.com\/results\?search_query=receitas/);
  const parsed = parseDiscoverSearchPage(success.value);
  assert.deepEqual(parsed.initialData, searchData);
  assert.equal(parsed.ytConfig.apiKey, 'key-1');
  const receipts = await pool.query('SELECT response FROM remote_ingestion.transport_receipts WHERE task_id=$1',
    [success.handle.admission.taskId]);
  assert.equal(receipts.rowCount, 1);
  assert.equal(receipts.rows[0].response.ok, true);
  const bindings = await pool.query('SELECT state FROM remote_ingestion.network_bindings WHERE task_id=$1', [success.handle.admission.taskId]);
  assert.deepEqual(bindings.rows.map(row => row.state), ['retired']);
  const taskId = success.handle.admission.taskId;
  await client.grantRoute({ task_id: taskId });
  await client.releaseRoute({ task_id: taskId });
  await client.abandonRoute({ task_id: taskId });
  assert.deepEqual(routeCalls.calls, [['grant', taskId], ['release', taskId], ['abandon', taskId]]);
  await assert.rejects(client.youtubeSession({ task_id: taskId }), /DISCOVER_SERVICE_UNAVAILABLE/);

  // The same challenge fetched locally is the reference for every classification.
  const challengeHtml = "<html><body>Sign in to confirm you're not a bot</body></html>";
  const local = await runWithProxyIdentity({ proxy_url: 'http://proxy.invalid:1', dispatcher: youtubeAgent(challengeHtml) },
    () => requestDiscoverSearchPage(request)).then(() => null, error => error);
  assert.ok(local, 'a YouTube challenge fails the local request');
  const challenge = await runPage(challengeHtml);
  assert.ok(challenge.error, 'a YouTube challenge fails the remote request');
  assert.equal(challenge.nodeStatus, 'failed');
  assert.equal(challenge.error.message, local.message);
  assert.equal(classifyYoutubeError(challenge.error), classifyYoutubeError(local));
  assert.deepEqual(decideYoutubeFailure({ error: challenge.error }), decideYoutubeFailure({ error: local }));
  assert.equal(retryableRotaFailure(challenge.error)?.observation, 'youtube_challenge');
  assert.deepEqual(retryableRotaFailure(challenge.error), retryableRotaFailure(local));
  const failedTask = (await pool.query('SELECT state FROM remote_ingestion.tasks WHERE task_id=$1', [challenge.handle.admission.taskId])).rows[0];
  assert.equal(failedTask.state, 'failed');
});
