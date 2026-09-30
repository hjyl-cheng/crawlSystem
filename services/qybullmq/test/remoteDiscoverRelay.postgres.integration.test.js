import assert from 'node:assert/strict';
import { spawn, execFileSync } from 'node:child_process';
import { generateKeyPairSync, randomBytes, randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import https from 'node:https';
import net from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import test from 'node:test';
import pg from 'pg';
import { ProxyAgent } from 'undici';

import { retryableRotaFailure } from '../src/managedWorkerExecution.js';
import { runWithProxyIdentity } from '../src/proxyIdentity.js';
import { PUBLICATION_WRITER_VERSION } from '../src/publicationWriterVersion.js';
import { classifyYoutubeError, requestDiscoverSearchPage } from '../src/youtube.js';
import { decideYoutubeFailure } from '../src/youtubeFailurePolicy.js';
import { RemoteChannelNetworkSession } from '../src/remoteNodes/channelNetworkSession.js';
import { RemoteChannelPlanStore } from '../src/remoteNodes/channelPlanStore.js';
import { RemoteChannelRouteStore } from '../src/remoteNodes/channelRouteStore.js';
import { DISCOVER_PAGE_CAPABILITY, discoverBusinessRunId, discoverTaskFrom } from '../src/remoteNodes/discoverPageContract.js';
import { RemoteDiscoverPageStore } from '../src/remoteNodes/discoverPageStore.js';
import { createDiscoverRouteRuntime, RemoteDiscoverExecutor } from '../src/remoteNodes/discoverNode.js';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';
import { createLocalRotaClient } from '../src/remoteNodes/localRotaClient.js';
import { RemoteManagedDiscoverRuntime } from '../src/remoteNodes/managedDiscoverRuntime.js';
import { createRemoteNatsClient } from '../src/remoteNodes/natsClient.js';
import { startRemoteNatsCenter } from '../src/remoteNodes/natsCenter.js';
import { RemoteResultSpool } from '../src/remoteNodes/spool.js';
import { RemoteNodeStore } from '../src/remoteNodes/store.js';
import { createTransportSignals } from '../src/remoteNodes/transportSignals.js';

const url = process.env.REMOTE_NATS_TEST_URL;
const binary = process.env.REMOTE_NODE_ROTA_TEST_BINARY;
const request = { kind: 'initial', queryText: 'receitas', language: 'pt', country: 'BR', ytConfig: null, continuation: null };
const searchHtml = '<html><body><script>var ytInitialData = {"contents":{}};</script></body></html>';

test('remote Discover requests cross the real node relay and its authorized upstream only', { skip: !url || !binary, timeout: 150000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'discover-relay-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  // A self-signed www.youtube.com certificate trusted only by this test's dispatcher.
  execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', join(directory, 'yt-key.pem'),
    '-out', join(directory, 'yt-cert.pem'), '-days', '1', '-subj', '/CN=www.youtube.com',
    '-addext', 'subjectAltName=DNS:www.youtube.com'], { stdio: 'ignore' });
  const ca = await readFile(join(directory, 'yt-cert.pem'));
  const key = await readFile(join(directory, 'yt-key.pem'));

  // Fake YouTube behind a fake upstream proxy; the response is slow enough for route renewals.
  const upstreamConnects = [];
  let upstreamMode = 'serve';
  const youtube = https.createServer({ key, cert: ca }, (req, res) => {
    setTimeout(() => {
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      res.end(req.url.startsWith('/results?') ? searchHtml : 'not found');
    }, 700);
  });
  const sockets = new Set();
  const upstream = net.createServer(socket => {
    sockets.add(socket); socket.on('close', () => sockets.delete(socket)); socket.on('error', () => {});
    let head = '';
    const onData = data => {
      head += data.toString('latin1');
      if (!head.includes('\r\n\r\n')) return;
      socket.off('data', onData);
      const [line, ...headers] = head.split('\r\n');
      const auth = headers.find(value => value.toLowerCase().startsWith('proxy-authorization:'))?.split(': ')[1] ?? null;
      upstreamConnects.push({ line, auth });
      if (upstreamMode === 'refuse') { socket.end('HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n'); return; }
      socket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
      youtube.emit('connection', socket);
    };
    socket.on('data', onData);
  });
  upstream.listen(0, '127.0.0.1'); await once(upstream, 'listening');
  t.after(() => { for (const socket of sockets) socket.destroy(); return new Promise(resolve => upstream.close(resolve)); });

  const pool = new pg.Pool({ connectionString: process.env.REMOTE_NODE_TEST_DATABASE_URL, max: 8,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}` });
  await assertIsolatedRemoteDatabase(pool);
  const guard = await pool.connect();
  await guard.query('SELECT pg_advisory_lock(781137981)');
  let center; let natsClient; let signals; let relay;
  t.after(async () => {
    await natsClient?.close(); await center?.close(); await signals?.close();
    if (relay && relay.exitCode === null) { relay.kill('SIGTERM'); await once(relay, 'exit'); }
    guard.release(); await pool.end();
  });
  await pool.query(await readFile(new URL('../src/schema.sql', import.meta.url), 'utf8'));
  for (const file of ['schema.sql', 'routeSchema.sql', 'natsSchema.sql', 'youtubeSessionSchema.sql', 'workerConnectionSchema.sql',
    'workerActivationSchema.sql', 'wholeChannelSchema.sql', 'fullCrawlSchema.sql', 'discoverSchema.sql']) {
    await pool.query(await readFile(new URL(`../src/remoteNodes/${file}`, import.meta.url), 'utf8'));
  }
  await pool.query('TRUNCATE remote_ingestion.nodes CASCADE');

  const keypair = generateKeyPairSync('ed25519');
  const store = new RemoteNodeStore({ pool });
  const pages = new RemoteDiscoverPageStore({ store });
  const upstreamSource = { protocol: 'http', address: `127.0.0.1:${upstream.address().port}`, username: 'user', password: 'fixture-upstream-secret' };
  const routes = new RemoteChannelRouteStore({ channelStore: pages,
    assertBusinessFence: (client, task) => pages.assertBusinessFence(client, task),
    businessRunId: task => discoverBusinessRunId(discoverTaskFrom(task).page_id),
    readRotaRoute: async fence => ({ ...fence, workload_scope: 'qy-test', credential_generation: 1, network_identity_key: 'net-1',
      profile_epoch: 0, identity_policy_id: 'qy-br-discover-anonymous-v1', identity_policy_version: 1,
      identity_policy_hash: 'sha256:x', egress_country: 'BR', upstream: upstreamSource, route_lease_until_ms: Date.now() + 60000 }),
    privateKey: keypair.privateKey, secretKey: randomBytes(32) });
  const nodeId = process.env.REMOTE_NATS_TEST_NODE_ID; const token = process.env.REMOTE_NATS_TEST_TOKEN;
  await store.registerNode({ nodeId, token, capabilities: [DISCOVER_PAGE_CAPABILITY] });
  await routes.registerSlot(nodeId, 'discover-1', 'rota-worker-1');
  signals = await createTransportSignals({ connectionString: process.env.REMOTE_NODE_TEST_DATABASE_URL });
  pages.transportSignals = signals;
  const tls = { caFile: process.env.REMOTE_NATS_TEST_CA };
  center = await startRemoteNatsCenter({ url, password: process.env.REMOTE_NATS_TEST_PASSWORD, tls, store, signals,
    channelPlans: new RemoteChannelPlanStore({ store }), discoverPages: pages, discoverRoutes: routes,
    workerConnections: { heartbeat: async (_id, value) => value,
      claim: (id, p) => store.claim(id, p.claim_id, p.slot, { authorize: async () => ({ allowNew: true, capability: DISCOVER_PAGE_CAPABILITY,
        assertTask: (client, task) => pages.assertBusinessFence(client, task) }) }) },
    routes: {}, youtubeSessions: {}, resultMaxBytes: 32 * 1024 * 1024 });
  natsClient = await createRemoteNatsClient({ url, token, nodeId, slot: 'discover-1', tls });

  // The node's real relay, as runNodeProcess starts it.
  await writeFile(join(directory, 'public.pem'), keypair.publicKey.export({ type: 'spki', format: 'pem' }), { mode: 0o600 });
  await writeFile(join(directory, 'relay-token'), token, { mode: 0o600 });
  relay = spawn(binary, ['-node-id', nodeId, '-slots', 'discover-1', '-public-key-file', join(directory, 'public.pem'),
    '-control-token-file', join(directory, 'relay-token'), '-proxy-listen', '127.0.0.1:0', '-control-listen', '127.0.0.1:0'],
  { env: { GOMAXPROCS: '2' }, stdio: ['ignore', 'pipe', 'pipe'] });
  relay.stderr.resume();
  const [line] = await once(createInterface({ input: relay.stdout }), 'line', { signal: AbortSignal.timeout(5000) });
  const startup = JSON.parse(line);
  const localRota = createLocalRotaClient({ controlUrl: `http://${startup.control_address}`, proxyUrl: `http://${startup.proxy_address}`, token, nodeId });

  const grants = [];
  const nodeClient = { ...natsClient, grantRoute: async value => { grants.push(value.action); return natsClient.grantRoute(value); } };
  const runtime = new RemoteManagedDiscoverRuntime({ pages, routes, nodeId, slot: 'discover-1', pollMs: 50 });
  const policy = { id: 'qy-br-discover-anonymous-v1', version: 1, hash: 'sha256:x', role: 'discover' };
  const assignment = { role: 'discover', identity_policy_id: policy.id, identity_policy_version: 1, identity_policy_hash: policy.hash,
    worker_id: 'rota-worker-1', worker_instance_id: 'center-1', slot_name: 'discover-slot', lease_id: 'lease-1', route_generation: 1,
    workload_scope: 'qy-test', network_identity_key: 'net-1', profile_epoch: 0, credential_generation: 1, egress_country: 'BR' };

  async function runPage() {
    const pageId = `page-${randomUUID()}`;
    await pool.query(`INSERT INTO crawler.query_pages(page_id,query_text,page_no,page_intent_hash,managed_fetch_status,status)
      VALUES($1,'receitas',1,$2,'running','running')`, [pageId, `hash-${pageId}`]);
    const prepared = { businessRunId: discoverBusinessRunId(pageId), page: { page_id: pageId, page_intent_hash: `hash-${pageId}` } };
    const handle = await runtime.acquire({ assignment, policy, task: { task_id: randomUUID(), job_execution_id: 'exec-1' }, prepared });
    const spool = new RemoteResultSpool({ directory: join(directory, pageId) });
    const networkSession = new RemoteChannelNetworkSession({ client: { ...nodeClient, pollCommands: nodeClient.discoverPoll },
      localRota, spool, slot: 'discover-1', renewMs: 150, retryMs: 10,
      withRuntime: createDiscoverRouteRuntime({ createDispatcher: proxy => new ProxyAgent({ uri: proxy, requestTls: { ca } }) }) });
    const executor = new RemoteDiscoverExecutor({ client: nodeClient, spool, networkSession });
    const centerSide = handle.execute({}, () => runtime.requestPage(request)).then(value => ({ value }), error => ({ error }));
    const nodeSide = executor.runOnce();
    const outcome = await centerSide;
    if (outcome.error) await runtime.retire(handle);
    const nodeStatus = await nodeSide;
    const quiet = await runtime.quiesce(handle);
    const binding = (await pool.query('SELECT state,release_receipt FROM remote_ingestion.network_bindings WHERE task_id=$1',
      [handle.admission.taskId])).rows[0];
    return { ...outcome, nodeStatus, quiet, binding, spool };
  }

  const success = await runPage();
  assert.equal(success.error, undefined, success.error?.stack);
  assert.equal(success.nodeStatus, 'applied');
  assert.equal(success.value.rawText, searchHtml);
  assert.equal(success.value.status, 200);
  assert.equal(upstreamConnects.length, 1);
  assert.equal(upstreamConnects[0].line, 'CONNECT www.youtube.com:443 HTTP/1.1');
  assert.equal(upstreamConnects[0].auth, `Basic ${Buffer.from('user:fixture-upstream-secret').toString('base64')}`,
    'the relay authenticates to the Rota upstream; the node never sees these credentials');
  assert.equal(grants[0], 'activate');
  assert.ok(grants.filter(action => action === 'renew').length >= 1, `route renewed during the request: ${grants}`);
  assert.deepEqual(success.quiet, { active_managed_requests: 0 });
  assert.equal(success.binding.state, 'retired');
  assert.equal(success.binding.release_receipt.in_flight, 0);
  assert.equal(await success.spool.read('network.json'), null);

  // A refused upstream is a failure of the route, never a silent success. Its
  // classification must match a local request through a proxy that returns 502.
  upstreamMode = 'refuse';
  const stub = net.createServer(socket => { socket.on('error', () => {});
    socket.once('data', () => socket.end('HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n')); });
  stub.listen(0, '127.0.0.1'); await once(stub, 'listening');
  t.after(() => new Promise(resolve => stub.close(resolve)));
  const local = await runWithProxyIdentity({ proxy_url: `http://127.0.0.1:${stub.address().port}`,
    dispatcher: new ProxyAgent({ uri: `http://127.0.0.1:${stub.address().port}` }) },
  () => requestDiscoverSearchPage(request)).then(() => null, error => error);
  assert.ok(local);
  const refused = await runPage();
  assert.ok(refused.error, 'the refused upstream fails the remote request');
  assert.equal(refused.nodeStatus, 'failed');
  assert.equal(refused.binding.state, 'retired');
  assert.equal(refused.binding.release_receipt.in_flight, 0);
  assert.equal(classifyYoutubeError(refused.error), classifyYoutubeError(local));
  assert.deepEqual(decideYoutubeFailure({ error: refused.error }).kind, decideYoutubeFailure({ error: local }).kind);
  assert.deepEqual(retryableRotaFailure(refused.error), retryableRotaFailure(local));
  t.diagnostic(`refused upstream: ${refused.error.message} -> ${JSON.stringify(retryableRotaFailure(refused.error))}`);
});
