import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes, randomUUID } from 'node:crypto';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn, fork } from 'node:child_process';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import { setTimeout as delay } from 'node:timers/promises';
import net from 'node:net';
import pg from 'pg';
import { RemoteNodeStore } from '../src/remoteNodes/store.js';
import { RemoteChannelPlanStore } from '../src/remoteNodes/channelPlanStore.js';
import { RemoteChannelRouteStore } from '../src/remoteNodes/channelRouteStore.js';
import { RemoteChannelNetworkSession } from '../src/remoteNodes/channelNetworkSession.js';
import { RemoteChannelPlanExecutor } from '../src/remoteNodes/channelPlanExecutor.js';
import { RemoteResultSpool } from '../src/remoteNodes/spool.js';
import { createRemoteNodeGateway } from '../src/remoteNodes/gateway.js';
import { createRemoteNodeClient } from '../src/remoteNodes/client.js';
import { createLocalRotaClient } from '../src/remoteNodes/localRotaClient.js';
import { runRemoteIncrementalPlan } from '../src/remoteNodes/incrementalCoordinator.js';
import { CHANNEL_PLAN_CAPABILITY } from '../src/remoteNodes/channelPlanContract.js';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';
import { PUBLICATION_WRITER_VERSION } from '../src/publicationWriterVersion.js';
import { INCREMENTAL_JOB_NAME, INCREMENTAL_QUEUE } from '../src/incrementalPlan.js';
import { emptyUploadsDecision } from '../src/youtubeUploadsCountry.js';
import { RotaSlotAdapter } from '../src/rotaSlotAdapter.js';
import { resolveWorkerIdentityPolicy } from '../src/identityPolicyCatalog.js';
import { createRemoteRotaChannelRuntime } from '../src/remoteNodes/rotaChannelRuntimeAdapter.js';
import { RemoteYoutubeSessionStore } from '../src/remoteNodes/youtubeSessionStore.js';
import { createRemoteYoutubeRuntime } from '../src/remoteNodes/youtubeRuntime.js';
import { createRemoteIncrementalWorker } from '../src/remoteNodes/incrementalWorker.js';

const resolvedPolicy = resolveWorkerIdentityPolicy({ role: 'channel', policyId: 'qy-br-channel-anonymous-v1',
  expectedWorkloadScope: 'qy-production', environment: {} });

const url = process.env.REMOTE_NODE_TEST_DATABASE_URL;
const binary = process.env.REMOTE_NODE_ROTA_TEST_BINARY;
test('channel lease owns its durable Rota binding and complete local network session', { skip: !url || !binary, timeout: 120000 }, async t => {
  const pool = new pg.Pool({ connectionString: url, max: 6, options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}` });
  const guard = await pool.connect();
  t.after(async () => { guard.release(); await pool.end(); });
  await assertIsolatedRemoteDatabase(pool);
  await guard.query('SELECT pg_advisory_lock(781137981)');
  for (const path of ['../src/schema.sql', '../src/remoteNodes/schema.sql', '../src/remoteNodes/routeSchema.sql', '../src/remoteNodes/youtubeSessionSchema.sql']) {
    await pool.query(await readFile(new URL(path, import.meta.url), 'utf8'));
  }
  const keypair = generateKeyPairSync('ed25519'); const secretKey = randomBytes(32);
  const store = new RemoteNodeStore({ pool }); const channelStore = new RemoteChannelPlanStore({ store });
  const contexts = new Map(); const sources = new Map(); let sourceHook = null;
  const assertBusinessFence = async (_client, task) => {
    if (contexts.get(task.task_id) !== task.context.execution_attempt_id) throw new Error('business fence stale');
  };
  const options = { channelStore, assertBusinessFence, privateKey: keypair.privateKey, secretKey,
    readRotaRoute: async fence => {
      await sourceHook?.(fence);
      const source = sources.get(fence.task_id);
      if (!source) throw new Error('Rota task unavailable');
      return { ...fence, ...source, route_lease_until_ms: Date.now() + 60000 };
    } };
  const routes = new RemoteChannelRouteStore(options);
  const youtubeSessions = new RemoteYoutubeSessionStore({ routes });
  const gateway = createRemoteNodeGateway({ store, channelPlans: channelStore, routes, youtubeSessions });
  gateway.listen(0, '127.0.0.1'); await once(gateway, 'listening');
  t.after(() => new Promise(resolve => { gateway.close(resolve); gateway.closeAllConnections(); }));
  const endpoint = `http://127.0.0.1:${gateway.address().port}`;

  async function fixture(childTest, { bind = true, scoped = true, videoOnly = false } = {}) {
    await pool.query('TRUNCATE remote_ingestion.nodes,remote_ingestion.tasks,remote_ingestion.claims,remote_ingestion.receipts,remote_ingestion.channel_commands CASCADE');
    contexts.clear(); sources.clear(); sourceHook = null;
    const directory = await mkdtemp(join(tmpdir(), 'remote-channel-network-'));
    childTest.after(() => rm(directory, { recursive: true, force: true }));
    const nodeId = randomUUID(); const token = randomBytes(32).toString('hex');
    await store.registerNode({ nodeId, token, capabilities: [CHANNEL_PLAN_CAPABILITY] });
    await routes.registerSlot(nodeId, 'worker-1', `remote-${nodeId}`);
    const client = createRemoteNodeClient({ url: endpoint, token, allowLoopbackHttp: true });
    const channelId = `UC${randomUUID().replaceAll('-', '').slice(0, 22)}`;
    await pool.query(`INSERT INTO crawler.channels(channel_id,channel_url,title,status) VALUES($1,$2,'Before network capture','active')`, [channelId, `https://www.youtube.com/channel/${channelId}`]);
    const planId = randomUUID(); const now = new Date().toISOString();
    const plan = { schema_version: 5, dispatch_generation: 1, job_id: `remote_network_${planId}`, plan_id: planId,
      plan_mode: 'standard', plan_day: now.slice(0, 10), scheduled_at: now, channel_id: channelId,
      task_mask: { about: !videoOnly, video: videoOnly, agent: false }, capacity: { factor: 1, player_cap: 20, next_cap: 8, version: 'capacity-1' },
      clock_version: 7, policy_version: 'v16-rule-1', planner_config_version: 'video-plan-1' };
    const queued = await channelStore.enqueue({ id: plan.job_id, name: INCREMENTAL_JOB_NAME, queueName: INCREMENTAL_QUEUE, data: plan }, { executionAttemptId: `execution:${planId}` });
    contexts.set(queued.taskId, `execution:${planId}`);
    const claimId = randomUUID(); const lease = await client.claim(claimId, scoped ? 'worker-1' : null);

    const sockets = new Set(); let upstreamCalls = 0;
    const upstream = net.createServer(socket => {
      sockets.add(socket); socket.on('close', () => sockets.delete(socket)); socket.on('error', () => {});
      let head = ''; let connected = false;
      socket.on('data', data => {
        if (connected) { socket.write(data); return; }
        head += data.toString();
        if (!head.includes('\r\n\r\n')) return;
        upstreamCalls++; connected = true;
        socket.write('HTTP/1.1 200 Connection Established\r\n\r\nBR');
      });
    });
    upstream.listen(0, '127.0.0.1'); await once(upstream, 'listening');
    childTest.after(() => { for (const socket of sockets) socket.destroy(); return new Promise(resolve => upstream.close(resolve)); });
    const fence = { slot_name: 'rota-channel-1', worker_id: `remote-${nodeId}`, worker_instance_id: 'instance-1', lease_id: randomUUID(),
      route_generation: 1, task_id: randomUUID(), business_run_id: `incremental:${planId}`, job_execution_id: `clock:${planId}:1` };
    const source = { workload_scope: 'qy-production', credential_generation: 1, network_identity_key: 'test-br', profile_epoch: 0,
      identity_policy_id: resolvedPolicy.policy.id, identity_policy_version: resolvedPolicy.policy.version,
      identity_policy_hash: resolvedPolicy.policy.hash, egress_country: 'BR',
      upstream: { protocol: 'http', address: `127.0.0.1:${upstream.address().port}`, username: 'user', password: 'fixture-upstream-secret' } };
    sources.set(fence.task_id, source);
    const binding = bind ? await routes.bind(nodeId, lease, 'worker-1', fence) : null;

    await writeFile(join(directory, 'public.pem'), keypair.publicKey.export({ type: 'spki', format: 'pem' }), { mode: 0o600 });
    await writeFile(join(directory, 'token'), token, { mode: 0o600 });
    const process = spawn(binary, ['-node-id', nodeId, '-public-key-file', join(directory, 'public.pem'), '-control-token-file', join(directory, 'token'),
      '-proxy-listen', '127.0.0.1:0', '-control-listen', '127.0.0.1:0'], { env: { GOMAXPROCS: '2' }, stdio: ['ignore', 'pipe', 'pipe'] });
    const exited = once(process, 'exit'); process.stderr.resume();
    childTest.after(async () => { if (process.exitCode === null) process.kill('SIGTERM'); await exited; });
    const lines = createInterface({ input: process.stdout });
    const [line] = await once(lines, 'line', { signal: AbortSignal.timeout(5000) });
    const startup = JSON.parse(line);
    const localRota = createLocalRotaClient({ controlUrl: `http://${startup.control_address}`, proxyUrl: `http://${startup.proxy_address}`, token, nodeId });
    const boot = await localRota.boot();
    const spool = new RemoteResultSpool({ directory: join(directory, 'spool') }); await spool.init();
    return { nodeId, client, lease, claimId, source, fence, binding, plan, channelId, localRota, boot, spool,
      childConfig: {center:{url:endpoint,token},relay:{controlUrl:`http://${startup.control_address}`,proxyUrl:`http://${startup.proxy_address}`,token,nodeId},directory:spool.directory,lease},
      request: { task_id: lease.task_id, generation: lease.generation, slot: 'worker-1', boot_id: boot.boot_id, action: 'activate', request_id: randomUUID() },
      upstreamCalls: () => upstreamCalls };
  }

  function session(f, client = f.client, withRuntime = (_route, invoke) => invoke()) {
    return new RemoteChannelNetworkSession({ client, localRota: f.localRota, spool: f.spool, slot: 'worker-1', withRuntime, renewMs: 100, retryMs: 10 });
  }
  async function connect(route, childTest) {
    const endpoint = new URL(route.proxyUrl); const socket = net.connect(Number(endpoint.port), endpoint.hostname);
    socket.on('error', () => {}); childTest.after(() => socket.destroy()); await once(socket, 'connect');
    const ready = new Promise(resolve => {
      let data = ''; socket.on('data', chunk => { data += chunk.toString(); if (data.endsWith('BR')) resolve(); });
    });
    socket.write(`CONNECT www.youtube.com:443 HTTP/1.1\r\nHost: www.youtube.com:443\r\nProxy-Authorization: Basic ${Buffer.from(`${decodeURIComponent(endpoint.username)}:${decodeURIComponent(endpoint.password)}`).toString('base64')}\r\n\r\n`);
    await ready; return socket;
  }

  function profileFor(f) {
    return { ...f.source, profile_group_id: randomUUID(), profile_revision: 1, clients: {
      youtubejs_chrome: { profile_id: 'fixture-youtube-profile', engine: 'youtubejs_chrome', impersonate_target: 'chrome136',
        user_agent: 'Fixture Chrome', visitor_data: 'CgtmaXh0dXJldmlzaXQ=', language: 'en', country: 'BR', timezone: 'America/Sao_Paulo',
        fingerprint_json: { max_connections: 1 }, cookie_state: { cookies: [{ name: 'VISITOR_INFO1_LIVE', value: 'private-cookie', domain: '.youtube.com', path: '/' }] } },
      ytdlp_safari: { must_not_be_transported: true },
    } };
  }

  await t.test('SIGKILL node retires persisted active network before waiting for a replacement instance', async tt => {
    const f = await fixture(tt);
    const child = fork(new URL('./fixtures/remoteNetworkCrash.mjs', import.meta.url), [], { stdio: ['ignore','ignore','pipe','ipc'] });
    child.stderr.resume(); const exited = once(child, 'exit');
    tt.after(async () => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); await exited; });
    const message = once(child, 'message', { signal: AbortSignal.timeout(10000) }); child.send(f.childConfig);
    assert.deepEqual((await message)[0], { active: true });
    child.kill('SIGKILL'); assert.equal((await exited)[1], 'SIGKILL');
    assert.equal((await f.spool.read('network.json')).phase, 'active');
    assert.equal((await f.client.pollCommands(f.lease)).status, 'leased');
    await session(f).recover().catch(error => { assert.equal(error.code, 'NETWORK_EXECUTION_CLOSED'); });
    const binding = (await pool.query('SELECT * FROM remote_ingestion.network_bindings WHERE binding_id=$1', [f.binding.binding_id])).rows[0];
    assert.equal(binding.state, 'retired', 'a restarted process cannot resume the dead browser session');
    assert.equal(binding.release_receipt.in_flight, 0);
  });

  await t.test('specific Worker claims are exclusive, replayable, and cannot bind another slot', async tt => {
    const f = await fixture(tt, { scoped: true });
    assert.equal(f.lease.worker_slot, 'worker-1');
    await assert.rejects(f.client.claim(randomUUID()), { code: 'WORKER_SLOT_REQUIRED' });
    assert.deepEqual(await f.client.claim(f.claimId, 'worker-1'), f.lease);
    await routes.registerSlot(f.nodeId, 'worker-2', `second-${f.nodeId}`);
    await pool.query('UPDATE remote_ingestion.nodes SET max_leases=3 WHERE node_id=$1', [f.nodeId]);
    for (let i = 0; i < 2; i++) await store.enqueue({ workKey: randomUUID(), capability: CHANNEL_PLAN_CAPABILITY, input: f.lease.input, context: {} });
    assert.equal(await f.client.claim(randomUUID(), 'worker-1'), null);
    const claims = await Promise.all([f.client.claim(randomUUID(), 'worker-2'), f.client.claim(randomUUID(), 'worker-2')]);
    assert.equal(claims.filter(Boolean).length, 1);
    assert.equal(claims.find(Boolean).worker_slot, 'worker-2');
    await assert.rejects(f.client.claim(f.claimId, 'worker-2'), { code: 'CLAIM_EXPIRED' });
    await assert.rejects(f.client.claim(randomUUID(), 'unknown'), { code: 'UNKNOWN_NETWORK_SLOT' });
    await assert.rejects(routes.bind(f.nodeId, f.lease, 'worker-2', f.fence), { code: 'WORKER_SLOT_MISMATCH' });
    await pool.query("UPDATE remote_ingestion.tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE task_id=$1", [f.lease.task_id]);
    assert.equal(await f.client.claim(randomUUID(), 'worker-1'), null, 'expired uncertain owner must be recovered by center first');
  });

  await t.test('authorized YouTubeJS session traverses the gateway, checkpoints encrypted state, and retires its relay', async tt => {
    const f = await fixture(tt, { scoped: true }); const profileGroup = profileFor(f);
    await youtubeSessions.prepare(f.binding.binding_id, { profileGroup, attemptId: contexts.get(f.lease.task_id) });
    await youtubeSessions.prepare(f.binding.binding_id, { profileGroup, attemptId: contexts.get(f.lease.task_id) });
    await assert.rejects(youtubeSessions.prepare(f.binding.binding_id, { profileGroup, attemptId: 'changed-attempt' }), { code: 'YOUTUBE_SESSION_ATTEMPT_MISMATCH' });
    await assert.rejects(youtubeSessions.prepare(f.binding.binding_id, { profileGroup: { ...profileGroup, profile_revision: 2 }, attemptId: contexts.get(f.lease.task_id) }), { code: 'YOUTUBE_SESSION_CONFLICT' });
    let request; let activeRoute; let captures = 0; let uploaded;
    const client = { ...f.client, youtubeSession: async value => {
      request = value; const bundle = await f.client.youtubeSession(value);
      await assert.rejects(f.client.youtubeSession({ ...value, profile_group: {} }), { code: 'INVALID_YOUTUBE_SESSION_REQUEST' });
      assert.deepEqual(Object.keys(bundle.profile_group.clients), ['youtubejs_chrome']);
      assert.equal(bundle.profile_group.upstream, undefined);
      return bundle;
    }, youtubeCheckpoint: async value => { uploaded = value; return f.client.youtubeCheckpoint(value); } };
    const runtime = createRemoteYoutubeRuntime({ client, spool: f.spool, gateway: {
      prepare: async ({ proxyUrl }) => { assert.equal(new URL(proxyUrl).hostname, '127.0.0.1'); },
      fetch: async () => { captures++; await connect(activeRoute, tt); return new Response(JSON.stringify({
        metadata: { channelMetadataRenderer: { title: 'Actual remote parser', externalId: f.channelId } },
        contents: { twoColumnBrowseResultsRenderer: { tabs: [{ tabRenderer: { title: 'Home', selected: true, content: { sectionListRenderer: { contents: [] } } } }] } },
      }), { headers: { 'content-type': 'application/json' } }); },
      snapshot: async () => ({ cookies: [{ name: 'VISITOR_INFO1_LIVE', value: 'private-updated-cookie', domain: '.youtube.com', path: '/' }] }),
      close: async () => {},
    } });
    const withRuntime = (route, invoke) => { activeRoute = route; return runtime.withRuntime(route, invoke); };
    withRuntime.recover = runtime.withRuntime.recover;
    const network = session(f, client, withRuntime);
    const mode = process.env.YOUTUBEJS_EXTRACTOR_MODE; process.env.YOUTUBEJS_EXTRACTOR_MODE = 'full';
    tt.after(() => { if (mode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE; else process.env.YOUTUBEJS_EXTRACTOR_MODE = mode; });
    const result = await network.run(f.lease, { signal: AbortSignal.timeout(10000) }, () => runtime.youtube.openChannel(f.channelId, { includeAbout: false }));
    assert.equal(result.metadata.title, 'Actual remote parser'); assert.equal(captures, 1); assert.equal(f.upstreamCalls(), 1);
    const checkpoint = await youtubeSessions.result(f.binding.binding_id);
    assert.equal(checkpoint.status, 'success'); assert.equal(checkpoint.metrics.request_count, 1);
    assert.equal(checkpoint.cookies.cookies[0].value, 'private-updated-cookie');
    const row = (await pool.query('SELECT * FROM remote_ingestion.youtube_sessions WHERE binding_id=$1', [f.binding.binding_id])).rows[0];
    assert.equal(row.session_cipher.includes(Buffer.from('private-cookie')), false);
    assert.equal(row.checkpoint_cipher.includes(Buffer.from('private-updated-cookie')), false);
    assert.equal((await f.client.youtubeCheckpoint(uploaded)).durable, true, 'lost receipt remains replayable after network retirement');
    await assert.rejects(f.client.youtubeCheckpoint({ ...uploaded, checkpoint: { ...checkpoint, cookies: { cookies: [] } } }), { code: 'YOUTUBE_CHECKPOINT_CONFLICT' });
    await assert.rejects(f.client.youtubeSession(request), { code: 'YOUTUBE_SESSION_BINDING_CLOSED' });
    await assert.rejects(f.client.youtubeCheckpoint({ ...uploaded, request: { ...request, generation: 2 } }), { code: 'STALE_LEASE' });
    await assert.rejects(f.client.youtubeCheckpoint({ ...uploaded, request: { ...request, boot_id: 'f'.repeat(48) } }), { code: 'YOUTUBE_SESSION_OWNERSHIP_MISMATCH' });
    assert.equal((await pool.query('SELECT state FROM remote_ingestion.network_bindings WHERE binding_id=$1', [f.binding.binding_id])).rows[0].state, 'retired');
  });

  await t.test('assembled Worker runs a whole video Plan through the original empty-uploads dormancy path', async tt => {
    const f = await fixture(tt, { bind: false, videoOnly: true });
    f.binding = await youtubeSessions.bind({ nodeId: f.nodeId, lease: f.lease, slot: 'worker-1', rotaFence: f.fence,
      profileGroup: profileFor(f), attemptId: contexts.get(f.lease.task_id) });
    let proxyUrl; let captures = 0;
    const mode = process.env.YOUTUBEJS_EXTRACTOR_MODE; process.env.YOUTUBEJS_EXTRACTOR_MODE = 'full';
    tt.after(() => { if (mode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE; else process.env.YOUTUBEJS_EXTRACTOR_MODE = mode; });
    const worker = createRemoteIncrementalWorker({ client: { ...f.client, claim: () => f.client.claim(f.claimId, 'worker-1') },
      localRota: f.localRota, slot: 'worker-1', spool: f.spool, pollMs: 5, timeoutMs: 15000, gateway: {
        prepare: async value => { proxyUrl = value.proxyUrl; },
        fetch: async () => { captures++; await connect({ proxyUrl }, tt); return new Response(JSON.stringify({
          metadata: { channelMetadataRenderer: { title: 'Empty uploads fixture', externalId: f.channelId } },
          contents: { twoColumnBrowseResultsRenderer: { tabs: [{ tabRenderer: { title: 'Home', selected: true, content: { sectionListRenderer: { contents: [] } } } }] } },
        }), { headers: { 'content-type': 'application/json' } }); },
        snapshot: async () => ({ cookies: [] }), close: async () => {},
      } });
    const results = await Promise.allSettled([
      runRemoteIncrementalPlan({ channelStore, lease: f.lease, assertBusinessFence, pollMs: 5, signal: AbortSignal.timeout(15000) }),
      worker.runOnce(),
    ]);
    for (const result of results) if (result.status === 'rejected') throw result.reason;
    assert.equal(results[1].value, 'applied'); assert.equal(captures, 1);
    assert.equal((await pool.query('SELECT status FROM crawler.channel_runs WHERE plan_id=$1', [f.plan.plan_id])).rows[0].status, 'done');
    assert.equal((await pool.query('SELECT status FROM crawler.channels WHERE channel_id=$1', [f.channelId])).rows[0].status, 'dormant');
    assert.equal((await youtubeSessions.result(f.binding.binding_id)).status, 'success');
  });

  await t.test('binding/grant retries survive center restart and encrypt the single cached configuration', async tt => {
    const f = await fixture(tt);
    const signed = await f.client.grantRoute(f.request);
    const restarted = new RemoteChannelRouteStore(options);
    assert.deepEqual(await restarted.grant(f.nodeId, f.request), signed);
    const row = (await pool.query('SELECT epoch,grant_cipher FROM remote_ingestion.network_slots WHERE node_id=$1', [f.nodeId])).rows[0];
    assert.equal(Number(row.epoch), 1);
    assert.ok(!row.grant_cipher.includes(Buffer.from('fixture-upstream-secret')));
    assert.equal((await routes.bind(f.nodeId, f.lease, 'worker-1', f.fence)).binding_id, f.binding.binding_id);
    await assert.rejects(f.client.grantRoute({ ...f.request, action: 'renew' }), { code: 'ROUTE_REQUEST_CONFLICT' });
    await assert.rejects(routes.bind(f.nodeId, f.lease, 'worker-1', { ...f.fence, business_run_id: 'other-plan' }), { code: 'ROTA_TASK_BINDING_MISMATCH' });
    const second = randomUUID(); await store.registerNode({ nodeId: second, token: 'second-node-'.repeat(4), capabilities: [CHANNEL_PLAN_CAPABILITY] });
    await assert.rejects(routes.grant(second, f.request), { code: 'STALE_LEASE' });
  });

  await t.test('recheck after Rota HTTP rejects a channel cancelled while reading its proxy', async tt => {
    const f = await fixture(tt);
    sourceHook = async () => pool.query("UPDATE remote_ingestion.tasks SET state='cancelled' WHERE task_id=$1", [f.lease.task_id]);
    await assert.rejects(f.client.grantRoute(f.request), { code: 'STALE_LEASE' });
    const slot = (await pool.query('SELECT epoch,grant_cipher FROM remote_ingestion.network_slots WHERE node_id=$1', [f.nodeId])).rows[0];
    assert.equal(Number(slot.epoch), 0); assert.equal(slot.grant_cipher, null);
  });

  await t.test('completion concurrent with network renewal cannot deadlock through the node foreign key', async tt => {
    const f = await fixture(tt); const { coordinatorId } = await channelStore.coordinate(f.lease);
    let ready; const coordinatorReady = new Promise(resolve => { ready = resolve; });
    let locked; const nodeLocked = new Promise(resolve => { locked = resolve; });
    const completion = channelStore.transaction(f.lease, coordinatorId, assertBusinessFence, async (client, task) => {
      ready(); await nodeLocked;
      await channelStore.complete(client, task, { done: true });
    });
    const renewal = (async () => {
      await coordinatorReady;
      return store.transaction(client => routes.owned({ query: async (sql, args) => {
        const result = await client.query(sql, args);
        if (sql.includes('FROM remote_ingestion.nodes') && sql.includes('FOR ')) locked();
        return result;
      } }, f.nodeId, f.lease, 'worker-1'));
    })();
    const results = await Promise.allSettled([completion, renewal]);
    if (results[0].status === 'rejected') throw results[0].reason;
    assert.equal(results[1].status, 'rejected'); assert.equal(results[1].reason.code, 'STALE_LEASE');
  });

  async function fullPlan(tt, loseRelease = false, useOriginalRota = false, useYoutubeSession = false) {
    const f = await fixture(tt, { bind: !useOriginalRota }); let captures = 0; let renewals = 0; let releases = 0; let activeRoute;
    const rotaCalls = [];
    const client = { ...f.client, claim: () => f.client.claim(f.claimId, 'worker-1'), grantRoute: value => {
      if (value.action === 'renew') renewals++;
      return f.client.grantRoute(value);
    }, releaseRoute: async value => {
      if (useOriginalRota) {
        const deadline = AbortSignal.timeout(5000);
        for (;;) {
          deadline.throwIfAborted();
          const row = (await pool.query('SELECT stop_requested FROM remote_ingestion.network_bindings WHERE binding_id=$1', [f.binding.binding_id])).rows[0];
          if (row.stop_requested) break;
          await delay(5);
        }
        // Center has asked to stop, but Rota must still wait for this receipt.
        assert.equal(rotaCalls.includes('complete'), false);
        await assert.rejects(f.client.grantRoute({ ...f.request, action: 'renew', request_id: randomUUID() }),
          error => ['STALE_LEASE', 'NETWORK_STOPPING'].includes(error.code));
      }
      releases++; const result = await f.client.releaseRoute(value);
      if (loseRelease && releases === 1) throw new Error('simulated lost release acknowledgement');
      return result;
    } };
    let withRuntime = async (route, invoke) => { activeRoute = route; return invoke(); };
    if (useYoutubeSession) {
      const mode = process.env.YOUTUBEJS_EXTRACTOR_MODE; process.env.YOUTUBEJS_EXTRACTOR_MODE = 'full';
      tt.after(() => { if (mode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE; else process.env.YOUTUBEJS_EXTRACTOR_MODE = mode; });
      const runtime = createRemoteYoutubeRuntime({ client, spool: f.spool, gateway: {
        prepare: async () => {}, fetch: () => assert.fail('About fixture performs network probe separately'),
        snapshot: async () => ({ cookies: [] }), close: async () => {},
      } });
      withRuntime = (route, invoke) => { activeRoute = route; return runtime.withRuntime(route, invoke); };
      withRuntime.recover = runtime.withRuntime.recover;
    }
    const network = session(f, client, withRuntime);
    const youtube = { openChannel: async (id, options) => {
      captures++;
      assert.equal(emptyUploadsDecision('BR').reason, 'country_checked');
      await connect(activeRoute, tt); await delay(450);
      return { about_requested: options.includeAbout, about_observed: true, metadata: { channel_id: id, title: 'Network Plan result',
        subscriber_count_text: '1,234 subscribers', subscriber_count_source: 'youtube_about',
        view_count_text: '98,765 views', view_count_source: 'youtube_about', video_count_text: '42 videos', video_count_source: 'youtube_about',
        keywords: [], external_links: [], external_links_status: 'observed', available_tabs: ['videos'] }, raw: { engine: 'youtubei.js@fixture' } };
    }, fetchDetail: () => assert.fail('About-only must not fetch detail') };
    const worker = new RemoteChannelPlanExecutor({ client, spool: f.spool, youtube, networkSession: network, pollMs: 5, timeoutMs: 15000 });
    let adapter;
    const runCenter = signal => runRemoteIncrementalPlan({ channelStore, lease: f.lease, assertBusinessFence, pollMs: 5, signal });
    if (useOriginalRota) {
      const assignment = { ...f.source, ...f.fence, ok: true, ready: true, protocol_version: 2, role: 'channel',
        control_state: 'leased_idle', proxy_user: 'isolated-worker', lease_remaining_ms: 60000,
        server_time: new Date().toISOString(), identity_action: 'keep' };
      const runtime = createRemoteRotaChannelRuntime({ routes, nodeId: f.nodeId, lease: f.lease, slot: 'worker-1', stopTimeoutMs: 5000,
        ...(useYoutubeSession ? { youtubeSessions, youtubeSession: { profileGroup: profileFor(f), attemptId: contexts.get(f.lease.task_id) } } : {}) });
      const acquire = runtime.acquire;
      runtime.acquire = async context => {
        const handle = await acquire(context); const execute = handle.execute;
        handle.execute = (input, invoke) => execute(input, () => { f.binding = handle.binding; return invoke(); });
        return handle;
      };
      const rotaClient = {
        claim: async () => { rotaCalls.push('claim'); return assignment; },
        renew: async () => assignment,
        beginTask: async request => {
          rotaCalls.push('begin');
          return { ...request, ok: true, task_id: f.fence.task_id, attempt_number: 1, started_at: new Date().toISOString() };
        },
        completeTask: async request => {
          const row = (await pool.query('SELECT state,release_receipt FROM remote_ingestion.network_bindings WHERE binding_id=$1', [f.binding.binding_id])).rows[0];
          assert.equal(row.state, 'retired'); assert.equal(row.release_receipt.in_flight, 0);
          assert.equal(request.attempt_quiesced, true); assert.equal(request.active_managed_requests, 0);
          assert.equal(request.outcome, 'success'); rotaCalls.push('complete');
          if (useYoutubeSession) assert.equal((await youtubeSessions.result(f.binding.binding_id)).status, 'success');
          return { ...request, ok: true, task_completed: true, control_state: 'READY_KEEP_ROUTE', ready: true,
            completed_task_route_generation: request.route_generation };
        },
        release: async request => { rotaCalls.push('release'); return { ...request, ok: true, released: true,
          route_generation: request.known_route_generation, status: 'released', released_at: new Date().toISOString() }; },
      };
      adapter = new RotaSlotAdapter({ client: rotaClient, role: 'channel', workerId: f.fence.worker_id,
        workerInstanceId: f.fence.worker_instance_id, resolvedPolicy, proxyBaseUrl: 'http://unused-center.invalid:8000',
        proxyPassword: 'not-sent-to-node', identityRuntime: runtime, renewIntervalMs: 60000 });
      tt.after(() => adapter.close()); await adapter.start();
    }
    const center = adapter ? adapter.executeJob({ id: f.plan.job_id, queueName: INCREMENTAL_QUEUE, attemptsMade: 0, attemptsStarted: 1, data: f.plan }, {
      prepare: async () => ({ kind: 'ready', businessRunId: `incremental:${f.plan.plan_id}`, workloadKind: 'incremental',
        identityPolicyId: resolvedPolicy.policy.id, identityPolicyVersion: resolvedPolicy.policy.version, identityPolicyHash: resolvedPolicy.policy.hash }),
      executeAttempt: async (_prepared, attempt) => ({ kind: 'managed_work_complete', businessState: 'terminal', result: await runCenter(attempt.abortSignal) }),
    }) : runCenter();
    if (loseRelease) {
      await assert.rejects(worker.runOnce(), /lost release acknowledgement/);
      assert.equal((await f.spool.read('network.json')).phase, 'release');
      await session(f).recover();
    } else assert.equal(await worker.runOnce(), 'applied');
    await center;
    if (adapter) { await adapter.close(); assert.deepEqual(rotaCalls, ['claim', 'begin', 'complete', 'release']); }
    assert.equal(captures, 1); assert.ok(renewals >= 1); assert.equal(f.upstreamCalls(), 1);
    assert.equal(await f.spool.read('network.json'), null);
    assert.deepEqual(await routes.waitQuiesced(f.binding.binding_id, { signal: AbortSignal.timeout(1000) }), { active_managed_requests: 0 });
    const route = (await pool.query('SELECT state,release_receipt FROM remote_ingestion.network_bindings WHERE binding_id=$1', [f.binding.binding_id])).rows[0];
    assert.equal(route.state, 'retired'); assert.equal(route.release_receipt.in_flight, 0);
    const slot = (await pool.query('SELECT epoch,grant_cipher FROM remote_ingestion.network_slots WHERE node_id=$1', [f.nodeId])).rows[0];
    assert.equal(Number(slot.epoch), 1); assert.equal(slot.grant_cipher, null);
    assert.equal((await pool.query('SELECT title FROM crawler.channels WHERE channel_id=$1', [f.channelId])).rows[0].title, 'Network Plan result');
    assert.equal((await pool.query('SELECT status FROM crawler.channel_runs WHERE plan_id=$1', [f.plan.plan_id])).rows[0].status, 'done');
  }
  await t.test('whole About Plan automatically acquires, renews and releases proxy through the real Go relay', tt => fullPlan(tt));
  await t.test('lost release acknowledgement replays only cleanup, with no second channel capture', tt => fullPlan(tt, true));
  await t.test('original RotaSlotAdapter completes its Task only after remote Plan and local connection retirement', tt => fullPlan(tt, false, true));
  await t.test('whole Plan uses a centrally frozen YouTube session and checkpoints before original Rota completion', tt => fullPlan(tt, false, true, true));

  await t.test('route cannot activate while its central browser profile is still being prepared', async tt => {
    const f = await fixture(tt, { bind: false });
    f.binding = await routes.bind(f.nodeId, f.lease, 'worker-1', f.fence, null, { youtubeSessionRequired: true });
    await assert.rejects(f.client.grantRoute(f.request), { code: 'YOUTUBE_SESSION_NOT_PREPARED' });
    await youtubeSessions.prepare(f.binding.binding_id, { profileGroup: profileFor(f), attemptId: contexts.get(f.lease.task_id) });
    await session(f).run(f.lease, { signal: AbortSignal.timeout(5000) }, async () => 'ready');
  });

  await t.test('stopping before grant retires safely and prevents a late activation', async tt => {
    const f = await fixture(tt);
    await routes.requestStop(f.binding.binding_id);
    assert.deepEqual(await routes.waitQuiesced(f.binding.binding_id, { signal: AbortSignal.timeout(1000) }), { active_managed_requests: 0 });
    await assert.rejects(f.client.grantRoute(f.request), { code: 'NETWORK_ALREADY_RETIRED' });
    assert.deepEqual(await f.client.abandonRoute(f.request), { abandoned: true });
    assert.equal(f.upstreamCalls(), 0);
  });

  await t.test('stopping an active route rejects renewals and waits for actual local retirement', async tt => {
    const f = await fixture(tt); const signed = await f.client.grantRoute(f.request);
    const applied = await f.localRota.apply(signed, { lease: f.lease, slot: 'worker-1', bootId: f.boot.boot_id });
    const socket = await connect(applied, tt); const closed = once(socket, 'close');
    await routes.requestStop(f.binding.binding_id);
    await assert.rejects(f.client.grantRoute({ ...f.request, action: 'renew', request_id: randomUUID() }), { code: 'NETWORK_STOPPING' });
    await assert.rejects(routes.waitQuiesced(f.binding.binding_id, { signal: AbortSignal.timeout(100) }), { name: 'AbortError' });
    const receipt = await f.localRota.retire({ ...f.request, epoch: applied.epoch });
    await closed; await f.client.releaseRoute(receipt);
    assert.deepEqual(await routes.waitQuiesced(f.binding.binding_id, { signal: AbortSignal.timeout(1000) }), { active_managed_requests: 0 });
  });

  await t.test('identity mismatch is rejected before a node can activate the binding', async tt => {
    const f = await fixture(tt, { bind: false });
    const runtime = createRemoteRotaChannelRuntime({ routes, nodeId: f.nodeId, lease: f.lease, slot: 'worker-1' });
    const handle = await runtime.acquire({ assignment: { ...f.fence, ...f.source, egress_country: 'US' },
      task: f.fence, prepared: { businessRunId: f.fence.business_run_id } });
    await assert.rejects(handle.execute({ job: { data: f.plan } }, () => assert.fail('must not run')),
      { code: 'REMOTE_ROTA_IDENTITY_MISMATCH' });
    assert.deepEqual(await runtime.quiesce(handle), { active_managed_requests: 0 });
    assert.equal(Number((await pool.query('SELECT count(*) FROM remote_ingestion.network_bindings')).rows[0].count), 0);
    await assert.rejects(f.client.grantRoute(f.request), { code: 'NETWORK_NOT_BOUND' });
  });

  await t.test('lost bind COMMIT response still finds and retires an authorization already used by the node', async tt => {
    const f = await fixture(tt, { bind: false }); let applied;
    const uncertainRoutes = Object.create(routes);
    uncertainRoutes.bind = async (...args) => {
      const binding = await routes.bind(...args); f.binding = binding;
      const signed = await f.client.grantRoute(f.request);
      applied = await f.localRota.apply(signed, { lease: f.lease, slot: 'worker-1', bootId: f.boot.boot_id });
      await connect(applied, tt);
      throw new Error('simulated lost bind COMMIT response');
    };
    const runtime = createRemoteRotaChannelRuntime({ routes: uncertainRoutes, nodeId: f.nodeId, lease: f.lease, slot: 'worker-1', stopTimeoutMs: 5000 });
    const handle = await runtime.acquire({ assignment: { ...f.fence, ...f.source }, task: f.fence,
      prepared: { businessRunId: f.fence.business_run_id } });
    await assert.rejects(handle.execute({ job: { data: f.plan } }, () => assert.fail('must not run')), /lost bind COMMIT/);
    assert.equal(handle.binding, null);
    let quiesced = false; const cleanup = runtime.quiesce(handle).then(value => { quiesced = true; return value; });
    const deadline = AbortSignal.timeout(5000);
    while (!(await pool.query('SELECT stop_requested FROM remote_ingestion.network_bindings WHERE binding_id=$1', [f.binding.binding_id])).rows[0].stop_requested) {
      deadline.throwIfAborted(); await delay(5);
    }
    assert.equal(quiesced, false);
    await f.client.releaseRoute(await f.localRota.retire({ ...f.request, epoch: applied.epoch }));
    assert.deepEqual(await cleanup, { active_managed_requests: 0 });
  });

  await t.test('executor restart retires saved activation without restarting the browser identity', async tt => {
    const f = await fixture(tt); const signed = await f.client.grantRoute(f.request);
    const ack = await f.localRota.apply(signed, { lease: f.lease, slot: 'worker-1', bootId: f.boot.boot_id });
    const { proxyUrl, ...applied } = ack;
    await f.spool.save('network.json', Buffer.from(JSON.stringify({ phase: 'active', lease: f.lease, boot_id: f.boot.boot_id, applied, request: f.request })));
    const restarted = session(f); await assert.rejects(restarted.recover(), { code: 'NETWORK_EXECUTION_CLOSED' });
    assert.equal((await f.spool.read('network.json')).phase, 'closed');
    await assert.rejects(restarted.run(f.lease, { signal: AbortSignal.timeout(5000) }, () => assert.fail('dead session must not collect')), { code: 'NETWORK_EXECUTION_CLOSED' });
    assert.equal(Number((await pool.query('SELECT epoch FROM remote_ingestion.network_slots WHERE node_id=$1', [f.nodeId])).rows[0].epoch), 1);
    await pool.query("UPDATE remote_ingestion.tasks SET state='failed' WHERE task_id=$1", [f.lease.task_id]);
    await restarted.recover(); assert.equal(await f.spool.read('network.json'), null);
  });

  await t.test('renewal failure aborts extraction and retires local network without completing the channel', async tt => {
    const f = await fixture(tt);
    const network = session(f, { ...f.client, grantRoute: request => {
      if (request.action === 'renew') throw new Error('simulated center outage');
      return f.client.grantRoute(request);
    } });
    await assert.rejects(network.run(f.lease, { signal: AbortSignal.timeout(5000) }, async ({ signal }) => {
      await delay(4000, null, { signal });
    }));
    assert.equal((await pool.query('SELECT state FROM remote_ingestion.tasks WHERE task_id=$1', [f.lease.task_id])).rows[0].state, 'leased');
    assert.equal((await pool.query('SELECT state FROM remote_ingestion.network_bindings WHERE binding_id=$1', [f.binding.binding_id])).rows[0].state, 'retired');
    assert.equal((await f.spool.read('network.json')).phase, 'closed');
    await assert.rejects(network.recover(), { code: 'NETWORK_EXECUTION_CLOSED' });
    // The interrupted executor must not prolong its old lease indefinitely.
    await pool.query("UPDATE remote_ingestion.tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE task_id=$1", [f.lease.task_id]);
    await network.recover();
    const nextLease = await f.client.claim(randomUUID(), 'worker-1');
    assert.equal(nextLease.generation, 2);
    const nextFence = { ...f.fence, task_id: randomUUID(), job_execution_id: `${f.fence.job_execution_id}:resume` };
    sources.set(nextFence.task_id, f.source);
    const nextBinding = await routes.bind(f.nodeId, nextLease, 'worker-1', nextFence);
    const oldReceipt = (await pool.query('SELECT release_receipt FROM remote_ingestion.network_bindings WHERE binding_id=$1', [f.binding.binding_id])).rows[0].release_receipt;
    await session(f).run(nextLease, { signal: AbortSignal.timeout(5000) }, async () => {
      await f.client.releaseRoute(oldReceipt); await routes.requestStop(f.binding.binding_id);
      const row = (await pool.query('SELECT state,stop_requested FROM remote_ingestion.network_bindings WHERE binding_id=$1', [nextBinding.binding_id])).rows[0];
      assert.equal(row.state, 'active'); assert.equal(row.stop_requested, false);
      return 'resumed';
    });
    assert.equal(Number((await pool.query('SELECT epoch FROM remote_ingestion.network_slots WHERE node_id=$1', [f.nodeId])).rows[0].epoch), 2);
  });

  await t.test('cancelled activation with a lost response is abandoned without ever connecting a proxy', async tt => {
    const f = await fixture(tt); await f.client.grantRoute(f.request);
    await f.spool.save('network.json', Buffer.from(JSON.stringify({ phase: 'activate', lease: f.lease, boot_id: f.boot.boot_id, request: f.request })));
    await pool.query("UPDATE remote_ingestion.tasks SET state='cancelled' WHERE task_id=$1", [f.lease.task_id]);
    await session(f).recover();
    assert.equal(await f.spool.read('network.json'), null); assert.equal(f.upstreamCalls(), 0);
    await assert.rejects(f.client.grantRoute(f.request), { code: 'STALE_LEASE' });
  });

  await t.test('lost activation response while lease is live abandons the request without extending the failed execution', async tt => {
    const f = await fixture(tt);
    const network = session(f, { ...f.client, grantRoute: async request => {
      await f.client.grantRoute(request); throw new Error('lost activation response');
    } });
    await assert.rejects(network.run(f.lease, { signal: AbortSignal.timeout(5000) }, () => assert.fail('must not capture')), /lost activation response/);
    assert.equal((await f.spool.read('network.json')).phase, 'closed');
    await assert.rejects(network.recover(), { code: 'NETWORK_EXECUTION_CLOSED' });
    assert.equal((await pool.query('SELECT state FROM remote_ingestion.network_bindings WHERE binding_id=$1', [f.binding.binding_id])).rows[0].state, 'retired');
    assert.equal(f.upstreamCalls(), 0);
  });

  await t.test('lost cleanup acknowledgement after interruption cannot restart the same retired execution', async tt => {
    const f = await fixture(tt); let lose = true;
    const network = session(f, { ...f.client, grantRoute: request => {
      if (request.action === 'renew') throw new Error('renewal unavailable');
      return f.client.grantRoute(request);
    }, releaseRoute: async receipt => {
      const result = await f.client.releaseRoute(receipt);
      if (lose) { lose = false; throw new Error('lost interrupted cleanup response'); }
      return result;
    } });
    await assert.rejects(network.run(f.lease, { signal: AbortSignal.timeout(5000) }, ({ signal }) => delay(4000, null, { signal })), /lost interrupted cleanup response/);
    const saved = await f.spool.read('network.json');
    assert.equal(saved.phase, 'release'); assert.equal(saved.interrupted, true);
    await assert.rejects(network.recover(), { code: 'NETWORK_EXECUTION_CLOSED' });
    assert.equal((await f.spool.read('network.json')).phase, 'closed');
  });

  await t.test('a persisted but unapplied grant can be retired and cannot activate afterwards', async tt => {
    const f = await fixture(tt); const signed = await f.client.grantRoute(f.request);
    const grant = JSON.parse(Buffer.from(signed.payload, 'base64').toString());
    const receipt = await f.localRota.retire({ boot_id: f.boot.boot_id, slot: 'worker-1', epoch: grant.epoch, task_id: f.lease.task_id, generation: f.lease.generation });
    assert.equal(receipt.in_flight, 0);
    await f.client.releaseRoute(receipt);
    await assert.rejects(f.localRota.apply(signed, { lease: f.lease, slot: 'worker-1', bootId: f.boot.boot_id }), { status: 409 });
  });
});
