import test from 'node:test';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {createServer} from 'node:http';
import {mkdtemp, readFile, writeFile, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomBytes, randomUUID, generateKeyPairSync} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {Queue} from 'bullmq';
import {fullCrawlRelayFixture} from './helpers/fullCrawlRelayFixture.js';
import {createRemoteNatsClient} from '../src/remoteNodes/natsClient.js';
import {createRemoteFullCrawlWorker} from '../src/remoteNodes/fullCrawlWorker.js';
import {RemoteResultSpool} from '../src/remoteNodes/spool.js';
import {currentChannelExecution} from '../src/channelExecutionContext.js';
import {LEGACY_FULL_CRAWL_FETCH_CONTRACT} from '../src/fullCrawlFetchContract.js';
import {fullCrawlFixture, channelSnapshot} from './helpers/remoteFullCrawlFixture.js';
import {resolveWorkerIdentityPolicy} from '../src/identityPolicyCatalog.js';
import {assertFullCrawlReleaseSchema, fullCrawlRollbackReadiness} from '../src/remoteNodes/fullCrawlReleaseSchema.js';
import {createFullCrawlDeploymentRuntime} from '../src/remoteNodes/fullCrawlDeploymentRuntime.js';

const url = process.env.REMOTE_NODE_TEST_DATABASE_URL;
const natsUrl = process.env.FULL_CRAWL_NATS_TEST_URL;
test('release entry starts the actual compatibility runtime, preserves default-off intake and closes all resources',
  {skip: !url || !natsUrl, timeout: 90000}, async t => {
  const policy = resolveWorkerIdentityPolicy({role: 'channel', policyId: 'qy-br-channel-anonymous-v1',
    expectedWorkloadScope: 'qy-production', environment: {}}).policy;
  const nodeId = 'a88a2231-3604-4f55-a815-08662ae85fd6';
  const token = 'full-p3-node-test-only-token-00000000000000';
  const pair = generateKeyPairSync('ed25519');
  let channelId;
  const relay = await fullCrawlRelayFixture(t, {nodeId, publicKey: pair.publicKey,
    observe: request => request.stage === 'admission' ? channelSnapshot(channelId) : {
      playlist_id: 'UUfixture', entries: [], activity_evidence_complete: true,
      scan: {complete: true, stop_reason: 'end', terminal_reason: 'end', pages: 1, inspected_count: 0, parse_gap_count: 0}}});
  const f = await fullCrawlFixture(t, {createAttempt: false, activate: false, identityPolicy: policy,
    nodeIdentity: {nodeId, token, relayBootId: (await relay.localRota.boot()).boot_id}});
  channelId = f.channelId;
  for (const file of ['youtubeSessionSchema.sql', 'natsSchema.sql', 'fullCrawlTransportSchema.sql']) {
    await f.query(await readFile(new URL(`../src/remoteNodes/${file}`, import.meta.url), 'utf8'));
  }
  await assertFullCrawlReleaseSchema(f.query);
  await f.query('ALTER TABLE remote_ingestion.full_crawl_stages DISABLE TRIGGER remote_full_stage_notify');
  try { await assert.rejects(assertFullCrawlReleaseSchema(f.query), /TRIGGERS_REQUIRED/); }
  finally { await f.query('ALTER TABLE remote_ingestion.full_crawl_stages ENABLE TRIGGER remote_full_stage_notify'); }
  const folder = await mkdtemp(join(tmpdir(), 'full-release-'));
  const children = [], commands = [];
  const assignments = new Map();
  let assignment;
  const rota = createServer(async (req, res) => {
    try {
      const chunks = []; for await (const bytes of req) chunks.push(bytes);
      const input = JSON.parse(Buffer.concat(chunks).toString() || '{}');
      commands.push(req.url);
      let result;
      if (req.url === '/claim') {
        assignment = {ok: true, ready: true, control_state: 'leased_idle', workload_scope: 'qy-production',
          protocol_version: 2, role: 'channel', worker_id: input.worker_id, worker_instance_id: input.worker_instance_id,
          slot_name: 'full-release-compatibility', proxy_user: 'fixture', lease_id: randomUUID(),
          lease_remaining_ms: 60000, server_time: new Date().toISOString(), route_generation: 1,
          credential_generation: 1, network_identity_key: 'full-release-network-' + input.worker_id, profile_epoch: 0,
          identity_policy_id: policy.id, identity_policy_version: policy.version, identity_policy_hash: policy.hash,
          identity_action: 'keep', egress_country: 'BR'};
        assignments.set(input.worker_id, assignment);
        result = assignment;
      } else if (req.url === '/renew') result = assignments.get(input.worker_id);
      else if (req.url === '/tasks/begin') result = {ok: true, task_id: randomUUID(), attempt_request_id: input.attempt_request_id,
        business_run_id: input.business_run_id, job_execution_id: input.job_execution_id, attempt_number: 1,
        slot_name: input.slot_name, route_generation: input.route_generation, started_at: new Date().toISOString()};
      else if (req.url === '/tasks/complete') result = {ok: true, task_completed: true, completion_request_id: input.completion_request_id,
        task_id: input.task_id, slot_name: input.slot_name, lease_id: input.lease_id, control_state: 'READY_KEEP_ROUTE',
        ready: true, completed_task_route_generation: input.route_generation};
      else if (req.url === '/internal/v1/remote-route') result = {...assignments.get(input.worker_id), ...input,
        server_time: new Date().toISOString(), lease_until: new Date(Date.now() + 60000).toISOString(), upstream: relay.upstream};
      else if (req.url === '/release') result = {ok: true, released: true, release_request_id: input.release_request_id,
        lease_id: input.lease_id, slot_name: input.slot_name, route_generation: input.known_route_generation,
        status: 'released', released_at: new Date().toISOString(), reason: input.reason};
      else throw new Error('unexpected route');
      res.setHeader('content-type', 'application/json'); res.end(JSON.stringify(result));
    } catch {res.statusCode = 500; res.end('{}');}
  });
  rota.listen(0, '127.0.0.1'); await once(rota, 'listening');
  t.after(async () => {
    for (const child of children) if (child.exitCode === null && child.signalCode === null) {child.kill('SIGKILL'); await once(child, 'exit');}
    await new Promise(resolve => rota.close(resolve));
    await rm(folder, {recursive: true, force: true});
  });
  const secrets = {REMOTE_NODE_ROUTE_PRIVATE_KEY_FILE: pair.privateKey.export({type: 'pkcs8', format: 'pem'}),
    REMOTE_NODE_ENCRYPTION_KEY_FILE: randomBytes(32).toString('hex'), REMOTE_NODE_ADMIN_TOKEN_FILE: randomBytes(32).toString('hex'),
    REMOTE_NODE_ROTA_TOKEN_FILE: randomBytes(32).toString('hex'), REMOTE_NODE_PROFILE_SECRET_FILE: randomBytes(32).toString('hex'),
    REMOTE_NODE_ROTA_CONTROL_TOKEN_FILE: randomBytes(32).toString('hex'),
    REMOTE_NODE_NATS_PASSWORD_FILE: 'full-p3-center-test-only-token-00000000000000'};
  const prefix = 'full-release-' + randomUUID();
  const env = {...process.env, DATABASE_URL: url, REMOTE_NODE_DATABASE_URL: url, SKIP_SCHEMA_MIGRATION: 'true',
    EXPECTED_CRAWLER_DATABASE: new URL(url).pathname.slice(1), FORBIDDEN_CRAWLER_DATABASE: 'business',
    REMOTE_NODE_EXECUTION_ENABLED: 'false', REMOTE_NODE_FULL_CRAWL_DEPLOYMENT_ENABLED: 'true',
    REMOTE_NODE_FULL_CRAWL_EXECUTION_ENABLED: 'true',
    REMOTE_NODE_QUEUE_PREFIX: prefix, BULLMQ_PREFIX: prefix,
    REMOTE_NODE_REDIS_URL: 'redis://redis:6379/0', REDIS_HOST: 'redis', REDIS_PORT: '6379', REDIS_PASSWORD: '',
    PROXY_SLOT_ROLE: 'channel', WORKER_QUEUES: 'youtube-channel-crawl', PROXY_WORKER_ID: 'full-release-' + randomUUID(),
    ROTA_PROXY_CONTROL_URL: `http://127.0.0.1:${rota.address().port}`, ROTA_PROXY_BASE_URL: 'http://127.0.0.1:9',
    ROTA_PROXY_CONTROL_TOKEN: secrets.REMOTE_NODE_ROTA_CONTROL_TOKEN_FILE, ROTA_BULLMQ_PROXY_PASSWORD: 'full-release-test-only',
    BROWSER_PROFILE_ENCRYPTION_KEY: secrets.REMOTE_NODE_PROFILE_SECRET_FILE,
    ROTA_IDENTITY_POLICY_ID: policy.id, ROTA_WORKLOAD_SCOPE_EXPECTED: 'qy-production', YOUTUBEJS_EXTRACTOR_MODE: 'full',
    YOUTUBEJS_VIDEO_API_BATCH_FALLBACK: 'true', REMOTE_NODE_NATS_URL: natsUrl,
    NODE_EXTRA_CA_CERTS: process.env.FULL_CRAWL_NATS_TEST_CA, REMOTE_NODE_NATS_AUTH_FILE: join(folder, 'auth.conf'),
    REMOTE_NODE_NATS_MAX_BYTES: String(32 * 1024 * 1024), REMOTE_NODE_CENTER_PORT: '0',
    REMOTE_NODE_ROTA_ROUTE_URL: `http://127.0.0.1:${rota.address().port}/internal/v1/remote-route`, REMOTE_NODE_GATEWAY_URL: 'https://fixture.invalid',
    REMOTE_NODE_COLLECT_IMAGE: 'fixture/incremental@sha256:' + 'a'.repeat(64),
    REMOTE_NODE_FULL_CRAWL_IMAGE: 'fixture/full@sha256:' + 'b'.repeat(64)};
  for (const [name, value] of Object.entries(secrets)) {
    env[name] = join(folder, name); await writeFile(env[name], value, {mode: 0o600});
  }
  const deploymentRuntime=createFullCrawlDeploymentRuntime({store:f.store,image:env.REMOTE_NODE_FULL_CRAWL_IMAGE,
    privateKey:pair.privateKey,secretKey:Buffer.from(secrets.REMOTE_NODE_ENCRYPTION_KEY_FILE,'hex'),readRotaRoute:()=>{throw new Error('fixture does not allocate routes');}});
  await f.query('INSERT INTO remote_ingestion.node_deployments(node_id,deployment_id,image,worker_count,credentials_cipher) VALUES($1,$2,$3,1,$4)',
    [f.nodeId,f.connection.deployment_id,env.REMOTE_NODE_FULL_CRAWL_IMAGE,
      deploymentRuntime.transport.routes.encrypt({nodeToken:token,relayTokens:{}},`node-deployment:${f.nodeId}`)]);
  const run = async (override, ready, beforeStop) => {
    const child = spawn(process.execPath, ['scripts/runRemoteNodeCenter.mjs'], {env: {...env, ...override}, stdio: ['ignore', 'pipe', 'pipe']});
    children.push(child); let output = '';
    child.stdout.on('data', bytes => {output += bytes;}); child.stderr.on('data', bytes => {output += bytes;});
    const exited = once(child, 'exit');
    for (let i = 0; i < 600 && child.exitCode === null && !output.includes('remote_node_center_listening'); i++) await delay(25);
    if (ready) {
      assert.match(output, /remote_node_center_listening/, output);
      try { await beforeStop?.(); } catch (error) {child.kill('SIGKILL'); await exited; error.message += '\n' + output.slice(-12000); throw error;}
      child.kill('SIGTERM');
    } else assert.doesNotMatch(output, /remote_node_center_listening/);
    const status = await Promise.race([exited, delay(10000).then(() => {throw new Error('entry did not exit: ' + output);})]);
    assert.equal(status[0], ready ? 0 : 1, output);
    for (const value of Object.values(secrets)) assert.ok(!output.includes(value), 'logs must not contain credentials');
    return output;
  };
  const before = (await f.query('SELECT count(*)::int AS n FROM remote_ingestion.tasks')).rows[0].n;
  const off = await run({REMOTE_NODE_FULL_CRAWL_EXECUTION_ENABLED: 'false'}, true);
  assert.doesNotMatch(off, /remote_full_crawl_execution_started/); assert.equal(commands.length, 0);
  await run({BULLMQ_PREFIX: 'wrong-prefix'}, false); assert.equal(commands.length, 0);
  const rivalGuard = await f.pool.connect();
  try {
    await rivalGuard.query('SELECT pg_advisory_lock(781138015,1)');
    await run({}, false);
    assert.equal(commands.length, 0, 'a competing release must not reserve another compatibility slot');
  } finally {await rivalGuard.query('SELECT pg_advisory_unlock(781138015,1)'); rivalGuard.release();}
  const enabled = await run({}, true);
  assert.match(enabled, /remote_full_crawl_execution_started/);
  assert.match(enabled, /"compatibility_slots":1,"remote_slots":null,"capacity_policy":"dynamic"/);
  assert.deepEqual(commands.filter(command => command !== '/renew'), ['/claim', '/release']);
  assert.equal((await f.query('SELECT count(*)::int AS n FROM remote_ingestion.tasks')).rows[0].n, before);
  assert.equal((await f.query('SELECT enabled,activation_requested FROM remote_ingestion.worker_connections WHERE node_id=$1', [f.nodeId])).rows[0].enabled, false);
  const connection = {host: 'redis', port: 6379, maxRetriesPerRequest: null};
  const queue = new Queue('youtube-channel-crawl', {connection, prefix});
  const finalize = new Queue('youtube-finalize', {connection, prefix});
  let client, timer, work;
  try {
    await run({}, true, async () => {
      client = await createRemoteNatsClient({url: natsUrl, token, nodeId, slot: f.slot,
        tls: {caFile: process.env.FULL_CRAWL_NATS_TEST_CA}});
      await f.query('UPDATE remote_ingestion.worker_connections SET activation_requested=true WHERE node_id=$1', [nodeId]);
      timer = setInterval(() => client.workerHeartbeat(f.connection).catch(() => {}), 500);
      const observation = input => {
        const context = currentChannelExecution();
        return context.fingerprint_gateway.fetch(context.profile_group.clients.youtubejs_chrome, input);
      };
      const worker = createRemoteFullCrawlWorker({client: {...client,
        claim: async (id, slot) => {
          const lease = await client.claim(id, slot, f.connection);
          return lease ? {...lease, connection: f.connection} : null;
        }}, localRota: relay.localRota, slot: f.slot, spool: new RemoteResultSpool({directory: join(folder, 'spool')}),
        gateway: relay.gateway, youtube: {acquire: async () => ({enabled: true}), release: async () => {}, close: async () => {},
          openChannel: () => observation({stage: 'admission'}), fetchUploads: () => observation({stage: 'uploads'}),
          fetchDetail: async () => assert.fail('empty channel has no details')}, pollMs: 20, timeoutMs: 30000});
      for (let i = 0; i < 200; i++) {
        if ((await client.workerHeartbeat(f.connection)).ready_for_tasks) break;
        await delay(50);
      }
      const job = await queue.add('channel-snapshot', f.job.data, {jobId: f.jobId, attempts: 1});
      work = (async () => {
        for (let i = 0; i < 600; i++) {const result = await worker.runOnce(); if (result !== 'idle') return result; await delay(25);}
        throw new Error('node did not receive work');
      })();
      work.catch(() => {});
      for (let i = 0; i < 1000; i++) {if (['completed', 'failed'].includes(await job.getState())) break; await delay(25);}
      const saved = await queue.getJob(job.id);
      assert.equal(await saved.getState(), 'completed', saved.failedReason);
      assert.equal(await work, 'closed');
      const candidate = (await f.query('SELECT status FROM crawler.channel_candidates WHERE candidate_id=$1', [f.candidateId])).rows[0];
      assert.equal(candidate.status, 'accepted');
      const tasks = (await f.query('SELECT state FROM remote_ingestion.tasks WHERE target_node_id=$1', [nodeId])).rows;
      assert.equal(tasks.length, 1); assert.equal(tasks[0].state, 'applied');
      assert.ok(await finalize.getWaitingCount() > 0, 'original finalize handoff must enqueue durable work');
      for (const name of ['channel-detail-repair', 'channel-checkpoint-repair', 'channel-snapshot']) {
        const invalid = await queue.add(name, {fetch_contract: LEGACY_FULL_CRAWL_FETCH_CONTRACT}, {attempts: 1});
        for (let i = 0; i < 300 && !['completed', 'failed'].includes(await invalid.getState()); i++) await delay(25);
        assert.equal(await invalid.getState(), 'failed', 'invalid compatibility jobs must use original validation');
        const events = (await f.query("SELECT status FROM crawler.task_events WHERE queue_name='youtube-channel-crawl' AND job_id=$1", [invalid.id])).rows;
        assert.ok(events.some(event => event.status === 'failed'), 'original failure lifecycle must persist the result');
      }
      assert.equal((await f.query('SELECT count(*)::int AS n FROM remote_ingestion.tasks WHERE target_node_id=$1', [nodeId])).rows[0].n, 1,
        'legacy and repair compatibility must never create remote snapshot tasks');
      await f.query('UPDATE remote_ingestion.worker_connections SET activation_requested=false WHERE node_id=$1', [nodeId]);
    });
  } finally {
    clearInterval(timer); await client?.close();
    await queue.obliterate({force: true}); await finalize.obliterate({force: true});
    await queue.close(); await finalize.close();
  }
  const preserved = (await f.query('SELECT count(*)::int AS n FROM remote_ingestion.worker_connections')).rows[0].n;
  await fullCrawlRollbackReadiness(f.pool);
  await f.query('UPDATE remote_ingestion.worker_connections SET activation_requested=true WHERE node_id=$1', [f.nodeId]);
  const blocked = await fullCrawlRollbackReadiness(f.pool);
  assert.ok(blocked.blockers.some(row => row.node_id === f.nodeId));
  await f.query('UPDATE remote_ingestion.worker_connections SET activation_requested=false WHERE node_id=$1', [f.nodeId]);
  assert.equal((await f.query('SELECT count(*)::int AS n FROM remote_ingestion.worker_connections')).rows[0].n, preserved);
});
