import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes, randomUUID } from 'node:crypto';
import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import pg from 'pg';
import { RemoteNodeStore } from '../src/remoteNodes/store.js';
import { RemoteChannelPlanStore } from '../src/remoteNodes/channelPlanStore.js';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';
import { assertRemoteIncrementalBusinessFence, validateRemoteIncrementalAdmission, enqueueRemoteIncrementalJob } from '../src/remoteNodes/incrementalBusinessFence.js';
import { incrementalPlanHash, INCREMENTAL_JOB_NAME, INCREMENTAL_QUEUE } from '../src/incrementalPlan.js';
import { IncrementalRunStore } from '../src/incrementalRunStore.js';
import { ProxyBusinessRunPreparer } from '../src/proxyBusinessRun.js';
import { BrowserProfileStore } from '../src/browserProfileStore.js';
import { resolveWorkerIdentityPolicy } from '../src/identityPolicyCatalog.js';
import { PUBLICATION_WRITER_VERSION } from '../src/publicationWriterVersion.js';
import { createRemoteNodeGateway } from '../src/remoteNodes/gateway.js';
import { createRemoteNodeClient } from '../src/remoteNodes/client.js';
import { RemoteChannelPlanExecutor } from '../src/remoteNodes/channelPlanExecutor.js';
import { RemoteResultSpool } from '../src/remoteNodes/spool.js';
import { runRemoteIncrementalPlan } from '../src/remoteNodes/incrementalCoordinator.js';
import { CHANNEL_PLAN_CAPABILITY } from '../src/remoteNodes/channelPlanContract.js';
import { RemoteChannelRouteStore } from '../src/remoteNodes/channelRouteStore.js';
import { RemoteYoutubeSessionStore } from '../src/remoteNodes/youtubeSessionStore.js';
import { createRemoteYoutubeCheckpointConsumer } from '../src/remoteNodes/youtubeProfileCheckpoint.js';

const url = process.env.REMOTE_NODE_TEST_DATABASE_URL;
test('remote work checks original Clock, managed run and execution records', { skip: !url, timeout: 120000 }, async t => {
  const pool = new pg.Pool({ connectionString: url, max: 6, options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}` });
  const guard = await pool.connect();
  t.after(async () => { guard.release(); await pool.end(); });
  await assertIsolatedRemoteDatabase(pool);
  await guard.query('SELECT pg_advisory_lock(781137981)');
  for (const path of ['../src/schema.sql', '../../feature-engine/sql/schema.sql', '../src/remoteNodes/schema.sql', '../src/remoteNodes/routeSchema.sql', '../src/remoteNodes/youtubeSessionSchema.sql']) {
    await pool.query(await readFile(new URL(path, import.meta.url), 'utf8'));
  }
  const store = new RemoteNodeStore({ pool }); const channelStore = new RemoteChannelPlanStore({ store });
  const transaction = action => store.transaction(action);
  const query = pool.query.bind(pool);
  const resolvedPolicy = resolveWorkerIdentityPolicy({ role: 'channel', policyId: 'qy-br-channel-anonymous-v1', expectedWorkloadScope: 'qy-production', environment: {} });
  const preparer = new ProxyBusinessRunPreparer({ queryFn: query, withTransaction: transaction, resolvedPolicy,
    incrementalRunStore: new IncrementalRunStore({ withTransaction: transaction }) });
  const profileSecret = randomBytes(32).toString('hex');
  const profiles = new BrowserProfileStore({ queryFn: query, transactionFn: transaction, secret: profileSecret });

  async function fixture(mask = { about: true, video: false, agent: false }) {
    await query('TRUNCATE remote_ingestion.nodes,remote_ingestion.tasks CASCADE');
    const channelId = `UC${randomUUID().replaceAll('-', '').slice(0, 22)}`;
    await query("INSERT INTO crawler.channels(channel_id,channel_url,title,status) VALUES($1,$2,'Before fenced capture','active')", [channelId, `https://www.youtube.com/channel/${channelId}`]);
    const planId = randomUUID(); const scheduled = new Date().toISOString().slice(0, 10) + 'T01:00:00.000Z';
    const plan = { schema_version: 5, dispatch_generation: 1, job_id: `remote_fence_${planId}`, plan_id: planId,
      plan_mode: 'standard', plan_day: scheduled.slice(0, 10), scheduled_at: scheduled, channel_id: channelId, task_mask: mask,
      capacity: { factor: 1, player_cap: 20, next_cap: 8, version: 'capacity-1' }, clock_version: 7,
      policy_version: 'v16-rule-1', planner_config_version: 'video-plan-1' };
    const job = { id: plan.job_id, name: INCREMENTAL_JOB_NAME, queueName: INCREMENTAL_QUEUE, data: plan, attemptsStarted: 1, attemptsMade: 0 };
    await query(`INSERT INTO feature_clock.daily_channel_plans(plan_id,plan_day,channel_id,due_day,due_at,eligible_at,scheduled_at,
      run_about,run_video,run_agent,dispatch_slot,capacity_factor,player_cap,next_cap,source_clock_version,
      policy_version,planner_config_version,capacity_version,status)
      VALUES($1,$2,$3,$2,$4,$4,$4,$5,$6,$7,0,1,20,8,7,$8,$9,$10,'dispatched')`,
    [planId, plan.plan_day, channelId, scheduled, mask.about, mask.video, mask.agent, plan.policy_version, plan.planner_config_version, plan.capacity.version]);
    await query(`INSERT INTO feature_clock.dispatch_outbox(dispatch_event_id,plan_id,job_id,queue_name,payload_json,payload_hash,status)
      VALUES($1,$2,$3,$4,$5,$6,'published')`, [randomUUID(), planId, job.id, job.queueName, plan, incrementalPlanHash(plan)]);
    const prepared = await preparer.prepareChannel(job);
    const proxy = { workload_scope: 'qy-production', worker_id: `test-${planId}`, worker_instance_id: 'instance-1', slot_name: `slot-${planId}`,
      lease_id: randomUUID(), route_generation: 1, network_identity_key: `network-${planId}`, profile_epoch: 0,
      identity_policy_id: resolvedPolicy.policy.id, identity_policy_version: resolvedPolicy.policy.version };
    const profileGroup = await profiles.loadOrCreate({ identityPolicyId: proxy.identity_policy_id, identityPolicyVersion: proxy.identity_policy_version,
      networkIdentityKey: proxy.network_identity_key, profileEpoch: 0, language: 'pt', country: 'BR', timezone: 'America/Sao_Paulo' });
    const attemptInput = { channelId, runId: prepared.businessRunId, queueName: job.queueName, jobId: job.id, jobAttempt: 0,
      dispatchGeneration: 1, workerId: proxy.worker_id, proxy, profileGroup, prepared,
      task: { task_id: randomUUID(), business_run_id: prepared.businessRunId, attempt_number: 1 } };
    const executionAttemptId = await profiles.beginAttempt(attemptInput);
    const task = { capability: CHANNEL_PLAN_CAPABILITY, input: { plan }, context: { plan_hash: incrementalPlanHash(plan), execution_attempt_id: executionAttemptId } };
    return { plan, job, prepared, executionAttemptId, task, attemptInput, channelId };
  }
  const check = f => transaction(client => assertRemoteIncrementalBusinessFence(client, f.task));

  async function checkpointFixture(mask, { verifyProfileGuard = false } = {}) {
    const f = await fixture(mask); await enqueueRemoteIncrementalJob(channelStore, f.job, f);
    const nodeId = randomUUID(); const token = randomBytes(32).toString('hex');
    await store.registerNode({ nodeId, token, capabilities: [CHANNEL_PLAN_CAPABILITY] });
    const source = { ...f.attemptInput.proxy, identity_policy_hash: resolvedPolicy.policy.hash, credential_generation: 1, egress_country: 'BR',
      upstream: { protocol: 'http', address: '127.0.0.1:9', username: 'test', password: 'not-used' } };
    const routes = new RemoteChannelRouteStore({ channelStore, assertBusinessFence: assertRemoteIncrementalBusinessFence,
      privateKey: generateKeyPairSync('ed25519').privateKey, secretKey: randomBytes(32),
      readRotaRoute: async fence => ({ ...source, ...fence, route_lease_until_ms: Date.now() + 60000 }) });
    await routes.registerSlot(nodeId, 'worker-1', source.worker_id);
    const lease = await store.claim(nodeId, randomUUID(), 'worker-1');
    const sessions = new RemoteYoutubeSessionStore({ routes });
    const binding = await sessions.bind({ nodeId, lease, slot: 'worker-1', profileGroup: f.attemptInput.profileGroup,
      attemptId: f.executionAttemptId, rotaFence: { ...source, task_id: f.attemptInput.task.task_id,
        business_run_id: f.prepared.businessRunId, job_execution_id: `execution-${f.plan.plan_id}` } });
    if (verifyProfileGuard) await assert.rejects(sessions.prepare(binding.binding_id, { attemptId: f.executionAttemptId,
      profileGroup: { ...f.attemptInput.profileGroup, profile_group_id: 'another-browser-group' } }), { code: 'YOUTUBE_SESSION_PROFILE_MISMATCH' });
    const bootId = 'a'.repeat(48);
    const grant = JSON.parse(Buffer.from((await routes.grant(nodeId, { task_id: lease.task_id, generation: lease.generation,
      slot: 'worker-1', boot_id: bootId, action: 'activate', request_id: randomUUID() })).payload, 'base64').toString());
    const request = { task_id: lease.task_id, generation: lease.generation, slot: 'worker-1', boot_id: bootId,
      epoch: grant.epoch, route_id: binding.binding_id, identity_id: grant.identity_id };
    const checkpoint = { status: 'success', cookies: { cookies: [{ name: 'VISITOR_INFO1_LIVE', value: 'checkpoint-cookie', domain: '.youtube.com', path: '/' }] },
      metrics: { request_count: 1, failure_count: 0, duration_ms: 10, by_engine: {}, failure_evidence: [] }, active_managed_requests: 0 };
    await sessions.checkpoint(nodeId, { request, checkpoint });
    // This fixture models the zero-connection receipt; real Go quiescence is
    // exercised separately in remoteChannelNetwork integration tests.
    await routes.release(nodeId, { task_id: lease.task_id, generation: lease.generation, slot: 'worker-1', boot_id: bootId,
      epoch: grant.epoch, retired: true, in_flight: 0 });
    const finish = () => transaction(async client => {
      const runs = new IncrementalRunStore({ withTransaction: action => action(client) });
      for (const [domain, enabled] of Object.entries(f.plan.task_mask)) if (enabled) await runs.markDomain(f.prepared.businessRunId, domain, domain === 'agent' ? 'queued' : 'complete');
      const run = await runs.finish(f.prepared.businessRunId, { waitingForAgent: f.plan.task_mask.agent });
      const task = (await client.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE', [lease.task_id])).rows[0];
      await channelStore.complete(client, task, { run_id: run.run_id, status: run.status });
    });
    const readCookies = async () => (await profiles.loadOrCreate({ identityPolicyId: source.identity_policy_id,
      identityPolicyVersion: source.identity_policy_version, networkIdentityKey: source.network_identity_key,
      profileEpoch: 0, language: 'pt', country: 'BR', timezone: 'America/Sao_Paulo' })).clients.youtubejs_chrome.cookie_state;
    return { ...f, lease, binding, sessions, finish, readCookies, consumer: createRemoteYoutubeCheckpointConsumer({ sessions, profileSecret }) };
  }

  await t.test('browser checkpoint uses original encryption and applies once after our own run completion', async () => {
    const f = await checkpointFixture(undefined, { verifyProfileGuard: true });
    assert.equal((await f.consumer.apply(f.binding.binding_id)).applied, false, 'receipt cannot preempt Plan completion');
    assert.deepEqual(await f.readCookies(), { cookies: [] });
    await f.finish();
    await assert.rejects(transaction(async client => assertRemoteIncrementalBusinessFence(client,
      (await client.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1', [f.lease.task_id])).rows[0])), { code: 'INCREMENTAL_BUSINESS_FENCE_STALE' }, 'ordinary collector writes remain closed');
    assert.deepEqual(await f.consumer.apply(f.binding.binding_id), { applied: true, replay: false });
    assert.equal((await f.readCookies()).cookies[0].value, 'checkpoint-cookie');
    await profiles.finishAttempt(f.executionAttemptId, { status: 'success' });
    assert.deepEqual(await f.consumer.apply(f.binding.binding_id), { applied: true, replay: true });
  });

  await t.test('waiting Agent is still a completed collection for browser checkpoint purposes', async () => {
    const f = await checkpointFixture({ about: true, video: false, agent: true }); await f.finish();
    assert.equal((await f.consumer.apply(f.binding.binding_id)).applied, true);
  });

  await t.test('newer execution cannot be overwritten by an older successful browser checkpoint', async () => {
    const f = await checkpointFixture(); await f.finish();
    await profiles.beginAttempt({ ...f.attemptInput, task: { ...f.attemptInput.task, task_id: randomUUID(), attempt_number: 2 } });
    await assert.rejects(f.consumer.apply(f.binding.binding_id), { code: 'INCREMENTAL_BUSINESS_FENCE_STALE' });
    assert.deepEqual(await f.readCookies(), { cookies: [] });
    assert.equal((await query('SELECT profile_applied_at FROM remote_ingestion.youtube_sessions WHERE binding_id=$1', [f.binding.binding_id])).rows[0].profile_applied_at, null);
  });

  await t.test('original preparer and profile attempt can admit the unchanged Plan exactly once', async () => {
    const f = await fixture();
    const first = await enqueueRemoteIncrementalJob(channelStore, f.job, f);
    const again = await enqueueRemoteIncrementalJob(channelStore, f.job, f);
    assert.equal(first.taskId, again.taskId);
    assert.equal((await check(f)).executionAttemptId, f.executionAttemptId);
    assert.equal((await query('SELECT count(*)::int AS n FROM remote_ingestion.tasks')).rows[0].n, 1);
  });

  for (const [name, sql, values] of [
    ['cancelled Clock', "UPDATE feature_clock.daily_channel_plans SET status='cancelled',error_code='manual_cancel' WHERE plan_id=$1", f => [f.plan.plan_id]],
    ['changed task mask', 'UPDATE feature_clock.daily_channel_plans SET run_video=true WHERE plan_id=$1', f => [f.plan.plan_id]],
    ['changed dispatch generation', "UPDATE feature_clock.dispatch_outbox SET payload_json=jsonb_set(payload_json,'{dispatch_generation}','2') WHERE plan_id=$1", f => [f.plan.plan_id]],
    ['stopped attempt', "UPDATE crawler.channel_execution_attempts SET status='aborted' WHERE attempt_id=$1", f => [f.executionAttemptId]],
    ['wrong channel owner', 'UPDATE crawler.channel_execution_attempts SET channel_id=$2 WHERE attempt_id=$1', f => [f.executionAttemptId, 'UCother']],
    ['terminated business run', "UPDATE crawler.business_run_bindings SET status='terminal',terminal_reason='test_cancel' WHERE business_run_id=$1", f => [f.prepared.businessRunId]],
    ['completed run', "UPDATE crawler.channel_runs SET status='done' WHERE run_id=$1", f => [f.prepared.businessRunId]],
    ['changed run identity', "UPDATE crawler.channel_runs SET identity_policy_hash='changed' WHERE run_id=$1", f => [f.prepared.businessRunId]],
  ]) await t.test(`${name} cannot admit or write remote work`, async () => {
    const f = await fixture(); await query(sql, values(f));
    await assert.rejects(enqueueRemoteIncrementalJob(channelStore, f.job, f), { code: 'INCREMENTAL_BUSINESS_FENCE_STALE' });
    await assert.rejects(check(f), { code: 'INCREMENTAL_BUSINESS_FENCE_STALE' });
    assert.equal((await query('SELECT count(*)::int AS n FROM remote_ingestion.tasks')).rows[0].n, 0);
  });

  await t.test('a newer original Rota attempt prevents an old running attempt from writing', async () => {
    const f = await fixture();
    await profiles.beginAttempt({ ...f.attemptInput, task: { ...f.attemptInput.task, task_id: randomUUID(), attempt_number: 2 } });
    await assert.rejects(check(f), { code: 'INCREMENTAL_BUSINESS_FENCE_STALE' });
  });

  await t.test('admission and remote insert roll back together', async () => {
    const f = await fixture();
    await assert.rejects(transaction(async client => {
      await channelStore.enqueue(f.job, { ...f, client });
      await validateRemoteIncrementalAdmission(client, f.job, f);
      throw new Error('after insert');
    }), /after insert/);
    assert.equal((await query('SELECT count(*)::int AS n FROM remote_ingestion.tasks')).rows[0].n, 0);
  });

  await t.test('concurrent admission retries and protected writes use the same lock order', async () => {
    const f = await fixture(); const queued = await enqueueRemoteIncrementalJob(channelStore, f.job, f);
    const writes = Array.from({ length: 4 }, () => transaction(async client => {
      const task = (await client.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE', [queued.taskId])).rows[0];
      await assertRemoteIncrementalBusinessFence(client, task);
    }));
    const admissions = Array.from({ length: 4 }, () => enqueueRemoteIncrementalJob(channelStore, f.job, f));
    const results = await Promise.allSettled([...writes, ...admissions]);
    for (const result of results) if (result.status === 'rejected') throw result.reason;
    for (const result of results.slice(4)) assert.equal(result.value.taskId, queued.taskId);
  });

  await t.test('own committed dormant result may finish, but an unrelated cancellation cannot', async () => {
    const f = await fixture({ about: false, video: true, agent: false });
    await query("UPDATE feature_clock.daily_channel_plans SET status='cancelled',error_code='channel_dormant' WHERE plan_id=$1", [f.plan.plan_id]);
    await assert.rejects(check(f), { code: 'INCREMENTAL_BUSINESS_FENCE_STALE' });
    await query("UPDATE crawler.channel_runs SET result_json=jsonb_set(result_json,'{domains,video}',$2::jsonb) WHERE run_id=$1",
      [f.prepared.businessRunId, { status: 'complete', lifecycle_status: 'dormant' }]);
    await check(f);
    await assert.rejects(enqueueRemoteIncrementalJob(channelStore, f.job, f), { code: 'INCREMENTAL_BUSINESS_FENCE_STALE' });
  });

  await t.test('whole About Plan writes with the real business fence even if Feature completes before final run cleanup', async tt => {
    const f = await fixture(); await enqueueRemoteIncrementalJob(channelStore, f.job, f);
    const nodeId = randomUUID(); const token = randomBytes(32).toString('hex');
    await store.registerNode({ nodeId, token, capabilities: [CHANNEL_PLAN_CAPABILITY] });
    const gateway = createRemoteNodeGateway({ store, channelPlans: channelStore });
    gateway.listen(0, '127.0.0.1'); await once(gateway, 'listening');
    tt.after(() => new Promise(resolve => { gateway.close(resolve); gateway.closeAllConnections(); }));
    const client = createRemoteNodeClient({ url: `http://127.0.0.1:${gateway.address().port}`, token, allowLoopbackHttp: true });
    const claimId = randomUUID(); const lease = await client.claim(claimId);
    const directory = await mkdtemp(join(tmpdir(), 'remote-fence-')); tt.after(() => rm(directory, { recursive: true, force: true }));
    const spool = new RemoteResultSpool({ directory });
    const worker = new RemoteChannelPlanExecutor({ client: { ...client, claim: () => client.claim(claimId) }, spool,
      withSession: (_lease, _options, invoke) => invoke(), pollMs: 5, timeoutMs: 15000,
      youtube: { openChannel: async id => ({ about_requested: true, about_observed: true,
        metadata: { channel_id: id, title: 'Fenced About result', subscriber_count_text: '1,234 subscribers', subscriber_count_source: 'youtube_about',
          view_count_text: '98,765 views', view_count_source: 'youtube_about', video_count_text: '42 videos', video_count_source: 'youtube_about',
          external_links: [], external_links_status: 'observed', keywords: [], available_tabs: ['videos'] }, raw: { engine: 'fixture' } }),
      fetchDetail: () => assert.fail('About-only') } });
    const fence = async (sqlClient, task) => {
      // Simulate Feature ingestion observing our committed About before final
      // completion. No synthetic status is allowed before domain evidence exists.
      await sqlClient.query(`UPDATE feature_clock.daily_channel_plans p SET status='succeeded'
        FROM crawler.channel_runs r WHERE p.plan_id=$1 AND r.plan_id=p.plan_id
        AND r.result_json#>>'{domains,about,status}'='complete'`, [f.plan.plan_id]);
      return assertRemoteIncrementalBusinessFence(sqlClient, task);
    };
    const results = await Promise.allSettled([
      runRemoteIncrementalPlan({ channelStore, lease, assertBusinessFence: fence, pollMs: 5, signal: AbortSignal.timeout(15000) }), worker.runOnce(),
    ]);
    for (const result of results) if (result.status === 'rejected') throw result.reason;
    assert.equal(results[1].value, 'applied');
    assert.equal((await query('SELECT status FROM crawler.channel_runs WHERE run_id=$1', [f.prepared.businessRunId])).rows[0].status, 'done');
    assert.equal((await query('SELECT title FROM crawler.channels WHERE channel_id=$1', [f.channelId])).rows[0].title, 'Fenced About result');
  });
});
