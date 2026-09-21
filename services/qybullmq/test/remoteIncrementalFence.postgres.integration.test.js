import { wholeChannelParts, decodeWholeChannelParts } from '../src/remoteNodes/wholeChannelProtocol.js';
import { encodeResult, hash } from '../src/remoteNodes/protocol.js';
import { createIncrementalVideoDispatchSnapshot, executeIncrementalYoutubeJsVideo, fetchIncrementalYoutubeJsVideoDetail } from '../src/incrementalYoutubeJsVideo.js';
import { planIncrementalVideoSnapshot } from '../src/incrementalVideoSnapshot.js';
import { runWithChannelExecution } from '../src/channelExecutionContext.js';
import { createVideoDetailApiFallback } from '../src/videoDetailApiFallback.js';
import { withVideoApiReplay } from '../src/videoApiContinuation.js';
import { completeVideoApiRequests } from '../src/videoApiBatchRequests.js';
import { WholeChannelStore } from '../src/remoteNodes/wholeChannelStore.js';
import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes, randomUUID } from 'node:crypto';
import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import pg from 'pg';
import {startRemoteNatsCenter} from '../src/remoteNodes/natsCenter.js';
import {createRemoteNatsClient} from '../src/remoteNodes/natsClient.js';
import {createTransportSignals} from '../src/remoteNodes/transportSignals.js';
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

function publicDetail(videoId) {
  return {
    id: videoId,
    title: `YouTubeJS ${videoId}`,
    thumbnail_url: "https://i.ytimg.com/vi/checkpoint/default.jpg",
    published_at: "2026-09-02T12:00:00.000Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "youtubejs_player",
    duration_seconds: 90,
    duration_source: "youtubejs_player",
    view_count: 321,
    view_count_text: "321",
    view_count_source: "youtubejs_player",
    like_count: 12,
    like_count_source: "youtubejs_player",
    comment_count: 0,
    comment_count_status: "exact",
    comment_count_source: "youtubejs_comments",
    comments_disabled: false,
    comments_first_page: {
      version: 1,
      total_count: 0,
      returned_count: 0,
      comments: [],
    },
    description: "Captured once",
    description_status: "exact",
    description_source: "youtubejs_player",
    description_observed: true,
    hashtags: ["checkpoint"],
    hashtags_observed: true,
    keywords: ["youtubejs"],
    keywords_observed: true,
    availability: "public",
    access_status: "public",
    access_status_source: "youtubejs_player",
    content_type_signals: {
      source: "youtubei_player",
      canonical_url: `https://www.youtube.com/watch?v=${videoId}`,
      is_shorts_eligible: false,
      is_live_content: false,
      is_live: false,
      is_upcoming: false,
      is_live_now: false,
    },
    extractor_version: "youtubei.js@test",
    source: "youtubejs_get_info",
  };
}

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

  await t.test('completed transaction releases coordinator instead of renewing a terminal task', async () => {
    const f = await checkpointFixture();
    const { coordinatorId } = await channelStore.coordinate(f.lease);
    await channelStore.transaction(f.lease, coordinatorId, assertRemoteIncrementalBusinessFence,
      client => channelStore.complete(client, f.lease, { status: 'done', run_id: f.prepared.businessRunId }));
    const row=(await query('SELECT state,coordinator_id,coordinator_until FROM remote_ingestion.tasks WHERE task_id=$1',[f.lease.task_id])).rows[0];
    assert.equal(row.state,'applied');assert.equal(row.coordinator_until,null);assert.equal(row.coordinator_id,null);
  });

  await t.test('terminal Plan releases only its quiesced historical API handoff without replaying collection',async()=>{
    const {settleTerminalRemoteHandoffs}=await import('../src/remoteNodes/centerExecutionRecovery.js');
    const f=await checkpointFixture();await f.finish();
    await profiles.finishAttempt(f.executionAttemptId,{status:'failed',error:new Error('API handoff')});
    await query("UPDATE feature_clock.daily_channel_plans SET status='failed',completed_at=now() WHERE plan_id=$1",[f.plan.plan_id]);
    await query("UPDATE remote_ingestion.tasks SET state='received',last_error='VIDEO_API_PENDING',applied_result=$2,coordinator_until=NULL WHERE task_id=$1",[f.lease.task_id,{request_id:'saved-api-evidence'}]);
    // A terminal business record does not authorize retiring an active network.
    await query("UPDATE remote_ingestion.network_bindings SET state='active' WHERE binding_id=$1",[f.binding.binding_id]);
    assert.equal(await settleTerminalRemoteHandoffs(store),0);
    await query("UPDATE remote_ingestion.network_bindings SET state='retired' WHERE binding_id=$1",[f.binding.binding_id]);
    await query("UPDATE remote_ingestion.tasks SET coordinator_until=now()+interval '60 seconds' WHERE task_id=$1",[f.lease.task_id]);
    assert.equal(await settleTerminalRemoteHandoffs(store),0);
    await query('UPDATE remote_ingestion.tasks SET coordinator_until=NULL WHERE task_id=$1',[f.lease.task_id]);
    assert.equal(await settleTerminalRemoteHandoffs(store),1);
    assert.equal(await settleTerminalRemoteHandoffs(store),0);
    const task=(await query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1',[f.lease.task_id])).rows[0];
    assert.equal(task.state,'failed');assert.equal(task.applied_result.request_id,'saved-api-evidence');
    assert.equal((await query('SELECT status FROM feature_clock.daily_channel_plans WHERE plan_id=$1',[f.plan.plan_id])).rows[0].status,'failed');
  });

  await t.test('finished historical coordinator does not block slot but unfinished and mismatched evidence does', async () => {
    const {remoteSlotUnsettled}=await import('../src/remoteNodes/centerExecutionRecovery.js');
    const f=await checkpointFixture();await f.finish();await profiles.finishAttempt(f.executionAttemptId,{status:'success'});
    const binding=(await query('SELECT node_id,slot FROM remote_ingestion.network_bindings WHERE binding_id=$1',[f.binding.binding_id])).rows[0];
    await query(`UPDATE remote_ingestion.tasks SET target_node_id=$2,target_worker_slot=$3,
      coordinator_id=$4,coordinator_until=now()+interval '60 seconds' WHERE task_id=$1`,[f.lease.task_id,binding.node_id,binding.slot,randomUUID()]);
    assert.equal(await remoteSlotUnsettled(pool,binding),false,'terminal history is not running work, even before old TTL expires');
    await profiles.beginAttempt({...f.attemptInput,task:{...f.attemptInput.task,task_id:randomUUID(),attempt_number:2}});
    assert.equal(await remoteSlotUnsettled(pool,binding),false,'a newer attempt must not trap an already finished historical record');
    await query("UPDATE remote_ingestion.network_bindings SET state='active' WHERE binding_id=$1",[f.binding.binding_id]);
    assert.equal(await remoteSlotUnsettled(pool,binding),true,'network still needs a quiescence receipt');
    await query("UPDATE remote_ingestion.network_bindings SET state='retired' WHERE binding_id=$1",[f.binding.binding_id]);
    await query("UPDATE crawler.channel_execution_attempts SET status='running',finished_at=NULL WHERE attempt_id=$1",[f.executionAttemptId]);
    assert.equal(await remoteSlotUnsettled(pool,binding),true,'applied result is not a finished execution');
    await query("UPDATE crawler.channel_execution_attempts SET status='success',finished_at=now(),business_run_id=$2 WHERE attempt_id=$1",[f.executionAttemptId,`mismatch:${f.executionAttemptId}`]);
    assert.equal(await remoteSlotUnsettled(pool,binding),true,'mismatched evidence remains quarantined');
  });

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

  async function transportClient(tt,nodeId,token,transport){
    if(transport.startsWith('nats')){
      let wholeChannels = null;
      if (transport === 'nats_whole') {
        await pool.query(await readFile(new URL('../src/remoteNodes/wholeChannelSchema.sql',import.meta.url),'utf8'));
        wholeChannels = new WholeChannelStore({channelPlans:channelStore,assertBusinessFence:assertRemoteIncrementalBusinessFence});
      }
      channelStore.testWholeChannels = wholeChannels;
      await pool.query(await readFile(new URL('../src/remoteNodes/natsSchema.sql',import.meta.url),'utf8'));
      const signals=await createTransportSignals({connectionString:url});channelStore.transportSignals=signals;
      const tls={caFile:process.env.REMOTE_NATS_TEST_CA};
      let center,client;
      tt.after(async()=>{await client?.close();await signals.close();await center?.close();delete channelStore.transportSignals;});
      center=await startRemoteNatsCenter({url:process.env.REMOTE_NATS_TEST_URL,password:process.env.REMOTE_NATS_TEST_PASSWORD,tls,
        store,channelPlans:channelStore,wholeChannels,signals,resultMaxBytes:32*1024*1024});
      client=await createRemoteNatsClient({url:process.env.REMOTE_NATS_TEST_URL,token,nodeId,slot:'incremental-1',tls});
      return client;
    }
    const gateway=createRemoteNodeGateway({store,channelPlans:channelStore});gateway.listen(0,'127.0.0.1');await once(gateway,'listening');
    tt.after(()=>new Promise(resolve=>{gateway.close(resolve);gateway.closeAllConnections();}));
    return createRemoteNodeClient({url:`http://127.0.0.1:${gateway.address().port}`,token,allowLoopbackHttp:true});
  }
  for(const transport of process.env.REMOTE_NATS_TEST_URL?['http','nats','nats_whole']:['http']) await t.test(`whole About Plan writes with the real business fence even if Feature completes before final run cleanup (${transport})`, async tt => {
    const f = await fixture(); await enqueueRemoteIncrementalJob(channelStore, f.job, f);
    const nodeId = transport.startsWith('nats')?process.env.REMOTE_NATS_TEST_NODE_ID:randomUUID(); const token = transport.startsWith('nats')?process.env.REMOTE_NATS_TEST_TOKEN:randomBytes(32).toString('hex');
    await store.registerNode({ nodeId, token, capabilities: [CHANNEL_PLAN_CAPABILITY] });
    const client = await transportClient(tt,nodeId,token,transport);
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
      runRemoteIncrementalPlan({ channelStore, lease, wholeChannels: transport === 'nats_whole' ? channelStore.testWholeChannels : null, assertBusinessFence: fence, pollMs: 5, signal: AbortSignal.timeout(15000) }), worker.runOnce(),
    ]);
    for (const result of results) if (result.status === 'rejected') throw result.reason;
    assert.equal(results[1].value, 'applied');
    assert.equal((await query('SELECT status FROM crawler.channel_runs WHERE run_id=$1', [f.prepared.businessRunId])).rows[0].status, 'done');
    assert.equal((await query('SELECT title FROM crawler.channels WHERE channel_id=$1', [f.channelId])).rows[0].title, 'Fenced About result');
  });
  for(const empty of [true,false]) for(const transport of process.env.REMOTE_NATS_TEST_URL?['http','nats','nats_whole']:['http']) await t.test(`Video completion remains writable when Feature observes its finalized checkpoint before Run cleanup (${transport}, empty=${empty})`, async tt => {
    const f = await fixture({ about: false, video: true, agent: false }); await enqueueRemoteIncrementalJob(channelStore, f.job, f);
    const nodeId = transport.startsWith('nats')?process.env.REMOTE_NATS_TEST_NODE_ID:randomUUID(); const token = transport.startsWith('nats')?process.env.REMOTE_NATS_TEST_TOKEN:randomBytes(32).toString('hex');
    await store.registerNode({ nodeId, token, capabilities: [CHANNEL_PLAN_CAPABILITY] });
    const client = await transportClient(tt,nodeId,token,transport);
    const claimId = randomUUID(); const lease = await client.claim(claimId);
    const directory = await mkdtemp(join(tmpdir(), 'remote-fence-')); tt.after(() => rm(directory, { recursive: true, force: true }));
    const spool = new RemoteResultSpool({ directory });
    const worker = new RemoteChannelPlanExecutor({ client: { ...client, claim: () => client.claim(claimId) }, spool,
      withSession: (_lease, _options, invoke) => invoke(), pollMs: 5, timeoutMs: 15000,
      youtube: { openChannel: async id => ({ scanUploads: async () => ({
        channel_id:id,playlist_id:'uploads',entries:empty?[]:[{id:'natsVideo01',video_id:'natsVideo01',position:1,title:'Captured once',published_at:'2026-09-02T12:00:00.000Z',published_at_status:'exact'}],pages:1,item_count:empty?0:1,parse_gap_count:0,
        first_page_item_count:empty?0:1,catch_up_item_count:0,anchor_matched:false,matched_anchor_id:null,
        crossed_anchor_ids:[],stop_reason:'list_end',terminal_reason:'list_end',complete:true,
        ...(empty?{empty_uploads:{version:1,outcome:'dormant',reason:'no_country',country:null}}:{})
      }) }), fetchDetail: id => {assert.equal(empty,false);return publicDetail(id);} } });
    const fence = async (sqlClient, task) => {
      // Simulate Feature consuming the actual committed Video Observation before
      // the next runner transaction. This must not fence its own completion.
      await sqlClient.query(`UPDATE feature_clock.daily_channel_plans p SET status='succeeded'
        WHERE p.plan_id=$1 AND EXISTS(SELECT 1 FROM crawler.crawl_observations o
          WHERE o.plan_id=p.plan_id AND o.observation_kind='video' AND o.outcome='complete')`, [f.plan.plan_id]);
      return assertRemoteIncrementalBusinessFence(sqlClient, task);
    };
    const results = await Promise.allSettled([
      runRemoteIncrementalPlan({ channelStore, lease, wholeChannels: transport === 'nats_whole' ? channelStore.testWholeChannels : null, assertBusinessFence: fence, pollMs: 5, signal: AbortSignal.timeout(15000) }), worker.runOnce(),
    ]);
    for (const result of results) if (result.status === 'rejected') throw result.reason;
    assert.equal(results[1].value, 'applied');
    if(!empty){const video=(await query('SELECT view_count,like_count,comment_count FROM crawler.contents WHERE channel_id=$1 AND source_content_id=$2',[f.channelId,'natsVideo01'])).rows[0];assert.deepEqual(Object.fromEntries(Object.entries(video).map(([k,v])=>[k,Number(v)])),{view_count:321,like_count:12,comment_count:0});}
    assert.equal((await query('SELECT status FROM crawler.channel_runs WHERE run_id=$1', [f.prepared.businessRunId])).rows[0].status, 'done');
    assert.equal((await query('SELECT status FROM feature_clock.daily_channel_plans WHERE plan_id=$1', [f.plan.plan_id])).rows[0].status, 'succeeded');
  });
  await t.test('dispatch snapshot includes feed-dated stored videos and reserves original pending repairs', async () => {
    const f = await fixture({about:false,video:true,agent:false});
    for (const id of ['undated','repair']) await query(`INSERT INTO crawler.contents(content_key,channel_id,source_content_id,content_type,published_at)
      VALUES($1,$2,$3,'video',$4)`, [`${f.channelId}:video:${id}`,f.channelId,id,id==='repair'?new Date():null]);
    await query(`INSERT INTO crawler.content_enrich_tasks(task_id,content_key,channel_id,job_type,status,next_retry_at)
      VALUES($1,$2,$3,'player-refresh','queued',now())`, [`repair:${f.plan.plan_id}`,`${f.channelId}:video:repair`,f.channelId]);
    const snapshot = await runWithChannelExecution({attempt_id:f.executionAttemptId}, () => store.transaction(client =>
      createIncrementalVideoDispatchSnapshot({client,plan:f.plan,runId:f.prepared.businessRunId}), {repeatableRead:true}));
    assert.ok(snapshot.knownVideoIds.includes('undated'));
    assert.equal(snapshot.recentRows.find(row=>row.source_content_id==='repair').checkpoint_content_enrich.fence.dispatch_generation,1);
    const scan={complete:true,entries:[{id:'undated',published_at:new Date().toISOString(),published_at_status:'exact',published_at_source:'youtubejs_feed'}, {id:'new',position:2}]};
    const targets=await planIncrementalVideoSnapshot(snapshot,scan);
    assert.ok(targets.items.some(item=>item.phase==='recent'&&item.video_id==='undated'));
    assert.ok(targets.items.some(item=>item.phase==='recent'&&item.video_id==='repair'));
    assert.deepEqual(targets.items.filter(item=>item.phase==='first_seen').map(item=>item.video_id),['new']);
  });

  if(process.env.REMOTE_NATS_TEST_URL) await t.test('30 video results use one channel command and a bounded number of fenced center transactions', async tt => {
    const f = await fixture({about:false,video:true,agent:false}); await enqueueRemoteIncrementalJob(channelStore,f.job,f);
    const nodeId=process.env.REMOTE_NATS_TEST_NODE_ID,token=process.env.REMOTE_NATS_TEST_TOKEN;
    await store.registerNode({nodeId,token,capabilities:[CHANNEL_PLAN_CAPABILITY]});
    const client=await transportClient(tt,nodeId,token,'nats_whole');
    const claimId=randomUUID(),lease=await client.claim(claimId);
    const directory=await mkdtemp(join(tmpdir(),'whole-many-'));tt.after(()=>rm(directory,{recursive:true,force:true}));
    const ids=Array.from({length:30},(_,i)=>`video${String(i).padStart(6,'0')}`);
    let inputReads=0,uploads=0,received=false,replayFences=0;
    const wholeChannels=channelStore.testWholeChannels;
    const load=wholeChannels.result.bind(wholeChannels);
    wholeChannels.result=async (...args)=>{const result=await load(...args);received=true;return result;};
    const worker=new RemoteChannelPlanExecutor({client:{...client,claim:()=>client.claim(claimId),
      wholeChannelInput:async(...args)=>{inputReads++;return client.wholeChannelInput(...args);},
      uploadWholeChannel:async(...args)=>{uploads++;return client.uploadWholeChannel(...args);}},spool:new RemoteResultSpool({directory}),
      withSession:(_lease,_options,invoke)=>invoke(),pollMs:5,timeoutMs:30000,
      youtube:{openChannel:async()=>({scanUploads:async()=>({entries:ids.map((id,i)=>({id,position:i+1})),complete:true,pages:1,stop_reason:'list_end',terminal_reason:'list_end'})}),
        fetchDetail:async id=>{assert.equal(inputReads,1);assert.equal(uploads,0);return publicDetail(id);}}});
    const results=await Promise.allSettled([runRemoteIncrementalPlan({channelStore,lease,wholeChannels,
      assertBusinessFence:async(c,task)=>{if(received)replayFences++;return assertRemoteIncrementalBusinessFence(c,task);},
      pollMs:5,signal:AbortSignal.timeout(30000)}),worker.runOnce()]);
    for(const result of results)if(result.status==='rejected')throw result.reason;
    assert.equal(results[1].value,'applied');
    assert.equal((await query('SELECT count(*)::int AS n FROM crawler.contents WHERE channel_id=$1',[f.channelId])).rows[0].n,30);
    assert.deepEqual((await query('SELECT operation FROM remote_ingestion.channel_commands WHERE task_id=$1',[lease.task_id])).rows,[{operation:'collect_channel'}]);
    assert.ok(replayFences<=10,`expected channel-level fences; got ${replayFences}`);
    tt.diagnostic(`30 videos: ${inputReads} input transfer, ${uploads} result transfer, ${replayFences} fenced transactions after receipt`);
  });

  if(process.env.REMOTE_NATS_TEST_URL) await t.test('autonomous API handoff persists shared API work and releases node before fallback completes', async tt => {
    const f=await fixture({about:false,video:true,agent:false}); await enqueueRemoteIncrementalJob(channelStore,f.job,f);
    const nodeId=process.env.REMOTE_NATS_TEST_NODE_ID,token=process.env.REMOTE_NATS_TEST_TOKEN;
    await store.registerNode({nodeId,token,capabilities:[CHANNEL_PLAN_CAPABILITY]});
    const client=await transportClient(tt,nodeId,token,'nats_whole');
    const claimId=randomUUID(),lease=await client.claim(claimId);
    const directory=await mkdtemp(join(tmpdir(),'whole-api-'));tt.after(()=>rm(directory,{recursive:true,force:true}));
    let details=0;
    const worker=new RemoteChannelPlanExecutor({client:{...client,claim:()=>client.claim(claimId)},spool:new RemoteResultSpool({directory}),
      withSession:(_lease,_options,invoke)=>invoke(),pollMs:5,timeoutMs:15000,
      youtube:{openChannel:async()=>({scanUploads:async()=>({entries:[{id:'missingViews',position:1},{id:'remaining',position:2}],complete:true,pages:1,stop_reason:'list_end',terminal_reason:'list_end'})}),
      fetchDetail:async()=>{details++;throw Object.assign(new Error('required view_count missing'),{name:'YoutubeJsRequiredSurfaceError',required_surface:'player',partial_detail:{id:'missingViews',like_count:12}});}}});
    const createApiFallback=options=>createVideoDetailApiFallback({...options,loadSettings:async()=>({fallbackMode:'emergency',apiKeys:['fixture'],dailyRequestLimit:100})});
    const results=await Promise.allSettled([runRemoteIncrementalPlan({channelStore,lease,wholeChannels:channelStore.testWholeChannels,
      loadWholeApiPolicy:async()=>({enabled:true,available:true,dailyRequestLimit:100}),createApiFallback,
      assertBusinessFence:assertRemoteIncrementalBusinessFence,pollMs:5,signal:AbortSignal.timeout(15000)}),worker.runOnce()]);
    assert.equal(results[0].status,'rejected');assert.equal(results[0].reason.code,'VIDEO_API_PENDING');
    assert.equal(results[1].status,'fulfilled');assert.equal(results[1].value,'waiting_central');
    assert.equal(details,3);
    const requests=(await query('SELECT * FROM crawler.youtube_api_detail_requests WHERE run_id=$1',[f.prepared.businessRunId])).rows;
    assert.equal(requests.length,1);assert.equal(requests[0].source_content_id,'missingViews');assert.equal(requests[0].status,'pending');
    assert.equal((await query("SELECT count(*)::int AS n FROM crawler.incremental_youtubejs_video_items WHERE run_id=$1 AND status='claimed'",[f.prepared.businessRunId])).rows[0].n,0);
    await query("UPDATE crawler.channel_execution_attempts SET status='failed',finished_at=now() WHERE attempt_id=$1",[f.executionAttemptId]);
    await transaction(c=>completeVideoApiRequests(c,requests[0].task_id,publicDetail('missingViews'),true));
    const replay=()=>executeIncrementalYoutubeJsVideo({plan:f.plan,runId:f.prepared.businessRunId,query,withTransaction:transaction,startedAt:new Date(),
      getChannelSnapshot:async()=>assert.fail('frozen scan must be reused'),
      fetchDetail:(id,options)=>fetchIncrementalYoutubeJsVideoDetail(id,{...options,videoApiFallback:createApiFallback({query,withTransaction:transaction}),fetchYoutubeJs:async()=>assert.fail('API replay must not call YouTube')})});
    await assert.rejects(withVideoApiReplay(()=>runWithChannelExecution({attempt_id:f.executionAttemptId},replay)),{code:'VIDEO_API_NETWORK_REQUIRED'});
    assert.equal((await query('SELECT status FROM crawler.incremental_youtubejs_video_items WHERE run_id=$1 AND video_id=$2',[f.prepared.businessRunId,'missingViews'])).rows[0].status,'captured');
    assert.equal(details,3);
  });

  if(process.env.REMOTE_NATS_TEST_URL) await t.test('whole-channel multipart delivery uses durable SQL receipts and rejects changed bytes after generation handoff', async tt => {
    const f=await fixture();await enqueueRemoteIncrementalJob(channelStore,f.job,f);
    const nodeId=process.env.REMOTE_NATS_TEST_NODE_ID,token=process.env.REMOTE_NATS_TEST_TOKEN;
    await store.registerNode({nodeId,token,capabilities:[CHANNEL_PLAN_CAPABILITY]});
    const client=await transportClient(tt,nodeId,token,'nats_whole');
    const lease=await client.claim(randomUUID());
    const input={version:1,plan:f.plan,generation:lease.generation};
    const commandId=await transaction(async c=>{
      const task=await channelStore.lock(c,lease);await assertRemoteIncrementalBusinessFence(c,task);
      return channelStore.testWholeChannels.prepare(c,task,input);
    });
    const incoming=await client.wholeChannelInput(lease,commandId,0);
    assert.deepEqual(decodeWholeChannelParts(incoming.manifest,[incoming.chunk]),input);
    const result={version:1,plan_id:f.plan.plan_id,channel_id:f.channelId,generation:lease.generation,
      input_sha256:incoming.manifest.sha256,about:{raw:{body:'多字节'.repeat(300000)}},items:[]};
    const {manifest,parts}=wholeChannelParts(result);assert.ok(manifest.bytes>2*1024*1024);
    const frame=chunk=>encodeResult({version:1,generation:lease.generation,batch_id:commandId,command_id:commandId,
      outcome:'success',data:{input_sha256:incoming.manifest.sha256,manifest,chunk}});
    const first=await frame(parts.at(-1));
    assert.equal((await client.uploadWholeChannel(lease,first)).complete,false);
    assert.equal((await query('SELECT received_at FROM remote_ingestion.whole_channel_inputs WHERE command_id=$1',[commandId])).rows[0].received_at,null);
    assert.equal((await client.uploadWholeChannel(lease,first)).complete,false);
    for (const part of parts.slice(0,-1)) await client.uploadWholeChannel(lease,await frame(part));
    const restored=new WholeChannelStore({channelPlans:channelStore,assertBusinessFence:assertRemoteIncrementalBusinessFence});
    const received=await transaction(c=>restored.result(c,commandId));assert.deepEqual(received.result,result);
    assert.equal((await query('SELECT count(*)::int AS n FROM remote_ingestion.whole_channel_chunks WHERE command_id=$1',[commandId])).rows[0].n,parts.length);
    await query("UPDATE remote_ingestion.tasks SET state='pending',node_id=NULL,generation=generation+1 WHERE task_id=$1",[lease.task_id]);
    assert.equal((await restored.receive(nodeId,{task_id:lease.task_id},first)).complete,true);
    const bytes=Buffer.from(parts[0].data,'base64');bytes[0]^=1;
    await assert.rejects(restored.receive(nodeId,{task_id:lease.task_id},await frame({...parts[0],data:bytes.toString('base64'),sha256:hash(bytes)})),{code:'WHOLE_CHANNEL_RESULT_CONFLICT'});
    assert.equal(await restored.pruneApplied(),0,'pending recovery retains its full payload');
    await query("UPDATE remote_ingestion.tasks SET state='applied',applied_at=now() WHERE task_id=$1",[lease.task_id]);
    assert.equal(await restored.pruneApplied(),0,'recently applied evidence remains available');
    await query("UPDATE remote_ingestion.tasks SET applied_at=now()-interval '8 days' WHERE task_id=$1",[lease.task_id]);
    assert.equal(await restored.pruneApplied(),1);
    assert.equal((await query('SELECT count(*)::int AS n FROM remote_ingestion.whole_channel_chunks WHERE command_id=$1 AND payload IS NOT NULL',[commandId])).rows[0].n,0);
    assert.equal((await restored.receive(nodeId,{task_id:lease.task_id},first)).complete,true,'hash receipts survive payload cleanup');
    await assert.rejects(transaction(c=>restored.result(c,commandId)),{code:'WHOLE_CHANNEL_RESULT_ARCHIVED'});
    assert.equal(await restored.pruneApplied(),0,'payload cleanup is idempotent');
  });

});
