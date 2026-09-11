import assert from 'node:assert/strict';
import { randomBytes, randomUUID } from 'node:crypto';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import pg from 'pg';
import { RemoteNodeStore } from '../src/remoteNodes/store.js';
import { RemoteChannelPlanStore } from '../src/remoteNodes/channelPlanStore.js';
import { createRemoteNodeGateway } from '../src/remoteNodes/gateway.js';
import { createRemoteNodeClient } from '../src/remoteNodes/client.js';
import { RemoteChannelPlanExecutor } from '../src/remoteNodes/channelPlanExecutor.js';
import { RemoteResultSpool } from '../src/remoteNodes/spool.js';
import { runRemoteIncrementalPlan } from '../src/remoteNodes/incrementalCoordinator.js';
import { CHANNEL_PLAN_CAPABILITY } from '../src/remoteNodes/channelPlanContract.js';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';
import { encodeResult, RemoteProtocolError } from '../src/remoteNodes/protocol.js';
import { toChannelWire, fromChannelWire } from '../src/remoteNodes/channelWire.js';
import { selectYoutubeFailure } from '../src/youtubeFailurePolicy.js';
import { createVideoDetailApiFallback } from '../src/videoDetailApiFallback.js';
import { waitForVideoApiDetail } from '../src/videoApiBatchRequests.js';
import { incrementalPlanHash, INCREMENTAL_JOB_NAME, INCREMENTAL_QUEUE } from '../src/incrementalPlan.js';
import { PUBLICATION_WRITER_VERSION } from '../src/publicationWriterVersion.js';

const url = process.env.REMOTE_NODE_TEST_DATABASE_URL;

function job(channelId, mask = { about: true }) {
  const planId = randomUUID();
  const now = new Date().toISOString();
  const data = { schema_version: 5, dispatch_generation: 1, job_id: `incremental__remote__${planId}`,
    plan_id: planId, plan_mode: 'standard', plan_day: now.slice(0, 10), scheduled_at: now,
    channel_id: channelId, task_mask: { about: false, video: false, agent: false, ...mask },
    capacity: { factor: 1, player_cap: 20, next_cap: 8, version: 'capacity-1' },
    clock_version: 7, policy_version: 'v16-rule-1', planner_config_version: 'video-plan-1' };
  return { id: data.job_id, name: INCREMENTAL_JOB_NAME, queueName: INCREMENTAL_QUEUE, data };
}

function detail(id) {
  return { id, title: 'Remote video fixture', thumbnail_url: `https://i.ytimg.com/vi/${id}/default.jpg`,
    published_at: new Date().toISOString(), published_at_status: 'exact', published_at_precision: 'second',
    published_at_source: 'youtubejs_player', duration_seconds: 90, duration_source: 'youtubejs_player',
    view_count: 321, view_count_text: '321', view_count_source: 'youtubejs_player',
    like_count: 12, like_count_source: 'youtubejs_player', comment_count: 0, comment_count_status: 'exact',
    comment_count_source: 'youtubejs_comments', comments_disabled: true,
    comments_first_page: { version: 1, total_count: 0, returned_count: 0, comments: [] },
    description: 'Remote captured evidence', description_status: 'exact', description_source: 'youtubejs_player',
    description_observed: true, hashtags: [], hashtags_observed: true, keywords: ['test'], keywords_observed: true,
    availability: 'public', access_status: 'public', access_status_source: 'youtubejs_player',
    content_type_signals: { source: 'youtubei_player', canonical_url: `https://www.youtube.com/watch?v=${id}`,
      is_shorts_eligible: false, is_live_content: false, is_live: false, is_upcoming: false, is_live_now: false },
    extractor_version: 'youtubei.js@test', source: 'youtubejs_get_info' };
}

test('one existing clock Plan per remote channel owner', { skip: !url, timeout: 120000 }, async (t) => {
  const pool = new pg.Pool({ connectionString: url, max: 6, options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}` });
  const guard = await pool.connect();
  t.after(async () => { guard.release(); await pool.end(); });
  await assertIsolatedRemoteDatabase(pool);
  await guard.query('SELECT pg_advisory_lock(781137981)');
  await pool.query(await readFile(new URL('../src/schema.sql', import.meta.url), 'utf8'));
  await pool.query(await readFile(new URL('../src/remoteNodes/schema.sql', import.meta.url), 'utf8'));
  const store = new RemoteNodeStore({ pool });
  const channelStore = new RemoteChannelPlanStore({ store });
  const server = createRemoteNodeGateway({ store, channelPlans: channelStore });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => { server.close(resolve); server.closeAllConnections(); }));
  const endpoint = `http://127.0.0.1:${server.address().port}`;
  const contexts = new Map();
  const assertBusinessFence = async (_client, task) => {
    if (contexts.get(task.task_id) !== task.context.execution_attempt_id
      || incrementalPlanHash(task.input.plan) !== task.context.plan_hash) throw new RemoteProtocolError('BUSINESS_FENCE_STALE');
  };

  async function reset() {
    await pool.query(`TRUNCATE remote_ingestion.channel_commands,remote_ingestion.receipts,remote_ingestion.claims,
      remote_ingestion.tasks,remote_ingestion.nodes CASCADE`);
    contexts.clear();
  }
  async function channel() {
    const channelId = `UC${randomUUID().replaceAll('-', '').slice(0, 22)}`;
    await pool.query(`INSERT INTO crawler.channels(channel_id,channel_url,title,status)
      VALUES($1,$2,'Original title','active')`, [channelId, `https://www.youtube.com/channel/${channelId}`]);
    return channelId;
  }
  async function enqueue(value) {
    const result = await channelStore.enqueue(value, { executionAttemptId: `attempt:${value.data.plan_id}` });
    if (result.taskId) contexts.set(result.taskId, `attempt:${value.data.plan_id}`);
    return result;
  }
  async function node() {
    const nodeId = randomUUID(); const token = randomBytes(32).toString('hex');
    await store.registerNode({ nodeId, token, capabilities: [CHANNEL_PLAN_CAPABILITY] });
    const client = createRemoteNodeClient({ url: endpoint, token, allowLoopbackHttp: true });
    return { nodeId, client, childCenter:{url:endpoint,token} };
  }
  function youtube(channelId, videoId = null) {
    const calls = [];
    const api = {
      openChannel: async (id, options) => {
        calls.push({ operation: 'open', id, options }); assert.equal(id, channelId);
        return { about_requested: options.includeAbout, about_observed: options.includeAbout, about_error: null,
          metadata: { channel_id: id, title: 'Plan About result', handle: '@remote-plan',
            subscriber_count_text: '1,234 subscribers', subscriber_count_source: 'youtube_about',
            video_count_text: '42 videos', video_count_source: 'youtube_about',
            view_count_text: '98,765 views', view_count_source: 'youtube_about',
            keywords: [], available_tabs: ['videos'], external_links: [], external_links_status: 'observed' },
          raw: { engine: 'youtubei.js@test' },
          scanUploads: async (scanOptions) => {
            calls.push({ operation: 'scan', options: scanOptions });
            return { entries: videoId ? [{ id: videoId, title: 'Discovered video', position: 1,
              published_at: new Date().toISOString(), published_day: new Date().toISOString().slice(0, 10),
              published_at_status: 'exact', published_at_precision: 'date_only', published_at_source: 'youtubejs_feed' }] : [],
            complete: true, pages: 1, item_count: videoId ? 1 : 0, parse_gap_count: 0,
            anchor_matched: false, stop_reason: 'list_end', terminal_reason: 'list_end',
            raw: { engine: 'youtubei.js@test' } };
          } };
      },
      fetchDetail: async (id, options) => { calls.push({ operation: 'detail', id, options }); return detail(id); },
    };
    return { api, calls };
  }
  async function execute(a, yt, { createApiFallback = null, upload = null } = {}) {
    const directory = await mkdtemp(join(tmpdir(), 'remote-channel-plan-'));
    let center; let claimed; let sessionCount = 0;
    const client = { ...a.client, claim: async (id) => {
      const lease = await a.client.claim(id); claimed = lease;
      if (lease) center = runRemoteIncrementalPlan({ channelStore, lease, assertBusinessFence, createApiFallback, pollMs: 5 })
        .then((value) => ({ value }), (error) => ({ error }));
      return lease;
    }, ...(upload ? { uploadCommand: upload } : {}) };
    const worker = new RemoteChannelPlanExecutor({ client, spool: new RemoteResultSpool({ directory }), youtube: yt,
      withSession: async (lease, _options, action) => { sessionCount++; assert.ok(lease.input.plan); return action(); },
      pollMs: 5, timeoutMs: 20000 });
    try {
      const status = await worker.runOnce();
      return { status, center: await center, claimed, sessionCount };
    } finally { await rm(directory, { recursive: true, force: true }); }
  }

  await t.test('two nodes claim different channels; About-only plans do not fetch video lists or details', async () => {
    await reset();
    const channels = [await channel(), await channel()];
    const jobs = channels.map((id) => job(id));
    await Promise.all(jobs.map(enqueue));
    const a = await node(); const b = await node();
    const seen = [];
    const dynamicYoutube = {
      openChannel: async (id, options) => { seen.push({ id, options }); return youtube(id).api.openChannel(id, options); },
      fetchDetail: async () => assert.fail('About-only must not fetch video details'),
    };
    const results = await Promise.all([execute(a, dynamicYoutube), execute(b, dynamicYoutube)]);
    for (const result of results) {
      if (result.center.error) throw result.center.error;
      assert.equal(result.status, 'applied');
      assert.equal(result.sessionCount, 1);
      assert.deepEqual(result.center.value.executed_domains, ['about']);
    }
    assert.equal(new Set(results.map((result) => result.claimed.task_id)).size, 2);
    assert.deepEqual(new Set(seen.map((item) => item.id)), new Set(channels));
    assert.ok(seen.every((item) => item.options.includeAbout));
    const commands = (await pool.query('SELECT operation FROM remote_ingestion.channel_commands')).rows;
    assert.deepEqual(commands.map((row) => row.operation), ['open_channel', 'open_channel']);
    const runs = (await pool.query('SELECT status FROM crawler.channel_runs WHERE plan_id=ANY($1::uuid[])', [jobs.map((j) => j.data.plan_id)])).rows;
    assert.ok(runs.every((row) => row.status === 'done'));
  });

  await t.test('video clock uses original checkpoint and detail writer, without creating an About observation', async () => {
    await reset();
    const channelId = await channel(); const videoId = randomBytes(8).toString('hex').slice(0, 11);
    const value = job(channelId, { video: true }); await enqueue(value);
    const yt = youtube(channelId, videoId);
    const result = await execute(await node(), yt.api);
    if (result.center.error) throw result.center.error;
    assert.equal(result.status, 'applied');
    assert.deepEqual(result.center.value.executed_domains, ['video']);
    assert.equal(yt.calls.find((call) => call.operation === 'open').options.includeAbout, false);
    assert.equal(yt.calls.filter((call) => call.operation === 'scan').length, 1);
    assert.equal(yt.calls.filter((call) => call.operation === 'detail').length, 1);
    assert.equal(yt.calls.find((call) => call.operation === 'detail').options.detailMode, 'full');
    const stored = (await pool.query(`SELECT view_count,like_count,comment_count FROM crawler.contents
      WHERE channel_id=$1 AND source_content_id=$2`, [channelId, videoId])).rows[0];
    assert.ok(stored);
    assert.deepEqual(Object.values(stored).map(Number), [321, 12, 0]);
    assert.equal((await pool.query(`SELECT count(*)::int AS n FROM crawler.crawl_observations
      WHERE channel_id=$1 AND observation_kind='about'`, [channelId])).rows[0].n, 0);
    assert.equal((await pool.query(`SELECT status FROM crawler.incremental_youtubejs_video_batches
      WHERE run_id=$1`, [`incremental:${value.data.plan_id}`])).rows[0].status, 'finalized');
  });

  await t.test('combined About/video/Agent plan uses one remote session; Agent remains in central backlog', async () => {
    await reset();
    const channelId = await channel(); const videoId = randomBytes(8).toString('hex').slice(0, 11);
    const value = job(channelId, { about: true, video: true, agent: true }); await enqueue(value);
    const yt = youtube(channelId, videoId);
    const result = await execute(await node(), yt.api);
    if (result.center.error) throw result.center.error;
    assert.equal(result.status, 'applied');
    assert.equal(result.center.value.status, 'waiting_agent');
    assert.deepEqual(result.center.value.executed_domains, ['about', 'video', 'agent']);
    assert.equal(yt.calls.filter((call) => call.operation === 'open').length, 1);
    assert.equal(result.sessionCount, 1);
    assert.equal((await pool.query('SELECT status FROM crawler.agent_refresh_requests WHERE plan_id=$1', [value.data.plan_id])).rows[0].status, 'pending');
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM remote_ingestion.tasks')).rows[0].n, 1);
  });

  await t.test('Agent-only clock stays central, and masks/clock versions cannot be rewritten under the same work key', async () => {
    await reset();
    const channelId = await channel();
    assert.equal((await enqueue(job(channelId, { agent: true }))).route, 'central');
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM remote_ingestion.tasks')).rows[0].n, 0);
    const value = job(channelId); await enqueue(value);
    await assert.rejects(enqueue({ ...value, data: { ...value.data, task_mask: { about: true, video: true, agent: false } } }), { code: 'WORK_KEY_CONFLICT' });
    await assert.rejects(enqueue({ ...value, data: { ...value.data, clock_version: 8 } }), { code: 'WORK_KEY_CONFLICT' });
  });

  await t.test('one channel cannot be owned concurrently by different nodes or central coordinators', async () => {
    await reset();
    const channelId = await channel(); await enqueue(job(channelId)); await enqueue(job(channelId));
    const a = await node(); const b = await node();
    const leases = await Promise.all([a.client.claim(randomUUID()), b.client.claim(randomUUID())]);
    assert.equal(leases.filter(Boolean).length, 1);
    const lease = leases.find(Boolean);
    const old = await channelStore.coordinate(lease);
    await assert.rejects(channelStore.coordinate(lease), { code: 'COORDINATOR_BUSY' });
    await pool.query("UPDATE remote_ingestion.tasks SET coordinator_until=clock_timestamp()-interval '1 second' WHERE task_id=$1", [lease.task_id]);
    const current = await channelStore.coordinate(lease);
    assert.notEqual(current.coordinatorId, old.coordinatorId);
    await assert.rejects(channelStore.transaction(lease, old.coordinatorId, assertBusinessFence, async () => assert.fail('stale central writer')),
      { code: 'STALE_COORDINATOR' });
  });

  await t.test('node cannot finish a channel itself, read another channel commands or submit an invented operation', async () => {
    await reset();
    const value = job(await channel()); const { taskId } = await enqueue(value);
    const a = await node(); const b = await node(); const lease = await a.client.claim(randomUUID());
    await assert.rejects(b.client.pollCommands(lease), { code: 'STALE_LEASE' });
    await assert.rejects(a.client.upload(taskId, await encodeResult({ version: 1, batch_id: randomUUID(), generation: lease.generation,
      outcome: 'success', data: { claimed_done: true } })), { code: 'CHANNEL_PLAN_REQUIRES_CENTRAL_COMPLETION' });
    await assert.rejects(a.client.uploadCommand(lease, await encodeResult({ version: 1, batch_id: randomUUID(), generation: lease.generation,
      command_id: randomUUID(), outcome: 'success', data: {} })), { code: 'UNKNOWN_COMMAND' });
  });

  await t.test('network and required-field errors retain their classifications across the wire', async () => {
    for (const error of [Object.assign(new Error('proxy transport'), { code: 'FINGERPRINT_PROXY_TRANSPORT', source: 'fingerprint_gateway' }),
      Object.assign(new Error('missing player data'), { name: 'YoutubeJsRequiredSurfaceError', required_surface: 'player', partial_detail: { id: 'fixture' } })]) {
      const replay = fromChannelWire(toChannelWire(error));
      assert.deepEqual(selectYoutubeFailure({ error: replay }).decision, selectYoutubeFailure({ error }).decision);
      assert.deepEqual(replay.partial_detail, error.partial_detail);
    }
  });

  await t.test('committed command receipt survives a lost response and node restart without another YouTube request', async () => {
    await reset();
    const channelId = await channel(); await enqueue(job(channelId));
    const a = await node(); const yt = youtube(channelId);
    const directory = await mkdtemp(join(tmpdir(), 'remote-plan-restart-'));
    const spool = new RemoteResultSpool({ directory });
    let center;
    const client = { ...a.client,
      claim: async (id) => {
        const lease = await a.client.claim(id);
        center = runRemoteIncrementalPlan({ channelStore, lease, assertBusinessFence, pollMs: 5 })
          .then((value) => ({ value }), (error) => ({ error }));
        return lease;
      },
      uploadCommand: async (...args) => { await a.client.uploadCommand(...args); throw new Error('lost receipt response'); },
    };
    const options = { spool, youtube: yt.api, withSession: async (_lease, _options, action) => action(), pollMs: 5 };
    try {
      await assert.rejects(new RemoteChannelPlanExecutor({ ...options, client }).runOnce(), /lost receipt response/);
      const completed = await center; if (completed.error) throw completed.error;
      assert.ok(await spool.read('pending.json'));
      const waiting=new RemoteChannelPlanExecutor({...options,client:a.client,networkSession:{
        recover:async()=>{throw Object.assign(new Error('awaiting central recovery'),{code:'NETWORK_EXECUTION_CLOSED'});},
        run:()=>assert.fail('receipt replay must not collect'),
      }});
      await assert.rejects(waiting.runOnce(),{code:'NETWORK_EXECUTION_CLOSED'});
      assert.equal(await spool.read('pending.json'),null,'a closed network must not block committed receipt replay');
      assert.equal(await new RemoteChannelPlanExecutor({ ...options, client: a.client }).runOnce(), 'applied');
      assert.equal(yt.calls.length, 1);
      assert.equal(await spool.read('pending.json'), null);
      assert.equal(await spool.read('claim.json'), null);
    } finally { await rm(directory, { recursive: true, force: true }); }
  });

  for(const commit of [false,true]) await t.test(`SIGKILL after durable node spool survives center receipt race (committed=${commit})`,async tt=>{
    await reset();const channelId=await channel();await enqueue(job(channelId));
    const a=await node();const claimId=randomUUID();const lease=await a.client.claim(claimId);
    const commandId=await store.transaction(async client=>{
      const task=await channelStore.lock(client,lease);
      return channelStore.request(client,task,'open_channel',{channel_id:channelId,options:{includeAbout:true}},'crash');
    });
    const directory=await mkdtemp(join(tmpdir(),'remote-receipt-kill-'));
    tt.after(()=>rm(directory,{recursive:true,force:true}));
    const snapshot=await youtube(channelId).api.openChannel(channelId,{includeAbout:true});delete snapshot.scanUploads;
    const child=fork(new URL('./fixtures/remoteReceiptCrash.mjs',import.meta.url),[],{stdio:['ignore','ignore','pipe','ipc']});
    child.stderr.resume();const exited=once(child,'exit');
    tt.after(async()=>{if(child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');await exited;});
    const message=once(child,'message',{signal:AbortSignal.timeout(10000)});
    child.send({center:a.childCenter,claimId,directory,snapshot,commit});
    assert.deepEqual((await message)[0],{saved:true,committed:commit});
    child.kill('SIGKILL');assert.equal((await exited)[1],'SIGKILL');
    const spool=new RemoteResultSpool({directory});const before=await spool.read('pending.json');assert.ok(before);
    // The center has closed this execution before the restarted node returns.
    await pool.query("UPDATE remote_ingestion.tasks SET state='failed' WHERE task_id=$1",[lease.task_id]);
    const executor=new RemoteChannelPlanExecutor({client:a.client,spool,
      withSession:()=>assert.fail('recovery must not restart collection'),
      youtube:{openChannel:()=>assert.fail('no duplicate collection'),fetchDetail:()=>assert.fail('no detail')}});
    assert.equal(await executor.runOnce(),'failed');
    assert.equal(await spool.read('pending.json'),null);assert.equal(await spool.writable(),true);
    const files=await readdir(directory);
    if(commit)assert.equal(files.length,0,'durable receipt is acknowledged even after central task closes');
    else{
      assert.equal(files.length,1);assert.ok(files[0].endsWith('.stale'));
      assert.deepEqual(JSON.parse(await readFile(join(directory,files[0]),'utf8')),before,'rejected stale data remains on disk');
    }
    assert.equal((await pool.query('SELECT state FROM remote_ingestion.channel_commands WHERE command_id=$1',[commandId])).rows[0].state,commit?'received':'pending');
  });

  await t.test('About COMMIT acknowledgement loss preserves completed domain and a recovery never writes a second observation', async () => {
    await reset();
    const channelId = await channel(); const value = job(channelId); const { taskId } = await enqueue(value);
    const yt = youtube(channelId);
    const originalTransaction = channelStore.transaction.bind(channelStore);
    let loseResponse = true;
    channelStore.transaction = async (...args) => {
      const result = await originalTransaction(...args);
      if (loseResponse && result?.observation_id) {
        loseResponse = false;
        throw Object.assign(new Error('About COMMIT response lost'), { code: 'TEST_COMMIT_RESPONSE_LOST' });
      }
      return result;
    };
    try {
      const first = await execute(await node(), yt.api);
      assert.equal(first.status, 'failed');
      assert.equal(first.center.error.code, 'TEST_COMMIT_RESPONSE_LOST');
      const run = (await pool.query('SELECT result_json FROM crawler.channel_runs WHERE plan_id=$1', [value.data.plan_id])).rows[0];
      assert.equal(run.result_json.domains.about.status, 'complete');
      // Simulate central authorization of a retry. The node cannot do this update.
      await pool.query(`UPDATE remote_ingestion.tasks SET state='pending',node_id=NULL,lease_until=NULL,
        coordinator_id=NULL,coordinator_until=NULL WHERE task_id=$1`, [taskId]);
      const recovered = await execute(await node(), yt.api);
      if (recovered.center.error) throw recovered.center.error;
      assert.equal(recovered.status, 'applied');
      assert.deepEqual(recovered.center.value.executed_domains, []);
      assert.equal(yt.calls.length, 1);
      assert.equal((await pool.query(`SELECT count(*)::int AS n FROM crawler.crawl_observations
        WHERE channel_id=$1 AND observation_kind='about'`, [channelId])).rows[0].n, 1);
    } finally { channelStore.transaction = originalTransaction; }
  });

  await t.test('existing API batch handoff releases the remote node and preserves unfinished video state', async () => {
    await reset();
    const channelId = await channel(); const videoId = randomBytes(8).toString('hex').slice(0, 11);
    const value = job(channelId, { video: true }); const { taskId } = await enqueue(value);
    const yt = youtube(channelId, videoId);
    let detailAttempts = 0;
    yt.api.fetchDetail = async () => {
      detailAttempts++;
      throw Object.assign(new Error('required player view_count missing'), {
        name: 'YoutubeJsRequiredSurfaceError', required_surface: 'player', partial_detail: { id: videoId, like_count: 12 },
      });
    };
    const createApiFallback = ({ query, withTransaction }) => createVideoDetailApiFallback({ query, withTransaction,
      loadSettings: async () => ({ fallbackMode: 'enabled', apiKeys: ['fixture-only'], dailyRequestLimit: 100 }),
      wait: waitForVideoApiDetail,
    });
    const result = await execute(await node(), yt.api, { createApiFallback });
    assert.equal(detailAttempts, 3);
    assert.equal(result.status, 'waiting_central');
    assert.equal(result.center.error.code, 'VIDEO_API_PENDING');
    const task = (await pool.query('SELECT state,applied_result FROM remote_ingestion.tasks WHERE task_id=$1', [taskId])).rows[0];
    assert.equal(task.state, 'received');
    assert.equal(task.applied_result.waiting_central, true);
    const run = (await pool.query('SELECT status,result_json FROM crawler.channel_runs WHERE plan_id=$1', [value.data.plan_id])).rows[0];
    assert.equal(run.status, 'running'); assert.equal(run.result_json.domains.video.status, 'running');
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM crawler.youtube_api_detail_requests WHERE run_id=$1', [`incremental:${value.data.plan_id}`])).rows[0].n, 1);
    assert.equal(await channelStore.resumeAfterApi(taskId, assertBusinessFence), false);
    const apiDetail = { ...detail(videoId), source: 'youtube_data_api_videos_list', privacy_status: 'public' };
    await pool.query(`UPDATE crawler.youtube_api_detail_requests SET status='done',detail_json=$2,finished_at=now()
      WHERE request_id=$1`, [task.applied_result.request_id, apiDetail]);
    assert.equal(await channelStore.resumeAfterApi(taskId, assertBusinessFence), true);
    const restored = await execute(await node(), yt.api, { createApiFallback });
    if (restored.center.error) throw restored.center.error;
    assert.equal(restored.status, 'applied');
    assert.equal(detailAttempts, 3);
    assert.equal(restored.claimed.generation, 2);
    assert.equal((await pool.query('SELECT lease_failures FROM remote_ingestion.tasks WHERE task_id=$1', [taskId])).rows[0].lease_failures, 0);
    assert.equal((await pool.query('SELECT status FROM crawler.channel_runs WHERE plan_id=$1', [value.data.plan_id])).rows[0].status, 'done');
  });
});
