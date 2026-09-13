// Explicit cross-host diagnostic. Never operates on production Plans/queues.
import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes, randomUUID, generateKeyPairSync } from 'node:crypto';
import { readFile, writeFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn, execFileSync } from 'node:child_process';
import { once } from 'node:events';
import { createServer, connect } from 'node:net';
import { Queue, QueueEvents, Worker } from 'bullmq';
import pg from 'pg';
import { RemoteNodeStore } from '../src/remoteNodes/store.js';
import { RemoteChannelPlanStore } from '../src/remoteNodes/channelPlanStore.js';
import { RemoteChannelRouteStore } from '../src/remoteNodes/channelRouteStore.js';
import { RemoteYoutubeSessionStore } from '../src/remoteNodes/youtubeSessionStore.js';
import { RemoteManagedIncrementalRuntime } from '../src/remoteNodes/managedIncrementalRuntime.js';
import { WholeChannelStore } from '../src/remoteNodes/wholeChannelStore.js';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';
import { assertRemoteIncrementalBusinessFence } from '../src/remoteNodes/incrementalBusinessFence.js';
import { CHANNEL_PLAN_CAPABILITY } from '../src/remoteNodes/channelPlanContract.js';
import { createCenterIncrementalProcessor } from '../src/remoteNodes/centerIncrementalProcessor.js';
import { createRotaRemoteRouteReader } from '../src/remoteNodes/rotaRouteSource.js';
import { createTransportSignals } from '../src/remoteNodes/transportSignals.js';
import { startRemoteNatsCenter } from '../src/remoteNodes/natsCenter.js';
import { RotaSlotAdapter } from '../src/rotaSlotAdapter.js';
import { ProxyControlClient } from '../src/proxyControlClient.js';
import { resolveWorkerIdentityPolicy } from '../src/identityPolicyCatalog.js';
import { incrementalPlanHash, INCREMENTAL_JOB_NAME, INCREMENTAL_QUEUE } from '../src/incrementalPlan.js';
import { PUBLICATION_WRITER_VERSION } from '../src/publicationWriterVersion.js';
import { closeDb } from '../src/db.js';

test('whole-channel real remote node, NATS and original BullMQ execution',
  { skip: process.env.REMOTE_WAN_LIVE !== 'true', timeout: 480000 }, async t => {
  const url = process.env.REMOTE_NODE_TEST_DATABASE_URL;
  assert.equal(process.env.DATABASE_URL, url);
  assert.equal(process.env.YOUTUBEJS_VIDEO_API_BATCH_FALLBACK, 'false');
  const endpoint = new URL(process.env.REMOTE_NATS_TEST_URL);
  assert.equal(endpoint.hostname, '127.0.0.1');
  const pool = new pg.Pool({ connectionString: url, max: 10,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}` });
  await assertIsolatedRemoteDatabase(pool);
  const query = pool.query.bind(pool);
  let rota, center, signals, tunnel;
  t.after(async () => {
    try { await rota?.close(); } finally {
      tunnel?.kill('SIGTERM');
      await center?.close(); await signals?.close(); await pool.end(); await closeDb();
    }
  });
  for (const file of ['../src/schema.sql', '../../feature-engine/sql/schema.sql',
    '../src/remoteNodes/schema.sql', '../src/remoteNodes/routeSchema.sql',
    '../src/remoteNodes/youtubeSessionSchema.sql', '../src/remoteNodes/natsSchema.sql',
    '../src/remoteNodes/wholeChannelSchema.sql']) {
    await query(await readFile(new URL(file, import.meta.url), 'utf8'));
  }
  const host = process.env.REMOTE_WAN_SSH_HOST;
  const image = process.env.REMOTE_WAN_IMAGE;
  assert.match(host, /^[a-z0-9@.-]+$/);
  assert.match(image, /^qy-whole-wan-test:[a-f0-9]{12}$/);
  const sshOptions = ['-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
    '-i', process.env.REMOTE_WAN_SSH_KEY, '-o', `UserKnownHostsFile=${process.env.REMOTE_WAN_KNOWN_HOSTS}`];
  const ssh = command => execFileSync('ssh', [...sshOptions, host, command], { encoding: 'utf8', timeout: 30000 });
  // The broker remains bound to loopback. SSH carries WSS/TLS across hosts;
  // no nginx, firewall, production broker or public port is changed.
  tunnel = spawn('ssh', [...sshOptions, '-N', '-T', '-o', 'ExitOnForwardFailure=yes',
    '-o', 'ServerAliveInterval=15', '-R', `127.0.0.1:0:127.0.0.1:${endpoint.port}`, host],
  { stdio: ['ignore', 'ignore', 'pipe'] });
  const remotePort = await new Promise((resolve, reject) => {
    let text = '';
    const timer = setTimeout(() => reject(new Error('TUNNEL_TIMEOUT')), 15000);
    tunnel.once('exit', () => { clearTimeout(timer); reject(new Error('TUNNEL_EXITED')); });
    tunnel.stderr.on('data', bytes => {
      text = (text + bytes).slice(-4096);
      const match = text.match(/Allocated port (\d+)/);
      if (match) { clearTimeout(timer); resolve(Number(match[1])); }
    });
  });
  endpoint.port = String(remotePort);
  const routeSockets = new Set();
  const routeProxy = createServer(socket => {
    const upstream = connect({ host: process.env.REMOTE_WAN_READER_HOST, port: 3188 });
    routeSockets.add(socket); routeSockets.add(upstream);
    socket.once('close', () => { routeSockets.delete(socket); upstream.destroy(); });
    upstream.once('close', () => { routeSockets.delete(upstream); socket.destroy(); });
    socket.on('error', () => upstream.destroy()); upstream.on('error', () => socket.destroy());
    socket.pipe(upstream).pipe(socket);
  });
  routeProxy.listen(0, '127.0.0.1'); await once(routeProxy, 'listening');
  t.after(async () => { for (const socket of routeSockets) socket.destroy(); await new Promise(resolve => routeProxy.close(resolve)); });
  const store = new RemoteNodeStore({ pool });
  const channelStore = new RemoteChannelPlanStore({ store });
  const keypair = generateKeyPairSync('ed25519');
  const routes = new RemoteChannelRouteStore({ channelStore, privateKey: keypair.privateKey, secretKey: randomBytes(32),
    assertBusinessFence: assertRemoteIncrementalBusinessFence,
    readRotaRoute: createRotaRemoteRouteReader({ url: `http://127.0.0.1:${routeProxy.address().port}/internal/v1/remote-route`,
      token: process.env.REMOTE_WAN_ROTA_ROUTE_TOKEN, allowLoopbackHttp: true }) });
  const sessions = new RemoteYoutubeSessionStore({ routes });
  const wholeChannels = new WholeChannelStore({ channelPlans: channelStore, assertBusinessFence: assertRemoteIncrementalBusinessFence });
  signals = await createTransportSignals({ connectionString: url }); channelStore.transportSignals = signals;
  center = await startRemoteNatsCenter({ url: process.env.REMOTE_NATS_TEST_URL,
    password: process.env.REMOTE_NATS_TEST_PASSWORD, tls: { caFile: process.env.REMOTE_NATS_TEST_CA },
    store, channelPlans: channelStore, wholeChannels, routes, youtubeSessions: sessions, signals, resultMaxBytes: 32 * 1024 * 1024 });
  const nodeId = process.env.REMOTE_NATS_TEST_NODE_ID, token = process.env.REMOTE_NATS_TEST_TOKEN;
  const workerId = `whole-wan-${randomUUID()}`;
  const resolvedPolicy = resolveWorkerIdentityPolicy({ role: 'channel', policyId: process.env.ROTA_IDENTITY_POLICY_ID,
    expectedWorkloadScope: process.env.ROTA_WORKLOAD_SCOPE_EXPECTED });
  const runtime = new RemoteManagedIncrementalRuntime({ channelStore, routes, youtubeSessions: sessions,
    nodeId, slot: 'worker-1', profileSecret: randomBytes(32).toString('hex'), claimTimeoutMs: 60000 });
  rota = new RotaSlotAdapter({ client: new ProxyControlClient(), role: 'channel', workerId, resolvedPolicy,
    proxyBaseUrl: process.env.ROTA_PROXY_BASE_URL, proxyPassword: process.env.ROTA_BULLMQ_PROXY_PASSWORD,
    identityRuntime: runtime, maxRouteSwitchesPerExecution: 0 });
  const deadline = setTimeout(() => rota.close(), 30000);
  try { await rota.start(); } finally { clearTimeout(deadline); }
  const fixture = JSON.parse(await readFile(process.env.REMOTE_WAN_FIXTURE_FILE, 'utf8'));
  assert.match(fixture.channelId, /^UC[\w-]{22}$/);
  assert.ok(fixture.anchor?.published_at);
  const results = [];
  for (const whole of [false, true]) await t.test(whole ? 'whole' : 'per-command', async tt => {
    await query('TRUNCATE remote_ingestion.nodes,remote_ingestion.tasks,feature_clock.daily_channel_plans,crawler.channels CASCADE');
    await store.registerNode({ nodeId, token, capabilities: [CHANNEL_PLAN_CAPABILITY] });
    await routes.registerSlot(nodeId, 'worker-1', workerId);
    runtime.wholeChannels = whole ? wholeChannels : null;
    const channelId = fixture.channelId, anchor = fixture.anchor, mask = { about: true, video: true, agent: false };
    await query("INSERT INTO crawler.channels(channel_id,channel_url,title,status) VALUES($1,$2,'Before WAN test','active')", [channelId, `https://www.youtube.com/channel/${channelId}`]);
    await query(`INSERT INTO crawler.contents(content_key,channel_id,content_type,source_content_id,published_at,title,
      published_at_status,published_at_source,published_at_precision) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
    [anchor.content_key,channelId,anchor.content_type,anchor.source_content_id,anchor.published_at,anchor.title,
      anchor.published_at_status,anchor.published_at_source,anchor.published_at_precision]);
    await query("INSERT INTO crawler.channel_domain_cursors(channel_id,observation_kind,anchor_video_ids) VALUES($1,'video',$2)", [channelId,[anchor.source_content_id]]);
    const planId = randomUUID(); const scheduled = new Date().toISOString().slice(0, 10) + 'T01:00:00.000Z';
    const plan = { schema_version: 5, dispatch_generation: 1, job_id: `remote_fence_${planId}`, plan_id: planId,
      plan_mode: 'standard', plan_day: scheduled.slice(0, 10), scheduled_at: scheduled, channel_id: channelId, task_mask: mask,
      capacity: { factor: 1, player_cap: 20, next_cap: 8, version: 'capacity-1' }, clock_version: 7,
      policy_version: 'v16-rule-1', planner_config_version: 'video-plan-1' };
    const job = {id:plan.job_id,queueName:INCREMENTAL_QUEUE};
    await query(`INSERT INTO feature_clock.daily_channel_plans(plan_id,plan_day,channel_id,due_day,due_at,eligible_at,scheduled_at,
      run_about,run_video,run_agent,dispatch_slot,capacity_factor,player_cap,next_cap,source_clock_version,
      policy_version,planner_config_version,capacity_version,status)
      VALUES($1,$2,$3,$2,$4,$4,$4,$5,$6,$7,0,1,20,8,7,$8,$9,$10,'dispatched')`,
    [planId, plan.plan_day, channelId, scheduled, mask.about, mask.video, mask.agent, plan.policy_version, plan.planner_config_version, plan.capacity.version]);
    await query(`INSERT INTO feature_clock.dispatch_outbox(dispatch_event_id,plan_id,job_id,queue_name,payload_json,payload_hash,status)
      VALUES($1,$2,$3,$4,$5,$6,'published')`, [randomUUID(), planId, job.id, job.queueName, plan, incrementalPlanHash(plan)]);
    const prefix = `whole-wan-${randomUUID()}`;
    const connection = { host: '127.0.0.1', port: Number(process.env.REMOTE_NODE_TEST_REDIS_PORT),
      password: 'remote-center-fixture-only', maxRetriesPerRequest: null };
    const queue = new Queue(INCREMENTAL_QUEUE, { connection, prefix });
    const events = new QueueEvents(INCREMENTAL_QUEUE, { connection, prefix });
    const processor = createCenterIncrementalProcessor({ channelStore, runtime, rota, resolvedPolicy, ready: async () => true });
    const consumer = new Worker(INCREMENTAL_QUEUE, processor, { connection, prefix, concurrency: 1 });
    tt.after(async () => { await consumer.close(); await events.close(); await queue.obliterate({ force: true }); await queue.close(); });
    await events.waitUntilReady();
    const directory = await mkdtemp(join(tmpdir(), 'whole-wan-node-'));
    const name = `qy-whole-wan-${randomUUID().slice(0,8)}`;
    const remoteDirectory = `/tmp/${name}`;
    await writeFile(join(directory,'config.json'), JSON.stringify({ isolated:true, nodeId, url:endpoint.href, mode:whole?'whole':'per-command' }), {mode:0o600});
    await writeFile(join(directory,'token'), token, {mode:0o600});
    await writeFile(join(directory,'route.pem'),keypair.publicKey.export({type:'spki',format:'pem'}),{mode:0o600});
    await writeFile(join(directory,'ca.pem'),await readFile(process.env.REMOTE_NATS_TEST_CA),{mode:0o600});
    const archive = execFileSync('tar',['-C',directory,'-czf','-','.']);
    execFileSync('ssh',[...sshOptions,host,`mkdir -m 700 ${remoteDirectory} && tar -xzf - -C ${remoteDirectory} && mkdir -m 700 ${remoteDirectory}/spool`],{input:archive,timeout:30000});
    tt.after(async () => {
      ssh(`sudo -n docker rm -f ${name} >/dev/null 2>&1 || true`);
      ssh(`rm -rf ${remoteDirectory}`);
      await rm(directory,{recursive:true,force:true});
    });
    const node = spawn('ssh',[...sshOptions,host,`sudo -n docker run --name ${name} --network host --read-only --cap-drop ALL --security-opt no-new-privileges --memory 512m --cpus 1 --pids-limit 96 --tmpfs /tmp:rw,size=320m,mode=1777 -v ${remoteDirectory}:/run/live-test:ro -v ${remoteDirectory}/spool:/tmp/whole-wan-spool --entrypoint node ${image} test/fixtures/remoteWholeChannelLiveNode.mjs`],{stdio:['ignore','pipe','pipe']});
    let output='', stderr='';
    node.stdout.on('data',bytes=>{output+=bytes;});node.stderr.on('data',bytes=>{stderr=(stderr+bytes).slice(-4096);});
    const nodeExit = once(node,'exit');
    const started = Date.now();
    const queued = await queue.add(INCREMENTAL_JOB_NAME, plan, {jobId:plan.job_id,attempts:1});
    let centralResult, failure;
    try {centralResult=await queued.waitUntilFinished(events,180000);} catch(error){failure=String(error.code||error.message);}
    const centralMs=Date.now()-started;
    const [code] = await nodeExit;
    const line=output.split('\n').find(line=>line.startsWith('{"event":"whole_wan_node_result"'));
    const metrics=line?JSON.parse(line).metrics:null;
    const remainingSpool=ssh(`find ${remoteDirectory}/spool -type f -printf '%f\n'`).trim().split('\n').filter(Boolean);
    const record={whole,disk_spool:true,remaining_spool:remainingSpool,central_ms:centralMs,central_result:centralResult,failure,node_exit:code,node:metrics,
      contents:(await query('SELECT to_jsonb(c) AS data FROM crawler.contents c WHERE channel_id=$1 ORDER BY source_content_id',[channelId])).rows.map(r=>r.data),
      run:(await query('SELECT status,result_json FROM crawler.channel_runs WHERE plan_id=$1',[plan.plan_id])).rows[0],
      attempts:(await query('SELECT status,finished_at,result_json FROM crawler.channel_execution_attempts WHERE run_id=$1',[`incremental:${plan.plan_id}`])).rows,
      bindings:(await query('SELECT state,identity,release_receipt FROM remote_ingestion.network_bindings')).rows,
      commands:(await query('SELECT operation,count(*)::int AS count FROM remote_ingestion.channel_commands GROUP BY operation')).rows};
    results.push(record);await writeFile(process.env.REMOTE_WAN_REPORT,JSON.stringify(results,null,2),{mode:0o600});
    console.log(JSON.stringify({event:'whole_wan_case',whole,central_ms:centralMs,node_exit:code,failure,node:metrics}));
    assert.equal(code,0,stderr);assert.equal(failure,undefined);
    assert.deepEqual(remainingSpool,[],'no unacknowledged task data after successful finish');assert.equal(metrics?.status,'applied');
    assert.equal(record.run.status,'done');assert.equal(record.run.result_json.domains.video.status,'complete');
    assert.equal(record.attempts.length,1);assert.ok(record.attempts.every(a=>a.status==='success'&&a.finished_at));
    assert.ok(record.bindings.length>0&&record.bindings.every(b=>b.state==='retired'&&b.release_receipt?.in_flight===0));
    const calls=metrics.operations.filter(o=>o.name==='detail');assert.ok(calls.length>=3);
    for(const op of calls){const c=record.contents.find(c=>c.source_content_id===op.id);
      assert.ok(c?.title&&c.published_at&&c.thumbnail_url);assert.notEqual(c.view_count_status,'unresolved');
      assert.notEqual(c.comment_count_status,'unresolved');assert.notEqual(c.like_count_status,'unresolved');
      assert.equal(c.comments_first_page.returned_count,c.comments_first_page.comments.length);}
    if(whole){assert.deepEqual(record.commands,[{operation:'collect_channel',count:1}]);assert.equal(metrics.rpc.uploadWholeChannel.calls,1);}
  });
  assert.equal(results.length,2);
  assert.equal(results[0].bindings[0].identity.network_identity_key,results[1].bindings[0].identity.network_identity_key,'same Rota network identity');
  assert.deepEqual(results[0].node.operations.filter(o=>o.name==='detail').map(o=>o.id),results[1].node.operations.filter(o=>o.name==='detail').map(o=>o.id));
  for(let i=0;i<results[0].contents.length;i++)for(const key of ['source_content_id','title','content_type','description','duration_seconds','published_at','view_count_status','like_count_status','comment_count_status']){
    assert.deepEqual(results[0].contents[i][key],results[1].contents[i][key],key);
  }
});
