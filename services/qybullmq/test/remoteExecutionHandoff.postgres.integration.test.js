import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes, randomUUID } from 'node:crypto';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn, fork } from 'node:child_process';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import pg from 'pg';
import { Job, Queue, QueueEvents, Worker } from 'bullmq';
import { setTimeout as delay } from 'node:timers/promises';
import { RemoteCenterExecutionSupervisor, supervisionLockKey } from '../src/remoteNodes/centerExecutionSupervisor.js';
import { RemoteWorkerActivationStore } from '../src/remoteNodes/workerActivationStore.js';
import { RemoteIncrementalProcess } from '../src/remoteNodes/nodeIncrementalRuntime.js';
import { createCenterIncrementalProcessor } from '../src/remoteNodes/centerIncrementalProcessor.js';
import { RemoteNodeStore } from '../src/remoteNodes/store.js';
import { RemoteChannelPlanStore } from '../src/remoteNodes/channelPlanStore.js';
import { RemoteChannelRouteStore } from '../src/remoteNodes/channelRouteStore.js';
import { RemoteYoutubeSessionStore } from '../src/remoteNodes/youtubeSessionStore.js';
import { RemoteManagedIncrementalRuntime } from '../src/remoteNodes/managedIncrementalRuntime.js';
import { createRemoteIncrementalWorker } from '../src/remoteNodes/incrementalWorker.js';
import { createRemoteNodeGateway } from '../src/remoteNodes/gateway.js';
import { createRemoteNodeClient } from '../src/remoteNodes/client.js';
import { createLocalRotaClient } from '../src/remoteNodes/localRotaClient.js';
import { RemoteResultSpool } from '../src/remoteNodes/spool.js';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';
import { assertRemoteIncrementalBusinessFence } from '../src/remoteNodes/incrementalBusinessFence.js';
import { remotePlanJob } from '../src/remoteNodes/executionContext.js';
import { runRemoteIncrementalApiReplay } from '../src/remoteNodes/incrementalCoordinator.js';
import { CHANNEL_PLAN_CAPABILITY } from '../src/remoteNodes/channelPlanContract.js';
import { RotaSlotAdapter } from '../src/rotaSlotAdapter.js';
import { executeManagedWorkerAttempt } from '../src/managedWorkerExecution.js';
import { ProxyBusinessRunPreparer } from '../src/proxyBusinessRun.js';
import { IncrementalRunStore } from '../src/incrementalRunStore.js';
import { resolveWorkerIdentityPolicy } from '../src/identityPolicyCatalog.js';
import { incrementalPlanHash, INCREMENTAL_JOB_NAME, INCREMENTAL_QUEUE } from '../src/incrementalPlan.js';
import { emptyUploadsDecision } from '../src/youtubeUploadsCountry.js';
import { acquireYoutubeJs, releaseYoutubeJs, closeYoutubeJs } from '../src/youtubeJs.js';
import { createVideoDetailApiFallback } from '../src/videoDetailApiFallback.js';
import { waitForVideoApiDetail } from '../src/videoApiBatchRequests.js';
import { runVideoApiResumable, gateVideoApiJob } from '../src/videoApiContinuation.js';
import { PUBLICATION_WRITER_VERSION } from '../src/publicationWriterVersion.js';
import { encodeResult, decodeResult } from '../src/remoteNodes/protocol.js';

const url = process.env.REMOTE_NODE_TEST_DATABASE_URL;
const binary = process.env.REMOTE_NODE_ROTA_TEST_BINARY;
const resolvedPolicy = resolveWorkerIdentityPolicy({ role: 'channel', policyId: 'qy-br-channel-anonymous-v1', expectedWorkloadScope: 'qy-production', environment: {} });

function detail(id) {
  return { id, title: 'API handoff fixture', thumbnail_url: `https://i.ytimg.com/vi/${id}/default.jpg`,
    published_at: new Date().toISOString(), published_at_status: 'exact', published_at_precision: 'second', published_at_source: 'youtubejs_player',
    duration_seconds: 90, duration_source: 'youtubejs_player', view_count: 321, view_count_text: '321', view_count_source: 'youtubejs_player',
    like_count: 12, like_count_source: 'youtubejs_player', comment_count: 0, comment_count_status: 'exact', comment_count_source: 'youtubejs_comments',
    comments_disabled: true, comments_first_page: { version: 1, total_count: 0, returned_count: 0, comments: [] },
    description: 'Fixture', description_status: 'exact', description_source: 'youtubejs_player', description_observed: true,
    hashtags: [], hashtags_observed: true, keywords: [], keywords_observed: true, availability: 'public', access_status: 'public', access_status_source: 'youtubejs_player',
    content_type_signals: { source: 'youtubei_player', canonical_url: `https://www.youtube.com/watch?v=${id}`,
      is_shorts_eligible: false, is_live_content: false, is_live: false, is_upcoming: false, is_live_now: false },
    extractor_version: 'youtubei.js@fixture', source: 'youtubejs_get_info' };
}

test('original Rota and API lifecycle carry one remote Plan across execution generations', { skip: !url || !binary, timeout: 300000 }, async t => {
  const pool = new pg.Pool({ connectionString: url, max: 8, options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}` });
  const guard = await pool.connect(); t.after(async () => { guard.release(); await pool.end(); });
  await assertIsolatedRemoteDatabase(pool); await guard.query('SELECT pg_advisory_lock(781137981)');
  for (const file of ['../src/schema.sql','../../feature-engine/sql/schema.sql','../src/remoteNodes/schema.sql','../src/remoteNodes/routeSchema.sql','../src/remoteNodes/youtubeSessionSchema.sql','../src/remoteNodes/workerConnectionSchema.sql','../src/remoteNodes/workerActivationSchema.sql']) {
    await pool.query(await readFile(new URL(file, import.meta.url), 'utf8'));
  }
  const previousMode = process.env.YOUTUBEJS_EXTRACTOR_MODE; process.env.YOUTUBEJS_EXTRACTOR_MODE = 'full';
  t.after(() => { if (previousMode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE; else process.env.YOUTUBEJS_EXTRACTOR_MODE = previousMode; });

  async function fixture(tt, { kind = 'country', reserve = true, api = false, remainingVideo = false, supervised = false, startNode = true, startSupervisor = true } = {}) {
    await pool.query('TRUNCATE remote_ingestion.nodes,remote_ingestion.tasks CASCADE');
    const store = new RemoteNodeStore({ pool }); const channelStore = new RemoteChannelPlanStore({ store });
    const query = pool.query.bind(pool); const transaction = action => store.transaction(action);
    const nodeId = randomUUID(); const token = randomBytes(32).toString('hex'); const keypair = generateKeyPairSync('ed25519');
    async function createJob({ about = ['network','about'].includes(kind), video = !['network','about'].includes(kind) } = {}) {
      const channelId = `UC${randomUUID().replaceAll('-','').slice(0,22)}`;
      await query(`INSERT INTO crawler.channels(channel_id,channel_url,title,status,country,country_code,country_source,total_video_count)
        VALUES($1,$2,'Before handoff','active','Brazil','BR','youtube_about',1)`, [channelId, `https://www.youtube.com/channel/${channelId}`]);
      const planId = randomUUID(); const now = new Date().toISOString();
      const plan = { schema_version: 5, dispatch_generation: 1, job_id: `remote-handoff-${planId}`, plan_id: planId,
        plan_mode: 'standard', plan_day: now.slice(0,10), scheduled_at: now, channel_id: channelId,
        task_mask: { about, video, agent: false },
        capacity: { factor: 1, player_cap: 20, next_cap: 8, version: 'capacity-1' }, clock_version: 7,
        policy_version: 'v16-rule-1', planner_config_version: 'video-plan-1' };
      // Actual BullMQ Job/getters; only Redis writes are replaced in this
      // PostgreSQL/HTTP test. The transport must not lose prototype identity.
      const job = new Job({ name: INCREMENTAL_QUEUE, keys: {}, opts: {}, toKey: key=>`fixture:${key}` },
        INCREMENTAL_JOB_NAME, plan, {}, plan.job_id);
      Object.assign(job, { attemptsStarted: 1,
        async updateData(data) { this.data = data; }, async updateProgress() {},
        async moveToDelayed() { this.delays = (this.delays || 0) + 1; } });
      assert.equal(Object.hasOwn(job,'queueName'),false);
      await query(`INSERT INTO feature_clock.daily_channel_plans(plan_id,plan_day,channel_id,due_day,due_at,eligible_at,scheduled_at,
        run_about,run_video,run_agent,dispatch_slot,capacity_factor,player_cap,next_cap,source_clock_version,
        policy_version,planner_config_version,capacity_version,status)
        VALUES($1,$2,$3,$2,$4,$4,$4,$5,$6,false,0,1,20,8,7,$7,$8,$9,'dispatched')`,
      [planId,plan.plan_day,channelId,now,plan.task_mask.about,plan.task_mask.video,plan.policy_version,plan.planner_config_version,plan.capacity.version]);
      await query(`INSERT INTO feature_clock.dispatch_outbox(dispatch_event_id,plan_id,job_id,queue_name,payload_json,payload_hash,status)
        VALUES($1,$2,$3,$4,$5,$6,'published')`, [randomUUID(),planId,job.id,job.queueName,plan,incrementalPlanHash(plan)]);
      return { job, plan };
    }
    const {job,plan} = await createJob();
    const {channel_id:channelId,plan_id:planId} = plan;
    const preparer = new ProxyBusinessRunPreparer({ queryFn: query, withTransaction: transaction, resolvedPolicy,
      incrementalRunStore: new IncrementalRunStore({ withTransaction: transaction }) });
    const workerId = `remote-${nodeId}`; const sources = new Map(); const calls = []; const visits = []; const receipts = []; const checkpoints = [];
    let routeGeneration = 1; const attemptNumbers = new Map(); let activeCountry = kind === 'country' ? 'US' : 'BR'; let activeProfile;
    const leaseId = randomUUID();
    let workerInstanceId='instance-1';
    const assignment = () => ({ ok: true, ready: true, protocol_version: 2, control_state: 'leased_idle', role: 'channel', workload_scope: 'qy-production',
      worker_id: workerId, worker_instance_id: workerInstanceId, slot_name: `rota-${nodeId}`, proxy_user: `worker-g${routeGeneration}`,
      lease_id: leaseId, lease_remaining_ms: 60000, server_time: new Date().toISOString(), route_generation: routeGeneration,
      credential_generation: routeGeneration, network_identity_key: `network-${nodeId}-${routeGeneration}`, profile_epoch: routeGeneration - 1,
      identity_policy_id: resolvedPolicy.policy.id, identity_policy_version: resolvedPolicy.policy.version, identity_policy_hash: resolvedPolicy.policy.hash,
      identity_action: routeGeneration === 1 ? 'keep' : 'rotate_profile', egress_country: activeCountry });
    const routes = new RemoteChannelRouteStore({ channelStore, assertBusinessFence: assertRemoteIncrementalBusinessFence,
      privateKey: keypair.privateKey, secretKey: randomBytes(32), readRotaRoute: async fence => {
        const source = sources.get(fence.task_id); if (!source) throw new Error('unknown Rota attempt');
        return { ...source, ...fence, route_lease_until_ms: Date.now() + 60000 };
      } });
    await store.registerNode({ nodeId, token, capabilities: [CHANNEL_PLAN_CAPABILITY] });
    await routes.registerSlot(nodeId, 'worker-1', workerId);
    const sessions = new RemoteYoutubeSessionStore({ routes });
    let supervisor;
    const activation=supervised?new RemoteWorkerActivationStore({store,verifyExecution:(client,row)=>supervisor?.verifyExecution(client,row)??false}):null;
    const nodeConfig={version:1,mode:'incremental_collect',node_id:nodeId,slot:'worker-1',deployment_id:randomUUID(),config_hash:randomBytes(32).toString('hex')};
    if(supervised)await activation.register({nodeId,slot:'worker-1',deploymentId:nodeConfig.deployment_id,configHash:nodeConfig.config_hash});
    const gateway = createRemoteNodeGateway({ store, channelPlans: channelStore, routes, youtubeSessions: sessions, ...(activation?{workerConnections:activation}:{}) });
    gateway.listen(0, '127.0.0.1'); await once(gateway, 'listening');
    const client = createRemoteNodeClient({ url: `http://127.0.0.1:${gateway.address().port}`, token, allowLoopbackHttp: true });
    const directory = await mkdtemp(join(tmpdir(), 'remote-handoff-'));
    await writeFile(join(directory,'public.pem'), keypair.publicKey.export({ type:'spki',format:'pem' }), { mode:0o600 });
    await writeFile(join(directory,'token'), token, { mode:0o600 });
    const relay = spawn(binary, ['-node-id',nodeId,'-public-key-file',join(directory,'public.pem'),'-control-token-file',join(directory,'token'),
      '-proxy-listen','127.0.0.1:0','-control-listen','127.0.0.1:0'], { env: { GOMAXPROCS:'2' },stdio:['ignore','pipe','pipe'] });
    relay.stderr.resume(); const relayExit = once(relay,'exit');
    const lines = createInterface({ input:relay.stdout }); const [line] = await once(lines,'line',{ signal:AbortSignal.timeout(5000) });
    const startup = JSON.parse(line); const localRota = createLocalRotaClient({ nodeId, token, controlUrl:`http://${startup.control_address}`,proxyUrl:`http://${startup.proxy_address}` });
    const createApiFallback = api ? ({ query,withTransaction }) => createVideoDetailApiFallback({ query,withTransaction,
      loadSettings: async () => ({ fallbackMode:'enabled',apiKeys:['fixture'],dailyRequestLimit:100 }), wait:waitForVideoApiDetail }) : null;
    const central = new RemoteManagedIncrementalRuntime({ channelStore, routes, youtubeSessions:sessions, nodeId, slot:'worker-1',
      profileSecret:randomBytes(32).toString('hex'), createApiFallback, pollMs:5, claimTimeoutMs:10000, stopTimeoutMs:7000 });
    const rota = new RotaSlotAdapter({ role:'channel',workerId,workerInstanceId:'instance-1',resolvedPolicy,
      proxyBaseUrl:'http://unused-center.invalid:8000',proxyPassword:'never-sent',identityRuntime:central, renewIntervalMs:60000,
      client: {
        claim: async () => assignment(), renew: async () => assignment(),
        beginTask: async request => { const attemptNumber = (attemptNumbers.get(request.business_run_id) || 0) + 1;
          attemptNumbers.set(request.business_run_id,attemptNumber); const taskId = randomUUID();
          calls.push({ event:'begin', ...request, task_id:taskId });
          sources.set(taskId,{ ...assignment(),upstream:{ protocol:'http',address:'127.0.0.1:9',username:'unused',password:'unused' } });
          return { ...request,ok:true,task_id:taskId,attempt_number:attemptNumber,started_at:new Date().toISOString() }; },
        businessRunBudget: async runId => ({ business_tasks_limit:3,business_tasks_used:attemptNumbers.get(runId) || 0 }),
        observe: async request => { calls.push({ event:'observe',...request }); return { ok:true,...request }; },
        completeTask: async request => {
          calls.push({ event:'complete',...request });
          const records = (await query(`SELECT b.state,a.status,a.finished_at FROM remote_ingestion.network_bindings b
            JOIN remote_ingestion.tasks t ON t.task_id=b.task_id
            JOIN crawler.channel_execution_attempts a ON a.attempt_id='channel-attempt:' || (b.rota_fence->>'task_id')
            WHERE b.rota_fence->>'task_id'=$1`, [request.task_id])).rows;
          assert.ok(records.every(row => row.state === 'retired' && row.status !== 'running' && row.finished_at));
          assert.equal(request.active_managed_requests,0);
          const switching = (request.outcome === 'failed' && request.observation_ids?.length > 0)
            || (request.recheck_country && reserve);
          if (switching) { routeGeneration++; if (request.recheck_country) activeCountry=request.recheck_country; }
          return { ...request,ok:true,task_completed:true,completed_task_route_generation:request.route_generation,
            control_state:switching?'PENDING_NEW_ROUTE':'READY_KEEP_ROUTE',ready:!switching,
            ...(switching?{ pending_route_generation:routeGeneration,retry_after_ms:1 }:{}),
            reason_code:request.recheck_country&&!reserve?'NO_COUNTRY_RESERVE':'WAITING_FOR_ROUTE_REFRESH' };
        },
        release: async request => ({ ...request,ok:true,released:true,route_generation:request.known_route_generation,status:'released',released_at:new Date().toISOString() }),
      } });
    const videoId=randomBytes(8).toString('hex').slice(0,11);
    const secondVideoId=randomBytes(8).toString('hex').slice(0,11); let detailAttempts=0;
    const youtube = { acquire:acquireYoutubeJs,release:releaseYoutubeJs,close:closeYoutubeJs,
      openChannel:async (requestedChannelId,options) => {
        visits.push({ country:activeCountry,profile:activeProfile });
        if (kind==='network' && visits.length===1) throw Object.assign(new Error('fingerprint gateway proxy_transport connection failed'),{ code:'FINGERPRINT_PROXY_TRANSPORT',source:'fingerprint_gateway' });
        return { about_requested:options.includeAbout,about_observed:options.includeAbout,metadata:{ channel_id:requestedChannelId,title:'After handoff',
          subscriber_count_text:'1,234 subscribers',subscriber_count_source:'youtube_about',view_count_text:'98,765 views',view_count_source:'youtube_about',
          video_count_text:'1 video',video_count_source:'youtube_about',keywords:[],external_links:[],external_links_status:'observed',available_tabs:['videos'] },raw:{ engine:'fixture' },
          scanUploads:async () => ({ ...(kind==='country'?{ empty_uploads:emptyUploadsDecision('BR') }:{}),
            entries:api?(remainingVideo?[videoId,secondVideoId]:[videoId]).map((id,index)=>({ id,title:'Discovered',position:index+1,published_at:new Date().toISOString(),published_day:new Date().toISOString().slice(0,10),
              published_at_status:'exact',published_at_precision:'date_only',published_at_source:'youtubejs_feed' })):[],
            complete:true,pages:1,item_count:api?(remainingVideo?2:1):0,parse_gap_count:0,anchor_matched:false,stop_reason:'list_end',terminal_reason:'list_end',raw:{engine:'fixture'} }) };
      },
      fetchDetail:async id => { detailAttempts++; if (id===secondVideoId) return detail(id); throw Object.assign(new Error('required view_count missing'),
        { name:'YoutubeJsRequiredSurfaceError',required_surface:'player',partial_detail:{ id:videoId,like_count:12 } }); },
    };
    const spool=new RemoteResultSpool({directory:join(directory,'spool')});
    let resolveGrantStarted; const grantStarted=new Promise(resolve=>{resolveGrantStarted=resolve;});
    const nodeClient={ ...client, grantRoute:request=>{resolveGrantStarted();return client.grantRoute(request);}, uploadCommand:async (lease,bytes) => { receipts.push({lease,bytes}); return client.uploadCommand(lease,bytes); },
      youtubeCheckpoint:async value => { checkpoints.push(value); return client.youtubeCheckpoint(value); } };
    const workerOptions={client:nodeClient,localRota,slot:'worker-1',spool,youtube,pollMs:5,renewMs:100,timeoutMs:15000,
      gateway:{ prepare:async ({profileGroup}) => {activeProfile=profileGroup.profile_group_id;},snapshot:async()=>({cookies:[]}),close:async()=>{},fetch:()=>assert.fail('fixture does not access YouTube') } };
    const nodeAbort=new AbortController();const workerErrors=[];
    const worker=supervised?new RemoteIncrementalProcess({...workerOptions,config:nodeConfig,intervalMs:50,
      createWorker:args=>createRemoteIncrementalWorker({...workerOptions,...args})}):createRemoteIncrementalWorker(workerOptions);
    const pump=startNode?worker.run({signal:nodeAbort.signal,pollMs:5,onStatus:status=>{
      if(status.status==='retrying')workerErrors.push(status);
    }}):Promise.resolve();
    pump.catch(error=>tt.diagnostic(`node pump failed: ${error.stack}`));
    tt.after(async()=>{
      if(supervised){await supervisor?.stop();nodeAbort.abort();}else worker.stop();
      try { await rota.close(); await pump; }
      finally {
        relay.kill('SIGTERM');await relayExit;
        await new Promise(resolve=>{gateway.close(resolve);gateway.closeAllConnections();});
        await rm(directory,{recursive:true,force:true});
      }
    });
    let queue,queueEvents,supervisorArgs;
    if(supervised){
      const port=Number(process.env.REMOTE_NODE_TEST_REDIS_PORT);assert.ok(Number.isInteger(port)&&port>1024);
      const redis={host:'127.0.0.1',port,password:'remote-center-fixture-only',maxRetriesPerRequest:null};
      const prefix='remote-supervisor-test-'+randomUUID();
      const guardPool=new pg.Pool({connectionString:url,max:4,connectionTimeoutMillis:2000});
      queue=new Queue(INCREMENTAL_QUEUE,{connection:redis,prefix});queueEvents=new QueueEvents(INCREMENTAL_QUEUE,{connection:redis,prefix});
      await queueEvents.waitUntilReady();
      tt.after(async()=>{await supervisor?.stop();await queueEvents.close();await queue.obliterate({force:true});await queue.close();await guardPool.end();});
      supervisorArgs={store,channelStore,routes,youtubeSessions:sessions,activation,guardPool,connection:redis,prefix,allowedNodeIds:[nodeId],resolvedPolicy,
        profileSecret:central.executions.profileSecret,rotaClient:rota.client,proxyBaseUrl:'http://unused-center.invalid:8000',proxyPassword:'fixture',intervalMs:50,
        createApiFallback,createRuntime:args=>{central.executions.assertAdmission=async client=>{const ok=await args.assertAdmission(client);if(!ok){const e=[...supervisor.entries.values()][0];tt.diagnostic(JSON.stringify({admission:false,owned:e?.owned,closing:e?.closing,aborting:e?.aborting,redis:e?.redis?.status,rota:e?.rota.status(),entries:supervisor.entries.size}));}return ok;};central.assertAdmission=central.executions.assertAdmission;return central;},
        createRota:args=>{workerInstanceId=args.workerInstanceId;rota.workerInstanceId=workerInstanceId;return rota;},
        createProcessor:args=>createCenterIncrementalProcessor({...args,apiDelayMs:200}),report:event=>tt.diagnostic(JSON.stringify(event))};
      supervisor=new RemoteCenterExecutionSupervisor(supervisorArgs);if(startSupervisor)supervisor.start();
    }else await rota.start();
    const execute=(currentJob=job)=>rota.executeJob(currentJob,{prepare:()=>preparer.prepareChannel(remotePlanJob(currentJob)),executeAttempt:(prepared,attempt)=>executeManagedWorkerAttempt({
      job:currentJob,prepared,attempt,execute:()=>central.executePlan(),persistRetryableCheckpoint:async()=>{
        assert.equal((await query('SELECT status FROM crawler.channel_runs WHERE plan_id=$1',[currentJob.data.plan_id])).rows[0].status,'failed');return true;
      } }) });
    return {store,channelStore,central,client,sessions,job,plan,nodeId,slot:'worker-1',calls,visits,receipts,checkpoints,spool,videoId,createApiFallback,execute,pump,worker,preparer,assignment,rota,routes,nodeClient,
      detailAttempts:()=>detailAttempts,workerErrors,query,createJob,secondVideoId,grantStarted,supervisor,supervisorArgs,activation,queue,queueEvents};
  }

  for(const stage of ['pending','active','committed']) await t.test(`SIGKILL center recovers the same BullMQ Plan without resetting attempts (stage=${stage})`, {skip:!process.env.REMOTE_NODE_TEST_REDIS_PORT}, async tt => {
    const hasNetwork=stage!=='pending';const activeNetwork=stage==='active';
    const f=await fixture(tt,{kind:'about',startNode:hasNetwork});
    const row={node_id:f.nodeId,slot:f.slot,rota_worker_id:f.assignment().worker_id};
    const prepared=await f.preparer.prepareChannel(remotePlanJob(f.job));
    const task=await f.rota.client.beginTask({business_run_id:prepared.businessRunId,job_execution_id:'crash-test'});
    const queueConfig={connection:{host:'127.0.0.1',port:Number(process.env.REMOTE_NODE_TEST_REDIS_PORT),password:'remote-center-fixture-only',maxRetriesPerRequest:null},
      prefix:'remote-center-kill-'+randomUUID()};
    const queue=new Queue(INCREMENTAL_QUEUE,queueConfig);const events=new QueueEvents(INCREMENTAL_QUEUE,queueConfig);
    await events.waitUntilReady();let replacement;
    tt.after(async()=>{await replacement?.close();await events.close();await queue.obliterate({force:true});await queue.close();});
    const queued=await queue.add(f.job.name,f.plan,{jobId:f.job.id,attempts:1});
    const child=fork(new URL('./fixtures/remoteAdmissionCrash.mjs',import.meta.url),[],{stdio:['ignore','ignore','pipe','ipc']});
    child.stderr.resume();const exited=once(child,'exit');
    tt.after(async()=>{if(child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');await exited;});
    const message=once(child,'message',{signal:AbortSignal.timeout(10000)});
    child.send({row,queue:queueConfig,profileSecret:f.central.executions.profileSecret,args:{assignment:f.assignment(),task,prepared,policy:resolvedPolicy.policy,nodeId:f.nodeId,slot:f.slot}});
    const started=(await message)[0];assert.ok(started.admission,JSON.stringify(started));
    let release;let binding;
    if(hasNetwork){
      const lease=await f.central.executions.waitClaim(started.admission,{nodeId:f.nodeId,slot:f.slot,signal:AbortSignal.timeout(5000),pollMs:5});
      const a=f.assignment();
      binding=await f.sessions.bind({nodeId:f.nodeId,lease,slot:f.slot,attemptId:started.admission.attemptId,profileGroup:started.admission.profileGroup,
        rotaFence:{slot_name:a.slot_name,worker_id:a.worker_id,worker_instance_id:a.worker_instance_id,lease_id:a.lease_id,
          route_generation:a.route_generation,task_id:task.task_id,business_run_id:prepared.businessRunId,job_execution_id:'crash-test'}});
      await eventually(async()=>(await f.query('SELECT state FROM remote_ingestion.network_bindings WHERE binding_id=$1',[binding.binding_id])).rows[0].state==='active');
      if(activeNetwork){
        const barrier=new Promise(resolve=>{release=resolve;});tt.after(()=>release());
        const originalRelease=f.nodeClient.releaseRoute;
        f.nodeClient.releaseRoute=async receipt=>{await barrier;return originalRelease(receipt);};
      }else{
        const completion=once(child,'message',{signal:AbortSignal.timeout(10000)});child.send({lease});
        assert.equal((await completion)[0].completed?.status,'done');
        await eventually(async()=>(await f.query('SELECT state FROM remote_ingestion.network_bindings WHERE binding_id=$1',[binding.binding_id])).rows[0].state==='retired');
      }
    }
    const guard=await pool.connect();
    tt.after(()=>guard.release(true));
    assert.equal((await guard.query('SELECT pg_try_advisory_lock(781138012,hashtext($1)) AS locked',[supervisionLockKey(row)])).rows[0].locked,false);
    const {recoverRemoteSlot}=await import('../src/remoteNodes/centerExecutionRecovery.js');
    await assert.rejects(recoverRemoteSlot({guard,row,lockKey:supervisionLockKey(row),profileSecret:f.central.executions.profileSecret}),{code:'REMOTE_RECOVERY_NOT_OWNER'});
    child.kill('SIGKILL');assert.equal((await exited)[1],'SIGKILL');
    await guard.query('SELECT pg_advisory_lock(781138012,hashtext($1))',[supervisionLockKey(row)]);
    // New owner must close only the orphan transport/attempt, not the Plan.
    const recover=()=>recoverRemoteSlot({guard,row,lockKey:supervisionLockKey(row),profileSecret:f.central.executions.profileSecret,checkpoints:f.central.checkpoints});
    const first=await recover();
    if(activeNetwork){
      assert.equal(first.settled,false,'a route timeout or stop request is not a quiescence receipt');
      assert.equal((await f.query('SELECT status FROM crawler.channel_execution_attempts WHERE attempt_id=$1',[started.admission.attemptId])).rows[0].status,'running');
      assert.equal((await f.query('SELECT stop_requested FROM remote_ingestion.network_bindings WHERE binding_id=$1',[binding.binding_id])).rows[0].stop_requested,true);
      release();await eventually(async()=>(await f.query('SELECT state FROM remote_ingestion.network_bindings WHERE binding_id=$1',[binding.binding_id])).rows[0].state==='retired');
      assert.equal((await recover()).settled,true);
    }else assert.equal(first.settled,true);
    assert.deepEqual(await recover(),{settled:true,closed:0},'recovery is idempotent');
    if(stage==='committed')assert.ok((await f.query('SELECT profile_applied_at FROM remote_ingestion.youtube_sessions WHERE binding_id=$1',[binding.binding_id])).rows[0].profile_applied_at);
    const old=(await f.query('SELECT * FROM crawler.channel_execution_attempts WHERE attempt_id=$1',[started.admission.attemptId])).rows[0];
    assert.equal(old.status,stage==='committed'?'success':'aborted');assert.ok(old.finished_at);
    assert.equal((await f.query('SELECT state FROM remote_ingestion.tasks WHERE task_id=$1',[started.admission.taskId])).rows[0].state,stage==='committed'?'applied':'failed');
    await assert.rejects(f.store.transaction(async client=>{
      const transport=(await client.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1',[started.admission.taskId])).rows[0];
      return assertRemoteIncrementalBusinessFence(client,transport);
    }),{code:'INCREMENTAL_BUSINESS_FENCE_STALE'});
    const pump=hasNetwork?f.pump:f.worker.run({pollMs:5});
    replacement=new Worker(INCREMENTAL_QUEUE,createCenterIncrementalProcessor({channelStore:f.channelStore,runtime:f.central,rota:f.rota,resolvedPolicy,
      createApiFallback:f.createApiFallback,ready:async()=>true}),{...queueConfig,lockDuration:3000,stalledInterval:500});
    replacement.on('error',()=>{});
    try {assert.equal((await queued.waitUntilFinished(events,20000)).status,'done');}finally{f.worker.stop();await pump;}
    const delivered=await queue.getJob(queued.id);assert.equal(delivered.attemptsStarted,2,'BullMQ redelivers the same job after the old lock expires');
    const attempts=stage==='committed'?1:2;
    assert.equal(f.calls.filter(c=>c.event==='begin').length,attempts);
    assert.equal((await f.query('SELECT max(attempt_number)::int AS n FROM crawler.channel_execution_attempts WHERE channel_id=$1',[f.plan.channel_id])).rows[0].n,attempts);
    assert.equal((await f.query('SELECT count(*)::int AS n FROM crawler.channel_runs WHERE plan_id=$1',[f.plan.plan_id])).rows[0].n,1);
    assert.equal((await f.query("SELECT count(*)::int AS n FROM crawler.crawl_observations WHERE channel_id=$1 AND observation_kind='about'",[f.plan.channel_id])).rows[0].n,1);
  });

  await t.test('replacement supervisor automatically settles killed admission and resumes real queue intake', {skip:!process.env.REMOTE_NODE_TEST_REDIS_PORT}, async tt=>{
    const f=await fixture(tt,{kind:'about',supervised:true,startSupervisor:false});
    const row={node_id:f.nodeId,slot:f.slot,rota_worker_id:f.assignment().worker_id};
    const prepared=await f.preparer.prepareChannel(remotePlanJob(f.job));
    const task=await f.rota.client.beginTask({business_run_id:prepared.businessRunId,job_execution_id:'supervisor-crash-test'});
    const queued=await f.queue.add(f.job.name,f.plan,{jobId:f.job.id,attempts:1});
    const child=fork(new URL('./fixtures/remoteAdmissionCrash.mjs',import.meta.url),[],{stdio:['ignore','ignore','pipe','ipc']});
    child.stderr.resume();const exited=once(child,'exit');
    tt.after(async()=>{if(child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');await exited;});
    const message=once(child,'message',{signal:AbortSignal.timeout(10000)});
    child.send({row,queue:{connection:f.supervisorArgs.connection,prefix:f.supervisorArgs.prefix},profileSecret:f.central.executions.profileSecret,
      args:{assignment:f.assignment(),task,prepared,policy:resolvedPolicy.policy,nodeId:f.nodeId,slot:f.slot}});
    const started=(await message)[0];assert.ok(started.admission,JSON.stringify(started));
    child.kill('SIGKILL');assert.equal((await exited)[1],'SIGKILL');
    f.supervisor.start();
    // Production BullMQ uses its default stalled interval; this assertion does
    // not manually move/retry the old job or edit any transport state.
    assert.equal((await queued.waitUntilFinished(f.queueEvents,45000)).status,'done');
    assert.equal((await f.query('SELECT status FROM crawler.channel_execution_attempts WHERE attempt_id=$1',[started.admission.attemptId])).rows[0].status,'aborted');
    assert.equal(f.calls.filter(c=>c.event==='begin').length,2);
    assert.equal(f.visits.length,1);
  });

  for(const reserve of [true,false]) await t.test(`US empty list uses the original country handoff (Brazil reserve=${reserve})`,async tt=>{
    const f=await fixture(tt,{reserve});const result=await f.execute();assert.equal(result.status,'done');
    assert.deepEqual(f.visits.map(v=>v.country),reserve?['US','BR']:['US']);
    assert.equal(f.calls.filter(c=>c.event==='begin').length,2);
    assert.equal(f.calls.filter(c=>c.event==='observe').length,0);
    assert.equal(f.job.data.uploads_country_recheck.status,reserve?'checked':'unavailable');
    const row=(await f.query('SELECT * FROM remote_ingestion.tasks')).rows[0];
    assert.equal(row.generation,2);assert.equal(row.lease_failures,0);assert.deepEqual(row.input.plan,f.plan);
    assert.equal((await f.query('SELECT status FROM crawler.channels WHERE channel_id=$1',[f.plan.channel_id])).rows[0].status,'dormant');
    assert.equal((await f.query('SELECT count(*)::int AS n FROM remote_ingestion.execution_handoffs')).rows[0].n,1);
    assert.equal((await f.query("SELECT count(*)::int AS n FROM crawler.channel_execution_attempts WHERE channel_id=$1 AND status='success'",[f.plan.channel_id])).rows[0].n,2);
    if(reserve)assert.notEqual(f.visits[0].profile,f.visits[1].profile);
  });

  await t.test('lost country Job update re-enters original Rota country decision, without guessing a reserve',async tt=>{
    const f=await fixture(tt);let first=true;
    f.job.updateData=async data=>{if(first && data.uploads_country_recheck?.status==='requested'){first=false;throw new Error('lost country job update');}f.job.data=data;};
    await assert.rejects(f.execute(),/lost country job update/);
    assert.equal(f.job.data.uploads_country_recheck,undefined);
    assert.equal((await f.query('SELECT last_error FROM remote_ingestion.tasks')).rows[0].last_error,'UPLOADS_COUNTRY_RECHECK');
    assert.equal((await f.execute()).status,'done');
    assert.deepEqual(f.visits.map(v=>v.country),['US','BR']);
    assert.equal(f.job.data.uploads_country_recheck.status,'checked');
    assert.equal(f.calls.filter(c=>c.event==='begin').length,3,'recovery must charge the original Rota budget');
  });

  await t.test('durable API handoff reconstructs missing BullMQ continuation after center interruption',async tt=>{
    const f=await fixture(tt,{kind:'api',api:true});
    const result=await f.execute();assert.ok(result.video_api_pending);
    assert.equal(f.job.data.video_api_continuation,undefined);
    const process=createCenterIncrementalProcessor({channelStore:f.channelStore,runtime:f.central,rota:f.rota,resolvedPolicy,
      createApiFallback:f.createApiFallback,ready:async()=>true,apiDelayMs:10});
    await assert.rejects(process(f.job,'fixture'),{name:'DelayedError'});
    assert.equal(f.job.data.video_api_continuation.request_id,result.video_api_pending);
    await f.query("UPDATE crawler.youtube_api_detail_requests SET status='done',detail_json=$2,finished_at=now() WHERE request_id=$1",
      [result.video_api_pending,{...detail(f.videoId),source:'youtube_data_api_videos_list',privacy_status:'public'}]);
    assert.equal((await process(f.job,'fixture')).status,'done');
    assert.equal(f.calls.filter(c=>c.event==='begin').length,1,'API replay needs no new Rota task');
    assert.equal(f.detailAttempts(),3,'no repeated YouTube detail request');
  });

  await t.test('network failure settles its node, switches through original Rota, and retains one run and budget',async tt=>{
    const f=await fixture(tt,{kind:'network'});const result=await f.execute();assert.equal(result.status,'done');
    assert.equal(f.calls.filter(c=>c.event==='observe').length,1);assert.equal(f.calls.find(c=>c.event==='observe').kind,'proxy_transport');
    assert.equal(f.calls.filter(c=>c.event==='begin').length,2);assert.equal(new Set(f.calls.filter(c=>c.event==='begin').map(c=>c.business_run_id)).size,1);
    assert.notEqual(f.visits[0].profile,f.visits[1].profile);
    const attempts=(await f.query('SELECT status FROM crawler.channel_execution_attempts WHERE channel_id=$1 ORDER BY attempt_number',[f.plan.channel_id])).rows;
    assert.deepEqual(attempts.map(v=>v.status),['failed','success']);
    const first=f.receipts.find(r=>r.lease.generation===1);
    assert.equal((await f.client.uploadCommand(first.lease,first.bytes)).durable,true);
    const {value}=await decodeResult(first.bytes);
    await assert.rejects(f.client.uploadCommand(first.lease,await encodeResult({...value,batch_id:randomUUID()})),{code:'BATCH_CONFLICT'});
    assert.equal((await f.client.youtubeCheckpoint(f.checkpoints[0])).durable,true);
    await assert.rejects(f.client.pollCommands(first.lease),{code:'STALE_LEASE'});
    assert.equal((await f.query('SELECT count(*)::int AS n FROM crawler.crawl_observations WHERE run_id=$1',[`incremental:${f.plan.plan_id}`])).rows[0].n,1);
  });

  await t.test('API waits release the Worker; stored API replay finishes without another Rota task or YouTube request',async tt=>{
    const f=await fixture(tt,{kind:'api',api:true});
    await assert.rejects(runVideoApiResumable({job:f.job,token:'fixture',execute:f.execute,executeReplay:()=>assert.fail('no continuation yet')}),{name:'DelayedError'});
    assert.equal(f.detailAttempts(),3);assert.equal(f.calls.filter(c=>c.event==='begin').length,1);
    const transport=(await f.query('SELECT * FROM remote_ingestion.tasks')).rows[0];assert.equal(transport.state,'received');
    assert.equal(f.calls.find(c=>c.event==='complete').api_continuation,true);
    assert.equal((await f.query("SELECT count(*)::int AS n FROM remote_ingestion.tasks WHERE state='leased'")).rows[0].n,0);
    await assert.rejects(gateVideoApiJob({query:f.query,job:f.job,token:'fixture'}),{name:'DelayedError'});
    const other=await f.createJob({about:true,video:false});
    assert.equal((await f.execute(other.job)).status,'done');
    assert.equal((await f.query('SELECT status FROM crawler.youtube_api_detail_requests WHERE request_id=$1',[f.job.data.video_api_continuation.request_id])).rows[0].status,'pending');
    const requestId=f.job.data.video_api_continuation.request_id;
    await assert.rejects(runRemoteIncrementalApiReplay({channelStore:f.channelStore,taskId:transport.task_id,requestId,createApiFallback:f.createApiFallback}),{code:'REMOTE_API_NOT_READY'});
    await f.query("UPDATE crawler.youtube_api_detail_requests SET status='done',detail_json=$2,finished_at=now() WHERE request_id=$1",
      [requestId,{...detail(f.videoId),source:'youtube_data_api_videos_list',privacy_status:'public'}]);
    const executeReplay=()=>runRemoteIncrementalApiReplay({channelStore:f.channelStore,taskId:transport.task_id,requestId,createApiFallback:f.createApiFallback,pollMs:5});
    const result=await runVideoApiResumable({job:f.job,token:'fixture',execute:()=>assert.fail('stored evidence needs no remote lease'),executeReplay});
    assert.equal(result.status,'done');assert.equal(f.calls.filter(c=>c.event==='begin').length,2);assert.equal(f.detailAttempts(),3);
    assert.equal((await executeReplay()).duplicate,true);
    const stored=(await f.query('SELECT view_count,like_count,comment_count FROM crawler.contents WHERE channel_id=$1 AND source_content_id=$2',[f.plan.channel_id,f.videoId])).rows[0];
    assert.deepEqual(Object.values(stored).map(Number),[321,12,0]);
  });

  await t.test('API evidence followed by an untouched video resumes through the original Rota budget',async tt=>{
    const f=await fixture(tt,{kind:'api',api:true,remainingVideo:true});
    await assert.rejects(runVideoApiResumable({job:f.job,token:'fixture',execute:f.execute,executeReplay:()=>assert.fail('no continuation yet')}),{name:'DelayedError'});
    const transport=(await f.query('SELECT * FROM remote_ingestion.tasks')).rows[0];
    const requestId=f.job.data.video_api_continuation.request_id;
    await f.query("UPDATE crawler.youtube_api_detail_requests SET status='done',detail_json=$2,finished_at=now() WHERE request_id=$1",
      [requestId,{...detail(f.videoId),source:'youtube_data_api_videos_list',privacy_status:'public'}]);
    const executeReplay=()=>runRemoteIncrementalApiReplay({channelStore:f.channelStore,taskId:transport.task_id,requestId,createApiFallback:f.createApiFallback,pollMs:5});
    // The original replay guard must stop before charging a new network item.
    await assert.rejects(executeReplay(),{code:'VIDEO_API_NETWORK_REQUIRED'});
    assert.equal(f.detailAttempts(),3);
    assert.equal(f.calls.filter(c=>c.event==='begin').length,1);
    const result=await runVideoApiResumable({job:f.job,token:'fixture',execute:f.execute,executeReplay});
    assert.equal(result.status,'done');assert.equal(f.detailAttempts(),4);
    assert.equal(f.calls.filter(c=>c.event==='begin').length,2);
    assert.equal(new Set(f.calls.filter(c=>c.event==='begin').map(c=>c.business_run_id)).size,1);
    const current=(await f.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1',[transport.task_id])).rows[0];
    assert.equal(current.generation,2);assert.equal(current.lease_failures,0);assert.deepEqual(current.input.plan,f.plan);
    assert.equal((await f.query('SELECT count(*)::int AS n FROM crawler.contents WHERE channel_id=$1',[f.plan.channel_id])).rows[0].n,2);
    assert.equal((await f.query('SELECT count(*)::int AS n FROM crawler.youtube_api_detail_requests WHERE run_id=$1',[`incremental:${f.plan.plan_id}`])).rows[0].n,1);
  });

  await t.test('cancelled Clock rolls back remote admission and the new original execution attempt',async tt=>{
    const f=await fixture(tt,{kind:'about'});
    const prepare=f.central.executions.prepare.bind(f.central.executions);
    f.central.executions.prepare=async args=>{
      await f.query("UPDATE feature_clock.daily_channel_plans SET status='cancelled',error_code='manual_cancel' WHERE plan_id=$1",[f.plan.plan_id]);
      return prepare(args);
    };
    await assert.rejects(f.execute(),{code:'INCREMENTAL_BUSINESS_FENCE_STALE'});
    assert.equal(f.visits.length,0);
    assert.equal((await f.query('SELECT count(*)::int AS n FROM remote_ingestion.tasks')).rows[0].n,0);
    assert.equal((await f.query('SELECT count(*)::int AS n FROM crawler.channel_execution_attempts WHERE channel_id=$1',[f.plan.channel_id])).rows[0].n,0);
  });

  for(const afterActivation of [false,true]) await t.test(`lost admission acknowledgement resumes in the same Worker (activation started=${afterActivation})`,async tt=>{
    const f=await fixture(tt,{kind:'about'});
    const prepare=f.central.executions.prepare.bind(f.central.executions);
    let first=true;let abandonedRequest;
    f.central.executions.prepare=async args=>{
      const admitted=await prepare(args);
      if(first){
        first=false;
        await f.central.executions.waitClaim(admitted,{nodeId:f.nodeId,slot:f.slot,signal:AbortSignal.timeout(5000),pollMs:5});
        if(afterActivation){
          await f.grantStarted;
          abandonedRequest=(await f.spool.read('network.json')).request;
          await assert.rejects(f.client.abandonRoute(abandonedRequest),{code:'NETWORK_ABANDON_STALE'});
        }
        throw new Error('fixture lost admission acknowledgement');
      }
      return admitted;
    };
    await assert.rejects(f.execute(),/fixture lost admission acknowledgement/);
    const stopped=(await f.query('SELECT * FROM remote_ingestion.tasks')).rows[0];
    assert.equal(stopped.state,'failed');assert.equal(f.visits.length,0);
    if(abandonedRequest){
      assert.equal((await f.client.abandonRoute(abandonedRequest)).abandoned,true);
      await assert.rejects(f.client.abandonRoute({...abandonedRequest,generation:abandonedRequest.generation+100}),{code:'NETWORK_ABANDON_STALE'});
      await assert.rejects(f.client.abandonRoute({...abandonedRequest,slot:'another-worker'}),{code:'NETWORK_ABANDON_STALE'});
    }
    assert.equal((await f.query('SELECT status FROM crawler.channel_execution_attempts WHERE attempt_id=$1',[stopped.context.execution_attempt_id])).rows[0].status,'failed');
    f.job.attemptsMade=1;f.job.attemptsStarted=2;
    assert.equal((await f.execute()).status,'done');
    const resumed=(await f.query('SELECT * FROM remote_ingestion.tasks')).rows[0];
    assert.equal(resumed.task_id,stopped.task_id);assert.deepEqual(resumed.input.plan,f.plan);
    assert.equal(resumed.context.execution_options.resume_mode,'bullmq_redelivery_resume');
    assert.equal(f.calls.filter(c=>c.event==='begin').length,2);
  });

  async function eventually(check,timeout=15000){const end=Date.now()+timeout;while(Date.now()<end){if(await check())return;await delay(25);}assert.fail('expected supervised state did not arrive');}
  await t.test('real BullMQ consumer runs original Clock through supervised remote process',{skip:!process.env.REMOTE_NODE_TEST_REDIS_PORT},async tt=>{
    const f=await fixture(tt,{kind:'about',supervised:true});
    const job=await f.queue.add(f.job.name,f.plan,{jobId:f.job.id,attempts:1});
    const result=await job.waitUntilFinished(f.queueEvents,25000);assert.equal(result.status,'done');
    assert.equal(await job.getState(),'completed');assert.equal(f.visits.length,1);
    assert.equal((await f.query('SELECT title FROM crawler.channels WHERE channel_id=$1',[f.plan.channel_id])).rows[0].title,'After handoff');
    assert.equal((await f.query('SELECT state FROM remote_ingestion.tasks')).rows[0].state,'applied');
    const rival=new RemoteCenterExecutionSupervisor({...f.supervisorArgs,createRota:()=>assert.fail('duplicate center must not allocate Rota')});
    await rival.tick();assert.equal(rival.entries.size,0);await rival.stop();
    await f.activation.drain(f.nodeId,f.slot);
    await eventually(()=>[...f.supervisor.entries.values()][0]?.worker.isPaused());
    const second=await f.createJob({about:true,video:false});const pending=await f.queue.add(second.job.name,second.plan,{jobId:second.job.id});
    await delay(250);assert.ok(['waiting','delayed'].includes(await pending.getState()));assert.equal(f.visits.length,1);
    await f.supervisor.stop();assert.equal((await f.query('SELECT enabled FROM remote_ingestion.worker_connections WHERE node_id=$1',[f.nodeId])).rows[0].enabled,false);
  });
  await t.test('real BullMQ API wait releases slot for another channel and replays stored result',{skip:!process.env.REMOTE_NODE_TEST_REDIS_PORT},async tt=>{
    const f=await fixture(tt,{kind:'api',api:true,supervised:true});
    const first=await f.queue.add(f.job.name,f.plan,{jobId:f.job.id,attempts:1});
    await eventually(async()=>!!(await f.query('SELECT request_id FROM crawler.youtube_api_detail_requests WHERE run_id=$1',[`incremental:${f.plan.plan_id}`])).rows[0]);
    const other=await f.createJob({about:true,video:false});const next=await f.queue.add(other.job.name,other.plan,{jobId:other.job.id,attempts:1});
    assert.equal((await next.waitUntilFinished(f.queueEvents,25000)).status,'done');
    await eventually(async()=>await first.getState()==='delayed');assert.equal(f.detailAttempts(),3);
    const request=(await f.query('SELECT request_id FROM crawler.youtube_api_detail_requests WHERE run_id=$1',[`incremental:${f.plan.plan_id}`])).rows[0];
    const authorize=f.central.assertAdmission;let releaseReplay;let markReplay;let held=false;
    const replayStarted=new Promise(resolve=>{markReplay=resolve;});const replayGate=new Promise(resolve=>{releaseReplay=resolve;});
    f.central.assertAdmission=async client=>{if(!held){held=true;markReplay();await replayGate;}return authorize(client);};
    await f.query("UPDATE crawler.youtube_api_detail_requests SET status='done',detail_json=$2,finished_at=now() WHERE request_id=$1",
      [request.request_id,{...detail(f.videoId),source:'youtube_data_api_videos_list',privacy_status:'public'}]);
    await replayStarted;const stopping=f.supervisor.stop();
    await eventually(()=>[...f.supervisor.entries.values()][0]?.closing);releaseReplay();
    assert.equal((await first.waitUntilFinished(f.queueEvents,25000)).status,'done');await stopping;
    assert.equal(f.calls.filter(c=>c.event==='begin').length,2,'API replay must not allocate another Rota task');
    const data=(await f.query('SELECT view_count,like_count,comment_count FROM crawler.contents WHERE channel_id=$1 AND source_content_id=$2',[f.plan.channel_id,f.videoId])).rows[0];
    assert.deepEqual(Object.values(data).map(Number),[321,12,0]);
  });
  await t.test('real BullMQ country handoff preserves the same Plan and finishes before graceful shutdown',{skip:!process.env.REMOTE_NODE_TEST_REDIS_PORT},async tt=>{
    const f=await fixture(tt,{supervised:true});const job=await f.queue.add(f.job.name,f.plan,{jobId:f.job.id,attempts:1});
    await eventually(()=>f.visits.length>0);
    const stop=f.supervisor.stop();
    assert.equal((await job.waitUntilFinished(f.queueEvents,25000)).status,'done');await stop;
    assert.deepEqual(f.visits.map(v=>v.country),['US','BR']);
    assert.equal((await f.query('SELECT count(*)::int AS n FROM remote_ingestion.execution_handoffs')).rows[0].n,1);
  });

});
