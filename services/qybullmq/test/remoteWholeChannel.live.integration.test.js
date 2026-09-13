import { WholeChannelStore } from '../src/remoteNodes/wholeChannelStore.js';
import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes, randomUUID } from 'node:crypto';
import { readFile, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import pg from 'pg';
import { startRemoteNatsCenter } from '../src/remoteNodes/natsCenter.js';
import { createRemoteNatsClient } from '../src/remoteNodes/natsClient.js';
import { createTransportSignals } from '../src/remoteNodes/transportSignals.js';
import { RemoteNodeStore } from '../src/remoteNodes/store.js';
import { RemoteChannelPlanStore } from '../src/remoteNodes/channelPlanStore.js';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';
import { assertRemoteIncrementalBusinessFence, enqueueRemoteIncrementalJob } from '../src/remoteNodes/incrementalBusinessFence.js';
import { incrementalPlanHash, INCREMENTAL_JOB_NAME, INCREMENTAL_QUEUE } from '../src/incrementalPlan.js';
import { IncrementalRunStore } from '../src/incrementalRunStore.js';
import { ProxyBusinessRunPreparer } from '../src/proxyBusinessRun.js';
import { BrowserProfileStore } from '../src/browserProfileStore.js';
import { resolveWorkerIdentityPolicy } from '../src/identityPolicyCatalog.js';
import { PUBLICATION_WRITER_VERSION } from '../src/publicationWriterVersion.js';
import { RemoteChannelPlanExecutor } from '../src/remoteNodes/channelPlanExecutor.js';
import { RemoteResultSpool } from '../src/remoteNodes/spool.js';
import { runRemoteIncrementalPlan } from '../src/remoteNodes/incrementalCoordinator.js';
import { CHANNEL_PLAN_CAPABILITY } from '../src/remoteNodes/channelPlanContract.js';

import { performance } from 'node:perf_hooks';
import { createServer } from 'node:net';
import { ChannelExecutionRuntimeAdapter } from '../src/channelExecutionRuntimeAdapter.js';
import { ChannelExecutionRuntime } from '../src/channelExecutionRuntime.js';
import { FingerprintGateway } from '../src/fingerprintGateway.js';
import { ProxyControlClient } from '../src/proxyControlClient.js';
import { RotaSlotAdapter } from '../src/rotaSlotAdapter.js';
import { buildManagedDiagnosticJob } from '../src/managedDiagnosticJob.js';
import { openYoutubeJsChannel, fetchYoutubeJsVideoDetail, closeYoutubeJs } from '../src/youtubeJs.js';
import { closeDb } from '../src/db.js';

// Explicit live opt-in. All application writes require the disposable database;
// only the separately configured diagnostic Rota slot touches a live service.
const enabled = process.env.REMOTE_WHOLE_LIVE === 'true';
const url = process.env.REMOTE_NODE_TEST_DATABASE_URL;
test('live YouTube old/whole comparison (isolated persistence, same diagnostic route)',
  {skip: !enabled, timeout: 600000}, async t => {
  assert.ok(url && process.env.DATABASE_URL === url);
  assert.ok(process.env.REMOTE_NATS_TEST_URL?.includes('127.0.0.1'));
  assert.equal(process.env.YOUTUBEJS_VIDEO_API_BATCH_FALLBACK, 'false');
  const pool = new pg.Pool({ connectionString: url, max: 6, options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}` });
  let slot;
  const guard = await pool.connect();
  t.after(async () => { try {await slot?.close();} finally {guard.release(); await pool.end();} });
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

  async function fixture(liveChannelId, mask = { about: true, video: false, agent: false }) {
    await query('TRUNCATE remote_ingestion.nodes,remote_ingestion.tasks CASCADE');
    const channelId = liveChannelId;
    await query('TRUNCATE feature_clock.daily_channel_plans,crawler.channels CASCADE');
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
    throw new Error('live comparison requires NATS');
  }
  const port = await new Promise(resolve => { const s=createServer(); s.listen(0,'127.0.0.1',()=>{
    const port=s.address().port; s.close(()=>resolve(port)); }); });
  const gateway = new FingerprintGateway({port});
  const workerId = `whole-live-${randomUUID()}`;
  const livePolicy = resolveWorkerIdentityPolicy({role:'channel',policyId:process.env.ROTA_IDENTITY_POLICY_ID,
    expectedWorkloadScope:process.env.ROTA_WORKLOAD_SCOPE_EXPECTED});
  const rotaClient = new ProxyControlClient();
  slot = new RotaSlotAdapter({client:rotaClient,role:'channel',workerId,resolvedPolicy:livePolicy,
    proxyBaseUrl:process.env.ROTA_PROXY_BASE_URL,proxyPassword:process.env.ROTA_BULLMQ_PROXY_PASSWORD,
    identityRuntime:new ChannelExecutionRuntimeAdapter({workerId,runtime:new ChannelExecutionRuntime({profileStore:profiles,gateway})}),
    maxRouteSwitchesPerExecution:0});
  t.after(async()=>{await slot.close();await closeYoutubeJs();await gateway.close();await rotaClient.close();await closeDb();});
  const startup=slot.start();
  const startupTimer=setTimeout(()=>slot.close(),30000);
  try {await startup;} finally {clearTimeout(startupTimer);}
  const output=[];
  const ids=String(process.env.REMOTE_WHOLE_LIVE_CHANNELS||'').split(',').filter(Boolean);
  assert.ok(ids.length>0 && ids.length<=3 && ids.every(id=>/^UC[\w-]{22}$/.test(id)));
  for(const [sample,channelId] of ids.entries()) {
    let anchor;
    const probe={...buildManagedDiagnosticJob({kind:'incremental_video_probe',channelId}),attemptsStarted:1};
    await slot.executeJob(probe,{prepare:async()=>({kind:'ready',businessRunId:`whole-live-seed:${randomUUID()}`,
      workloadKind:'channel_incremental',identityPolicyId:slot.policy.id,identityPolicyVersion:slot.policy.version,
      identityPolicyHash:slot.policy.hash,initialResumeMode:'initial'}),executeAttempt:async()=>{
        const page=await (await openYoutubeJsChannel(channelId,{includeAbout:false})).scanUploads({maxPages:1});
        anchor=page.entries[3];assert.ok(anchor?.id,'a real anchor is required');
        if(!anchor.published_at){const detail=await fetchYoutubeJsVideoDetail(anchor.id);
          anchor={...anchor,published_at:detail.published_at,published_at_status:detail.published_at_status,
            published_at_source:detail.published_at_source,published_at_precision:detail.published_at_precision};}
        assert.ok(anchor.published_at,'a real dated anchor is required');
        return {kind:'managed_work_complete',businessState:'terminal',result:{diagnostic:true}};
      }});

    for(const mode of sample%2===0?['nats','nats_whole']:['nats_whole','nats']) await t.test(`${sample+1} ${mode}`,async tt=>{
      const f=await fixture(channelId,{about:true,video:true,agent:false});
      await query("INSERT INTO crawler.contents(content_key,channel_id,content_type,source_content_id,published_at,title,published_at_status,published_at_source,published_at_precision) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        [`youtube:video:${anchor.id}`,channelId,anchor.content_type??'video',anchor.id,anchor.published_at,anchor.title,anchor.published_at_status,anchor.published_at_source,anchor.published_at_precision]);
      await query("INSERT INTO crawler.channel_domain_cursors(channel_id,observation_kind,anchor_video_ids) VALUES($1,'video',$2)",[channelId,[anchor.id]]);
      await enqueueRemoteIncrementalJob(channelStore,f.job,f);
      const nodeId=process.env.REMOTE_NATS_TEST_NODE_ID,token=process.env.REMOTE_NATS_TEST_TOKEN;
      await store.registerNode({nodeId,token,capabilities:[CHANNEL_PLAN_CAPABILITY]});
      const client=await transportClient(tt,nodeId,token,mode);
      const claimId=randomUUID(),lease=await client.claim(claimId);
      const directory=await mkdtemp(join(tmpdir(),'whole-live-spool-'));
      tt.after(()=>rm(directory,{recursive:true,force:true}));
      const metrics={mode,channel_id:channelId,started_at:new Date().toISOString(),operations:[],rpc:{},sql_calls:0,sql_ms:0,seed_anchor_id:anchor.id};

      // Include queries on checked-out transaction clients too, without logging SQL values.
      const originalConnect=pool.connect.bind(pool);
      pool.connect=(callback)=>{if(callback)return originalConnect(callback);return (async()=>{const c=await originalConnect();const original=c.query.bind(c);const release=c.release.bind(c);
        c.query=async(...args)=>{const at=performance.now();metrics.sql_calls++;try{return await original(...args);}finally{metrics.sql_ms+=performance.now()-at;}};
        c.release=(...args)=>{c.query=original;c.release=release;return release(...args);};return c;})();};
      // Count promise-based checked-out clients (including fenced transactions).
      // Callback-based Pool.query calls are excluded; this is not total server SQL.
      const instrumented={...client,claim:()=>client.claim(claimId)};
      for(const name of ['pollCommands','uploadCommand','wholeChannelInput','uploadWholeChannel','heartbeat']) {
        if(!client[name])continue;
        instrumented[name]=async(...args)=>{const at=performance.now();const m=metrics.rpc[name]??={calls:0,ms:0};m.calls++;
          try{return await client[name](...args);}finally{m.ms+=performance.now()-at;}};
      }
      const timed=async(name,action,videoId=null)=>{const at=performance.now();
        const entry={name,video_id:videoId,start_ms:at};metrics.operations.push(entry);
        try{const value=await action();if(name==='scan')entry.scan={count:value.entries?.length,complete:value.complete,stop:value.stop_reason};return value;}
        catch(e){entry.error=String(e.code||e.name);throw e;}finally{entry.end_ms=performance.now();entry.ms=entry.end_ms-at;}};
      let detailEnd=null,centerFinished=null;
      const yt={openChannel:async(id,options)=>{const snapshot=await timed('about',()=>openYoutubeJsChannel(id,options));
        const scan=snapshot.scanUploads.bind(snapshot);return {...snapshot,scanUploads:options=>timed('scan',()=>scan(options))};},
        fetchDetail:async(id,options)=>{try{return await timed('detail',()=>fetchYoutubeJsVideoDetail(id,options),id);}finally{detailEnd=performance.now();}}};
      const worker=new RemoteChannelPlanExecutor({client:instrumented,spool:new RemoteResultSpool({directory}),youtube:yt,
        withSession:(_lease,_options,invoke)=>invoke(),timeoutMs:180000});
      const started=performance.now();
      const coordinator=runRemoteIncrementalPlan({channelStore,lease,wholeChannels:channelStore.testWholeChannels,
        assertBusinessFence:assertRemoteIncrementalBusinessFence,signal:AbortSignal.timeout(180000)}).finally(()=>{centerFinished=performance.now();});
      // Attach failure handler immediately while the managed session is starting.
      const centerResult=coordinator.then(value=>({value}),error=>({error}));
      const job={...buildManagedDiagnosticJob({kind:'incremental_video_probe',channelId}),attemptsStarted:1};
      let nodeState,executionError;
      try {
        await slot.executeJob(job,{prepare:async()=>({kind:'ready',businessRunId:`whole-live:${randomUUID()}`,
          workloadKind:'channel_incremental',identityPolicyId:slot.policy.id,identityPolicyVersion:slot.policy.version,
          identityPolicyHash:slot.policy.hash,initialResumeMode:'initial'}),executeAttempt:async(_prepared,attempt)=>{
            metrics.route_generation=attempt.routeGeneration;metrics.egress_country=attempt.egressCountry;
            nodeState=await worker.runOnce();return {kind:'managed_work_complete',businessState:'terminal',result:{diagnostic:true}};
          }});
      } catch(error){executionError=error;}
      const center=await centerResult;
      pool.connect=originalConnect;
      metrics.total_ms=performance.now()-started;metrics.node_state=nodeState;
      metrics.center_after_last_detail_ms=detailEnd?centerFinished-detailEnd:null;
      metrics.error=String(executionError?.code||executionError?.message||center.error?.code||center.error?.message||'')||null;
      metrics.commands=(await query('SELECT operation,count(*)::int AS count FROM remote_ingestion.channel_commands WHERE task_id=$1 GROUP BY operation',[lease.task_id])).rows;
      metrics.run=(await query('SELECT status,result_json FROM crawler.channel_runs WHERE run_id=$1',[f.prepared.businessRunId])).rows[0];
      metrics.channel=(await query('SELECT to_jsonb(c) AS data FROM crawler.channels c WHERE channel_id=$1',[channelId])).rows[0]?.data;
      metrics.contents=(await query('SELECT to_jsonb(c) AS data FROM crawler.contents c WHERE channel_id=$1 ORDER BY source_content_id',[channelId])).rows.map(row=>row.data);
      for(const entry of metrics.operations){entry.start_ms-=started;entry.end_ms-=started;}
      output.push(metrics);
      await writeFile(process.env.REMOTE_WHOLE_LIVE_REPORT,JSON.stringify(output,null,2),{mode:0o600});
      console.log(JSON.stringify({event:'whole_live_case',sample:sample+1,mode,node_state:nodeState,error:metrics.error,
        total_ms:Math.round(metrics.total_ms),operations:metrics.operations.map(({name,ms,error})=>({name,ms:Math.round(ms),error})),
        sql_calls:metrics.sql_calls,contents:metrics.contents.length,commands:metrics.commands}));
      assert.ifError(executionError);assert.ifError(center.error);assert.equal(nodeState,'applied');
      const requestedIds=metrics.operations.filter(o=>o.name==='detail').map(o=>o.video_id);
      assert.ok(requestedIds.length>=3,'real video details must actually be requested');
      assert.equal(new Set(requestedIds).size,requestedIds.length,'no repeated detail operation in this successful pass');
      for(const id of requestedIds){
        const c=metrics.contents.find(c=>c.source_content_id===id);
        assert.ok(c?.title && c.thumbnail_url && c.published_at && c.duration_seconds>=0);
        assert.ok(['exact','estimated'].includes(c.view_count_status));
        assert.notEqual(c.like_count_status,'unresolved');
        assert.notEqual(c.comment_count_status,'unresolved');
        assert.notEqual(c.description_status,'unresolved');
        assert.notEqual(c.published_at_status,'unresolved');
        const page=c.comments_first_page;
        assert.ok(page && page.returned_count===page.comments.length);
        assert.equal(new Set(page.comments.map(item=>item.comment_id)).size,page.comments.length);
      }
      assert.equal(metrics.run.result_json.domains.about.status,'complete');
      assert.equal(metrics.run.result_json.domains.video.status,'complete');
      const whole=mode==='nats_whole';
      assert.equal(metrics.commands.reduce((sum,c)=>sum+c.count,0),whole?1:2+requestedIds.length);
      if(whole){assert.equal(metrics.rpc.wholeChannelInput.calls,1);assert.equal(metrics.rpc.uploadWholeChannel.calls,1);}
    });
    const pair=output.filter(row=>row.channel_id===channelId);
    assert.equal(pair.length,2);
    assert.deepEqual(pair[0].contents.map(c=>c.source_content_id),pair[1].contents.map(c=>c.source_content_id),'same real targets');
    assert.equal(pair[0].channel.title,pair[1].channel.title);
    assert.deepEqual(pair[0].operations.filter(o=>o.name==='detail').map(o=>o.video_id),
      pair[1].operations.filter(o=>o.name==='detail').map(o=>o.video_id));
    const stableFields=['title','content_type','description','description_status','hashtags','keywords',
      'duration_seconds','duration_status','published_at','published_at_status','access_status','comments_disabled',
      'view_count_status','like_count_status','comment_count_status'];
    for(let index=0;index<pair[0].contents.length;index++){
      const a=pair[0].contents[index],b=pair[1].contents[index];
      for(const key of stableFields)assert.deepEqual(a[key],b[key],`${a.source_content_id}: ${key}`);
    }
    assert.equal(pair[0].route_generation,pair[1].route_generation,'same Rota route');
  }
});
