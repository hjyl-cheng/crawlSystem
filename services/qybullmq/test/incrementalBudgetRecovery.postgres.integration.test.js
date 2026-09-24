import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import pg from 'pg';
import { Queue, Worker, UnrecoverableError } from 'bullmq';
import { RemoteNodeStore } from '../src/remoteNodes/store.js';
import { RemoteChannelPlanStore } from '../src/remoteNodes/channelPlanStore.js';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';
import { createCenterIncrementalProcessor } from '../src/remoteNodes/centerIncrementalProcessor.js';
import { IncrementalRunStore } from '../src/incrementalRunStore.js';
import { ProxyBusinessRunPreparer } from '../src/proxyBusinessRun.js';
import { resolveWorkerIdentityPolicy } from '../src/identityPolicyCatalog.js';
import { incrementalPlanHash, INCREMENTAL_QUEUE, INCREMENTAL_JOB_NAME } from '../src/incrementalPlan.js';
import { terminateExhaustedIncrementalRun } from '../src/incrementalBudgetRecovery.js';
import { RotaBusinessRunBudgetExhaustedError } from '../src/rotaSlotAdapter.js';
import { ProxyControlRequestError } from '../src/proxyControlClient.js';
import { PUBLICATION_WRITER_VERSION } from '../src/publicationWriterVersion.js';
import { failure, unwrap } from '../src/remoteNodes/natsProtocol.js';

const url = process.env.REMOTE_NODE_TEST_DATABASE_URL;
const budgetError = () => new RotaBusinessRunBudgetExhaustedError(new ProxyControlRequestError(
  'proxy control business run budget exhausted', {code:'BUSINESS_RUN_BUDGET_EXHAUSTED',status:409,retryable:false}));
test('incremental budget termination survives retries, transaction failures and queue delivery', {skip:!url,timeout:120000}, async t => {
  const pool = new pg.Pool({connectionString:url,max:6,options:`-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`});
  t.after(()=>pool.end());
  await assertIsolatedRemoteDatabase(pool);
  for (const path of ['../src/schema.sql','../../feature-engine/sql/schema.sql','../src/remoteNodes/schema.sql','../src/remoteNodes/routeSchema.sql']) {
    await pool.query(await readFile(new URL(path,import.meta.url),'utf8'));
  }
  const store = new RemoteNodeStore({pool});
  const withTransaction = action => store.transaction(action);
  const resolvedPolicy = resolveWorkerIdentityPolicy({role:'channel',policyId:'qy-br-channel-anonymous-v1',expectedWorkloadScope:'qy-production',environment:{}});
  const runs = new IncrementalRunStore({withTransaction});
  const preparer = new ProxyBusinessRunPreparer({queryFn:pool.query.bind(pool),withTransaction,resolvedPolicy,incrementalRunStore:runs});
  async function fixture(mask={about:true,video:true,agent:false}) {
    const planId=randomUUID(), channelId=`UC${randomUUID().replaceAll('-','').slice(0,22)}`, now=new Date().toISOString();
    const plan={schema_version:5,dispatch_generation:1,job_id:`budget-${planId}`,plan_id:planId,plan_mode:'standard',plan_day:now.slice(0,10),scheduled_at:now,
      channel_id:channelId,task_mask:mask,capacity:{factor:1,player_cap:20,next_cap:8,version:'capacity-1'},clock_version:7,policy_version:'v16-rule-1',planner_config_version:'video-plan-1'};
    await pool.query("INSERT INTO crawler.channels(channel_id,channel_url,title,status) VALUES($1,$2,'Budget fixture','active')",[channelId,`https://youtube.com/channel/${channelId}`]);
    await pool.query(`INSERT INTO feature_clock.daily_channel_plans(plan_id,plan_day,channel_id,due_day,due_at,eligible_at,scheduled_at,
      run_about,run_video,run_agent,dispatch_slot,capacity_factor,player_cap,next_cap,source_clock_version,policy_version,planner_config_version,capacity_version,status)
      VALUES($1,$2,$3,$2,$4,$4,$4,$5,$6,$7,0,1,20,8,7,$8,$9,$10,'dispatched')`,
    [planId,plan.plan_day,channelId,now,mask.about,mask.video,mask.agent,plan.policy_version,plan.planner_config_version,plan.capacity.version]);
    await pool.query(`INSERT INTO feature_clock.dispatch_outbox(dispatch_event_id,plan_id,job_id,queue_name,payload_json,payload_hash,status)
      VALUES($1,$2,$3,$4,$5,$6,'published')`,[randomUUID(),planId,plan.job_id,INCREMENTAL_QUEUE,plan,incrementalPlanHash(plan)]);
    const job={id:plan.job_id,name:INCREMENTAL_JOB_NAME,queueName:INCREMENTAL_QUEUE,data:plan,attemptsStarted:1,attemptsMade:0,opts:{attempts:5},discard(){this.discarded=true;}};
    await preparer.prepareChannel(job);
    return {job,plan,runId:`incremental:${planId}`};
  }
  const terminate=f=>terminateExhaustedIncrementalRun(withTransaction,f.job,budgetError());
  const terminal=e=>e instanceof UnrecoverableError && e.code==='BUSINESS_RUN_BUDGET_EXHAUSTED';
  const read=f=>pool.query('SELECT status,result_json FROM crawler.channel_runs WHERE run_id=$1',[f.runId]);
  await t.test('repeated settlement writes one failure per unfinished domain, claim never revives it',async()=>{
    const f=await fixture();
    await assert.rejects(terminate(f),terminal);
    f.job.attemptsStarted++;
    await assert.rejects(terminate(f),terminal);
    await assert.rejects(runs.claim(f.plan),terminal);
    assert.equal((await read(f)).rows[0].status,'failed');
    assert.equal((await read(f)).rows[0].result_json.domains.video.status,'failed');
    assert.equal((await pool.query('SELECT count(*)::int n FROM crawler.crawl_observations WHERE run_id=$1',[f.runId])).rows[0].n,2);
    assert.equal((await pool.query('SELECT status FROM crawler.business_run_bindings WHERE business_run_id=$1',[f.runId])).rows[0].status,'terminal');
  });
  await t.test('transaction failure rolls back all terminal writes and remains retryable',async()=>{
    const f=await fixture();
    await assert.rejects(terminateExhaustedIncrementalRun(action=>withTransaction(async client=>{
      await action(client);throw Object.assign(new Error('injected commit failure'),{code:'40001'});
    }),f.job,budgetError()),e=>e.code==='BUSINESS_RUN_BUDGET_RECOVERY_FAILED');
    assert.equal((await read(f)).rows[0].status,'running');
    assert.equal((await pool.query('SELECT count(*)::int n FROM crawler.crawl_observations WHERE run_id=$1',[f.runId])).rows[0].n,0);
    await assert.rejects(terminate(f),terminal);
  });
  await t.test('partial success and already queued Agent work are preserved',async()=>{
    const f=await fixture({about:true,video:true,agent:true});
    await runs.markDomain(f.runId,'about','complete',{value:'keep'});
    await runs.markDomain(f.runId,'agent','queued',{batch_id:'existing-agent'});
    await assert.rejects(terminate(f),terminal);
    const run=(await read(f)).rows[0];
    assert.equal(run.result_json.domains.about.value,'keep');
    assert.equal(run.result_json.domains.agent.batch_id,'existing-agent');
    assert.deepEqual((await pool.query('SELECT observation_kind FROM crawler.crawl_observations WHERE run_id=$1',[f.runId])).rows.map(r=>r.observation_kind),['video']);
  });
  await t.test('ordinary failure remains resumable; replaced dispatch cannot be terminated',async()=>{
    const f=await fixture();
    await runs.fail(f.runId,new Error('temporary'));
    assert.equal((await runs.claim(f.plan)).resumed,true);
    await pool.query("UPDATE feature_clock.dispatch_outbox SET payload_json=jsonb_set(payload_json,'{dispatch_generation}','2') WHERE plan_id=$1",[f.plan.plan_id]);
    await assert.rejects(terminate(f),e=>e.code==='INCREMENTAL_BUSINESS_FENCE_STALE');
    assert.equal((await read(f)).rows[0].status,'running');
  });
  await t.test('terminal binding alone prevents claim from reviving the run and can be reconciled',async()=>{
    const f=await fixture();
    await pool.query("UPDATE crawler.business_run_bindings SET status='terminal',terminal_reason='proxy_control_business_run_budget_exhausted' WHERE business_run_id=$1",[f.runId]);
    await assert.rejects(runs.claim(f.plan),terminal);
    await assert.rejects(terminate(f),terminal);
    assert.equal((await read(f)).rows[0].result_json.proxy_control.status,'business_run_budget_exhausted');
  });
  await t.test('pending API continuation is preserved until its durable result settles',async()=>{
    const f=await fixture();
    const contentId=randomUUID(),requestId=randomUUID();
    const task=(await pool.query('INSERT INTO crawler.youtube_api_tasks(source_content_id) VALUES($1) RETURNING task_id',[contentId])).rows[0];
    await pool.query(`INSERT INTO crawler.youtube_api_detail_requests(request_id,run_id,source_content_id,consumer,task_id)
      VALUES($1,$2,$3,'incremental',$4)`,[requestId,f.runId,contentId,task.task_id]);
    await assert.rejects(terminate(f),e=>e.code==='BUSINESS_RUN_BUDGET_RECOVERY_FAILED');
    assert.equal((await read(f)).rows[0].status,'running');
    assert.equal((await pool.query('SELECT count(*)::int n FROM crawler.crawl_observations WHERE run_id=$1',[f.runId])).rows[0].n,0);
    await pool.query("UPDATE crawler.youtube_api_detail_requests SET status='done',finished_at=now() WHERE request_id=$1",[requestId]);
    await runs.markDomain(f.runId,'video','complete',{source:'api'});
    await assert.rejects(terminate(f),terminal);
    assert.equal((await read(f)).rows[0].result_json.domains.video.source,'api');
    assert.deepEqual((await pool.query('SELECT observation_kind FROM crawler.crawl_observations WHERE run_id=$1',[f.runId])).rows.map(r=>r.observation_kind),['about']);
  });
  await t.test('remote lease and release receipt must prove quiescence before terminal settlement',async()=>{
    const f=await fixture();
    const nodeId=randomUUID(),taskId=randomUUID(),bindingId=randomUUID();
    await store.registerNode({nodeId,token:'e'.repeat(64),capabilities:['youtube.channel-plan.v1']});
    await pool.query(`INSERT INTO remote_ingestion.tasks(task_id,work_key,capability,input,context,state,node_id,generation,lease_until)
      VALUES($1,$2,'youtube.channel-plan.v1',$3,'{}','leased',$4,1,now()+interval '90 seconds')`,
    [taskId,`incremental-plan:${f.plan.plan_id}:1`,{plan:f.plan},nodeId]);
    await pool.query(`INSERT INTO remote_ingestion.network_slots(node_id,slot,rota_worker_id)
      VALUES($1,'test-slot',$2)`,[nodeId,`fixture-${nodeId}`]);
    await pool.query(`INSERT INTO remote_ingestion.network_bindings(binding_id,node_id,slot,task_id,generation,rota_fence,identity,upstream_hash,state)
      VALUES($1,$2,'test-slot',$3,1,'{}','{}','fixture','active')`,[bindingId,nodeId,taskId]);
    await assert.rejects(terminate(f),e=>e.code==='BUSINESS_RUN_BUDGET_RECOVERY_FAILED');
    await pool.query("UPDATE remote_ingestion.tasks SET state='applied' WHERE task_id=$1",[taskId]);
    for (const receipt of [null,{in_flight:1}]) {
      await pool.query("UPDATE remote_ingestion.network_bindings SET state='retired',release_receipt=$2 WHERE binding_id=$1",[bindingId,receipt]);
      await assert.rejects(terminate(f),e=>e.code==='BUSINESS_RUN_BUDGET_RECOVERY_FAILED');
      assert.equal((await read(f)).rows[0].status,'running');
    }
    await pool.query("UPDATE remote_ingestion.network_bindings SET release_receipt=$2 WHERE binding_id=$1",[bindingId,{in_flight:0}]);
    await assert.rejects(terminate(f),terminal);
  });
  await t.test('lease expiry and generation replacement remain distinct across NATS error wire',async()=>{
    const f=await fixture();
    const nodeId=randomUUID(),taskId=randomUUID();
    await store.registerNode({nodeId,token:'f'.repeat(64),capabilities:['youtube.channel-plan.v1']});
    await pool.query(`INSERT INTO remote_ingestion.tasks(task_id,work_key,capability,input,context,state,node_id,generation,lease_until)
      VALUES($1,$2,'youtube.channel-plan.v1',$3,'{}','leased',$4,1,now()-interval '1 second')`,[taskId,`lease-budget-${taskId}`,{plan:f.plan},nodeId]);
    await assert.rejects(store.heartbeat(nodeId,taskId,1),error=>{
      assert.equal(error.lease_evidence.reason,'lease_expired');
      assert.throws(()=>unwrap(failure(error)),wire=>wire.lease_evidence.reason==='lease_expired');return true;
    });
    await pool.query("UPDATE remote_ingestion.tasks SET generation=2,lease_until=now()+interval '90 seconds' WHERE task_id=$1",[taskId]);
    await assert.rejects(store.heartbeat(nodeId,taskId,1),error=>error.lease_evidence.reason==='generation_changed');
    assert.ok((await store.heartbeat(nodeId,taskId,2)).lease_until);
  });
  await t.test('committed response loss at real center and BullMQ entry does not schedule another attempt',async()=>{
    assert.ok(process.env.REMOTE_NODE_TEST_REDIS_PORT,'isolated Redis required');
    const f=await fixture({about:true,video:false,agent:false});
    const connection={host:'127.0.0.1',port:Number(process.env.REMOTE_NODE_TEST_REDIS_PORT),password:'remote-center-fixture-only',maxRetriesPerRequest:null};
    const prefix=`budget-test-${randomUUID()}`;
    const queue=new Queue(INCREMENTAL_QUEUE,{connection,prefix});
    let invoked=0;
    const processor=createCenterIncrementalProcessor({channelStore:new RemoteChannelPlanStore({store}),runtime:{nodeId:randomUUID(),slot:'worker-test'},resolvedPolicy,
      ready:async()=>true,rota:{async executeJob(job,{prepare}) {invoked++;await prepare();throw budgetError();}}});
    const worker=new Worker(INCREMENTAL_QUEUE,processor,{connection,prefix});
    try {
      const failed=new Promise((resolve,reject)=>{worker.once('failed',(job,error)=>resolve({job,error}));worker.once('error',reject);});
      await queue.add(INCREMENTAL_JOB_NAME,f.plan,{jobId:f.job.id,attempts:5,backoff:10});
      const result=await failed;
      assert.ok(terminal(result.error));
      assert.equal(await result.job.getState(),'failed');
      assert.equal(invoked,1);
      assert.equal(result.job.attemptsMade,1);
      // Redelivery after the SQL commit must only replay the terminal state.
      result.job.attemptsStarted=2;
      await assert.rejects(processor(result.job,'test-token'),terminal);
      assert.equal((await pool.query('SELECT count(*)::int n FROM crawler.crawl_observations WHERE run_id=$1',[f.runId])).rows[0].n,1);
    } finally {await worker.close();await queue.obliterate({force:true});await queue.close();}
  });
});
