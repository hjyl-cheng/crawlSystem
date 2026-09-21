import assert from 'node:assert/strict';
import {randomBytes,randomUUID} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {setTimeout as delay} from 'node:timers/promises';
import test from 'node:test';
import pg from 'pg';
import {BusinessRunBindingStore} from '../src/businessRunBindingStore.js';
import {BrowserProfileStore} from '../src/browserProfileStore.js';
import {FullCrawlYoutubeJsStore} from '../src/fullCrawlYoutubeJsStore.js';
import {fullCrawlUploadsDocument} from '../src/fullCrawlYoutubeJsModel.js';
import {contentDetailExecutionFence} from '../src/contentDetailExecutionFence.js';
import {activeChannelCandidateAttemptFence} from '../src/channelCandidateAttemptFence.js';
import {YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT} from '../src/fullCrawlFetchContract.js';
import {RemoteNodeStore} from '../src/remoteNodes/store.js';
import {RemoteFullCrawlExecutionStore} from '../src/remoteNodes/fullCrawlExecutionStore.js';
import {RemoteFullCrawlDetailStore} from '../src/remoteNodes/fullCrawlDetailStore.js';
import {assertRemoteFullCrawlTaskFence} from '../src/remoteNodes/fullCrawlBusinessFence.js';
import {FULL_CRAWL_WORKLOAD} from '../src/remoteNodes/collectingWorkload.js';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';

const url=process.env.REMOTE_NODE_TEST_DATABASE_URL;
test('full-crawl admission and writes use real candidate, binding, attempt and connection ownership',
  {skip:!url,timeout:90000},async t=>{
  const pool=new pg.Pool({connectionString:url,max:6});
  const guard=await pool.connect();
  await assertIsolatedRemoteDatabase(pool);await guard.query('SELECT pg_advisory_lock(781137981)');
  await pool.query(await readFile(new URL('../src/schema.sql',import.meta.url),'utf8'));
  for(const file of ['schema.sql','routeSchema.sql','workerConnectionSchema.sql','workerActivationSchema.sql','fullCrawlSchema.sql','fullCrawlBusinessSchema.sql']){
    await pool.query(await readFile(new URL(`../src/remoteNodes/${file}`,import.meta.url),'utf8'));
  }
  const store=new RemoteNodeStore({pool});const query=pool.query.bind(pool);
  const full=new RemoteFullCrawlExecutionStore({store,verifyExecution:async()=>true});
  const suffix=randomUUID(),nodeId=randomUUID(),channelId=`UC${suffix.replaceAll('-','').slice(0,22)}`;
  const runId=`full-fence:${suffix}`,businessRunKey=`candidate-fence:${suffix}`,batchId=`fence-batch:${suffix}`;
  const jobId=`snapshot-${suffix}`,workerId=`rota-full-${suffix}`,slot='full-crawl-1';
  const policy={id:'full-fence-policy',version:1,hash:'sha256:fixture-policy'};
  let profileGroup,taskId,candidateId;
  t.after(async()=>{
    try {
    if(taskId){
      await query(`DELETE FROM remote_ingestion.full_crawl_detail_reservations WHERE stage_id IN
        (SELECT stage_id FROM remote_ingestion.full_crawl_stages WHERE task_id=$1)`,[taskId]);
      await query('DELETE FROM remote_ingestion.full_crawl_stages WHERE task_id=$1',[taskId]);
      await query('DELETE FROM remote_ingestion.full_crawl_executions WHERE task_id=$1',[taskId]);
      await query('DELETE FROM remote_ingestion.claims WHERE task_id=$1',[taskId]);
      await query('DELETE FROM remote_ingestion.tasks WHERE task_id=$1',[taskId]);
    }
    for(const table of ['worker_connections','network_slots','nodes'])await query(`DELETE FROM remote_ingestion.${table} WHERE node_id=$1`,[nodeId]);
    await query('DELETE FROM crawler.channel_execution_attempts WHERE business_run_id=$1',[runId]);
    // Full business fixtures remain in the isolated DB as in the existing full
    // store integration suite; every identity is unique across repeated runs.
    } finally {guard.release();await pool.end();}
  });
  await store.registerNode({nodeId,token:randomBytes(32).toString('hex'),capabilities:[FULL_CRAWL_WORKLOAD.capability]});
  await query('INSERT INTO remote_ingestion.network_slots(node_id,slot,rota_worker_id) VALUES($1,$2,$3)',[nodeId,slot,workerId]);
  const registration={nodeId,slot,deploymentId:randomUUID(),configHash:'a'.repeat(64),role:'fullcrawl',mode:FULL_CRAWL_WORKLOAD.mode};
  await full.activation.register(registration);
  const connection={version:1,mode:registration.mode,node_id:nodeId,slot,deployment_id:registration.deploymentId,
    config_hash:registration.configHash,instance_id:randomUUID(),relay_boot_id:'b'.repeat(48),
    runtime_revision:FULL_CRAWL_WORKLOAD.revisions[0],accepting:true};
  await full.activation.heartbeat(nodeId,connection);await full.activation.activate(connection);
  const transaction=action=>store.transaction(action);
  await query("INSERT INTO crawler.query_dispatch_batches(dispatch_batch_id,pipeline_cycle_id,status) VALUES($1,$1,'validation_closed')",[batchId]);
  candidateId=Number((await query(`INSERT INTO crawler.channel_candidates
    (dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt)
    VALUES($1,$1,$2,$3,'queued',1,$4,1) RETURNING candidate_id`,[batchId,channelId,`https://www.youtube.com/channel/${channelId}`,jobId])).rows[0].candidate_id);
  const {binding}=await new BusinessRunBindingStore({withTransaction:transaction}).resolve({businessRunKey,explicitBusinessRunId:runId,
    requestedStatus:'reserved',runKind:'full',channelId,candidateId,policy,
    intent:{job_name:'channel-snapshot',crawl_mode:'full',fetch_contract:YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT}});
  const profiles=new BrowserProfileStore({queryFn:query,transactionFn:transaction,secret:randomBytes(32).toString('hex')});
  const proxy={workload_scope:'full-fence-test',worker_id:workerId,worker_instance_id:`center-${suffix}`,slot_name:`rota-slot-${suffix}`,
    route_generation:1,network_identity_key:`network-${suffix}`,identity_policy_id:policy.id,identity_policy_version:policy.version};
  profileGroup=await profiles.loadOrCreate({identityPolicyId:policy.id,identityPolicyVersion:policy.version,networkIdentityKey:proxy.network_identity_key,
    profileEpoch:0,language:'pt',country:'BR',timezone:'America/Sao_Paulo'});
  const attemptArgs={channelId,runId,queueName:FULL_CRAWL_WORKLOAD.queue,jobId,jobAttempt:0,dispatchGeneration:1,
    workerId,proxy,profileGroup,prepared:{businessRunId:runId},task:{task_id:randomUUID(),business_run_id:runId,attempt_number:1}};
  const attemptId=await profiles.beginAttempt(attemptArgs);
  const execution={version:1,queue_name:FULL_CRAWL_WORKLOAD.queue,job_name:'channel-snapshot',job_id:jobId,job_attempt:1,
    candidate_id:candidateId,channel_id:channelId,run_id:runId,business_run_id:runId,business_run_key:businessRunKey,
    intent_hash:binding.intent_hash,dispatch_generation:1,execution_attempt_id:attemptId,fetch_contract:YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT};
  let lease,request;
  await t.test('prepare freezes complete ownership; claim accepts the original pre-admission attempt',async()=>{
    assert.equal((await query('SELECT run_id FROM crawler.channel_execution_attempts WHERE attempt_id=$1',[attemptId])).rows[0].run_id,null);
    const prepared=await full.prepare({execution,connection});taskId=prepared.taskId;
    assert.deepEqual(await full.prepare({execution,connection}),prepared);
    assert.equal((await query('SELECT generation FROM remote_ingestion.tasks WHERE task_id=$1',[taskId])).rows[0].generation,0);
    assert.equal(await store.claim(nodeId,randomUUID(),slot),null);
    await query('UPDATE crawler.channel_candidates SET snapshot_dispatch_generation=2 WHERE candidate_id=$1',[candidateId]);
    await assert.rejects(full.activation.claim(nodeId,{claim_id:randomUUID(),slot,connection}),{code:'FULL_CRAWL_BUSINESS_FENCE_STALE'});
    assert.equal((await query('SELECT generation FROM remote_ingestion.tasks WHERE task_id=$1',[taskId])).rows[0].generation,0,
      'failed real business admission must not consume a transport generation');
    await query('UPDATE crawler.channel_candidates SET snapshot_dispatch_generation=1 WHERE candidate_id=$1',[candidateId]);
    lease=await full.activation.claim(nodeId,{claim_id:randomUUID(),slot,connection});
    assert.equal(lease.task_id,taskId);assert.equal(lease.generation,1);
    request={task_id:taskId,generation:lease.generation,connection};
    assert.deepEqual(await full.prepare({execution,connection}),prepared);
    assert.ok((await full.renew(nodeId,request)).lease_until);
  });
  assert.ok(request,'ownership fixture must be admitted before testing mutations');
  const task=async client=>(await client.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE',[taskId])).rows[0];
  const owner=async client=>(await client.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2 FOR UPDATE',[nodeId,slot])).rows[0];
  await t.test('missing attempt serialization cannot silently weaken the business fence',async()=>{
    await transaction(async client=>{
      await client.query('SAVEPOINT schema_guard');
      await client.query('ALTER TABLE crawler.channel_execution_attempts DISABLE TRIGGER full_crawl_attempt_insert_lock');
      await assert.rejects(assertRemoteFullCrawlTaskFence(client,await task(client),await owner(client)),{code:'FULL_CRAWL_BUSINESS_SCHEMA_REQUIRED'});
      await client.query('ROLLBACK TO SAVEPOINT schema_guard');
    });
  });
  const mutations=[
    ['dispatch generation',"UPDATE crawler.channel_candidates SET snapshot_dispatch_generation=2 WHERE candidate_id=$1",[candidateId]],
    ['BullMQ attempt',"UPDATE crawler.channel_candidates SET snapshot_active_job_attempt=2 WHERE candidate_id=$1",[candidateId]],
    ['job identity',"UPDATE crawler.channel_candidates SET snapshot_active_job_id='replaced' WHERE candidate_id=$1",[candidateId]],
    ['candidate status',"UPDATE crawler.channel_candidates SET status='rejected' WHERE candidate_id=$1",[candidateId]],
    ['binding intent',"UPDATE crawler.business_run_bindings SET intent_json=jsonb_set(intent_json,'{intent,crawl_mode}','\"incremental\"') WHERE business_run_key=$1",[businessRunKey]],
    ['terminal binding',"UPDATE crawler.business_run_bindings SET status='terminal',terminal_reason='cancelled' WHERE business_run_key=$1",[businessRunKey]],
    ['binding policy',"UPDATE crawler.business_run_bindings SET identity_policy_hash='wrong' WHERE business_run_key=$1",[businessRunKey]],
    ['attempt business run',"UPDATE crawler.channel_execution_attempts SET business_run_id='wrong' WHERE attempt_id=$1",[attemptId]],
    ['business attempt',"UPDATE crawler.channel_execution_attempts SET job_attempt=1 WHERE attempt_id=$1",[attemptId]],
    ['finished attempt',"UPDATE crawler.channel_execution_attempts SET status='failed',finished_at=now() WHERE attempt_id=$1",[attemptId]],
    ['route generation',"UPDATE crawler.channel_execution_attempts SET route_generation=2 WHERE attempt_id=$1",[attemptId]],
    ['Rota instance',"UPDATE crawler.channel_execution_attempts SET worker_instance_id='replaced' WHERE attempt_id=$1",[attemptId]],
    ['network slot',"UPDATE remote_ingestion.network_slots SET rota_worker_id='wrong' WHERE node_id=$1",[nodeId]],
    ['remote instance',"UPDATE remote_ingestion.worker_connections SET instance_id=$2 WHERE node_id=$1",[nodeId,randomUUID()]],
    ['relay boot',"UPDATE remote_ingestion.worker_connections SET relay_boot_id=$2 WHERE node_id=$1",[nodeId,'c'.repeat(48)]],
    ['frozen task input',"UPDATE remote_ingestion.tasks SET input=jsonb_set(input,'{job_attempt}','2') WHERE task_id=$1",[taskId]],
    ['transport generation',"UPDATE remote_ingestion.tasks SET generation=2 WHERE task_id=$1",[taskId]],
  ];
  for(const [name,sql,args] of mutations)await t.test(`rejects changed ${name} before a protected write`,async()=>{
    await transaction(async client=>{
      await client.query('SAVEPOINT mutation');await client.query(sql,args);
      const connectionRow=await owner(client),taskRow=await task(client);
      await assert.rejects(assertRemoteFullCrawlTaskFence(client,taskRow,connectionRow),{code:'FULL_CRAWL_BUSINESS_FENCE_STALE'});
      await client.query('ROLLBACK TO SAVEPOINT mutation');
    });
  });
  await t.test('a network binding must retain the original Rota route identity',async()=>{
    await transaction(async client=>{
      await client.query('SAVEPOINT route_fixture');
      const evidence=(await client.query('SELECT rota_fence FROM remote_ingestion.full_crawl_executions WHERE task_id=$1',[taskId])).rows[0];
      const bindingId=randomUUID();
      await client.query(`INSERT INTO remote_ingestion.network_bindings
        (binding_id,node_id,slot,task_id,generation,rota_fence,identity,upstream_hash)
        VALUES($1,$2,$3,$4,1,$5,$6,'fixture')`,[bindingId,nodeId,slot,taskId,evidence.rota_fence,{workload_scope:proxy.workload_scope,network_identity_key:proxy.network_identity_key}]);
      assert.ok(await assertRemoteFullCrawlTaskFence(client,await task(client),await owner(client)));
      await client.query("UPDATE remote_ingestion.network_bindings SET rota_fence=jsonb_set(rota_fence,'{route_generation}','2') WHERE binding_id=$1",[bindingId]);
      await assert.rejects(assertRemoteFullCrawlTaskFence(client,await task(client),await owner(client)),{code:'FULL_CRAWL_BUSINESS_FENCE_STALE'});
      await client.query('ROLLBACK TO SAVEPOINT route_fixture');
    });
  });
  await t.test('wrong connection and expired lease cannot renew; drain can finish its own work',async()=>{
    await assert.rejects(full.renew(nodeId,{...request,connection:{...connection,instance_id:randomUUID()}}),{code:'WORKER_CONNECTION_STALE'});
    await assert.rejects(full.renew(nodeId,{...request,generation:2}),{code:'STALE_LEASE'});
    await full.activation.drain(nodeId,slot);
    await assert.rejects(full.prepare({execution,connection}),{code:'WORKER_NOT_READY'});
    assert.ok((await full.renew(nodeId,request)).lease_until);
    await full.activation.activate(connection);
    await query("UPDATE remote_ingestion.tasks SET lease_until=now()-interval '1 second' WHERE task_id=$1",[taskId]);
    await assert.rejects(full.renew(nodeId,request),{code:'STALE_LEASE'});
    await query("UPDATE remote_ingestion.tasks SET lease_until=now()+interval '90 seconds' WHERE task_id=$1",[taskId]);
  });
  await t.test('protected business write and surrounding evidence roll back together',async()=>{
    const before=(await query('SELECT snapshot_attempts FROM crawler.channel_candidates WHERE candidate_id=$1',[candidateId])).rows[0].snapshot_attempts;
    await assert.rejects(full.withLease(nodeId,request,async client=>{
      const local=new FullCrawlYoutubeJsStore({query:client.query.bind(client),withTransaction:action=>action(client)});
      await local.beginAdmission({id:jobId,attemptsStarted:1,data:{...execution,dispatch_batch_id:batchId}});
      throw new Error('fixture interrupted before evidence commit');
    }),/fixture interrupted/);
    assert.equal((await query('SELECT snapshot_attempts FROM crawler.channel_candidates WHERE candidate_id=$1',[candidateId])).rows[0].snapshot_attempts,before);
  });
  await t.test('new pre-admission attempts wait for the same business fence and invalidate the previous owner',async()=>{
    let release,entered;const held=new Promise(resolve=>{release=resolve}),started=new Promise(resolve=>{entered=resolve});
    const writing=full.withLease(nodeId,request,async()=>{entered();await held;});
    await started;
    const inserter=await pool.connect();let inserting;
    try{
      const pid=(await inserter.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
      const nextProfiles=new BrowserProfileStore({queryFn:inserter.query.bind(inserter),transactionFn:action=>action(inserter),secret:randomBytes(32).toString('hex')});
      inserting=nextProfiles.beginAttempt({...attemptArgs,task:{...attemptArgs.task,task_id:randomUUID(),attempt_number:2}});
      let blocked=false;
      for(let i=0;i<100&&!blocked;i++){
        blocked=(await query("SELECT wait_event_type='Lock' AS blocked FROM pg_stat_activity WHERE pid=$1",[pid])).rows[0]?.blocked;
        if(!blocked)await delay(10);
      }
      assert.equal(blocked,true,'new attempt must wait even when no channel run exists');
      release();await writing;await inserting;
      await assert.rejects(full.renew(nodeId,request),{code:'FULL_CRAWL_BUSINESS_FENCE_STALE'});
      await query('DELETE FROM crawler.channel_execution_attempts WHERE business_run_id=$1 AND attempt_number=2',[runId]);
    }finally{release();await writing;await inserting;inserter.release();}
  });
  await t.test('the same attempt remains valid after real local admission materializes its run',async()=>{
    await full.withLease(nodeId,request,async client=>{
      const local=new FullCrawlYoutubeJsStore({query:client.query.bind(client),withTransaction:action=>action(client)});
      const job={id:jobId,attemptsStarted:1,data:{...execution,dispatch_batch_id:batchId}};
      await local.beginAdmission(job);
      await local.commitAdmission(job,{metadata:{channel_id:channelId,channel_url:`https://www.youtube.com/channel/${channelId}`,
        title:'Full fence fixture',country:'Brazil',country_code:'BR',country_canonical_name:'Brazil',subscriber_count:10000},
      sourceJson:{channel_extractor:'youtubejs'},aboutObservation:null,observedAt:new Date().toISOString(),settings:{channelContentLimit:2,contentMaxAgeDays:90}});
    });
    assert.ok((await full.renew(nodeId,request)).lease_until);
    await transaction(async client=>{
      await client.query('SAVEPOINT mutation');
      await client.query("UPDATE crawler.channel_runs SET status='failed' WHERE run_id=$1",[runId]);
      await assert.rejects(assertRemoteFullCrawlTaskFence(client,await task(client),await owner(client)),{code:'FULL_CRAWL_BUSINESS_FENCE_STALE'});
      await client.query('ROLLBACK TO SAVEPOINT mutation');
    });
  });
  const job={id:jobId,attemptsStarted:1,data:{...execution,dispatch_batch_id:batchId}};
  const detailFence=contentDetailExecutionFence(job,{executionMode:'channel_inline',candidateAttemptFence:activeChannelCandidateAttemptFence(job)});
  const details=new RemoteFullCrawlDetailStore({executionStore:full});
  let command;
  await t.test('reservations use the frozen uploads and consume no attempt budget',async()=>{
    await full.withLease(nodeId,request,async client=>{
      const local=new FullCrawlYoutubeJsStore({query:client.query.bind(client),withTransaction:action=>action(client)});
      const document=fullCrawlUploadsDocument({playlist_id:`UU${suffix}`,entries:[1,2].map(position=>({
        video_id:`upcoming-${suffix}-${position}`,position,title:`Upcoming ${position}`,is_upcoming:true,live_status:'is_upcoming'})),
      activity_evidence_complete:true,scan:{complete:true,stop_reason:'limit',terminal_reason:'limit',pages:1,inspected_count:2,parse_gap_count:0}});
      await local.commitUploads(job,{document,targets:document.entries,activityEvidence:null,observedAt:new Date().toISOString()});
      assert.ok(await local.claimDetailExecution(detailFence));
    });
    const stageId=randomUUID();
    command=await details.reserve(nodeId,request,{stageId,sequence:1,detailFence});
    assert.equal(command.input.targets.length,2);
    assert.deepEqual(await details.reserve(nodeId,request,{stageId,sequence:1,detailFence}),command);
    assert.deepEqual((await query('SELECT attempts,detail_status FROM crawler.content_candidates WHERE run_id=$1 ORDER BY position',[runId])).rows,
      [{attempts:0,detail_status:'queued'},{attempts:0,detail_status:'queued'}]);
    await assert.rejects(details.reserve(nodeId,request,{stageId:randomUUID(),sequence:2,detailFence}),{code:'FULL_CRAWL_STAGE_ORDER'});
    await assert.rejects(details.reserve(nodeId,request,{stageId,sequence:1,detailFence:{...detailFence,jobAttempt:2}}),{code:'FULL_CRAWL_DETAIL_FENCE_STALE'});
  });
  assert.ok(command,'detail fixture must have frozen reservations');
  const ids=command.input.targets.map(target=>({stageId:command.stage_id,reservationId:target.reservation_id,startId:randomUUID()}));
  await t.test('changed frozen target evidence cannot authorize a start',async()=>{
    const original=(await query('SELECT candidate_id,result_json FROM crawler.content_candidates WHERE run_id=$1 ORDER BY position LIMIT 1',[runId])).rows[0];
    try{
      await query("UPDATE crawler.content_candidates SET result_json=jsonb_set(result_json,'{full_crawl_target,title}','\"changed\"') WHERE candidate_id=$1",[original.candidate_id]);
      await assert.rejects(details.started(nodeId,request,ids[0]),{code:'FULL_CRAWL_TARGET_CONFLICT'});
      assert.equal((await query('SELECT attempts FROM crawler.content_candidates WHERE candidate_id=$1',[original.candidate_id])).rows[0].attempts,0);
    }finally{await query('UPDATE crawler.content_candidates SET result_json=$2 WHERE candidate_id=$1',[original.candidate_id,original.result_json]);}
  });
  await t.test('started evidence is ordered, idempotent and charges only the started prefix',async()=>{
    await assert.rejects(details.started(nodeId,request,ids[1]),{code:'FULL_CRAWL_START_ORDER'});
    const first=await details.started(nodeId,request,ids[0]);
    assert.deepEqual(await details.started(nodeId,request,ids[0]),first);
    await assert.rejects(details.started(nodeId,request,{...ids[0],startId:randomUUID()}),{code:'FULL_CRAWL_START_CONFLICT'});
    await assert.rejects(details.started(nodeId,request,{...ids[1],startId:ids[0].startId}),{code:'FULL_CRAWL_START_CONFLICT'});
    assert.deepEqual((await query('SELECT attempts,detail_status FROM crawler.content_candidates WHERE run_id=$1 ORDER BY position',[runId])).rows,
      [{attempts:1,detail_status:'running'},{attempts:0,detail_status:'queued'}]);
    await assert.rejects(details.started(nodeId,{...request,generation:2},ids[1]),{code:'STALE_LEASE'});
    await details.started(nodeId,request,ids[1]);
  });
  const results=command.input.targets.map(({video_id})=>Buffer.from(JSON.stringify({detail:{id:video_id,title:'Upcoming',is_upcoming:true,
    live_status:'is_upcoming',access_status:'public',access_status_source:'youtubejs_uploads',source:'youtubejs_uploads'},
  access:{access_status:'public',access_status_source:'youtubejs_uploads'},
  classification:{content_type:'live',source:'youtubejs_uploads_live_flag',authoritative:true},
  terminalReason:'upcoming_live',observedAt:new Date().toISOString(),locale:'en'})));
  const applyResult=async(client,{candidate,detailFence,payload})=>{
    const local=new FullCrawlYoutubeJsStore({query:client.query.bind(client),withTransaction:action=>action(client)});
    return local.commitDetail(detailFence,candidate,JSON.parse(payload));
  };
  await t.test('application is ordered and business writes roll back if the applied receipt fails',async()=>{
    await assert.rejects(details.apply(nodeId,request,{...ids[1],payload:results[1]},applyResult),{code:'FULL_CRAWL_APPLY_ORDER'});
    await assert.rejects(details.apply(nodeId,request,{...ids[0],payload:results[0]},async(client,args)=>{
      await applyResult(client,args);throw new Error('interrupted before applied marker');
    }),/interrupted before applied marker/);
    assert.equal((await query('SELECT state FROM remote_ingestion.full_crawl_detail_reservations WHERE reservation_id=$1',[ids[0].reservationId])).rows[0].state,'started');
    assert.deepEqual((await query('SELECT attempts,detail_status,disposition FROM crawler.content_candidates WHERE run_id=$1 ORDER BY position',[runId])).rows,
      [{attempts:1,detail_status:'running',disposition:null},{attempts:1,detail_status:'running',disposition:null}]);
    await assert.rejects(details.apply(nodeId,request,{...ids[0],payload:results[0]},async()=>({ok:true})),{code:'FULL_CRAWL_RESULT_NOT_APPLIED'});
  });
  await t.test('identical result replay does not repeat business writes; conflicting results are refused',async()=>{
    const first=await details.apply(nodeId,request,{...ids[0],payload:results[0]},applyResult);
    assert.deepEqual(await details.apply(nodeId,request,{...ids[0],payload:results[0]},()=>assert.fail('replayed writer')),first);
    await assert.rejects(details.apply(nodeId,request,{...ids[0],payload:Buffer.from('{}')},applyResult),{code:'FULL_CRAWL_RESULT_CONFLICT'});
    await details.apply(nodeId,request,{...ids[1],payload:results[1]},applyResult);
    assert.ok((await query('SELECT applied_at FROM remote_ingestion.full_crawl_stages WHERE stage_id=$1',[command.stage_id])).rows[0].applied_at);
    assert.deepEqual((await query('SELECT attempts,detail_status FROM crawler.content_candidates WHERE run_id=$1 ORDER BY position',[runId])).rows,
      [{attempts:1,detail_status:'done'},{attempts:1,detail_status:'done'}]);
    assert.equal(await details.reserve(nodeId,request,{stageId:randomUUID(),sequence:2,detailFence}),null);
  });
});
