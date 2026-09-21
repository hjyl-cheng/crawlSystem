import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,generateKeyPairSync} from 'node:crypto';
import {createRemoteDeploymentAdmin} from '../src/remoteNodes/deploymentAdmin.js';
import {setTimeout as delay} from 'node:timers/promises';
import pg from 'pg';
import {Queue,Worker} from 'bullmq';
import {fullCrawlFixture,channelSnapshot} from './helpers/remoteFullCrawlFixture.js';
import {createFullCrawlCenter} from '../src/remoteNodes/fullCrawlCenter.js';
import {RemoteFullCrawlStageStore} from '../src/remoteNodes/fullCrawlStageStore.js';
import {fullCrawlResultParts} from '../src/remoteNodes/fullCrawlMessages.js';
import {resolveWorkerIdentityPolicy} from '../src/identityPolicyCatalog.js';
import {RotaSlotDeferredError} from '../src/rotaSlotAdapter.js';
const url=process.env.REMOTE_NODE_TEST_DATABASE_URL,port=Number(process.env.REMOTE_NODE_TEST_REDIS_PORT);
async function until(check,message='full center fixture timed out'){for(let i=0;i<350;i++){if(await check())return;await delay(20);}assert.fail(typeof message==='function'?message():message);}

test('real full queue, supervisor and Rota adapter execute stages, renew locks and preserve drain',{skip:!url||!port,timeout:60000},async t=>{
  const resolvedPolicy=resolveWorkerIdentityPolicy({role:'channel',policyId:'qy-br-channel-anonymous-v1',expectedWorkloadScope:'qy-production',environment:{}});
  let center,admin;
  const f=await fullCrawlFixture(t,{createAttempt:false,activate:false,identityPolicy:resolvedPolicy.policy,
    verifyExecution:(client,row)=>center?.supervisor.verifyExecution(client,row)??false});
  const guardPool=new pg.Pool({connectionString:url,max:4});
  const connection={host:'127.0.0.1',port,password:'remote-center-fixture-only',maxRetriesPerRequest:null};
  const prefix='full-center-'+randomUUID();
  const queue=new Queue('youtube-channel-crawl',{connection,prefix});
  const incremental=new Queue('youtube-channel-incremental',{connection,prefix});
  const centers=[],events=[],commands=[],compatibilityJobs=[],rotaCalls=[];
  let releaseAdmission,admissionEntered=false,transportReady=true,execution,currentAssignment,networkStopped=false;
  const admissionWait=new Promise(resolve=>{releaseAdmission=resolve;});
  // Stop queue consumers in finally before the fixture database cleanup runs.
  const close=async()=>{releaseAdmission();for(const c of centers)await c.supervisor.stop();
    await queue.obliterate({force:true});await incremental.obliterate({force:true});await queue.close();await incremental.close();await guardPool.end();
    await f.query('DELETE FROM remote_ingestion.intake_controls WHERE node_key=$1',[f.nodeId]);
    for(const table of ['node_intake_requests','node_deployments'])await f.query(`DELETE FROM remote_ingestion.${table} WHERE node_id=$1`,[f.nodeId]);};
  const client={
    async claim(request){currentAssignment={ok:true,ready:true,control_state:'leased_idle',workload_scope:'qy-production',protocol_version:2,role:'channel',
      worker_id:request.worker_id,worker_instance_id:request.worker_instance_id,slot_name:'p2-full-slot',proxy_user:'p2-fixture',lease_id:randomUUID(),
      lease_remaining_ms:60000,server_time:new Date().toISOString(),route_generation:1,credential_generation:11,network_identity_key:'p2-network-'+f.nodeId,
      profile_epoch:0,identity_policy_id:resolvedPolicy.policy.id,identity_policy_version:resolvedPolicy.policy.version,
      identity_policy_hash:resolvedPolicy.policy.hash,identity_action:'keep',egress_country:'BR'};return currentAssignment;},
    async renew(){return currentAssignment;},
    async beginTask(request){rotaCalls.push('begin');return {ok:true,task_id:randomUUID(),attempt_request_id:request.attempt_request_id,
      business_run_id:request.business_run_id,job_execution_id:request.job_execution_id,attempt_number:1,slot_name:request.slot_name,
      route_generation:request.route_generation,started_at:new Date().toISOString()};},
    async completeTask(request){rotaCalls.push('complete');assert.equal(request.active_managed_requests,0);assert.equal(networkStopped,true);
      return {ok:true,task_completed:true,completion_request_id:request.completion_request_id,task_id:request.task_id,slot_name:request.slot_name,
        lease_id:request.lease_id,control_state:'READY_KEEP_ROUTE',ready:true,completed_task_route_generation:request.route_generation};},
    async release(request){return {ok:true,released:true,release_request_id:request.release_request_id,lease_id:request.lease_id,slot_name:request.slot_name,
      route_generation:request.known_route_generation,status:'released',released_at:new Date().toISOString(),reason:request.reason};},
  };
  const args={store:f.store,guardPool,connection,prefix,allowedNodeIds:[f.nodeId],resolvedPolicy,profileSecret:'p2-full-profile-fixture-secret',
    rotaClient:client,proxyBaseUrl:'http://fixture.invalid:8000',proxyPassword:'fixture-only',report:event=>events.push(event),
    WorkerClass:class extends Worker{constructor(name,processor,options){super(name,processor,{...options,lockDuration:1000,lockRenewTime:200});}},
    handoff:{candidateSettled:async()=>{},fetchCompleted:async()=>{}},
    compatibility:{execute:async job=>{compatibilityJobs.push(job.name);return {local_compatibility:true};},replay:async()=>assert.fail('no API replay expected')},
    transportFactory:({executions})=>{
      const mailbox=new RemoteFullCrawlStageStore({executionStore:executions});
      return {ready:()=>transportReady,
        async notifyTask(args){execution=args.execution;await executions.activation.claim(f.nodeId,{claim_id:randomUUID(),slot:f.slot,connection:f.connection});},
        async open(){return {fixture_network:true};},
        async stop(){networkStopped=true;return {active_managed_requests:0};},
        async notifyStage({request,command}){
          commands.push(command.stage);let data;
          if(command.stage==='admission'){admissionEntered=true;await admissionWait;data=channelSnapshot(f.channelId);}
          if(command.stage==='uploads')data={playlist_id:'UUfixture',entries:[],activity_evidence_complete:true,
            scan:{complete:true,stop_reason:'end',terminal_reason:'end',pages:1,inspected_count:0,parse_gap_count:0}};
          if(command.stage==='close_fetch')data={network_stopped:true,active_requests:0};
          const value={version:1,task_id:command.task_id,generation:command.generation,stage_id:command.stage_id,sequence:1,
            input_hash:command.input_hash,target_hash:command.target_hash,outcome:'success',records:[{observed_at:new Date().toISOString(),data}],error:null,handoff:null};
          for(const part of fullCrawlResultParts(value,command,execution,randomUUID()))await mailbox.receivePart(f.nodeId,request,part);
        }};
    }};
  try{
    center=createFullCrawlCenter(args);centers.push(center);
    const image='registry.example/full@sha256:'+'a'.repeat(64);
    await f.query('INSERT INTO remote_ingestion.node_deployments(node_id,deployment_id,image,worker_count,credentials_cipher) VALUES($1,$2,$3,1,$4)',[f.nodeId,f.connection.deployment_id,image,Buffer.from('status-only-fixture')]);
    admin=createRemoteDeploymentAdmin({store:f.store,routes:{privateKey:generateKeyPairSync('ed25519').privateKey},token:'p4-admin-fixture-only-password-000000000000',
      image:'registry.example/incremental@sha256:'+'b'.repeat(64),gatewayUrl:'https://center.example',fullCrawl:{image,execution:center.supervisor,activation:center.activation}});
    const control={nodeId:f.nodeId,deploymentId:f.connection.deployment_id,workerCount:1};
    await admin.setExecution({...control,enabled:true,expectedRequested:false});
    await center.supervisor.tick();
    await until(async()=>(await center.activation.heartbeat(f.nodeId,f.connection)).ready_for_tasks,()=>JSON.stringify(events));
    assert.equal((await admin.status(control)).counts.ready,1,'dashboard readiness is verified by the actual full supervisor');
    const entry=[...center.supervisor.entries.values()][0];
    assert.equal(entry.worker.name,'youtube-channel-crawl');
    const rival=createFullCrawlCenter(args);centers.push(rival);await rival.supervisor.tick();assert.equal(rival.supervisor.entries.size,0);
    transportReady=false;assert.equal(await center.supervisor.ready(entry),false);transportReady=true;
    await incremental.add('incremental-fixture',{}, {jobId:'untouched'});
    const {fetch_contract:uncachedContract,...uncachedData}=f.job.data;
    const job=await queue.add('channel-snapshot',uncachedData,{jobId:f.jobId,attempts:2});
    await until(()=>admissionEntered,()=>JSON.stringify(events));
    const redis=await queue.client;const lockKey=queue.toKey(job.id)+':lock';
    await delay(1200);assert.equal(await redis.exists(lockKey),1,'active lock must renew beyond initial duration');
    const draining=await admin.setExecution({...control,enabled:false,expectedRequested:true});
    assert.equal(draining.counts.finishing,1);
    await center.supervisor.tick();
    await delay(1200);assert.equal(await redis.exists(lockKey),1,'drain must preserve active job lock');
    releaseAdmission();
    await until(async()=>['completed','failed'].includes(await job.getState()),()=>JSON.stringify(events));
    const stored=await queue.getJob(job.id);assert.equal(await stored.getState(),'completed',stored.failedReason);
    assert.deepEqual(stored.data.fetch_contract,uncachedContract);
    assert.equal(stored.attemptsMade,1);assert.deepEqual(commands,['admission','uploads','close_fetch']);assert.deepEqual(rotaCalls,['begin','complete']);
    const attempt=(await f.query('SELECT status,finished_at FROM crawler.channel_execution_attempts WHERE business_run_id=$1',[f.runId])).rows[0];
    assert.equal(attempt.status,'success');assert.ok(attempt.finished_at);
    assert.equal((await f.query('SELECT state FROM remote_ingestion.tasks WHERE target_node_id=$1',[f.nodeId])).rows[0].state,'applied');
    assert.equal(await (await incremental.getJob('untouched')).getState(),'waiting');
    await until(()=>center.supervisor.entries.size===0);
    await center.supervisor.tick();assert.equal(center.supervisor.entries.size,0,'restart preserves explicit drain');
    // Idle reactivation uses a new real supervisor/adapter, never incremental recovery.
    await admin.setExecution({...control,enabled:true,expectedRequested:false});
    await center.activation.heartbeat(f.nodeId,f.connection);await center.supervisor.tick();
    await until(async()=>(await center.activation.heartbeat(f.nodeId,f.connection)).ready_for_tasks);
    const repairs=await Promise.all(['channel-detail-repair','channel-checkpoint-repair'].map(name=>queue.add(name,{channel_id:f.channelId})));
    await until(async()=>(await Promise.all(repairs.map(j=>j.getState()))).every(state=>state==='completed'));
    assert.deepEqual(compatibilityJobs,['channel-detail-repair','channel-checkpoint-repair']);
    // No capacity is a delayed job, not an exhausted business retry.
    const nextEntry=[...center.supervisor.entries.values()][0];
    const execute=nextEntry.rota.executeJob.bind(nextEntry.rota);nextEntry.rota.executeJob=async()=>{throw new RotaSlotDeferredError('local_capacity');};
    await queue.pause();
    const deferred=await queue.add('channel-snapshot',{...f.job.data,dispatch_generation:1},{jobId:f.jobId+'-deferred',attempts:1});
    await f.query('UPDATE crawler.channel_candidates SET snapshot_active_job_id=$2,snapshot_active_job_attempt=1 WHERE candidate_id=$1',[f.candidateId,deferred.id]);
    await queue.resume();
    await until(async()=>['delayed','failed'].includes(await deferred.getState()));
    const saved=await queue.getJob(deferred.id);assert.equal(await saved.getState(),'delayed',saved.failedReason);assert.equal(saved.attemptsMade,0);
    nextEntry.rota.executeJob=execute;
  }finally{await close();}
});
