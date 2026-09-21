import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm,appendFile,writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import * as crypto from 'node:crypto';
import {tmpdir} from 'node:os';
import {fullCrawlFixture,channelSnapshot,publicDetail} from './helpers/remoteFullCrawlFixture.js';
import {FullCrawlNodeJournal} from '../src/remoteNodes/fullCrawlNode.js';
import {RemoteResultSpool} from '../src/remoteNodes/spool.js';
import {RemoteFullCrawlTransportStore} from '../src/remoteNodes/fullCrawlTransportStore.js';
import {runRemoteFullCrawl} from '../src/remoteNodes/fullCrawlCoordinator.js';
import {fullCrawlResultError} from '../src/remoteNodes/fullCrawlMessages.js';
import {fullCrawlErrorEvidence} from '../src/remoteNodes/fullCrawlNode.js';
const url=process.env.REMOTE_NODE_TEST_DATABASE_URL;
const uploads={playlist_id:'UUfixture',entries:[{video_id:'dQw4w9WgXcQ',position:1,title:'normal'},
  {video_id:'dQw4w9WgXcR',position:2,title:'upcoming',is_upcoming:true,live_status:'is_upcoming'}],
  activity_evidence_complete:true,scan:{complete:true,stop_reason:'limit',terminal_reason:'limit',pages:1,inspected_count:2,parse_gap_count:0}};

test('full node journal: per-video start, real business writes, lost SQL ACK and expired-lease evidence replay',{skip:!url,timeout:60000},async t=>{
  const f=await fullCrawlFixture(t),request=await f.claim(),transport=new RemoteFullCrawlTransportStore({executions:f.full});
  const directory=await mkdtemp(join(tmpdir(),'full-node-'));t.after(()=>rm(directory,{recursive:true,force:true}));
  const spool=new RemoteResultSpool({directory,maxBytes:16*1024*1024});await spool.init();
  let dropped=false,detailCalls=0,lastPart;
  const client={transport:'nats',fullCrawlRecovered:value=>transport.recovered(f.nodeId,value),fullCrawlStarted:value=>transport.started(f.nodeId,value),
    async uploadFullCrawl(request,part){lastPart=part;const ack=await transport.receive(f.nodeId,request.task_id,Buffer.from(JSON.stringify({request,...part,payload:part.payload.toString('base64')})));
      if(!dropped){dropped=true;throw Error('SIMULATED_ACK_LOSS');}return ack;}};
  const node=new FullCrawlNodeJournal({spool,client});
  const youtube={openChannel:async()=>channelSnapshot(f.channelId),fetchUploads:async()=>uploads,
    fetchDetail:async id=>{detailCalls++;return publicDetail(id);}};
  const stages=[];
  const result=await runRemoteFullCrawl({executionStore:f.full,nodeId:f.nodeId,request,job:f.job,
    signal:AbortSignal.timeout(30000),handoff:{candidateSettled:async()=>{},fetchCompleted:async()=>{}},
    onCommand:async command=>{
      stages.push(command.stage);
      if(command.stage==='details'){
        const filler=join(directory,'disk-pressure');await writeFile(filler,Buffer.alloc(9*1024*1024));
        await assert.rejects(node.execute({request,execution:f.execution,command,youtube,signal:AbortSignal.timeout(5000)}),/SPOOL_FULL/);
        assert.equal(detailCalls,0);assert.equal((await f.query('SELECT sum(attempts)::int AS n FROM crawler.content_candidates WHERE run_id=$1',[f.runId])).rows[0].n,0);
        await rm(filler);
      }
      const execute=()=>node.execute({request,execution:f.execution,command,youtube,signal:AbortSignal.timeout(10000),networkStopped:command.stage==='close_fetch'});
      try{await execute();}catch(error){if(error.message!=='SIMULATED_ACK_LOSS')throw error;await appendFile(join(directory,'full',command.stage_id,'journal.ndjson'),'{"key":');await node.recover();await execute();}
    }});
  assert.equal(result.ok,true);assert.equal(detailCalls,1);
  assert.deepEqual(stages,['admission','uploads','details','close_fetch']);
  assert.deepEqual((await f.query('SELECT attempts,detail_status FROM crawler.content_candidates WHERE run_id=$1 ORDER BY position',[f.runId])).rows,
    [{attempts:1,detail_status:'done'},{attempts:1,detail_status:'done'}]);
  await f.query("UPDATE remote_ingestion.tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE task_id=$1",[request.task_id]);
  await f.query("UPDATE remote_ingestion.worker_connections SET connected_until=clock_timestamp()-interval '1 second' WHERE node_id=$1",[f.nodeId]);
  const bytes=Buffer.from(JSON.stringify({request,...lastPart,payload:lastPart.payload.toString('base64')}));
  assert.equal((await transport.receive(f.nodeId,request.task_id,bytes)).state,'applied');
  await assert.rejects(transport.heartbeat(f.nodeId,request),{code:'WORKER_CONNECTION_STALE'});
  const wrong={...request,connection:{...request.connection,instance_id:'00000000-0000-4000-8000-000000000001'}};
  await assert.rejects(transport.receive(f.nodeId,request.task_id,Buffer.from(JSON.stringify({request:wrong,...lastPart,payload:lastPart.payload.toString('base64')}))),{code:'STALE_LEASE'});
});

test('node wire preserves network failure evidence and original API policy classification',()=>{
  const cause=Object.assign(Error('upstream failed'),{name:'ParserContractError',code:'ETIMEDOUT',partial_detail:{id:'abc'},youtube_failure_evidence:{kind:'proxy_transport'}});
  const revived=fullCrawlResultError(fullCrawlErrorEvidence(cause));
  assert.equal(revived.name,cause.name);assert.equal(revived.code,cause.code);
  assert.deepEqual(revived.partial_detail,cause.partial_detail);assert.deepEqual(revived.youtube_failure_evidence,cause.youtube_failure_evidence);
});

test('exclusive center recovery preserves captured details and new owner reuses them without another attempt',{skip:!url,timeout:60000},async t=>{
  const {BrowserProfileStore}=await import('../src/browserProfileStore.js');
  const {recoverFullCrawlSlot}=await import('../src/remoteNodes/fullCrawlCenterRecovery.js');
  const {supervisionLockKey}=await import('../src/remoteNodes/centerExecutionRecovery.js');
  const f=await fullCrawlFixture(t),oldRequest=await f.claim(),transport=new RemoteFullCrawlTransportStore({executions:f.full});
  const directory=await mkdtemp(join(tmpdir(),'full-recover-'));t.after(()=>rm(directory,{recursive:true,force:true}));
  const spool=new RemoteResultSpool({directory});await spool.init();let calls=0;
  const client={transport:'nats',fullCrawlRecovered:value=>transport.recovered(f.nodeId,value),fullCrawlStarted:value=>transport.started(f.nodeId,value),uploadFullCrawl:(request,part)=>transport.receive(f.nodeId,request.task_id,
    Buffer.from(JSON.stringify({request,...part,payload:part.payload.toString('base64')})))};
  const node=new FullCrawlNodeJournal({spool,client});
  const youtube={openChannel:async()=>channelSnapshot(f.channelId),fetchUploads:async()=>uploads,fetchDetail:async id=>{calls++;return publicDetail(id);}};
  const handoff={candidateSettled:async()=>{},fetchCompleted:async()=>{}};
  await assert.rejects(runRemoteFullCrawl({executionStore:f.full,nodeId:f.nodeId,request:oldRequest,job:f.job,signal:AbortSignal.timeout(10000),handoff,
    onCommand:async command=>{await node.execute({request:oldRequest,execution:f.execution,command,youtube,signal:AbortSignal.timeout(5000)});
      if(command.stage==='details')throw Error('CENTER_INTERRUPTED_BEFORE_APPLY');}}),/CENTER_INTERRUPTED_BEFORE_APPLY/);
  const row={...f.connection,rota_worker_id:f.workerId},lockKey=supervisionLockKey(row),guard=await f.pool.connect();
  try{
    await assert.rejects(recoverFullCrawlSlot({guard,row,lockKey,profileSecret:f.profileSecret}),{code:'REMOTE_RECOVERY_NOT_OWNER'});
    await guard.query('SELECT pg_advisory_lock(781138012,hashtext($1))',[lockKey]);
    assert.deepEqual(await recoverFullCrawlSlot({guard,row,lockKey,profileSecret:f.profileSecret}),{closed:1,settled:true});
  }finally{await guard.query('SELECT pg_advisory_unlock(781138012,hashtext($1))',[lockKey]);guard.release();}
  await f.query('UPDATE crawler.channel_candidates SET snapshot_active_job_attempt=2 WHERE candidate_id=$1',[f.candidateId]);
  const profiles=new BrowserProfileStore({queryFn:f.query,transactionFn:action=>f.store.transaction(action),secret:f.profileSecret});
  const attempt=await profiles.beginAttempt({...f.attemptArgs,jobAttempt:1,task:{...f.attemptArgs.task,task_id:crypto.randomUUID(),attempt_number:2}});
  const execution={...f.execution,job_attempt:2,execution_attempt_id:attempt};
  const prepared=await f.full.prepare({execution,connection:f.connection});
  const {randomUUID}=await import('node:crypto');
  const lease=await f.full.activation.claim(f.nodeId,{claim_id:randomUUID(),slot:f.slot,connection:f.connection});
  const request={task_id:prepared.taskId,generation:lease.generation,connection:f.connection};
  f.job.attemptsStarted=2;
  const result=await runRemoteFullCrawl({executionStore:f.full,nodeId:f.nodeId,request,job:f.job,signal:AbortSignal.timeout(15000),handoff,
    onCommand:command=>node.execute({request,execution,command,youtube,signal:AbortSignal.timeout(5000),networkStopped:command.stage==='close_fetch'})});
  assert.equal(result.ok,true);assert.equal(calls,1,'captured raw detail was reused');
  assert.deepEqual((await f.query('SELECT attempts,detail_status FROM crawler.content_candidates WHERE run_id=$1 ORDER BY position',[f.runId])).rows,
    [{attempts:1,detail_status:'done'},{attempts:1,detail_status:'done'}]);
  await assert.rejects(f.full.renew(f.nodeId,oldRequest),{code:'STALE_LEASE'});
});

test('v3 API handoff restores a lost BullMQ continuation and replays without network or another detail attempt',{skip:!url,timeout:60000},async t=>{
  const {YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT}=await import('../src/fullCrawlFetchContract.js');
  const {createVideoDetailApiFallback}=await import('../src/videoDetailApiFallback.js');
  const {restoreFullCrawlHandoff,runFullCrawlApiReplay}=await import('../src/remoteNodes/fullCrawlApiReplay.js');
  const {gateVideoApiJob}=await import('../src/videoApiContinuation.js');
  const f=await fullCrawlFixture(t,{contract:YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT}),request=await f.claim();
  const directory=await mkdtemp(join(tmpdir(),'full-api-'));t.after(()=>rm(directory,{recursive:true,force:true}));
  const spool=new RemoteResultSpool({directory});await spool.init();
  const transport=new RemoteFullCrawlTransportStore({executions:f.full});
  const client={fullCrawlRecovered:value=>transport.recovered(f.nodeId,value),fullCrawlStarted:value=>transport.started(f.nodeId,value),uploadFullCrawl:(request,part)=>transport.receive(f.nodeId,request.task_id,
    Buffer.from(JSON.stringify({request,...part,payload:part.payload.toString('base64')})))};
  const node=new FullCrawlNodeJournal({spool,client});let calls=0;
  const videoId='api'+crypto.randomBytes(4).toString('hex');
  const youtube={openChannel:async()=>channelSnapshot(f.channelId),fetchUploads:async()=>({...uploads,entries:[{video_id:videoId,position:1,title:'API'}]}),
    fetchDetail:async()=>{calls++;throw Object.assign(Error('required player field missing'),{name:'YoutubeJsRequiredSurfaceError',required_surface:'player',partial_detail:publicDetail(videoId)});}};
  const createApiFallback=args=>createVideoDetailApiFallback({...args,loadSettings:async()=>({apiKeys:['fixture'],dailyRequestLimit:100,fallbackMode:'emergency'})});
  const handoff={candidateSettled:async()=>{},fetchCompleted:async()=>{}};let pending;
  try{await runRemoteFullCrawl({executionStore:f.full,nodeId:f.nodeId,request,job:f.job,signal:AbortSignal.timeout(10000),handoff,
    videoApiFallback:createApiFallback({query:f.query,withTransaction:action=>f.store.transaction(action)}),
    onCommand:command=>node.execute({request,execution:f.execution,command,youtube,signal:AbortSignal.timeout(5000)})});assert.fail('API must wait');}
  catch(error){assert.equal(error.code,'VIDEO_API_PENDING');pending=error;}
  assert.equal(calls,3,'parser retries perform real node collection attempts');
  const requestId=JSON.stringify(['full',f.runId,videoId]);assert.equal(pending.requestId,requestId);
  await f.query("UPDATE remote_ingestion.tasks SET state='received',last_error='VIDEO_API_PENDING',applied_result=$2,lease_until=NULL WHERE task_id=$1",
    [request.task_id,{version:1,code:pending.code,request_id:requestId,run_id:f.runId}]);
  await f.query("UPDATE crawler.channel_execution_attempts SET status='success',finished_at=clock_timestamp() WHERE attempt_id=$1",[f.execution.execution_attempt_id]);
  await restoreFullCrawlHandoff({query:f.query,job:f.job});assert.equal(f.job.data.video_api_continuation.request_id,requestId);
  let delayed;f.job.moveToDelayed=async value=>{delayed=value;};
  await assert.rejects(gateVideoApiJob({query:f.query,job:f.job,token:'test',delayMs:1000}),{name:'DelayedError'});assert.ok(delayed>Date.now());
  await f.query("UPDATE crawler.youtube_api_detail_requests SET status='done',detail_json=$2,finished_at=now() WHERE request_id=$1",
    [requestId,{...publicDetail(videoId),privacy_status:'public',source:'youtube_data_api_videos_list'}]);
  const result=await runFullCrawlApiReplay({store:f.store,job:f.job,handoff,createApiFallback});
  assert.equal(result.ok,true);assert.equal(calls,3);
  assert.equal((await f.query('SELECT attempts FROM crawler.content_candidates WHERE run_id=$1',[f.runId])).rows[0].attempts,1);
  delete f.job.data.video_api_continuation;
  await restoreFullCrawlHandoff({query:f.query,job:f.job});assert.equal(f.job.data.video_api_continuation,undefined);
});

test('same-owner coordinator restart keeps node-started candidates running and applies the durable prefix once',{skip:!url,timeout:30000},async t=>{
  const f=await fullCrawlFixture(t),request=await f.claim(),transport=new RemoteFullCrawlTransportStore({executions:f.full});
  const directory=await mkdtemp(join(tmpdir(),'full-same-owner-'));t.after(()=>rm(directory,{recursive:true,force:true}));
  const spool=new RemoteResultSpool({directory});await spool.init();let calls=0,interrupt=true;
  const client={fullCrawlStarted:value=>transport.started(f.nodeId,value),uploadFullCrawl:(request,part)=>transport.receive(f.nodeId,request.task_id,
    Buffer.from(JSON.stringify({request,...part,payload:part.payload.toString('base64')})))};
  const node=new FullCrawlNodeJournal({spool,client}),youtube={openChannel:async()=>channelSnapshot(f.channelId),fetchUploads:async()=>uploads,fetchDetail:async id=>{calls++;return publicDetail(id);}};
  const args={executionStore:f.full,nodeId:f.nodeId,request,job:f.job,signal:AbortSignal.timeout(15000),handoff:{candidateSettled:async()=>{},fetchCompleted:async()=>{}},
    onCommand:async command=>{await node.execute({request,execution:f.execution,command,youtube,signal:AbortSignal.timeout(5000),networkStopped:command.stage==='close_fetch'});
      if(command.stage==='details'&&interrupt){interrupt=false;throw Error('INTERRUPTED');}}};
  await assert.rejects(runRemoteFullCrawl(args),/INTERRUPTED/);
  assert.equal((await runRemoteFullCrawl({...args,resumeMode:'same_owner_resume'})).ok,true);assert.equal(calls,1);
  assert.deepEqual((await f.query('SELECT attempts,detail_status FROM crawler.content_candidates WHERE run_id=$1 ORDER BY position',[f.runId])).rows,
    [{attempts:1,detail_status:'done'},{attempts:1,detail_status:'done'}]);
});


test('a continuation created by a local worker retains the explicit local replay in a no-network scope',async()=>{
  const {runFullCrawlApiReplay}=await import('../src/remoteNodes/fullCrawlApiReplay.js');
  const {isVideoApiReplay}=await import('../src/videoApiContinuation.js');
  const job={id:'original-local-job',data:{run_id:'original-local-run'}};let calls=0;
  const result=await runFullCrawlApiReplay({store:{pool:{query:async()=>({rows:[]})}},job,createApiFallback:()=>{},
    replayLocal:async original=>{calls++;assert.equal(original,job);assert.equal(isVideoApiReplay(),true);return {ok:true};}});
  assert.deepEqual(result,{ok:true});assert.equal(calls,1);
});
