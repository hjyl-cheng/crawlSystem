import {YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT,YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT} from '../src/fullCrawlFetchContract.js';
import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {fullCrawlFixture,channelSnapshot,publicDetail} from './helpers/remoteFullCrawlFixture.js';
import {runRemoteFullCrawl} from '../src/remoteNodes/fullCrawlCoordinator.js';
import {RemoteFullCrawlStageStore} from '../src/remoteNodes/fullCrawlStageStore.js';
import {fullCrawlResultParts} from '../src/remoteNodes/fullCrawlMessages.js';

const url=process.env.REMOTE_NODE_TEST_DATABASE_URL;
for(const source of ['Query','Migration']) for(const contract of [YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT,YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT])
test(`remote ${source} v${contract.executor_version} stage evidence drives the original full lifecycle and atomic business receipts`,{skip:!url,timeout:60000},async t=>{
  const f=await fullCrawlFixture(t,{contract});
  f.job.data.reject_if_no_recent_content=source==='Migration';
  const request=await f.claim();
  const mailbox=new RemoteFullCrawlStageStore({executionStore:f.full});const stages=[],handoffs=[];
  let firstPart;
  const result=await runRemoteFullCrawl({executionStore:f.full,nodeId:f.nodeId,request,job:f.job,signal:AbortSignal.timeout(30000),
    handoff:{candidateSettled:async()=>handoffs.push('candidate'),fetchCompleted:async()=>handoffs.push('finalize')},
    onCommand:async command=>{
      stages.push(command.stage);
      const now=new Date().toISOString();let records;
      if(command.stage==='admission')records=[{observed_at:now,data:channelSnapshot(f.channelId)}];
      if(command.stage==='uploads')records=[{observed_at:now,data:{playlist_id:'UUfixture',entries:[
        {video_id:'dQw4w9WgXcQ',position:1,title:'normal',published_at:now,published_at_status:'exact',published_at_precision:'date_only',published_at_source:'youtubejs_player_microformat'},{video_id:'dQw4w9WgXcR',position:2,title:'upcoming',is_upcoming:true,live_status:'is_upcoming'}],
        activity_evidence_complete:true,scan:{complete:true,stop_reason:'limit',terminal_reason:'limit',pages:1,inspected_count:2,parse_gap_count:0}}}];
      if(command.stage==='details')records=command.input.targets.map(target=>({reservation_id:target.reservation_id,video_id:target.video_id,
        start_id:randomUUID(),observed_at:now,outcome:target.target.is_upcoming?'preflight':'captured',
        data:target.target.is_upcoming?{}:{...publicDetail(target.video_id),published_at:now,published_at_status:'exact',published_at_precision:'date_only',published_at_source:'youtubejs_player_microformat'},error:null}));
      if(command.stage==='close_fetch')records=[{observed_at:now,data:{network_stopped:true,active_requests:0}}];
      const value={version:1,task_id:command.task_id,generation:command.generation,stage_id:command.stage_id,sequence:1,
        input_hash:command.input_hash,target_hash:command.target_hash,outcome:'success',records,error:null,handoff:null};
      const parts=fullCrawlResultParts(value,command,f.execution,randomUUID());
      for(const part of parts){
        assert.equal((await mailbox.receivePart(f.nodeId,request,part)).state,'durable_received');
        assert.equal((await mailbox.receivePart(f.nodeId,request,part)).state,'durable_received');
      }
      if(!firstPart){firstPart=parts[0];await assert.rejects(mailbox.receivePart(f.nodeId,request,{...firstPart,part_hash:'0'.repeat(64)}),{code:'FULL_CRAWL_PART_HASH_CONFLICT'});}
      if(command.stage==='uploads'){
        const batchId=parts[0].manifest.batch_id;
        await assert.rejects(mailbox.apply(f.nodeId,request,batchId,async client=>{
          await client.query('UPDATE crawler.channel_candidates SET snapshot_attempts=snapshot_attempts+100 WHERE candidate_id=$1',[f.candidateId]);
          throw new Error('receipt transaction interrupted');
        }),/receipt transaction interrupted/);
        assert.equal((await f.query('SELECT state FROM remote_ingestion.full_crawl_result_batches WHERE batch_id=$1',[batchId])).rows[0].state,'received');
      }
    }});
  assert.equal(result.ok,true);
  assert.equal(result.migration_activity_gate.decision,source==='Migration'?'passed':'not_required');
  assert.deepEqual((await f.query("SELECT result_json->'fetch_contract' AS contract FROM crawler.channel_runs WHERE run_id=$1",[f.runId])).rows[0].contract,contract);
  assert.deepEqual(stages,['admission','uploads','details','close_fetch']);assert.deepEqual(handoffs,['candidate','finalize']);
  assert.deepEqual((await f.query('SELECT attempts,detail_status,disposition FROM crawler.content_candidates WHERE run_id=$1 ORDER BY position',[f.runId])).rows,
    [{attempts:1,detail_status:'done',disposition:'stored'},{attempts:1,detail_status:'done',disposition:'terminal_excluded'}]);
  assert.equal((await f.query("SELECT count(*)::int AS n FROM remote_ingestion.full_crawl_result_batches b JOIN remote_ingestion.full_crawl_stages s USING(stage_id) WHERE s.task_id=$1 AND b.state='applied'",[request.task_id])).rows[0].n,4);
  assert.equal((await f.query('SELECT snapshot_attempts FROM crawler.channel_candidates WHERE candidate_id=$1',[f.candidateId])).rows[0].snapshot_attempts,1);
});

function nodeFixture(f,request,{api=false,reject=false}={}){
  const mailbox=new RemoteFullCrawlStageStore({executionStore:f.full}),sent=new Map();let apiSent=false;
  const onCommand=async command=>{
    if(sent.has(command.stage_id))return;
    const now=new Date().toISOString();let records,outcome='success';
    if(command.stage==='admission'){
      const data=channelSnapshot(f.channelId);if(reject){data.metadata.subscriber_count=1;data.metadata.subscriber_count_text='1';}
      records=[{observed_at:now,data}];
    }
    if(command.stage==='uploads')records=[{observed_at:now,data:{playlist_id:'UUfixture',entries:[1,2].map(position=>({
      video_id:position===1?'dQw4w9WgXcQ':'dQw4w9WgXcR',position,title:'fixture'})),activity_evidence_complete:true,
      scan:{complete:true,stop_reason:'limit',terminal_reason:'limit',pages:1,inspected_count:2,parse_gap_count:0}}}];
    if(command.stage==='details'){
      const targets=api&&!apiSent?command.input.targets.slice(0,1):command.input.targets;
      outcome=api&&!apiSent?'api_required':'success';
      records=targets.map(target=>({reservation_id:target.reservation_id,video_id:target.video_id,start_id:randomUUID(),observed_at:now,
        outcome:outcome==='api_required'?'api_required':'captured',data:publicDetail(target.video_id),
        error:outcome==='api_required'?{name:'YoutubeJsRequiredSurfaceError',code:null,message:'required player evidence missing',status:null,surface:'player'}:null}));
      apiSent||=api;
    }
    if(command.stage==='close_fetch')records=[{observed_at:now,data:{network_stopped:true,active_requests:0}}];
    const value={version:1,task_id:command.task_id,generation:command.generation,stage_id:command.stage_id,sequence:1,
      input_hash:command.input_hash,target_hash:command.target_hash,outcome,records,error:null,handoff:null};
    const parts=fullCrawlResultParts(value,command,f.execution,randomUUID());sent.set(command.stage_id,{command,parts});
    for(const part of parts)await mailbox.receivePart(f.nodeId,request,part);
  };
  return {onCommand,sent,mailbox};
}
const handoff={candidateSettled:async()=>{},fetchCompleted:async()=>{}};
test('same-owner resume applies only the remaining durable detail prefix',{skip:!url,timeout:60000},async t=>{
  const f=await fullCrawlFixture(t),request=await f.claim(),node=nodeFixture(f,request);
  const args={executionStore:f.full,nodeId:f.nodeId,request,job:f.job,signal:AbortSignal.timeout(30000),handoff,onCommand:node.onCommand};
  f.job.updateProgress=async()=>{throw new Error('simulated center interruption after first applied receipt');};
  await assert.rejects(runRemoteFullCrawl(args),/simulated center interruption/);
  assert.deepEqual((await f.query('SELECT attempts,detail_status FROM crawler.content_candidates WHERE run_id=$1 ORDER BY position',[f.runId])).rows,
    [{attempts:1,detail_status:'done'},{attempts:0,detail_status:'queued'}]);
  f.job.updateProgress=async()=>{};
  assert.equal((await runRemoteFullCrawl({...args,resumeMode:'same_owner_resume'})).ok,true);
  assert.deepEqual((await f.query('SELECT attempts,detail_status FROM crawler.content_candidates WHERE run_id=$1 ORDER BY position',[f.runId])).rows,
    [{attempts:1,detail_status:'done'},{attempts:1,detail_status:'done'}]);
});
test('rejected admission has a durable replayable ACK but grants no further business writes',{skip:!url,timeout:60000},async t=>{
  const f=await fullCrawlFixture(t),request=await f.claim(),node=nodeFixture(f,request,{reject:true});
  Object.assign(f.job.data,{enforce_min_subscribers:true,min_subscriber_count:1000});
  const result=await runRemoteFullCrawl({executionStore:f.full,nodeId:f.nodeId,request,job:f.job,signal:AbortSignal.timeout(30000),handoff,onCommand:node.onCommand});
  assert.equal(result.skipped,true);
  const part=[...node.sent.values()][0].parts[0];
  assert.equal((await node.mailbox.receivePart(f.nodeId,request,part)).state,'applied');
  await assert.rejects(f.full.renew(f.nodeId,request),{code:'FULL_CRAWL_BUSINESS_FENCE_STALE'});
  await assert.rejects(node.mailbox.receivePart(f.nodeId,{...request,generation:2},part),{code:'STALE_LEASE'});
});
test('v3 API evidence uses the original center fallback and stable request before reissuing unstarted targets',{skip:!url,timeout:60000},async t=>{
  const {YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT}=await import('../src/fullCrawlFetchContract.js');
  const {createVideoDetailApiFallback}=await import('../src/videoDetailApiFallback.js');
  const f=await fullCrawlFixture(t,{contract:YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT}),request=await f.claim(),node=nodeFixture(f,request,{api:true});
  const requests=[];
  const fallback=createVideoDetailApiFallback({query:f.query,withTransaction:action=>f.store.transaction(action),
    loadSettings:async()=>({apiKeys:['fixture-key'],dailyRequestLimit:50,fallbackMode:'emergency'}),
    request:async(_transaction,value)=>requests.push(value),wait:async()=>({title:'API fixture',published_at:new Date().toISOString(),
      view_count_text:'100',duration_seconds:60,privacy_status:'public',comments_disabled:true,source:'youtube_data_api_videos_list'})});
  assert.equal((await runRemoteFullCrawl({executionStore:f.full,nodeId:f.nodeId,request,job:f.job,signal:AbortSignal.timeout(30000),handoff,
    onCommand:node.onCommand,videoApiFallback:fallback})).ok,true);
  assert.equal(requests.length,1);assert.equal(requests[0].requestId,JSON.stringify(['full',f.runId,'dQw4w9WgXcQ']));
  const detailStages=[...node.sent.values()].filter(row=>row.command.stage==='details');assert.equal(detailStages.length,2);
  assert.equal(detailStages[1].command.input.targets.length,1);
  assert.deepEqual((await f.query('SELECT attempts,detail_status FROM crawler.content_candidates WHERE run_id=$1 ORDER BY position',[f.runId])).rows,
    [{attempts:1,detail_status:'done'},{attempts:1,detail_status:'done'}]);
});
