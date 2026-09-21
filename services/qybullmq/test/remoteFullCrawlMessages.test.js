import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {fullCrawlInputHash,FULL_CRAWL_LIMITS} from '../src/remoteNodes/fullCrawlProtocol.js';
import {validateFullCrawlCommand,encodeFullCrawlResult,decodeFullCrawlResult,fullCrawlResultParts} from '../src/remoteNodes/fullCrawlMessages.js';
import {YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT,YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT} from '../src/fullCrawlFetchContract.js';
import {fullCrawlJobRoute} from '../src/remoteNodes/centerFullCrawlProcessor.js';
import {supervisionLockKey} from '../src/remoteNodes/centerExecutionSupervisor.js';

function fixture(contract=YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,stage='admission'){
  const execution={version:1,queue_name:'youtube-channel-crawl',job_name:'channel-snapshot',job_id:'fixture',job_attempt:1,
    candidate_id:1,channel_id:'UCfixture',run_id:'full-fixture',business_run_id:'full-fixture',business_run_key:'full-candidate:1',
    intent_hash:'sha256:'+'a'.repeat(64),dispatch_generation:1,execution_attempt_id:'channel-attempt:'+randomUUID(),fetch_contract:contract};
  const collection={channel_id:execution.channel_id,fetch_contract:contract,locale:'en',reference_at:new Date().toISOString(),
    channel_content_limit:20,content_max_age_days:90,optional_comments:contract.executor_version>=2};
  const target={reservation_id:randomUUID(),video_id:'dQw4w9WgXcQ',ordinal:1,target:{video_id:'dQw4w9WgXcQ',position:1},excluded_detail:null};
  const input=stage==='details'?{collection,detail_fence:{},targets:[target]}:{collection,include_about:true};
  const command={version:1,task_id:randomUUID(),generation:1,stage_id:randomUUID(),stage,sequence:1,
    execution_hash:fullCrawlInputHash(execution),input_hash:fullCrawlInputHash(input),target_hash:stage==='details'?'b'.repeat(64):null,input};
  const value={version:1,task_id:command.task_id,generation:1,stage_id:command.stage_id,sequence:1,input_hash:command.input_hash,
    target_hash:command.target_hash,outcome:'success',error:null,handoff:null,records:[{observed_at:new Date().toISOString(),
      data:{metadata:{channel_id:execution.channel_id},about_requested:true,about_observed:true}}]};
  return {execution,command,value,target};
}

test('wire results split at 512KiB and preserve strict uncompressed JSON evidence',()=>{
  const f=fixture();f.value.records[0].data.raw='x'.repeat(FULL_CRAWL_LIMITS.partBytes);
  const parts=fullCrawlResultParts(f.value,f.command,f.execution,randomUUID());
  assert.equal(parts.length,2);assert.equal(parts[0].payload.length,FULL_CRAWL_LIMITS.partBytes);
  assert.deepEqual(decodeFullCrawlResult(Buffer.concat(parts.map(p=>p.payload)),f.command,f.execution),f.value);
  assert.throws(()=>decodeFullCrawlResult(Buffer.from([0xff]),f.command,f.execution),{code:'FULL_CRAWL_RESULT_JSON'});
  f.value.records[0].data.raw='x'.repeat(FULL_CRAWL_LIMITS.batchBytes);
  assert.throws(()=>encodeFullCrawlResult(f.value,f.command,f.execution),{code:'FULL_CRAWL_BATCH_SIZE'});
});
test('commands and results reject changed identity, unknown decisions and missing About',()=>{
  const f=fixture();const bad=structuredClone(f.command);bad.input.collection.optional_comments=false;bad.input_hash=fullCrawlInputHash(bad.input);
  assert.throws(()=>validateFullCrawlCommand(bad,f.execution),{code:'FULL_CRAWL_COLLECTION_CONFLICT'});
  for(const patch of [{generation:2},{task_id:randomUUID()},{input_hash:'b'.repeat(64)},{qualification:'accepted'}])
    assert.throws(()=>encodeFullCrawlResult({...f.value,...patch},f.command,f.execution));
  const uploads={...f.command,stage:'uploads',input:{collection:f.command.input.collection,has_content:false,country:['BR'],network:{egress_country:'BR',country_recheck:null}}};
  uploads.input_hash=fullCrawlInputHash(uploads.input);
  assert.throws(()=>validateFullCrawlCommand(uploads,f.execution),{code:'FULL_CRAWL_UPLOADS_INPUT'});
  f.value.records[0].data.about_observed=false;
  assert.throws(()=>encodeFullCrawlResult(f.value,f.command,f.execution),{code:'FULL_CRAWL_ABOUT_REQUIRED'});
});
for(const contract of [YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT,YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT]){
  test(`API-needed evidence is gated by the frozen executor version ${contract.executor_version}`,()=>{
    const f=fixture(contract,'details');f.value.outcome='api_required';f.value.records=[{reservation_id:f.target.reservation_id,video_id:f.target.video_id,
      start_id:randomUUID(),observed_at:new Date().toISOString(),outcome:'api_required',data:{id:f.target.video_id},error:{name:'YoutubeJsRequiredSurfaceError',code:null,message:'Missing required surface',status:null,surface:'player'}}];
    if(contract.executor_version===3)assert.ok(encodeFullCrawlResult(f.value,f.command,f.execution));
    else assert.throws(()=>encodeFullCrawlResult(f.value,f.command,f.execution),{code:'FULL_CRAWL_API_HANDOFF'});
    f.value.records[0].video_id='other';assert.throws(()=>encodeFullCrawlResult(f.value,f.command,f.execution),{code:'FULL_CRAWL_TARGET_CONFLICT'});
  });
}
test('mixed queue routes retain local repairs and isolate incremental queue and supervisor locks',()=>{
  const f=fixture();const job={queueName:f.execution.queue_name,name:'channel-snapshot',data:{candidate_id:1,fetch_contract:f.execution.fetch_contract}};
  assert.equal(fullCrawlJobRoute(job),'remote');
  for(const name of ['channel-detail-repair','channel-checkpoint-repair'])assert.equal(fullCrawlJobRoute({...job,name}),'compatibility');
  assert.equal(fullCrawlJobRoute({...job,data:{...job.data,publication_gap_scope:'about_only'}}),'compatibility');
  assert.equal(fullCrawlJobRoute({...job,data:{candidate_id:1}}),'compatibility');
  assert.throws(()=>fullCrawlJobRoute({...job,queueName:'youtube-channel-incremental'}));
  const row={node_id:randomUUID(),slot:'full-crawl-1'};
  assert.notEqual(supervisionLockKey({...row,mode:'full_crawl_collect'}),supervisionLockKey({...row,mode:'incremental_collect'}));
});
