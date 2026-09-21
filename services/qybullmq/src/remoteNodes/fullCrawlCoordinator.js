import {pendingFullCrawlCountryRecheck} from './fullCrawlCountryReplay.js';
import {UploadsCountryRecheck} from '../youtubeUploadsCountry.js';
import {fromChannelWire} from './channelWire.js';
import {randomUUID} from 'node:crypto';
import {runWithChannelExecution,currentChannelExecution} from '../channelExecutionContext.js';
import {FullCrawlYoutubeJsStore} from '../fullCrawlYoutubeJsStore.js';
import {createFullCrawlYoutubeJsExecutor} from '../fullCrawlYoutubeJsFactory.js';
import {RemoteFullCrawlDetailStore} from './fullCrawlDetailStore.js';
import {RemoteFullCrawlStageStore} from './fullCrawlStageStore.js';
import {fullCrawlResultError,validateFullCrawlCommand} from './fullCrawlMessages.js';
import {RemoteProtocolError} from './protocol.js';

const local=client=>new FullCrawlYoutubeJsStore({query:client.query.bind(client),withTransaction:action=>action(client)});
const fail=code=>{throw new RemoteProtocolError(code);};

// Shared lifecycle + a durable mailbox Collector Adapter. It does no YouTube
// I/O. onCommand only wakes the transport; a notification is never a result.
export async function runRemoteFullCrawl({executionStore,nodeId,request,job,handoff,signal,
  onCommand=async()=>{},videoApiFallback=null,resumeMode='initial',egressCountry=null,countryRecheck=null,locale='en',stageTimeoutMs=120000}){
  if(!signal)throw new TypeError('full-crawl execution abort signal required');
  for(const name of ['candidateSettled','fetchCompleted'])if(typeof handoff?.[name]!=='function')throw new TypeError(`handoff.${name} required`);
  const stages=new RemoteFullCrawlStageStore({executionStore}),details=new RemoteFullCrawlDetailStore({executionStore});
  const frozen=await executionStore.withLease(nodeId,request,async(client,ownership)=>{
    let settings=ownership.evidence.settings_snapshot,reference=ownership.evidence.reference_at;
    if(!settings){settings=await local(client).loadSettings();reference=new Date();
      await client.query(`UPDATE remote_ingestion.full_crawl_executions SET settings_snapshot=$3,reference_at=$4
        WHERE task_id=$1 AND generation=$2`,[request.task_id,request.generation,settings,reference]);}
    const last=(await client.query(`SELECT sequence FROM remote_ingestion.full_crawl_stages
      WHERE task_id=$1 AND generation=$2 AND applied_at IS NOT NULL ORDER BY sequence DESC LIMIT 1`,[request.task_id,request.generation])).rows[0];
    return {settings,reference:new Date(reference).toISOString(),execution:ownership.input,sequence:last?.sequence??0};
  });
  const restored=await executionStore.withLease(nodeId,request,client=>local(client).restore(job));
  if(restored.run){
    frozen.reference=new Date(restored.run.started_at).toISOString();
    frozen.settings={...frozen.settings,channelContentLimit:Number(restored.run.content_limit),
      contentMaxAgeDays:Number(restored.run.result_json?.content_max_age_days??frozen.settings.contentMaxAgeDays)};
  }
  const collection={channel_id:frozen.execution.channel_id,fetch_contract:frozen.execution.fetch_contract,locale,
    reference_at:frozen.reference,channel_content_limit:frozen.settings.channelContentLimit,
    content_max_age_days:frozen.settings.contentMaxAgeDays,optional_comments:frozen.execution.fetch_contract.executor_version>=2};
  let observedAt=null;
  let sequence=frozen.sequence,pending=null,batch=null,records=[],activeRecord=null,detailCommand=null,batchSequence=1;
  const wait=async(command,number=1)=>{
    const bounded=AbortSignal.any([signal,AbortSignal.timeout(stageTimeoutMs)]);
    bounded.throwIfAborted();
    const existing=await stages.read(nodeId,request,command.stage_id,number);if(existing)return existing;
    let abort;
    const cancelled=new Promise((_resolve,reject)=>{abort=()=>reject(bounded.reason);bounded.addEventListener('abort',abort,{once:true});if(bounded.aborted)abort();});
    try{await Promise.race([Promise.resolve().then(()=>onCommand(command,{signal:bounded})),cancelled]);}
    finally{bounded.removeEventListener('abort',abort);}
    return stages.wait(nodeId,request,command,{sequence:number,signal:bounded});
  };
  const issue=async(stage,input,targetHash=null)=>{
    const command=await stages.issue(nodeId,request,{stage,sequence:++sequence,input:{collection,...input},targetHash});
    pending=await wait(command);observedAt=pending.value.records[0]?.observed_at??null;
    if(pending.value.outcome==='error')throw fullCrawlResultError(pending.value.error);
    if(pending.value.outcome==='country_recheck'){
      const error=new Error('Full Crawl uploads require a country recheck');error.code='UPLOADS_COUNTRY_RECHECK';error.country=pending.value.handoff.country;throw error;
    }
    return fromChannelWire(pending.value.records[0].data);
  };
  const store={
    loadSettings:async()=>frozen.settings,
    restore:()=>executionStore.withLease(nodeId,request,client=>local(client).restore(job)),
    beginAdmission:()=>executionStore.withLease(nodeId,request,async(client,ownership)=>{
      if(ownership.evidence.admission_started_at)return;
      await local(client).beginAdmission(job);
      await client.query(`UPDATE remote_ingestion.full_crawl_executions SET admission_started_at=clock_timestamp()
        WHERE task_id=$1 AND generation=$2`,[request.task_id,request.generation]);
    }),
    claimDetailExecution:fence=>executionStore.withLease(nodeId,request,async client=>{
      const active=await client.query(`SELECT 1 FROM remote_ingestion.full_crawl_detail_reservations r JOIN remote_ingestion.full_crawl_stages s USING(stage_id)
        WHERE s.task_id=$1 AND s.generation=$2 AND r.state='started' LIMIT 1`,[request.task_id,request.generation]);
      return local(client).claimDetailExecution(fence,{recoverPending:!active.rowCount});
    }),
    async claimNextDetail(fence){
      while(records.length===0){
        if(!detailCommand||detailCommand.input.targets.at(-1).reservation_id===activeRecord?.reservation_id){
          const prior=(await executionStore.store.pool.query(`SELECT stage_id FROM remote_ingestion.full_crawl_stages
            WHERE task_id=$1 AND generation=$2 AND sequence=$3`,[request.task_id,request.generation,sequence+1])).rows[0];
          detailCommand=await details.reserve(nodeId,request,{stageId:prior?.stage_id??randomUUID(),sequence:sequence+1,detailFence:fence,collection});
          if(!detailCommand)return null;
          sequence++;batchSequence=1;validateFullCrawlCommand(detailCommand,frozen.execution);
        }
        batch=await wait(detailCommand,batchSequence++);
        const applied=new Set((await executionStore.store.pool.query(
          "SELECT reservation_id FROM remote_ingestion.full_crawl_detail_reservations WHERE stage_id=$1 AND state='applied'",[detailCommand.stage_id])).rows.map(row=>row.reservation_id));
        records=batch.value.records.filter(record=>!applied.has(record.reservation_id));
        if(!records.length)activeRecord=batch.value.records.at(-1);
      }
      activeRecord=records.shift();observedAt=activeRecord.observed_at;
      await details.started(nodeId,request,{stageId:detailCommand.stage_id,reservationId:activeRecord.reservation_id,startId:activeRecord.start_id});
      return executionStore.withLease(nodeId,request,async client=>{
        const row=(await client.query(`SELECT c.*,known.content_key AS known_content_key,known.content_type AS known_content_type,
          known.content_type_source AS known_content_type_source FROM crawler.content_candidates c LEFT JOIN LATERAL
          (SELECT content_key,content_type,content_type_source FROM crawler.contents
            WHERE channel_id=c.channel_id AND source_content_id=c.source_content_id ORDER BY last_seen_at DESC NULLS LAST LIMIT 1) known ON true
          WHERE c.run_id=$1 AND c.source_content_id=$2`,[frozen.execution.run_id,activeRecord.video_id])).rows[0];
        const target=detailCommand.input.targets.find(t=>t.reservation_id===activeRecord.reservation_id);
        return {...row,target:target.target,excluded_detail:target.excluded_detail};
      });
    },
    async commitDetail(fence,candidate,result){
      if(candidate.source_content_id!==activeRecord?.video_id)fail('FULL_CRAWL_TARGET_CONFLICT');
      const receipt=await details.apply(nodeId,request,{stageId:detailCommand.stage_id,reservationId:activeRecord.reservation_id,
        startId:activeRecord.start_id,payload:Buffer.from(JSON.stringify(activeRecord))},
      client=>local(client).commitDetail(fence,candidate,result),client=>stages.finishDetailBatch(client,batch.batch,batch.value));
      if(!records.length&&batch.value.outcome==='api_required')detailCommand=null;
      return receipt;
    },
    async closeFetch(fence,options){
      const targetHash=await executionStore.withLease(nodeId,request,async(_client,ownership)=>ownership.run.result_json.full_crawl.uploads.target_hash.slice(7));
      await issue('close_fetch',{},targetHash);
      return stages.apply(nodeId,request,pending.batch.batch_id,client=>local(client).closeFetch(fence,options));
    },
  };
  for(const name of ['commitAdmission','rejectAdmission','settleTerminalChannel','commitUploads'])store[name]=async(...args)=>{
    if(!pending)fail('FULL_CRAWL_BATCH_NOT_RECEIVED');
    const result=await stages.apply(nodeId,request,pending.batch.batch_id,client=>local(client)[name](...args));pending=null;return result;
  };
  for(const name of ['settleTerminalCheckpoint','settleExistingChannel'])store[name]=(...args)=>executionStore.withLease(nodeId,request,client=>local(client)[name](...args));
  const collector={
    collectAdmission:()=>issue('admission',{include_about:true}),
    collectUploads:async(_channel,limit,options)=>{
      const pendingCountry=await executionStore.withLease(nodeId,request,(client,owner)=>pendingFullCrawlCountryRecheck(client,owner,{egressCountry,countryRecheck}));
      if(pendingCountry)throw new UploadsCountryRecheck(pendingCountry);
      collection.channel_content_limit=limit;collection.reference_at=new Date(options.now).toISOString();
      return issue('uploads',{has_content:options.hasContent,country:options.country,network:{egress_country:egressCountry,country_recheck:countryRecheck}});
    },
    async collectDetail(videoId){
      if(activeRecord?.video_id!==videoId)fail('FULL_CRAWL_TARGET_CONFLICT');
      if(activeRecord.outcome==='error')throw fullCrawlResultError(activeRecord.error);
      if(activeRecord.outcome==='api_required'){
        // Only v3 can emit this evidence. The original center fallback decides
        // eligibility/budget and creates the stable API request, never the node.
        const error=fullCrawlResultError(activeRecord.error);error.partial_detail=activeRecord.data;
        throw error;
      }
      if(activeRecord.outcome!=='captured')fail('FULL_CRAWL_DETAIL_OBSERVATION_MISSING');
      return fromChannelWire(activeRecord.data);
    },
  };
  return runWithChannelExecution({...currentChannelExecution(),attempt_id:frozen.execution.execution_attempt_id,abort_signal:signal},
    ()=>createFullCrawlYoutubeJsExecutor({store,collector,videoApiFallback,handoff,locale,
      clock:()=>new Date(observedAt??Date.now())})(job,{resumeMode}));
}
