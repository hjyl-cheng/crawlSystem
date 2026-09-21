import {FULL_CRAWL_LIMITS,fullCrawlInputHash,validateFullCrawlExecution,validateFullCrawlStage,validateFullCrawlBatch} from './fullCrawlProtocol.js';
import {RemoteProtocolError,hash,uuid,generation} from './protocol.js';
import {fromChannelWire} from './channelWire.js';
import {normalizeFullCrawlTargets} from '../fullCrawlYoutubeJsModel.js';

const fail=code=>{throw new RemoteProtocolError(code,400);};
const object=value=>value!==null && typeof value==='object' && !Array.isArray(value) && Object.getPrototypeOf(value)===Object.prototype;
function fields(value,keys){if(!object(value)||Object.keys(value).some(k=>!keys.includes(k))||keys.some(k=>!Object.hasOwn(value,k)))fail('FULL_CRAWL_MESSAGE_FIELDS');}
function text(value,max=500){if(typeof value!=='string'||!value.trim()||value.length>max)fail('FULL_CRAWL_MESSAGE_TEXT');}
function date(value){if(typeof value!=='string'||!Number.isFinite(Date.parse(value))||new Date(value).toISOString()!==value)fail('FULL_CRAWL_MESSAGE_TIME');}
function canonical(value,depth=0){
  if(depth>32)fail('FULL_CRAWL_JSON_DEPTH');
  if(value===null||typeof value==='string'||typeof value==='boolean'||typeof value==='number'&&Number.isFinite(value))return value;
  if(Array.isArray(value)){if(Object.keys(value).length!==value.length)fail('FULL_CRAWL_INVALID_JSON');return value.map(v=>canonical(v,depth+1));}
  if(!object(value))fail('FULL_CRAWL_INVALID_JSON');
  return Object.fromEntries(Object.keys(value).sort().map(k=>[k,canonical(value[k],depth+1)]));
}
const same=(a,b)=>fullCrawlInputHash(a)===fullCrawlInputHash(b);
const country=value=>typeof value==='string'&&/^[A-Z]{2}$/.test(value);

export function validateFullCrawlCommand(command,execution){
  validateFullCrawlStage(command);validateFullCrawlExecution(execution);
  if(command.execution_hash!==fullCrawlInputHash(execution))fail('FULL_CRAWL_EXECUTION_HASH_CONFLICT');
  const input=command.input,c=input.collection;
  fields(c,['channel_id','fetch_contract','locale','reference_at','channel_content_limit','content_max_age_days','optional_comments']);
  if(c.channel_id!==execution.channel_id||!same(c.fetch_contract,execution.fetch_contract))fail('FULL_CRAWL_COLLECTION_CONFLICT');
  text(c.locale,40);date(c.reference_at);generation(c.channel_content_limit);
  if(c.channel_content_limit>100||!Number.isSafeInteger(c.content_max_age_days)||c.content_max_age_days<0||c.content_max_age_days>3650
    ||c.optional_comments!==(execution.fetch_contract.executor_version>=2))fail('FULL_CRAWL_COLLECTION_CONFLICT');
  if(command.stage==='admission'){
    fields(input,['collection','include_about']);if(input.include_about!==true)fail('FULL_CRAWL_ABOUT_REQUIRED');
  }else if(command.stage==='uploads'){
    fields(input,['collection','has_content','country','network']);
    fields(input.network,['egress_country','country_recheck']);
    if(input.network.egress_country!==null&&!country(input.network.egress_country))fail('FULL_CRAWL_UPLOADS_INPUT');
    if(input.network.country_recheck!==null){
      fields(input.network.country_recheck,['country','status']);
      if(!country(input.network.country_recheck.country)||!['requested','checked','unavailable'].includes(input.network.country_recheck.status))fail('FULL_CRAWL_UPLOADS_INPUT');
    }
    if(typeof input.has_content!=='boolean'||(input.country!==null&&!country(input.country)))fail('FULL_CRAWL_UPLOADS_INPUT');
  }else if(command.stage==='details'){
    fields(input,['collection','detail_fence','targets']);
    if(!object(input.detail_fence))fail('FULL_CRAWL_DETAIL_FENCE_STALE');
    for(const target of input.targets){
      fields(target,['reservation_id','video_id','ordinal','target','excluded_detail',...(Object.hasOwn(target,'recovered_record')?['recovered_record']:[])]);
      if(target.recovered_record){
        const cached=target.recovered_record;fields(cached,['source_reservation_id','record_hash','bytes','part_count']);uuid(cached.source_reservation_id);
        if(!/^[a-f0-9]{64}$/.test(cached.record_hash)||!Number.isSafeInteger(cached.bytes)||cached.bytes<1||cached.bytes>FULL_CRAWL_LIMITS.batchBytes
          ||cached.part_count!==Math.ceil(cached.bytes/FULL_CRAWL_LIMITS.partBytes))fail('FULL_CRAWL_TARGET_CONFLICT');
      }
      if(!object(target.target)||normalizeFullCrawlTargets([target.target])[0].video_id!==target.video_id
        ||target.target.position!==target.ordinal||(target.excluded_detail!==null&&!object(target.excluded_detail)))fail('FULL_CRAWL_TARGET_CONFLICT');
    }
  }else fields(input,['collection']);
  return command;
}

function errorEvidence(value,depth=0){
  if(depth>4)fail('FULL_CRAWL_ERROR_EVIDENCE');
  if(!object(value))fail('FULL_CRAWL_ERROR_EVIDENCE');
  const {cause,details,...core}=value;
  fields(core,['name','code','message','status','surface']);
  text(value.name,100);text(value.message,2000);
  if(value.code!==null)text(value.code,100);
  if(value.surface!==null)text(value.surface,100);
  if(value.status!==null&&(!Number.isSafeInteger(value.status)||value.status<100||value.status>599))fail('FULL_CRAWL_ERROR_EVIDENCE');
  if(cause!==undefined&&cause!==null)errorEvidence(cause,depth+1);
  if(details!==undefined&&details!==null){if(!object(details))fail('FULL_CRAWL_ERROR_EVIDENCE');canonical(details);}
}

// Nodes return observations, never qualification/disposition/attempt decisions.
// The shared center lifecycle validates raw data and computes those decisions.
export function validateFullCrawlResult(value,command,execution){
  validateFullCrawlCommand(command,execution);
  fields(value,['version','task_id','generation','stage_id','sequence','input_hash','target_hash','outcome','records','error','handoff']);
  if(value.version!==1)fail('FULL_CRAWL_MESSAGE_VERSION');
  generation(value.sequence);
  for(const key of ['task_id','generation','stage_id','input_hash','target_hash'])if(value[key]!==command[key])fail('FULL_CRAWL_RESULT_IDENTITY');
  if(!Array.isArray(value.records)||!['success','error','country_recheck','api_required'].includes(value.outcome))fail('FULL_CRAWL_RESULT_OUTCOME');
  if(value.outcome==='country_recheck'){
    fields(value.handoff,['country']);
    if(command.stage!=='uploads'||!country(value.handoff.country)||value.records.length||value.error!==null)fail('FULL_CRAWL_COUNTRY_HANDOFF');
    return value;
  }
  if(value.handoff!==null)fail('FULL_CRAWL_RESULT_OUTCOME');
  if(command.stage==='details'){
    if(value.error!==null||!value.records.length||value.records.length>FULL_CRAWL_LIMITS.detailTargets)fail('FULL_CRAWL_RESULT_RECORDS');
    const start=command.input.targets.findIndex(t=>t.reservation_id===value.records[0]?.reservation_id);
    if(start<0)fail('FULL_CRAWL_TARGET_CONFLICT');
    const starts=new Set();
    for(const [i,record]of value.records.entries()){
      fields(record,['reservation_id','video_id','start_id','observed_at','outcome','data','error']);
      uuid(record.start_id);date(record.observed_at);
      const target=command.input.targets[start+i];
      if(!target||target.reservation_id!==record.reservation_id||target.video_id!==record.video_id||starts.has(record.start_id))fail('FULL_CRAWL_TARGET_CONFLICT');
      starts.add(record.start_id);
      if(!['captured','preflight','error','api_required'].includes(record.outcome))fail('FULL_CRAWL_RESULT_OUTCOME');
      if(record.outcome==='error'){
        errorEvidence(record.error);
        if(record.data!==null||i!==value.records.length-1||value.outcome!=='error')fail('FULL_CRAWL_RESULT_OUTCOME');
      }else{
        if(!object(record.data))fail('FULL_CRAWL_RESULT_RECORDS');
        if(record.outcome==='api_required')errorEvidence(record.error);
        else if(record.error!==null)fail('FULL_CRAWL_RESULT_RECORDS');
        if(record.outcome==='captured'&&record.data.id!==record.video_id)fail('FULL_CRAWL_TARGET_CONFLICT');
        if(record.outcome==='preflight'&&Object.keys(record.data).length)fail('FULL_CRAWL_RESULT_RECORDS');
        if(record.outcome==='api_required'&&(execution.fetch_contract.executor_version!==3
          ||i!==value.records.length-1||value.outcome!=='api_required'
          ||(record.data.id!==undefined&&record.data.id!==record.video_id)))fail('FULL_CRAWL_API_HANDOFF');
      }
    }
    const last=value.records.at(-1).outcome;
    if((value.outcome==='success'&&!['captured','preflight'].includes(last))
      ||(value.outcome==='error'&&last!=='error')||(value.outcome==='api_required'&&last!=='api_required'))fail('FULL_CRAWL_RESULT_OUTCOME');
  }else if(value.outcome==='error'){
    if(value.records.length)fail('FULL_CRAWL_RESULT_RECORDS');errorEvidence(value.error);
  }else{
    if(value.outcome!=='success'||value.error!==null||value.records.length!==1)fail('FULL_CRAWL_RESULT_RECORDS');
    const record=value.records[0];fields(record,['observed_at','data']);date(record.observed_at);
    if(!object(record.data))fail('FULL_CRAWL_RESULT_RECORDS');
    if(command.stage==='admission'&&(record.data.metadata?.channel_id!==execution.channel_id
      ||record.data.about_requested!==true||record.data.about_observed!==true))fail('FULL_CRAWL_ABOUT_REQUIRED');
    if(command.stage==='uploads'&&!Array.isArray(record.data.entries))fail('FULL_CRAWL_UPLOADS_RESULT');
    if(command.stage==='close_fetch'){
      fields(record.data,['network_stopped','active_requests']);
      if(record.data.network_stopped!==true||record.data.active_requests!==0)fail('FULL_CRAWL_NETWORK_NOT_QUIESCED');
    }
  }
  return value;
}

export function encodeFullCrawlResult(value,command,execution){
  validateFullCrawlResult(value,command,execution);
  const bytes=Buffer.from(JSON.stringify(canonical(value)));
  if(bytes.length>FULL_CRAWL_LIMITS.batchBytes)fail('FULL_CRAWL_BATCH_SIZE');
  return bytes;
}
export function decodeFullCrawlResult(bytes,command,execution){
  if(!Buffer.isBuffer(bytes)||!bytes.length||bytes.length>FULL_CRAWL_LIMITS.batchBytes)fail('FULL_CRAWL_BATCH_SIZE');
  let value;try{value=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));}catch{fail('FULL_CRAWL_RESULT_JSON');}
  canonical(value);return validateFullCrawlResult(value,command,execution);
}
export function fullCrawlResultParts(value,command,execution,batchId){
  uuid(batchId);const bytes=encodeFullCrawlResult(value,command,execution);
  const manifest={version:1,task_id:command.task_id,generation:command.generation,stage_id:command.stage_id,
    batch_id:batchId,sequence:value.sequence,input_hash:command.input_hash,target_hash:command.target_hash,
    payload_hash:hash(bytes),payload_bytes:bytes.length,part_count:Math.ceil(bytes.length/FULL_CRAWL_LIMITS.partBytes)};
  validateFullCrawlBatch(manifest,command);
  return Array.from({length:manifest.part_count},(_,part_number)=>{
    const payload=bytes.subarray(part_number*FULL_CRAWL_LIMITS.partBytes,(part_number+1)*FULL_CRAWL_LIMITS.partBytes);
    return {manifest,part_number,part_hash:hash(payload),payload};
  });
}
export function fullCrawlResultError(evidence){
  errorEvidence(evidence);const error=new Error(evidence.message);error.name=evidence.name;
  if(evidence.code!==null)error.code=evidence.code;
  if(evidence.status!==null)error.status=evidence.status;
  if(evidence.surface!==null)error.required_surface=evidence.surface;
  if(evidence.cause)error.cause=fullCrawlResultError(evidence.cause);
  if(evidence.details){error.details=structuredClone(evidence.details);
    if(evidence.details.wire){const original=fromChannelWire(evidence.details.wire);
      for(const key of Object.keys(original))if(!['name','code','status','required_surface','cause','message'].includes(key))error[key]=original[key];}}
  return error;
}
