import {setTimeout as delay} from 'node:timers/promises';
import {randomUUID} from 'node:crypto';
import {join} from 'node:path';
import {readdir,rm,open} from 'node:fs/promises';
import {Journal} from './wholeChannelJournal.js';
import {uuid,hash,RemoteProtocolError} from './protocol.js';
import {fullCrawlResultParts,validateFullCrawlCommand} from './fullCrawlMessages.js';
import {channelSnapshotWire,toChannelWire} from './channelWire.js';
import {classifyFullCrawlTargetBeforeDetail,validateFullCrawlYoutubeJsDetail} from '../fullCrawlYoutubeJsModel.js';
import {withUploadsCountryExecution} from '../youtubeUploadsCountry.js';
import {selectYoutubeFailure} from '../youtubeFailurePolicy.js';

const networkKinds=new Set(['proxy_transport','youtube_rate_limited','youtube_challenge','upstream_transient','token_or_client']);
export function fullCrawlErrorEvidence(error,depth=0){
  const wire=toChannelWire(error);delete wire.cause;
  return {name:String(error.name||'Error').slice(0,100),code:error.code?String(error.code).slice(0,100):null,
    message:String(error.message||'Collection failed').slice(0,2000),
    status:Number.isInteger(error.status)&&error.status>=100&&error.status<=599?error.status:null,
    surface:error.required_surface?String(error.required_surface).slice(0,100):null,
    details:{wire},
    ...(error.cause instanceof Error&&depth<4?{cause:fullCrawlErrorEvidence(error.cause,depth+1)}:{})};
}
const result=(command,sequence,records,outcome='success',error=null,handoff=null)=>({version:1,
  task_id:command.task_id,generation:command.generation,stage_id:command.stage_id,sequence,
  input_hash:command.input_hash,target_hash:command.target_hash,outcome,records,error,handoff});

// Every stage owns an immutable journal. One record per batch bounds memory and
// makes each completed video independently replayable after a process crash.
export class FullCrawlNodeJournal {
  constructor({spool,client}){Object.assign(this,{spool,client});}
  async journal(stageId){
    const path=join(this.spool.directory,'full',uuid(stageId));
    const journal=await new Journal(path,this.spool.maxBytes).init();
    // Persist creation of both directory levels before any network work.
    for(const path of [join(this.spool.directory,'full'),this.spool.directory]){
      const fd=await open(path,'r');try{await fd.sync();}finally{await fd.close();}
    }
    const put=journal.put.bind(journal);
    journal.put=(key,value)=>this.spool.exclusive(async()=>{
      if(!journal.get(key)&&(await this.spool.usageUnlocked())+Buffer.byteLength(JSON.stringify(value))+key.length+256>this.spool.maxBytes)throw Error('SPOOL_FULL');
      return put(key,value);
    });
    return journal;
  }
  async upload(journal,number,signal){
    const delivery=journal.get('delivery'),batch=journal.get(`batch:${number}`);
    if(!batch||journal.get(`receipt:${number}`))return;
    const parts=fullCrawlResultParts(batch.value,delivery.command,delivery.execution,batch.batchId);
    let receipt;
    for(const part of parts){
      for(;;){
        signal?.throwIfAborted();
        let onAbort;
        const cancelled=signal?new Promise((_,reject)=>{onAbort=()=>reject(signal.reason);signal.addEventListener('abort',onAbort,{once:true});if(signal.aborted)onAbort();}):null;
        try{
          receipt=await (cancelled?Promise.race([this.client.uploadFullCrawl(delivery.request,part),cancelled]):this.client.uploadFullCrawl(delivery.request,part));break;
        }catch(error){if(!signal||signal.aborted||!(error.status>=500))throw error;await delay(250,null,{signal});}
        finally{if(onAbort)signal.removeEventListener('abort',onAbort);}
      }
      if(receipt?.batch_id!==batch.batchId||!['receiving','durable_received','applied'].includes(receipt.state))throw new RemoteProtocolError('FULL_CRAWL_INVALID_RECEIPT',502);
    }
    if(!['durable_received','applied'].includes(receipt.state))throw new RemoteProtocolError('FULL_CRAWL_INCOMPLETE_RECEIPT',503);
    await journal.put(`receipt:${number}`,receipt);
  }
  async saveBatch(journal,value,signal){
    const delivery=journal.get('delivery'),batchId=randomUUID();
    // Validate/size-check before committing a delivery that cannot be encoded.
    fullCrawlResultParts(value,delivery.command,delivery.execution,batchId);
    await journal.put(`batch:${value.sequence}`,{batchId,value});
    await this.upload(journal,value.sequence,signal);
  }
  async recover(){
    const entries=await readdir(join(this.spool.directory,'full'),{withFileTypes:true}).catch(error=>{if(error.code==='ENOENT')return [];throw error;});
    for(const entry of entries){
      if(!entry.isDirectory())throw Error('FULL_CRAWL_INVALID_JOURNAL');
      const journal=await this.journal(entry.name),delivery=journal.get('delivery');
      if(!delivery)continue;
      if(delivery.command.stage_id!==entry.name)throw Error('FULL_CRAWL_JOURNAL_IDENTITY');
      let number=1;
      for(;journal.get(`batch:${number}`);number++)await this.upload(journal,number);
      const receipt=await this.client.fullCrawlReceipt?.({request:delivery.request,stageId:entry.name});
      if(receipt?.applied||(receipt&&!['pending','leased'].includes(receipt.task_state)))await this.applied(entry.name);
      // Missing observations remain uncertain. Never restart network work from
      // a dead browser identity or synthesize a successful observation.
    }
  }
  async applied(stageId){
    await this.spool.exclusive(async()=>{
      await rm(join(this.spool.directory,'full',uuid(stageId)),{recursive:true,force:true});
      const fd=await open(join(this.spool.directory,'full'),'r').catch(error=>{if(error.code==='ENOENT')return null;throw error;});if(fd)try{await fd.sync();}finally{await fd.close();}
    });
  }
  async execute({request,execution,command,youtube,signal,networkStopped=false}){
    validateFullCrawlCommand(command,execution);
    const journal=await this.journal(command.stage_id);
    await journal.put('delivery',{request,execution,command});
    const c=command.input.collection;
    if(command.stage==='details'){
      for(const [index,target]of command.input.targets.entries()){
        signal.throwIfAborted();
        const number=index+1;
        if(journal.get(`batch:${number}`)){
          await this.upload(journal,number,signal);
          if(journal.get(`batch:${number}`).value.outcome!=='success')return;
          continue;
        }
        if((await this.spool.usage())+8*1024*1024+65536>this.spool.maxBytes)throw Error('SPOOL_FULL');
        let start=journal.get(`start:${number}`);
        if(!start){start={startId:randomUUID(),stageId:command.stage_id,reservationId:target.reservation_id};await journal.put(`start:${number}`,start);}
        const ack=await this.client.fullCrawlStarted({request,...start});
        if(ack?.start_id!==start.startId)throw new RemoteProtocolError('FULL_CRAWL_INVALID_START_RECEIPT',502);
        await journal.put(`started:${number}`,{start_id:ack.start_id});
        if(target.recovered_record){
          const reference=target.recovered_record,parts=[];
          for(let partNumber=0;partNumber<reference.part_count;partNumber++){
            const part=await this.client.fullCrawlRecovered({request,stageId:command.stage_id,reservationId:target.reservation_id,partNumber});
            const payload=Buffer.from(part.payload,'base64');
            if(part.part_number!==partNumber||part.part_count!==reference.part_count||hash(payload)!==part.part_hash||payload.length>524288)throw Error('FULL_CRAWL_RECOVERED_PART_CONFLICT');
            parts.push(payload);
          }
          const raw=Buffer.concat(parts);if(raw.length!==reference.bytes||hash(raw)!==reference.record_hash)throw Error('FULL_CRAWL_RECOVERED_PART_CONFLICT');
          const record={...JSON.parse(raw),reservation_id:target.reservation_id,start_id:start.startId};
          await this.saveBatch(journal,result(command,number,[record]),signal);continue;
        }
        let data=null,error=null,outcome='captured';
        const preflight=classifyFullCrawlTargetBeforeDetail(target.target,{contentMaxAgeDays:c.content_max_age_days,observedAt:c.reference_at});
        if(target.excluded_detail||preflight.terminalReason){outcome='preflight';data={};}
        else for(let attempt=Math.max(1,ack.attempts??1);;attempt++){
          try{
            data=toChannelWire(await youtube.fetchDetail(target.video_id,{signal,strictRequiredSurfaces:true,optionalComments:c.optional_comments,detailMode:'full',requireContentType:true}));
            validateFullCrawlYoutubeJsDetail(target.video_id,data,{optionalComments:c.optional_comments});break;
          }catch(cause){
            signal.throwIfAborted();
            const parser=['YoutubeJsRequiredSurfaceError','ParserContractError'].includes(cause.name);
            const network=networkKinds.has(selectYoutubeFailure({error:cause}).decision.kind);
            if(execution.fetch_contract.executor_version===3&&parser&&!network&&attempt<3)continue;
            error=fullCrawlErrorEvidence(cause);
            outcome=execution.fetch_contract.executor_version===3?'api_required':'error';
            data=outcome==='api_required'?toChannelWire(cause.partial_detail??data??{}):null;break;
          }
        }
        const record={reservation_id:target.reservation_id,video_id:target.video_id,start_id:start.startId,
          observed_at:new Date().toISOString(),outcome,data,error};
        let body=result(command,number,[record],['error','api_required'].includes(outcome)?outcome:'success');
        try{fullCrawlResultParts(body,command,execution,randomUUID());}
        catch(cause){if(cause.code!=='FULL_CRAWL_BATCH_SIZE')throw cause;
          body=result(command,number,[{...record,outcome:'error',data:null,error:fullCrawlErrorEvidence(cause)}],'error');}
        await this.saveBatch(journal,body,signal);
        if(body.outcome!=='success')return;
      }
      return;
    }
    if(journal.get('batch:1')){await this.upload(journal,1,signal);return;}
    let body;
    try{
      signal.throwIfAborted();let data;
      if(command.stage==='admission')data=channelSnapshotWire(await youtube.openChannel(c.channel_id,{includeAbout:true,signal}));
      else if(command.stage==='uploads')data=toChannelWire(await withUploadsCountryExecution({egressCountry:command.input.network.egress_country,recheck:command.input.network.country_recheck},
        ()=>youtube.fetchUploads(c.channel_id,c.channel_content_limit,{hasContent:command.input.has_content,country:command.input.country,locale:c.locale,now:Date.parse(c.reference_at),signal})));
      else {if(!networkStopped)throw Error('FULL_CRAWL_NETWORK_NOT_QUIESCED');data={network_stopped:true,active_requests:0};}
      body=result(command,1,[{observed_at:new Date().toISOString(),data}]);
      fullCrawlResultParts(body,command,execution,randomUUID());
    }catch(error){
      signal.throwIfAborted();
      body=error.code==='UPLOADS_COUNTRY_RECHECK'?result(command,1,[],'country_recheck',null,{country:error.country})
        :result(command,1,[],'error',fullCrawlErrorEvidence(error));
    }
    await this.saveBatch(journal,body,signal);
  }
}
