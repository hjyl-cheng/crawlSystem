import {randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {FULL_CRAWL_LIMITS,fullCrawlInputHash,validateFullCrawlBatch} from './fullCrawlProtocol.js';
import {validateFullCrawlCommand,decodeFullCrawlResult} from './fullCrawlMessages.js';
import {fullCrawlConnectionIdentity} from './fullCrawlBusinessFence.js';
import {RemoteProtocolError,generation,hash,uuid} from './protocol.js';

const fail=code=>{throw new RemoteProtocolError(code);};
const same=(a,b)=>fullCrawlInputHash(a)===fullCrawlInputHash(b);

// Durable stage mailbox, independent of NATS/HTTP. P3 transports use these
// same methods; neither receipt nor decoding authorizes a business write.
export class RemoteFullCrawlStageStore {
  constructor({executionStore}){this.executions=executionStore;}
  command(row,ownership){return validateFullCrawlCommand({version:1,task_id:row.task_id,generation:row.generation,
    stage_id:row.stage_id,stage:row.stage,sequence:row.sequence,execution_hash:ownership.evidence.execution_hash,
    input_hash:row.input_hash,target_hash:row.target_hash,input:row.input},ownership.input);}

  async issue(nodeId,request,{stage,sequence,input,targetHash=null}){
    generation(sequence);input=structuredClone(input);
    return this.executions.withLease(nodeId,request,async(client,ownership)=>{
      const previous=(await client.query(`SELECT * FROM remote_ingestion.full_crawl_stages
        WHERE task_id=$1 AND generation=$2 ORDER BY sequence DESC LIMIT 1 FOR UPDATE`,[request.task_id,request.generation])).rows[0];
      if(previous?.sequence===sequence){
        if(previous.stage!==stage||previous.target_hash!==targetHash||!same(previous.input,input))fail('FULL_CRAWL_STAGE_CONFLICT');
        return this.command(previous,ownership);
      }
      if(sequence!==(previous?.sequence??0)+1||(previous&&!previous.applied_at))fail('FULL_CRAWL_STAGE_ORDER');
      if(stage==='details')fail('FULL_CRAWL_RESERVATION_REQUIRED');
      if((stage==='admission'&&ownership.binding.status!=='reserved')
        ||(stage==='uploads'&&(!ownership.run||ownership.run.result_json?.full_crawl?.uploads))||(stage==='close_fetch'&&!ownership.run?.result_json?.full_crawl?.uploads))fail('FULL_CRAWL_STAGE_ORDER');
      if(stage==='close_fetch'&&targetHash!==ownership.run.result_json.full_crawl.uploads.target_hash?.slice(7))fail('FULL_CRAWL_TARGET_CONFLICT');
      const row={stage_id:randomUUID(),task_id:request.task_id,generation:request.generation,stage,sequence,input,
        input_hash:fullCrawlInputHash(input),target_hash:targetHash};
      const command=this.command(row,ownership);
      await client.query(`INSERT INTO remote_ingestion.full_crawl_stages
        (stage_id,task_id,generation,stage,sequence,input,input_hash,target_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
      [row.stage_id,row.task_id,row.generation,stage,sequence,input,row.input_hash,targetHash]);
      return command;
    });
  }

  async lockedStage(client,ownership,stageId){
    const row=(await client.query('SELECT * FROM remote_ingestion.full_crawl_stages WHERE stage_id=$1 FOR UPDATE',[stageId])).rows[0];
    if(!row||row.task_id!==ownership.task.task_id||row.generation!==ownership.task.generation)fail('FULL_CRAWL_STAGE_CONFLICT');
    return {row,command:this.command(row,ownership)};
  }
  async payload(client,batch,command,execution){
    const parts=(await client.query('SELECT payload FROM remote_ingestion.full_crawl_result_parts WHERE batch_id=$1 ORDER BY part_number',[batch.batch_id])).rows;
    const bytes=Buffer.concat(parts.map(p=>p.payload));
    if(parts.length!==batch.part_count||bytes.length!==batch.payload_bytes||hash(bytes)!==batch.payload_hash)fail('FULL_CRAWL_PAYLOAD_HASH_CONFLICT');
    const value=decodeFullCrawlResult(bytes,command,execution);
    if(value.sequence!==batch.sequence)fail('FULL_CRAWL_BATCH_IDENTITY_CONFLICT');
    return value;
  }

  // Exact applied ACKs remain readable after business settlement. This
  // grants no new write and still fences the instance and task generation.
  async appliedPartReceipt(nodeId,request,manifest,number,partHash){
    return this.executions.store.transaction(async client=>{
      const row=(await client.query(`SELECT b.*,s.task_id,s.generation,s.input_hash,s.target_hash,p.part_hash,
        t.input,t.context,t.node_id,t.worker_slot,t.generation AS current_generation,
        e.execution_hash,e.connection_identity
        FROM remote_ingestion.full_crawl_result_batches b JOIN remote_ingestion.full_crawl_stages s USING(stage_id)
        JOIN remote_ingestion.full_crawl_result_parts p USING(batch_id)
        JOIN remote_ingestion.tasks t ON t.task_id=s.task_id
        JOIN remote_ingestion.full_crawl_executions e ON e.task_id=s.task_id AND e.generation=s.generation
        WHERE b.batch_id=$1 AND b.state='applied' AND p.part_number=$2`,[manifest.batch_id,number])).rows[0];
      if(!row)return null;
      const connection=await this.executions.lockConnection(client,nodeId,request.connection);
      if(row.task_id!==request.task_id||row.generation!==request.generation||row.current_generation!==request.generation
        ||row.node_id!==nodeId||row.worker_slot!==connection.slot||!same(row.connection_identity,fullCrawlConnectionIdentity(connection))
        ||row.execution_hash!==fullCrawlInputHash(row.input)||row.context.execution_hash!==row.execution_hash)fail('STALE_LEASE');
      if(manifest.version!==1||['task_id','generation','stage_id','sequence','input_hash','target_hash','payload_hash','payload_bytes','part_count'].some(k=>row[k]!==manifest[k])
        ||row.part_hash!==partHash)fail('FULL_CRAWL_BATCH_CONFLICT');
      return {state:'applied',batch_id:row.batch_id};
    });
  }

  async receivePart(nodeId,request,part,{evidenceOnly=false}={}){
    const manifest=structuredClone(part.manifest),number=part.part_number;
    const keys=['version','task_id','generation','stage_id','batch_id','sequence','input_hash','target_hash','payload_hash','payload_bytes','part_count'];
    if(!manifest||Object.keys(manifest).length!==keys.length||keys.some(key=>!Object.hasOwn(manifest,key)))fail('FULL_CRAWL_INVALID_BATCH');
    if(!Buffer.isBuffer(part.payload)||part.payload.length>FULL_CRAWL_LIMITS.partBytes)fail('FULL_CRAWL_PART_SIZE');
    const bytes=Buffer.from(part.payload);
    if(!Number.isSafeInteger(number)||number<0||number>=manifest.part_count||hash(bytes)!==part.part_hash)fail('FULL_CRAWL_PART_HASH_CONFLICT');
    uuid(manifest.batch_id);uuid(request.task_id);generation(request.generation);
    const receipt=evidenceOnly?null:await this.appliedPartReceipt(nodeId,request,manifest,number,part.part_hash);
    if(receipt)return receipt;
    return this.executions[evidenceOnly?'withEvidence':'withLease'](nodeId,request,async(client,ownership)=>{
      const {row:stage,command}=await this.lockedStage(client,ownership,manifest.stage_id);
      validateFullCrawlBatch(manifest,command);
      const expected=number===manifest.part_count-1?manifest.payload_bytes-FULL_CRAWL_LIMITS.partBytes*(manifest.part_count-1):FULL_CRAWL_LIMITS.partBytes;
      if(bytes.length!==expected)fail('FULL_CRAWL_PART_SIZE');
      let batch=(await client.query('SELECT * FROM remote_ingestion.full_crawl_result_batches WHERE batch_id=$1 FOR UPDATE',[manifest.batch_id])).rows[0];
      if(batch){
        if(batch.state==='conflict')fail('FULL_CRAWL_BATCH_CONFLICT');
        if(['stage_id','sequence','payload_hash','payload_bytes','part_count'].some(k=>batch[k]!==manifest[k]))fail('FULL_CRAWL_BATCH_CONFLICT');
      }else{
        if(stage.applied_at)fail('FULL_CRAWL_STAGE_ALREADY_APPLIED');
        const previous=(await client.query('SELECT sequence,state FROM remote_ingestion.full_crawl_result_batches WHERE stage_id=$1 ORDER BY sequence DESC LIMIT 1',[command.stage_id])).rows[0];
        if(manifest.sequence!==(previous?.sequence??0)+1||(previous&&!['received','applied'].includes(previous.state))
          ||(command.stage!=='details'&&manifest.sequence!==1))fail('FULL_CRAWL_BATCH_ORDER');
        batch=(await client.query(`INSERT INTO remote_ingestion.full_crawl_result_batches
          (batch_id,stage_id,sequence,payload_hash,payload_bytes,part_count) VALUES($1,$2,$3,$4,$5,$6) RETURNING *`,
        [manifest.batch_id,command.stage_id,manifest.sequence,manifest.payload_hash,manifest.payload_bytes,manifest.part_count])).rows[0];
      }
      const old=(await client.query('SELECT part_hash FROM remote_ingestion.full_crawl_result_parts WHERE batch_id=$1 AND part_number=$2',[manifest.batch_id,number])).rows[0];
      if(old&&old.part_hash!==part.part_hash)fail('FULL_CRAWL_PART_HASH_CONFLICT');
      if(!old)await client.query(`INSERT INTO remote_ingestion.full_crawl_result_parts
        (batch_id,part_number,part_count,payload_bytes,part_hash,payload) VALUES($1,$2,$3,$4,$5,$6)`,
      [manifest.batch_id,number,manifest.part_count,manifest.payload_bytes,part.part_hash,bytes]);
      const count=Number((await client.query('SELECT count(*) AS n FROM remote_ingestion.full_crawl_result_parts WHERE batch_id=$1',[manifest.batch_id])).rows[0].n);
      if(count!==manifest.part_count)return {state:'receiving',batch_id:manifest.batch_id};
      const value=await this.payload(client,batch,command,ownership.input);
      if(command.stage==='details'){
        if(evidenceOnly)for(const record of value.records){
          const reservation=(await client.query('SELECT start_id FROM remote_ingestion.full_crawl_detail_reservations WHERE reservation_id=$1 AND stage_id=$2',[record.reservation_id,command.stage_id])).rows[0];
          if(!reservation?.start_id||reservation.start_id!==record.start_id)fail('FULL_CRAWL_START_CONFLICT');
        }
        const preceding=(await client.query(`SELECT * FROM remote_ingestion.full_crawl_result_batches
          WHERE stage_id=$1 AND sequence<$2 ORDER BY sequence`,[command.stage_id,batch.sequence])).rows;
        let offset=0;
        for(const prior of preceding){const body=await this.payload(client,prior,command,ownership.input);
          if(body.outcome!=='success')fail('FULL_CRAWL_BATCH_AFTER_HANDOFF');offset+=body.records.length;}
        if(value.records.some((record,i)=>record.reservation_id!==command.input.targets[offset+i]?.reservation_id))fail('FULL_CRAWL_BATCH_ORDER');
      }
      if(batch.state==='receiving')await client.query(`UPDATE remote_ingestion.full_crawl_result_batches
        SET state='received',received_at=clock_timestamp() WHERE batch_id=$1`,[batch.batch_id]);
      return {state:batch.state==='applied'?'applied':'durable_received',batch_id:batch.batch_id};
    });
  }

  async read(nodeId,request,stageId,sequence){
    uuid(stageId);generation(sequence);
    return this.executions.withLease(nodeId,request,async(client,ownership)=>{
      const {command}=await this.lockedStage(client,ownership,stageId);
      const batch=(await client.query(`SELECT * FROM remote_ingestion.full_crawl_result_batches
        WHERE stage_id=$1 AND sequence=$2 AND state IN ('received','applied')`,[stageId,sequence])).rows[0];
      return batch?{batch,command,value:await this.payload(client,batch,command,ownership.input)}:null;
    });
  }
  async wait(nodeId,request,command,{sequence=1,signal,pollMs=50}={}){
    if(!signal)throw new TypeError('bounded stage wait signal required');
    for(;;){signal.throwIfAborted();const result=await this.read(nodeId,request,command.stage_id,sequence);
      if(result)return result;await delay(pollMs,null,{signal});}
  }

  async markApplied(client,batchId,result){
    await client.query(`UPDATE remote_ingestion.full_crawl_result_batches
      SET state='applied',applied_at=clock_timestamp(),applied_result=$2 WHERE batch_id=$1`,[batchId,JSON.parse(JSON.stringify(result??null))]);
  }
  async apply(nodeId,request,batchId,action){
    uuid(batchId);
    return this.executions.withLease(nodeId,request,async(client,ownership)=>{
      const batch=(await client.query('SELECT * FROM remote_ingestion.full_crawl_result_batches WHERE batch_id=$1 FOR UPDATE',[batchId])).rows[0];
      if(!batch||!['received','applied'].includes(batch.state))fail('FULL_CRAWL_BATCH_NOT_RECEIVED');
      const {command}=await this.lockedStage(client,ownership,batch.stage_id);
      if(command.stage==='details')fail('FULL_CRAWL_DETAIL_APPLICATION_REQUIRED');
      if(batch.state==='applied')return batch.applied_result;
      const value=await this.payload(client,batch,command,ownership.input);
      const result=await action(client,value,ownership);
      await this.markApplied(client,batchId,result);
      await client.query('UPDATE remote_ingestion.full_crawl_stages SET applied_at=clock_timestamp() WHERE stage_id=$1',[command.stage_id]);
      return result;
    });
  }

  async finishDetailBatch(client,batch,value){
    const remaining=await client.query(`SELECT 1 FROM remote_ingestion.full_crawl_detail_reservations
      WHERE reservation_id=ANY($1::uuid[]) AND state<>'applied' LIMIT 1`,[value.records.map(r=>r.reservation_id)]);
    if(!remaining.rowCount){
      await this.markApplied(client,batch.batch_id,{applied:value.records.length});
      if(value.outcome==='api_required'){
        // Synchronous center API replay has applied the started prefix. The
        // node stopped; remaining unstarted targets can be reserved again.
        await client.query("UPDATE remote_ingestion.full_crawl_detail_reservations SET state='cancelled' WHERE stage_id=$1 AND state='reserved'",[batch.stage_id]);
        await client.query(`UPDATE remote_ingestion.full_crawl_stages SET applied_at=clock_timestamp()
          WHERE stage_id=$1 AND NOT EXISTS(SELECT 1 FROM remote_ingestion.full_crawl_detail_reservations
            WHERE stage_id=$1 AND state NOT IN ('applied','cancelled'))`,[batch.stage_id]);
      }
    }
  }
}
