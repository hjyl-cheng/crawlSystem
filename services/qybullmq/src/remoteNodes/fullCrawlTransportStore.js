import {fullCrawlRecoveredPart} from './fullCrawlReplay.js';
import {RemoteFullCrawlStageStore} from './fullCrawlStageStore.js';
import {RemoteFullCrawlDetailStore} from './fullCrawlDetailStore.js';
import {RemoteProtocolError,uuid,generation} from './protocol.js';
import {assertRemoteFullCrawlTaskFence} from './fullCrawlBusinessFence.js';
import {validateFullCrawlCommand} from './fullCrawlMessages.js';

const fail=code=>{throw new RemoteProtocolError(code);};
// Authenticated transport methods; collection decisions stay in the coordinator.
export class RemoteFullCrawlTransportStore {
  constructor({executions,routes=null,youtubeSessions=null}){
    Object.assign(this,{executions,store:executions.store,routes,youtubeSessions});
    this.stages=new RemoteFullCrawlStageStore({executionStore:executions});
    this.details=new RemoteFullCrawlDetailStore({executionStore:executions});
  }
  receipt(nodeId,{request,stageId}){
    uuid(stageId);
    return this.executions.withEvidence(nodeId,request,async(client,owner)=>{
      const stage=(await client.query('SELECT applied_at FROM remote_ingestion.full_crawl_stages WHERE stage_id=$1 AND task_id=$2 AND generation=$3',[stageId,request.task_id,request.generation])).rows[0];
      if(!stage)fail('FULL_CRAWL_STAGE_CONFLICT');
      return {task_state:owner.task.state,applied:Boolean(stage.applied_at)};
    });
  }
  heartbeat(nodeId,request){return this.executions.renew(nodeId,request);}
  async poll(nodeId,request){
    uuid(request.task_id);generation(request.generation);
    return this.store.transaction(async client=>{
      const connection=await this.executions.lockConnection(client,nodeId,request.connection);
      const task=(await client.query('SELECT *,lease_until>clock_timestamp() AS alive FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE',[request.task_id])).rows[0];
      if(!task||task.node_id!==nodeId||task.worker_slot!==connection.slot||task.generation!==request.generation)fail('STALE_LEASE');
      const binding=(await client.query('SELECT stop_requested,state FROM remote_ingestion.network_bindings WHERE task_id=$1 AND generation=$2',[task.task_id,task.generation])).rows[0];
      if(task.state!=='leased'||!task.alive||binding?.stop_requested)return {status:'closed',commands:[]};
      const rows=(await client.query(`SELECT s.*,e.execution_hash,e.execution_input FROM remote_ingestion.full_crawl_stages s
        JOIN remote_ingestion.full_crawl_executions e USING(task_id,generation)
        WHERE s.task_id=$1 AND s.generation=$2 ORDER BY sequence`,[task.task_id,task.generation])).rows;
      // Applied stages are returned as receipts, including terminal admission.
      const pending=rows.find(row=>!row.applied_at);
      if(pending)await assertRemoteFullCrawlTaskFence(client,task,connection);
      return {status:'leased',commands:pending?[validateFullCrawlCommand({version:1,task_id:task.task_id,generation:task.generation,
        stage_id:pending.stage_id,stage:pending.stage,sequence:pending.sequence,execution_hash:pending.execution_hash,
        input_hash:pending.input_hash,target_hash:pending.target_hash,input:pending.input},pending.execution_input)]:[],
        applied:rows.filter(row=>row.applied_at).map(row=>row.stage_id)};
    });
  }
  recovered(nodeId,{request,...value}){return this.executions.withLease(nodeId,request,(client,owner)=>fullCrawlRecoveredPart(client,owner,value));}
  started(nodeId,{request,...value}){return this.details.started(nodeId,request,value);}
  async receive(nodeId,taskId,bytes){
    if(bytes.length>800000)fail('FULL_CRAWL_PART_SIZE');
    let value;try{value=JSON.parse(bytes.toString('utf8'));}catch{fail('FULL_CRAWL_RESULT_JSON');}
    const {request,manifest,part_number,part_hash,payload}=value;
    if(request?.task_id!==taskId||typeof payload!=='string')fail('FULL_CRAWL_BATCH_IDENTITY_CONFLICT');
    const binary=Buffer.from(payload,'base64');if(binary.toString('base64')!==payload)fail('FULL_CRAWL_RESULT_JSON');
    return this.stages.receivePart(nodeId,request,{manifest,part_number,part_hash,payload:binary},{evidenceOnly:true});
  }
}
