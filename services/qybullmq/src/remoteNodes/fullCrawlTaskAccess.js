import {RemoteProtocolError,uuid,generation} from './protocol.js';
import {assertRemoteFullCrawlTaskFence} from './fullCrawlBusinessFence.js';
import {FULL_CRAWL_WORKLOAD} from './collectingWorkload.js';

// Adapter for shared route/session storage. No incremental Plan identifiers.
export function fullCrawlTaskAccess(executions){
  const fail=code=>{throw new RemoteProtocolError(code);};
  const connectionFor=async(client,taskId,nodeId)=>{
    const row=(await client.query(`SELECT e.connection_identity FROM remote_ingestion.full_crawl_executions e
      JOIN remote_ingestion.tasks t USING(task_id) WHERE e.task_id=$1 AND e.generation=t.generation AND e.node_id=$2`,[taskId,nodeId])).rows[0];
    if(!row)fail('STALE_LEASE');
    return executions.lockConnection(client,nodeId,{...row.connection_identity,version:1,mode:FULL_CRAWL_WORKLOAD.mode,accepting:true});
  };
  return {store:executions.store,
    async lock(client,request){
      uuid(request.task_id);uuid(request.node_id);generation(request.generation);
      await connectionFor(client,request.task_id,request.node_id);
      const task=(await client.query('SELECT *,lease_until>clock_timestamp() AS alive FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE',[request.task_id])).rows[0];
      if(!task||task.state!=='leased'||!task.alive||task.node_id!==request.node_id||task.generation!==request.generation)fail('STALE_LEASE');
      return task;
    },
    async assertBusinessFence(client,task){
      const connection=(await client.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2',[task.node_id,task.worker_slot])).rows[0];
      if(!connection)fail('WORKER_CONNECTION_STALE');
      const owner=await assertRemoteFullCrawlTaskFence(client,task,connection);
      return {...owner,executionAttemptId:owner.attempt.attempt_id,rotaTask:owner.rotaFence};
    },
  };
}
