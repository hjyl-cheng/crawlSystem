import { FULL_CRAWL_WORKLOAD } from './collectingWorkload.js';
import { assertRemoteFullCrawlBusinessFence, assertRemoteFullCrawlTaskFence, fullCrawlConnectionIdentity } from './fullCrawlBusinessFence.js';
import { fullCrawlInputHash, validateFullCrawlExecution } from './fullCrawlProtocol.js';
import { RemoteProtocolError, generation, uuid } from './protocol.js';
import { RemoteWorkerActivationStore } from './workerActivationStore.js';

const fail = code => { throw new RemoteProtocolError(code); };

// Center-only durable ownership. The processor supplies original business
// records and Rota attempt; this module never invents a replacement attempt.
export class RemoteFullCrawlExecutionStore {
  constructor({store,verifyExecution}) {
    if (typeof verifyExecution!=='function') throw new TypeError('full-crawl supervisor verifier required');
    this.store=store;
    this.activation=new RemoteWorkerActivationStore({store,fullCrawlExecution:{
      verifyExecution,assertTask:assertRemoteFullCrawlTaskFence,
    }});
    this.verifyExecution=verifyExecution;
  }

  async lockConnection(client,nodeId,connection,{admission=false}={}) {
    const workload=this.activation.identity(nodeId,connection);
    if (workload!==FULL_CRAWL_WORKLOAD) fail('WORKER_NODE_CAPABILITY_MISMATCH');
    const node=(await client.query('SELECT * FROM remote_ingestion.nodes WHERE node_id=$1 FOR SHARE',[nodeId])).rows[0];
    if (!node || node.state==='disabled') fail('UNAUTHORIZED');
    this.activation.assertCapability(node,workload);
    const row=(await client.query(`SELECT *,connected_until>clock_timestamp() AS alive
      FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2 FOR UPDATE`,[nodeId,connection.slot])).rows[0];
    if (!this.activation.matches(row,connection) || !row.alive
      || fullCrawlInputHash(fullCrawlConnectionIdentity(row))!==fullCrawlInputHash(fullCrawlConnectionIdentity(connection))) {
      fail('WORKER_CONNECTION_STALE');
    }
    if (admission && (node.state!=='active' || !row.enabled || !row.accepting || !row.activation_requested
      || await this.verifyExecution(client,row)!==true)) fail('WORKER_NOT_READY');
    return row;
  }

  async prepare({execution,connection}) {
    validateFullCrawlExecution(execution);
    // Copy before the first await: callers cannot change the frozen task input
    // while admission is waiting for a database lock.
    const input=structuredClone(execution); connection=structuredClone(connection);
    const executionHash=fullCrawlInputHash(input);
    return this.store.transaction(async client=>{
      const row=await this.lockConnection(client,connection.node_id,connection,{admission:true});
      const taskId=await this.store.enqueue({workKey:`full-crawl:${input.execution_attempt_id}`,
        capability:FULL_CRAWL_WORKLOAD.capability,scopeKey:`channel:${input.channel_id}`,input,
        context:{execution_hash:executionHash,execution_attempt_id:input.execution_attempt_id}},{client});
      let task=(await client.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE',[taskId])).rows[0];
      const existing=(await client.query('SELECT 1 FROM remote_ingestion.full_crawl_executions WHERE task_id=$1 LIMIT 1',[taskId])).rowCount;
      if (existing) {
        await assertRemoteFullCrawlTaskFence(client,task,row);
        return {taskId,generation:task.state==='pending'?task.generation+1:task.generation,executionHash};
      }
      if (task.state!=='pending' || task.generation!==0 || task.target_node_id) fail('FULL_CRAWL_EXECUTION_CONFLICT');
      const ownership=await assertRemoteFullCrawlBusinessFence(client,input);
      await client.query(`UPDATE remote_ingestion.tasks SET target_node_id=$2,target_worker_slot=$3
        WHERE task_id=$1`,[taskId,row.node_id,row.slot]);
      await client.query(`INSERT INTO remote_ingestion.full_crawl_executions
        (task_id,generation,node_id,worker_slot,instance_id,protocol_version,execution_input,execution_hash,connection_identity,rota_fence)
        VALUES($1,1,$2,$3,$4,1,$5,$6,$7,$8)`,
      [taskId,row.node_id,row.slot,row.instance_id,input,executionHash,fullCrawlConnectionIdentity(row),ownership.rotaFence]);
      task=(await client.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1',[taskId])).rows[0];
      await assertRemoteFullCrawlTaskFence(client,task,row);
      return {taskId,generation:1,executionHash};
    });
  }

  // All stage mutations use this transaction, including their applied marker.
  // Draining stops admission but permits the exact current owner to settle.
  async withLease(nodeId,request,action) {
    uuid(request.task_id); generation(request.generation);
    const value=structuredClone(request);
    return this.store.transaction(async client=>{
      const connection=await this.lockConnection(client,nodeId,value.connection);
      const task=(await client.query(`SELECT *,lease_until>clock_timestamp() AS alive
        FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE`,[value.task_id])).rows[0];
      if (!task || task.state!=='leased' || task.generation!==value.generation || !task.alive) fail('STALE_LEASE');
      const ownership=await assertRemoteFullCrawlTaskFence(client,task,connection);
      const result=await action(client,{task,connection,...ownership});
      const alive=(await client.query('SELECT lease_until>clock_timestamp() AS alive FROM remote_ingestion.tasks WHERE task_id=$1',
        [task.task_id])).rows[0]?.alive;
      if (!alive) fail('STALE_LEASE');
      return result;
    });
  }

  // Evidence replay has no authority to renew, start I/O, or apply business data.
  // Match the frozen original connection even after a process/lease expires.
  async withEvidence(nodeId,request,action) {
    uuid(nodeId);uuid(request.task_id);generation(request.generation);
    const value=structuredClone(request);
    return this.store.transaction(async client=>{
      const node=(await client.query('SELECT state FROM remote_ingestion.nodes WHERE node_id=$1 FOR SHARE',[nodeId])).rows[0];
      if(!node||node.state==='disabled')fail('UNAUTHORIZED');
      const task=(await client.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE',[value.task_id])).rows[0];
      const evidence=(await client.query('SELECT * FROM remote_ingestion.full_crawl_executions WHERE task_id=$1 AND generation=$2',[value.task_id,value.generation])).rows[0];
      if(!task||!evidence||task.node_id!==nodeId||task.generation!==value.generation
        ||evidence.node_id!==nodeId||task.worker_slot!==evidence.worker_slot
        ||fullCrawlInputHash(evidence.connection_identity)!==fullCrawlInputHash(fullCrawlConnectionIdentity(value.connection??{}))
        ||evidence.execution_hash!==fullCrawlInputHash(task.input)||task.context.execution_hash!==evidence.execution_hash
        ||fullCrawlInputHash(evidence.execution_input)!==evidence.execution_hash)fail('STALE_LEASE');
      validateFullCrawlExecution(evidence.execution_input);
      return action(client,{task,evidence,input:evidence.execution_input});
    });
  }

  async renew(nodeId,request) {
    return this.withLease(nodeId,request,async(client,{task})=>{
      const row=(await client.query(`UPDATE remote_ingestion.tasks
        SET lease_until=clock_timestamp()+($2 * interval '1 second')
        WHERE task_id=$1 AND lease_until>clock_timestamp() RETURNING lease_until`,
      [task.task_id,this.store.leaseSeconds])).rows[0];
      if (!row) fail('STALE_LEASE');
      return row;
    });
  }
}
