import { RemoteProtocolError, uuid } from './protocol.js';
import { collectingWorkload, collectingSlotValid, FULL_CRAWL_WORKLOAD } from './collectingWorkload.js';

export const REMOTE_RUNTIME_REVISION = 'youtubejs-incremental-v1';
export const WHOLE_CHANNEL_RUNTIME_REVISION = 'youtubejs-incremental-whole-v1';
const fail = code => { throw new RemoteProtocolError(code); };
const slotValid = value => typeof value === 'string' && /^[a-z0-9-]{1,60}$/.test(value);
const hashValid = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);

// Center-only registration/activation; the public gateway exposes heartbeat and
// gated claims, never these administrative methods. Activation also requires a
// live central execution supervisor, supplied as a transactional readiness check.
export class RemoteWorkerActivationStore {
  constructor({ store, verifyExecution = null, fullCrawlExecution = null, ttlSeconds = 45 }) {
    if (!Number.isInteger(ttlSeconds) || ttlSeconds < 15 || ttlSeconds > 120) throw new TypeError('invalid connection TTL');
    if (fullCrawlExecution !== null && (typeof fullCrawlExecution?.verifyExecution !== 'function'
      || typeof fullCrawlExecution?.assertTask !== 'function')) throw new TypeError('full-crawl readiness and task fence required');
    Object.assign(this, { store, verifyExecution, fullCrawlExecution, ttlSeconds });
  }

  async register({ nodeId, slot, deploymentId, configHash, role = 'incremental', mode = 'incremental_collect' }) {
    uuid(nodeId);uuid(deploymentId);
    const workload = collectingWorkload(mode);
    if (!workload || role !== workload.role || !collectingSlotValid(workload,slot) || !hashValid(configHash)) throw new TypeError('invalid collecting deployment');
    return this.store.transaction(async client => {
      const node = (await client.query('SELECT state,capabilities FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE',[nodeId])).rows[0];
      if (node?.state !== 'active') fail('REMOTE_NODE_NOT_ACTIVE');
      this.assertCapability(node,workload);
      if ((await client.query(
        'SELECT 1 FROM remote_ingestion.worker_connections WHERE node_id=$1 AND role<>$2 LIMIT 1',[nodeId,role])).rowCount) fail('WORKER_NODE_WORKLOAD_CONFLICT');
      await client.query(`INSERT INTO remote_ingestion.worker_connections(node_id,slot,deployment_id,config_hash,role,mode,activation_requested)
        VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(node_id,slot) DO NOTHING`,[nodeId,slot,deploymentId,configHash,role,mode,role==='incremental']);
      const row=(await client.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2 FOR UPDATE',[nodeId,slot])).rows[0];
      if(row.retirement_id || row.retired_at || row.deployment_id!==deploymentId || row.config_hash!==configHash || row.mode!==mode || row.role!==role)fail('WORKER_DEPLOYMENT_CONFLICT');
      await client.query('UPDATE remote_ingestion.nodes SET slot_claims_required=true WHERE node_id=$1',[nodeId]);
      return {node_id:nodeId,slot,deployment_id:deploymentId,config_hash:configHash,role,mode};
    });
  }

  identity(nodeId,value) {
    const fields=['version','mode','node_id','slot','deployment_id','config_hash','instance_id','relay_boot_id','runtime_revision','accepting'];
    const workload=collectingWorkload(value?.mode);
    if(!value || !workload || Object.keys(value).some(key=>!fields.includes(key)) || value.version!==1
      || value.node_id!==nodeId || !collectingSlotValid(workload,value.slot) || !hashValid(value.config_hash)
      || !/^[a-f0-9]{48}$/.test(value.relay_boot_id) || !workload.revisions.includes(value.runtime_revision)
      || typeof value.accepting!=='boolean')throw new RemoteProtocolError('INVALID_WORKER_CONNECTION',400);
    uuid(value.node_id);uuid(value.deployment_id);uuid(value.instance_id);
    return workload;
  }

  assertCapability(node,workload) {
    const capabilities=node?.capabilities ?? [];
    if (workload.role==='fullcrawl'
      ? capabilities.length!==1 || capabilities[0]!==workload.capability
      : capabilities.includes(FULL_CRAWL_WORKLOAD.capability)) fail('WORKER_NODE_CAPABILITY_MISMATCH');
  }

  verifier(mode) {
    return mode===FULL_CRAWL_WORKLOAD.mode
      ? this.fullCrawlExecution?.verifyExecution?.bind(this.fullCrawlExecution) : this.verifyExecution;
  }

  matches(row,value) {
    return row && !row.retirement_id && !row.retired_at && row.role===collectingWorkload(value.mode)?.role && row.mode===value.mode && row.deployment_id===value.deployment_id && row.config_hash===value.config_hash;
  }

  async heartbeat(nodeId,value) {
    const workload=this.identity(nodeId,value);
    return this.store.transaction(async client=>{
      const node=(await client.query('SELECT state,capabilities FROM remote_ingestion.nodes WHERE node_id=$1 FOR SHARE',[nodeId])).rows[0];
      if(!node || node.state==='disabled')throw new RemoteProtocolError('UNAUTHORIZED',401);
      this.assertCapability(node,workload);
      const row=(await client.query(`SELECT *,connected_until>clock_timestamp() AS alive
        FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2 FOR UPDATE`,[nodeId,value.slot])).rows[0];
      if(!this.matches(row,value))fail('WORKER_DEPLOYMENT_MISMATCH');
      if(row.instance_id===value.instance_id && row.runtime_revision && row.runtime_revision!==value.runtime_revision)fail('WORKER_RUNTIME_CHANGED');
      const replacement=row.instance_id!==value.instance_id || row.relay_boot_id!==value.relay_boot_id;
      if(replacement && row.instance_id){
        if(row.alive)fail('WORKER_INSTANCE_BUSY');
        const busy=(await client.query(`SELECT 1 WHERE EXISTS(SELECT 1 FROM remote_ingestion.tasks
          WHERE node_id=$1 AND worker_slot=$2 AND state='leased') OR EXISTS(SELECT 1 FROM remote_ingestion.network_bindings
          WHERE node_id=$1 AND slot=$2 AND state<>'retired') OR ($3 AND EXISTS(SELECT 1 FROM remote_ingestion.tasks
          WHERE target_node_id=$1 AND target_worker_slot=$2 AND state IN ('pending','leased','received')))`,
        [nodeId,value.slot,workload.role==='fullcrawl'])).rowCount;
        if(busy)fail('WORKER_PREVIOUS_EXECUTION_UNSETTLED');
      }
      const seen=(await client.query(`UPDATE remote_ingestion.worker_connections SET instance_id=$3,relay_boot_id=$4,
        accepting=$5,runtime_revision=$6,enabled=CASE WHEN $7 THEN false ELSE enabled END,
        last_seen_at=clock_timestamp(),connected_until=clock_timestamp()+($8 * interval '1 second')
        WHERE node_id=$1 AND slot=$2 RETURNING *`,[nodeId,value.slot,value.instance_id,value.relay_boot_id,
        value.accepting,value.runtime_revision,replacement,this.ttlSeconds])).rows[0];
      const verify=this.verifier(seen.mode);
      const ready=seen.enabled && seen.accepting && node.state==='active'
        && typeof verify==='function' && await verify(client,seen)===true;
      return {...value,state:!seen.accepting?'draining':ready?'ready':'connected_waiting_activation',ready_for_tasks:ready,
        server_time:seen.last_seen_at.toISOString(),connected_until:seen.connected_until.toISOString()};
    });
  }

  async activate(value, { requireRequested = false } = {}) {
    const workload=this.identity(value?.node_id,value);
    const verify=this.verifier(value.mode);
    if(typeof verify!=='function')fail('CENTRAL_EXECUTION_NOT_CONFIGURED');
    return this.store.transaction(async client=>{
      const node=(await client.query('SELECT state,capabilities FROM remote_ingestion.nodes WHERE node_id=$1 FOR SHARE',[value.node_id])).rows[0];
      const row=(await client.query(`SELECT *,connected_until>clock_timestamp() AS alive
        FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2 FOR UPDATE`,[value.node_id,value.slot])).rows[0];
      if(node?.state!=='active' || !this.matches(row,value) || !row.alive || !row.accepting || (requireRequested && !row.activation_requested)
        || row.instance_id!==value.instance_id || row.relay_boot_id!==value.relay_boot_id
        || row.runtime_revision!==value.runtime_revision)fail('WORKER_NOT_READY');
      this.assertCapability(node,workload);
      // The supervisor check runs under the same node/connection locks as claim.
      // It must verify its exact slot/instance, queue processor and Rota identity;
      // a page checkbox or an image label cannot authorize channel execution.
      if(await verify(client,row)!==true)fail('CENTRAL_EXECUTION_NOT_READY');
      await client.query('UPDATE remote_ingestion.worker_connections SET enabled=true,activation_requested=CASE WHEN $3 THEN activation_requested ELSE true END,activated_at=clock_timestamp() WHERE node_id=$1 AND slot=$2',[value.node_id,value.slot,requireRequested]);
      return {enabled:true};
    });
  }

  async drain(nodeId,slot,{keepRequested=false}={}) {
    uuid(nodeId);if(!slotValid(slot))throw new TypeError('invalid slot');
    return this.store.transaction(async client=>{
      if(!keepRequested)await client.query(`UPDATE remote_ingestion.node_intake_requests
        SET selected_slots=array_remove(selected_slots,$2),revision=revision+1,updated_at=clock_timestamp()
        WHERE node_id=$1 AND $2=ANY(selected_slots)`,[nodeId,slot]);
      await client.query('SELECT node_id FROM remote_ingestion.nodes WHERE node_id=$1 FOR SHARE',[nodeId]);
      const changed=await client.query(`UPDATE remote_ingestion.worker_connections SET enabled=false,
        activation_requested=CASE WHEN $3 THEN activation_requested ELSE false END WHERE node_id=$1 AND slot=$2 RETURNING slot`,[nodeId,slot,keepRequested]);
      if(!changed.rowCount)fail('WORKER_DEPLOYMENT_MISMATCH');
      return {enabled:false};
    });
  }

  async claim(nodeId,value) {
    const workload=this.identity(nodeId,value?.connection);
    if(value.slot!==value.connection.slot)fail('WORKER_SLOT_MISMATCH');
    return this.store.claim(nodeId,uuid(value.claim_id),value.slot,{retryOnBusy:true,authorize:async (client,node)=>{
      this.assertCapability(node,workload);
      const row=(await client.query(`SELECT *,connected_until>clock_timestamp() AS alive
        FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2 FOR UPDATE`,[nodeId,value.slot])).rows[0];
      if(!this.matches(row,value.connection) || !row.alive || row.instance_id!==value.connection.instance_id
        || row.relay_boot_id!==value.connection.relay_boot_id || row.runtime_revision!==value.connection.runtime_revision)fail('WORKER_CONNECTION_STALE');
      const verify=this.verifier(row.mode);
      return {allowNew:row.enabled && row.accepting && typeof verify==='function'
        && await verify(client,row)===true,
        ...(workload.role==='fullcrawl' ? {capability:workload.capability,assertTask:async (connection,task)=>{
          if(!this.fullCrawlExecution)fail('CENTRAL_EXECUTION_NOT_CONFIGURED');
          if(task.target_node_id!==nodeId || task.target_worker_slot!==row.slot)fail('WORKER_TASK_TARGET_MISMATCH');
          await this.fullCrawlExecution.assertTask(connection,task,row);
        }} : {})};
    }});
  }
}
