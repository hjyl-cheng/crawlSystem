import { RemoteProtocolError, uuid } from './protocol.js';

export const REMOTE_RUNTIME_REVISION = 'youtubejs-incremental-v1';
const fail = code => { throw new RemoteProtocolError(code); };
const slotValid = value => typeof value === 'string' && /^[a-z0-9-]{1,60}$/.test(value);
const hashValid = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);

// Center-only registration/activation; the public gateway exposes heartbeat and
// gated claims, never these administrative methods. Activation also requires a
// live central execution supervisor, supplied as a transactional readiness check.
export class RemoteWorkerActivationStore {
  constructor({ store, verifyExecution = null, ttlSeconds = 45 }) {
    if (!Number.isInteger(ttlSeconds) || ttlSeconds < 15 || ttlSeconds > 120) throw new TypeError('invalid connection TTL');
    Object.assign(this, { store, verifyExecution, ttlSeconds });
  }

  async register({ nodeId, slot, deploymentId, configHash, role = 'incremental', mode = 'incremental_collect' }) {
    uuid(nodeId);uuid(deploymentId);
    if (!slotValid(slot) || !hashValid(configHash) || role !== 'incremental' || mode !== 'incremental_collect') throw new TypeError('invalid collecting deployment');
    return this.store.transaction(async client => {
      const node = (await client.query('SELECT state FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE',[nodeId])).rows[0];
      if (node?.state !== 'active') fail('REMOTE_NODE_NOT_ACTIVE');
      await client.query(`INSERT INTO remote_ingestion.worker_connections(node_id,slot,deployment_id,config_hash,role,mode)
        VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(node_id,slot) DO NOTHING`,[nodeId,slot,deploymentId,configHash,role,mode]);
      const row=(await client.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2 FOR UPDATE',[nodeId,slot])).rows[0];
      if(row.deployment_id!==deploymentId || row.config_hash!==configHash || row.mode!==mode)fail('WORKER_DEPLOYMENT_CONFLICT');
      await client.query('UPDATE remote_ingestion.nodes SET slot_claims_required=true WHERE node_id=$1',[nodeId]);
      return {node_id:nodeId,slot,deployment_id:deploymentId,config_hash:configHash,role,mode};
    });
  }

  identity(nodeId,value) {
    const fields=['version','mode','node_id','slot','deployment_id','config_hash','instance_id','relay_boot_id','runtime_revision','accepting'];
    if(!value || Object.keys(value).some(key=>!fields.includes(key)) || value.version!==1 || value.mode!=='incremental_collect'
      || value.node_id!==nodeId || !slotValid(value.slot) || !hashValid(value.config_hash)
      || !/^[a-f0-9]{48}$/.test(value.relay_boot_id) || value.runtime_revision!==REMOTE_RUNTIME_REVISION
      || typeof value.accepting!=='boolean')throw new RemoteProtocolError('INVALID_WORKER_CONNECTION',400);
    uuid(value.node_id);uuid(value.deployment_id);uuid(value.instance_id);
  }

  matches(row,value) {
    return row && row.mode===value.mode && row.deployment_id===value.deployment_id && row.config_hash===value.config_hash;
  }

  async heartbeat(nodeId,value) {
    this.identity(nodeId,value);
    return this.store.transaction(async client=>{
      const node=(await client.query('SELECT state FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE',[nodeId])).rows[0];
      if(!node || node.state==='disabled')throw new RemoteProtocolError('UNAUTHORIZED',401);
      const row=(await client.query(`SELECT *,connected_until>clock_timestamp() AS alive
        FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2 FOR UPDATE`,[nodeId,value.slot])).rows[0];
      if(!this.matches(row,value))fail('WORKER_DEPLOYMENT_MISMATCH');
      const replacement=row.instance_id!==value.instance_id || row.relay_boot_id!==value.relay_boot_id;
      if(replacement && row.instance_id){
        if(row.alive)fail('WORKER_INSTANCE_BUSY');
        const busy=(await client.query(`SELECT 1 WHERE EXISTS(SELECT 1 FROM remote_ingestion.tasks
          WHERE node_id=$1 AND worker_slot=$2 AND state='leased') OR EXISTS(SELECT 1 FROM remote_ingestion.network_bindings
          WHERE node_id=$1 AND slot=$2 AND state<>'retired')`,[nodeId,value.slot])).rowCount;
        if(busy)fail('WORKER_PREVIOUS_EXECUTION_UNSETTLED');
      }
      const seen=(await client.query(`UPDATE remote_ingestion.worker_connections SET instance_id=$3,relay_boot_id=$4,
        accepting=$5,runtime_revision=$6,enabled=CASE WHEN $7 THEN false ELSE enabled END,
        last_seen_at=clock_timestamp(),connected_until=clock_timestamp()+($8 * interval '1 second')
        WHERE node_id=$1 AND slot=$2 RETURNING *`,[nodeId,value.slot,value.instance_id,value.relay_boot_id,
        value.accepting,value.runtime_revision,replacement,this.ttlSeconds])).rows[0];
      const ready=seen.enabled && seen.accepting && node.state==='active'
        && typeof this.verifyExecution==='function' && await this.verifyExecution(client,seen)===true;
      return {...value,state:!seen.accepting?'draining':ready?'ready':'connected_waiting_activation',ready_for_tasks:ready,
        server_time:seen.last_seen_at.toISOString(),connected_until:seen.connected_until.toISOString()};
    });
  }

  async activate(value) {
    this.identity(value?.node_id,value);
    if(typeof this.verifyExecution!=='function')fail('CENTRAL_EXECUTION_NOT_CONFIGURED');
    return this.store.transaction(async client=>{
      const node=(await client.query('SELECT state FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE',[value.node_id])).rows[0];
      const row=(await client.query(`SELECT *,connected_until>clock_timestamp() AS alive
        FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2 FOR UPDATE`,[value.node_id,value.slot])).rows[0];
      if(node?.state!=='active' || !this.matches(row,value) || !row.alive || !row.accepting
        || row.instance_id!==value.instance_id || row.relay_boot_id!==value.relay_boot_id
        || row.runtime_revision!==REMOTE_RUNTIME_REVISION)fail('WORKER_NOT_READY');
      // The supervisor check runs under the same node/connection locks as claim.
      // It must verify its exact slot/instance, queue processor and Rota identity;
      // a page checkbox or an image label cannot authorize channel execution.
      if(await this.verifyExecution(client,row)!==true)fail('CENTRAL_EXECUTION_NOT_READY');
      await client.query('UPDATE remote_ingestion.worker_connections SET enabled=true,activation_requested=true,activated_at=clock_timestamp() WHERE node_id=$1 AND slot=$2',[value.node_id,value.slot]);
      return {enabled:true};
    });
  }

  async drain(nodeId,slot,{keepRequested=false}={}) {
    uuid(nodeId);if(!slotValid(slot))throw new TypeError('invalid slot');
    return this.store.transaction(async client=>{
      await client.query('SELECT node_id FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE',[nodeId]);
      const changed=await client.query(`UPDATE remote_ingestion.worker_connections SET enabled=false,
        activation_requested=CASE WHEN $3 THEN activation_requested ELSE false END WHERE node_id=$1 AND slot=$2 RETURNING slot`,[nodeId,slot,keepRequested]);
      if(!changed.rowCount)fail('WORKER_DEPLOYMENT_MISMATCH');
      return {enabled:false};
    });
  }

  async claim(nodeId,value) {
    this.identity(nodeId,value?.connection);
    if(value.slot!==value.connection.slot)fail('WORKER_SLOT_MISMATCH');
    return this.store.claim(nodeId,uuid(value.claim_id),value.slot,{authorize:async client=>{
      const row=(await client.query(`SELECT *,connected_until>clock_timestamp() AS alive
        FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2 FOR UPDATE`,[nodeId,value.slot])).rows[0];
      if(!this.matches(row,value.connection) || !row.alive || row.instance_id!==value.connection.instance_id
        || row.relay_boot_id!==value.connection.relay_boot_id)fail('WORKER_CONNECTION_STALE');
      return {allowNew:row.enabled && row.accepting && typeof this.verifyExecution==='function'
        && await this.verifyExecution(client,row)===true};
    }});
  }
}
