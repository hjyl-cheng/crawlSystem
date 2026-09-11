import { RemoteProtocolError, uuid } from './protocol.js';

// Only central code registers the exact deployment. A heartbeat cannot enroll
// a new slot, increase capacity, enable collection or alter a channel lease.
export class RemoteWorkerConnectionStore {
  constructor({ store, ttlSeconds = 45 }) {
    if (!Number.isInteger(ttlSeconds) || ttlSeconds < 15 || ttlSeconds > 120) throw new TypeError('invalid connection TTL');
    Object.assign(this, { store, ttlSeconds });
  }

  async claim() { throw new RemoteProtocolError('WORKER_ACTIVATION_REQUIRED'); }

  async register({ nodeId, slot, deploymentId, configHash, role = 'incremental' }) {
    uuid(nodeId); uuid(deploymentId);
    if (typeof slot !== 'string' || !/^[a-z0-9-]{1,60}$/.test(slot) || !/^[a-f0-9]{64}$/.test(configHash) || role !== 'incremental') throw new TypeError('invalid worker deployment');
    return this.store.transaction(async client => {
      await client.query(`INSERT INTO remote_ingestion.worker_connections(node_id,slot,deployment_id,config_hash,role)
        VALUES($1,$2,$3,$4,$5) ON CONFLICT(node_id,slot) DO NOTHING`, [nodeId, slot, deploymentId, configHash, role]);
      const row = (await client.query(`SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2 FOR UPDATE`, [nodeId, slot])).rows[0];
      if (row.deployment_id !== deploymentId || row.config_hash !== configHash || row.role !== role) {
        // Updating/replacing a deployment requires a future drain operation;
        // connection expiry alone is not evidence that a worker has stopped.
        throw new RemoteProtocolError('WORKER_DEPLOYMENT_CONFLICT');
      }
      return { node_id: nodeId, slot, deployment_id: deploymentId, config_hash: configHash, role };
    });
  }

  async heartbeat(nodeId, value) {
    const fields = ['version', 'mode', 'node_id', 'slot', 'deployment_id', 'config_hash', 'instance_id', 'relay_boot_id'];
    if (!value || Object.keys(value).some(key => !fields.includes(key)) || value.version !== 1 || value.mode !== 'connect_only'
      || value.node_id !== nodeId || typeof value.slot !== 'string' || !/^[a-z0-9-]{1,60}$/.test(value.slot)
      || !/^[a-f0-9]{64}$/.test(value.config_hash) || !/^[a-f0-9]{48}$/.test(value.relay_boot_id)) {
      throw new RemoteProtocolError('INVALID_WORKER_CONNECTION', 400);
    }
    uuid(value.deployment_id); uuid(value.instance_id);
    return this.store.transaction(async client => {
      const node = (await client.query('SELECT state FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE', [uuid(nodeId)])).rows[0];
      if (!node || node.state === 'disabled') throw new RemoteProtocolError('UNAUTHORIZED', 401);
      const row = (await client.query(`SELECT *, connected_until > clock_timestamp() AS alive
        FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2 FOR UPDATE`, [nodeId, value.slot])).rows[0];
      if (!row || row.deployment_id !== value.deployment_id || row.config_hash !== value.config_hash) throw new RemoteProtocolError('WORKER_DEPLOYMENT_MISMATCH');
      if (row.alive && (row.instance_id !== value.instance_id || row.relay_boot_id !== value.relay_boot_id)) throw new RemoteProtocolError('WORKER_INSTANCE_BUSY');
      const seen = (await client.query(`UPDATE remote_ingestion.worker_connections SET instance_id=$3,relay_boot_id=$4,
        last_seen_at=clock_timestamp(),connected_until=clock_timestamp()+($5 * interval '1 second')
        WHERE node_id=$1 AND slot=$2 RETURNING last_seen_at,connected_until`, [nodeId, value.slot, value.instance_id, value.relay_boot_id, this.ttlSeconds])).rows[0];
      return { version: 1, mode: 'connect_only', node_id: nodeId, slot: value.slot,
        deployment_id: row.deployment_id, config_hash: row.config_hash, instance_id: value.instance_id,
        relay_boot_id: value.relay_boot_id, state: 'connected_waiting_activation', ready_for_tasks: false,
        server_time: seen.last_seen_at.toISOString(), connected_until: seen.connected_until.toISOString() };
    });
  }
}
