import { randomUUID } from 'node:crypto';
import { canonicalIncrementalJson } from '../incrementalPlan.js';
import { CHANNEL_PLAN_CAPABILITY, remoteChannelPlan, planFromTask, assertChannelOperation } from './channelPlanContract.js';
import { decodeResult, hash, RemoteProtocolError, uuid } from './protocol.js';

export class RemoteChannelPlanStore {
  constructor({ store }) { this.store = store; }

  async enqueue(job, { executionAttemptId, client = null }) {
    const contract = remoteChannelPlan(job);
    if (contract.route === 'central') return contract;
    if (typeof executionAttemptId !== 'string' || !executionAttemptId) throw new TypeError('central execution attempt required');
    const taskId = await this.store.enqueue({
      workKey: contract.workKey, capability: CHANNEL_PLAN_CAPABILITY,
      scopeKey: `channel:${contract.plan.channel_id}`,
      input: { plan: contract.plan },
      context: { plan_hash: contract.planHash, execution_attempt_id: executionAttemptId },
    }, { client });
    return { ...contract, taskId };
  }

  async lock(client, lease, { coordinatorId = null, requireLive = true, allowHistoricalReceipt = false } = {}) {
    const task = (await client.query(`SELECT *,lease_until>clock_timestamp() AS live,
      coordinator_until>clock_timestamp() AS coordinated FROM remote_ingestion.tasks
      WHERE task_id=$1 FOR UPDATE`, [uuid(lease.task_id)])).rows[0];
    if (!task || (!allowHistoricalReceipt && (task.generation !== lease.generation || (lease.node_id && task.node_id !== lease.node_id)))) {
      throw new RemoteProtocolError('STALE_LEASE');
    }
    planFromTask(task);
    if (requireLive && (task.state !== 'leased' || !task.live)) throw new RemoteProtocolError('STALE_LEASE');
    if (coordinatorId && (task.coordinator_id !== coordinatorId || !task.coordinated)) {
      throw new RemoteProtocolError('STALE_COORDINATOR');
    }
    return task;
  }

  async coordinate(lease) {
    return this.store.transaction(async (client) => {
      const task = await this.lock(client, lease);
      if (task.coordinated) throw new RemoteProtocolError('COORDINATOR_BUSY');
      const coordinatorId = randomUUID();
      await client.query(`UPDATE remote_ingestion.tasks SET coordinator_id=$2,
        coordinator_until=clock_timestamp()+interval '60 seconds' WHERE task_id=$1`, [task.task_id, coordinatorId]);
      return { task, coordinatorId };
    });
  }

  async coordinateApiReplay(taskId, requestId, assertBusinessFence) {
    return this.store.transaction(async client => {
      const task = (await client.query('SELECT *,coordinator_until>clock_timestamp() AS coordinated FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE', [uuid(taskId)])).rows[0];
      if (!task) throw new RemoteProtocolError('STALE_LEASE');
      const plan = planFromTask(task);
      const api = (await client.query('SELECT status,run_id FROM crawler.youtube_api_detail_requests WHERE request_id=$1', [requestId])).rows[0];
      if (!api || api.run_id !== `incremental:${plan.plan_id}`) throw new RemoteProtocolError('API_REQUEST_IDENTITY_CONFLICT');
      if (task.state === 'applied') return { task, terminal: true };
      await assertBusinessFence(client, task);
      if (task.coordinated) throw new RemoteProtocolError('COORDINATOR_BUSY');
      const coordinatorId = randomUUID();
      await client.query("UPDATE remote_ingestion.tasks SET coordinator_id=$2,coordinator_until=clock_timestamp()+interval '60 seconds' WHERE task_id=$1", [task.task_id, coordinatorId]);
      return { task, coordinatorId };
    });
  }

  async apiReplayTransaction(lease, coordinatorId, assertBusinessFence, action) {
    return this.store.transaction(async client => {
      const task = await this.lock(client, lease, { coordinatorId, requireLive: false });
      await assertBusinessFence(client, task);
      const result = await action(client, task);
      await client.query("UPDATE remote_ingestion.tasks SET coordinator_until=clock_timestamp()+interval '60 seconds' WHERE task_id=$1", [lease.task_id]);
      return result;
    });
  }

  async transaction(lease, coordinatorId, assertBusinessFence, action) {
    return this.store.transaction(async (client) => {
      const task = await this.lock(client, lease, { coordinatorId });
      await assertBusinessFence(client, task);
      const result = await action(client, task);
      await client.query(`UPDATE remote_ingestion.tasks SET coordinator_until=clock_timestamp()+interval '60 seconds'
        WHERE task_id=$1`, [lease.task_id]);
      return result;
    });
  }

  async request(client, task, operation, input, requestKey) {
    assertChannelOperation(planFromTask(task), operation, input);
    if (Buffer.byteLength(JSON.stringify(input)) > 65536) throw new RemoteProtocolError('COMMAND_TOO_LARGE', 400);
    const commandKey = hash(canonicalIncrementalJson({ operation, input, requestKey }));
    const existing = (await client.query(`SELECT command_id FROM remote_ingestion.channel_commands
      WHERE task_id=$1 AND generation=$2 AND command_key=$3`, [task.task_id, task.generation, commandKey])).rows[0];
    if (existing) return existing.command_id;
    const count = (await client.query(`SELECT count(*)::int AS n FROM remote_ingestion.channel_commands
      WHERE task_id=$1 AND generation=$2`, [task.task_id, task.generation])).rows[0].n;
    if (count >= 1000) throw new RemoteProtocolError('CHANNEL_COMMAND_LIMIT');
    const id = randomUUID();
    await client.query(`INSERT INTO remote_ingestion.channel_commands(command_id,task_id,generation,command_key,operation,input)
      VALUES($1,$2,$3,$4,$5,$6)`, [id, task.task_id, task.generation, commandKey, operation, input]);
    return id;
  }

  async poll(nodeId, lease) {
    return this.store.transaction(async (client) => {
      const task = await this.lock(client, { ...lease, node_id: nodeId }, { requireLive: false });
      if (task.state !== 'leased') return { status: task.state, commands: [] };
      if (!task.live) throw new RemoteProtocolError('STALE_LEASE');
      const rows = (await client.query(`SELECT command_id,operation,input FROM remote_ingestion.channel_commands
        WHERE task_id=$1 AND generation=$2 AND state='pending' ORDER BY created_at,command_id LIMIT 8`,
      [lease.task_id, lease.generation])).rows;
      return { status: 'leased', commands: rows };
    });
  }

  async receive(nodeId, lease, compressed) {
    const { value, sha256 } = await decodeResult(compressed);
    const commandId = uuid(value.command_id);
    lease = { ...lease, generation: lease.generation ?? value.generation };
    if (value.generation !== lease.generation) throw new RemoteProtocolError('STALE_LEASE');
    return this.store.transaction(async (client) => {
      const task = await this.lock(client, { ...lease, node_id: nodeId }, { requireLive: false, allowHistoricalReceipt: true });
      const historical = task.generation !== lease.generation || task.node_id !== nodeId;
      if (historical) {
        const owner = (await client.query('SELECT 1 FROM remote_ingestion.claims WHERE task_id=$1 AND generation=$2 AND node_id=$3 LIMIT 1', [lease.task_id, lease.generation, nodeId])).rows[0];
        if (!owner) throw new RemoteProtocolError('STALE_LEASE');
      }
      const command = (await client.query(`SELECT * FROM remote_ingestion.channel_commands
        WHERE command_id=$1 AND task_id=$2 AND generation=$3 FOR UPDATE`,
      [commandId, lease.task_id, lease.generation])).rows[0];
      if (!command) throw new RemoteProtocolError('UNKNOWN_COMMAND', 404);
      if (command.state === 'received') {
        if (command.batch_id !== value.batch_id || command.sha256 !== sha256) throw new RemoteProtocolError('BATCH_CONFLICT');
      } else {
        if (historical) throw new RemoteProtocolError('STALE_LEASE');
        if (task.state !== 'leased' || !task.live) throw new RemoteProtocolError('STALE_LEASE');
        try {
          await client.query(`UPDATE remote_ingestion.channel_commands SET state='received',batch_id=$2,
            sha256=$3,payload_gzip=$4,received_at=clock_timestamp() WHERE command_id=$1`,
          [commandId, value.batch_id, sha256, compressed]);
        } catch (error) {
          if (error.code === '23505') throw new RemoteProtocolError('BATCH_CONFLICT');
          throw error;
        }
      }
      return { durable: true, batch_id: value.batch_id, command_id: commandId, status: 'received' };
    });
  }

  async complete(client, task, result) {
    await client.query(`UPDATE remote_ingestion.tasks SET state='applied',applied_at=clock_timestamp(),
      applied_result=$2,last_error=NULL WHERE task_id=$1`, [task.task_id, result]);
  }

  // Called by central orchestration after API completion; never an HTTP node action.
  async resumeAfterApi(taskId, assertBusinessFence) {
    if (typeof assertBusinessFence !== 'function') throw new TypeError('business fence required');
    return this.store.transaction(async (client) => {
      const task = (await client.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE', [uuid(taskId)])).rows[0];
      if (!task || task.state !== 'received' || task.last_error !== 'VIDEO_API_PENDING') return false;
      const plan = planFromTask(task);
      await assertBusinessFence(client, task);
      const api = (await client.query(`SELECT status,run_id FROM crawler.youtube_api_detail_requests
        WHERE request_id=$1`, [task.applied_result?.request_id])).rows[0];
      if (!api || api.run_id !== `incremental:${plan.plan_id}`) throw new RemoteProtocolError('API_REQUEST_IDENTITY_CONFLICT');
      if (api.status === 'pending') return false;
      await client.query(`UPDATE remote_ingestion.tasks SET state='pending',coordinator_id=NULL,coordinator_until=NULL,
        lease_until=NULL,node_id=NULL,applied_result=NULL,last_error=NULL WHERE task_id=$1`, [taskId]);
      return true;
    });
  }
}
