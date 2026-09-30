import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { canonicalIncrementalJson } from '../incrementalPlan.js';
import { boundedPostgresRead } from './boundedPostgresRead.js';
import {
  DISCOVER_PAGE_CAPABILITY,
  DISCOVER_PAGE_OPERATION,
  assertDiscoverRequest,
  discoverTaskFrom,
  discoverTaskInput,
  discoverWorkKey,
} from './discoverPageContract.js';
import { decodeResult, hash, RemoteProtocolError, staleLeaseError, uuid } from './protocol.js';

const fail = code => { throw new RemoteProtocolError(code); };
const same = (a, b) => canonicalIncrementalJson(a) === canonicalIncrementalJson(b);

// Center-side transport for remote Discover pages. Every task carries one
// frozen search request and exactly one command. The Page Intent in
// crawler.query_pages stays the business owner; this store grants no writes to it.
export class RemoteDiscoverPageStore {
  constructor({ store, transportSignals = null, readTimeoutMs = 5000 }) {
    Object.assign(this, { store, transportSignals, readTimeoutMs });
  }

  read(text, values, signal) {
    return boundedPostgresRead(this.store.pool, { text, values, signal, timeoutMs: this.readTimeoutMs });
  }

  // The remote request may only serve the Page Intent that created it while
  // that intent is still being fetched.
  async assertBusinessFence(client, task) {
    const input = discoverTaskFrom(task);
    const page = (await client.query(`SELECT page_intent_hash,managed_fetch_status
      FROM crawler.query_pages WHERE page_id=$1`, [input.page_id])).rows[0];
    if (!page || page.page_intent_hash !== input.page_intent_hash || page.managed_fetch_status !== 'running') {
      fail('DISCOVER_BUSINESS_FENCE_STALE');
    }
    return null;
  }

  async lock(client, lease, { requireLive = true, allowHistoricalReceipt = false } = {}) {
    const task = (await client.query(`SELECT *,clock_timestamp() AS observed_at,lease_until>clock_timestamp() AS live
      FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE`, [uuid(lease.task_id)])).rows[0];
    if (!task || (!allowHistoricalReceipt && (task.generation !== lease.generation
      || (lease.node_id && task.node_id !== lease.node_id)))) throw staleLeaseError(task, lease, 'lock');
    discoverTaskFrom(task);
    if (requireLive && (task.state !== 'leased' || !task.live)) throw staleLeaseError(task, lease, 'lock');
    return task;
  }

  // Binds one Rota attempt to a durable remote delivery for one node slot.
  async admit({ nodeId, slot, attemptId, rotaWorkerId, pageId, pageIntentHash, request, assertAdmission = null }) {
    uuid(nodeId);
    const input = discoverTaskInput({ pageId, pageIntentHash, request });
    const context = { execution_attempt_id: attemptId };
    const workKey = discoverWorkKey(pageId);
    return this.store.transaction(async client => {
      const node = (await client.query('SELECT state FROM remote_ingestion.nodes WHERE node_id=$1 FOR SHARE', [nodeId])).rows[0];
      if (node?.state !== 'active') fail('REMOTE_NODE_NOT_ACTIVE');
      if (assertAdmission && await assertAdmission(client, nodeId, slot) !== true) fail('REMOTE_SUPERVISOR_NOT_READY');
      let task = (await client.query('SELECT * FROM remote_ingestion.tasks WHERE work_key=$1 FOR UPDATE', [workKey])).rows[0];
      if (task && task.capability !== DISCOVER_PAGE_CAPABILITY) fail('WORK_KEY_CONFLICT');
      const sameAttempt = task?.context.execution_attempt_id === attemptId;
      if (task && sameAttempt) {
        if (!['pending', 'leased'].includes(task.state) || task.target_node_id !== nodeId
          || task.target_worker_slot !== slot || !same(task.input, input)) fail('REMOTE_EXECUTION_REPLAY_CONFLICT');
      } else if (task) {
        if (['pending', 'leased'].includes(task.state)) fail('REMOTE_EXECUTION_NOT_SETTLED');
        const unfinished = await client.query(`SELECT 1 FROM remote_ingestion.network_bindings
          WHERE task_id=$1 AND state<>'retired' LIMIT 1`, [task.task_id]);
        if (unfinished.rowCount) fail('REMOTE_PREVIOUS_NETWORK_NOT_RETIRED');
        await client.query(`UPDATE remote_ingestion.tasks SET state='pending',input=$2,context=$3,
          target_node_id=$4,target_worker_slot=$5,node_id=NULL,worker_slot=NULL,lease_until=NULL,
          coordinator_id=NULL,coordinator_until=NULL,last_error=NULL,applied_result=NULL,received_at=NULL,applied_at=NULL
          WHERE task_id=$1`, [task.task_id, input, context, nodeId, slot]);
      } else {
        const taskId = await this.store.enqueue({ workKey, capability: DISCOVER_PAGE_CAPABILITY, input, context }, { client });
        await client.query(`UPDATE remote_ingestion.tasks SET target_node_id=$2,target_worker_slot=$3
          WHERE task_id=$1`, [taskId, nodeId, slot]);
        task = { task_id: taskId };
      }
      const registered = (await client.query(`SELECT rota_worker_id FROM remote_ingestion.network_slots
        WHERE node_id=$1 AND slot=$2`, [nodeId, slot])).rows[0];
      if (registered?.rota_worker_id !== rotaWorkerId) fail('NETWORK_SLOT_CONFLICT');
      return { taskId: task.task_id, attemptId };
    }, { operation: 'discover_admission' });
  }

  async waitClaim(admission, { nodeId, slot, signal, pollMs = 100 }) {
    for (;;) {
      signal.throwIfAborted();
      const notification = this.transportSignals?.watch(`task:${admission.taskId}`, { timeoutMs: 5000, signal });
      try {
        const task = (await this.read(`SELECT *,lease_until>clock_timestamp() AS alive
          FROM remote_ingestion.tasks WHERE task_id=$1`, [admission.taskId], signal)).rows[0];
        signal.throwIfAborted();
        if (task?.context.execution_attempt_id !== admission.attemptId) fail('REMOTE_EXECUTION_REPLACED');
        if (task.state === 'leased' && task.alive && task.node_id === nodeId && task.worker_slot === slot) return this.store.lease(task);
        if (task.state !== 'pending') fail('REMOTE_EXECUTION_NOT_AVAILABLE');
        if (notification) await notification.wait; else await delay(pollMs, null, { signal });
      } finally { notification?.cancel(); }
    }
  }

  async find(pageId, attemptId) {
    const row = (await this.read(`SELECT task_id FROM remote_ingestion.tasks
      WHERE work_key=$1 AND context->>'execution_attempt_id'=$2`, [discoverWorkKey(pageId), attemptId])).rows[0];
    return row ? { taskId: row.task_id, attemptId } : null;
  }

  // One command per task generation. A repeated request for the same frozen
  // input returns the original command, so a lost response never duplicates work.
  async request(lease, { assertAdmission = null } = {}) {
    return this.store.transaction(async client => {
      const task = await this.lock(client, lease);
      if (assertAdmission && await assertAdmission(client) !== true) fail('REMOTE_SUPERVISOR_NOT_READY');
      await this.assertBusinessFence(client, task);
      const input = assertDiscoverRequest(task.input.request);
      const commandKey = hash(canonicalIncrementalJson({ operation: DISCOVER_PAGE_OPERATION, input }));
      const existing = (await client.query(`SELECT command_id FROM remote_ingestion.channel_commands
        WHERE task_id=$1 AND generation=$2 AND command_key=$3`, [task.task_id, task.generation, commandKey])).rows[0];
      if (existing) return existing.command_id;
      const other = await client.query(`SELECT 1 FROM remote_ingestion.channel_commands
        WHERE task_id=$1 AND generation=$2 LIMIT 1`, [task.task_id, task.generation]);
      if (other.rowCount) fail('DISCOVER_COMMAND_CONFLICT');
      const commandId = randomUUID();
      await client.query(`INSERT INTO remote_ingestion.channel_commands(command_id,task_id,generation,command_key,operation,input)
        VALUES($1,$2,$3,$4,$5,$6)`, [commandId, task.task_id, task.generation, commandKey, DISCOVER_PAGE_OPERATION, input]);
      return commandId;
    }, { operation: 'discover_request' });
  }

  async awaitResult(lease, commandId, { signal, pollMs = 100 }) {
    for (;;) {
      signal.throwIfAborted();
      const notification = this.transportSignals?.watch(`task:${lease.task_id}`, { timeoutMs: 5000, signal });
      try {
        const row = (await this.read(`SELECT c.state,c.payload_gzip,t.state AS task_state,t.generation
          FROM remote_ingestion.channel_commands c JOIN remote_ingestion.tasks t USING(task_id)
          WHERE c.command_id=$1 AND c.task_id=$2 AND c.generation=$3`, [commandId, lease.task_id, lease.generation], signal)).rows[0];
        signal.throwIfAborted();
        if (!row) fail('UNKNOWN_COMMAND');
        if (row.state === 'received') return (await decodeResult(row.payload_gzip)).value;
        if (row.task_state !== 'leased' || row.generation !== lease.generation) {
          throw staleLeaseError({ ...row, state: row.task_state }, lease, 'await_result');
        }
        if (notification) await notification.wait; else await delay(pollMs, null, { signal });
      } finally { notification?.cancel(); }
    }
  }

  // Closes the delivery once its single result is durably received. The node
  // sees a non-leased task on its next poll and retires the network session.
  async complete(lease, result) {
    return this.store.transaction(async client => {
      const task = await this.lock(client, lease, { requireLive: false });
      if (task.state !== 'leased') fail('REMOTE_EXECUTION_NOT_AVAILABLE');
      await client.query(`UPDATE remote_ingestion.tasks SET state='applied',applied_at=clock_timestamp(),
        applied_result=$2,last_error=NULL,coordinator_id=NULL,coordinator_until=NULL WHERE task_id=$1`, [task.task_id, result]);
    }, { operation: 'discover_complete' });
  }

  // Stops only the exact attempt's delivery; completed or newer ones are kept.
  async stop(admission, error) {
    await this.store.transaction(async client => {
      const task = (await client.query('SELECT state,context FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE',
        [admission.taskId])).rows[0];
      if (task?.context.execution_attempt_id !== admission.attemptId || !['pending', 'leased'].includes(task.state)) return;
      await client.query(`UPDATE remote_ingestion.tasks SET state='failed',last_error=$2,
        coordinator_until=NULL WHERE task_id=$1`, [admission.taskId, String(error?.code || 'REMOTE_EXECUTION_STOPPED').slice(0, 300)]);
    }, { operation: 'discover_stop' });
  }

  async bindings(admission) {
    const rows = await this.read(`SELECT b.* FROM remote_ingestion.tasks t
      LEFT JOIN remote_ingestion.network_bindings b ON b.task_id=t.task_id
      WHERE t.task_id=$1 AND t.context->>'execution_attempt_id'=$2`, [admission.taskId, admission.attemptId]);
    if (!rows.rows.length) fail('REMOTE_EXECUTION_REPLACED');
    return rows.rows.filter(row => row.binding_id);
  }

  // Node API. The node learns its single command; it never learns the page row.
  async poll(nodeId, lease) {
    return this.store.transaction(async client => {
      const task = await this.lock(client, { ...lease, node_id: nodeId }, { requireLive: false });
      if (task.state !== 'leased') return { status: task.state, commands: [] };
      if (!task.live) throw staleLeaseError(task, { ...lease, node_id: nodeId }, 'poll');
      const rows = (await client.query(`SELECT command_id,operation,input FROM remote_ingestion.channel_commands
        WHERE task_id=$1 AND generation=$2 AND state='pending' ORDER BY created_at,command_id LIMIT 1`,
      [lease.task_id, lease.generation])).rows;
      return { status: 'leased', commands: rows };
    }, { operation: 'discover_poll' });
  }

  async receive(nodeId, lease, compressed) {
    const { value, sha256 } = await decodeResult(compressed);
    const commandId = uuid(value.command_id);
    lease = { ...lease, generation: lease.generation ?? value.generation };
    if (value.generation !== lease.generation) throw new RemoteProtocolError('STALE_LEASE');
    return this.store.transaction(async client => {
      const task = await this.lock(client, { ...lease, node_id: nodeId }, { requireLive: false, allowHistoricalReceipt: true });
      const historical = task.generation !== lease.generation || task.node_id !== nodeId;
      if (historical) {
        const owner = await client.query(`SELECT 1 FROM remote_ingestion.claims
          WHERE task_id=$1 AND generation=$2 AND node_id=$3 LIMIT 1`, [lease.task_id, lease.generation, nodeId]);
        if (!owner.rowCount) throw new RemoteProtocolError('STALE_LEASE');
      }
      const command = (await client.query(`SELECT * FROM remote_ingestion.channel_commands
        WHERE command_id=$1 AND task_id=$2 AND generation=$3 FOR UPDATE`, [commandId, lease.task_id, lease.generation])).rows[0];
      if (!command || command.operation !== DISCOVER_PAGE_OPERATION) throw new RemoteProtocolError('UNKNOWN_COMMAND', 404);
      if (command.state === 'received') {
        if (command.batch_id !== value.batch_id || command.sha256 !== sha256) throw new RemoteProtocolError('BATCH_CONFLICT');
      } else {
        if (historical || task.state !== 'leased' || !task.live) throw new RemoteProtocolError('STALE_LEASE');
        try {
          await client.query(`UPDATE remote_ingestion.channel_commands SET state='received',batch_id=$2,
            sha256=$3,payload_gzip=$4,received_at=clock_timestamp() WHERE command_id=$1`, [commandId, value.batch_id, sha256, compressed]);
        } catch (error) {
          if (error.code === '23505') throw new RemoteProtocolError('BATCH_CONFLICT');
          throw error;
        }
      }
      return { durable: true, batch_id: value.batch_id, command_id: commandId, status: 'received' };
    }, { operation: 'discover_receive' });
  }
}
