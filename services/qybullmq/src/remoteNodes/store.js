import { randomUUID } from 'node:crypto';
import { decodeResult, generation, hash, RemoteProtocolError, uuid } from './protocol.js';

const conflict = (code) => { throw new RemoteProtocolError(code); };

export class RemoteNodeStore {
  constructor({ pool, leaseSeconds = 90, maxBacklog = 1000, maxExecutions = 3,
    maxProcessAttempts = 5, retrySeconds = 15 }) {
    for (const value of [leaseSeconds, maxBacklog, maxExecutions, maxProcessAttempts, retrySeconds]) {
      if (!Number.isSafeInteger(value) || value < 1) throw new TypeError('positive integer limits required');
    }
    Object.assign(this, { pool, leaseSeconds, maxBacklog, maxExecutions, maxProcessAttempts, retrySeconds });
  }

  async transaction(action) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query("SET LOCAL synchronous_commit = on");
      await client.query("SET LOCAL statement_timeout = '15s'");
      await client.query("SET LOCAL lock_timeout = '3s'");
      const result = await action(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally { client.release(); }
  }

  // Central-only administration. These methods have no public HTTP route.
  async registerNode({ nodeId, token, capabilities, maxLeases = 1 }) {
    uuid(nodeId);
    if (typeof token !== 'string' || token.length < 32 || !Array.isArray(capabilities)
      || !capabilities.length || capabilities.some((v) => typeof v !== 'string' || !v)) {
      throw new TypeError('strong token and capabilities required');
    }
    await this.pool.query(`INSERT INTO remote_ingestion.nodes(node_id,token_hash,capabilities,max_leases)
      VALUES($1,$2,$3,$4)`, [nodeId, hash(token), capabilities, maxLeases]);
  }

  async authenticate(token) {
    if (typeof token !== 'string' || token.length < 32 || token.length > 256) {
      throw new RemoteProtocolError('UNAUTHORIZED', 401);
    }
    const node = (await this.pool.query(`SELECT node_id FROM remote_ingestion.nodes
      WHERE token_hash=$1 AND state <> 'disabled'`, [hash(token)])).rows[0];
    if (!node) throw new RemoteProtocolError('UNAUTHORIZED', 401);
    return node.node_id;
  }

  async setNodeState(nodeId, state) {
    return this.pool.query('UPDATE remote_ingestion.nodes SET state=$2 WHERE node_id=$1', [uuid(nodeId), state]);
  }

  async enqueue({ workKey, capability, input, context, scopeKey = null }, { client = null } = {}) {
    if (typeof workKey !== 'string' || !workKey || workKey.length > 500
      || typeof capability !== 'string' || !capability || !input || !context) throw new TypeError('task contract required');
    if (Buffer.byteLength(JSON.stringify({ input, context })) > 65536) throw new TypeError('task contract too large');
    const write = async (client) => {
      await client.query(`INSERT INTO remote_ingestion.tasks(task_id,work_key,capability,input,context,scope_key)
        VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(work_key) DO NOTHING`,
      [randomUUID(), workKey, capability, input, context, scopeKey]);
      const row = (await client.query(`SELECT *, (capability=$2 AND input=$3::jsonb AND context=$4::jsonb
        AND scope_key IS NOT DISTINCT FROM $5) AS same
        FROM remote_ingestion.tasks WHERE work_key=$1 FOR UPDATE`, [workKey, capability, input, context, scopeKey])).rows[0];
      if (!row.same) conflict('WORK_KEY_CONFLICT');
      return row.task_id;
    };
    return client ? write(client) : this.transaction(write);
  }

  lease(row) {
    return { task_id: row.task_id, generation: row.generation, capability: row.capability,
      ...(row.worker_slot ? { worker_slot: row.worker_slot } : {}),
      input: row.input, lease_until: row.lease_until, heartbeat_ms: Math.max(1000, this.leaseSeconds * 1000 / 3) };
  }

  async claim(nodeId, claimId, slot = null, { authorize = null } = {}) {
    uuid(nodeId); uuid(claimId);
    if (slot !== null && (typeof slot !== 'string' || !/^[a-zA-Z0-9_.-]{1,100}$/.test(slot))) {
      throw new RemoteProtocolError('INVALID_WORKER_SLOT', 400);
    }
    return this.transaction(async (client) => {
      // Serialize capacity checks without blocking task foreign-key checks.
      const node = (await client.query(`SELECT * FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE`, [nodeId])).rows[0];
      if (!node || node.state === 'disabled') throw new RemoteProtocolError('UNAUTHORIZED', 401);
      const permission = authorize ? await authorize(client) : { allowNew: true };
      if (node.slot_claims_required && slot === null) conflict('WORKER_SLOT_REQUIRED');
      await client.query('UPDATE remote_ingestion.nodes SET last_seen_at=clock_timestamp() WHERE node_id=$1', [nodeId]);
      const previous = (await client.query(`SELECT t.*, c.node_id AS claiming_node, c.generation AS claimed_generation,
        c.worker_slot AS claiming_slot,
        (t.lease_until > clock_timestamp()) AS alive FROM remote_ingestion.claims c
        JOIN remote_ingestion.tasks t USING(task_id) WHERE c.claim_id=$1 FOR UPDATE OF t`, [claimId])).rows[0];
      if (previous) {
        if (previous.claiming_node !== nodeId || previous.node_id !== nodeId || previous.claiming_slot !== slot
          || previous.claimed_generation !== previous.generation || previous.state !== 'leased' || !previous.alive) {
          conflict('CLAIM_EXPIRED');
        }
        return this.lease(previous);
      }
      if (node.state !== 'active' || !permission.allowNew) return null;
      if (slot !== null) {
        const registered = (await client.query('SELECT 1 FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2', [nodeId, slot])).rows[0];
        if (!registered) conflict('UNKNOWN_NETWORK_SLOT');
        // An expired lease still owns its slot until the center recovers it.
        // Never run a second channel beside an uncertain prior execution.
        const occupied = (await client.query(`SELECT 1 FROM remote_ingestion.tasks t
          WHERE node_id=$1 AND worker_slot=$2 AND state='leased'
          AND (lease_until>clock_timestamp() OR NOT EXISTS (SELECT 1 FROM remote_ingestion.network_bindings b
            WHERE b.task_id=t.task_id AND b.generation=t.generation AND b.state='retired')) LIMIT 1`, [nodeId, slot])).rows[0];
        if (occupied) return null;
        const retiring = (await client.query(`SELECT 1 FROM remote_ingestion.network_slots s
          JOIN remote_ingestion.network_bindings b ON b.binding_id=s.binding_id
          WHERE s.node_id=$1 AND s.slot=$2 AND b.state<>'retired'`, [nodeId, slot])).rows[0];
        if (retiring) return null;
      }
      const busy = (await client.query(`SELECT count(*)::integer AS n FROM remote_ingestion.tasks
        WHERE node_id=$1 AND state='leased' AND lease_until > clock_timestamp()`, [nodeId])).rows[0].n;
      if (busy >= node.max_leases) return null;
      const backlog = (await client.query(`SELECT count(*)::integer AS n FROM
        (SELECT 1 FROM remote_ingestion.tasks WHERE state='received' LIMIT $1) q`, [this.maxBacklog])).rows[0].n;
      if (backlog >= this.maxBacklog) return null;
      const row = (await client.query(`SELECT * FROM remote_ingestion.tasks candidate
        WHERE capability=ANY($1) AND (state='pending' OR (state='leased' AND lease_until <= clock_timestamp()))
        AND (target_node_id IS NULL OR (target_node_id=$2 AND target_worker_slot=$3 AND state='pending'))
        AND (scope_key IS NULL OR NOT EXISTS (SELECT 1 FROM remote_ingestion.tasks owner
          WHERE owner.scope_key=candidate.scope_key AND owner.task_id<>candidate.task_id AND owner.state='leased'))
        ORDER BY (node_id=$2 AND worker_slot=$3 AND state='leased') DESC NULLS LAST,created_at,task_id
        LIMIT 1 FOR UPDATE SKIP LOCKED`, [node.capabilities, nodeId, slot])).rows[0];
      if (!row) return null;
      // A new ownership generation after an API handoff is not a failed execution.
      const leaseFailures = row.lease_failures + (row.state === 'leased' ? 1 : 0);
      if (leaseFailures >= this.maxExecutions) {
        await client.query(`UPDATE remote_ingestion.tasks SET state='failed',lease_failures=$2,last_error='EXECUTION_LEASES_EXHAUSTED'
          WHERE task_id=$1`, [row.task_id, leaseFailures]);
        return null;
      }
      const leased = (await client.query(`UPDATE remote_ingestion.tasks SET state='leased',node_id=$2,
        generation=generation+1,coordinator_id=NULL,coordinator_until=NULL,
        lease_failures=$4,worker_slot=$5,lease_until=clock_timestamp()+($3 * interval '1 second') WHERE task_id=$1 RETURNING *`,
      [row.task_id, nodeId, this.leaseSeconds, leaseFailures, slot])).rows[0];
      await client.query(`INSERT INTO remote_ingestion.claims(claim_id,node_id,task_id,generation,worker_slot) VALUES($1,$2,$3,$4,$5)`,
        [claimId, nodeId, leased.task_id, leased.generation, slot]);
      return this.lease(leased);
    }).catch((error) => {
      if (error.code === '23505' && error.constraint === 'remote_channel_scope_lease') return null;
      throw error;
    });
  }

  async heartbeat(nodeId, taskId, attempt) {
    uuid(nodeId); uuid(taskId); generation(attempt);
    const row = (await this.pool.query(`UPDATE remote_ingestion.tasks
      SET lease_until=clock_timestamp()+($4 * interval '1 second')
      WHERE task_id=$1 AND node_id=$2 AND generation=$3 AND state='leased' AND lease_until>clock_timestamp()
      RETURNING lease_until`, [taskId, nodeId, attempt, this.leaseSeconds])).rows[0];
    if (!row) conflict('STALE_LEASE');
    return row;
  }

  async receive(nodeId, taskId, compressed) {
    uuid(nodeId); uuid(taskId);
    const { value, sha256 } = await decodeResult(compressed);
    return this.transaction(async (client) => {
      const task = (await client.query(`SELECT *,lease_until>clock_timestamp() AS alive
        FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE`, [taskId])).rows[0];
      if (task?.capability === 'youtube.incremental.plan.v1') conflict('CHANNEL_PLAN_REQUIRES_CENTRAL_COMPLETION');
      const old = (await client.query('SELECT * FROM remote_ingestion.receipts WHERE batch_id=$1', [value.batch_id])).rows[0];
      if (old) {
        if (old.node_id !== nodeId || old.task_id !== taskId || old.generation !== value.generation || old.sha256 !== sha256) {
          conflict('BATCH_CONFLICT');
        }
        return { batch_id: old.batch_id, durable: true, status: task.state };
      }
      if (!task || task.state !== 'leased' || !task.alive || task.node_id !== nodeId || task.generation !== value.generation) {
        conflict('STALE_LEASE');
      }
      try {
        await client.query(`INSERT INTO remote_ingestion.receipts(batch_id,task_id,generation,node_id,sha256,payload_gzip)
          VALUES($1,$2,$3,$4,$5,$6)`, [value.batch_id, taskId, value.generation, nodeId, sha256, compressed]);
      } catch (error) {
        if (error.code === '23505') conflict('BATCH_CONFLICT');
        throw error;
      }
      await client.query(`UPDATE remote_ingestion.tasks SET state='received',received_at=clock_timestamp()
        WHERE task_id=$1`, [taskId]);
      return { batch_id: value.batch_id, durable: true, status: 'received' };
    });
  }

  async receipt(nodeId, batchId) {
    const row = (await this.pool.query(`SELECT r.batch_id,t.state AS status,r.received_at,t.applied_at
      FROM remote_ingestion.receipts r JOIN remote_ingestion.tasks t USING(task_id)
      WHERE r.batch_id=$1 AND r.node_id=$2`, [uuid(batchId), uuid(nodeId)])).rows[0];
    if (!row) throw new RemoteProtocolError('NOT_FOUND', 404);
    return { ...row, durable: true };
  }

  async cancel(taskId) {
    const row = (await this.pool.query(`UPDATE remote_ingestion.tasks SET state='cancelled',last_error='CENTRAL_CANCELLED'
      WHERE task_id=$1 AND state NOT IN ('applied','cancelled') RETURNING task_id`, [uuid(taskId)])).rows[0];
    return Boolean(row);
  }

  // apply must perform only transactional writes through this client. No network or COMMIT.
  async processOne(handlers) {
    return this.transaction(async (client) => {
      const task = (await client.query(`SELECT * FROM remote_ingestion.tasks
        WHERE state='received' AND next_process_at<=clock_timestamp() AND capability=ANY($1)
        ORDER BY next_process_at,received_at LIMIT 1 FOR UPDATE SKIP LOCKED`, [Object.keys(handlers)])).rows[0];
      if (!task) return null;
      const receipt = (await client.query(`SELECT * FROM remote_ingestion.receipts WHERE task_id=$1 AND generation=$2`,
        [task.task_id, task.generation])).rows[0];
      await client.query('SAVEPOINT apply_business');
      try {
        const { value } = await decodeResult(receipt.payload_gzip);
        if (value.outcome === 'failure') {
          // Preserve the failure for the central orchestrator; never turn it into success.
          await client.query(`UPDATE remote_ingestion.tasks SET state='failed',last_error=$2,
            process_attempts=process_attempts+1 WHERE task_id=$1`,
          [task.task_id, `REMOTE:${value.error.code}`.slice(0, 300)]);
          return { task_id: task.task_id, status: 'failed' };
        }
        const result = await handlers[task.capability](client, { task, data: value.data, receivedAt: receipt.received_at });
        await client.query(`UPDATE remote_ingestion.tasks SET state='applied',applied_at=clock_timestamp(),
          process_attempts=process_attempts+1,applied_result=$2,last_error=NULL WHERE task_id=$1`,
        [task.task_id, result ?? null]);
        return { task_id: task.task_id, status: 'applied' };
      } catch (error) {
        await client.query('ROLLBACK TO SAVEPOINT apply_business');
        const failed = (error instanceof RemoteProtocolError && error.status < 500)
          || error instanceof TypeError || task.process_attempts + 1 >= this.maxProcessAttempts;
        await client.query(`UPDATE remote_ingestion.tasks SET process_attempts=process_attempts+1,
          state=$2,last_error=$3,next_process_at=clock_timestamp()+($4 * interval '1 second') WHERE task_id=$1`,
        [task.task_id, failed ? 'failed' : 'received', String(error.code || error.name || 'APPLY_FAILED').slice(0, 300),
          this.retrySeconds * 2 ** Math.min(task.process_attempts, 6)]);
        return { task_id: task.task_id, status: failed ? 'failed' : 'received', retry: !failed };
      }
    });
  }
}
