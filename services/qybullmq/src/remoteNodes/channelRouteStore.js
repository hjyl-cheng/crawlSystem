import { createCipheriv, createDecipheriv, createHmac, randomBytes, randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { canonicalIncrementalJson } from '../incrementalPlan.js';
import { planFromTask } from './channelPlanContract.js';
import { createRemoteRouteIssuer } from './routeGrant.js';
import { hash, uuid, generation, RemoteProtocolError } from './protocol.js';

const fail = code => { throw new RemoteProtocolError(code); };
const identityFields = ['workload_scope', 'credential_generation', 'network_identity_key', 'profile_epoch',
  'identity_policy_id', 'identity_policy_version', 'identity_policy_hash', 'egress_country'];
const fenceFields = ['slot_name', 'worker_id', 'worker_instance_id', 'lease_id', 'route_generation', 'task_id', 'business_run_id', 'job_execution_id'];
const same = (a, b) => canonicalIncrementalJson(a) === canonicalIncrementalJson(b);

// Durable mapping only. Rota's existing adapter still begins/renews/completes
// its Task and chooses routes. Remote nodes cannot register slots or bind Tasks.
export class RemoteChannelRouteStore {
  constructor({ channelStore, readRotaRoute, assertBusinessFence, privateKey, secretKey, grantTtlMs = 30000 }) {
    if (typeof readRotaRoute !== 'function' || typeof assertBusinessFence !== 'function'
      || !Buffer.isBuffer(secretKey) || secretKey.length !== 32) throw new TypeError('route ownership adapters and 32-byte key required');
    Object.assign(this, { channelStore, store: channelStore.store, readRotaRoute, assertBusinessFence, privateKey, grantTtlMs });
    this.encryptionKey = createHmac('sha256', secretKey).update('remote-route-encryption-v1').digest();
    this.identityKey = createHmac('sha256', secretKey).update('remote-route-identity-v1').digest();
    // Validate signer configuration eagerly.
    createRemoteRouteIssuer({ privateKey, authorize: async () => null, maxTtlMs: grantTtlMs });
  }

  digest(value) { return createHmac('sha256', this.identityKey).update(canonicalIncrementalJson(value)).digest('hex'); }
  encrypt(value, aad) {
    const iv = randomBytes(12); const cipher = createCipheriv('aes-256-gcm', this.encryptionKey, iv);
    cipher.setAAD(Buffer.from(aad));
    const encrypted = Buffer.concat([cipher.update(JSON.stringify(value)), cipher.final()]);
    return Buffer.concat([iv, cipher.getAuthTag(), encrypted]);
  }
  decrypt(bytes, aad) {
    const decipher = createDecipheriv('aes-256-gcm', this.encryptionKey, bytes.subarray(0, 12));
    decipher.setAuthTag(bytes.subarray(12, 28)); decipher.setAAD(Buffer.from(aad));
    return JSON.parse(Buffer.concat([decipher.update(bytes.subarray(28)), decipher.final()]).toString());
  }
  identity(source) { return Object.fromEntries(identityFields.map(field => [field, source[field]])); }

  // Lock order agrees with claim: node -> channel Task -> local slot.
  // NO KEY UPDATE still serializes node ownership but permits the task FK's
  // KEY SHARE check during completion, avoiding a task -> node lock cycle.
  async owned(client, nodeId, lease, slotName) {
    const node = (await client.query('SELECT * FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE', [uuid(nodeId)])).rows[0];
    if (!node || node.state === 'disabled') throw new RemoteProtocolError('UNAUTHORIZED', 401);
    const task = await this.channelStore.lock(client, { ...lease, node_id: nodeId });
    if (task.worker_slot !== slotName) fail('WORKER_SLOT_MISMATCH');
    const businessOwnership = await this.assertBusinessFence(client, task);
    const slot = (await client.query('SELECT * FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2 FOR UPDATE', [nodeId, slotName])).rows[0];
    if (!slot) fail('UNKNOWN_NETWORK_SLOT');
    return { task, slot, businessOwnership };
  }

  async registerSlot(nodeId, slot, rotaWorkerId) {
    if (![slot, rotaWorkerId].every(value => typeof value === 'string' && /^[a-zA-Z0-9_.:-]{1,100}$/.test(value)) || slot.includes(':')) {
      throw new TypeError('valid registered slot and worker ID required');
    }
    await this.store.transaction(async client => {
      await client.query('UPDATE remote_ingestion.nodes SET slot_claims_required=true WHERE node_id=$1', [nodeId]);
      await client.query(`INSERT INTO remote_ingestion.network_slots(node_id,slot,rota_worker_id) VALUES($1,$2,$3)
        ON CONFLICT(node_id,slot) DO NOTHING`, [uuid(nodeId), slot, rotaWorkerId]);
      const row = (await client.query('SELECT rota_worker_id FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2', [nodeId, slot])).rows[0];
      if (row.rota_worker_id !== rotaWorkerId) fail('NETWORK_SLOT_CONFLICT');
    });
  }

  async bind(nodeId, lease, slotName, rotaFence, expectedIdentity = null, { youtubeSessionRequired = false } = {}) {
    const fence = Object.fromEntries(fenceFields.map(field => [field, rotaFence[field]]));
    const before = await this.store.transaction(client => this.owned(client, nodeId, lease, slotName));
    if (fence.worker_id !== before.slot.rota_worker_id || fence.business_run_id !== `incremental:${planFromTask(before.task).plan_id}`) fail('ROTA_TASK_BINDING_MISMATCH');
    // Never hold a SQL transaction across Rota HTTP.
    const source = await this.readRotaRoute(fence);
    if (!fenceFields.every(field => source[field] === fence[field])) fail('ROTA_TASK_BINDING_MISMATCH');
    const matchesBusiness = ownership => !ownership?.rotaTask
      || Object.entries(ownership.rotaTask).every(([key, value]) => source[key] === value);
    if (!matchesBusiness(before.businessOwnership)) fail('ROTA_TASK_BINDING_MISMATCH');
    if (expectedIdentity && Object.entries(expectedIdentity).some(([key, value]) => source[key] !== value)) fail('REMOTE_ROTA_IDENTITY_MISMATCH');
    return this.store.transaction(async client => {
      const { task, slot, businessOwnership } = await this.owned(client, nodeId, lease, slotName);
      if (!matchesBusiness(businessOwnership)) fail('ROTA_TASK_BINDING_MISMATCH');
      if (slot.rota_worker_id !== fence.worker_id) fail('NETWORK_SLOT_CONFLICT');
      const existing = (await client.query('SELECT * FROM remote_ingestion.network_bindings WHERE task_id=$1 AND generation=$2', [task.task_id, task.generation])).rows[0];
      const identity = this.identity(source); const upstreamHash = this.digest(source.upstream);
      if (existing) {
        if (existing.node_id !== nodeId || existing.slot !== slotName || !same(existing.rota_fence, fence)
          || existing.youtube_session_required !== youtubeSessionRequired
          || !same(existing.identity, identity) || existing.upstream_hash !== upstreamHash || existing.state === 'retired' || existing.stop_requested) fail('NETWORK_BINDING_CONFLICT');
        return existing;
      }
      if (slot.binding_id) {
        const old = (await client.query('SELECT state FROM remote_ingestion.network_bindings WHERE binding_id=$1', [slot.binding_id])).rows[0];
        if (old?.state !== 'retired') fail('NETWORK_SLOT_BUSY');
      }
      const id = randomUUID();
      const row = (await client.query(`INSERT INTO remote_ingestion.network_bindings
        (binding_id,node_id,slot,task_id,generation,rota_fence,identity,upstream_hash,youtube_session_required)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`, [id, nodeId, slotName, task.task_id, task.generation, fence, identity, upstreamHash, youtubeSessionRequired])).rows[0];
      await client.query(`UPDATE remote_ingestion.network_slots SET binding_id=$3,boot_id=NULL,grant_request=NULL,
        grant_cipher=NULL,grant_until=NULL WHERE node_id=$1 AND slot=$2`, [nodeId, slotName, id]);
      return row;
    });
  }

  async checkGrant(client, nodeId, request) {
    const { task, slot } = await this.owned(client, nodeId, request, request.slot);
    const binding = (await client.query(`SELECT * FROM remote_ingestion.network_bindings
      WHERE binding_id=$1 AND task_id=$2 AND generation=$3`, [slot.binding_id, task.task_id, task.generation])).rows[0];
    if (!binding) throw new RemoteProtocolError('NETWORK_NOT_BOUND', 503);
    if (binding.state === 'retired') fail('NETWORK_ALREADY_RETIRED');
    if (binding.stop_requested) fail('NETWORK_STOPPING');
    if (binding.youtube_session_required) {
      const session = (await client.query('SELECT 1 FROM remote_ingestion.youtube_sessions WHERE binding_id=$1', [binding.binding_id])).rows[0];
      if (!session) throw new RemoteProtocolError('YOUTUBE_SESSION_NOT_PREPARED', 503);
    }
    return { task, slot, binding };
  }

  async grant(nodeId, value) {
    const request = { task_id: uuid(value.task_id), generation: generation(value.generation),
      request_id: uuid(value.request_id), slot: value.slot, boot_id: value.boot_id, action: value.action };
    if (typeof request.slot !== 'string' || request.slot.length > 100 || !/^[a-f0-9]{48}$/.test(request.boot_id)
      || !['activate', 'renew'].includes(request.action)) throw new RemoteProtocolError('INVALID_ROUTE_REQUEST', 400);
    const before = await this.store.transaction(client => this.checkGrant(client, nodeId, request));
    const source = await this.readRotaRoute(before.binding.rota_fence);
    return this.store.transaction(async client => {
      const { task, slot, binding } = await this.checkGrant(client, nodeId, request);
      if (binding.binding_id !== before.binding.binding_id || !same(binding.identity, this.identity(source))
        || !fenceFields.every(field => source[field] === binding.rota_fence[field])
        || binding.upstream_hash !== this.digest(source.upstream)) fail('ROTA_ROUTE_CHANGED');
      const aad = canonicalIncrementalJson({ nodeId, binding: binding.binding_id, request });
      if (slot.grant_request?.request_id === request.request_id) {
        if (!same(slot.grant_request, request)) fail('ROUTE_REQUEST_CONFLICT');
        if (slot.grant_until.getTime() <= Date.now() + 1000) fail('ROUTE_GRANT_EXPIRED');
        return this.decrypt(slot.grant_cipher, aad);
      }
      let epoch = Number(slot.epoch);
      if (request.action === 'activate') {
        if (binding.state === 'active') fail('NETWORK_ALREADY_ACTIVE');
        epoch++;
      } else if (binding.state !== 'active' || slot.boot_id !== request.boot_id || slot.grant_until.getTime() <= Date.now()) {
        fail('NETWORK_RENEWAL_STALE');
      }
      const token = this.digest({ nodeId, slot: request.slot, epoch, boot: request.boot_id, binding: binding.binding_id });
      const authorized = { node_id: nodeId, slot: request.slot, task_id: task.task_id, generation: task.generation, epoch,
        route_id: binding.binding_id, identity_id: hash(canonicalIncrementalJson(binding.identity)),
        egress_country: source.egress_country, proxy_token: token, upstream: source.upstream,
        task_lease_until_ms: task.lease_until.getTime(), route_lease_until_ms: source.route_lease_until_ms };
      const issue = createRemoteRouteIssuer({ privateKey: this.privateKey, authorize: async () => authorized, maxTtlMs: this.grantTtlMs });
      const signed = await issue({ ...request, node_id: nodeId });
      const grant = JSON.parse(Buffer.from(signed.payload, 'base64').toString());
      await client.query(`UPDATE remote_ingestion.network_slots SET epoch=$3,boot_id=$4,grant_request=$5,grant_cipher=$6,grant_until=$7
        WHERE node_id=$1 AND slot=$2`, [nodeId, request.slot, epoch, request.boot_id, request, this.encrypt(signed, aad), new Date(grant.expires_at_ms)]);
      await client.query("UPDATE remote_ingestion.network_bindings SET state='active' WHERE binding_id=$1", [binding.binding_id]);
      return signed;
    });
  }

  // This is an acknowledgement that the node stopped network access, not a
  // channel success signal. Rota Task completion remains with the center.
  async release(nodeId, receipt) {
    uuid(receipt.task_id); generation(receipt.generation);
    if (receipt.retired !== true || receipt.in_flight !== 0 || !Number.isSafeInteger(receipt.epoch)) fail('INVALID_NETWORK_RELEASE');
    return this.store.transaction(async client => {
      const node = (await client.query('SELECT state FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE', [uuid(nodeId)])).rows[0];
      if (!node || node.state === 'disabled') throw new RemoteProtocolError('UNAUTHORIZED', 401);
      const binding = (await client.query(`SELECT * FROM remote_ingestion.network_bindings
        WHERE node_id=$1 AND slot=$2 AND task_id=$3 AND generation=$4`, [nodeId, receipt.slot, receipt.task_id, receipt.generation])).rows[0];
      if (binding?.state === 'retired' && same(binding.release_receipt, receipt)) {
        return { released: true, binding_id: binding.binding_id, task_id: receipt.task_id, generation: receipt.generation };
      }
      const slot = (await client.query('SELECT * FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2 FOR UPDATE', [nodeId, receipt.slot])).rows[0];
      if (!binding || slot?.binding_id !== binding.binding_id || slot.boot_id !== receipt.boot_id || Number(slot.epoch) !== receipt.epoch) fail('NETWORK_RELEASE_STALE');
      if (binding.state === 'retired' && !same(binding.release_receipt, receipt)) fail('NETWORK_RELEASE_CONFLICT');
      await client.query(`UPDATE remote_ingestion.network_bindings SET state='retired',release_receipt=$2,
        retired_at=COALESCE(retired_at,clock_timestamp()) WHERE binding_id=$1`, [binding.binding_id, receipt]);
      await client.query(`UPDATE remote_ingestion.network_slots SET grant_cipher=NULL,grant_request=NULL
        WHERE node_id=$1 AND slot=$2`, [nodeId, receipt.slot]);
      return { released: true, binding_id: binding.binding_id, task_id: receipt.task_id, generation: receipt.generation };
    });
  }

  async waitQuiesced(bindingId, { signal, pollMs = 100 } = {}) {
    for (;;) {
      signal?.throwIfAborted();
      const row = (await this.store.pool.query('SELECT state,release_receipt FROM remote_ingestion.network_bindings WHERE binding_id=$1', [uuid(bindingId)])).rows[0];
      if (!row) fail('NETWORK_BINDING_MISSING');
      if (row.state === 'retired' && row.release_receipt?.in_flight === 0) return { active_managed_requests: 0 };
      await delay(pollMs, null, { signal });
    }
  }

  // Used after an uncertain bind COMMIT. Take the same node lock as bind so
  // cleanup sees any committed binding before claiming there were no requests.
  async bindingForExecution(nodeId, lease, slot) {
    return this.store.transaction(async client => {
      await client.query('SELECT node_id FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE', [uuid(nodeId)]);
      return (await client.query(`SELECT * FROM remote_ingestion.network_bindings
        WHERE node_id=$1 AND slot=$2 AND task_id=$3 AND generation=$4`,
      [nodeId, slot, uuid(lease.task_id), generation(lease.generation)])).rows[0] ?? null;
    });
  }

  // Center's existing Rota adapter calls this before completing its Task. It
  // stops new authorizations, but never fabricates quiescence for an issued one.
  async requestStop(bindingId) {
    const known = (await this.store.pool.query('SELECT node_id,slot FROM remote_ingestion.network_bindings WHERE binding_id=$1', [uuid(bindingId)])).rows[0];
    if (!known) fail('NETWORK_BINDING_MISSING');
    await this.store.transaction(async client => {
      await client.query('SELECT node_id FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE', [known.node_id]);
      const slot = (await client.query('SELECT * FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2 FOR UPDATE', [known.node_id, known.slot])).rows[0];
      const binding = (await client.query('SELECT * FROM remote_ingestion.network_bindings WHERE binding_id=$1 FOR UPDATE', [bindingId])).rows[0];
      if (binding.state === 'retired') return;
      const unissued = binding.state === 'bound' && slot.binding_id === bindingId && !slot.grant_until;
      await client.query(`UPDATE remote_ingestion.network_bindings SET stop_requested=true,
        state=CASE WHEN $2 THEN 'retired' ELSE state END,
        release_receipt=CASE WHEN $2 THEN '{"stopped_before_grant":true,"in_flight":0}'::jsonb ELSE release_receipt END,
        retired_at=CASE WHEN $2 THEN clock_timestamp() ELSE retired_at END WHERE binding_id=$1`, [bindingId, unissued]);
    });
  }

  // A persisted activation request whose response never reached the executor
  // has never been applied to its relay. Cancel only that exact request.
  async abandon(nodeId, request) {
    uuid(request.task_id); generation(request.generation); uuid(request.request_id);
    if (request.action !== 'activate') fail('INVALID_NETWORK_ABANDON');
    return this.store.transaction(async client => {
      const node = (await client.query('SELECT state FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE', [uuid(nodeId)])).rows[0];
      if (!node || node.state === 'disabled') throw new RemoteProtocolError('UNAUTHORIZED', 401);
      const task = (await client.query('SELECT state,generation FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE', [request.task_id])).rows[0];
      const slot = (await client.query('SELECT * FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2 FOR UPDATE', [nodeId, request.slot])).rows[0];
      const binding = (await client.query(`SELECT * FROM remote_ingestion.network_bindings
        WHERE node_id=$1 AND slot=$2 AND task_id=$3 AND generation=$4`, [nodeId, request.slot, request.task_id, request.generation])).rows[0];
      const receipt = { ...request, never_applied: true, in_flight: 0 };
      if (!binding && slot && task && (task.generation !== request.generation || task.state !== 'leased')) {
        // Admission may be stopped before the center ever binds a network.
        // With no binding there cannot be a grant; acknowledge only the exact
        // historical owner after that execution is closed. Keep node -> task
        // -> slot locks so binding cannot race this check.
        const owner = (await client.query(`SELECT 1 FROM remote_ingestion.claims
          WHERE task_id=$1 AND generation=$2 AND node_id=$3 AND worker_slot=$4 LIMIT 1`,
        [request.task_id, request.generation, nodeId, request.slot])).rows[0];
        if (owner) return { abandoned: true };
      }
      if (binding?.state === 'retired' && binding.release_receipt?.stopped_before_grant) return { abandoned: true };
      if (binding?.state === 'retired' && same(binding.release_receipt, receipt)) return { abandoned: true };
      if (!binding || slot?.binding_id !== binding.binding_id || binding.state === 'retired'
        || (slot.grant_request && !same(slot.grant_request, request))) fail('NETWORK_ABANDON_STALE');
      await client.query(`UPDATE remote_ingestion.network_bindings SET state='retired',release_receipt=$2,retired_at=clock_timestamp()
        WHERE binding_id=$1`, [binding.binding_id, receipt]);
      await client.query('UPDATE remote_ingestion.network_slots SET grant_cipher=NULL,grant_request=NULL WHERE node_id=$1 AND slot=$2', [nodeId, request.slot]);
      return { abandoned: true };
    });
  }
}
