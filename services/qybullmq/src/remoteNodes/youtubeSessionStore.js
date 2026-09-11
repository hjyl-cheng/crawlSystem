import { canonicalIncrementalJson } from '../incrementalPlan.js';
import { hash, uuid, generation, RemoteProtocolError } from './protocol.js';
import { youtubeProfileGroup, SESSION_BYTES } from './youtubeSessionContract.js';
import { remoteExecutionOptions } from './executionContext.js';

const fail = code => { throw new RemoteProtocolError(code); };
export class RemoteYoutubeSessionStore {
  constructor({ routes }) { this.routes = routes; this.store = routes.store; }

  // Central-only: the caller uses the original BrowserProfileStore and attempt
  // lifecycle. Freezing this exact profile never creates a node-local identity.
  async prepare(bindingId, { profileGroup, attemptId }) {
    if (typeof attemptId !== 'string' || !attemptId || attemptId.length > 200) throw new TypeError('central attempt ID required');
    return this.store.transaction(async client => {
      const known = (await client.query('SELECT * FROM remote_ingestion.network_bindings WHERE binding_id=$1', [uuid(bindingId)])).rows[0];
      if (!known) fail('YOUTUBE_SESSION_BINDING_CLOSED');
      const { task, businessOwnership } = await this.routes.owned(client, known.node_id, known, known.slot);
      const binding = (await client.query('SELECT * FROM remote_ingestion.network_bindings WHERE binding_id=$1 FOR UPDATE', [bindingId])).rows[0];
      if (!binding || binding.state !== 'bound' || binding.stop_requested) fail('YOUTUBE_SESSION_BINDING_CLOSED');
      if (attemptId !== task.context.execution_attempt_id) fail('YOUTUBE_SESSION_ATTEMPT_MISMATCH');
      const group = youtubeProfileGroup(profileGroup, binding.identity);
      if (businessOwnership?.executionAttemptId) {
        const attempt = (await client.query('SELECT profile_group_id,profile_revision,youtubejs_profile_id FROM crawler.channel_execution_attempts WHERE attempt_id=$1', [attemptId])).rows[0];
        if (attempt?.profile_group_id !== group.profile_group_id || Number(attempt?.profile_revision) !== group.profile_revision
          || attempt?.youtubejs_profile_id !== group.clients.youtubejs_chrome.profile_id) fail('YOUTUBE_SESSION_PROFILE_MISMATCH');
      }
      const proxy = { ...binding.identity, ...binding.rota_fence };
      const bundle = { version: 1, attempt_id: attemptId, identity: binding.identity, proxy, profile_group: group };
      if (task.context.execution_options) {
        bundle.execution_options = remoteExecutionOptions(task.context.execution_options);
        if ((binding.identity.egress_country || null) !== bundle.execution_options.egress_country) fail('REMOTE_ROTA_IDENTITY_MISMATCH');
      }
      const digest = hash(canonicalIncrementalJson(bundle));
      const aad = `youtube-session:${bindingId}`;
      await client.query(`INSERT INTO remote_ingestion.youtube_sessions(binding_id,session_hash,session_cipher)
        VALUES($1,$2,$3) ON CONFLICT(binding_id) DO NOTHING`, [bindingId, digest, this.routes.encrypt(bundle, aad)]);
      const existing = (await client.query('SELECT session_hash FROM remote_ingestion.youtube_sessions WHERE binding_id=$1', [bindingId])).rows[0];
      if (existing.session_hash !== digest) fail('YOUTUBE_SESSION_CONFLICT');
    });
  }

  async bind({ nodeId, lease, slot, rotaFence, expectedIdentity, profileGroup, attemptId }) {
    // Until the frozen browser profile is durable, route grants return 503.
    // The node can safely retry that readiness check without starting requests.
    const binding = await this.routes.bind(nodeId, lease, slot, rotaFence, expectedIdentity, { youtubeSessionRequired: true });
    await this.prepare(binding.binding_id, { profileGroup, attemptId });
    return binding;
  }

  async owned(client, nodeId, request, { finishing = false } = {}) {
    const fields = ['task_id','generation','slot','boot_id','epoch','route_id','identity_id'];
    if (!request || Object.keys(request).some(key => !fields.includes(key))) fail('INVALID_YOUTUBE_SESSION_REQUEST');
    uuid(nodeId); uuid(request.task_id); uuid(request.route_id); generation(request.generation);
    if (typeof request.slot !== 'string' || !/^[a-zA-Z0-9_.-]{1,100}$/.test(request.slot)
      || !/^[a-f0-9]{48}$/.test(request.boot_id) || !Number.isSafeInteger(request.epoch)) fail('INVALID_YOUTUBE_SESSION_REQUEST');
    const node = (await client.query('SELECT state FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE', [nodeId])).rows[0];
    if (!node || node.state === 'disabled') throw new RemoteProtocolError('UNAUTHORIZED', 401);
    const task = (await client.query('SELECT *,lease_until>clock_timestamp() AS alive FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE', [request.task_id])).rows[0];
    if (!task || task.node_id !== nodeId || task.generation !== request.generation
      || task.worker_slot !== request.slot || (!finishing && (task.state !== 'leased' || !task.alive))) fail('STALE_LEASE');
    if (!finishing) await this.routes.assertBusinessFence(client, task);
    const slot = (await client.query('SELECT *,grant_until>clock_timestamp() AS alive FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2 FOR UPDATE', [nodeId, request.slot])).rows[0];
    const binding = (await client.query('SELECT * FROM remote_ingestion.network_bindings WHERE binding_id=$1', [request.route_id])).rows[0];
    if (!slot || !binding || binding.node_id !== nodeId || binding.slot !== request.slot
      || binding.task_id !== task.task_id || binding.generation !== task.generation
      || slot.binding_id !== binding.binding_id || slot.boot_id !== request.boot_id || Number(slot.epoch) !== request.epoch
      || hash(canonicalIncrementalJson(binding.identity)) !== request.identity_id) fail('YOUTUBE_SESSION_OWNERSHIP_MISMATCH');
    // Final cookie/telemetry delivery can follow central task completion or
    // grant expiry; it grants no permission to perform another network request.
    if (!finishing && (binding.state !== 'active' || binding.stop_requested || !slot.alive)) fail('YOUTUBE_SESSION_BINDING_CLOSED');
    return binding;
  }

  async get(nodeId, request) {
    return this.store.transaction(async client => {
      const binding = await this.owned(client, nodeId, request);
      const row = (await client.query('SELECT * FROM remote_ingestion.youtube_sessions WHERE binding_id=$1', [binding.binding_id])).rows[0];
      if (!row) throw new RemoteProtocolError('YOUTUBE_SESSION_NOT_PREPARED', 503);
      if (row.checkpoint_hash) fail('YOUTUBE_SESSION_CLOSED');
      return { ...this.routes.decrypt(row.session_cipher, `youtube-session:${binding.binding_id}`), ...request };
    });
  }

  async checkpoint(nodeId, value) {
    const { request, checkpoint } = value ?? {};
    if (!checkpoint || !['success','failed','aborted'].includes(checkpoint.status)
      || checkpoint.active_managed_requests !== 0 || !checkpoint.metrics
      || (checkpoint.cookies !== null && (!Array.isArray(checkpoint.cookies?.cookies) || checkpoint.status !== 'success'))
      || Buffer.byteLength(JSON.stringify(checkpoint)) > SESSION_BYTES - 16384) fail('INVALID_YOUTUBE_CHECKPOINT');
    return this.store.transaction(async client => {
      // A byte-identical durable receipt remains acknowledgeable after the
      // center advances to another execution. It cannot authorize new writes.
      const old = (await client.query(`SELECT s.checkpoint_hash,s.checkpoint_request,b.node_id,n.state AS node_state
        FROM remote_ingestion.youtube_sessions s JOIN remote_ingestion.network_bindings b USING(binding_id)
        JOIN remote_ingestion.nodes n ON n.node_id=b.node_id WHERE s.binding_id=$1`, [uuid(request?.route_id)])).rows[0];
      const digest = hash(canonicalIncrementalJson(checkpoint));
      if (old?.node_id === nodeId && old.node_state !== 'disabled' && old.checkpoint_hash === digest
        && canonicalIncrementalJson(old.checkpoint_request) === canonicalIncrementalJson(request)) {
        return { durable: true, route_id: request.route_id, sha256: digest };
      }
      const binding = await this.owned(client, nodeId, request, { finishing: true });
      const row = (await client.query('SELECT * FROM remote_ingestion.youtube_sessions WHERE binding_id=$1 FOR UPDATE', [binding.binding_id])).rows[0];
      if (!row) fail('YOUTUBE_SESSION_NOT_PREPARED');
      if (row.checkpoint_hash && row.checkpoint_hash !== digest) fail('YOUTUBE_CHECKPOINT_CONFLICT');
      if (!row.checkpoint_hash) await client.query(`UPDATE remote_ingestion.youtube_sessions SET checkpoint_hash=$2,
        checkpoint_cipher=$3,checkpoint_request=$4,checkpoint_at=clock_timestamp() WHERE binding_id=$1`,
      [binding.binding_id, digest, this.routes.encrypt(checkpoint, `youtube-checkpoint:${binding.binding_id}`), request]);
      return { durable: true, route_id: binding.binding_id, sha256: digest };
    });
  }

  // Center consumes this only after its existing quiesce/fence checks. Receipt
  // persistence alone never updates crawler rows or declares a Plan successful.
  async result(bindingId) {
    const row = (await this.store.pool.query('SELECT * FROM remote_ingestion.youtube_sessions WHERE binding_id=$1', [uuid(bindingId)])).rows[0];
    return row?.checkpoint_cipher ? this.routes.decrypt(row.checkpoint_cipher, `youtube-checkpoint:${bindingId}`) : null;
  }
}
