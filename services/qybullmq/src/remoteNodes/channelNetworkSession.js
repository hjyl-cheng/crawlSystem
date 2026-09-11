import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { withUploadsCountryExecution } from '../youtubeUploadsCountry.js';

const closedExecution = () => Object.assign(new Error('network execution already closed; await central recovery'), { code: 'NETWORK_EXECUTION_CLOSED' });

// One local slot belongs to one executor/spool. withRuntime must run the real
// YouTubeJS identity/fingerprint session with the supplied proxy URL and signal.
// There is no direct-network default. Database/Rota-admin credentials stay out.
export class RemoteChannelNetworkSession {
  constructor({ client, localRota, spool, slot, withRuntime, renewMs = 5000, retryMs = 250, startTimeoutMs = 30000 }) {
    if (typeof withRuntime !== 'function' || !slot || !Number.isSafeInteger(renewMs) || renewMs < 50) {
      throw new TypeError('local managed runtime and renewal interval required');
    }
    Object.assign(this, { client, localRota, spool, slot, withRuntime, renewMs, retryMs, startTimeoutMs });
    this.busy = false;
  }

  async save(state) { await this.spool.save('network.json', Buffer.from(JSON.stringify(state))); }

  // Replayed before claiming another channel. A lost release response retries
  // only that acknowledgement; it does not collect again or retire a newer task.
  async recover() {
    await this.withRuntime.recover?.();
    const state = await this.spool.read('network.json');
    if (!state) return;
    if (state.phase === 'closed') {
      try { if ((await this.client.pollCommands(state.lease)).status === 'leased') throw closedExecution(); }
      catch (error) { if (error.code !== 'STALE_LEASE') throw error; }
      await this.spool.remove('network.json');
      return;
    }
    // runOnce calls recovery only outside an executing session. An active
    // spool here belongs to a dead/interrupted browser process. A live task
    // lease cannot restore its lost in-memory identity or request budgets.
    if (state.phase !== 'release' && !state.cleanup_required) {
      state.cleanup_required = true;
      state.interrupted = true;
      await this.save(state);
    }
    await this.cleanup(state);
    if (state.interrupted) await this.recover();
  }

  async finishCleanup(state) {
    if (state.interrupted) await this.save({ phase: 'closed', lease: state.lease });
    else await this.spool.remove('network.json');
  }

  async cleanup(state) {
    if (!state.grant && !state.applied && !state.receipt) {
      const ack = await this.client.abandonRoute(state.request);
      if (!ack.abandoned) throw new Error('INVALID_NETWORK_RELEASE_RECEIPT');
      await this.finishCleanup(state);
      return;
    }
    if (!state.receipt) {
      const grant = state.applied ?? JSON.parse(Buffer.from(state.grant.payload, 'base64').toString());
      // Local retirement needs no center connection and waits for in-flight
      // handshakes/tunnels to stop, including when the original grant expired.
      state.receipt = await this.localRota.retire({ boot_id: state.boot_id, slot: this.slot,
        epoch: grant.epoch, task_id: state.lease.task_id, generation: state.lease.generation });
      state.phase = 'release';
      delete state.grant;
      await this.save(state);
    }
    const ack = await this.client.releaseRoute(state.receipt);
    if (!ack.released || ack.task_id !== state.lease.task_id || ack.generation !== state.lease.generation) throw new Error('INVALID_NETWORK_RELEASE_RECEIPT');
    await this.finishCleanup(state);
  }

  async run(lease, { signal }, invoke) {
    if (this.busy) throw new Error('network session already running');
    this.busy = true;
    const abort = new AbortController();
    const activeSignal = AbortSignal.any([signal, abort.signal]);
    let state; let timer; let expiry; let renewing = Promise.resolve(); let done = false; let finished = false;
    try {
      await this.spool.init();
      state = await this.spool.read('network.json');
      if (state?.phase === 'closed') throw closedExecution();
      if (state && (state.lease.task_id !== lease.task_id || state.lease.generation !== lease.generation || state.phase === 'release' || state.cleanup_required)) {
        await this.cleanup(state);
        if (state.interrupted) throw closedExecution();
        state = null;
      }
      const boot = await this.localRota.boot();
      if (state && state.boot_id !== boot.boot_id) {
        await this.cleanup(state);
        throw new Error('NETWORK_BOOT_CHANGED'); // center resumes the Plan with a new execution generation
      }
      if (!state) {
        state = { phase: 'activate', lease: { task_id: lease.task_id, generation: lease.generation }, boot_id: boot.boot_id,
          request: { task_id: lease.task_id, generation: lease.generation, slot: this.slot, boot_id: boot.boot_id,
            request_id: randomUUID(), action: 'activate' } };
        await this.save(state);
      }
      const apply = async () => {
        if (!state.grant) {
          state.grant = await this.client.grantRoute(state.request);
          await this.save(state); // response durability before activating proxy
        }
        const ack = await this.localRota.apply(state.grant, { lease, slot: this.slot, bootId: state.boot_id });
        const prior = state.applied;
        if (prior && ['epoch', 'route_id', 'identity_id', 'egress_country'].some(field => prior[field] !== ack[field])) throw new Error('NETWORK_SESSION_IDENTITY_CHANGED');
        // Keep proxy credentials only in the pending grant, not in metadata.
        const { proxyUrl, ...metadata } = ack;
        state.applied = metadata; state.phase = 'active';
        delete state.grant;
        await this.save(state);
        clearTimeout(expiry);
        const remaining = ack.expires_at_ms - Date.now() - 500;
        if (remaining <= 0) throw new Error('NETWORK_AUTHORIZATION_EXPIRED');
        expiry = setTimeout(() => abort.abort(new Error('NETWORK_AUTHORIZATION_EXPIRED')), remaining);
        return { ...metadata, proxyUrl };
      };
      const startSignal = AbortSignal.any([activeSignal, AbortSignal.timeout(this.startTimeoutMs)]);
      let route;
      for (;;) {
        startSignal.throwIfAborted();
        try {
          // After a process restart, reclaim the persisted activation response
          // first. A successful prior activation must still have its exact grant.
          route = await apply(); break;
        } catch (error) {
          if (error.status !== 503 && !['NETWORK_NOT_BOUND', 'GATEWAY_UNAVAILABLE'].includes(error.code)) throw error;
          await delay(this.retryMs, null, { signal: startSignal });
        }
      }
      const schedule = () => {
        timer = setTimeout(() => {
          renewing = (async () => {
            state.request = { ...state.request, action: 'renew', request_id: randomUUID() };
            delete state.grant;
            await this.save(state);
            await apply();
          })().then(() => { if (!done) schedule(); }).catch(async error => {
            if (['STALE_LEASE', 'NETWORK_STOPPING', 'NETWORK_ALREADY_RETIRED'].includes(error.code)) {
              try { if ((await this.client.pollCommands(lease)).status !== 'leased') return; } catch {}
            }
            abort.abort(error);
          });
        }, Math.min(this.renewMs, Math.max(50, (state.applied.expires_at_ms - Date.now()) / 3)));
      };
      schedule();
      const result = await withUploadsCountryExecution({ egressCountry: route.egress_country || null },
        () => this.withRuntime({ lease, ...route, slot: this.slot, bootId: boot.boot_id, signal: activeSignal }, () => invoke({ signal: activeSignal })));
      activeSignal.throwIfAborted();
      finished = true;
      return result;
    } finally {
      done = true; clearTimeout(timer); clearTimeout(expiry);
      await renewing;
      clearTimeout(expiry);
      try {
        if (state && state.phase !== 'closed') {
          state.cleanup_required = true;
          state.interrupted ||= !finished;
          await this.save(state);
          await this.cleanup(state);
        }
      } finally { this.busy = false; }
    }
  }
}
