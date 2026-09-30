import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { ProxyAgent } from 'undici';
import { ManagedRequestTracker } from '../executionRuntimeSupport.js';
import { currentProxyIdentity, runWithProxyIdentity } from '../proxyIdentity.js';
import { requestDiscoverSearchPage } from '../youtube.js';
import { RemoteChannelNetworkSession } from './channelNetworkSession.js';
import { toChannelWire } from './channelWire.js';
import { DISCOVER_WORKLOAD } from './collectingWorkload.js';
import { DISCOVER_PAGE_CAPABILITY, DISCOVER_PAGE_OPERATION, assertDiscoverRequest } from './discoverPageContract.js';
import { checkNodeIncrementalHealth, RemoteIncrementalProcess } from './nodeIncrementalRuntime.js';
import { runNodeProcess } from './nodeConnectionRuntime.js';
import { decodeResult, encodeResult, RemoteProtocolError } from './protocol.js';
import { RemoteResultSpool } from './spool.js';
import { loadWorkerFiles } from './workerConfig.js';

// Runs requests only through the route the center granted for this task.
// There is no direct-network fallback: without a route, no request is sent.
export function createDiscoverRouteRuntime({ createDispatcher = url => new ProxyAgent(url) } = {}) {
  const withRuntime = async (context, run) => {
    if (typeof context?.proxyUrl !== 'string' || !context.proxyUrl) throw new Error('DISCOVER_ROUTE_REQUIRED');
    const dispatcher = createDispatcher(context.proxyUrl);
    const tracker = new ManagedRequestTracker();
    try {
      return await runWithProxyIdentity({ proxy_url: context.proxyUrl, dispatcher, abort_signal: context.signal,
        managed_request_tracker: tracker, slot_name: context.slot }, run);
    } finally {
      await tracker.quiesce();
      try { await dispatcher.close?.(); } catch { dispatcher.destroy?.(); }
    }
  };
  // Discover keeps no browser or profile state across a process restart.
  withRuntime.recover = async () => {};
  return withRuntime;
}

// The same request the local worker sends, inside the route's proxy identity.
export async function executeDiscoverCommand(command, { request = requestDiscoverSearchPage } = {}) {
  if (command?.operation !== DISCOVER_PAGE_OPERATION) throw new RemoteProtocolError('UNKNOWN_DISCOVER_OPERATION', 400);
  const input = assertDiscoverRequest(command.input);
  if (!currentProxyIdentity().dispatcher) throw new Error('DISCOVER_ROUTE_REQUIRED');
  return request(input);
}

// One process/slot/spool, one page at a time. A result is fsynced to the
// spool before upload and replayed until the center acknowledges it.
export class RemoteDiscoverExecutor {
  constructor({ client, spool, networkSession, pollMs = 100, timeoutMs = 5 * 60 * 1000, execute = executeDiscoverCommand }) {
    Object.assign(this, { client, spool, networkSession, pollMs, timeoutMs, execute });
    this.busy = false; this.stopping = false; this.intakeReady = true;
  }

  stop() { this.stopping = true; }

  async flush() {
    const pending = await this.spool.read('pending.json');
    if (!pending) return false;
    const payload = Buffer.from(pending.payload, 'base64');
    const { value } = await decodeResult(payload);
    try {
      const ack = await this.client.uploadDiscover({ task_id: pending.task_id, generation: value.generation }, payload);
      if (!ack.durable || ack.batch_id !== value.batch_id || ack.command_id !== value.command_id || ack.status !== 'received') {
        throw new RemoteProtocolError('INVALID_RECEIPT', 502);
      }
    } catch (error) {
      if (error.code === 'STALE_LEASE' && error.status === 409) {
        await this.spool.archiveStaleResult();
        return true;
      }
      if ([400, 401, 403, 404, 409, 413, 415].includes(error.status)) await this.spool.block();
      throw error;
    }
    await this.spool.remove('pending.json');
    return true;
  }

  async result(lease, command, executionSignal) {
    let result;
    try {
      result = { outcome: 'success', data: await this.execute(command) };
    } catch (error) {
      executionSignal.throwIfAborted();
      result = { outcome: 'failure', error: { ...toChannelWire(error), code: error.code || error.name || 'DISCOVER_REQUEST_FAILED' } };
    }
    const envelope = { version: 1, generation: lease.generation, batch_id: randomUUID(), command_id: command.command_id };
    try { return await encodeResult({ ...envelope, ...result }); }
    catch (error) {
      if (error.code !== 'RESULT_TOO_LARGE') throw error;
      return encodeResult({ ...envelope, outcome: 'failure', error: { ...toChannelWire(error), code: error.code } });
    }
  }

  async runOnce() {
    if (this.busy) throw new Error('executor already running');
    this.busy = true;
    try {
      await this.spool.init();
      let recoveryError;
      try { await this.networkSession.recover(); } catch (error) { recoveryError = error; }
      // An unacknowledged result is replayed before anything else.
      await this.flush();
      if (recoveryError) throw recoveryError;
      if (!(await this.spool.writable())) { this.intakeReady = false; return 'blocked'; }
      this.intakeReady = true;
      let claim = await this.spool.read('claim.json');
      if (!claim && this.stopping) return 'stopped';
      if (claim?.lease) {
        try {
          const previous = await this.client.discoverPoll(claim.lease);
          if (previous.status !== 'leased') {
            await this.spool.remove('claim.json');
            return previous.status;
          }
        } catch (error) {
          if (error.code !== 'STALE_LEASE') throw error;
          await this.spool.remove('claim.json');
          return 'expired';
        }
      }
      if (!claim) {
        claim = { claim_id: randomUUID() };
        await this.spool.save('claim.json', Buffer.from(JSON.stringify(claim)));
      }
      let lease;
      try { lease = await this.client.claim(claim.claim_id, this.networkSession.slot); }
      catch (error) {
        if (error.code === 'CLAIM_EXPIRED') await this.spool.remove('claim.json');
        throw error;
      }
      if (!lease) { await this.spool.remove('claim.json'); return 'idle'; }
      if (lease.worker_slot && lease.worker_slot !== this.networkSession.slot) throw new RemoteProtocolError('WORKER_SLOT_MISMATCH');
      if (lease.capability !== DISCOVER_PAGE_CAPABILITY) throw new RemoteProtocolError('DISCOVER_TASK_REQUIRED');
      await this.spool.save('claim.json', Buffer.from(JSON.stringify({ ...claim, lease })));
      await this.client.heartbeat(lease);
      const abort = new AbortController();
      const signal = AbortSignal.any([abort.signal, AbortSignal.timeout(this.timeoutMs)]);
      let done = false; let timer; let renewal = Promise.resolve();
      const schedule = () => {
        timer = setTimeout(() => {
          renewal = this.client.heartbeat(lease)
            .then(() => { if (!done) schedule(); })
            .catch(error => abort.abort(error));
        }, lease.heartbeat_ms);
      };
      schedule();
      try {
        return await this.networkSession.run(lease, { signal }, async ({ signal: executionSignal }) => {
          for (;;) {
            executionSignal.throwIfAborted();
            await this.flush();
            const { status, commands } = await this.client.discoverPoll(lease);
            if (status !== 'leased') {
              await this.spool.remove('claim.json');
              return status;
            }
            for (const command of commands) {
              executionSignal.throwIfAborted();
              const bytes = await this.result(lease, command, executionSignal);
              await this.spool.save('pending.json', Buffer.from(JSON.stringify({ task_id: lease.task_id, payload: bytes.toString('base64') })));
              await this.flush();
            }
            if (!commands.length && this.client.transport !== 'nats') await delay(this.pollMs, null, { signal: executionSignal });
          }
        });
      } finally {
        done = true; clearTimeout(timer); await renewal;
      }
    } catch (error) {
      this.intakeReady = false;
      throw error;
    } finally { this.busy = false; }
  }

  async run({ pollMs = 1000, onStatus = () => {}, shutdownUploadMs = 30000 } = {}) {
    let failures = 0; let stoppedAt = null;
    for (;;) {
      if (this.stopping) stoppedAt ??= Date.now();
      try {
        const status = await this.runOnce();
        onStatus({ status });
        failures = 0;
        if (status === 'stopped' || status === 'blocked') return;
        if (this.stopping && !(await this.spool.read('claim.json'))) return;
        if (status === 'idle') await this.client.waitForActivation?.();
      } catch (error) {
        failures++;
        onStatus({ status: 'retrying', code: error.code || 'TRANSPORT_ERROR' });
        if ([400, 401, 403, 404, 413, 415].includes(error.status) || error.code === 'BATCH_CONFLICT') return;
      }
      if (stoppedAt && Date.now() - stoppedAt >= shutdownUploadMs) return;
      await delay(Math.min(30000, pollMs * 2 ** Math.min(failures, 4)));
    }
  }
}

export function createRemoteDiscoverWorker({ client, localRota, slot, spool, renewMs = 5000, pollMs = 100,
  timeoutMs = 5 * 60 * 1000, createDispatcher }) {
  if (client.transport !== 'nats') throw new TypeError('remote Discover requires NATS transport');
  // The network session's lease checks use this workload's own command poll.
  const networkSession = new RemoteChannelNetworkSession({ client: { ...client, pollCommands: client.discoverPoll },
    localRota, slot, spool, withRuntime: createDiscoverRouteRuntime({ createDispatcher }), renewMs });
  return new RemoteDiscoverExecutor({ client, spool, networkSession, pollMs, timeoutMs });
}

export const checkNodeDiscoverHealth = checkNodeIncrementalHealth;

export function runNodeDiscover({ spoolDirectory = '/var/lib/qy-node/discover-spool', ...options } = {}) {
  return runNodeProcess({ ...options, loadFiles: files => loadWorkerFiles(files, { mode: DISCOVER_WORKLOAD.mode }),
    runWorker: ({ signal, ...args }) => new RemoteIncrementalProcess({ ...args, wholeChannel: false,
      workload: DISCOVER_WORKLOAD, createWorker: createRemoteDiscoverWorker,
      spool: new RemoteResultSpool({ directory: spoolDirectory, maxBytes: 64 * 1024 * 1024 }) }).run({ signal }) });
}
