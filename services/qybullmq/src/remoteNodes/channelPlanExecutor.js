import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { planFromTask, assertChannelOperation } from './channelPlanContract.js';
import { channelSnapshotWire, toChannelWire } from './channelWire.js';
import { decodeResult, encodeResult, RemoteProtocolError } from './protocol.js';

// One process/session/lease owns a whole channel Plan. Commands are its internal
// network operations, never separately configured or distributed to other nodes.
export class RemoteChannelPlanExecutor {
  constructor({ client, spool, youtube, withSession, networkSession = null, pollMs = 100, timeoutMs = 15 * 60 * 1000 }) {
    if (networkSession) withSession = (...args) => networkSession.run(...args);
    if (typeof withSession !== 'function' || typeof youtube?.openChannel !== 'function'
      || typeof youtube?.fetchDetail !== 'function') throw new TypeError('local managed YouTubeJS session required');
    Object.assign(this, { client, spool, youtube, withSession, networkSession, pollMs, timeoutMs });
    this.stopping = false;
    this.busy = false;
  }

  stop() { this.stopping = true; }

  async flush() {
    const pending = await this.spool.read('pending.json');
    if (!pending) return false;
    const payload = Buffer.from(pending.payload, 'base64');
    const { value } = await decodeResult(payload);
    try {
      const ack = await this.client.uploadCommand({ task_id: pending.task_id, generation: value.generation }, payload);
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

  async runOnce() {
    if (this.busy) throw new Error('executor already running');
    this.busy = true;
    try {
      await this.spool.init();
      let recoveryError;
      try { await this.networkSession?.recover(); } catch (error) { recoveryError = error; }
      // Receipt replay must remain possible while a retired session waits for
      // the center to close its task. Never let that wait strand durable data.
      await this.flush();
      if (recoveryError) throw recoveryError;
      if (!(await this.spool.writable())) return 'blocked';
      let claim = await this.spool.read('claim.json');
      if (!claim && this.stopping) return 'stopped';
      if (claim?.lease) {
        try {
          const previous = await this.client.pollCommands(claim.lease);
          if (previous.status !== 'leased') {
            await this.spool.remove('claim.json');
            return previous.status === 'received' ? 'waiting_central' : previous.status;
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
      try { lease = await this.client.claim(claim.claim_id, this.networkSession?.slot ?? null); }
      catch (error) {
        if (error.code === 'CLAIM_EXPIRED') await this.spool.remove('claim.json');
        throw error;
      }
      if (!lease) { await this.spool.remove('claim.json'); return 'idle'; }
      if (lease.worker_slot && lease.worker_slot !== this.networkSession?.slot) throw new RemoteProtocolError('WORKER_SLOT_MISMATCH');
      const plan = planFromTask(lease);
      await this.spool.save('claim.json', Buffer.from(JSON.stringify({ ...claim, lease })));
      await this.client.heartbeat(lease);
      const abort = new AbortController();
      const signal = AbortSignal.any([abort.signal, AbortSignal.timeout(this.timeoutMs)]);
      let done = false; let timer; let renewal = Promise.resolve();
      const schedule = () => {
        timer = setTimeout(() => {
          renewal = this.client.heartbeat(lease)
            .then(() => { if (!done) schedule(); })
            .catch((error) => abort.abort(error));
        }, lease.heartbeat_ms);
      };
      schedule();
      try {
        return await this.withSession(lease, { signal }, async (network = {}) => {
          const executionSignal = network.signal ?? signal;
          let snapshot = null;
          let openOptions = null;
          const open = async (options) => {
            if (!snapshot) {
              openOptions = options;
              snapshot = await this.youtube.openChannel(plan.channel_id, { ...options, signal: executionSignal });
            }
            if (openOptions.includeAbout !== options.includeAbout) throw new RemoteProtocolError('CHANNEL_SESSION_CONFLICT');
            return snapshot;
          };
          for (;;) {
            executionSignal.throwIfAborted();
            await this.flush();
            const { status, commands } = await this.client.pollCommands(lease);
            if (status !== 'leased') {
              await this.spool.remove('claim.json');
              return status === 'received' ? 'waiting_central' : status;
            }
            for (const command of commands) {
              executionSignal.throwIfAborted();
              let result;
              try {
                assertChannelOperation(plan, command.operation, command.input);
                const input = command.input;
                let data;
                if (command.operation === 'open_channel') data = channelSnapshotWire(await open(input.options));
                else if (command.operation === 'scan_uploads') data = toChannelWire(await (await open(input.channel_options)).scanUploads(input.options));
                else data = toChannelWire(await this.youtube.fetchDetail(input.video_id, { ...input.options, signal: executionSignal }));
                result = { outcome: 'success', data };
              } catch (error) {
                executionSignal.throwIfAborted();
                result = { outcome: 'failure', error: { ...toChannelWire(error), code: error.code || error.name || 'EXTRACTION_FAILED' } };
              }
              const envelope = { version: 1, generation: lease.generation, batch_id: randomUUID(), command_id: command.command_id };
              let bytes;
              try { bytes = await encodeResult({ ...envelope, ...result }); }
              catch (error) {
                if (error.code !== 'RESULT_TOO_LARGE') throw error;
                bytes = await encodeResult({ ...envelope, outcome: 'failure',
                  error: { ...toChannelWire(error), code: error.code } });
              }
              await this.spool.save('pending.json', Buffer.from(JSON.stringify({ task_id: lease.task_id, payload: bytes.toString('base64') })));
              await this.flush();
            }
            if (!commands.length) await delay(this.pollMs, null, { signal: executionSignal });
          }
        });
      } finally {
        done = true; clearTimeout(timer); await renewal;
      }
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
      } catch (error) {
        failures++;
        onStatus({ status: 'retrying', code: error.code || 'TRANSPORT_ERROR' });
        // Losing one execution does not stop the Worker. The next runOnce
        // retires/replays its durable network state and clears an expired
        // claim before considering new work. Unacknowledged results still
        // block intake through flush/spool; no stale write is accepted here.
        if ([400, 401, 403, 404, 413, 415].includes(error.status) || error.code === 'BATCH_CONFLICT') return;
      }
      if (stoppedAt && Date.now() - stoppedAt >= shutdownUploadMs) return;
      await delay(Math.min(30000, pollMs * 2 ** Math.min(failures, 4)));
    }
  }
}
