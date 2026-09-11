import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { decodeResult, encodeResult, RemoteProtocolError } from './protocol.js';

export class RemoteNodeExecutor {
  constructor({ client, spool, execute, executionTimeoutMs = 10 * 60 * 1000 }) {
    Object.assign(this, { client, spool, execute, executionTimeoutMs });
    this.stopping = false;
    this.busy = false;
  }

  stop() { this.stopping = true; }

  async flush() {
    const pending = await this.spool.read('pending.json');
    if (!pending) return false;
    const bytes = Buffer.from(pending.payload, 'base64');
    const { value } = await decodeResult(bytes);
    try {
      const ack = await this.client.upload(pending.task_id, bytes);
      if (ack.durable !== true || ack.batch_id !== value.batch_id
        || !['received', 'applied', 'failed', 'cancelled'].includes(ack.status)) {
        throw new RemoteProtocolError('INVALID_RECEIPT', 502);
      }
    } catch (error) {
      if ([400, 401, 403, 404, 409, 413, 415].includes(error.status)) {
        await this.spool.block();
        await this.spool.remove('claim.json');
      }
      throw error;
    }
    // Remove claim first: a crash between deletes still retries the durable result.
    await this.spool.remove('claim.json');
    await this.spool.remove('pending.json');
    return true;
  }

  async runOnce() {
    if (this.busy) throw new Error('executor already running');
    this.busy = true;
    try {
      await this.spool.init();
      if (await this.flush()) return 'uploaded';
      if (this.stopping) return 'stopped';
      if (!(await this.spool.writable())) return 'blocked';
      let claim = await this.spool.read('claim.json');
      if (!claim) {
        claim = { claim_id: randomUUID() };
        await this.spool.save('claim.json', Buffer.from(JSON.stringify(claim)));
      }
      let lease;
      try { lease = await this.client.claim(claim.claim_id); }
      catch (error) {
        if (error.code === 'CLAIM_EXPIRED') await this.spool.remove('claim.json');
        throw error;
      }
      if (!lease) { await this.spool.remove('claim.json'); return 'idle'; }
      // A replayed claim may have very little lease time left. Renew before doing I/O.
      await this.client.heartbeat(lease);
      const abort = new AbortController();
      const signal = AbortSignal.any([abort.signal, AbortSignal.timeout(this.executionTimeoutMs)]);
      let timer;
      let renewing = Promise.resolve();
      let finished = false;
      const schedule = () => {
        timer = setTimeout(() => {
          renewing = this.client.heartbeat(lease)
            .then(() => { if (!finished) schedule(); })
            .catch(() => abort.abort(new Error('LEASE_RENEWAL_FAILED')));
        }, lease.heartbeat_ms);
      };
      schedule();
      let result;
      try {
        const data = await this.execute(lease, { signal });
        signal.throwIfAborted();
        result = { outcome: 'success', data };
      } catch (error) {
        result = { outcome: 'failure', error: { code: String(error.code || error.name || 'EXTRACTION_FAILED').slice(0, 200) } };
      } finally {
        finished = true;
        clearTimeout(timer);
        await renewing;
      }
      const envelope = { version: 1, batch_id: randomUUID(), generation: lease.generation };
      let payload;
      try { payload = await encodeResult({ ...envelope, ...result }); }
      catch (error) {
        if (error.code !== 'RESULT_TOO_LARGE') throw error;
        payload = await encodeResult({ ...envelope, outcome: 'failure', error: { code: 'RESULT_TOO_LARGE' } });
      }
      await this.spool.save('pending.json', Buffer.from(JSON.stringify({ task_id: lease.task_id, payload: payload.toString('base64') })));
      await this.flush();
      return 'uploaded';
    } finally { this.busy = false; }
  }

  async run({ pollMs = 2000, shutdownUploadMs = 30000, onStatus = () => {} } = {}) {
    let stoppedAt = null;
    let failures = 0;
    while (true) {
      if (this.stopping) stoppedAt ??= Date.now();
      try {
        const status = await this.runOnce();
        onStatus({ status });
        failures = 0;
        if (status === 'stopped' || status === 'blocked' || (this.stopping && status === 'uploaded')) return;
      } catch (error) {
        failures++;
        onStatus({ status: 'retrying', code: error.code || 'TRANSPORT_ERROR' });
        if ([400, 401, 403, 404, 413, 415].includes(error.status) || error.code === 'BATCH_CONFLICT' || error.code === 'STALE_LEASE') return;
      }
      if (stoppedAt && Date.now() - stoppedAt >= shutdownUploadMs) return;
      await delay(Math.min(30000, pollMs * 2 ** Math.min(failures, 4)));
    }
  }
}
