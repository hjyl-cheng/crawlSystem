import { randomUUID } from 'node:crypto';
import { uptime } from 'node:os';
import { setTimeout as delay } from 'node:timers/promises';

// The first deployment stage verifies real processes and center connectivity.
// It never calls claim(), even if a misconfigured center advertises readiness.
export class RemoteWorkerConnection {
  constructor({ config, client, localRota, report = async () => {}, intervalMs = 10000, now = Date.now }) {
    if (config.mode !== 'connect_only' || !Number.isInteger(intervalMs) || intervalMs < 50) throw new TypeError('connection-only runtime required');
    Object.assign(this, { config, client, localRota, report, intervalMs, now });
    this.instanceId = randomUUID();
  }

  async probe() {
    const started = this.now(); const startedUptime = uptime();
    const boot = await this.localRota.boot();
    const value = { version: 1, mode: 'connect_only', node_id: this.config.node_id, slot: this.config.slot,
      deployment_id: this.config.deployment_id, config_hash: this.config.config_hash, instance_id: this.instanceId, relay_boot_id: boot.boot_id };
    const ack = await this.client.workerHeartbeat(value);
    if (Object.keys(value).some(key => ack?.[key] !== value[key]) || ack.state !== 'connected_waiting_activation'
      || ack.ready_for_tasks !== false) throw new Error('NODE_CONNECTION_RECEIPT_MISMATCH');
    const serverTime = Date.parse(ack.server_time); const remaining = Date.parse(ack.connected_until) - serverTime;
    if (!Number.isFinite(serverTime) || serverTime < started - 5000 || serverTime > this.now() + 5000
      || !Number.isFinite(remaining) || remaining < 1000 || remaining > 120000
      || startedUptime + remaining / 1000 <= uptime()) throw new Error('NODE_CONNECTION_CLOCK_OR_LEASE_INVALID');
    return { version: 1, node_id: value.node_id, slot: value.slot, deployment_id: value.deployment_id,
      instance_id: value.instance_id, relay_boot_id: value.relay_boot_id,
      state: 'connected_waiting_activation', ready_for_tasks: false, valid_until_uptime: startedUptime + remaining / 1000 };
  }

  async run({ signal }) {
    let failures = 0;
    try {
      while (!signal.aborted) {
        try {
          const result = await this.probe();
          signal.throwIfAborted();
          await this.report(result);
          failures = 0;
        } catch (error) {
          if (signal.aborted) break;
          await this.report({ version: 1, state: 'disconnected', ready_for_tasks: false, code: 'NODE_CONNECTION_UNAVAILABLE', valid_until_uptime: 0 });
          failures++;
          if ([400, 401, 403, 404].includes(error.status) || error.message === 'NODE_CONNECTION_RECEIPT_MISMATCH') throw new Error('NODE_CONNECTION_REJECTED');
        }
        await delay(Math.min(15000, this.intervalMs * 2 ** Math.min(failures, 3)), null, { signal }).catch(error => { if (!signal.aborted) throw error; });
      }
    } finally {
      await this.report({ version: 1, state: 'stopped', ready_for_tasks: false, valid_until_uptime: 0 });
    }
  }
}
