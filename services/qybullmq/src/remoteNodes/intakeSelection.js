import { RemoteProtocolError } from './protocol.js';

export function selectIntakeWorkers(rows, count) {
  if (!Number.isInteger(count) || count < 0 || count > rows.length) throw new RemoteProtocolError('INVALID_EXECUTION_COUNT', 400);
  return [...rows].sort((a, b) => Number(b.activation_requested) - Number(a.activation_requested)
    || Number(b.alive) - Number(a.alive)
    || String(a.slot).localeCompare(String(b.slot), 'en', { numeric: true })).slice(0, count);
}

export function intakeStatus(workers) {
  const allowed = workers.filter(w => w.requested).length;
  const draining = workers.filter(w => !w.requested && (w.active || w.enabled)).length;
  return {
    allowedCount: allowed, requested: allowed > 0, draining: draining > 0,
    counts: { deployed: workers.length, connected: workers.filter(w => w.connected).length,
      allowed, ready: workers.filter(w => w.readyForTasks).length,
      collecting:workers.filter(w=>w.processing??w.active).length,awaiting:workers.filter(w=>w.awaitingRecovery).length,
      finishing:workers.filter(w=>!w.requested && !w.awaitingRecovery && (w.processing||w.enabled)).length,
      active: workers.filter(w => w.active).length, running: workers.filter(w => w.active && w.requested).length,
      draining, idle: workers.filter(w => w.readyForTasks && !w.active).length,
      standby: workers.filter(w => w.connected && !w.requested && !w.active && !w.enabled).length },
  };
}
