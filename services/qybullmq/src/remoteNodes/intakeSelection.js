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
  const phases={collecting:0,processing:0,awaiting:0,unready:0,idle:0,finishing:0,standby:0,offline:0};
  for(const worker of workers){
    const processing=worker.processing??worker.active;
    const phase=!worker.requested && (processing||worker.enabled) && !worker.awaitingRecovery ? 'finishing'
      : processing ? (worker.executionPhase && worker.executionPhase!=='collecting' ? 'processing' : 'collecting')
      : worker.awaitingRecovery || worker.active ? 'awaiting'
      : !worker.connected ? 'offline'
      : worker.readyForTasks ? 'idle'
      : !worker.requested && !worker.enabled ? 'standby' : 'unready';
    phases[phase]++;
  }
  return {
    allowedCount: allowed, requested: allowed > 0, draining: draining > 0,
    counts: { deployed: workers.length, connected: workers.filter(w => w.connected).length,
      allowed, ready: workers.filter(w => w.readyForTasks).length,
      active: workers.filter(w => w.active).length, running: workers.filter(w => w.active && w.requested).length,
      draining, ...phases },
  };
}
