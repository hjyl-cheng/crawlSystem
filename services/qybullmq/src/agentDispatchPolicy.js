function positiveInt(value, fallback = 1) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(1, Math.min(100, Math.floor(parsed)));
}

export function agentConfigWorkerLimit(config) {
  return positiveInt(config?.max_workers, 1);
}

export function agentConcurrencyLimit(configs = [], registeredWorkers = 0) {
  const workers = Math.max(0, Math.floor(Number(registeredWorkers) || 0));
  const configured = configs
    .filter((config) => config?.enabled !== false)
    .reduce((total, config) => total + agentConfigWorkerLimit(config), 0);
  return Math.min(workers, configured);
}

export function buildAgentDispatchPlan({
  configs = [],
  outstandingByConfig = new Map(),
  outstandingTotal = 0,
  workerCapacity = 0,
  maxBatches = 100,
} = {}) {
  const remainingWorkers = Math.max(0, Math.floor(Number(workerCapacity) || 0) - Math.max(0, outstandingTotal));
  const limit = Math.min(remainingWorkers, positiveInt(maxBatches, 100));
  const states = configs
    .filter((config) => config?.enabled !== false)
    .map((config) => {
      const configId = Number(config.config_id);
      const capacity = agentConfigWorkerLimit(config);
      const outstanding = Math.max(0, Number(outstandingByConfig.get(configId) ?? 0));
      return { config, configId, capacity, outstanding, allocated: 0 };
    })
    .filter((state) => Number.isFinite(state.configId) && state.configId > 0 && state.outstanding < state.capacity);

  const plan = [];
  while (plan.length < limit) {
    const available = states
      .filter((state) => state.outstanding + state.allocated < state.capacity)
      .sort((left, right) => {
        const leftUse = (left.outstanding + left.allocated) / left.capacity;
        const rightUse = (right.outstanding + right.allocated) / right.capacity;
        return leftUse - rightUse || right.capacity - left.capacity || left.configId - right.configId;
      });
    if (available.length === 0) break;
    available[0].allocated += 1;
    plan.push(available[0].config);
  }
  return plan;
}
