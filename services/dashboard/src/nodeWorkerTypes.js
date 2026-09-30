// Shared by registration, deployment validation and the browser. Adding a type
// makes it selectable; deployment still requires its own implemented executor.
export const nodeWorkerTypes = Object.freeze({
  incremental: Object.freeze({ label: '增量抓取', queue: 'youtube-channel-incremental', selectable: true, deployable: true,
    mode: 'incremental_collect', slotPrefix: 'incremental' }),
  fullcrawl: Object.freeze({ label: '全量抓取', queue: 'youtube-channel-crawl', selectable: true, deployable: true,
    mode: 'full_crawl_collect', slotPrefix: 'full-crawl' }),
  discover: Object.freeze({ label: 'Query / 发现', queue: 'youtube-discover-page', selectable: true, deployable: true,
    mode: 'discover_collect', slotPrefix: 'discover' }),
  query_quality: Object.freeze({ label: 'Query 质量评估', queue: 'youtube-query-quality', selectable: false, deployable: false }),
});

export const workerSlotPattern = /^(?:incremental|full-crawl|discover)-[1-9][0-9]*$/;

export function nodeWorkerRoleForMode(mode) {
  return Object.keys(nodeWorkerTypes).find(role => nodeWorkerTypes[role].mode === mode) ?? null;
}

// Every Worker of this deployment was removed. Its identity, credentials and
// history remain, but it no longer fixes the node's function type.
export function deploymentRetired(deployment) {
  return deployment?.state === 'connected' && deployment.appliedCount === 0 && deployment.desiredCount === 0
    && (deployment.slots ?? []).length === 0;
}

export function nodeWorkerRole(node) {
  if (node?.workerRole !== undefined) return node.workerRole;
  // Existing deployments use the incremental runtime. Preserve old saved plans
  // when they name a single role; empty registrations default to incremental.
  if (node?.deployment?.mode && node.deployment.mode !== 'incremental_collect'
    && nodeWorkerRoleForMode(node.deployment.mode)) return nodeWorkerRoleForMode(node.deployment.mode);
  if (node?.deployment || node?.localIntake) return 'incremental';
  return node?.workers?.length === 1 ? node.workers[0].role : 'incremental';
}

export function nodeWorkerType(node) {
  const role = nodeWorkerRole(node);
  return Object.hasOwn(nodeWorkerTypes, role) ? nodeWorkerTypes[role] : null;
}

export function workerTypeDescription(type) {
  if (!type) return 'Worker 功能类型无效';
  return type.deployable
    ? `任务队列：${type.queue}。完成初始化和运行环境准备后可部署。`
    : `任务队列：${type.queue}。可保存类型并初始化服务器，远程 Worker 部署暂未开放。`;
}

export function assertNodeWorkerDeployment(node, role = nodeWorkerRole(node)) {
  const type = nodeWorkerType(node);
  if (type && !type.deployable) {
    throw Object.assign(new Error(`${type.label}（${type.queue}）的远程部署暂未开放`), { statusCode: 409 });
  }
  if (!type || role !== nodeWorkerRole(node)) {
    throw Object.assign(new Error('Worker 类型与服务器登记的功能类型不一致'), { statusCode: 409 });
  }
}
