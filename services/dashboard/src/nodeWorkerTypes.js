// Shared by registration, deployment validation and the browser. Adding a type
// makes it selectable; deployment still requires its own implemented executor.
export const nodeWorkerTypes = Object.freeze({
  incremental: Object.freeze({ label: '增量抓取', queue: 'youtube-channel-incremental', selectable: true, deployable: true }),
  fullcrawl: Object.freeze({ label: '全量抓取', queue: 'youtube-channel-crawl', selectable: true, deployable: false }),
  discover: Object.freeze({ label: 'Query / 发现', queue: 'youtube-discover-page', selectable: false, deployable: false }),
  query_quality: Object.freeze({ label: 'Query 质量评估', queue: 'youtube-query-quality', selectable: false, deployable: false }),
});

export function nodeWorkerRole(node) {
  if (node?.workerRole !== undefined) return node.workerRole;
  // Existing deployments use the incremental runtime. Preserve old saved plans
  // when they name a single role; empty registrations default to incremental.
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
