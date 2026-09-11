import { randomUUID, createHash } from 'node:crypto';

// A reviewable deployment recipe. Producing it performs no SSH, registration,
// Docker or queue operations. Secrets are referenced as files, never embedded.
export function buildNodeConnectionDeployment({ node, gatewayUrl, image, deploymentId = randomUUID() }) {
  if (node.kind !== 'execution' || node.provisioning?.state !== 'ready' || node.runtime?.state !== 'ready') throw new Error('请先完成节点初始化和运行环境准备');
  if (!/^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(node.id) || !/^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(deploymentId)) throw new Error('节点或部署标识无效');
  const url = new URL(gatewayUrl);
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) throw new Error('中心接入地址必须使用 HTTPS');
  if (typeof image !== 'string' || !/^[a-z0-9][a-z0-9./:_-]*@sha256:[a-f0-9]{64}$/.test(image)) throw new Error('节点接入镜像必须固定到 SHA256 摘要');
  const workers = node.workers ?? [];
  if (workers.some(worker => worker.role !== 'incremental' && worker.count > 0)) throw new Error('当前接入包只支持增量节点，其他 Worker 计划请保留到对应入口接通后部署');
  const count = workers.find(worker => worker.role === 'incremental')?.count ?? 0;
  if (!Number.isInteger(count) || count < 1 || count > 32) throw new Error('请先保存 1～32 个增量 Worker 的计划');
  const root = `/etc/qy-node/runtime/deployments/${deploymentId}`;
  const services = {}; const files = {}; const registrations = [];
  for (let index = 1; index <= count; index++) {
    const slot = `incremental-${index}`;
    const config = { version: 1, mode: 'connect_only', role: 'incremental', node_id: node.id, slot, deployment_id: deploymentId, gateway_url: url.href.replace(/\/$/, '') };
    const bytes = JSON.stringify(config) + '\n';
    files[`${slot}.json`] = bytes;
    registrations.push({ nodeId: node.id, slot, deploymentId, role: 'incremental', configHash: createHash('sha256').update(bytes).digest('hex') });
    services[slot] = { image, init: true, restart: 'unless-stopped', user: '1000:1000', read_only: true,
      cap_drop: ['ALL'], security_opt: ['no-new-privileges:true'], pids_limit: 64, mem_limit: '256m', cpus: 0.5,
      stop_grace_period: '25s', tmpfs: ['/tmp:rw,noexec,nosuid,size=16m,uid=1000,gid=1000', '/run/qy-node:rw,noexec,nosuid,size=1m,uid=1000,gid=1000'],
      volumes: [
        { type: 'bind', source: `${root}/${slot}.json`, target: '/run/secrets/node-config.json', read_only: true, bind: { create_host_path: false } },
        { type: 'bind', source: `${root}/node-token`, target: '/run/secrets/node-token', read_only: true, bind: { create_host_path: false } },
        { type: 'bind', source: `${root}/${slot}.relay-token`, target: '/run/secrets/relay-token', read_only: true, bind: { create_host_path: false } },
        { type: 'bind', source: `${root}/route-public.pem`, target: '/run/secrets/route-public.pem', read_only: true, bind: { create_host_path: false } },
      ],
      logging: { driver: 'json-file', options: { 'max-size': '5m', 'max-file': '2' } },
      labels: { 'qy.node.id': node.id, 'qy.node.slot': slot, 'qy.deployment.id': deploymentId, 'qy.remote.mode': 'connect_only' },
    };
  }
  return { deploymentId, nodeId: node.id, mode: 'connect_only', readyForTasks: false,
    count, memoryLimitMiB: count * 256, image, gatewayOrigin: url.origin,
    compose: { name: `qy-node-${node.id.replaceAll('-', '').slice(0, 16)}`, services }, files, registrations };
}

export function connectionDeploymentPreview(node, env = process.env) {
  if (!env.SERVER_NODE_CONNECTION_IMAGE || !env.SERVER_NODE_GATEWAY_URL) return { available: false,
    reason: '中心尚未配置节点接入镜像和 HTTPS 网关；已保存的 Worker 计划保留。' };
  try { return { available: true, ...buildNodeConnectionDeployment({ node, image: env.SERVER_NODE_CONNECTION_IMAGE, gatewayUrl: env.SERVER_NODE_GATEWAY_URL }) }; }
  catch (error) { return { available: false, reason: error.message }; }
}
