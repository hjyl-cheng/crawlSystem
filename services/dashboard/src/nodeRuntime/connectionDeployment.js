import {plannedWorkerSlots} from './workerSlots.js';
import { randomUUID, createHash } from 'node:crypto';
import { assertNodeWorkerDeployment, nodeWorkerRole } from '../nodeWorkerTypes.js';

// A reviewable deployment recipe. Producing it performs no SSH, registration,
// Docker or queue operations. Secrets are referenced as files, never embedded.
export function buildNodeConnectionDeployment({ node, gatewayUrl, image, deploymentId = randomUUID(), collecting = false }) {
  const role=nodeWorkerRole(node);
  assertNodeWorkerDeployment(node, collecting?role:'incremental');
  if (node.kind !== 'execution' || node.provisioning?.state !== 'ready' || node.runtime?.state !== 'ready') throw new Error('请先完成节点初始化和运行环境准备');
  if (!/^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(node.id) || !/^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(deploymentId)) throw new Error('节点或部署标识无效');
  const url = new URL(gatewayUrl);
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) throw new Error('中心接入地址必须使用 HTTPS');
  if (typeof image !== 'string' || !/^[a-z0-9][a-z0-9./:_-]*@sha256:[a-f0-9]{64}$/.test(image)) throw new Error('节点接入镜像必须固定到 SHA256 摘要');
  const workers = node.workers ?? [];
  if (workers.some(worker => worker.role !== role && worker.count > 0)) throw new Error('Worker 计划与服务器功能类型不一致');
  const count = workers.find(worker => worker.role === role)?.count ?? 0;
  if (!Number.isSafeInteger(count) || count < 1) throw new Error('请先保存 Worker 数量（正整数）');
  // Every service/config exceeds this lower bound. Reject an impossible
  // installer payload before allocating its arrays; the existing wire limit
  // remains 1 MiB, independent of a per-node Worker limit.
  if (count * 128 > 1024 * 1024) throw new Error('部署配置超过单次传输大小限制');
  const root = `/etc/qy-node/runtime/deployments/${deploymentId}`;
  const services = {}; const files = {}; const registrations = [];
  const {slots,allocationSlots,slotSequence}=plannedWorkerSlots(node,count);
  for (const slot of slots) {
    const config = { version: 1, mode: 'connect_only', role, node_id: node.id, slot, deployment_id: deploymentId, gateway_url: url.href.replace(/\/$/, '') };
    const bytes = JSON.stringify(config) + '\n';
    files[`${slot}.json`] = bytes;
    registrations.push({ nodeId: node.id, slot, deploymentId, role, configHash: createHash('sha256').update(bytes).digest('hex') });
    services[slot] = { image, init: true, restart: 'unless-stopped', healthcheck: { disable: true }, user: '1000:1000', read_only: true,
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
    count, slots, allocationSlots, slotSequence, memoryLimitMiB: count * 256, image, gatewayOrigin: url.origin,
    compose: { name: `qy-node-${node.id.replaceAll('-', '').slice(0, 16)}`, services }, files, registrations };
}

export function connectionDeploymentPreview(node, env = process.env) {
  if (!env.SERVER_NODE_CONNECTION_IMAGE || !env.SERVER_NODE_GATEWAY_URL) return { available: false,
    reason: '中心尚未配置节点接入镜像和 HTTPS 网关；已保存的 Worker 计划保留。' };
  try { return { available: true, ...buildNodeConnectionDeployment({ node, image: env.SERVER_NODE_CONNECTION_IMAGE, gatewayUrl: env.SERVER_NODE_GATEWAY_URL }) }; }
  catch (error) { return { available: false, reason: error.message }; }
}
