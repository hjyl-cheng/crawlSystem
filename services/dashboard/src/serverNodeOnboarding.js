import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { createNodeSsh, validateBootstrapPassword } from './serverNodeSsh.js';
import { createNodeMonitoring } from './serverNodeMonitoring.js';

export function createNodeOnboarding({ store, ssh, monitoring, pollInterval = 5000, sampleTimeout = 180000 }) {
  const active = new Map();
  async function execute(node, operationId, password) {
    let connection;
    let step = 'ssh';
    const advance = patch => store.advanceInitialization(node.id, operationId, patch);
    try {
      await advance({ steps: { ssh: 'running' } });
      await monitoring.available();
      connection = await ssh.connect(node, { password });
      const arch = await ssh.verify(connection, password);
      await advance({ remoteChanges: true, steps: { ssh: 'completed', key: 'running' } });
      step = 'key';
      await ssh.installKey(connection);
      const keyConnection = await ssh.connect(node, { keyOnly: true });
      ssh.close(keyConnection);
      await advance({ fingerprint: connection.fingerprint, steps: { key: 'completed', monitoring: 'running' } });
      step = 'monitoring';
      const config = await monitoring.prepare(node);
      await advance({ systemId: config.systemId });
      await ssh.installMonitoring(connection, node, arch, config, password);
      password = undefined;
      ssh.close(connection); connection = null;
      await advance({ steps: { monitoring: 'completed', metrics: 'running' } });
      step = 'metrics';
      const until = Date.now() + sampleTimeout;
      let observation;
      while (Date.now() < until) {
        observation = await monitoring.observe(config.systemId, { fresh: true });
        if (observation.online && Date.parse(observation.sampleAt) >= Date.parse(node.provisioning.startedAt)) break;
        await delay(pollInterval);
      }
      if (!observation?.online || Date.parse(observation.sampleAt) < Date.parse(node.provisioning.startedAt)) throw new Error('Beszel 已安装，但尚未收到新监控数据，请检查服务器到中心的 HTTPS 网络后重试');
      await advance({ state: 'ready', finishedAt: new Date().toISOString(), steps: { metrics: 'completed' }, error: null });
    } catch (error) {
      // Adapter errors are intentionally sanitized; never persist raw SSH stderr,
      // request bodies, agent tokens or user credentials.
      const safeMessage = password ? String(error.message).replaceAll(password, '[已隐藏]') : String(error.message);
      await advance({ state: 'failed', finishedAt: new Date().toISOString(), steps: { [step]: 'failed' }, error: safeMessage.slice(0, 500) }).catch(() => {});
    } finally { password = undefined; ssh.close(connection); active.delete(node.id); }
  }
  async function start({ id, version, password = '' }) {
    validateBootstrapPassword(password);
    if (active.has(id)) throw Object.assign(new Error('该节点正在初始化，请勿重复提交'), { statusCode: 409 });
    const operationId = randomUUID();
    const registry = await store.beginInitialization({ id, version, operationId });
    const node = registry.nodes.find(item => item.id === id);
    const task = execute(node, operationId, password);
    active.set(id, task);
    void task.catch(() => {});
    return registry;
  }
  return { start, observe: monitoring.observe, waitForIdle: () => Promise.allSettled([...active.values()]) };
}

export function nodeOnboardingFromEnv(store, env = process.env) {
  const enabled = !!(env.SERVER_NODE_STATE_DIR && env.BESZEL_URL && env.BESZEL_CREDENTIALS_FILE && env.BESZEL_PUBLIC_URL);
  if (!enabled) return null;
  return createNodeOnboarding({ store,
    ssh: createNodeSsh({ stateDir: env.SERVER_NODE_STATE_DIR }),
    monitoring: createNodeMonitoring({ url: env.BESZEL_URL, credentialsFile: env.BESZEL_CREDENTIALS_FILE, publicUrl: env.BESZEL_PUBLIC_URL }),
  });
}
