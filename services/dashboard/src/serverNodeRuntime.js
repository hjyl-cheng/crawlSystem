import { randomUUID } from 'node:crypto';
import { createNodeSsh, validateBootstrapPassword } from './serverNodeSsh.js';

export function createNodeRuntime({ store, ssh }) {
  const active = new Map();
  async function execute(node, operationId, password) {
    let connection; let step = 'ssh';
    const advance = patch => store.advanceRuntime(node.id, operationId, patch);
    try {
      await advance({ steps: { ssh: 'running' } });
      connection = await ssh.connect(node, { keyOnly: true });
      await ssh.verify(connection, password);
      await advance({ steps: { ssh: 'completed' } });
      const result = await ssh.prepareRuntime(connection, node, password, async next => {
        step = next;
        await advance({ remoteChanges: true, steps: { [step]: 'running' } });
      }, async finished => advance({ steps: { [finished]: 'completed' } }));
      await advance({ state: 'ready', details: result, finishedAt: new Date().toISOString(), error: null });
    } catch (error) {
      const message = password ? String(error.message).replaceAll(password, '[已隐藏]') : String(error.message);
      await advance({ state: 'failed', finishedAt: new Date().toISOString(), steps: { [step]: 'failed' }, error: message.slice(0, 500) }).catch(() => {});
    } finally { password = undefined; ssh.close(connection); active.delete(node.id); }
  }
  return {
    async start({ id, version, password = '' }) {
      validateBootstrapPassword(password);
      if (active.has(id)) throw Object.assign(new Error('该节点正在准备运行环境'), { statusCode: 409 });
      const operationId = randomUUID();
      const registry = await store.beginRuntime({ id, version, operationId });
      const task = execute(registry.nodes.find(node => node.id === id), operationId, password);
      active.set(id, task); void task.catch(() => {});
      return registry;
    },
    waitForIdle: () => Promise.allSettled([...active.values()]),
  };
}

export function nodeRuntimeFromEnv(store, env = process.env) {
  return env.SERVER_NODE_STATE_DIR ? createNodeRuntime({ store, ssh: createNodeSsh({ stateDir: env.SERVER_NODE_STATE_DIR }) }) : null;
}
