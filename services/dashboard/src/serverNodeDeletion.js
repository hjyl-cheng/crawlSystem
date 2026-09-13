import { randomUUID } from 'node:crypto';
import { serverNodeDeletionEligibility, unusedNodeDeletionEligibility } from './serverNodes.js';
import { createNodeSsh, validateBootstrapPassword } from './serverNodeSsh.js';
import { createNodeMonitoring } from './serverNodeMonitoring.js';

const fail = (message, statusCode = 409) => Object.assign(new Error(message), { statusCode });
export function createUnusedNodeCheck(query) {
  return async node => {
    // An absent center identity cannot claim work. Any center registration,
    // even disabled/historical, requires a separate decommission workflow.
    let result;
    try {
      result = await query(`SELECT
        EXISTS(SELECT 1 FROM remote_ingestion.nodes WHERE node_id=$1) AS registered,
        EXISTS(SELECT 1 FROM remote_ingestion.node_deployments WHERE node_id=$1) AS deployed,
        EXISTS(SELECT 1 FROM remote_ingestion.worker_connections WHERE node_id=$1) AS workers,
        EXISTS(SELECT 1 FROM remote_ingestion.tasks WHERE target_node_id=$1 OR node_id=$1) AS tasks`, [node.id]);
    } catch { throw fail('无法核实中心节点和任务状态，已保留节点，请稍后重试', 503); }
    const row = result.rows[0];
    if (!row || !['registered','deployed','workers','tasks'].every(key => row[key] === false)) {
      throw fail('中心已有节点接入、Worker 或任务记录，需要先完成退役检查，不能直接删除');
    }
  };
}

export function createNodeDeletion({ store, ssh, monitoring, checkCenter }) {
  const active = new Set();
  return {
    async check(id) {
      const registry = await store.load();
      const node = registry.nodes.find(item => item.id === id);
      if (!node) throw fail('服务器不存在', 404);
      const direct = serverNodeDeletionEligibility(node);
      if (direct.allowed) return { id, version: registry.version, ...direct };
      const eligibility = unusedNodeDeletionEligibility(node);
      if (eligibility.allowed) {
        try { await checkCenter(node); }
        catch (error) { return { id, version: registry.version, allowed: false, reason: error.message }; }
      }
      return { id, version: registry.version, ...eligibility,
        ...(eligibility.allowed && node.deletion?.state === 'failed' ? { reason: `上次删除未完成：${node.deletion.error}。可点击确认重试；已清理的步骤会安全跳过。` } : {}),
        requiresRemoteCheck: eligibility.allowed };
    },
    async remove({ id, version, password = '' }) {
      validateBootstrapPassword(password);
      const registry = await store.load();
      const node = registry.nodes.find(item => item.id === id);
      if (!node) throw fail('服务器不存在', 404);
      if (serverNodeDeletionEligibility(node).allowed) return store.remove({ id, version });
      if (active.has(id)) throw fail('节点正在删除，请等待操作结束');
      const operationId = randomUUID();
      await store.beginDeletion({ id, version, operationId });
      active.add(id);
      let connection;
      let stage = '中心任务检查';
      try {
        await checkCenter(node);
        stage = 'SSH 连接或 sudo 权限检查';
        connection = await ssh.connect(node, { keyOnly: true });
        await ssh.verify(connection, password);
        stage = '远程 Worker 与环境检查';
        await ssh.removeUnusedNode(connection, node, password, 'check');
        stage = '中心任务复核';
        await checkCenter(node);
        stage = '远程监控清理';
        await ssh.removeUnusedNode(connection, node, password, 'cleanup');
        stage = '中心监控登记清理';
        await monitoring.remove(node);
        stage = '节点登记删除';
        return await store.finishDeletion(id, operationId);
      } catch (error) {
        const message = error.deletionSafe ? error.message : `${stage}未完成，节点登记已保留，可重新打开删除窗口重试${stage.includes('sudo') ? '；需要 sudo 密码时请填写服务器密码' : ''}`;
        await store.failDeletion(id, operationId, message).catch(() => {});
        throw fail(message, error.statusCode ?? 409);
      } finally { password = undefined; ssh.close(connection); active.delete(id); }
    },
  };
}

export function nodeDeletionFromEnv(store, query, env = process.env) {
  if (!env.SERVER_NODE_STATE_DIR || !env.BESZEL_URL || !env.BESZEL_CREDENTIALS_FILE) return null;
  return createNodeDeletion({ store, checkCenter: createUnusedNodeCheck(query),
    ssh: createNodeSsh({ stateDir: env.SERVER_NODE_STATE_DIR }),
    monitoring: createNodeMonitoring({ url: env.BESZEL_URL, credentialsFile: env.BESZEL_CREDENTIALS_FILE, publicUrl: env.BESZEL_PUBLIC_URL }),
  });
}
