import { randomUUID } from "node:crypto";
import { isIP } from "node:net";

const settingKey = "dashboard_server_nodes_v1";
export const workerRoles = Object.freeze({
  fullcrawl: "迁移 / Full Crawl",
  incremental: "增量采集",
  discover: "Query / 发现",
  query_quality: "Query 质量评估",
});

function invalid(message, statusCode = 400) {
  return Object.assign(new Error(message), { statusCode });
}

function text(value, label, max, required = false) {
  if (typeof value !== "string") throw invalid(`${label}格式不正确`);
  const result = value.trim();
  if ((required && !result) || result.length > max) throw invalid(`${label}${required ? "不能为空，且" : ""}最多 ${max} 个字符`);
  return result;
}

function integer(value, label, min, max) {
  if (!Number.isInteger(value) || value < min || value > max) throw invalid(`${label}应为 ${min}–${max} 的整数`);
  return value;
}

export function normalizeServerNode(input) {
  if (!input || typeof input !== "object" || Array.isArray(input)) throw invalid("请填写服务器信息");
  const allowed = new Set(["name", "host", "port", "username", "sshAlias", "kind", "notes", "workers"]);
  if (Object.keys(input).some(key => !allowed.has(key))) throw invalid("包含不支持的字段；请勿填写密码或私钥");
  const name = text(input.name, "服务器名称", 80, true);
  const host = text(input.host, "服务器地址", 253, true).toLowerCase();
  const domain = /^(?=.{1,253}$)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$/;
  if (!isIP(host) && (!domain.test(host) || /^[\d.]+$/.test(host))) throw invalid("请输入有效的 IP 或主机名，不包含协议和端口");
  const port = integer(input.port, "SSH 端口", 1, 65535);
  const username = text(input.username, "SSH 用户名", 64, true);
  if (!/^[a-z_][a-z0-9_-]*\$?$/i.test(username)) throw invalid("SSH 用户名格式不正确");
  const sshAlias = text(input.sshAlias ?? "", "SSH 配置别名", 80);
  if (sshAlias && !/^[a-z0-9][a-z0-9_-]*$/i.test(sshAlias)) throw invalid("SSH 配置别名只能包含字母、数字、下划线和短横线");
  if (!["center", "execution"].includes(input.kind)) throw invalid("请选择节点类型");
  const notes = text(input.notes ?? "", "备注", 500);
  if (!Array.isArray(input.workers) || input.workers.length > Object.keys(workerRoles).length) throw invalid("Worker 配置格式不正确");
  const seen = new Set();
  const workers = input.workers.map(worker => {
    if (!worker || typeof worker !== "object" || Object.keys(worker).some(k => !["role", "count"].includes(k))) throw invalid("Worker 配置格式不正确");
    if (!Object.hasOwn(workerRoles, worker.role) || seen.has(worker.role)) throw invalid("Worker 类型不支持或重复");
    seen.add(worker.role);
    return { role: worker.role, count: integer(worker.count, "Worker 数量", 1, 100) };
  });
  return { name, host, port, username, sshAlias, kind: input.kind, notes, workers };
}

// This registry stores user-entered configuration only. Runtime observations and
// deployment commands must not be inferred from the saved worker counts.
export function createServerNodeStore(query) {
  async function load() {
    const result = await query("SELECT value_json FROM crawler.settings WHERE setting_key=$1", [settingKey]);
    return result.rows[0]?.value_json ?? { version: 0, nodes: [] };
  }

  async function save({ id = null, version, node }) {
    integer(version, "配置版本", 0, Number.MAX_SAFE_INTEGER);
    const normalized = normalizeServerNode(node);
    const previous = await load();
    if (previous.version !== version) throw invalid("配置已被更新，请刷新页面后重试；你的输入仍保留在表单中", 409);
    if (id && !previous.nodes.some(item => item.id === id)) throw invalid("服务器不存在", 404);
    if (!id && previous.nodes.length >= 200) throw invalid("最多登记 200 台服务器");
    if (previous.nodes.some(item => item.id !== id && item.host === normalized.host && item.port === normalized.port)) {
      throw invalid("此地址和 SSH 端口的服务器已经添加", 409);
    }
    const now = new Date().toISOString();
    const saved = {
      ...normalized,
      id: id ?? randomUUID(),
      createdAt: previous.nodes.find(item => item.id === id)?.createdAt ?? now,
      updatedAt: now,
    };
    const next = { version: version + 1, nodes: id ? previous.nodes.map(item => item.id === id ? saved : item) : [...previous.nodes, saved] };
    // Compare and swap also protects concurrent creation of the initial registry.
    const result = await query(`INSERT INTO crawler.settings(setting_key,value_json,updated_at)
      VALUES($1,$2::jsonb,now()) ON CONFLICT(setting_key) DO UPDATE
      SET value_json=EXCLUDED.value_json,updated_at=now()
      WHERE (crawler.settings.value_json->>'version')::bigint=$3
      RETURNING value_json`, [settingKey, JSON.stringify(next), version]);
    if (result.rowCount !== 1) throw invalid("配置已被更新，请刷新页面后重试；你的输入仍保留在表单中", 409);
    return next;
  }
  return { load, save };
}
