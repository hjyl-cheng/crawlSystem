import { createHash, randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';

export const beszelVersion = '0.19.0';
export function monitoringSystemId(nodeId) { return createHash('sha256').update(`qy-node:${nodeId}`).digest('hex').slice(0, 15); }

// Only normalized observations leave this adapter. Hub credentials and agent
// registration tokens never enter the node registry or browser responses.
export function createNodeMonitoring({ url, credentialsFile, publicUrl, fetchImpl = fetch }) {
  let auth;
  let authenticating;
  const cache = new Map();
  async function login() {
    if (!authenticating) authenticating = (async () => {
      const credentials = JSON.parse(await readFile(credentialsFile, 'utf8'));
      const response = await fetchImpl(`${url}/api/collections/users/auth-with-password`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ identity: credentials.email, password: credentials.password }), signal: AbortSignal.timeout(15000),
      });
      if (!response.ok) throw new Error('Beszel 登录失败，请检查中心监控服务配置');
      auth = await response.json();
    })().finally(() => { authenticating = null; });
    await authenticating;
  }
  async function api(path, options = {}, retry = true) {
    if (!auth) await login();
    const response = await fetchImpl(`${url}${path}`, { ...options,
      headers: { 'Content-Type': 'application/json', Authorization: auth.token }, signal: AbortSignal.timeout(15000) });
    if (response.status === 401 && retry) { auth = null; return api(path, options, false); }
    if (!response.ok) throw Object.assign(new Error(`Beszel 请求失败（HTTP ${response.status}）`), { status: response.status });
    return response.status === 204 ? null : response.json();
  }
  async function prepare(node) {
    const id = monitoringSystemId(node.id);
    await login();
    let system;
    try { system = await api(`/api/collections/systems/records/${id}`); }
    catch (error) { if (error.status !== 404) throw error; }
    if (!system) await api('/api/collections/systems/records', { method: 'POST', body: JSON.stringify({
      id, name: node.name, host: `qy-node-${node.id}`, port: '45876', users: [auth.record.id], status: 'pending',
    }) });
    const fingerprints = await api(`/api/collections/fingerprints/records?filter=${encodeURIComponent(`system="${id}"`)}`);
    let token = fingerprints.items[0]?.token;
    if (!token) {
      token = randomBytes(32).toString('hex');
      await api('/api/collections/fingerprints/records', { method: 'POST', body: JSON.stringify({ system: id, token }) });
    }
    const info = await api('/api/beszel/info');
    return { systemId: id, token, key: info.key, hubUrl: publicUrl };
  }
  async function observe(systemId, { fresh = false } = {}) {
    if (!/^[a-z0-9]{15}$/.test(systemId)) throw new Error('监控节点标识无效');
    const cached = cache.get(systemId);
    if (!fresh && cached && Date.now() - cached.time < 15000) return cached.value;
    const [system, records] = await Promise.all([
      api(`/api/collections/systems/records/${systemId}`),
      api(`/api/collections/system_stats/records?filter=${encodeURIComponent(`system="${systemId}" && type="1m"`)}&sort=-created&perPage=1`),
    ]);
    const record = records.items[0];
    const stats = record?.stats;
    const sampleAt = record?.created;
    const complete = stats && ['cpu', 'm', 'mu', 'mp', 'd', 'du', 'dp'].every(k => Number.isFinite(stats[k]));
    const online = system.status === 'up' && complete && Date.now() - Date.parse(sampleAt) < 180000;
    const value = { online: !!online, status: system.status, sampleAt: sampleAt ?? null, metrics: complete ? {
      cpuPercent: stats.cpu, memoryPercent: stats.mp, memoryUsedGiB: stats.mu, memoryTotalGiB: stats.m,
      diskPercent: stats.dp, diskUsedGiB: stats.du, diskTotalGiB: stats.d,
      uploadBytesPerSecond: stats.b?.[0] ?? 0, downloadBytesPerSecond: stats.b?.[1] ?? 0,
    } : null };
    cache.set(systemId, { time: Date.now(), value });
    return value;
  }
  return { prepare, observe, async available() { await login(); await api('/api/beszel/info'); } };
}
