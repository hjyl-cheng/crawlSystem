// Center-only client. Its dedicated Rota credential never goes to a remote
// Worker. Call with the Rota Task fence saved by central orchestration, not with
// a node-supplied proxy ID, address, country, or arbitrary SQL.
const fenceFields = ['slot_name', 'worker_id', 'worker_instance_id', 'lease_id', 'route_generation',
  'task_id', 'business_run_id', 'job_execution_id'];
const protocols = new Set(['http', 'https', 'socks4', 'socks4a', 'socks5', 'vless', 'vmess', 'trojan', 'shadowsocks', 'hysteria2']);

export function createRotaRemoteRouteReader({ url, token, allowLoopbackHttp = false, fetchImpl = fetch, now = Date.now }) {
  const endpoint = new URL(url);
  if (endpoint.username || endpoint.password || endpoint.search || endpoint.hash
    || endpoint.pathname !== '/internal/v1/remote-route'
    || (endpoint.protocol !== 'https:' && !(allowLoopbackHttp && endpoint.protocol === 'http:'
      && ['127.0.0.1', '[::1]'].includes(endpoint.hostname)))) throw new TypeError('protected Rota route endpoint required');
  if (typeof token !== 'string' || token.length < 32) throw new TypeError('dedicated Rota route token required');
  return async (fence) => {
    if (fenceFields.some(field => field === 'route_generation'
      ? !Number.isSafeInteger(fence[field]) || fence[field] < 0
      : typeof fence[field] !== 'string' || !fence[field] || fence[field].length > 512)) throw new TypeError('complete Rota task fence required');
    const expected = Object.fromEntries(fenceFields.map(field => [field, fence[field]]));
    const started = now();
    let data;
    try {
      const response = await fetchImpl(endpoint, { method: 'POST', redirect: 'error',
        headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
        body: JSON.stringify(expected), signal: AbortSignal.timeout(5000) });
      // A rejected response can contain infrastructure error text. Discard it;
      // never attach a credential-bearing response body to an Error or a log.
      if (!response.ok) {
        await response.body?.cancel();
        const error = new Error('ROTA_ROUTE_READ_REJECTED'); error.status = response.status; throw error;
      }
      const chunks = []; let size = 0;
      for await (const chunk of response.body) {
        size += chunk.length;
        if (size > 32768) throw new Error('ROTA_ROUTE_RESPONSE_TOO_LARGE');
        chunks.push(chunk);
      }
      data = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    } catch (error) {
      if (error.message === 'ROTA_ROUTE_READ_REJECTED') throw error;
      throw new Error('ROTA_ROUTE_READ_UNAVAILABLE');
    }
    if (!data || data.ok !== true || fenceFields.some(field => data[field] !== expected[field])
      || !Number.isSafeInteger(data.credential_generation) || data.credential_generation < 0
      || !Number.isSafeInteger(data.profile_epoch) || data.profile_epoch < 0
      || !Number.isSafeInteger(data.identity_policy_version) || data.identity_policy_version < 1
      || !['network_identity_key', 'identity_policy_id', 'identity_policy_hash', 'workload_scope'].every(field => typeof data[field] === 'string' && data[field])
      || typeof data.egress_country !== 'string' || !/^(?:[A-Z]{2})?$/.test(data.egress_country)
      || !protocols.has(data.upstream?.protocol) || typeof data.upstream.address !== 'string' || !data.upstream.address
      || !['username', 'password'].every(field => typeof data.upstream[field] === 'string')) throw new Error('ROTA_ROUTE_RESPONSE_MISMATCH');
    const serverTime = Date.parse(data.server_time);
    const remaining = Date.parse(data.lease_until) - serverTime;
    // Use the server's remaining duration from the START of this request. This
    // conservatively charges the entire round trip and avoids extending a lease
    // merely because Rota's wall clock is ahead of the coordinator's clock.
    const localDeadline = started + remaining;
    if (!Number.isSafeInteger(localDeadline) || remaining <= 0 || localDeadline <= now() + 3000) throw new Error('ROTA_ROUTE_LEASE_EXPIRING');
    return { ...expected, workload_scope: data.workload_scope, credential_generation: data.credential_generation,
      network_identity_key: data.network_identity_key, profile_epoch: data.profile_epoch,
      identity_policy_id: data.identity_policy_id, identity_policy_version: data.identity_policy_version,
      identity_policy_hash: data.identity_policy_hash, egress_country: data.egress_country,
      route_lease_until_ms: localDeadline,
      upstream: { protocol: data.upstream.protocol, address: data.upstream.address,
        username: data.upstream.username, password: data.upstream.password } };
  };
}
