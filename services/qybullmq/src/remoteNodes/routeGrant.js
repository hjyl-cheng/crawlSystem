import { createPrivateKey, sign } from 'node:crypto';

const protocols = new Set(['http', 'https', 'socks4', 'socks4a', 'socks5', 'vless', 'vmess', 'trojan', 'shadowsocks', 'hysteria2']);
const fields = ['node_id', 'slot', 'epoch', 'task_id', 'generation', 'route_id', 'identity_id', 'egress_country', 'proxy_token'];

// Center-only. authorize must load and fence BOTH the channel lease and the
// existing Rota assignment, plus its node/slot mapping, on EVERY call. It owns
// durable epoch/token allocation; this module does not select or rotate proxies.
// No HTTP route exposes this issuer until the real ownership adapter is wired.
export function createRemoteRouteIssuer({ privateKey, authorize, now = Date.now, maxTtlMs = 60000 }) {
  const key = privateKey?.type === 'private' ? privateKey : createPrivateKey(privateKey);
  if (key.asymmetricKeyType !== 'ed25519' || typeof authorize !== 'function'
    || !Number.isSafeInteger(maxTtlMs) || maxTtlMs < 1000 || maxTtlMs > 300000) throw new TypeError('route signing configuration invalid');
  return async (request) => {
    const { node_id, slot, task_id, generation, boot_id, action = 'activate' } = request;
    if (![node_id, slot, task_id].every(value => typeof value === 'string' && value.length > 0 && value.length <= 100)
      || !Number.isSafeInteger(generation) || generation < 1 || !/^[a-f0-9]{48}$/.test(boot_id)
      || !['activate', 'renew', 'revoke'].includes(action)) throw new TypeError('invalid route request');
    const route = await authorize(Object.freeze({ node_id, slot, task_id, generation, boot_id, action }));
    if (!route || route.node_id !== node_id || route.slot !== slot || route.task_id !== task_id || route.generation !== generation
      || !Number.isSafeInteger(route.epoch) || route.epoch < 1
      || ![route.route_id, route.identity_id, route.proxy_token].every(value => typeof value === 'string' && value.length > 0)
      || route.proxy_token.length < 32 || route.proxy_token.length > 256
      || typeof route.egress_country !== 'string' || !/^(?:[A-Z]{2})?$/.test(route.egress_country)
      || !Number.isSafeInteger(route.task_lease_until_ms) || !Number.isSafeInteger(route.route_lease_until_ms)) {
      throw new Error('ROUTE_AUTHORIZATION_MISMATCH');
    }
    const upstream = route.upstream;
    if (!protocols.has(upstream?.protocol) || typeof upstream.address !== 'string' || !upstream.address
      || ['username', 'password'].some(field => upstream[field] !== undefined && typeof upstream[field] !== 'string')) {
      throw new Error('INVALID_AUTHORIZED_UPSTREAM');
    }
    const current = now();
    // Never let the proxy outlive either ownership lease. Reserve time for a
    // bounded local/center clock offset; a stalled authority cannot renew it.
    const expiresAt = Math.min(current + maxTtlMs, route.task_lease_until_ms - 2000, route.route_lease_until_ms - 2000);
    if (expiresAt <= current + 1000) throw new Error('ROUTE_LEASE_EXPIRING');
    const grant = { version: 1, action, boot_id, ...Object.fromEntries(fields.map(field => [field, route[field]])),
      expires_at_ms: expiresAt, upstream: { protocol: upstream.protocol, address: upstream.address,
        ...(upstream.username !== undefined ? { username: upstream.username } : {}),
        ...(upstream.password !== undefined ? { password: upstream.password } : {}) } };
    const payload = Buffer.from(JSON.stringify(grant));
    if (payload.length > 16384) throw new Error('ROUTE_GRANT_TOO_LARGE');
    return { payload: payload.toString('base64'), signature: sign(null, payload, key).toString('base64') };
  };
}
