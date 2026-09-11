// Worker-side client for its loopback Rota data plane. Does not import the
// center-only issuer, database modules, or pool/credential management.
export function createLocalRotaClient({ controlUrl, proxyUrl, token, nodeId, fetchImpl = fetch }) {
  const local = (value) => {
    const url = new URL(value);
    if (url.protocol !== 'http:' || !['127.0.0.1', '[::1]'].includes(url.hostname)
      || url.username || url.password || url.search || url.hash || url.pathname !== '/') throw new TypeError('loopback Rota endpoint required');
    return url;
  };
  const control = local(controlUrl);
  const proxy = local(proxyUrl);
  if (typeof token !== 'string' || token.length < 32 || typeof nodeId !== 'string' || !nodeId) throw new TypeError('node control identity required');
  const request = async (path, body) => {
    const response = await fetchImpl(new URL(path, control), { method: body ? 'POST' : 'GET', redirect: 'error',
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      ...(body ? { body: JSON.stringify(body) } : {}), signal: AbortSignal.timeout(10000) });
    const chunks = []; let size = 0;
    for await (const chunk of response.body) {
      size += chunk.length;
      if (size > 8192) { const error = new Error('INVALID_LOCAL_ROTA_RESPONSE'); throw error; }
      chunks.push(chunk);
    }
    // Never include an arbitrary body or a grant/credential in error messages.
    if (!response.ok) { const error = new Error('LOCAL_ROTA_REJECTED'); error.status = response.status; throw error; }
    try { return JSON.parse(Buffer.concat(chunks).toString('utf8')); }
    catch { throw new Error('INVALID_LOCAL_ROTA_RESPONSE'); }
  };
  return {
    async retire(binding) {
      const expected = Object.fromEntries(['boot_id', 'slot', 'epoch', 'task_id', 'generation'].map(field => [field, binding[field]]));
      const ack = await request('/v1/retire', expected);
      if (ack.retired !== true || ack.in_flight !== 0
        || Object.keys(expected).some(field => ack[field] !== expected[field])) throw new Error('LOCAL_ROTA_RECEIPT_MISMATCH');
      return ack;
    },
    async boot() {
      const boot = await request('/v1/boot');
      if (boot.version !== 1 || boot.node_id !== nodeId || !/^[a-f0-9]{48}$/.test(boot.boot_id)) throw new Error('LOCAL_ROTA_IDENTITY_MISMATCH');
      return boot;
    },
    async apply(signed, { lease, slot, bootId }) {
      if (typeof signed?.payload !== 'string' || signed.payload.length > 24000
        || typeof signed.signature !== 'string' || signed.signature.length > 100) throw new Error('INVALID_ROUTE_GRANT');
      let grant;
      try { grant = JSON.parse(Buffer.from(signed.payload, 'base64').toString('utf8')); }
      catch { throw new Error('INVALID_ROUTE_GRANT'); }
      if (grant.version !== 1 || grant.node_id !== nodeId || grant.boot_id !== bootId || grant.slot !== slot
        || grant.task_id !== lease.task_id || grant.generation !== lease.generation) throw new Error('ROUTE_LEASE_MISMATCH');
      // The Go relay verifies the signature before it changes any route. Node
      // task matching above also prevents applying another channel's grant.
      const ack = await request('/v1/route', signed);
      for (const field of ['version', 'action', 'slot', 'epoch', 'task_id', 'generation', 'route_id', 'identity_id', 'egress_country', 'expires_at_ms']) {
        if (ack[field] !== grant[field]) throw new Error('LOCAL_ROTA_RECEIPT_MISMATCH');
      }
      const url = new URL(proxy);
      url.username = slot; url.password = grant.proxy_token;
      return { ...ack, ...(grant.action === 'revoke' ? {} : { proxyUrl: url.href }), egressCountry: grant.egress_country || null };
    },
  };
}
