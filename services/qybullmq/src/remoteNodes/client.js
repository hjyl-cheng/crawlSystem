import { RemoteProtocolError } from './protocol.js';

export function createRemoteNodeClient({ url, token, allowLoopbackHttp = false, fetchImpl = fetch, timeoutMs = 15000 }) {
  const endpoint = new URL(url);
  if (endpoint.username || endpoint.password || endpoint.search || endpoint.hash
    || (endpoint.protocol !== 'https:' && !(allowLoopbackHttp && endpoint.protocol === 'http:'
      && ['127.0.0.1', '[::1]'].includes(endpoint.hostname)))) {
    throw new TypeError('HTTPS gateway without URL credentials required');
  }
  if (typeof token !== 'string' || token.length < 32) throw new TypeError('node token required');
  const request = async (path, value, compressed = false, responseLimit = 65536) => {
    const response = await fetchImpl(`${endpoint.href.replace(/\/$/, '')}${path}`, {
      method: value === undefined ? 'GET' : 'POST',
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json',
        ...(compressed ? { 'content-encoding': 'gzip' } : {}) },
      body: value === undefined ? undefined : compressed ? value : JSON.stringify(value),
      redirect: 'error',
      signal: AbortSignal.timeout(timeoutMs),
    });
    let size = 0;
    const chunks = [];
    for await (const chunk of response.body) {
      size += chunk.length;
      if (size > responseLimit) throw new RemoteProtocolError('INVALID_GATEWAY_RESPONSE', 502);
      chunks.push(chunk);
    }
    let data;
    try { data = JSON.parse(Buffer.concat(chunks).toString('utf8')); }
    catch { throw new RemoteProtocolError('INVALID_GATEWAY_RESPONSE', 502); }
    if (!response.ok) throw new RemoteProtocolError(data.error || 'GATEWAY_ERROR', response.status);
    return data;
  };
  return {
    youtubeSession: value => request('/v1/youtube/session', value, false, 512 * 1024),
    youtubeCheckpoint: value => request('/v1/youtube/checkpoint', value),
    workerHeartbeat: value => request('/v1/node/heartbeat', value),
    grantRoute: (requestValue) => request('/v1/network/grant', requestValue),
    releaseRoute: (receipt) => request('/v1/network/release', receipt),
    abandonRoute: (requestValue) => request('/v1/network/abandon', requestValue),
    claim: async (claimId, slot = null, connection = null) => (await request('/v1/work/claim', { claim_id: claimId,
      ...(slot ? { slot } : {}), ...(connection ? { connection } : {}) })).lease,
    heartbeat: (lease) => request(`/v1/work/${lease.task_id}/heartbeat`, { generation: lease.generation }),
    upload: (taskId, bytes) => request(`/v1/work/${taskId}/results`, bytes, true),
    receipt: (batchId) => request(`/v1/receipts/${batchId}`),
    pollCommands: (lease) => request(`/v1/channel-plans/${lease.task_id}/commands`, { generation: lease.generation }),
    uploadCommand: (lease, bytes) => request(`/v1/channel-plans/${lease.task_id}/results`, bytes, true),
  };
}
