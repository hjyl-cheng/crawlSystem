import { constants } from 'node:fs';
import { open } from 'node:fs/promises';
import { createPublicKey } from 'node:crypto';
import { hash, uuid } from './protocol.js';

export async function readNodeFile(path, { secret = false, maxBytes = 16384 } = {}) {
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const stat = await file.stat();
    if (!stat.isFile() || stat.size > maxBytes || (secret && (stat.mode & 0o077))) throw new Error('NODE_FILE_INVALID');
    const bytes = await file.readFile();
    if (bytes.length > maxBytes) throw new Error('NODE_FILE_INVALID');
    return bytes;
  } finally { await file.close(); }
}

export function parseWorkerConfig(bytes, { allowLoopbackHttp = false, mode = 'connect_only' } = {}) {
  let value;
  try { value = JSON.parse(bytes.toString('utf8')); } catch { throw new Error('NODE_CONFIG_INVALID'); }
  const fields = ['version', 'mode', 'role', 'node_id', 'slot', 'deployment_id', 'gateway_url'];
  if (!value || Object.keys(value).some(key => !fields.includes(key)) || value.version !== 1
    || !['connect_only','incremental_collect'].includes(mode) || value.mode !== mode || value.role !== 'incremental' || typeof value.slot !== 'string'
    || !/^[a-z0-9-]{1,60}$/.test(value.slot)) throw new Error('NODE_CONFIG_INVALID');
  uuid(value.node_id); uuid(value.deployment_id);
  const endpoint = new URL(value.gateway_url);
  if (endpoint.username || endpoint.password || endpoint.search || endpoint.hash
    || (endpoint.protocol !== 'https:' && !(allowLoopbackHttp && endpoint.protocol === 'http:' && ['127.0.0.1', '[::1]'].includes(endpoint.hostname)))) throw new Error('NODE_GATEWAY_HTTPS_REQUIRED');
  return Object.freeze({ ...value, config_hash: hash(bytes) });
}

export async function loadWorkerFiles({ configFile, nodeTokenFile, relayTokenFile, publicKeyFile }, options) {
  const config = parseWorkerConfig(await readNodeFile(configFile), options);
  const nodeToken = (await readNodeFile(nodeTokenFile, { secret: true, maxBytes: 512 })).toString().trim();
  const relayToken = (await readNodeFile(relayTokenFile, { secret: true, maxBytes: 512 })).toString().trim();
  if (![nodeToken, relayToken].every(token => /^[A-Za-z0-9_-]{32,256}$/.test(token))) throw new Error('NODE_TOKEN_INVALID');
  const key = createPublicKey(await readNodeFile(publicKeyFile));
  if (key.asymmetricKeyType !== 'ed25519') throw new Error('NODE_ROUTE_KEY_INVALID');
  return { config, nodeToken, relayToken };
}
