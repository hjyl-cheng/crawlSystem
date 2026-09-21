import {databaseUrl} from '../databaseConnection.js';

// Validate before importing/constructing the embedded local worker. Its legacy
// modules use the original process environment for DB, queues and local Rota.
export function fullCrawlReleaseConfig(env, {controlToken, profileSecret}) {
  const required = name => {
    const value = String(env[name] ?? '').trim();
    if (!value) throw new Error(`FULL_CRAWL_CONFIG_REQUIRED:${name}`);
    return value;
  };
  if (env.REMOTE_NODE_FULL_CRAWL_DEPLOYMENT_ENABLED !== 'true'
    || env.SKIP_SCHEMA_MIGRATION !== 'true' || env.YOUTUBEJS_EXTRACTOR_MODE !== 'full'
    || env.PROXY_SLOT_ROLE !== 'channel' || env.WORKER_QUEUES !== 'youtube-channel-crawl'
    || env.FULL_CRAWL_CANARY_WORKER === 'true' || env.ROTA_FIXED_PROXY_USER) {
    throw new Error('FULL_CRAWL_COMPATIBILITY_CONFIG_INVALID');
  }
  const allowedNodeIds = required('REMOTE_NODE_FULL_CRAWL_NODE_IDS').split(',').map(id => id.trim());
  if (allowedNodeIds.some(id => !/^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(id))
    || new Set(allowedNodeIds).size !== allowedNodeIds.length) throw new Error('FULL_CRAWL_ALLOWLIST_INVALID');
  const totalSlots = Number(required('REMOTE_NODE_FULL_CRAWL_TOTAL_SLOTS'));
  if (!Number.isSafeInteger(totalSlots) || totalSlots < 2) throw new Error('FULL_CRAWL_CAPACITY_INVALID');
  const prefix = required('REMOTE_NODE_QUEUE_PREFIX');
  const redis = new URL(required('REMOTE_NODE_REDIS_URL'));
  // Original queue producers have no username/TLS support. Reject unsupported
  // transports instead of silently putting continuations into another Redis.
  if (redis.protocol !== 'redis:' || redis.username || redis.search || redis.hash
    || !['', '/', '/0'].includes(redis.pathname)
    || redis.hostname !== (env.REDIS_HOST || '127.0.0.1')
    || Number(redis.port || 6379) !== Number(env.REDIS_PORT || 6379)
    || decodeURIComponent(redis.password) !== (env.REDIS_PASSWORD || '')
    || prefix !== env.BULLMQ_PREFIX) throw new Error('FULL_CRAWL_QUEUE_IDENTITY_MISMATCH');
  const target = env.REMOTE_NODE_TRANSACTION_DATABASE_URL || required('REMOTE_NODE_DATABASE_URL');
  if (new URL(databaseUrl(env)).href !== new URL(target).href) throw new Error('FULL_CRAWL_DATABASE_IDENTITY_MISMATCH');
  required('EXPECTED_CRAWLER_DATABASE');
  required('FORBIDDEN_CRAWLER_DATABASE');
  required('REMOTE_NODE_NATS_URL');
  required('REMOTE_NODE_FULL_CRAWL_IMAGE');
  required('ROTA_PROXY_BASE_URL');
  required('PROXY_WORKER_ID');
  if (env.ROTA_PROXY_CONTROL_TOKEN !== controlToken || controlToken.length < 12
    || (env.ROTA_BULLMQ_PROXY_PASSWORD || '').length < 12
    || (env.BROWSER_PROFILE_ENCRYPTION_KEY || env.ROTA_BULLMQ_PROXY_PASSWORD) !== profileSecret) {
    throw new Error('FULL_CRAWL_LOCAL_IDENTITY_MISMATCH');
  }
  return {allowedNodeIds, maxSlots: totalSlots - 1, totalSlots, compatibilitySlots: 1, prefix,
    connection: {host: redis.hostname, port: Number(redis.port || 6379),
      password: decodeURIComponent(redis.password) || undefined, maxRetriesPerRequest: null}};
}
