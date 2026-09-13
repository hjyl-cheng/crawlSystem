// Run from a trusted diagnostic environment with Rota credentials already set.
// No production database connection is read. Application data goes to the
// disposable PostgreSQL/NATS services created by testRemoteNatsDocker.
import { parseArgs } from 'node:util';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';

const { values } = parseArgs({ options: {
  'channel-id': { type: 'string', multiple: true },
  report: { type: 'string' },
} });
const ids = values['channel-id'] ?? [];
if (!ids.length || ids.length > 3 || !ids.every(id => /^UC[\w-]{22}$/.test(id)) || !values.report) {
  throw new Error('Usage: node scripts/compareRemoteWholeChannelLive.mjs --channel-id UC... [--channel-id UC...] --report /private/path.json');
}
for (const name of ['ROTA_PROXY_CONTROL_URL', 'ROTA_PROXY_CONTROL_TOKEN', 'ROTA_PROXY_BASE_URL',
  'ROTA_BULLMQ_PROXY_PASSWORD', 'ROTA_IDENTITY_POLICY_ID', 'ROTA_WORKLOAD_SCOPE_EXPECTED']) {
  if (!process.env[name]) throw new Error(`Missing diagnostic configuration: ${name}`);
}
process.env.REMOTE_WHOLE_LIVE = 'true';
process.env.REMOTE_WHOLE_LIVE_CHANNELS = ids.join(',');
process.env.REMOTE_WHOLE_LIVE_REPORT = resolve(values.report);
process.env.YOUTUBEJS_EXTRACTOR_MODE = 'full';
// This comparison measures successful YouTubeJS collection. Failure/API recovery
// has separate managed integration tests; do not spend production API quota here.
process.env.YOUTUBEJS_VIDEO_API_BATCH_FALLBACK = 'false';
process.env.INCREMENTAL_DISCOVERY_MAX_PAGES = '1';
process.env.INCREMENTAL_DISCOVERY_CATCH_UP_MAX_ITEMS = '3';
process.chdir(fileURLToPath(new URL('..', import.meta.url)));
process.argv = [process.execPath, process.argv[1], 'test/remoteWholeChannel.live.integration.test.js'];
await import('./testRemoteNatsDocker.mjs');
