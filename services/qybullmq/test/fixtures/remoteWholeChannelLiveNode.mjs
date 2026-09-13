// One temporary remote process, using the production collector/network adapters.
// Only an isolated center's token, route public key and broker CA are mounted.
import { readFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import { setTimeout as delay } from 'node:timers/promises';
import { performance } from 'node:perf_hooks';
import { hostname, totalmem } from 'node:os';
import { createRemoteNatsClient } from '../../src/remoteNodes/natsClient.js';
import { createLocalRotaClient } from '../../src/remoteNodes/localRotaClient.js';
import { createRemoteIncrementalWorker } from '../../src/remoteNodes/incrementalWorker.js';
import { RemoteResultSpool } from '../../src/remoteNodes/spool.js';
import { acquireYoutubeJs, releaseYoutubeJs, closeYoutubeJs,
  openYoutubeJsChannel, fetchYoutubeJsVideoDetail } from '../../src/youtubeJs.js';

const root = '/run/live-test';
const config = JSON.parse(await readFile(`${root}/config.json`, 'utf8'));
if (process.env.DATABASE_URL || process.env.POSTGRES_PASSWORD || !config.isolated) throw new Error('ISOLATED_NODE_REQUIRED');
const token = (await readFile(`${root}/token`, 'utf8')).trim();
const metrics = { runtime: process.version, hostname: hostname(), host_memory: totalmem(),
  transport: 'nats', mode: config.mode, operations: [], rpc: {} };
const start = performance.now();
let client, worker;
const relay = spawn('/usr/local/bin/node-forward', ['-node-id', config.nodeId, '-slots', 'worker-1',
  '-public-key-file', `${root}/route.pem`, '-control-token-file', `${root}/token`,
  '-proxy-listen', '127.0.0.1:0', '-control-listen', '127.0.0.1:0'],
{ env: { PATH: process.env.PATH, GOMAXPROCS: '1' }, stdio: ['ignore', 'pipe', 'pipe'] });
relay.stderr.resume();
const exited = once(relay, 'exit');
try {
  const lines = createInterface({ input: relay.stdout });
  const [line] = await once(lines, 'line', { signal: AbortSignal.timeout(10000) });
  const ready = JSON.parse(line);
  if (ready.event !== 'node_forward_ready') throw new Error('RELAY_NOT_READY');
  client = await createRemoteNatsClient({ url: config.url, nodeId: config.nodeId, slot: 'worker-1', token,
    tls: { caFile: `${root}/ca.pem` } });
  const tracked = { ...client };
  for (const [name, method] of Object.entries(client)) {
    if (typeof method !== 'function' || name === 'close') continue;
    tracked[name] = async (...args) => {
      const at = performance.now();
      const metric = metrics.rpc[name] ??= { calls: 0, ms: 0 };
      metric.calls++;
      try { return await method(...args); } finally { metric.ms += performance.now() - at; }
    };
  }
  const timed = async (name, id, action) => {
    const op = { name, id, start_ms: performance.now() - start };
    metrics.operations.push(op);
    try { return await action(); }
    catch (error) { op.error = String(error.code || error.name); throw error; }
    finally { op.end_ms = performance.now() - start; op.ms = op.end_ms - op.start_ms; }
  };
  const youtube = { acquire: acquireYoutubeJs, release: releaseYoutubeJs, close: closeYoutubeJs,
    openChannel: async (id, options) => {
      const snapshot = await timed('about', id, () => openYoutubeJsChannel(id, options));
      return { ...snapshot, scanUploads: options => timed('scan', id, () => snapshot.scanUploads(options)) };
    }, fetchDetail: (id, options) => timed('detail', id, () => fetchYoutubeJsVideoDetail(id, options)) };
  worker = createRemoteIncrementalWorker({ client: tracked, slot: 'worker-1', youtube,
    spool: new RemoteResultSpool({ directory: '/tmp/whole-wan-spool', maxBytes: 256 * 1024 * 1024 }),
    localRota: createLocalRotaClient({ token, nodeId: config.nodeId,
      proxyUrl: `http://${ready.proxy_address}`, controlUrl: `http://${ready.control_address}` }),
    timeoutMs: 180000 });
  const deadline = Date.now() + 180000;
  for (;;) {
    if (Date.now() > deadline) throw new Error('LIVE_NODE_TIMEOUT');
    const status = await worker.runOnce();
    if (status === 'idle') { await delay(250); continue; }
    metrics.status = status;
    if (status !== 'applied') throw new Error(`LIVE_NODE_${status}`);
    break;
  }
} catch (error) {
  metrics.error = String(error.code || error.name);
  process.exitCode = 1;
} finally {
  worker?.stop();
  await client?.close();
  relay.kill('SIGTERM');
  const timer = setTimeout(() => relay.kill('SIGKILL'), 5000);
  await exited;
  clearTimeout(timer);
  metrics.total_ms = performance.now() - start;
  console.log(JSON.stringify({ event: 'whole_wan_node_result', metrics }));
}
