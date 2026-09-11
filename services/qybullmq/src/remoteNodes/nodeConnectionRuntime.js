import { spawn } from 'node:child_process';
import { mkdir, rename, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { uptime } from 'node:os';
import { randomUUID } from 'node:crypto';
import { createRemoteNodeClient } from './client.js';
import { createLocalRotaClient } from './localRotaClient.js';
import { loadWorkerFiles, readNodeFile } from './workerConfig.js';
import { RemoteWorkerConnection } from './workerConnection.js';

export const nodeConnectionFiles = Object.freeze({ configFile: '/run/secrets/node-config.json', nodeTokenFile: '/run/secrets/node-token',
  relayTokenFile: '/run/secrets/relay-token', publicKeyFile: '/run/secrets/route-public.pem' });
export const nodeHealthFile = '/run/qy-node/health.json';

export async function checkNodeConnectionHealth(path = nodeHealthFile) {
  const state = JSON.parse((await readNodeFile(path, { maxBytes: 4096 })).toString());
  if (state.version !== 1 || state.state !== 'connected_waiting_activation' || state.ready_for_tasks !== false
    || !Number.isFinite(state.valid_until_uptime) || state.valid_until_uptime <= uptime()) throw new Error('NODE_CONNECTION_UNHEALTHY');
}

export async function runNodeProcess({ files = nodeConnectionFiles, healthFile = nodeHealthFile,
  relayBinary = '/usr/local/bin/node-forward', signal, onStatus = () => {},
  loadFiles = loadWorkerFiles, runWorker } = {}) {
  if (!signal) throw new TypeError('shutdown signal required');
  const { config, nodeToken, relayToken } = await loadFiles(files);
  await mkdir(dirname(healthFile), { recursive: true, mode: 0o700 });
  const report = async state => {
    const path = `${healthFile}.${randomUUID()}.tmp`;
    await writeFile(path, JSON.stringify(state), { mode: 0o600 });
    await rename(path, healthFile);
    onStatus({ state: state.state, ready_for_tasks: state.ready_for_tasks });
  };
  await report({ version: 1, state: 'starting', ready_for_tasks: false, valid_until_uptime: 0 });
  const abort = new AbortController();
  const executionSignal = AbortSignal.any([signal, abort.signal]);
  const relay = spawn(relayBinary, ['-node-id', config.node_id, '-slots', config.slot,
    '-public-key-file', files.publicKeyFile, '-control-token-file', files.relayTokenFile,
    '-proxy-listen', '127.0.0.1:8000', '-control-listen', '127.0.0.1:8001'], {
    stdio: ['ignore', 'pipe', 'pipe'], env: { PATH: process.env.PATH, GOMAXPROCS: '2' },
  });
  // Node/route credentials never go into child argv, stdout or error logs.
  relay.stderr.resume();
  const exited = new Promise(resolve => {
    relay.once('exit', code => { abort.abort(new Error('NODE_RELAY_EXITED')); resolve(code); });
    relay.once('error', () => { abort.abort(new Error('NODE_RELAY_START_FAILED')); resolve(-1); });
  });
  try {
    let output = '';
    await new Promise((resolve, reject) => {
      let settled = false;
      const finish = error => {
        if (settled) return;
        settled = true; clearTimeout(timeout); executionSignal.removeEventListener('abort', stopped);
        error ? reject(error) : resolve();
      };
      const timeout = setTimeout(() => finish(new Error('NODE_RELAY_START_TIMEOUT')), 10000);
      const stopped = () => finish(new Error('NODE_START_ABORTED'));
      executionSignal.addEventListener('abort', stopped, { once: true });
      relay.stdout.on('data', chunk => {
        if (settled) return;
        output += chunk.toString();
        if (output.length > 4096) { finish(new Error('NODE_RELAY_START_INVALID')); return; }
        if (!output.includes('\n')) return;
        try {
          const event = JSON.parse(output.split('\n')[0]);
          if (event.event !== 'node_forward_ready' || event.node_id !== config.node_id
            || event.proxy_address !== '127.0.0.1:8000' || event.control_address !== '127.0.0.1:8001') throw new Error();
          finish();
        } catch { finish(new Error('NODE_RELAY_START_INVALID')); }
      });
      if (executionSignal.aborted) stopped();
    });
    const client = createRemoteNodeClient({ url: config.gateway_url, token: nodeToken });
    const localRota = createLocalRotaClient({ controlUrl: 'http://127.0.0.1:8001', proxyUrl: 'http://127.0.0.1:8000', token: relayToken, nodeId: config.node_id });
    await runWorker({ config, client, localRota, report, signal: executionSignal });
    if (abort.signal.aborted && !signal.aborted) throw new Error('NODE_RELAY_EXITED');
  } finally {
    if (relay.exitCode === null) relay.kill('SIGTERM');
    const timer = setTimeout(() => relay.kill('SIGKILL'), 6000);
    await exited; clearTimeout(timer);
    await report({ version: 1, state: 'stopped', ready_for_tasks: false, valid_until_uptime: 0 });
  }
}

export function runNodeConnection(options = {}) {
  return runNodeProcess({ ...options,
    loadFiles: loadWorkerFiles,
    runWorker: ({ signal, ...args }) => new RemoteWorkerConnection(args).run({ signal }),
  });
}
