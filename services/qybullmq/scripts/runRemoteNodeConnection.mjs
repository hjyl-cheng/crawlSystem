import { runNodeConnection, checkNodeConnectionHealth } from '../src/remoteNodes/nodeConnectionRuntime.js';

if (process.argv[2] === '--healthcheck') {
  try { await checkNodeConnectionHealth(); } catch { process.exitCode = 1; }
} else if (process.argv.length !== 2) {
  process.exitCode = 64;
} else {
  const abort = new AbortController();
  process.once('SIGTERM', () => abort.abort());
  process.once('SIGINT', () => abort.abort());
  try {
    let previous;
    await runNodeConnection({ signal: abort.signal, onStatus(status) {
      if (status.state !== previous) console.log(JSON.stringify({ event: 'remote_node_connection', ...status }));
      previous = status.state;
    } });
  } catch {
    // Exception causes may contain transport URLs; expose only a fixed code.
    console.error(JSON.stringify({ event: 'remote_node_connection_failed' }));
    process.exitCode = 1;
  }
}
