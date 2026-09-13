import { execFileSync, spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname } from 'node:path';
import { createServer } from 'node:net';
const cwd = dirname(fileURLToPath(import.meta.url));
const project = `qy-nats-probe-${process.pid}-${Date.now()}`;
const args = ['compose', '-f', `${cwd}/compose.yml`, '-p', project];
function docker(rest, options = {}) { return execFileSync('docker', [...args, ...rest], { cwd, encoding:'utf8', timeout:120000, ...options }); }
const port = await new Promise((resolve, reject) => { const server=createServer();server.once('error',reject);server.listen(0,'127.0.0.1',()=>{const value=server.address().port;server.close(()=>resolve(value));}); });
process.env.PROBE_NATS_PORT=String(port);
try {
  docker(['up', '-d', '--wait'], { stdio:'inherit' });
  const nats = docker(['port','broker','4222']).trim();
  const postgres = docker(['port','postgres','5432']).trim();
  if (![nats,postgres].every(value => /^127\.0\.0\.1:\d+$/.test(value))) throw Error('Loopback-only experiment required: '+JSON.stringify({nats,postgres}));
  await new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['verify.mjs'], {cwd,stdio:'inherit',timeout:180000,env:{...process.env,
      PROBE_NATS_URL:`nats://${nats}`,PROBE_PG_URL:`postgresql://probe:isolated-probe-only@${postgres}/remote_node_ingestion_test`,PROBE_COMPOSE_PROJECT:project}});
    child.once('error',reject);child.once('exit',code=>code===0?resolve():reject(Error(`Verification failed: ${code}`)));
  });
} finally {
  // Only resources created by this invocation; no production container changes.
  docker(['down', '-v', '--remove-orphans'], {stdio:'inherit'});
}
