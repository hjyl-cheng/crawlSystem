import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync } from 'node:crypto';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import net from 'node:net';
import { createRemoteRouteIssuer } from '../src/remoteNodes/routeGrant.js';
import { createLocalRotaClient } from '../src/remoteNodes/localRotaClient.js';

test('Node center -> signed route -> real Go relay -> selected upstream, never center transit',
  { skip: !process.env.REMOTE_NODE_ROTA_TEST_BINARY, timeout: 20000 }, async t => {
    const folder = await mkdtemp(join(tmpdir(), 'remote-rota-'));
    t.after(() => rm(folder, { recursive: true, force: true }));
    const pair = generateKeyPairSync('ed25519');
    await writeFile(join(folder, 'public.pem'), pair.publicKey.export({ type: 'spki', format: 'pem' }), { mode: 0o600 });
    const token = 'local-control-token-'.repeat(3);
    await writeFile(join(folder, 'token'), token, { mode: 0o600 });
    const child = spawn(process.env.REMOTE_NODE_ROTA_TEST_BINARY, ['-node-id', 'node-a', '-public-key-file', join(folder, 'public.pem'),
      '-control-token-file', join(folder, 'token'), '-proxy-listen', '127.0.0.1:0', '-control-listen', '127.0.0.1:0'],
    { env: { PATH: process.env.PATH, GOMAXPROCS: '2' }, stdio: ['ignore', 'pipe', 'pipe'] });
    const exited = once(child, 'exit');
    t.after(async () => { if (child.exitCode === null) child.kill('SIGTERM'); await exited; });
    let stderr = '';
    child.stderr.on('data', chunk => { stderr = (stderr + chunk).slice(-2000); });
    const lines = createInterface({ input: child.stdout });
    const startup = await Promise.race([
      once(lines, 'line', { signal: AbortSignal.timeout(5000) }).then(([line]) => JSON.parse(line)),
      exited.then(() => { throw new Error(`relay exited before startup: ${stderr}`); }),
    ]);
    assert.equal(startup.event, 'node_forward_ready');
    const client = createLocalRotaClient({ nodeId: 'node-a', token, proxyUrl: `http://${startup.proxy_address}`,
      controlUrl: `http://${startup.control_address}` });
    const boot = await client.boot();
    const calls = [];
    const sockets = new Set();
    const upstream = net.createServer(socket => {
      sockets.add(socket); socket.on('close', () => sockets.delete(socket));
      socket.on('error', () => {});
      let head = Buffer.alloc(0); let ready = false;
      socket.on('data', data => {
        if (ready) { socket.write(data); return; }
        head = Buffer.concat([head, data]);
        const end = head.indexOf('\r\n\r\n');
        if (end < 0) return;
        calls.push(head.subarray(0, end).toString());
        ready = true;
        socket.write('HTTP/1.1 200 Connection Established\r\n\r\nLOCAL-BR');
        if (head.length > end + 4) socket.write(head.subarray(end + 4));
      });
    });
    upstream.listen(0, '127.0.0.1'); await once(upstream, 'listening');
    t.after(() => { for (const socket of sockets) socket.destroy(); return new Promise(resolve => upstream.close(resolve)); });
    const lease = { task_id: 'channel-plan-a', generation: 1 };
    const state = { node_id: 'node-a', slot: 'worker-1', ...lease, epoch: 1, route_id: 'assigned-br', identity_id: 'br-profile',
      egress_country: 'BR', proxy_token: 'proxy-secret-'.repeat(4), task_lease_until_ms: Date.now() + 60000,
      route_lease_until_ms: Date.now() + 45000,
      upstream: { protocol: 'http', address: `127.0.0.1:${upstream.address().port}`, username: 'upstream-user', password: 'upstream-secret' } };
    const issue = createRemoteRouteIssuer({ privateKey: pair.privateKey, authorize: async () => state });
    const request = { node_id: 'node-a', slot: 'worker-1', ...lease, boot_id: boot.boot_id };
    const binding = { lease, slot: 'worker-1', bootId: boot.boot_id };
    const signed = await issue(request);
    const applied = await client.apply(signed, binding);
    assert.equal(applied.egressCountry, 'BR');
    assert.equal(applied.route_id, state.route_id);
    const endpoint = new URL(applied.proxyUrl);
    const socket = net.connect({ host: endpoint.hostname, port: Number(endpoint.port) });
    socket.on('error', () => {});
    t.after(() => socket.destroy());
    await once(socket, 'connect');
    const data = [];
    const received = new Promise(resolve => socket.on('data', chunk => { data.push(chunk); if (Buffer.concat(data).includes('LOCAL-BRhello')) resolve(); }));
    socket.write(`CONNECT www.youtube.com:443 HTTP/1.1\r\nHost: www.youtube.com:443\r\nProxy-Authorization: Basic ${Buffer.from(`${decodeURIComponent(endpoint.username)}:${decodeURIComponent(endpoint.password)}`).toString('base64')}\r\n\r\nhello`);
    await received;
    assert.equal(calls.length, 1);
    assert.ok(calls[0].includes(Buffer.from('upstream-user:upstream-secret').toString('base64')));
    assert.ok(!calls[0].includes(state.proxy_token));
    const tampered = { ...signed, signature: Buffer.alloc(64).toString('base64') };
    await assert.rejects(client.apply(tampered, binding), { message: 'LOCAL_ROTA_REJECTED', status: 409 });
    const renewal = await issue({ ...request, action: 'renew' });
    await client.apply(renewal, binding);
    const closed = once(socket, 'close');
    const revoked = await client.apply(await issue({ ...request, action: 'revoke' }), binding);
    assert.equal(revoked.proxyUrl, undefined);
    await closed;
    await assert.rejects(client.apply(signed, binding), { message: 'LOCAL_ROTA_REJECTED', status: 409 });
    child.kill('SIGTERM');
    const [code] = await exited;
    assert.equal(code, 0, stderr);
  });
