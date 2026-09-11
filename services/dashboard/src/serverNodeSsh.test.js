import assert from 'node:assert/strict';
import test from 'node:test';
import ssh2 from 'ssh2';
import { mkdtemp, rm, readdir, readFile, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createNodeSsh, generateNodeKey } from './serverNodeSsh.js';

test('invalid generated key pairs are rejected before persistence and failure is bounded', () => {
  const valid = generateNodeKey();
  let calls = 0;
  const generated = generateNodeKey('test', () => ++calls === 1 ? { public: 'invalid', private: 'invalid' } : valid);
  assert.equal(generated, valid);
  assert.equal(calls, 2);
  calls = 0;
  assert.throws(() => generateNodeKey('test', () => { calls++; return { public: 'invalid', private: 'invalid' }; }), /生成校验失败/);
  assert.equal(calls, 4);
  const second = generateNodeKey();
  assert.throws(() => generateNodeKey('test', () => ({ private: valid.private, public: second.public })), /生成校验失败/);
});

// A real SSH handshake verifies host pinning independently of bootstrap scripts.
test('SSH pins authenticated host keys and stores only per-node keys with restricted permissions', async t => {
  const stateDir = await mkdtemp(join(tmpdir(), 'qy-ssh-test-'));
  t.after(() => rm(stateDir, { recursive: true, force: true }));
  const password = 'test-only-secret';
  let hostKey = generateNodeKey().private;
  async function serve(port = 0) {
    const server = new ssh2.Server({ hostKeys: [hostKey] }, client => {
      client.on('error', () => {});
      client.on('authentication', context => context.method === 'password' && context.password === password ? context.accept() : context.reject());
    });
    await new Promise(resolve => server.listen(port, '127.0.0.1', resolve));
    return server;
  }
  let server = await serve();
  t.after(() => new Promise(resolve => server.close(resolve)));
  const port = server.address().port;
  const node = { id: 'ef7c8d30-0cfb-4909-a641-16187269cf98', host: '127.0.0.1', port, username: 'ubuntu' };
  const ssh = createNodeSsh({ stateDir });
  await assert.rejects(() => ssh.connect(node, { password: 'wrong' }), /SSH 登录失败/);
  assert.ok(!(await readdir(stateDir)).some(name => name.endsWith('.host')), 'unauthenticated peer must not become trusted');
  const connection = await ssh.connect(node, { password });
  ssh.close(connection);
  await new Promise(resolve => server.close(resolve));
  for (const file of await readdir(stateDir)) {
    assert.equal((await stat(join(stateDir, file))).mode & 0o777, 0o600);
    assert.ok(!(await readFile(join(stateDir, file), 'utf8')).includes(password));
  }
  hostKey = generateNodeKey().private;
  server = await serve(port);
  await assert.rejects(() => ssh.connect(node, { password }), /SSH 连接失败/);
});
