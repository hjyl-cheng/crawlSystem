import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createNodeSsh } from './serverNodeSsh.js';
const port = Number(process.env.NODE_DELETION_TEST_SSH_PORT);
const quote = value => "'" + value.replaceAll("'", "'\\''") + "'";
test('unused node deletion over real SSH preserves data and permits new identity', { skip: !port }, async t => {
  assert.ok(Number.isInteger(port) && port > 1024);
  const password = 'runtime-fixture-only!$';
  const stateDir = await mkdtemp(join(tmpdir(), 'qy-delete-ssh-'));
  t.after(() => rm(stateDir, { recursive: true, force: true }));
  const node = { id: 'bd4d8455-7881-43be-ae52-18cfcf160514', host: '127.0.0.1', port, username: 'ubuntu' };
  const ssh = createNodeSsh({ stateDir });
  const connection = await ssh.connect(node, { password });
  t.after(() => ssh.close(connection));
  const exec = (command, input = '') => new Promise((resolve, reject) => connection.client.exec(command, (error, stream) => {
    if (error) return reject(error);
    let output = '';
    stream.on('data', data => { output += data; }); stream.stderr.resume();
    stream.on('close', code => code === 0 ? resolve(output.trim()) : reject(new Error(`fixture command failed: ${code}`)));
    stream.on('error', reject); stream.end(input);
  }));
  assert.equal(await exec('cat /test-state/fixture-only'), 'qy-node-runtime-fixture-v1');
  const root = cmd => exec(`sudo -S -p '' sh -c ${quote(cmd)}`, `${password}\n`);
  await ssh.verify(connection, password);
  await ssh.installKey(connection);
  const keyConnection = await ssh.connect(node, { keyOnly: true }); ssh.close(keyConnection);
  await root(`mkdir -p /etc/qy-node/runtime /var/lib/qy-node/runtime/spool /etc/qy-node/runtime/deployments
printf '%s' ${quote(node.id)} > /etc/qy-node/runtime/node-id
printf 'ExecStart=/opt/qy-node/beszel-agent\\nEnvironmentFile=/etc/qy-node/beszel.env\\n' > /etc/systemd/system/qy-beszel-agent.service
printf 'test-token' > /etc/qy-node/beszel.env
touch /test-state/monitoring-active
ln -s /fixture/mock-command /usr/local/bin/docker`);
  const check = mode => ssh.removeUnusedNode(connection, node, password, mode);
  await check('check');
  assert.equal(await root('cat /etc/qy-node/runtime/node-id'), node.id, 'preview is read-only');
  await root('touch /test-state/has-containers');
  await assert.rejects(() => check('cleanup'), /仍有容器/);
  assert.equal(await root('cat /etc/qy-node/beszel.env'), 'test-token');
  await root('rm /test-state/has-containers; touch /test-state/docker-unavailable');
  await assert.rejects(() => check('cleanup'), /Docker/);
  await root('rm /test-state/docker-unavailable; printf preserve > /var/lib/qy-node/runtime/spool/result');
  await assert.rejects(() => check('cleanup'), /暂存数据/);
  assert.equal(await root('cat /var/lib/qy-node/runtime/spool/result'), 'preserve');
  await root('rm /var/lib/qy-node/runtime/spool/result; mkdir /etc/qy-node/runtime/deployments/unused');
  await assert.rejects(() => check('cleanup'), /部署文件/);
  await root('rmdir /etc/qy-node/runtime/deployments/unused');
  await assert.rejects(() => ssh.removeUnusedNode(connection, { ...node, id: 'cd4d8455-7881-43be-ae52-18cfcf160514' }, password, 'cleanup'), /另一个节点/);
  await check('cleanup');
  await check('cleanup'); // retry after interrupted registry/monitoring update
  assert.equal(await root('test ! -e /etc/qy-node/runtime/node-id && test ! -e /etc/qy-node/beszel.env && test ! -e /test-state/monitoring-active && echo cleaned'), 'cleaned');
  assert.equal(await root('test -x /usr/local/bin/docker && echo retained'), 'retained');
  await ssh.removeUnusedNode(connection, { ...node, id: 'cd4d8455-7881-43be-ae52-18cfcf160514' }, password, 'check');
  const log = await root('cat /test-state/commands');
  assert.ok(!log.includes(password));
  assert.doesNotMatch(log, /docker.* (rm|stop|prune)\b/);
});
