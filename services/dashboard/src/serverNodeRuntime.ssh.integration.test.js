import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { buildNodeCollectDeployment } from './nodeRuntime/collectDeployment.js';
import {randomUUID,randomBytes,generateKeyPairSync} from 'node:crypto';
import { createNodeSsh } from './serverNodeSsh.js';

// Only point this at test-fixtures/node-runtime, bound on loopback. No real
// Docker daemon/socket or package manager is used by that SSH container.
const port = Number(process.env.NODE_RUNTIME_TEST_SSH_PORT);
const quote = value => "'" + value.replaceAll("'", "'\\''") + "'";
test('runtime script over real SSH, key login, SFTP and sudo in disposable fixture', { skip: !port }, async t => {
  assert.ok(Number.isInteger(port) && port > 1024 && port < 65536);
  const password = 'runtime-fixture-only!$';
  const stateDir = await mkdtemp(join(tmpdir(), 'qy-runtime-ssh-'));
  t.after(() => rm(stateDir, { recursive: true, force: true }));
  const node = { id: 'bd4d8455-7881-43be-ae52-18cfcf160514', host: '127.0.0.1', port, username: 'ubuntu' };
  const ssh = createNodeSsh({ stateDir });
  let connection = await ssh.connect(node, { password });
  t.after(() => ssh.close(connection));
  const exec = (command, input = '') => new Promise((resolve, reject) => {
    connection.client.exec(command, (error, channel) => {
      if (error) return reject(error);
      let output = '';
      channel.on('data', data => { output += data; });
      channel.stderr.resume();
      channel.on('error', reject);
      channel.on('close', code => code === 0 ? resolve(output.trim()) : reject(new Error(`fixture control failed: ${code}`)));
      channel.end(input);
    });
  });
  // Confirm the fixture identity before any mutation or script upload.
  assert.equal(await exec('cat /test-state/fixture-only'), 'qy-node-runtime-fixture-v1');
  const root = command => exec(`sudo -S -p '' sh -c ${quote(command)}`, `${password}\n`);
  await ssh.verify(connection, password);
  await ssh.installKey(connection);
  ssh.close(connection);
  connection = await ssh.connect(node, { keyOnly: true });
  await ssh.verify(connection, password);
  const prepare = (target = node) => {
    const steps = [];
    return ssh.prepareRuntime(connection, target, password, async step => steps.push(`start:${step}`), async step => steps.push(`done:${step}`)).then(details => ({ steps, details }));
  };
  let installed;
  await t.test('fresh environment installs Docker and Compose, verifies protected directories', async () => {
    const { steps, details } = await prepare();
    assert.deepEqual(steps, ['start:check', 'done:check', 'start:docker', 'done:docker', 'start:layout', 'done:layout', 'start:verify', 'done:verify']);
    assert.equal(details.dockerVersion, '28.3.2');
    assert.equal(details.composeVersion, 'v2.39.2');
    assert.match(details.scriptSha256, /^[a-f0-9]{64}$/);
    installed = `/opt/qy-node/runtime/bootstrap/${details.scriptSha256}.sh`;
    assert.equal(await root(`stat -c '%a:%u' ${quote(installed)}`), '700:0');
    assert.equal(await root('stat -c %a:%u /var/lib/qy-node/runtime/spool /etc/qy-node/runtime/secrets'), '700:0\n700:0');
    assert.equal(await root('cat /etc/qy-node/runtime/node-id'), node.id);
    const log = await root('cat /test-state/commands');
    assert.match(log, /apt-get .*install.*docker-ce/);
    assert.match(log, /curl .*https:\/\/download.docker.com\/linux\/debian\/gpg/);
    assert.match(log, /systemctl start docker/);
    assert.ok(!log.includes(password));
  });
  await t.test('repeat preparation reuses existing engine and preserves spool contents', async () => {
    await root('printf existing-result > /var/lib/qy-node/runtime/spool/retained-result; : > /test-state/commands');
    await prepare();
    assert.equal(await root('cat /var/lib/qy-node/runtime/spool/retained-result'), 'existing-result');
    const log = await root('cat /test-state/commands');
    assert.doesNotMatch(log, /apt-get|systemctl start|systemctl restart|docker .* (run|pull|rm|stop)( |$)/m);
    assert.equal(await exec('find /tmp -maxdepth 1 -type d -name "qy-runtime-*"'), '');
  });
  await t.test('duplicate node identity fails without installing or changing ownership', async () => {
    await root(': > /test-state/commands');
    await assert.rejects(() => prepare({ ...node, id: 'cd4d8455-7881-43be-ae52-18cfcf160514' }), /属于另一个节点/);
    assert.equal(await root('cat /etc/qy-node/runtime/node-id'), node.id);
    assert.equal(await root('cat /test-state/commands'), '');
  });
  await t.test('apt failure and invalid key do not mark Docker complete; retry succeeds', async () => {
    await root('rm /usr/local/bin/docker /test-state/compose /test-state/docker-running /etc/apt/sources.list.d/qy-node-docker.list; touch /test-state/fail-apt');
    const completed = [];
    await assert.rejects(() => ssh.prepareRuntime(connection, node, password, async () => {}, async step => completed.push(step)), /软件源更新失败/);
    assert.deepEqual(completed, ['check']);
    await root('rm /test-state/fail-apt; touch /test-state/bad-key');
    await assert.rejects(() => prepare(), /密钥校验失败/);
    await root('rm /test-state/bad-key');
    assert.equal((await prepare()).details.revision, 1);
  });
  await t.test('conflicting runtime is not replaced', async () => {
    await root('rm /usr/local/bin/docker; touch /usr/local/bin/containerd; chmod 755 /usr/local/bin/containerd; : > /test-state/commands');
    await assert.rejects(() => prepare(), /已有其他容器运行时/);
    assert.equal(await root('cat /test-state/commands'), '');
    await root('rm /usr/local/bin/containerd; ln -s /fixture/mock-command /usr/local/bin/docker');
  });
  await t.test('script refuses wrong system and invalid directory permissions', async () => {
    await root('cp /etc/os-release /test-state/os-release; printf "ID=unknown\\nVERSION_ID=1\\n" > /etc/os-release');
    await assert.rejects(() => prepare(), /目前支持 Ubuntu/);
    await root('cp /test-state/os-release /etc/os-release; chmod 755 /var/lib/qy-node/runtime/spool');
    await assert.rejects(() => root(`sh ${quote(installed)} verify ${quote(node.id)}`), /fixture control failed/);
    await prepare();
    assert.equal(await root('cat /var/lib/qy-node/runtime/spool/retained-result'), 'existing-result');
  });
  const deploymentId=randomUUID();
  const plan=count=>buildNodeCollectDeployment({node:{...node,kind:'execution',provisioning:{state:'ready'},runtime:{state:'ready'},workers:[{role:'incremental',count}]},
    image:'fixture.example/collect@sha256:'+'a'.repeat(64),gatewayUrl:'https://fixture.example',deploymentId});
  const credentials={nodeId:node.id,deploymentId,nodeToken:randomBytes(32).toString('hex'),
    publicKey:generateKeyPairSync('ed25519').publicKey.export({type:'spki',format:'pem'}),
    relayTokens:{'incremental-1':randomBytes(32).toString('hex'),'incremental-2':randomBytes(32).toString('hex')}};
  const deploy=async recipe=>{
    const steps=[];await ssh.deployWorkers(connection,node,recipe,credentials,password,async step=>steps.push('start:'+step),async step=>steps.push('done:'+step));return steps;
  };
  const deploymentRoot='/etc/qy-node/runtime/deployments/'+deploymentId;
  await t.test('fixed installer deploys collecting containers and keeps secrets private',async()=>{
    assert.deepEqual(await deploy(plan(1)),['start:files','done:files','start:start','done:start','start:verify','done:verify']);
    assert.equal(await root(`stat -c '%a:%u' ${quote(deploymentRoot+'/node-token')}`),'600:1000');
    assert.equal(await root("stat -c '%a:%u' /var/lib/qy-node/spool/incremental-1"),'700:1000');
    assert.equal(await root('cat '+quote(deploymentRoot+'/incremental-1.json')),plan(1).files['incremental-1.json'].trim());
    const commands=await root('cat /test-state/commands');
    assert.match(commands,/up -d --no-recreate --pull never/);
    assert.ok(!commands.includes(credentials.nodeToken));
    assert.equal(await exec('find /tmp -maxdepth 1 -type d -name "qy-workers-*"'),'');
  });
  await t.test('private registry pull uses temporary root-only config and cleans it after failure',async()=>{
    credentials.registry={server:'fixture.example',username:'node-pull',password:'d'.repeat(64)};
    await deploy(plan(1));
    const commands=await root('cat /test-state/commands');
    assert.match(commands,/--config \/run\/qy-registry-/);
    assert.ok(!commands.includes(credentials.registry.password));
    assert.equal(await root('find /run -maxdepth 1 -name "qy-registry-*"'),'');
    await root('touch /test-state/fail-pull');
    await assert.rejects(deploy(plan(1)));
    assert.equal(await root('find /run -maxdepth 1 -name "qy-registry-*"'),'');
    await root('rm /test-state/fail-pull');
    credentials.registry.server='wrong.example';await assert.rejects(deploy(plan(1)));
    delete credentials.registry;
  });
  await t.test('retry and additive expansion preserve configuration and spool',async()=>{
    await root('printf retained > /var/lib/qy-node/spool/incremental-1/result');
    await deploy(plan(1));await deploy(plan(2));
    assert.equal(await root('cat /var/lib/qy-node/spool/incremental-1/result'),'retained');
    assert.equal(await root('cat '+quote(deploymentRoot+'/incremental-1.json')),plan(1).files['incremental-1.json'].trim());
    await root('touch /test-state/fail-pull');
    await assert.rejects(deploy(plan(2)));
    await root('rm /test-state/fail-pull');await deploy(plan(2));
    assert.equal(await root('cat /var/lib/qy-node/spool/incremental-1/result'),'retained');
  });
  await t.test('wrong identity, altered config and hostile volume are rejected before Docker start',async()=>{
    const original=credentials.nodeToken;credentials.nodeToken='c'.repeat(64);
    await assert.rejects(deploy(plan(2)));credentials.nodeToken=original;
    const bad=plan(2);bad.compose.services['incremental-1'].volumes[0].source='/etc/shadow';
    await assert.rejects(deploy(bad));
    await root('printf wrong-node > /etc/qy-node/runtime/node-id');
    await assert.rejects(deploy(plan(2)));
    await root('printf '+quote(node.id)+' > /etc/qy-node/runtime/node-id');
    assert.equal(await root('cat '+quote(deploymentRoot+'/node-token')),original);
    await deploy(plan(2));
  });

});
