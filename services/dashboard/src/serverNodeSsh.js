import ssh2 from 'ssh2';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { beszelVersion } from './serverNodeMonitoring.js';

const quote = value => "'" + String(value).replaceAll("'", "'\\''") + "'";
const { Client, utils } = ssh2;
const uuidPattern = /^[a-f0-9-]{36}$/;
export function validateBootstrapPassword(password) {
  if (typeof password !== 'string' || password.length > 1024 || /[\r\n\0]/.test(password)) {
    throw Object.assign(new Error('密码格式不正确，不支持换行或超过 1024 个字符'), { statusCode: 400 });
  }
  return password;
}

export function createNodeSsh({ stateDir, fetchImpl = fetch }) {
  async function keyFor(id) {
    if (!uuidPattern.test(id)) throw new Error('节点标识无效');
    await mkdir(stateDir, { recursive: true, mode: 0o700 });
    const file = join(stateDir, `${id}.key`);
    try { return JSON.parse(await readFile(file, 'utf8')); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    const key = utils.generateKeyPairSync('ed25519', { comment: `qy-managed-${id}` });
    try { await writeFile(file, JSON.stringify(key), { mode: 0o600, flag: 'wx' }); }
    catch (error) { if (error.code !== 'EEXIST') throw error; }
    return JSON.parse(await readFile(file, 'utf8'));
  }
  async function connect(node, { password, keyOnly = false } = {}) {
    const key = await keyFor(node.id);
    const pinFile = join(stateDir, `${node.id}.host`);
    let known;
    try { known = (await readFile(pinFile, 'utf8')).trim(); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    let fingerprint;
    const client = new Client();
    await new Promise((resolve, reject) => {
      client.once('ready', resolve);
      client.on('error', error => reject(new Error(error.level === 'client-authentication' ? 'SSH 登录失败，请检查用户名和密码' : 'SSH 连接失败，请检查地址、端口、主机密钥及网络')));
      client.once('close', () => reject(new Error('SSH 连接已关闭')));
      client.connect({ host: node.host, port: node.port, username: node.username,
        privateKey: key.private, ...(!keyOnly && password ? { password } : {}),
        readyTimeout: 20000, keepaliveInterval: 10000, keepaliveCountMax: 3,
        hostHash: 'sha256', hostVerifier(hash) { fingerprint = hash; return !known || known === hash; },
      });
    }).catch(error => { client.destroy(); throw error; });
    if (!known) {
      try { await writeFile(pinFile, fingerprint, { mode: 0o600, flag: 'wx' }); }
      catch (error) {
        if (error.code !== 'EEXIST' || (await readFile(pinFile, 'utf8')).trim() !== fingerprint) { client.destroy(); throw new Error('SSH 主机密钥已变化'); }
      }
    }
    return { client, key, fingerprint };
  }
  async function exec(connection, command, { input = '', timeout = 20000 } = {}) {
    return new Promise((resolve, reject) => {
      let output = '';
      let stream;
      const timer = setTimeout(() => { stream?.close(); connection.client.destroy(); reject(new Error('远程操作超时，请检查服务器网络和服务状态')); }, timeout);
      const done = (error, result) => { clearTimeout(timer); error ? reject(error) : resolve(result); };
      connection.client.exec(command, (error, channel) => {
        if (error) return done(new Error('无法执行远程初始化操作'));
        stream = channel;
        channel.on('data', data => { if (output.length < 8192) output += data.toString(); });
        channel.stderr.resume(); // Never retain remote output that could contain credentials.
        channel.on('error', () => done(new Error('远程操作连接中断')));
        channel.on('close', code => code === 0 ? done(null, output.trim()) : done(new Error(`远程操作失败（退出码 ${code ?? '未知'}），请检查 sudo 权限、systemd 或安装环境`)));
        channel.end(input);
      });
    });
  }
  async function rootExec(connection, script, password, timeout = 20000) {
    const command = `timeout 180 sh -c ${quote(script)}`;
    if (connection.root) return exec(connection, command, { timeout });
    if (connection.passwordSudo) return exec(connection, `sudo -S -p '' ${command}`, { input: `${password}\n`, timeout });
    return exec(connection, `sudo -n ${command}`, { timeout });
  }
  async function verify(connection, password) {
    connection.root = (await exec(connection, 'id -u')) === '0';
    if (!connection.root) {
      try { await exec(connection, 'sudo -n true'); }
      catch {
        if (!password) throw new Error('sudo 需要密码，请填写服务器密码后重试');
        connection.passwordSudo = true;
        await rootExec(connection, 'true', password);
      }
    }
    const arch = await exec(connection, 'test "$(uname -s)" = Linux && command -v systemctl >/dev/null && test -d /run/systemd/system && uname -m');
    if (!['x86_64', 'aarch64'].includes(arch)) throw new Error('当前自动初始化支持使用 systemd 的 Linux x86_64 / arm64 服务器');
    return arch;
  }
  async function installKey(connection) {
    await exec(connection, `umask 077; mkdir -p "$HOME/.ssh"; touch "$HOME/.ssh/authorized_keys"; chmod 700 "$HOME/.ssh"; chmod 600 "$HOME/.ssh/authorized_keys"; grep -qxF ${quote(connection.key.public)} "$HOME/.ssh/authorized_keys" || printf '%s\\n' ${quote(connection.key.public)} >> "$HOME/.ssh/authorized_keys"`);
  }
  const artifacts = new Map();
  async function artifact(arch) {
    const name = `beszel-agent_linux_${arch === 'x86_64' ? 'amd64' : 'arm64'}.tar.gz`;
    if (!artifacts.has(name)) artifacts.set(name, (async () => {
      const base = `https://github.com/henrygd/beszel/releases/download/v${beszelVersion}`;
      const checks = await fetchImpl(`${base}/beszel_${beszelVersion}_checksums.txt`, { signal: AbortSignal.timeout(30000) });
      if (!checks.ok) throw new Error('无法下载 Beszel 校验清单');
      const line = (await checks.text()).split('\n').find(line => line.trim().split(/\s+/).at(-1) === name);
      const digest = line?.split(/\s+/)[0];
      if (!/^[a-f0-9]{64}$/.test(digest ?? '')) throw new Error('Beszel 安装包校验信息缺失');
      const response = await fetchImpl(`${base}/${name}`, { signal: AbortSignal.timeout(90000) });
      if (!response.ok) throw new Error('无法下载 Beszel 安装包');
      const buffer = Buffer.from(await response.arrayBuffer());
      if (createHash('sha256').update(buffer).digest('hex') !== digest) throw new Error('Beszel 安装包校验失败');
      return { buffer, digest };
    })().catch(error => { artifacts.delete(name); throw error; }));
    return artifacts.get(name);
  }
  async function installMonitoring(connection, node, arch, config, password) {
    if (!/^https?:\/\//.test(config.hubUrl) || /[\r\n"\\]/.test(config.hubUrl)) throw new Error('中心监控地址配置不正确');
    if (!/^ssh-ed25519 [A-Za-z0-9+/=]+(?: [^\r\n"]*)?$/.test(config.key) || !/^[a-zA-Z0-9-]+$/.test(config.token)) throw new Error('Beszel 返回的密钥或令牌格式不正确');
    const { buffer, digest } = await artifact(arch);
    const staging = `/tmp/qy-node-${randomUUID()}`;
    await exec(connection, `umask 077; mkdir ${quote(staging)}`);
    try {
      const sftp = await new Promise((resolve, reject) => {
        const timer = setTimeout(() => { connection.client.destroy(); reject(new Error('SFTP 连接超时')); }, 20000);
        connection.client.sftp((error, session) => { clearTimeout(timer); error ? reject(new Error('SFTP 连接失败')) : resolve(session); });
      });
      try {
        await new Promise((resolve, reject) => {
          const stream = sftp.createWriteStream(`${staging}/agent.tar.gz`, { mode: 0o600 });
          const timer = setTimeout(() => { connection.client.destroy(); reject(new Error('Beszel 安装包上传超时')); }, 60000);
          stream.on('error', () => { clearTimeout(timer); reject(new Error('Beszel 安装包上传失败')); });
          stream.on('close', () => { clearTimeout(timer); resolve(); }); stream.end(buffer);
        });
      } finally { sftp.end(); }
      const env = `KEY="${config.key}"\nTOKEN="${config.token}"\nHUB_URL="${config.hubUrl}"\nBESZEL_AGENT_DISABLE_SSH=true\nBESZEL_AGENT_DATA_DIR=/var/lib/qy-beszel\n`;
      const unit = `[Unit]\nDescription=QY node Beszel monitoring agent\nAfter=network-online.target\nWants=network-online.target\n[Service]\nUser=qy-beszel\nGroup=qy-beszel\nEnvironmentFile=/etc/qy-node/beszel.env\nExecStart=/opt/qy-node/beszel-agent\nWorkingDirectory=/var/lib/qy-beszel\nRestart=always\nRestartSec=5\nNoNewPrivileges=true\nProtectSystem=strict\nProtectHome=read-only\nReadWritePaths=/var/lib/qy-beszel\n[Install]\nWantedBy=multi-user.target\n`;
      await rootExec(connection, `set -eu
exec 9>/run/lock/qy-node-initialize.lock
flock -n 9
printf '%s  %s\\n' ${quote(digest)} ${quote(`${staging}/agent.tar.gz`)} | sha256sum -c - >/dev/null
id qy-beszel >/dev/null 2>&1 || useradd --system --home-dir /var/lib/qy-beszel --shell /usr/sbin/nologin qy-beszel
install -d -m 755 /opt/qy-node
install -d -m 700 /etc/qy-node
install -d -m 750 -o qy-beszel -g qy-beszel /var/lib/qy-beszel
# Extract only the expected binary from a checksum-verified release archive.
tar -xzf ${quote(`${staging}/agent.tar.gz`)} -C ${quote(staging)} beszel-agent
install -m 755 ${quote(`${staging}/beszel-agent`)} /opt/qy-node/beszel-agent
printf '%s' ${quote(Buffer.from(env).toString('base64'))} | base64 -d > /etc/qy-node/beszel.env
chmod 600 /etc/qy-node/beszel.env
printf '%s' ${quote(Buffer.from(unit).toString('base64'))} | base64 -d > /etc/systemd/system/qy-beszel-agent.service
systemctl daemon-reload
systemctl enable qy-beszel-agent.service >/dev/null
systemctl restart qy-beszel-agent.service
systemctl is-active --quiet qy-beszel-agent.service`, password, 190000);
    } finally { await exec(connection, `rm -rf -- ${quote(staging)}`).catch(() => {}); }
  }
  return { connect, verify, installKey, installMonitoring, close(connection) { connection?.client.end(); } };
}
