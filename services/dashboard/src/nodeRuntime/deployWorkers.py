#!/usr/bin/env python3
"""Fixed node installer. No arbitrary command, path, environment or volume input."""
import base64
import tempfile
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time
import uuid


def require(value):
    if not value:
        raise RuntimeError('NODE_DEPLOYMENT_INVALID')


def directory(path, mode=0o700, owner=0):
    for parent in reversed([path, *path.parents]):
        if parent.exists() or parent.is_symlink():
            require(stat.S_ISDIR(parent.lstat().st_mode))
    path.mkdir(parents=True, exist_ok=True, mode=mode)
    os.chmod(path, mode)
    os.chown(path, owner, owner)


def immutable(path, content, owner=1000):
    data = content.encode()
    if path.exists() or path.is_symlink():
        require(stat.S_ISREG(path.lstat().st_mode) and path.read_bytes() == data)
    else:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'wb') as out:
            out.write(data)
            out.flush()
            os.fsync(out.fileno())
    os.chmod(path, 0o600)
    os.chown(path, owner, owner)


def run(args, timeout=300):
    # Container/registry error output can include auth diagnostics. Never echo it.
    if args[0] == 'docker':
        args = ['docker', '--host', 'unix:///var/run/docker.sock', *args[1:]]
    result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=True)
    return result.stdout.decode()


def deploy(bundle_file, step):
    require(step in ['files', 'start', 'verify'])
    bundle_path = Path(bundle_file)
    require(bundle_path.is_file() and bundle_path.stat().st_size <= 1024 * 1024)
    bundle = json.loads(bundle_path.read_text())
    plan, credentials = bundle['plan'], bundle['credentials']
    node, deployment = plan['nodeId'], plan['deploymentId']
    require(re.fullmatch(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}', node))
    require(re.fullmatch(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}', deployment))
    require(plan['mode'] == 'incremental_collect' and 1 <= plan['count'] <= 32)
    require(re.fullmatch(r'[a-z0-9][a-z0-9./:_-]*@sha256:[a-f0-9]{64}', plan['image']))
    require(credentials['nodeId'] == node and credentials['deploymentId'] == deployment)
    root = Path('/etc/qy-node/runtime/deployments') / deployment
    project = 'qy-node-' + node.replace('-', '')[:16]
    slots = [f'incremental-{i}' for i in range(1, plan['count'] + 1)]
    compose = plan['compose']
    require(compose['name'] == project and set(compose) == {'name', 'services'} and set(compose['services']) == set(slots))
    for slot, service in compose['services'].items():
        require(service['image'] == plan['image'] and service['user'] == '1000:1000' and service['read_only'] is True)
        require(service['cap_drop'] == ['ALL'] and service['security_opt'] == ['no-new-privileges:true'])
        require(set(service) == {'image','init','restart','user','read_only','cap_drop','security_opt','pids_limit','mem_limit','cpus','stop_grace_period','tmpfs','volumes','logging','labels'})
        require(service['mem_limit'] == '768m' and service['cpus'] == 0.5 and service['pids_limit'] == 128)
        require(service['labels'] == {'qy.node.id':node,'qy.node.slot':slot,'qy.deployment.id':deployment,'qy.remote.mode':'incremental_collect'})
        expected = {('/run/secrets/node-config.json', str(root / (slot + '.json')), True),
                    ('/run/secrets/node-token', str(root / 'node-token'), True),
                    ('/run/secrets/relay-token', str(root / (slot + '.relay-token')), True),
                    ('/run/secrets/route-public.pem', str(root / 'route-public.pem'), True),
                    ('/var/lib/qy-node/spool', '/var/lib/qy-node/spool/' + slot, False)}
        require(len(service['volumes']) == len(expected))
        require({(m['target'], m['source'], m['read_only']) for m in service['volumes']} == expected)
        require(all(m['type'] == 'bind' and m['bind'] == {'create_host_path':False} for m in service['volumes']))
        config = json.loads(plan['files'][slot + '.json'])
        require(config['node_id'] == node and config['deployment_id'] == deployment and config['slot'] == slot and config['mode'] == 'incremental_collect')
    require(Path('/etc/qy-node/runtime/node-id').read_text().strip() == node)
    command = ['docker','compose','-p',project,'-f',str(root / 'compose.json')]
    if step == 'files':
        memory_kib = int(next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemTotal:')))
        require(memory_kib >= (plan['count'] * 768 + 512) * 1024)
        directory(root)
        immutable(root / 'node-token', credentials['nodeToken'])
        immutable(root / 'route-public.pem', credentials['publicKey'])
        for slot in slots:
            immutable(root / (slot + '.json'), plan['files'][slot + '.json'])
            immutable(root / (slot + '.relay-token'), credentials['relayTokens'][slot])
            directory(Path('/var/lib/qy-node/spool') / slot, owner=1000)
        temporary = root / ('compose.' + str(uuid.uuid4()) + '.json')
        immutable(temporary, json.dumps(compose), owner=0)
        os.replace(temporary, root / 'compose.json')
        run(command + ['config','--quiet'], 30)
    elif step == 'start':
        registry = credentials.get('registry')
        if registry is None:
            run(command + ['pull'], 600)
        else:
            require(set(registry) == {'server', 'username', 'password'})
            require(registry['server'] == plan['image'].split('/')[0])
            require(re.fullmatch(r'[a-z0-9.-]+(?::[0-9]{1,5})?', registry['server']))
            require(re.fullmatch(r'[a-zA-Z0-9_-]{1,64}', registry['username']))
            require(re.fullmatch(r'[a-zA-Z0-9_-]{32,256}', registry['password']))
            # The root-only Docker config exists only for pull, including on
            # failure. No credentials in argv, node config or global login.
            with tempfile.TemporaryDirectory(prefix='qy-registry-', dir='/run') as config_dir:
                auth = base64.b64encode((registry['username'] + ':' + registry['password']).encode()).decode()
                immutable(Path(config_dir) / 'config.json', json.dumps({'auths': {registry['server']: {'auth': auth}}}), owner=0)
                run(['docker', '--config', config_dir, *command[1:], 'pull'], 600)
        run(command + ['up','-d','--no-recreate','--pull','never'], 120)
    else:
        deadline = time.monotonic() + 90
        while True:
            ids = run(command + ['ps','--all','-q'], 30).split()
            containers = json.loads(run(['docker','inspect',*ids], 30)) if ids else []
            if len(containers) == len(slots) and all(c['State']['Running'] and c['State'].get('Health',{}).get('Status') == 'healthy' for c in containers):
                break
            require(time.monotonic() < deadline)
            time.sleep(1)
    print(json.dumps({'nodeId':node,'deploymentId':deployment,'step':step,'count':len(slots)}))


if __name__ == '__main__':
    try:
        require(os.geteuid() == 0 and len(sys.argv) == 3)
        deploy(sys.argv[1], sys.argv[2])
    except Exception:
        print('NODE_DEPLOYMENT_STEP_FAILED', file=sys.stderr)
        sys.exit(1)
