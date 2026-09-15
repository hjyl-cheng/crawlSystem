#!/usr/bin/env python3
"""Remove one center-fenced idle Worker; retain spool, configs and history."""
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import uuid


def require(value):
    if not value:
        raise RuntimeError('WORKER_REMOVAL_INVALID')


def run(args):
    return subprocess.run(['docker', '--host', 'unix:///var/run/docker.sock', *args],
                          capture_output=True, check=True, timeout=60).stdout.decode()


def remove(value, root_path=Path('/etc/qy-node/runtime')):
    require(set(value) == {'nodeId', 'deploymentId', 'slot', 'operationId'})
    node, deployment, slot, operation = (value[k] for k in ['nodeId', 'deploymentId', 'slot', 'operationId'])
    for identifier in [node, deployment, operation]:
        require(str(uuid.UUID(identifier)) == identifier)
    require(re.fullmatch(r'incremental-[1-9][0-9]*', slot))
    require((root_path / 'node-id').read_text().strip() == node)
    root = root_path / 'deployments' / deployment
    for parent in [root, *root.parents]:
        require(stat.S_ISDIR(parent.lstat().st_mode))
    compose_path = root / 'compose.json'
    require(stat.S_ISREG(compose_path.lstat().st_mode))
    compose = json.loads(compose_path.read_text())
    require(compose['name'] == 'qy-node-' + node.replace('-', '')[:16])
    marker = root / (slot + '.removed.json')
    if marker.exists():
        require(stat.S_ISREG(marker.lstat().st_mode) and json.loads(marker.read_text()) == value)
    ids = run(['ps', '-aq', '--filter', 'label=qy.node.id=' + node,
               '--filter', 'label=qy.node.slot=' + slot]).split()
    require(len(ids) <= 1)
    if ids:
        container = json.loads(run(['inspect', ids[0]]))[0]
        labels = container['Config']['Labels']
        require(all(labels.get(k) == v for k, v in {'qy.node.id': node, 'qy.node.slot': slot,
                    'qy.deployment.id': deployment, 'qy.remote.mode': 'incremental_collect'}.items()))
        require(slot in compose['services'])
        run(['stop', '--timeout', '20', ids[0]])
        run(['rm', ids[0]])  # No volume deletion; no command touches the spool.
    compose['services'].pop(slot, None)
    temporary = root / ('compose.' + str(uuid.uuid4()) + '.json')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as stream:
        json.dump(compose, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, compose_path)
    if not marker.exists():
        fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump(value, stream)
            stream.flush()
            os.fsync(stream.fileno())
    require(not run(['ps', '-aq', '--filter', 'label=qy.node.id=' + node,
                     '--filter', 'label=qy.node.slot=' + slot]).strip())
    return {**value, 'removed': True}


if __name__ == '__main__':
    try:
        require(os.geteuid() == 0 and len(sys.argv) == 2)
        print(json.dumps(remove(json.loads(sys.argv[1]))))
    except Exception:
        print('WORKER_REMOVAL_FAILED', file=sys.stderr)
        sys.exit(1)
