import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import uuid

spec = importlib.util.spec_from_file_location('remove_worker', Path(__file__).with_name('removeWorker.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class RemoveWorkerTest(unittest.TestCase):
    def test_full_crawl_preserves_evidence(self):
        self.check_removal("full-crawl", "full_crawl_collect")

    def test_removes_only_selected_container_preserves_files_and_retries(self):
        self.check_removal("incremental", "incremental_collect")

    def check_removal(self, prefix, mode):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            value = dict(nodeId=str(uuid.uuid4()), deploymentId=str(uuid.uuid4()),
                         slot=prefix+'-2', operationId=str(uuid.uuid4()))
            (root / 'node-id').write_text(value['nodeId'])
            deployment = root / 'deployments' / value['deploymentId']
            deployment.mkdir(parents=True)
            compose = {'name': 'qy-node-' + value['nodeId'].replace('-', '')[:16],
                       'services': {prefix+'-1': {'image': 'same'}, prefix+'-2': {'image': 'same'}}}
            (deployment / 'compose.json').write_text(json.dumps(compose))
            spool = root / 'spool'
            spool.mkdir()
            (spool / 'result.json').write_text('preserved')
            labels = {'qy.node.id': value['nodeId'], 'qy.node.slot': value['slot'],
                      'qy.deployment.id': value['deploymentId'], 'qy.remote.mode': mode}
            present = [True]
            calls = []

            def run(args):
                calls.append(args)
                if args[0] == 'ps':
                    return 'fixture-id' if present[0] else ''
                if args[0] == 'inspect':
                    return json.dumps([{'Config': {'Labels': labels}}])
                if args[0] == 'rm':
                    self.assertEqual(args, ['rm', 'fixture-id'])
                    present[0] = False
                return ''

            module.run = run
            self.assertTrue(module.remove(value, root)['removed'])
            self.assertTrue(module.remove(value, root)['removed'])
            self.assertEqual(sum(c[0] == 'stop' for c in calls), 1)
            self.assertEqual(json.loads((deployment / 'compose.json').read_text())['services'], {prefix+'-1': {'image': 'same'}})
            self.assertEqual((spool / 'result.json').read_text(), 'preserved')
            # A stale or conflicting operation cannot touch this slot again.
            with self.assertRaises(RuntimeError):
                module.remove({**value, 'operationId': str(uuid.uuid4())}, root)

    def test_rejects_wrong_container_owner_before_stop(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            value = dict(nodeId=str(uuid.uuid4()), deploymentId=str(uuid.uuid4()), slot='incremental-1', operationId=str(uuid.uuid4()))
            (root / 'node-id').write_text(value['nodeId'])
            target = root / 'deployments' / value['deploymentId']
            target.mkdir(parents=True)
            (target / 'compose.json').write_text(json.dumps({'name': 'qy-node-' + value['nodeId'].replace('-', '')[:16], 'services': {'incremental-1': {}}}))
            calls = []
            def run(args):
                calls.append(args[0])
                return 'foreign' if args[0] == 'ps' else json.dumps([{'Config': {'Labels': {}}}])
            module.run = run
            with self.assertRaises(RuntimeError):
                module.remove(value, root)
            self.assertEqual(calls, ['ps', 'inspect'])


if __name__ == '__main__':
    unittest.main()
