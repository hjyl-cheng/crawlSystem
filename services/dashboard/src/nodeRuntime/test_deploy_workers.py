import importlib.util
from pathlib import Path
import subprocess
import tempfile
import json
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('deploy_workers', Path(__file__).with_name('deployWorkers.py'))
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)
IMAGE = 'registry.example/worker@sha256:' + 'a' * 64

class ImageReuseTests(unittest.TestCase):
    def test_exact_digest_cached_skips_download(self):
        with patch.object(installer, 'run', return_value='[{}]') as run:
            installer.pull_image(IMAGE, None)
        self.assertEqual(run.call_args_list[0].args[0], ['docker', 'image', 'inspect', IMAGE])
        self.assertEqual(run.call_count, 1)

    def test_missing_digest_downloads_once_with_thirty_minutes(self):
        with patch.object(installer, 'run', side_effect=[subprocess.CalledProcessError(1, ['docker']), '']) as run:
            installer.pull_image(IMAGE, None)
        self.assertEqual(run.call_count, 2)
        self.assertEqual(run.call_args.args, (['docker', 'pull', IMAGE], 1800))

    def test_private_registry_download_budget_and_credentials_cleanup(self):
        registry = {'server': 'registry.example', 'username': 'node-pull', 'password': 'b' * 64}
        temporary = tempfile.TemporaryDirectory()
        path = Path(temporary.name)
        def execute(args, timeout):
            if args[1:3] == ['image', 'inspect']:
                raise subprocess.CalledProcessError(1, args)
            self.assertEqual(args, ['docker', '--config', str(path), 'pull', IMAGE])
            self.assertEqual(timeout, 1800)
            self.assertEqual(set(json.loads((path / 'config.json').read_text())['auths']), {'registry.example'})
            return ''
        with patch.object(installer.tempfile, 'TemporaryDirectory', return_value=temporary), patch.object(installer, 'run', side_effect=execute):
            installer.pull_image(IMAGE, registry)
        self.assertFalse(path.exists())

    def test_failed_pull_propagates_and_cannot_start_workers(self):
        with patch.object(installer, 'run', side_effect=[subprocess.CalledProcessError(1, ['docker']), subprocess.TimeoutExpired(['docker'], 1800)]):
            with self.assertRaises(subprocess.TimeoutExpired):
                installer.pull_image(IMAGE, None)

if __name__ == '__main__':
    unittest.main()
