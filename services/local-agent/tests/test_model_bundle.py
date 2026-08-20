import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from qy_channel_profile.errors import ModelBundleError
from qy_channel_profile.features import FEATURE_SCHEMA_VERSION
from qy_channel_profile.language_id import file_sha256
from qy_channel_profile.model_bundle import (
    MODEL_BUNDLE_SCHEMA_VERSION,
    ModelBundle,
)


class ModelBundleTest(unittest.TestCase):
    def _manifest(self, artifact_path: Path, artifact_hash: str) -> dict:
        return {
            "schema_version": MODEL_BUNDLE_SCHEMA_VERSION,
            "bundle_version": "test-bundle-v1",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "taxonomy_version": "qy-taxonomy-v1",
            "compatible_processor_major": 1,
            "production_eligible": False,
            "artifacts": [
                {
                    "artifact_id": "creator_language.fasttext",
                    "field": "creator_language",
                    "kind": "fasttext_language",
                    "relative_path": artifact_path.name,
                    "sha256": artifact_hash,
                    "status": "active",
                    "training_source": "FastText public pretrained model",
                    "label_quality": "pretrained_public",
                }
            ],
        }

    def test_bundle_rejects_artifact_hash_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "model.bin"
            artifact.write_bytes(b"fixed model bytes")
            manifest = self._manifest(artifact, "sha256:" + "0" * 64)
            path = root / "manifest.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaises(ModelBundleError):
                ModelBundle.load(path)

    def test_bundle_validates_local_fasttext_artifact_without_loading_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "model.bin"
            artifact.write_bytes(b"fixed model bytes")
            manifest = self._manifest(artifact, file_sha256(artifact))
            path = root / "manifest.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")
            bundle = ModelBundle.load(path)
            self.assertEqual(bundle.version, "test-bundle-v1")
            self.assertFalse(bundle.production_eligible)


if __name__ == "__main__":
    unittest.main()
