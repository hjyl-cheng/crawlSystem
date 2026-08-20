from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest


SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "ops"
    / "feature-migration"
    / "init_secrets.py"
)


class DeploymentSecretTests(unittest.TestCase):
    def test_runtime_uses_pgbouncer_and_admin_url_stays_direct(self) -> None:
        spec = importlib.util.spec_from_file_location("feature_init_secrets", SCRIPT)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        with tempfile.TemporaryDirectory() as directory:
            module.ROOT = Path(directory)
            module.DATABASE_HOST = "qy-pgbouncer"
            module.DATABASE_PORT = "6432"
            module.ADMIN_DATABASE_HOST = "qy-postgres"
            module.ADMIN_DATABASE_PORT = "5432"
            module.DATABASE_NAME = "crawler"
            module.DATABASE_USER = "feature_user"
            module.LEGACY_DATABASE_HOST = "legacy-postgres"
            module.LEGACY_DATABASE_NAME = "feature_clock"
            module.main()

            password = (module.ROOT / "feature_db_password").read_text().strip()
            runtime_url = (module.ROOT / "feature_database_url").read_text().strip()
            admin_url = (module.ROOT / "feature_admin_database_url").read_text().strip()
            pgpass = (module.ROOT / "feature_pgpass").read_text().splitlines()

        self.assertEqual(
            runtime_url,
            f"postgresql://feature_user:{password}@qy-pgbouncer:6432/crawler",
        )
        self.assertEqual(
            admin_url,
            f"postgresql://feature_user:{password}@qy-postgres:5432/crawler",
        )
        self.assertEqual(
            pgpass,
            [
                f"qy-pgbouncer:6432:crawler:feature_user:{password}",
                f"qy-postgres:5432:crawler:feature_user:{password}",
                f"legacy-postgres:5432:feature_clock:feature_user:{password}",
            ],
        )


if __name__ == "__main__":
    unittest.main()
