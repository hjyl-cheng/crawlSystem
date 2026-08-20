from pathlib import Path
import tempfile
import unittest

from feature_engine.runtime_environment import (
    environment_value,
    optional_environment,
    required_environment,
)


class RuntimeEnvironmentTests(unittest.TestCase):
    def test_reads_direct_or_file_backed_values(self) -> None:
        self.assertEqual(required_environment({"SETTING": "direct"}, "SETTING"), "direct")
        with tempfile.TemporaryDirectory() as directory:
            secret = Path(directory) / "setting"
            secret.write_text("from-file\n", encoding="utf-8")
            self.assertEqual(
                required_environment({"SETTING_FILE": str(secret)}, "SETTING"),
                "from-file",
            )

    def test_rejects_ambiguous_or_missing_required_values(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "cannot both be set"):
            environment_value(
                {"SETTING": "direct", "SETTING_FILE": "/tmp/setting"},
                "SETTING",
            )
        with self.assertRaisesRegex(RuntimeError, "is required"):
            required_environment({}, "SETTING")
        self.assertIsNone(optional_environment({}, "SETTING"))

    def test_does_not_include_secret_file_errors_in_the_value(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "cannot read SETTING_FILE"):
            required_environment({"SETTING_FILE": "/missing/setting"}, "SETTING")


if __name__ == "__main__":
    unittest.main()
