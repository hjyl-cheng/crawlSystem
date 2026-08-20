from __future__ import annotations

from dataclasses import dataclass
import unittest

from feature_engine.bootstrap import BootstrapResult
from feature_engine.bootstrap_service import (
    run_configured_bootstrap,
    validate_bundle_expectations,
)


@dataclass(frozen=True)
class ManifestStub:
    source_database: str = "bullmq_crawler_migration"
    channel_count: int = 1552


@dataclass(frozen=True)
class BundleStub:
    manifest: ManifestStub = ManifestStub()


class BootstrapperStub:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []

    @staticmethod
    def _result(recalculation_id: str) -> BootstrapResult:
        return BootstrapResult(
            recalculation_id=recalculation_id,
            baseline_version="baseline-1",
            policy_version="v16-rule-1",
            status="succeeded",
            processed_events=0,
            processed_channels=0,
            failed_channels=0,
            succeeded_shards=1,
            failed_shards=0,
        )

    def bootstrap(self, *args, **kwargs) -> BootstrapResult:
        self.calls.append(("bootstrap", args, kwargs))
        return self._result("00000000-0000-0000-0000-000000000030")

    def resume(self, *args, **kwargs) -> BootstrapResult:
        self.calls.append(("resume", args, kwargs))
        return self._result(args[0])


class BootstrapServiceTests(unittest.TestCase):
    def test_requires_the_expected_crawler_database_and_channel_count(self) -> None:
        validate_bundle_expectations(
            BundleStub(),
            {
                "EXPECTED_BASELINE_SOURCE_DATABASE": "bullmq_crawler_migration",
                "EXPECTED_BASELINE_CHANNEL_COUNT": "1552",
            },
        )
        with self.assertRaisesRegex(RuntimeError, "unexpected Crawler database"):
            validate_bundle_expectations(
                BundleStub(),
                {
                    "EXPECTED_BASELINE_SOURCE_DATABASE": "wrong",
                    "EXPECTED_BASELINE_CHANNEL_COUNT": "1552",
                },
            )

    def test_starts_a_new_bootstrap_with_frozen_identity(self) -> None:
        bootstrapper = BootstrapperStub()
        bundle = BundleStub()

        run_configured_bootstrap(
            bootstrapper,
            bundle,
            {
                "BOOTSTRAP_SHARD_COUNT": "8",
                "BOOTSTRAP_BATCH_SIZE": "25",
                "BOOTSTRAP_POLICY_VERSION": "v16-rule-1",
                "BOOTSTRAP_WORKER_ID": "worker-1",
            },
        )

        self.assertEqual(
            bootstrapper.calls,
            [
                (
                    "bootstrap",
                    (bundle,),
                    {
                        "shard_count": 8,
                        "batch_size": 25,
                        "policy_version": "v16-rule-1",
                        "worker_id": "worker-1",
                    },
                )
            ],
        )

    def test_resume_rejects_new_identity_settings(self) -> None:
        recalculation_id = "00000000-0000-0000-0000-000000000031"
        for field in ("BOOTSTRAP_SHARD_COUNT", "BOOTSTRAP_POLICY_VERSION"):
            with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, field):
                run_configured_bootstrap(
                    BootstrapperStub(),
                    BundleStub(),
                    {"BOOTSTRAP_ID": recalculation_id, field: "unexpected"},
                )


if __name__ == "__main__":
    unittest.main()
