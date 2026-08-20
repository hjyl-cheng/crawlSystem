from __future__ import annotations

import unittest

from feature_engine.rebuild import PolicyRebuildResult
from feature_engine.rebuild_service import run_configured_rebuild


class StubRebuilder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []

    @staticmethod
    def _result(recalculation_id: str) -> PolicyRebuildResult:
        return PolicyRebuildResult(
            recalculation_id=recalculation_id,
            policy_version="v16-rule-1",
            status="succeeded",
            processed_channels=0,
            failed_channels=0,
            succeeded_shards=1,
            failed_shards=0,
        )

    def rebuild(self, **kwargs) -> PolicyRebuildResult:
        self.calls.append(("rebuild", (), kwargs))
        return self._result("00000000-0000-0000-0000-000000000010")

    def resume(self, *args, **kwargs) -> PolicyRebuildResult:
        self.calls.append(("resume", args, kwargs))
        return self._result(args[0])


class RebuildServiceConfigurationTests(unittest.TestCase):
    def test_starts_a_new_run_when_no_recalculation_id_is_configured(self) -> None:
        rebuilder = StubRebuilder()

        run_configured_rebuild(
            rebuilder,
            {
                "POLICY_REBUILD_SHARD_COUNT": "8",
                "POLICY_REBUILD_BATCH_SIZE": "25",
                "POLICY_REBUILD_VERSION": "v16-rule-1",
                "POLICY_REBUILD_SOURCE_VERSION": "reference-1",
                "POLICY_REBUILD_WORKER_ID": "worker-1",
            },
        )

        self.assertEqual(
            rebuilder.calls,
            [
                (
                    "rebuild",
                    (),
                    {
                        "shard_count": 8,
                        "batch_size": 25,
                        "policy_version": "v16-rule-1",
                        "source_baseline_version": "reference-1",
                        "worker_id": "worker-1",
                    },
                )
            ],
        )

    def test_resumes_the_configured_run_without_creating_another(self) -> None:
        rebuilder = StubRebuilder()
        recalculation_id = "00000000-0000-0000-0000-000000000020"

        result = run_configured_rebuild(
            rebuilder,
            {
                "POLICY_REBUILD_ID": recalculation_id,
                "POLICY_REBUILD_BATCH_SIZE": "10",
                "POLICY_REBUILD_SOURCE_VERSION": "reference-2",
                "POLICY_REBUILD_WORKER_ID": "worker-2",
            },
        )

        self.assertEqual(result.recalculation_id, recalculation_id)
        self.assertEqual(
            rebuilder.calls,
            [
                (
                    "resume",
                    (recalculation_id,),
                    {
                        "batch_size": 10,
                        "source_baseline_version": "reference-2",
                        "worker_id": "worker-2",
                    },
                )
            ],
        )

    def test_resume_rejects_new_run_identity_settings(self) -> None:
        recalculation_id = "00000000-0000-0000-0000-000000000020"
        for field in ("POLICY_REBUILD_SHARD_COUNT", "POLICY_REBUILD_VERSION"):
            with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, field):
                run_configured_rebuild(
                    StubRebuilder(),
                    {"POLICY_REBUILD_ID": recalculation_id, field: "unexpected"},
                )

    def test_resume_requires_a_uuid(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "must be a UUID"):
            run_configured_rebuild(StubRebuilder(), {"POLICY_REBUILD_ID": "not-a-uuid"})


if __name__ == "__main__":
    unittest.main()
