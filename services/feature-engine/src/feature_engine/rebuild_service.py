from __future__ import annotations

import json
import os
from typing import Any, Mapping
from uuid import UUID

from .rebuild import FeaturePolicyRebuilder, PolicyRebuildResult
from .database_topology import validate_shared_feature_database
from .runtime_environment import required_environment


def _required_environment(name: str) -> str:
    return required_environment(os.environ, name)


def _optional_environment(environment: Mapping[str, str], name: str) -> str | None:
    return str(environment.get(name) or "").strip() or None


def run_configured_rebuild(
    rebuilder: FeaturePolicyRebuilder,
    environment: Mapping[str, str],
) -> PolicyRebuildResult:
    recalculation_id = _optional_environment(environment, "POLICY_REBUILD_ID")
    batch_size = int(environment.get("POLICY_REBUILD_BATCH_SIZE") or "500")
    source_version = _optional_environment(environment, "POLICY_REBUILD_SOURCE_VERSION")
    worker_id = str(
        environment.get("POLICY_REBUILD_WORKER_ID") or "feature-policy-rebuild"
    )
    if recalculation_id is not None:
        try:
            recalculation_id = str(UUID(recalculation_id))
        except ValueError as error:
            raise RuntimeError("POLICY_REBUILD_ID must be a UUID") from error
        if _optional_environment(environment, "POLICY_REBUILD_SHARD_COUNT") is not None:
            raise RuntimeError("POLICY_REBUILD_SHARD_COUNT cannot be changed when resuming a Run")
        if _optional_environment(environment, "POLICY_REBUILD_VERSION") is not None:
            raise RuntimeError("POLICY_REBUILD_VERSION cannot be changed when resuming a Run")
        return rebuilder.resume(
            recalculation_id,
            batch_size=batch_size,
            source_baseline_version=source_version,
            worker_id=worker_id,
        )
    return rebuilder.rebuild(
        shard_count=int(environment.get("POLICY_REBUILD_SHARD_COUNT") or "128"),
        batch_size=batch_size,
        policy_version=_optional_environment(environment, "POLICY_REBUILD_VERSION"),
        source_baseline_version=source_version,
        worker_id=worker_id,
    )


def main() -> None:
    import psycopg

    database_url = _required_environment("FEATURE_DATABASE_URL")
    expected_database = _required_environment("EXPECTED_FEATURE_DATABASE")
    expected_user = _required_environment("EXPECTED_FEATURE_DATABASE_USER")

    def connect() -> Any:
        return psycopg.connect(database_url, options="-c timezone=UTC")

    validate_shared_feature_database(
        connect,
        expected_database=expected_database,
        expected_user=expected_user,
        required_feature_relations=(
            "feature_clock.recalculation_runs",
            "feature_clock.channel_clock_state",
        ),
    )
    result = run_configured_rebuild(FeaturePolicyRebuilder(connect), os.environ)
    print(
        json.dumps(
            {
                "event": "feature_policy_rebuild_completed",
                "database": expected_database,
                "recalculation_id": result.recalculation_id,
                "policy_version": result.policy_version,
                "status": result.status,
                "processed_channels": result.processed_channels,
                "failed_channels": result.failed_channels,
                "succeeded_shards": result.succeeded_shards,
                "failed_shards": result.failed_shards,
            },
            separators=(",", ":"),
        )
    )
    if result.status != "succeeded":
        raise RuntimeError("Policy Rebuild did not finish successfully")


if __name__ == "__main__":
    main()
