from __future__ import annotations

import json
import os
from typing import Any, Mapping
from uuid import UUID

from .bootstrap import (
    BaselineBundle,
    BootstrapResult,
    FeatureBootstrapper,
    load_baseline_bundle,
)
from .database_topology import validate_shared_feature_database
from .runtime_environment import required_environment


def _required_environment(environment: Mapping[str, str], name: str) -> str:
    return required_environment(environment, name)


def _optional_environment(environment: Mapping[str, str], name: str) -> str | None:
    return str(environment.get(name) or "").strip() or None


def validate_bundle_expectations(
    bundle: BaselineBundle,
    environment: Mapping[str, str],
) -> None:
    expected_database = _required_environment(
        environment, "EXPECTED_BASELINE_SOURCE_DATABASE"
    )
    try:
        expected_channels = int(
            _required_environment(environment, "EXPECTED_BASELINE_CHANNEL_COUNT")
        )
    except ValueError as error:
        raise RuntimeError("EXPECTED_BASELINE_CHANNEL_COUNT must be a positive integer") from error
    if expected_channels <= 0:
        raise RuntimeError("EXPECTED_BASELINE_CHANNEL_COUNT must be a positive integer")
    if bundle.manifest.source_database != expected_database:
        raise RuntimeError("Baseline Manifest comes from an unexpected Crawler database")
    if bundle.manifest.channel_count != expected_channels:
        raise RuntimeError("Baseline Manifest has an unexpected Channel count")


def run_configured_bootstrap(
    bootstrapper: FeatureBootstrapper,
    bundle: BaselineBundle,
    environment: Mapping[str, str],
) -> BootstrapResult:
    recalculation_id = _optional_environment(environment, "BOOTSTRAP_ID")
    batch_size = int(environment.get("BOOTSTRAP_BATCH_SIZE") or "100")
    worker_id = str(
        environment.get("BOOTSTRAP_WORKER_ID") or "feature-initial-bootstrap"
    )
    if recalculation_id is not None:
        try:
            recalculation_id = str(UUID(recalculation_id))
        except ValueError as error:
            raise RuntimeError("BOOTSTRAP_ID must be a UUID") from error
        for field in ("BOOTSTRAP_SHARD_COUNT", "BOOTSTRAP_POLICY_VERSION"):
            if _optional_environment(environment, field) is not None:
                raise RuntimeError(f"{field} cannot be changed when resuming a Run")
        return bootstrapper.resume(
            recalculation_id,
            bundle,
            batch_size=batch_size,
            worker_id=worker_id,
        )
    return bootstrapper.bootstrap(
        bundle,
        shard_count=int(environment.get("BOOTSTRAP_SHARD_COUNT") or "128"),
        batch_size=batch_size,
        policy_version=_optional_environment(environment, "BOOTSTRAP_POLICY_VERSION"),
        worker_id=worker_id,
    )


def main() -> None:
    import psycopg

    manifest_path = _required_environment(os.environ, "BOOTSTRAP_MANIFEST_PATH")
    bundle = load_baseline_bundle(manifest_path)
    validate_bundle_expectations(bundle, os.environ)
    database_url = _required_environment(os.environ, "FEATURE_DATABASE_URL")
    expected_database = _required_environment(os.environ, "EXPECTED_FEATURE_DATABASE")
    expected_user = _required_environment(os.environ, "EXPECTED_FEATURE_DATABASE_USER")

    def connect() -> Any:
        return psycopg.connect(database_url, options="-c timezone=UTC")

    validate_shared_feature_database(
        connect,
        expected_database=expected_database,
        expected_user=expected_user,
        required_feature_relations=(
            "feature_clock.baseline_bundle_manifests",
            "feature_clock.bootstrap_channel_receipts",
            "feature_clock.channel_clock_state",
        ),
    )
    result = run_configured_bootstrap(
        FeatureBootstrapper(connect),
        bundle,
        os.environ,
    )
    print(
        json.dumps(
            {
                "event": "feature_initial_bootstrap_completed",
                "database": expected_database,
                "source_database": bundle.manifest.source_database,
                "recalculation_id": result.recalculation_id,
                "baseline_version": result.baseline_version,
                "policy_version": result.policy_version,
                "status": result.status,
                "processed_events": result.processed_events,
                "processed_channels": result.processed_channels,
                "failed_channels": result.failed_channels,
                "succeeded_shards": result.succeeded_shards,
                "failed_shards": result.failed_shards,
            },
            separators=(",", ":"),
        )
    )
    if result.status != "succeeded":
        raise RuntimeError("Initial Bootstrap did not finish successfully")


if __name__ == "__main__":
    main()
