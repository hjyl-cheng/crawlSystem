from __future__ import annotations

from datetime import date, datetime, timezone
import json
import os
from typing import Any

from .database_topology import validate_shared_feature_database
from .reference_data import FeatureReferenceRefresher
from .rebuild import FeaturePolicyRebuilder
from .runtime_environment import required_environment


def _required_environment(name: str) -> str:
    return required_environment(os.environ, name)


def _as_of_day() -> date:
    value = str(os.environ.get("REFERENCE_AS_OF_DAY") or "").strip()
    return date.fromisoformat(value) if value else datetime.now(timezone.utc).date()


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
            "feature_clock.feature_reference_distributions",
            "feature_clock.collection_priority_signals",
        ),
    )

    as_of_day = _as_of_day()
    rebuilder = FeaturePolicyRebuilder(connect)
    recalculation_id = rebuilder.start(
        shard_count=int(os.environ.get("POLICY_REBUILD_SHARD_COUNT") or "128"),
    )
    try:
        result = FeatureReferenceRefresher(connect).refresh(
            as_of_day=as_of_day,
            minimum_cohort_size=int(os.environ.get("REFERENCE_MINIMUM_COHORT_SIZE") or "20"),
            batch_size=int(os.environ.get("REFERENCE_REFRESH_BATCH_SIZE") or "1000"),
        )
        rebuild = rebuilder.resume(
            recalculation_id,
            batch_size=int(os.environ.get("POLICY_REBUILD_BATCH_SIZE") or "500"),
            source_baseline_version=result.reference_distribution_version,
            worker_id=str(
                os.environ.get("POLICY_REBUILD_WORKER_ID") or "feature-reference-refresh"
            ),
        )
    except Exception:
        rebuilder.fail(recalculation_id)
        raise
    print(
        json.dumps(
            {
                "event": "feature_reference_refresh_completed",
                "database": expected_database,
                "as_of_day": result.as_of_day.isoformat(),
                "method_version": result.method_version,
                "distribution_count": result.distribution_count,
                "processed_channels": result.processed_channels,
                "reference_distribution_version": result.reference_distribution_version,
                "recalculation_id": rebuild.recalculation_id,
                "rebuild_status": rebuild.status,
                "rebuilt_channels": rebuild.processed_channels,
            },
            separators=(",", ":"),
        )
    )
    if rebuild.status != "succeeded":
        raise RuntimeError("reference refresh Policy Rebuild did not finish successfully")


if __name__ == "__main__":
    main()
