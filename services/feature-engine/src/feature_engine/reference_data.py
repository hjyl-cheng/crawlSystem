from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from hashlib import sha256
import json
from typing import Any, Callable, Mapping

from .shared_features import (
    CollectionPrioritySignals,
    QUANTILE_PROBABILITIES,
    REFERENCE_METHOD_VERSION,
    QuantileDistribution,
    ReferenceCatalog,
    SharedFeatureInputs,
    derive_recent_change_probability,
    derive_shared_features,
)
from .utc import as_utc


_COHORT_SQL = """
CASE
  WHEN last_subscriber_count IS NULL THEN 'subs:unknown'
  WHEN last_subscriber_count < 1000 THEN 'subs:0-1k'
  WHEN last_subscriber_count < 10000 THEN 'subs:1k-10k'
  WHEN last_subscriber_count < 100000 THEN 'subs:10k-100k'
  WHEN last_subscriber_count < 1000000 THEN 'subs:100k-1m'
  WHEN last_subscriber_count < 10000000 THEN 'subs:1m-10m'
  WHEN last_subscriber_count < 100000000 THEN 'subs:10m-100m'
  ELSE 'subs:100m+'
END
""".strip()

_REFERENCE_FEATURE_COLUMNS = {
    "subscriber_count": "last_subscriber_count",
    "subscriber_velocity_ewma": "subscriber_velocity_ewma",
    "view_velocity_ewma": "view_velocity_ewma",
}

_SHARED_STATE_COLUMNS = (
    "subscriber_size_percentile",
    "subscriber_growth_percentile",
    "view_growth_percentile",
    "growth_momentum",
    "user_query_demand",
    "data_incompleteness",
    "manual_priority",
    "collection_priority",
    "channel_activity",
    "recent_change_probability",
    "reference_distribution_version",
)


@dataclass(frozen=True, slots=True)
class ReferenceRefreshResult:
    as_of_day: date
    method_version: str
    distribution_count: int
    processed_channels: int
    reference_distribution_version: str | None


def _column_name(description: Any) -> str:
    return description.name if hasattr(description, "name") else description[0]


def _row(cursor: Any) -> dict[str, Any] | None:
    value = cursor.fetchone()
    if value is None:
        return None
    if isinstance(value, Mapping):
        return dict(value)
    return {
        _column_name(description): item
        for description, item in zip(cursor.description, value, strict=True)
    }


def _rows(cursor: Any) -> list[dict[str, Any]]:
    values = cursor.fetchall()
    columns = [_column_name(description) for description in cursor.description]
    return [
        dict(value) if isinstance(value, Mapping) else dict(zip(columns, value, strict=True))
        for value in values
    ]


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _distribution(
    *,
    as_of_day: date,
    feature_name: str,
    cohort_key: str,
    sample_count: int,
    values: list[float] | tuple[float, ...] | None,
    method_version: str,
) -> QuantileDistribution | None:
    if sample_count <= 0 or not values:
        return None
    return QuantileDistribution(
        as_of_day=as_of_day,
        cohort_key=cohort_key,
        feature_name=feature_name,
        sample_count=sample_count,
        probabilities=QUANTILE_PROBABILITIES,
        values=tuple(float(value) for value in values),
        method_version=method_version,
    )


def build_reference_distributions(
    cursor: Any,
    *,
    as_of_day: date,
    method_version: str = REFERENCE_METHOD_VERSION,
) -> tuple[QuantileDistribution, ...]:
    probabilities = list(QUANTILE_PROBABILITIES)
    output: list[QuantileDistribution] = []
    for feature_name, column in _REFERENCE_FEATURE_COLUMNS.items():
        cursor.execute(
            f"""
            SELECT count({column})::bigint AS sample_count,
                   percentile_cont(%s::double precision[])
                     WITHIN GROUP (ORDER BY {column}) AS values
            FROM feature_clock.channel_feature_state
            WHERE {column} IS NOT NULL
            """,
            (probabilities,),
        )
        row = _row(cursor) or {}
        item = _distribution(
            as_of_day=as_of_day,
            feature_name=feature_name,
            cohort_key="all",
            sample_count=int(row.get("sample_count") or 0),
            values=row.get("values"),
            method_version=method_version,
        )
        if item is not None:
            output.append(item)

    for feature_name in ("subscriber_velocity_ewma", "view_velocity_ewma"):
        column = _REFERENCE_FEATURE_COLUMNS[feature_name]
        cursor.execute(
            f"""
            SELECT cohort_key,count(value)::bigint AS sample_count,
                   percentile_cont(%s::double precision[])
                     WITHIN GROUP (ORDER BY value) AS values
            FROM (
              SELECT {_COHORT_SQL} AS cohort_key,{column} AS value
              FROM feature_clock.channel_feature_state
              WHERE {column} IS NOT NULL
            ) samples
            GROUP BY cohort_key
            ORDER BY cohort_key
            """,
            (probabilities,),
        )
        for row in _rows(cursor):
            item = _distribution(
                as_of_day=as_of_day,
                feature_name=feature_name,
                cohort_key=str(row["cohort_key"]),
                sample_count=int(row["sample_count"]),
                values=row["values"],
                method_version=method_version,
            )
            if item is not None:
                output.append(item)
    return tuple(output)


def save_reference_distributions(
    cursor: Any,
    distributions: tuple[QuantileDistribution, ...],
) -> None:
    for item in distributions:
        quantiles = {
            "probabilities": list(item.probabilities),
            "values": list(item.values),
        }
        checksum_body = {
            "as_of_day": item.as_of_day.isoformat(),
            "cohort_key": item.cohort_key,
            "feature_name": item.feature_name,
            "sample_count": item.sample_count,
            "quantiles": quantiles,
            "method_version": item.method_version,
        }
        checksum = f"sha256:{sha256(_json(checksum_body).encode('utf-8')).hexdigest()}"
        cursor.execute(
            """
            INSERT INTO feature_clock.feature_reference_distributions (
              as_of_day,cohort_key,feature_name,sample_count,quantiles,
              method_version,checksum
            ) VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s)
            ON CONFLICT (as_of_day,cohort_key,feature_name,method_version)
            DO UPDATE SET sample_count=EXCLUDED.sample_count,
                          quantiles=EXCLUDED.quantiles,
                          checksum=EXCLUDED.checksum,
                          created_at=now()
            """,
            (
                item.as_of_day,
                item.cohort_key,
                item.feature_name,
                item.sample_count,
                _json(quantiles),
                item.method_version,
                checksum,
            ),
        )


def load_reference_catalog(
    cursor: Any,
    *,
    as_of_day: date,
    method_version: str = REFERENCE_METHOD_VERSION,
    minimum_cohort_size: int = 20,
) -> ReferenceCatalog:
    cursor.execute(
        """
        SELECT as_of_day,cohort_key,feature_name,sample_count,quantiles,method_version
        FROM feature_clock.feature_reference_distributions
        WHERE method_version=%s
          AND as_of_day=(
            SELECT max(as_of_day)
            FROM feature_clock.feature_reference_distributions
            WHERE method_version=%s AND as_of_day<=%s
          )
        ORDER BY feature_name,cohort_key
        """,
        (method_version, method_version, as_of_day),
    )
    distributions: list[QuantileDistribution] = []
    for row in _rows(cursor):
        quantiles = row["quantiles"]
        if isinstance(quantiles, str):
            quantiles = json.loads(quantiles)
        distributions.append(
            QuantileDistribution(
                as_of_day=row["as_of_day"],
                cohort_key=str(row["cohort_key"]),
                feature_name=str(row["feature_name"]),
                sample_count=int(row["sample_count"]),
                probabilities=tuple(float(value) for value in quantiles["probabilities"]),
                values=tuple(float(value) for value in quantiles["values"]),
                method_version=str(row["method_version"]),
            )
        )
    return ReferenceCatalog(tuple(distributions), minimum_cohort_size=minimum_cohort_size)


def load_collection_priority_signals(
    cursor: Any,
    *,
    channel_id: str,
    observed_at: datetime,
) -> CollectionPrioritySignals:
    cursor.execute(
        """
        SELECT user_query_demand,manual_priority,user_query_demand_expires_at
        FROM feature_clock.collection_priority_signals
        WHERE channel_id=%s
        """,
        (channel_id,),
    )
    row = _row(cursor)
    if row is None:
        return CollectionPrioritySignals()
    expires_at = row["user_query_demand_expires_at"]
    demand = float(row["user_query_demand"])
    if expires_at is not None and as_utc(expires_at, "user_query_demand_expires_at") <= as_utc(
        observed_at,
        "observed_at",
    ):
        demand = 0.0
    return CollectionPrioritySignals(
        user_query_demand=demand,
        manual_priority=float(row["manual_priority"]),
    )


def _shared_inputs(row: Mapping[str, Any]) -> SharedFeatureInputs:
    return SharedFeatureInputs(
        subscriber_count=row.get("last_subscriber_count"),
        subscriber_velocity_ewma=row.get("subscriber_velocity_ewma"),
        view_velocity_ewma=row.get("view_velocity_ewma"),
        recent30_video_count=row.get("recent30_video_count"),
        last_publish_at=row.get("last_publish_at"),
        about_identity_observed=row.get("last_about_observed_at") is not None,
        about_observed=row.get("last_about_observed_at") is not None,
        discovery_observed=row.get("last_discovery_observed_at") is not None,
        recent_sampling_observed=row.get("last_recent_sampling_at") is not None,
        agent_observed=row.get("last_agent_observed_at") is not None,
    )


def _shared_feature_values(result: Any, row: Mapping[str, Any]) -> tuple[Any, ...]:
    values = {
        column: getattr(result, column)
        for column in _SHARED_STATE_COLUMNS
        if column != "recent_change_probability"
    }
    values["recent_change_probability"] = derive_recent_change_probability(
        view_change=row.get("recent_view_change_ewma"),
        engagement_change=row.get("recent_engagement_change_ewma"),
        upload_change=row.get("recent_upload_change_ewma"),
        channel_activity=result.channel_activity,
    )
    return tuple(values[column] for column in _SHARED_STATE_COLUMNS)


def refresh_shared_feature_states(
    connection: Any,
    *,
    references: ReferenceCatalog,
    as_of_day: date,
    batch_size: int = 1000,
) -> int:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    as_of_instant = datetime.combine(as_of_day, time.min, tzinfo=timezone.utc)
    processed = 0
    with connection.cursor(name="feature_shared_state_scan") as reader:
        reader.execute(
            """
            SELECT state.channel_id,state.last_subscriber_count,
                   state.subscriber_velocity_ewma,state.view_velocity_ewma,
                   state.recent30_video_count,state.last_publish_at,
                   state.last_about_observed_at,
                   state.last_discovery_observed_at,state.last_recent_sampling_at,
                   state.last_agent_observed_at,
                   COALESCE(signal.user_query_demand,0.0) AS signal_user_query_demand,
                   COALESCE(signal.manual_priority,0.0) AS signal_manual_priority,
                   signal.user_query_demand_expires_at
            FROM feature_clock.channel_feature_state state
            LEFT JOIN feature_clock.collection_priority_signals signal
              ON signal.channel_id=state.channel_id
            ORDER BY state.channel_id
            """
        )
        columns = [_column_name(description) for description in reader.description]
        with connection.cursor() as writer:
            while True:
                values = reader.fetchmany(batch_size)
                if not values:
                    break
                updates: list[tuple[Any, ...]] = []
                for value in values:
                    row = dict(value) if isinstance(value, Mapping) else dict(
                        zip(columns, value, strict=True)
                    )
                    demand = float(row["signal_user_query_demand"])
                    expires_at = row["user_query_demand_expires_at"]
                    if expires_at is not None and as_utc(
                        expires_at,
                        "user_query_demand_expires_at",
                    ) <= as_of_instant:
                        demand = 0.0
                    result = derive_shared_features(
                        _shared_inputs(row),
                        references=references,
                        signals=CollectionPrioritySignals(
                            user_query_demand=demand,
                            manual_priority=float(row["signal_manual_priority"]),
                        ),
                        observed_at=as_of_instant,
                    )
                    feature_values = _shared_feature_values(result, row)
                    updates.append((*feature_values, row["channel_id"], *feature_values))
                writer.executemany(
                    """
                    UPDATE feature_clock.channel_feature_state
                    SET subscriber_size_percentile=%s,
                        subscriber_growth_percentile=%s,
                        view_growth_percentile=%s,
                        growth_momentum=%s,
                        user_query_demand=%s,
                        data_incompleteness=%s,
                        manual_priority=%s,
                        collection_priority=%s,
                        channel_activity=%s,
                        recent_change_probability=%s,
                        reference_distribution_version=%s,
                        state_version=state_version+1,
                        updated_at=now()
                    WHERE channel_id=%s
                      AND ROW(
                        subscriber_size_percentile,subscriber_growth_percentile,
                        view_growth_percentile,growth_momentum,user_query_demand,
                        data_incompleteness,manual_priority,collection_priority,
                        channel_activity,recent_change_probability,
                        reference_distribution_version
                      ) IS DISTINCT FROM ROW(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    updates,
                )
                processed += len(values)
    return processed


class FeatureReferenceRefresher:
    def __init__(self, connection_factory: Callable[[], Any]) -> None:
        self._connection_factory = connection_factory

    def refresh(
        self,
        *,
        as_of_day: date,
        minimum_cohort_size: int = 20,
        batch_size: int = 1000,
    ) -> ReferenceRefreshResult:
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    distributions = build_reference_distributions(
                        cursor,
                        as_of_day=as_of_day,
                    )
                    save_reference_distributions(cursor, distributions)
                    references = load_reference_catalog(
                        cursor,
                        as_of_day=as_of_day,
                        minimum_cohort_size=minimum_cohort_size,
                    )
                processed = refresh_shared_feature_states(
                    connection,
                    references=references,
                    as_of_day=as_of_day,
                    batch_size=batch_size,
                )
            return ReferenceRefreshResult(
                as_of_day=as_of_day,
                method_version=REFERENCE_METHOD_VERSION,
                distribution_count=len(distributions),
                processed_channels=processed,
                reference_distribution_version=references.version,
            )
        finally:
            connection.close()


class CollectionPrioritySignalStore:
    def __init__(self, connection_factory: Callable[[], Any]) -> None:
        self._connection_factory = connection_factory

    def upsert(
        self,
        *,
        channel_id: str,
        signals: CollectionPrioritySignals,
        source_version: str,
        observed_at: datetime,
        user_query_demand_expires_at: datetime | None = None,
    ) -> bool:
        normalized_channel_id = str(channel_id).strip()
        normalized_source = str(source_version).strip()
        if not normalized_channel_id or not normalized_source:
            raise ValueError("channel_id and source_version are required")
        observed = as_utc(observed_at, "observed_at")
        expires = (
            as_utc(user_query_demand_expires_at, "user_query_demand_expires_at")
            if user_query_demand_expires_at is not None
            else None
        )
        if expires is not None and expires <= observed:
            raise ValueError("user_query_demand_expires_at must be after observed_at")
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        """
                        INSERT INTO feature_clock.collection_priority_signals (
                          channel_id,user_query_demand,manual_priority,source_version,
                          observed_at,user_query_demand_expires_at
                        ) VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (channel_id) DO UPDATE
                        SET user_query_demand=EXCLUDED.user_query_demand,
                            manual_priority=EXCLUDED.manual_priority,
                            source_version=EXCLUDED.source_version,
                            observed_at=EXCLUDED.observed_at,
                            user_query_demand_expires_at=EXCLUDED.user_query_demand_expires_at,
                            updated_at=now()
                        WHERE EXCLUDED.observed_at >=
                              feature_clock.collection_priority_signals.observed_at
                        RETURNING channel_id
                        """,
                        (
                            normalized_channel_id,
                            signals.user_query_demand,
                            signals.manual_priority,
                            normalized_source,
                            observed,
                            expires,
                        ),
                    )
                    if cursor.fetchone() is None:
                        return False

                    cursor.execute(
                        """
                        SELECT *
                        FROM feature_clock.channel_feature_state
                        WHERE channel_id=%s
                        FOR UPDATE
                        """,
                        (normalized_channel_id,),
                    )
                    state_row = _row(cursor)
                    if state_row is None:
                        return True

                    references = load_reference_catalog(
                        cursor,
                        as_of_day=observed.date(),
                    )
                    effective_signals = load_collection_priority_signals(
                        cursor,
                        channel_id=normalized_channel_id,
                        observed_at=observed,
                    )
                    shared = derive_shared_features(
                        _shared_inputs(state_row),
                        references=references,
                        signals=effective_signals,
                        observed_at=observed,
                    )
                    feature_values = _shared_feature_values(shared, state_row)
                    current_values = tuple(
                        state_row.get(column) for column in _SHARED_STATE_COLUMNS
                    )
                    if current_values == feature_values:
                        return True

                    cursor.execute(
                        """
                        UPDATE feature_clock.channel_feature_state
                        SET subscriber_size_percentile=%s,
                            subscriber_growth_percentile=%s,
                            view_growth_percentile=%s,
                            growth_momentum=%s,
                            user_query_demand=%s,
                            data_incompleteness=%s,
                            manual_priority=%s,
                            collection_priority=%s,
                            channel_activity=%s,
                            recent_change_probability=%s,
                            reference_distribution_version=%s,
                            state_version=state_version+1,
                            updated_at=now()
                        WHERE channel_id=%s
                        """,
                        (*feature_values, normalized_channel_id),
                    )
                    cursor.execute(
                        """
                        SELECT state.*,
                               channel_clock.about_due_day AS clock_about_due_day,
                               channel_clock.about_tier AS clock_about_tier,
                               channel_clock.video_due_day AS clock_video_due_day,
                               channel_clock.video_tier AS clock_video_tier,
                               channel_clock.agent_due_day AS clock_agent_due_day,
                               channel_clock.agent_tier AS clock_agent_tier,
                               channel_clock.clock_version AS channel_clock_version
                        FROM feature_clock.channel_feature_state state
                        JOIN feature_clock.channel_clock_state channel_clock USING (channel_id)
                        WHERE state.channel_id=%s
                        FOR UPDATE OF state,channel_clock
                        """,
                        (normalized_channel_id,),
                    )
                    clock_row = _row(cursor)
                    if clock_row is None:
                        raise RuntimeError(
                            "Feature State row must have a Channel Clock row"
                        )

                    # Imported lazily because the online Applier imports this module.
                    from .rebuild import _active_policy, recalculate_locked_clock_row

                    policy = _active_policy(cursor)
                    recalculate_locked_clock_row(
                        cursor,
                        row=clock_row,
                        policy=policy,
                        decision_mode="repair",
                        audit_reason="collection_priority_signal_update",
                        audit_context={
                            "source_version": normalized_source,
                            "observed_at": observed.isoformat(),
                            "user_query_demand_expires_at": (
                                expires.isoformat() if expires is not None else None
                            ),
                        },
                    )
                    return True
        finally:
            connection.close()
