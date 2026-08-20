from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Annotated, Any, Callable, Literal, Mapping
from uuid import UUID, uuid4, uuid5

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
)

from .applier import FeatureObservationApplier
from .clock_window import clock_due_at_for_day
from .contracts import ActivePolicyContract
from .events import CrawlerObservationRecorded, EventValidationError
from .rebuild import _active_policy, _json, _row
from .recalculation import (
    RECALCULATION_COORDINATION_LOCK_ID,
    RECALCULATION_RESUME_LOCK_ID,
    UNRESOLVED_RECALCULATION_STATUSES,
)


BASELINE_BUNDLE_FORMAT = "crawler-observation-ndjson-v1"
BOOTSTRAP_DECISION_NAMESPACE = UUID("895d9e61-f25e-4472-94f5-c3d83888596c")
OBSERVATION_KINDS = (
    "about",
    "video",
    "agent",
)
NonEmptyText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
Sha256Text = Annotated[str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")]


class BootstrapError(RuntimeError):
    pass


class BaselineBundleValidationError(ValueError):
    pass


class BaselineManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    schema_version: Literal[1]
    bundle_format: Literal[BASELINE_BUNDLE_FORMAT]
    baseline_version: NonEmptyText
    source_database: NonEmptyText
    source_schema: Literal["crawler"]
    source_snapshot_id: NonEmptyText
    exported_at: AwareDatetime
    events_file: NonEmptyText
    event_count: Annotated[int, Field(gt=0)]
    channel_count: Annotated[int, Field(gt=0)]
    byte_count: Annotated[int, Field(gt=0)]
    events_sha256: Sha256Text

    @field_validator("exported_at")
    @classmethod
    def normalize_exported_at(cls, value: datetime) -> datetime:
        return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class BaselineBundle:
    manifest: BaselineManifest
    manifest_sha256: str
    events_path: Path
    events: tuple[CrawlerObservationRecorded, ...]
    channel_ids: tuple[str, ...]
    checkpoint_sequences: tuple[tuple[str, str, int], ...]


@dataclass(frozen=True, slots=True)
class BootstrapResult:
    recalculation_id: str
    baseline_version: str
    policy_version: str
    status: str
    processed_events: int
    processed_channels: int
    failed_channels: int
    succeeded_shards: int
    failed_shards: int


def _digest(data: bytes) -> str:
    return f"sha256:{sha256(data).hexdigest()}"


def _manifest_from_bytes(data: bytes) -> BaselineManifest:
    try:
        return BaselineManifest.model_validate_json(data)
    except (ValidationError, ValueError) as error:
        raise BaselineBundleValidationError(f"invalid Baseline Manifest: {error}") from error


def _resolve_events_path(manifest_path: Path, events_file: str) -> Path:
    relative = Path(events_file)
    if relative.is_absolute() or relative.name != events_file:
        raise BaselineBundleValidationError("events_file must be a file name beside the Manifest")
    bundle_directory = manifest_path.parent.resolve()
    events_path = (bundle_directory / relative).resolve()
    if events_path.parent != bundle_directory:
        raise BaselineBundleValidationError("events_file escapes the Baseline bundle directory")
    if not events_path.is_file():
        raise BaselineBundleValidationError("Baseline events file does not exist")
    return events_path


def load_baseline_bundle(manifest_path: str | Path) -> BaselineBundle:
    """Validate the complete immutable input before a Feature-state write is possible."""

    path = Path(manifest_path).resolve()
    if not path.is_file():
        raise BaselineBundleValidationError("Baseline Manifest does not exist")
    manifest_bytes = path.read_bytes()
    manifest = _manifest_from_bytes(manifest_bytes)
    events_path = _resolve_events_path(path, manifest.events_file)
    event_bytes = events_path.read_bytes()
    if len(event_bytes) != manifest.byte_count:
        raise BaselineBundleValidationError("Baseline events byte_count does not match Manifest")
    if _digest(event_bytes) != manifest.events_sha256:
        raise BaselineBundleValidationError("Baseline events SHA-256 does not match Manifest")

    raw_lines = event_bytes.splitlines()
    if len(raw_lines) != manifest.event_count:
        raise BaselineBundleValidationError("Baseline event_count does not match Manifest")

    events: list[CrawlerObservationRecorded] = []
    seen_event_ids: set[str] = set()
    seen_observation_ids: set[str] = set()
    seen_sequence_ids: set[tuple[str, str, int]] = set()
    checkpoint_sequences: dict[tuple[str, str], int] = {}
    channels_with_facts: set[str] = set()
    previous_sort_key: tuple[str, str, int] | None = None
    for line_number, raw_line in enumerate(raw_lines, start=1):
        if not raw_line.strip():
            raise BaselineBundleValidationError(
                f"Baseline event line {line_number} cannot be empty"
            )
        try:
            source = json.loads(raw_line)
            if not isinstance(source, Mapping):
                raise TypeError("event must be an object")
            event = CrawlerObservationRecorded.from_mapping(source)
        except (json.JSONDecodeError, EventValidationError, TypeError) as error:
            raise BaselineBundleValidationError(
                f"invalid Baseline event line {line_number}: {error}"
            ) from error
        if event.observed_at > manifest.exported_at:
            raise BaselineBundleValidationError(
                f"Baseline event line {line_number} is newer than exported_at"
            )
        sequence_id = (event.channel_id, event.observation_kind, event.kind_sequence)
        sort_key = sequence_id
        if previous_sort_key is not None and sort_key <= previous_sort_key:
            raise BaselineBundleValidationError(
                "Baseline events must be strictly sorted by channel_id, observation_kind, "
                "kind_sequence"
            )
        previous_sort_key = sort_key
        if event.event_id in seen_event_ids:
            raise BaselineBundleValidationError("Baseline contains a duplicate event_id")
        if event.observation_id in seen_observation_ids:
            raise BaselineBundleValidationError("Baseline contains a duplicate observation_id")
        if sequence_id in seen_sequence_ids:
            raise BaselineBundleValidationError("Baseline contains a duplicate kind_sequence")
        key = (event.channel_id, event.observation_kind)
        expected_sequence = checkpoint_sequences.get(key, 0) + 1
        if event.kind_sequence != expected_sequence:
            raise BaselineBundleValidationError(
                f"Baseline sequence for {event.channel_id}/{event.observation_kind} "
                f"must be contiguous from 1; expected {expected_sequence}, "
                f"got {event.kind_sequence}"
            )
        checkpoint_sequences[key] = event.kind_sequence
        seen_event_ids.add(event.event_id)
        seen_observation_ids.add(event.observation_id)
        seen_sequence_ids.add(sequence_id)
        if event.outcome != "failed":
            channels_with_facts.add(event.channel_id)
        events.append(event)

    channel_ids = tuple(sorted({event.channel_id for event in events}))
    if len(channel_ids) != manifest.channel_count:
        raise BaselineBundleValidationError("Baseline channel_count does not match Manifest")
    missing_facts = sorted(set(channel_ids) - channels_with_facts)
    if missing_facts:
        raise BaselineBundleValidationError(
            f"{len(missing_facts)} Baseline Channels have no non-failed Observation"
        )
    return BaselineBundle(
        manifest=manifest,
        manifest_sha256=_digest(manifest_bytes),
        events_path=events_path,
        events=tuple(events),
        channel_ids=channel_ids,
        checkpoint_sequences=tuple(
            (channel_id, kind, sequence)
            for (channel_id, kind), sequence in sorted(checkpoint_sequences.items())
        ),
    )


def stable_bootstrap_shard(channel_id: str, shard_count: int) -> int:
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    digest = sha256(f"{channel_id}:initial-bootstrap-shard".encode()).digest()
    return int.from_bytes(digest[:8], "big") % shard_count


def stable_bootstrap_offset(
    channel_id: str,
    clock_kind: str,
    baseline_version: str,
    minimum_days: int,
    maximum_days: int,
) -> int:
    if minimum_days <= 0 or maximum_days < minimum_days:
        raise ValueError("invalid Bootstrap spread window")
    digest = sha256(
        f"{baseline_version}:{channel_id}:{clock_kind}:initial-bootstrap-due".encode()
    ).digest()
    return minimum_days + (
        int.from_bytes(digest[:8], "big") % (maximum_days - minimum_days + 1)
    )


class FeatureBootstrapper:
    """Import a validated Crawler Baseline bundle with resumable Feature shards."""

    def __init__(self, connection_factory: Callable[[], Any]) -> None:
        self._connection_factory = connection_factory
        self._applier = FeatureObservationApplier(connection_factory)

    @staticmethod
    def _run_checksum(
        bundle: BaselineBundle,
        *,
        policy_version: str,
        shard_count: int,
    ) -> str:
        body = {
            "mode": "initial_bootstrap",
            "baseline_version": bundle.manifest.baseline_version,
            "manifest_sha256": bundle.manifest_sha256,
            "events_sha256": bundle.manifest.events_sha256,
            "event_count": bundle.manifest.event_count,
            "channel_count": bundle.manifest.channel_count,
            "policy_version": policy_version,
            "shard_count": shard_count,
        }
        return _digest(_json(body).encode())

    def start(
        self,
        bundle: BaselineBundle,
        *,
        shard_count: int = 128,
        policy_version: str | None = None,
    ) -> str:
        if not isinstance(bundle, BaselineBundle):
            raise TypeError("a validated BaselineBundle is required")
        if shard_count <= 0:
            raise ValueError("shard_count must be positive")
        recalculation_id = str(uuid4())
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        "SELECT pg_advisory_xact_lock(%s)",
                        (RECALCULATION_COORDINATION_LOCK_ID,),
                    )
                    cursor.execute(
                        """
                        SELECT recalculation_id,status,checksum,policy_version,shard_count
                        FROM feature_clock.recalculation_runs
                        WHERE mode='initial_bootstrap' AND source_baseline_version=%s
                        FOR UPDATE
                        """,
                        (bundle.manifest.baseline_version,),
                    )
                    existing = _row(cursor)
                    if existing is not None:
                        if shard_count != int(existing["shard_count"]):
                            raise BootstrapError(
                                "Bootstrap shard_count cannot change for an existing Baseline"
                            )
                        if (
                            policy_version is not None
                            and policy_version != str(existing["policy_version"])
                        ):
                            raise BootstrapError(
                                "Bootstrap policy_version cannot change for an existing Baseline"
                            )
                        checksum = self._run_checksum(
                            bundle,
                            policy_version=str(existing["policy_version"]),
                            shard_count=int(existing["shard_count"]),
                        )
                        if existing["checksum"] != checksum:
                            raise BootstrapError(
                                "Baseline version already exists with different immutable inputs"
                            )
                        self._verify_manifest(cursor, bundle)
                        return str(existing["recalculation_id"])
                    policy = _active_policy(cursor, policy_version)
                    checksum = self._run_checksum(
                        bundle,
                        policy_version=policy.policy_version,
                        shard_count=shard_count,
                    )
                    cursor.execute(
                        """
                        SELECT recalculation_id,status
                        FROM feature_clock.recalculation_runs
                        WHERE status=ANY(%s)
                        ORDER BY created_at,recalculation_id
                        LIMIT 1
                        FOR UPDATE
                        """,
                        (list(UNRESOLVED_RECALCULATION_STATUSES),),
                    )
                    unresolved = _row(cursor)
                    if unresolved is not None:
                        raise BootstrapError(
                            "unresolved Feature recalculation "
                            f"{unresolved['recalculation_id']} ({unresolved['status']}) "
                            "must be resumed or cancelled before Bootstrap"
                        )
                    self._register_manifest(cursor, bundle)
                    cursor.execute(
                        """
                        INSERT INTO feature_clock.recalculation_runs (
                          recalculation_id,mode,policy_version,source_baseline_version,
                          shard_count,status,started_at,checksum
                        ) VALUES (%s,'initial_bootstrap',%s,%s,%s,'running',now(),%s)
                        """,
                        (
                            recalculation_id,
                            policy.policy_version,
                            bundle.manifest.baseline_version,
                            shard_count,
                            checksum,
                        ),
                    )
                    cursor.executemany(
                        """
                        INSERT INTO feature_clock.recalculation_shards (
                          recalculation_id,shard_id,status
                        ) VALUES (%s,%s,'pending')
                        """,
                        ((recalculation_id, shard_id) for shard_id in range(shard_count)),
                    )
            return recalculation_id
        finally:
            connection.close()

    @staticmethod
    def _register_manifest(cursor: Any, bundle: BaselineBundle) -> None:
        manifest = bundle.manifest
        cursor.execute(
            """
            INSERT INTO feature_clock.baseline_bundle_manifests (
              baseline_version,schema_version,bundle_format,source_database,
              source_schema,source_snapshot_id,exported_at,events_file,
              event_count,channel_count,byte_count,events_sha256,manifest_sha256
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (baseline_version) DO NOTHING
            RETURNING baseline_version
            """,
            (
                manifest.baseline_version,
                manifest.schema_version,
                manifest.bundle_format,
                manifest.source_database,
                manifest.source_schema,
                manifest.source_snapshot_id,
                manifest.exported_at,
                manifest.events_file,
                manifest.event_count,
                manifest.channel_count,
                manifest.byte_count,
                manifest.events_sha256,
                bundle.manifest_sha256,
            ),
        )
        if _row(cursor) is not None:
            return
        cursor.execute(
            """
            SELECT schema_version,bundle_format,source_database,source_schema,
                   source_snapshot_id,exported_at,events_file,event_count,
                   channel_count,byte_count,events_sha256,manifest_sha256
            FROM feature_clock.baseline_bundle_manifests
            WHERE baseline_version=%s
            """,
            (manifest.baseline_version,),
        )
        existing = _row(cursor)
        expected = {
            "schema_version": manifest.schema_version,
            "bundle_format": manifest.bundle_format,
            "source_database": manifest.source_database,
            "source_schema": manifest.source_schema,
            "source_snapshot_id": manifest.source_snapshot_id,
            "exported_at": manifest.exported_at,
            "events_file": manifest.events_file,
            "event_count": manifest.event_count,
            "channel_count": manifest.channel_count,
            "byte_count": manifest.byte_count,
            "events_sha256": manifest.events_sha256,
            "manifest_sha256": bundle.manifest_sha256,
        }
        if existing != expected:
            raise BootstrapError(
                "Baseline version already has a different persisted Manifest"
            )

    def bootstrap(
        self,
        bundle: BaselineBundle,
        *,
        shard_count: int = 128,
        batch_size: int = 100,
        policy_version: str | None = None,
        worker_id: str = "feature-initial-bootstrap",
    ) -> BootstrapResult:
        recalculation_id = self.start(
            bundle,
            shard_count=shard_count,
            policy_version=policy_version,
        )
        return self.resume(
            recalculation_id,
            bundle,
            batch_size=batch_size,
            worker_id=worker_id,
        )

    def resume(
        self,
        recalculation_id: str,
        bundle: BaselineBundle,
        *,
        batch_size: int = 100,
        worker_id: str = "feature-initial-bootstrap",
    ) -> BootstrapResult:
        if not isinstance(bundle, BaselineBundle):
            raise TypeError("a validated BaselineBundle is required")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if not str(worker_id).strip():
            raise ValueError("worker_id is required")
        execution_connection = self._acquire_execution_lock()
        try:
            run = self._prepare_run(recalculation_id, bundle)
            if run["status"] == "succeeded":
                return self._finish_run(recalculation_id, bundle)
            try:
                shard_count = int(run["shard_count"])
                for shard_id in range(shard_count):
                    self._process_shard(
                        recalculation_id,
                        bundle,
                        shard_id=shard_id,
                        shard_count=shard_count,
                        policy_version=str(run["policy_version"]),
                        batch_size=batch_size,
                        worker_id=str(worker_id).strip(),
                    )
                return self._finish_run(recalculation_id, bundle)
            except Exception:
                self.fail(recalculation_id)
                raise
        finally:
            execution_connection.close()

    def _acquire_execution_lock(self) -> Any:
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        "SELECT pg_try_advisory_lock(%s) AS acquired",
                        (RECALCULATION_RESUME_LOCK_ID,),
                    )
                    row = _row(cursor)
                    if row is None or not bool(row["acquired"]):
                        raise BootstrapError(
                            "another Feature recalculation worker is already running"
                        )
            return connection
        except Exception:
            connection.close()
            raise

    def _prepare_run(
        self,
        recalculation_id: str,
        bundle: BaselineBundle,
    ) -> dict[str, Any]:
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        """
                        SELECT *
                        FROM feature_clock.recalculation_runs
                        WHERE recalculation_id=%s AND mode='initial_bootstrap'
                        FOR UPDATE
                        """,
                        (recalculation_id,),
                    )
                    run = _row(cursor)
                    if run is None:
                        raise BootstrapError("initial Bootstrap run does not exist")
                    if run["source_baseline_version"] != bundle.manifest.baseline_version:
                        raise BootstrapError("Bootstrap Run and Manifest versions differ")
                    expected_checksum = self._run_checksum(
                        bundle,
                        policy_version=str(run["policy_version"]),
                        shard_count=int(run["shard_count"]),
                    )
                    if run["checksum"] != expected_checksum:
                        raise BootstrapError("Bootstrap Run immutable inputs differ")
                    self._verify_manifest(cursor, bundle)
                    if run["status"] == "cancelled":
                        raise BootstrapError("cancelled initial Bootstrap cannot be resumed")
                    if run["status"] == "succeeded":
                        return run
                    _active_policy(cursor, str(run["policy_version"]))
                    cursor.execute(
                        """
                        UPDATE feature_clock.recalculation_runs
                        SET status='running',completed_at=NULL,updated_at=now()
                        WHERE recalculation_id=%s
                        """,
                        (recalculation_id,),
                    )
                    run["status"] = "running"
                    return run
        finally:
            connection.close()

    @staticmethod
    def _verify_manifest(cursor: Any, bundle: BaselineBundle) -> None:
        cursor.execute(
            """
            SELECT events_sha256,manifest_sha256,event_count,channel_count,byte_count
            FROM feature_clock.baseline_bundle_manifests
            WHERE baseline_version=%s
            """,
            (bundle.manifest.baseline_version,),
        )
        row = _row(cursor)
        expected = (
            bundle.manifest.events_sha256,
            bundle.manifest_sha256,
            bundle.manifest.event_count,
            bundle.manifest.channel_count,
            bundle.manifest.byte_count,
        )
        actual = (
            row["events_sha256"],
            row["manifest_sha256"],
            int(row["event_count"]),
            int(row["channel_count"]),
            int(row["byte_count"]),
        ) if row is not None else None
        if actual != expected:
            raise BootstrapError("persisted Baseline Manifest does not match input bundle")

    def _process_shard(
        self,
        recalculation_id: str,
        bundle: BaselineBundle,
        *,
        shard_id: int,
        shard_count: int,
        policy_version: str,
        batch_size: int,
        worker_id: str,
    ) -> None:
        indexed_events = tuple(
            (index, event)
            for index, event in enumerate(bundle.events, start=1)
            if stable_bootstrap_shard(event.channel_id, shard_count) == shard_id
        )
        shard_channels = tuple(
            channel_id
            for channel_id in bundle.channel_ids
            if stable_bootstrap_shard(channel_id, shard_count) == shard_id
        )
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        """
                        UPDATE feature_clock.recalculation_shards
                        SET status='running',lease_owner=%s,
                            lease_expires_at=now()+interval '15 minutes',
                            started_at=COALESCE(started_at,now()),completed_at=NULL,
                            last_error=NULL,updated_at=now()
                        WHERE recalculation_id=%s AND shard_id=%s
                          AND (
                            status IN ('pending','failed')
                            OR (status='running' AND lease_expires_at<=now())
                          )
                        RETURNING cursor
                        """,
                        (worker_id, recalculation_id, shard_id),
                    )
                    claimed = _row(cursor)
                    if claimed is None:
                        return
                    last_index = int(claimed["cursor"] or 0)

            remaining = [(index, event) for index, event in indexed_events if index > last_index]
            for start in range(0, len(remaining), batch_size):
                batch = remaining[start : start + batch_size]
                for _index, event in batch:
                    applied = self._applier.apply_crawler_observation(event)
                    if applied.status != "applied":
                        raise BootstrapError(
                            f"Baseline event {event.event_id} ended as {applied.status}"
                        )
                last_index = batch[-1][0]
                with connection.transaction():
                    with connection.cursor() as cursor:
                        cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                        cursor.execute(
                            """
                            UPDATE feature_clock.recalculation_shards
                            SET cursor=%s,processed_rows=processed_rows+%s,
                                failed_rows=0,lease_expires_at=now()+interval '15 minutes',
                                updated_at=now()
                            WHERE recalculation_id=%s AND shard_id=%s
                              AND status='running' AND lease_owner=%s
                            """,
                            (
                                str(last_index),
                                len(batch),
                                recalculation_id,
                                shard_id,
                                worker_id,
                            ),
                        )
                        if cursor.rowcount != 1:
                            raise BootstrapError("initial Bootstrap shard lease was lost")

            expected_by_channel: dict[str, dict[str, int]] = {}
            for channel_id, kind, sequence in bundle.checkpoint_sequences:
                expected_by_channel.setdefault(channel_id, {})[kind] = sequence
            for channel_id in shard_channels:
                self._spread_channel_clocks(
                    recalculation_id,
                    bundle,
                    channel_id=channel_id,
                    expected_sequences=expected_by_channel[channel_id],
                    policy_version=policy_version,
                )

            checksum_body = {
                "recalculation_id": recalculation_id,
                "baseline_version": bundle.manifest.baseline_version,
                "shard_id": shard_id,
                "event_indexes": [index for index, _event in indexed_events],
                "channel_ids": list(shard_channels),
            }
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        """
                        UPDATE feature_clock.recalculation_shards
                        SET status='succeeded',failed_rows=0,lease_owner=NULL,
                            lease_expires_at=NULL,completed_at=now(),last_error=NULL,
                            checksum=%s,updated_at=now()
                        WHERE recalculation_id=%s AND shard_id=%s
                          AND status='running' AND lease_owner=%s
                        """,
                        (
                            _digest(_json(checksum_body).encode()),
                            recalculation_id,
                            shard_id,
                            worker_id,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise BootstrapError("initial Bootstrap shard lease was lost")
        except Exception as error:
            self._mark_shard_failed(recalculation_id, shard_id, worker_id, error)
        finally:
            connection.close()

    def _spread_channel_clocks(
        self,
        recalculation_id: str,
        bundle: BaselineBundle,
        *,
        channel_id: str,
        expected_sequences: Mapping[str, int],
        policy_version: str,
    ) -> None:
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        """
                        SELECT checksum
                        FROM feature_clock.bootstrap_channel_receipts
                        WHERE baseline_version=%s AND channel_id=%s
                        FOR UPDATE
                        """,
                        (bundle.manifest.baseline_version, channel_id),
                    )
                    if _row(cursor) is not None:
                        return
                    cursor.execute(
                        """
                        SELECT observation_kind,last_applied_sequence
                        FROM feature_clock.channel_observation_checkpoints
                        WHERE channel_id=%s
                        ORDER BY observation_kind
                        FOR UPDATE
                        """,
                        (channel_id,),
                    )
                    checkpoint_rows = cursor.fetchall()
                    actual_sequences = {
                        (row["observation_kind"] if isinstance(row, Mapping) else row[0]): int(
                            row["last_applied_sequence"] if isinstance(row, Mapping) else row[1]
                        )
                        for row in checkpoint_rows
                    }
                    if actual_sequences != dict(expected_sequences):
                        raise BootstrapError(
                            f"Feature checkpoints for {channel_id} moved beyond the Baseline"
                        )
                    cursor.execute(
                        """
                        SELECT state.state_version,state.reference_distribution_version,
                               state.last_about_observed_at,
                               state.last_discovery_observed_at,state.last_recent_sampling_at,
                               state.last_agent_observed_at,
                               channel.about_due_at,
                               channel.about_due_day,channel.about_tier,
                               channel.video_due_at,
                               channel.video_due_day,channel.video_tier,
                               channel.agent_due_at,
                               channel.agent_due_day,channel.agent_tier,
                               channel.clock_version AS channel_clock_version,
                               channel.policy_version AS channel_policy_version
                        FROM feature_clock.channel_feature_state state
                        JOIN feature_clock.channel_clock_state channel USING (channel_id)
                        WHERE state.channel_id=%s
                        FOR UPDATE OF state,channel
                        """,
                        (channel_id,),
                    )
                    row = _row(cursor)
                    if row is None:
                        raise BootstrapError(
                            f"Baseline Channel {channel_id} has incomplete Feature/Clock coverage"
                        )
                    if row["channel_policy_version"] != policy_version:
                        raise BootstrapError("Bootstrap Clock policy version changed during import")
                    policy = _active_policy(cursor, policy_version)
                    schedule = self._spread_schedule(
                        bundle,
                        channel_id=channel_id,
                        row=row,
                        policy=policy,
                    )
                    channel_before = int(row["channel_clock_version"])
                    channel_after = channel_before + 1
                    about_due_at = schedule["about"][0]
                    video_due_at = schedule["video"][0]
                    agent_due_at = schedule["agent"][0]
                    next_run_at = min(about_due_at,video_due_at,agent_due_at)
                    next_run_day = min(
                        schedule["about"][0].date(),
                        schedule["video"][0].date(),schedule["agent"][0].date(),
                    )
                    cursor.execute(
                        """
                        UPDATE feature_clock.channel_clock_state
                        SET about_due_at=%s,about_due_day=%s,
                            video_due_at=%s,video_due_day=%s,video_tier=%s,
                            agent_due_at=%s,agent_due_day=%s,
                            channel_next_run_at=%s,channel_next_run_day=%s,
                            clock_version=%s,updated_at=now()
                        WHERE channel_id=%s AND clock_version=%s
                        """,
                        (
                            about_due_at,
                            about_due_at.date(),
                            video_due_at,
                            video_due_at.date(),
                            int(row["video_tier"]),
                            agent_due_at,
                            agent_due_at.date(),
                            next_run_at,
                            next_run_day,
                            channel_after,
                            channel_id,
                            channel_before,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise BootstrapError("Channel Clock Bootstrap spread update failed")
                    previous = {
                        "about": row["about_due_at"],
                        "video": row["video_due_at"],
                        "agent": row["agent_due_at"],
                    }
                    tiers = {
                        "about": int(row["about_tier"]),
                        "video": int(row["video_tier"]),
                        "agent": int(row["agent_tier"]),
                    }
                    for kind in OBSERVATION_KINDS:
                        due_at, offset, minimum, maximum = schedule[kind]
                        cursor.execute(
                            """
                            INSERT INTO feature_clock.clock_decision_log (
                              decision_id,channel_id,clock_kind,decision_mode,
                              previous_due_at,previous_due_day,decided_due_at,decided_due_day,
                              tier,reason_codes,
                              feature_state_version,feature_summary_json,policy_version,
                              reference_distribution_version,
                              clock_version_before,clock_version_after
                            ) VALUES (
                              %s,%s,%s,'bootstrap',%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,
                              %s,%s,%s,%s
                            )
                            """,
                            (
                                str(
                                    uuid5(
                                        BOOTSTRAP_DECISION_NAMESPACE,
                                        f"{bundle.manifest.baseline_version}:{channel_id}:{kind}",
                                    )
                                ),
                                channel_id,
                                kind,
                                previous[kind],
                                previous[kind].date(),
                                due_at,
                                due_at.date(),
                                tiers[kind],
                                ["initial_bootstrap_spread"],
                                int(row["state_version"]),
                                _json(
                                    {
                                        "baseline_version": bundle.manifest.baseline_version,
                                        "bootstrap_base_day": bundle.manifest.exported_at.date().isoformat(),
                                        "stable_offset_days": offset,
                                        "spread_min_days": minimum,
                                        "spread_max_days": maximum,
                                        "policy_due_at": previous[kind].isoformat(),
                                        "recalculation_id": recalculation_id,
                                    }
                                ),
                                policy_version,
                                row["reference_distribution_version"],
                                channel_before,
                                channel_after,
                            ),
                        )
                    receipt_body = {
                        "baseline_version": bundle.manifest.baseline_version,
                        "channel_id": channel_id,
                        "due_days": {
                            kind: schedule[kind][0].date().isoformat()
                            for kind in OBSERVATION_KINDS
                        },
                        "due_at": {
                            kind: schedule[kind][0].isoformat()
                            for kind in OBSERVATION_KINDS
                        },
                        "channel_clock_version": channel_after,
                    }
                    cursor.execute(
                        """
                        INSERT INTO feature_clock.bootstrap_channel_receipts (
                          baseline_version,channel_id,recalculation_id,
                          channel_clock_version,checksum
                        ) VALUES (%s,%s,%s,%s,%s)
                        """,
                        (
                            bundle.manifest.baseline_version,
                            channel_id,
                            recalculation_id,
                            channel_after,
                            _digest(_json(receipt_body).encode()),
                        ),
                    )
        finally:
            connection.close()

    @staticmethod
    def _spread_schedule(
        bundle: BaselineBundle,
        *,
        channel_id: str,
        row: Mapping[str, Any],
        policy: ActivePolicyContract,
    ) -> dict[str, tuple[datetime, int, int, int]]:
        base_at = bundle.manifest.exported_at.astimezone(timezone.utc)
        specifications = {
            "about": (
                row["about_due_at"],
                row["about_due_day"],
                row["about_tier"],
                row["last_about_observed_at"],
                1,
                int(policy.about_config.baseline_interval_days),
            ),
            "video": (
                row["video_due_at"],
                row["video_due_day"],
                row["video_tier"],
                row["last_discovery_observed_at"],
                1,
                int(policy.discovery_config.fallback_interval_days),
            ),
            "agent": (
                row["agent_due_at"],
                row["agent_due_day"],
                row["agent_tier"],
                row["last_agent_observed_at"],
                int(policy.agent_config.bootstrap_min_days),
                int(policy.agent_config.bootstrap_max_days),
            ),
        }
        output: dict[str, tuple[datetime, int, int, int]] = {}
        for kind, (
            previous_due_at,previous_due_day,tier,observed_at,fallback_min,fallback_max
        ) in specifications.items():
            minimum = 1 if observed_at is not None else fallback_min
            maximum = max(minimum, int(tier) if observed_at is not None else fallback_max)
            offset = stable_bootstrap_offset(
                channel_id,
                kind,
                bundle.manifest.baseline_version,
                minimum,
                maximum,
            )
            candidate_day = base_at.date() + timedelta(days=offset)
            selected_due_day = (
                candidate_day
                if observed_at is None or previous_due_day <= base_at.date()
                else min(previous_due_day, candidate_day)
            )
            due_at = clock_due_at_for_day(selected_due_day)
            output[kind] = (due_at, offset, minimum, maximum)
        return output

    def _mark_shard_failed(
        self,
        recalculation_id: str,
        shard_id: int,
        worker_id: str,
        error: Exception,
    ) -> None:
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        """
                        UPDATE feature_clock.recalculation_shards
                        SET status='failed',failed_rows=1,lease_owner=NULL,
                            lease_expires_at=NULL,completed_at=now(),last_error=%s,
                            updated_at=now()
                        WHERE recalculation_id=%s AND shard_id=%s
                          AND status='running' AND lease_owner=%s
                        """,
                        (str(error)[:2000], recalculation_id, shard_id, worker_id),
                    )
        finally:
            connection.close()

    def fail(self, recalculation_id: str) -> None:
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        """
                        UPDATE feature_clock.recalculation_runs
                        SET status='failed',completed_at=now(),updated_at=now()
                        WHERE recalculation_id=%s
                          AND mode='initial_bootstrap'
                          AND status IN ('pending','running','partial')
                        """,
                        (recalculation_id,),
                    )
        finally:
            connection.close()

    def _finish_run(
        self,
        recalculation_id: str,
        bundle: BaselineBundle,
    ) -> BootstrapResult:
        connection = self._connection_factory()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor.execute(
                        """
                        SELECT policy_version,status
                        FROM feature_clock.recalculation_runs
                        WHERE recalculation_id=%s AND mode='initial_bootstrap'
                        FOR UPDATE
                        """,
                        (recalculation_id,),
                    )
                    run = _row(cursor)
                    if run is None:
                        raise BootstrapError("initial Bootstrap run does not exist")
                    cursor.execute(
                        """
                        SELECT count(*)::int AS total_shards,
                               COALESCE(sum(processed_rows),0)::bigint AS processed_events,
                               count(*) FILTER (WHERE status='succeeded')::int AS succeeded_shards,
                               count(*) FILTER (WHERE status<>'succeeded')::int AS failed_shards
                        FROM feature_clock.recalculation_shards
                        WHERE recalculation_id=%s
                        """,
                        (recalculation_id,),
                    )
                    totals = _row(cursor)
                    if totals is None or int(totals["total_shards"]) == 0:
                        raise BootstrapError("initial Bootstrap run has no shards")
                    succeeded_shards = int(totals["succeeded_shards"])
                    failed_shards = int(totals["failed_shards"])
                    processed_events = int(totals["processed_events"])
                    if run["status"] == "cancelled":
                        status = "cancelled"
                        processed_channels, failed_channels = self._receipt_counts(cursor, bundle)
                    elif run["status"] == "succeeded":
                        status = "succeeded"
                        processed_channels, failed_channels = self._verify_coverage(cursor, bundle)
                    elif failed_shards:
                        processed_channels, failed_channels = self._receipt_counts(cursor, bundle)
                        status = "partial" if processed_events or processed_channels else "failed"
                        cursor.execute(
                            """
                            UPDATE feature_clock.recalculation_runs
                            SET status=%s,processed_channels=%s,failed_channels=%s,
                                completed_at=now(),updated_at=now()
                            WHERE recalculation_id=%s AND status<>'cancelled'
                            """,
                            (
                                status,
                                processed_channels,
                                failed_channels,
                                recalculation_id,
                            ),
                        )
                    else:
                        processed_channels, failed_channels = self._verify_coverage(cursor, bundle)
                        if processed_events != bundle.manifest.event_count:
                            raise BootstrapError(
                                "Bootstrap processed event count does not match Manifest"
                            )
                        status = "succeeded"
                        cursor.execute(
                            """
                            UPDATE feature_clock.recalculation_runs
                            SET status='succeeded',processed_channels=%s,failed_channels=0,
                                completed_at=now(),updated_at=now()
                            WHERE recalculation_id=%s AND status<>'cancelled'
                            """,
                            (processed_channels, recalculation_id),
                        )
                    return BootstrapResult(
                        recalculation_id=recalculation_id,
                        baseline_version=bundle.manifest.baseline_version,
                        policy_version=str(run["policy_version"]),
                        status=status,
                        processed_events=processed_events,
                        processed_channels=processed_channels,
                        failed_channels=failed_channels,
                        succeeded_shards=succeeded_shards,
                        failed_shards=failed_shards,
                    )
        except Exception:
            self.fail(recalculation_id)
            raise
        finally:
            connection.close()

    @staticmethod
    def _receipt_counts(cursor: Any, bundle: BaselineBundle) -> tuple[int, int]:
        cursor.execute(
            """
            SELECT count(*)::int AS processed
            FROM feature_clock.bootstrap_channel_receipts
            WHERE baseline_version=%s
            """,
            (bundle.manifest.baseline_version,),
        )
        row = _row(cursor)
        processed = int(row["processed"] if row is not None else 0)
        return processed, max(0, bundle.manifest.channel_count - processed)

    @staticmethod
    def _verify_coverage(cursor: Any, bundle: BaselineBundle) -> tuple[int, int]:
        cursor.execute(
            """
            SELECT
              (SELECT count(*) FROM feature_clock.channel_feature_state)::bigint AS feature_count,
              (SELECT count(*) FROM feature_clock.channel_clock_state)::bigint AS channel_clock_count,
              (SELECT count(*) FROM feature_clock.bootstrap_channel_receipts
               WHERE baseline_version=%s)::bigint AS receipt_count,
              (SELECT count(*) FROM feature_clock.crawler_event_inbox
               WHERE status='waiting_gap')::bigint AS waiting_gap_count
            """,
            (bundle.manifest.baseline_version,),
        )
        row = _row(cursor)
        if row is None:
            raise BootstrapError("Bootstrap coverage query returned no result")
        expected = bundle.manifest.channel_count
        counts = {
            "Feature": int(row["feature_count"]),
            "Channel Clock": int(row["channel_clock_count"]),
            "Bootstrap receipt": int(row["receipt_count"]),
        }
        mismatches = [f"{name}={count}" for name, count in counts.items() if count != expected]
        if mismatches:
            raise BootstrapError(
                f"Bootstrap coverage must equal Manifest channel_count={expected}: "
                + ", ".join(mismatches)
            )
        if int(row["waiting_gap_count"]):
            raise BootstrapError("Bootstrap completed with waiting Sequence Gaps")
        return expected, 0
