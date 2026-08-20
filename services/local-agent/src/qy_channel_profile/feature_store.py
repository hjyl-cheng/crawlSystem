from __future__ import annotations

import json
import multiprocessing
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from pydantic import BaseModel, ConfigDict, Field

from .analyzers import (
    analyze_age,
    analyze_categories,
    analyze_country,
    analyze_gender,
    analyze_language,
    analyze_tags,
    controlled_tag_names,
)
from .contracts import AnalysisPolicy, ChannelSnapshot
from .errors import FeatureStoreError
from .features import FEATURE_SCHEMA_VERSION, NUMERIC_FEATURE_NAMES, build_channel_features
from .io import read_jsonl, write_json
from .language_id import FASTTEXT_LANGUAGE_VERSION, FastTextLanguageIdentifier, file_sha256
from .priors import PriorCatalog
from .taxonomy import TAXONOMY_VERSION


FEATURE_STORE_SCHEMA_VERSION = "qy-channel-profile-feature-store-v4-field-lineage"


class FeatureStoreManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = FEATURE_STORE_SCHEMA_VERSION
    feature_schema_version: str = FEATURE_SCHEMA_VERSION
    taxonomy_version: str = TAXONOMY_VERSION
    output_label_language: str = "English"
    created_at: datetime
    source_path: str
    source_sha256: str
    row_count: int = Field(ge=0)
    output_path: str
    output_sha256: str
    language_model_version: str
    language_model_sha256: str
    prior_catalog_version: str
    prior_catalog_hash: str
    worker_count: int = Field(ge=1)
    label_policy: str
    label_coverage: dict[str, int]
    replay_quality: dict[str, int]
    content_coverage: dict[str, int]
    comment_coverage: dict[str, int]
    lineage_status: str
    source_rows_with_lineage_version: int = Field(ge=0)
    agent_reference_columns_present: bool = False


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _snapshot_from_envelope(envelope: dict[str, Any]) -> ChannelSnapshot:
    value = envelope.get("snapshot") if isinstance(envelope.get("snapshot"), dict) else envelope
    return ChannelSnapshot.from_mapping(value)


def _feature_record(
    envelope: dict[str, Any],
    detector: FastTextLanguageIdentifier,
    catalog: PriorCatalog,
) -> dict[str, Any]:
    snapshot = _snapshot_from_envelope(envelope)
    features = build_channel_features(snapshot, detector)
    language = analyze_language(features.language, AnalysisPolicy.COMPLETE_ESTIMATE)
    categories, _ = analyze_categories(
        snapshot,
        AnalysisPolicy.COMPLETE_ESTIMATE,
        channel_text=features.channel_text,
    )
    tags = analyze_tags(
        snapshot,
        categories,
        language,
        AnalysisPolicy.COMPLETE_ESTIMATE,
        full_text=features.full_text,
        comments=features.comments,
    )
    gender = analyze_gender(
        snapshot,
        AnalysisPolicy.EVIDENCE_FIRST,
        comments=features.comments,
        identity=features.creator_evidence,
    )
    category_name = (
        categories.value.get("level_1", "default")
        if isinstance(categories.value, dict)
        else "default"
    )
    age = analyze_age(
        snapshot,
        category_name,
        catalog,
        AnalysisPolicy.EVIDENCE_FIRST,
        identity=features.creator_evidence,
    )
    country = analyze_country(
        snapshot,
        features.language.probabilities,
        catalog,
        AnalysisPolicy.EVIDENCE_FIRST,
        channel_text=features.channel_text,
    )

    category_label = (
        categories.value
        if categories.value and categories.evidence_strength in {"explicit", "strong"}
        else None
    )
    controlled = set(controlled_tag_names())
    supported_tag_values = tags.metadata.get("supported_tags", [])
    tag_labels = [value for value in supported_tag_values if value in controlled]
    country_label = country.value if country.source_type == "observed" else None
    gender_label = (
        gender.value
        if gender.value and gender.evidence_strength in {"explicit", "strong"}
        else None
    )
    age_label = (
        age.value
        if age.value is not None and age.evidence_strength in {"explicit", "strong"}
        else None
    )
    base = features.to_record()
    record = {
        "channel_id": base["channel_id"],
        "as_of": base["as_of"],
        "feature_schema_version": base["feature_schema_version"],
        "replay_quality": snapshot.replay_quality,
        "source_lineage_version": str(snapshot.provenance.get("data_lineage_version") or ""),
        "profile_text_hash": base["profile_text_hash"],
        "content_text_hash": base["content_text_hash"],
        "channel_stats_hash": base["channel_stats_hash"],
        "content_stats_hash": base["content_stats_hash"],
        "comment_text_hash": base["comment_text_hash"],
        "comment_stats_hash": base["comment_stats_hash"],
        "media_asset_hash": base["media_asset_hash"],
        "lineage_hash": base["lineage_hash"],
        "snapshot_hash": base["snapshot_hash"],
        "channel_text": base["channel_text"],
        "content_text": base["content_text"],
        "full_text": base["full_text"],
        "identity_text": features.model_text("creator_gender"),
        "category_text": features.model_text("channel_categories"),
        "tag_text": features.model_text("channel_tags"),
        "comment_feature_schema_version": base["comment_feature_schema_version"],
        "comment_topic_text": base["comment_topic_text"],
        "comment_creator_address_text": base["comment_creator_address_text"],
        "comment_language_probabilities_json": _json(base["comment_language_probabilities"]),
        "comment_region_probabilities_json": _json(base["comment_region_probabilities"]),
        "comment_age_probabilities_json": _json(base["comment_age_probabilities"]),
        "comment_gender_probabilities_json": _json(base["comment_gender_probabilities"]),
        "comment_age_gender_probabilities_json": _json(base["comment_age_gender_probabilities"]),
        "comment_creator_gender_probabilities_json": _json(base["comment_creator_gender_probabilities"]),
        "comment_diagnostics_json": _json(base["comment_diagnostics"]),
        "language_probabilities_json": _json(base["language_probabilities"]),
        "language_label": language.value,
        "language_confidence": language.model_confidence,
        "country_label": country_label,
        "country_label_source": "crawler_explicit" if country_label else None,
        "gender_label": gender_label,
        "gender_label_source": gender.evidence_strength if gender_label else None,
        "age_label": age_label,
        "age_label_source": age.evidence_strength if age_label is not None else None,
        "category_level1_label": category_label.get("level_1") if category_label else None,
        "category_level2_labels_json": _json(category_label.get("level_2", [])) if category_label else "[]",
        "category_label_source": "high_precision_rule" if category_label else None,
        "tag_labels_json": _json(tag_labels),
        "tag_label_source": "high_precision_rule" if tag_labels else None,
        "missing_json": _json(base["missing"]),
        **base["numeric"],
    }
    if any("agent" in key.casefold() for key in record):
        raise FeatureStoreError("Agent reference leaked into independent feature record")
    return record


def iter_feature_records(
    envelopes: Iterable[dict[str, Any]],
    detector: FastTextLanguageIdentifier,
    catalog: PriorCatalog,
) -> Iterable[dict[str, Any]]:
    for envelope in envelopes:
        yield _feature_record(envelope, detector, catalog)


_WORKER_DETECTOR: FastTextLanguageIdentifier | None = None
_WORKER_CATALOG: PriorCatalog | None = None


def _initialize_feature_worker(
    language_model_path: str,
    prior_catalog_path: str | None,
) -> None:
    global _WORKER_DETECTOR, _WORKER_CATALOG
    _WORKER_DETECTOR = FastTextLanguageIdentifier(language_model_path)
    _WORKER_CATALOG = PriorCatalog.load(prior_catalog_path)


def _parallel_feature_record(envelope: dict[str, Any]) -> dict[str, Any]:
    if _WORKER_DETECTOR is None or _WORKER_CATALOG is None:
        raise FeatureStoreError("parallel feature worker was not initialized")
    return _feature_record(envelope, _WORKER_DETECTOR, _WORKER_CATALOG)


def _parallel_feature_records(
    envelopes: Iterable[dict[str, Any]],
    *,
    workers: int,
    language_model_path: Path,
    prior_catalog_path: str | Path | None,
) -> Iterable[dict[str, Any]]:
    context = multiprocessing.get_context("spawn")
    normalized_prior = str(Path(prior_catalog_path).expanduser().resolve()) if prior_catalog_path else None
    with context.Pool(
        processes=workers,
        initializer=_initialize_feature_worker,
        initargs=(str(language_model_path), normalized_prior),
    ) as pool:
        yield from pool.imap(_parallel_feature_record, envelopes, chunksize=2)


def build_feature_store(
    input_path: str | Path,
    output_path: str | Path,
    *,
    language_model_path: str | Path,
    prior_catalog_path: str | Path | None = None,
    workers: int = 1,
) -> FeatureStoreManifest:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:
        raise FeatureStoreError("polars and pyarrow are required to build the feature store") from error

    source = Path(input_path).resolve()
    output = Path(output_path).resolve()
    if not source.is_file():
        raise FeatureStoreError(f"snapshot JSONL not found: {source}")
    if not 1 <= workers <= 32:
        raise FeatureStoreError("feature workers must be in [1, 32]")
    language_model = Path(language_model_path).expanduser().resolve()
    if not language_model.is_file():
        raise FeatureStoreError(f"language model not found: {language_model}")
    catalog = PriorCatalog.load(prior_catalog_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    label_columns = (
        "country_label",
        "gender_label",
        "age_label",
        "category_level1_label",
        "tag_label_source",
    )
    label_coverage = {
        name.removesuffix("_label").removesuffix("_source"): 0
        for name in label_columns
    }
    replay_quality: Counter[str] = Counter()
    content_coverage = {
        "with_content_text": 0,
        "with_view_stats": 0,
        "with_engagement": 0,
    }
    comment_coverage = {
        "with_comment_pages": 0,
        "with_sampled_comments": 0,
        "with_comment_language": 0,
        "with_comment_region": 0,
        "with_comment_life_stage": 0,
    }
    string_columns = (
        "channel_id", "as_of", "feature_schema_version", "replay_quality",
        "source_lineage_version",
        "profile_text_hash", "content_text_hash", "channel_stats_hash",
        "content_stats_hash", "comment_text_hash", "comment_stats_hash",
        "media_asset_hash", "lineage_hash", "snapshot_hash",
        "channel_text", "content_text", "full_text", "identity_text", "category_text", "tag_text",
        "comment_feature_schema_version", "comment_topic_text",
        "comment_creator_address_text", "comment_language_probabilities_json",
        "comment_region_probabilities_json", "comment_age_probabilities_json",
        "comment_gender_probabilities_json", "comment_age_gender_probabilities_json",
        "comment_creator_gender_probabilities_json", "comment_diagnostics_json",
        "language_probabilities_json",
        "language_label", "country_label", "country_label_source", "gender_label",
        "gender_label_source", "age_label_source", "category_level1_label",
        "category_level2_labels_json", "category_label_source", "tag_labels_json",
        "tag_label_source", "missing_json",
    )
    schema = pa.schema([
        *(pa.field(name, pa.string()) for name in string_columns),
        pa.field("language_confidence", pa.float64()),
        pa.field("age_label", pa.int64()),
        *(pa.field(name, pa.float64()) for name in NUMERIC_FEATURE_NAMES),
    ])
    temporary = output.with_suffix(output.suffix + ".tmp")
    writer: Any = None
    batch: list[dict[str, Any]] = []
    row_count = 0
    source_rows_with_lineage_version = 0
    if workers == 1:
        detector = FastTextLanguageIdentifier(language_model)
        feature_rows = iter_feature_records(read_jsonl(source), detector, catalog)
        language_model_version = detector.version
        language_model_sha256 = detector.sha256
    else:
        feature_rows = _parallel_feature_records(
            read_jsonl(source),
            workers=workers,
            language_model_path=language_model,
            prior_catalog_path=prior_catalog_path,
        )
        language_model_version = FASTTEXT_LANGUAGE_VERSION
        language_model_sha256 = file_sha256(language_model)
    try:
        for row in feature_rows:
            row_count += 1
            source_rows_with_lineage_version += int(
                row["source_lineage_version"] == "snapshot-field-lineage-v1"
            )
            replay_quality[str(row["replay_quality"])] += 1
            for name in label_columns:
                if row.get(name) is not None:
                    key = name.removesuffix("_label").removesuffix("_source")
                    label_coverage[key] += 1
            content_coverage["with_content_text"] += int(bool(row["content_text"]))
            content_coverage["with_view_stats"] += int(not bool(row["view_stats_missing"]))
            content_coverage["with_engagement"] += int(not bool(row["engagement_missing"]))
            comment_coverage["with_comment_pages"] += int(row["comment_page_count"] > 0)
            comment_coverage["with_sampled_comments"] += int(row["comment_sample_count"] > 0)
            comment_coverage["with_comment_language"] += int(
                row["comment_language_probabilities_json"] != "{}"
            )
            comment_coverage["with_comment_region"] += int(
                row["comment_region_probabilities_json"] != "{}"
            )
            comment_coverage["with_comment_life_stage"] += int(
                row["comment_life_stage_author_count"] > 0
            )
            batch.append(row)
            if len(batch) >= 500:
                table = pa.Table.from_pylist(batch, schema=schema)
                writer = writer or pq.ParquetWriter(temporary, schema, compression="zstd")
                writer.write_table(table)
                batch.clear()
        if batch:
            table = pa.Table.from_pylist(batch, schema=schema)
            writer = writer or pq.ParquetWriter(temporary, schema, compression="zstd")
            writer.write_table(table)
        if writer is None:
            raise FeatureStoreError("cannot build an empty feature store")
    finally:
        if writer is not None:
            writer.close()
    temporary.replace(output)
    manifest = FeatureStoreManifest(
        created_at=datetime.now(timezone.utc),
        source_path=str(source),
        source_sha256=file_sha256(source),
        row_count=row_count,
        output_path=str(output),
        output_sha256=file_sha256(output),
        language_model_version=language_model_version,
        language_model_sha256=language_model_sha256,
        prior_catalog_version=catalog.version,
        prior_catalog_hash=catalog.content_hash,
        worker_count=workers,
        label_policy="explicit_facts_and_per_label_high_precision_rules_only_no_agent_labels-v2",
        label_coverage=label_coverage,
        replay_quality=dict(replay_quality),
        content_coverage=content_coverage,
        comment_coverage=comment_coverage,
        lineage_status=(
            "complete"
            if source_rows_with_lineage_version == row_count
            else "legacy_snapshot_partial"
        ),
        source_rows_with_lineage_version=source_rows_with_lineage_version,
        agent_reference_columns_present=False,
    )
    write_json(output.with_suffix(output.suffix + ".manifest.json"), manifest.model_dump(mode="json"))
    return manifest
