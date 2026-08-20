from __future__ import annotations

from collections import Counter, defaultdict
from statistics import median
from typing import Any, Iterable

from .evaluation import FIELDS, agent_values
from .hashing import sha256_json


_CONTENT_LINEAGE_FIELDS = {
    "content_type": "content_type_source",
    "description": "description_source",
    "published_at": "published_at_source",
    "duration_seconds": "duration_source",
    "view_count": "view_count_source",
    "like_count": "like_count_source",
    "comment_count": "comment_count_source",
}


def _top(counter: Counter[Any], limit: int = 20) -> list[dict[str, Any]]:
    return [
        {"value": value, "count": count}
        for value, count in counter.most_common(limit)
    ]


def build_snapshot_audit(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    field_present: Counter[str] = Counter()
    replay_quality: Counter[str] = Counter()
    content_counts: list[int] = []
    content_coverage: Counter[str] = Counter()
    scalar_frequencies: defaultdict[str, Counter[Any]] = defaultdict(Counter)
    structure_fingerprints: defaultdict[str, Counter[str]] = defaultdict(Counter)
    profile_fingerprints: Counter[str] = Counter()
    input_content_ids_retained: Counter[int] = Counter()
    models: Counter[str] = Counter()
    lineage_counts: Counter[str] = Counter()
    lineage_sources: defaultdict[str, Counter[str]] = defaultdict(Counter)

    for envelope in rows:
        counts["rows"] += 1
        snapshot = envelope.get("snapshot") if isinstance(envelope.get("snapshot"), dict) else envelope
        contents = snapshot.get("contents") if isinstance(snapshot, dict) else []
        channel = snapshot.get("channel") if isinstance(snapshot, dict) else {}
        contents = contents if isinstance(contents, list) else []
        channel = channel if isinstance(channel, dict) else {}
        lineage_counts["snapshot_rows"] += 1
        lineage_counts["content_rows"] += len(contents)
        provenance = snapshot.get("provenance") if isinstance(snapshot, dict) else {}
        provenance = provenance if isinstance(provenance, dict) else {}
        if provenance.get("data_lineage_version"):
            lineage_counts["snapshot_rows_with_lineage_version"] += 1
        if channel.get("channel_extractor"):
            lineage_counts["channel_rows_with_extractor"] += 1
            lineage_sources["channel_extractor"][str(channel["channel_extractor"])] += 1
        if channel.get("country") or channel.get("country_canonical_name") or channel.get("country_code"):
            lineage_counts["channel_country_values"] += 1
            if channel.get("country_source"):
                lineage_counts["channel_country_values_with_source"] += 1
                lineage_sources["country"][str(channel["country_source"])] += 1
        for item in contents:
            if not isinstance(item, dict):
                continue
            for value_field, source_field in _CONTENT_LINEAGE_FIELDS.items():
                value_present = item.get(value_field) is not None
                if value_present:
                    lineage_counts[f"content_{value_field}_values"] += 1
                    if item.get(source_field):
                        lineage_counts[f"content_{value_field}_values_with_source"] += 1
                        lineage_sources[value_field][str(item[source_field])] += 1
            if item.get("extractor_version"):
                lineage_counts["content_rows_with_extractor_version"] += 1
                lineage_sources["extractor_version"][str(item["extractor_version"])] += 1
            if isinstance(item.get("comments_first_page"), dict):
                lineage_counts["comment_page_rows"] += 1
                if provenance.get("comment_page_source_status") == "persisted":
                    lineage_counts["comment_page_rows_with_source"] += 1
        replay_quality[str(snapshot.get("replay_quality") or "unknown")] += 1
        content_counts.append(len(contents))
        if channel.get("country") or channel.get("country_canonical_name") or channel.get("country_code"):
            content_coverage["explicit_country"] += 1
        if channel.get("about_description") or channel.get("summary"):
            content_coverage["channel_description"] += 1
        if contents:
            content_coverage["any_content"] += 1
        if any(item.get("description") for item in contents if isinstance(item, dict)):
            content_coverage["content_description"] += 1
        if any(item.get("view_count") is not None for item in contents if isinstance(item, dict)):
            content_coverage["content_views"] += 1
        if any(
            item.get("like_count") is not None or item.get("comment_count") is not None
            for item in contents if isinstance(item, dict)
        ):
            content_coverage["content_engagement"] += 1

        retained = int((provenance or {}).get("input_content_ids_retained") or 0)
        input_content_ids_retained[retained] += 1

        reference = envelope.get("agent_reference")
        if not isinstance(reference, dict):
            counts["without_agent_reference"] += 1
            continue
        counts["with_agent_reference"] += 1
        models[str(reference.get("agent_model") or "unknown")] += 1
        values = agent_values(reference)
        complete = True
        for field in FIELDS:
            value = values.get(field)
            if value is not None:
                field_present[field] += 1
            else:
                complete = False
            if field in {"country", "creator_gender", "creator_age_range", "creator_language", "active_subscriber_ratio"}:
                scalar_frequencies[field][str(value)] += 1
            if field in {"audience_region", "audience_age_gender", "audience_language", "channel_tags", "channel_categories"}:
                structure_fingerprints[field][sha256_json(value)] += 1
        if complete:
            counts["complete_agent_profiles"] += 1
        profile_fingerprints[sha256_json(values)] += 1

    row_count = counts["rows"]
    repeated_structures: dict[str, Any] = {}
    suspicion_floor = max(10, round(row_count * 0.005))
    for field, fingerprints in structure_fingerprints.items():
        repeated = [count for count in fingerprints.values() if count >= suspicion_floor]
        repeated_structures[field] = {
            "unique_fingerprints": len(fingerprints),
            "largest_repeat": max(fingerprints.values(), default=0),
            "channels_in_fingerprints_repeated_at_least_threshold": sum(repeated),
            "review_threshold": suspicion_floor,
        }

    sorted_content = sorted(content_counts)
    return {
        "report_type": "independent_snapshot_and_agent_reference_audit",
        "summary": dict(counts),
        "agent_field_coverage": {field: field_present[field] for field in FIELDS},
        "snapshot_quality": dict(replay_quality),
        "snapshot_signal_coverage": dict(content_coverage),
        "content_count": {
            "minimum": min(sorted_content, default=0),
            "median": median(sorted_content) if sorted_content else 0,
            "maximum": max(sorted_content, default=0),
        },
        "input_content_ids_retained": {str(key): value for key, value in sorted(input_content_ids_retained.items())},
        "agent_models": dict(models),
        "snapshot_lineage_coverage": dict(lineage_counts),
        "snapshot_lineage_sources": {
            field: dict(values)
            for field, values in sorted(lineage_sources.items())
        },
        "scalar_frequency_top": {field: _top(values) for field, values in scalar_frequencies.items()},
        "repeated_structure_audit": repeated_structures,
        "whole_profile_templates": {
            "unique_fingerprints": len(profile_fingerprints),
            "largest_repeat": max(profile_fingerprints.values(), default=0),
            "top_repeat_counts": [count for _, count in profile_fingerprints.most_common(20)],
        },
        "interpretation": [
            "Agent coverage and Agent agreement are not ground-truth accuracy.",
            "Repeated fingerprints identify review candidates; they do not by themselves prove a hard-coded template.",
            "Empty retained input_content_ids limits strict historical replay but does not remove Agent output data.",
            "Lineage coverage counts source presence; it does not by itself approve a derived use.",
        ],
    }
