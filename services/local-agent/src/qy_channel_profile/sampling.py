from __future__ import annotations

import hashlib
import heapq
import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any

from .errors import SnapshotError
from .language_id import file_sha256
from .io import write_json, write_jsonl


SAMPLE_SCHEMA_VERSION = "qy-channel-profile-holdout-v1"
ANNOTATION_PILOT_SCHEMA_VERSION = "qy-channel-profile-annotation-pilot-v1"
_FORBIDDEN_ANNOTATION_KEYS = frozenset({
    "agent_reference",
    "agent_model",
    "agent_version_hash",
    "result",
    "facts",
    "prediction",
    "predictions",
    "label",
    "labels",
    "category_level1_label",
    "category_level2_labels_json",
    "tag_labels_json",
})
_BOUNDARY_TERMS = re.compile(
    r"\b(?:vlog|daily life|rotina|screen recording|screencast|photograph\w*|"
    r"self[- ]?improvement|psycholog\w*|career|productiv\w*|education|tutorial|"
    r"software|app\w*|artificial intelligence|\bai\b|tech\w*|device\w*|gadget\w*)\b",
    re.IGNORECASE,
)


def _channel_id(envelope: dict[str, Any]) -> str:
    snapshot = envelope.get("snapshot") if isinstance(envelope.get("snapshot"), dict) else envelope
    channel = snapshot.get("channel") if isinstance(snapshot, dict) else None
    channel_id = str((channel or {}).get("channel_id") or "").strip()
    if not channel_id:
        raise SnapshotError("holdout sampling requires snapshot.channel.channel_id")
    return channel_id


def _snapshot_channel(envelope: dict[str, Any]) -> dict[str, Any]:
    snapshot = envelope.get("snapshot") if isinstance(envelope.get("snapshot"), dict) else envelope
    channel = snapshot.get("channel") if isinstance(snapshot, dict) else None
    return channel if isinstance(channel, dict) else {}


def _country_coverage_key(envelope: dict[str, Any]) -> str:
    channel = _snapshot_channel(envelope)
    for field in ("country_canonical_name", "country", "country_code"):
        value = str(channel.get(field) or "").strip()
        if value:
            return value
    return "Unspecified"


def _selection_rank(seed: str, channel_id: str, duplicate_index: int) -> int:
    return int.from_bytes(
        hashlib.sha256(f"{seed}\0{channel_id}\0{duplicate_index}".encode("utf-8")).digest(),
        "big",
    )


def build_deterministic_holdout(
    input_path: str | Path,
    output_path: str | Path,
    *,
    sample_size: int,
    seed: str = "qy-agent-free-holdout-v1",
) -> dict[str, Any]:
    """Select a stable bottom-k hash sample without consulting Agent values."""

    if sample_size <= 0:
        raise ValueError("sample_size must be positive")
    source = Path(input_path).expanduser().resolve()
    output = Path(output_path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"snapshot JSONL not found: {source}")

    # Heap root is the currently largest selected rank. Rows are retained only
    # for the requested holdout, so memory is O(sample_size), not O(source).
    selected: list[tuple[int, int, int, dict[str, Any]]] = []
    occurrence: dict[str, int] = {}
    source_rows = 0
    with source.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            text = line.strip()
            if not text:
                continue
            try:
                envelope = json.loads(text)
            except json.JSONDecodeError as error:
                raise ValueError(f"invalid JSONL at {source}:{line_number}: {error}") from error
            if not isinstance(envelope, dict):
                raise ValueError(f"JSONL value at {source}:{line_number} must be an object")
            source_rows += 1
            channel_id = _channel_id(envelope)
            duplicate_index = occurrence.get(channel_id, 0)
            occurrence[channel_id] = duplicate_index + 1
            rank = _selection_rank(seed, channel_id, duplicate_index)
            entry = (-rank, -source_rows, source_rows, envelope)
            if len(selected) < sample_size:
                heapq.heappush(selected, entry)
            elif entry > selected[0]:
                heapq.heapreplace(selected, entry)

    if source_rows < sample_size:
        raise ValueError(f"requested {sample_size} rows but source contains only {source_rows}")
    rows = [entry[3] for entry in sorted(selected, key=lambda item: item[2])]
    selected_ids = [_channel_id(row) for row in rows]
    write_jsonl(output, rows)
    manifest = {
        "schema_version": SAMPLE_SCHEMA_VERSION,
        "selection_method": "sha256_bottom_k_channel_id_occurrence",
        "selection_uses_agent_values": False,
        "seed": seed,
        "source_path": str(source),
        "source_sha256": file_sha256(source),
        "source_rows": source_rows,
        "sample_size": len(rows),
        "unique_channel_ids": len(set(selected_ids)),
        "selected_channel_ids_sha256": "sha256:" + hashlib.sha256(
            "\n".join(selected_ids).encode("utf-8")
        ).hexdigest(),
        "output_path": str(output),
        "output_sha256": file_sha256(output),
    }
    write_json(output.with_suffix(output.suffix + ".manifest.json"), manifest)
    return manifest


def build_deterministic_country_coverage_sample(
    input_path: str | Path,
    output_path: str | Path,
    *,
    sample_size: int,
    seed: str = "qy-agent-free-country-coverage-v1",
) -> dict[str, Any]:
    """Select stable country representatives, then fill by global hash rank.

    Only crawler snapshot fields and channel IDs participate. Historical Agent
    output remains opaque to selection. The result is useful for qualitative
    boundary review and deliberately is not population-representative.
    """

    if sample_size <= 0:
        raise ValueError("sample_size must be positive")
    source = Path(input_path).expanduser().resolve()
    output = Path(output_path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"snapshot JSONL not found: {source}")

    group_best: dict[str, tuple[int, int, dict[str, Any]]] = {}
    global_best: list[tuple[int, int, int, dict[str, Any]]] = []
    group_counts: dict[str, int] = {}
    occurrence: dict[str, int] = {}
    source_rows = 0
    with source.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            text = line.strip()
            if not text:
                continue
            try:
                envelope = json.loads(text)
            except json.JSONDecodeError as error:
                raise ValueError(f"invalid JSONL at {source}:{line_number}: {error}") from error
            if not isinstance(envelope, dict):
                raise ValueError(f"JSONL value at {source}:{line_number} must be an object")
            source_rows += 1
            channel_id = _channel_id(envelope)
            duplicate_index = occurrence.get(channel_id, 0)
            occurrence[channel_id] = duplicate_index + 1
            rank = _selection_rank(seed, channel_id, duplicate_index)
            group = _country_coverage_key(envelope)
            group_counts[group] = group_counts.get(group, 0) + 1
            current = group_best.get(group)
            entry = (rank, source_rows, envelope)
            if current is None or entry[:2] < current[:2]:
                group_best[group] = entry

            heap_entry = (-rank, -source_rows, source_rows, envelope)
            if len(global_best) < sample_size:
                heapq.heappush(global_best, heap_entry)
            elif heap_entry > global_best[0]:
                heapq.heapreplace(global_best, heap_entry)

    if source_rows < sample_size:
        raise ValueError(f"requested {sample_size} rows but source contains only {source_rows}")

    representatives = sorted(
        group_best.items(),
        key=lambda item: (
            _selection_rank(seed + "\0coverage-group", item[0], 0),
            item[0],
        ),
    )[:sample_size]
    selected = [entry for _, entry in representatives]
    selected_ids = {_channel_id(entry[2]) for entry in selected}
    for negative_rank, _, source_position, envelope in sorted(
        global_best,
        key=lambda item: (-item[0], item[2]),
    ):
        del negative_rank
        channel_id = _channel_id(envelope)
        if channel_id in selected_ids:
            continue
        selected.append((0, source_position, envelope))
        selected_ids.add(channel_id)
        if len(selected) == sample_size:
            break
    if len(selected) != sample_size:
        raise ValueError("country coverage sample could not be filled to requested size")

    selected.sort(key=lambda entry: entry[1])
    rows = [entry[2] for entry in selected]
    ordered_ids = [_channel_id(row) for row in rows]
    selected_groups = sorted({_country_coverage_key(row) for row in rows})
    write_jsonl(output, rows)
    manifest = {
        "schema_version": SAMPLE_SCHEMA_VERSION,
        "selection_method": "country_coverage_then_sha256_bottom_k_channel_id_occurrence",
        "selection_uses_agent_values": False,
        "population_representative": False,
        "coverage_field": "snapshot.channel.country",
        "seed": seed,
        "source_path": str(source),
        "source_sha256": file_sha256(source),
        "source_rows": source_rows,
        "sample_size": len(rows),
        "unique_channel_ids": len(set(ordered_ids)),
        "coverage_groups_available": len(group_counts),
        "coverage_groups_selected": selected_groups,
        "coverage_group_counts": dict(sorted(group_counts.items())),
        "selected_channel_ids_sha256": "sha256:" + hashlib.sha256(
            "\n".join(ordered_ids).encode("utf-8")
        ).hexdigest(),
        "output_path": str(output),
        "output_sha256": file_sha256(output),
    }
    write_json(output.with_suffix(output.suffix + ".manifest.json"), manifest)
    return manifest


def _snapshot_only(envelope: dict[str, Any]) -> dict[str, Any]:
    snapshot = envelope.get("snapshot") if isinstance(envelope.get("snapshot"), dict) else envelope
    if not isinstance(snapshot, dict):
        raise SnapshotError("annotation sampling requires an object snapshot")
    clean = deepcopy(snapshot)
    channel = clean.get("channel")
    if isinstance(channel, dict):
        channel.pop("avatar_url", None)
    for content in clean.get("contents") or []:
        if not isinstance(content, dict):
            continue
        content.pop("thumbnail_url", None)
        page = content.get("comments_first_page")
        if not isinstance(page, dict):
            continue
        for comment in page.get("comments") or []:
            if isinstance(comment, dict):
                comment.pop("author_avatar_url", None)
    clean_provenance = clean.get("provenance")
    if isinstance(clean_provenance, dict):
        clean["provenance"] = {
            key: value
            for key, value in clean_provenance.items()
            if "agent" not in key.casefold()
        }
    return clean


def _assert_annotation_blind(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).casefold()
            if normalized in _FORBIDDEN_ANNOTATION_KEYS or normalized.startswith("agent_"):
                raise SnapshotError(f"annotation pilot contains a forbidden key at {path}.{key}")
            _assert_annotation_blind(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_annotation_blind(child, f"{path}[{index}]")


def _pilot_challenge_flags(snapshot: dict[str, Any]) -> tuple[str, ...]:
    channel = snapshot.get("channel") if isinstance(snapshot.get("channel"), dict) else {}
    contents = [item for item in (snapshot.get("contents") or []) if isinstance(item, dict)]
    texts = [
        channel.get("title"), channel.get("summary"), channel.get("about_description"),
        " ".join(str(value) for value in (channel.get("keywords") or [])),
    ]
    texts.extend(
        " ".join((
            str(item.get("title") or ""),
            str(item.get("description") or ""),
            " ".join(str(value) for value in (item.get("keywords") or [])),
            " ".join(str(value) for value in (item.get("hashtags") or [])),
        ))
        for item in contents
    )
    text = " ".join(str(value or "") for value in texts)
    flags: list[str] = []
    if len(contents) <= 5 or len(text.strip()) < 500:
        flags.append("sparse_evidence")
    content_types = {str(item.get("content_type") or "") for item in contents}
    if len(content_types.intersection({"video", "short", "live"})) >= 2:
        flags.append("mixed_formats")
    if contents and sum(item.get("content_type") == "short" for item in contents) / len(contents) >= 0.7:
        flags.append("shorts_heavy")
    if any(item.get("content_type") == "live" for item in contents):
        flags.append("live_present")
    if any(isinstance(item.get("comments_first_page"), dict) for item in contents):
        flags.append("comments_present")
    scripts = {
        "latin" if ord(character) < 0x0250 else
        "cyrillic" if 0x0400 <= ord(character) <= 0x04FF else
        "arabic" if 0x0600 <= ord(character) <= 0x06FF else
        "indic" if 0x0900 <= ord(character) <= 0x097F else
        "cjk" if 0x3040 <= ord(character) <= 0x30FF or 0x4E00 <= ord(character) <= 0x9FFF else
        "other"
        for character in text
        if character.isalpha()
    }
    if len(scripts.difference({"other"})) >= 2:
        flags.append("mixed_scripts")
    if _BOUNDARY_TERMS.search(text):
        flags.append("taxonomy_boundary_terms")
    if not channel.get("country") and not channel.get("country_code"):
        flags.append("country_missing")
    return tuple(flags)


def _write_annotation_template(path: Path, blind_ids: list[str]) -> None:
    rows = [{
        "blind_id": blind_id,
        "taxonomy_version": "qy-taxonomy-v2-draft.1",
        "guideline_version": "qy-annotation-guideline-v0.1",
        "topics": [],
        "purposes_or_genres": [],
        "formats": [],
        "content_sources": [],
        "account_entity_type": "",
        "creator_gender": "",
        "evidence_refs": [],
        "insufficient_evidence": False,
        "mixed": False,
        "notes": "",
    } for blind_id in blind_ids]
    write_jsonl(path, rows)


def build_blind_annotation_pilot(
    input_path: str | Path,
    output_path: str | Path,
    *,
    sample_size: int = 200,
    challenge_fraction: float = 0.5,
    seed: str = "qy-taxonomy-v2-pilot-v1",
) -> dict[str, Any]:
    if sample_size < 2:
        raise ValueError("annotation pilot sample_size must be at least 2")
    if not 0.0 < challenge_fraction < 1.0:
        raise ValueError("challenge_fraction must be in (0, 1)")
    source = Path(input_path).expanduser().resolve()
    output = Path(output_path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"snapshot JSONL not found: {source}")

    candidates: list[dict[str, Any]] = []
    source_rows = 0
    source_rows_with_lineage_version = 0
    with source.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            text = line.strip()
            if not text:
                continue
            try:
                envelope = json.loads(text)
            except json.JSONDecodeError as error:
                raise ValueError(f"invalid JSONL at {source}:{line_number}: {error}") from error
            if not isinstance(envelope, dict):
                raise ValueError(f"JSONL value at {source}:{line_number} must be an object")
            source_rows += 1
            snapshot = _snapshot_only(envelope)
            provenance = snapshot.get("provenance")
            if isinstance(provenance, dict) and provenance.get("data_lineage_version"):
                source_rows_with_lineage_version += 1
            channel_id = _channel_id(snapshot)
            flags = _pilot_challenge_flags(snapshot)
            candidates.append({
                "channel_id": channel_id,
                "rank": _selection_rank(seed, channel_id, 0),
                "challenge_rank": _selection_rank(seed + "\0challenge", channel_id, 0),
                "mix_rank": _selection_rank(seed + "\0mix", channel_id, 0),
                "flags": flags,
                "snapshot": snapshot,
            })
    if len(candidates) < sample_size:
        raise ValueError(f"requested {sample_size} rows but source contains only {len(candidates)}")
    if len({row["channel_id"] for row in candidates}) != len(candidates):
        raise ValueError("annotation pilot source must contain unique channel IDs")

    challenge_size = round(sample_size * challenge_fraction)
    natural_size = sample_size - challenge_size
    natural = sorted(candidates, key=lambda row: (row["rank"], row["channel_id"]))[:natural_size]
    selected_ids = {row["channel_id"] for row in natural}
    challenge_pool = [row for row in candidates if row["flags"] and row["channel_id"] not in selected_ids]
    challenge = sorted(
        challenge_pool,
        key=lambda row: (-len(row["flags"]), row["challenge_rank"], row["channel_id"]),
    )[:challenge_size]
    if len(challenge) != challenge_size:
        raise ValueError("not enough challenge rows to fill the annotation pilot")
    selected = sorted((*natural, *challenge), key=lambda row: (row["mix_rank"], row["channel_id"]))

    rows: list[dict[str, Any]] = []
    challenge_flag_counts: dict[str, int] = {}
    for index, selected_row in enumerate(selected, 1):
        for flag in selected_row["flags"]:
            challenge_flag_counts[flag] = challenge_flag_counts.get(flag, 0) + 1
        row = {
            "blind_id": f"QYV2-P{index:04d}",
            "snapshot": selected_row["snapshot"],
        }
        _assert_annotation_blind(row)
        rows.append(row)
    write_jsonl(output, rows)
    blind_ids = [row["blind_id"] for row in rows]
    annotator_a = output.with_name(output.stem + ".annotator-a.template.jsonl")
    annotator_b = output.with_name(output.stem + ".annotator-b.template.jsonl")
    _write_annotation_template(annotator_a, blind_ids)
    _write_annotation_template(annotator_b, blind_ids)
    manifest = {
        "schema_version": ANNOTATION_PILOT_SCHEMA_VERSION,
        "dataset_role": "annotation_pilot_not_gold_test",
        "selection_method": "agent_blind_natural_plus_public_evidence_challenge",
        "selection_uses_agent_values": False,
        "selection_uses_local_predictions": False,
        "grok_reference_present_in_output": False,
        "seed": seed,
        "source_path": str(source),
        "source_sha256": file_sha256(source),
        "source_rows": source_rows,
        "source_rows_with_lineage_version": source_rows_with_lineage_version,
        "lineage_status": (
            "complete_version_marker"
            if source_rows_with_lineage_version == source_rows
            else "legacy_snapshot_partial"
        ),
        "excluded_visual_fields": [
            "snapshot.channel.avatar_url",
            "snapshot.contents[].thumbnail_url",
            "snapshot.contents[].comments_first_page.comments[].author_avatar_url",
        ],
        "sample_size": len(rows),
        "natural_sample_size": natural_size,
        "challenge_sample_size": challenge_size,
        "challenge_flags_are_hidden_from_annotators": True,
        "challenge_flag_counts": dict(sorted(challenge_flag_counts.items())),
        "selected_channel_ids_sha256": "sha256:" + hashlib.sha256(
            "\n".join(row["channel_id"] for row in selected).encode("utf-8")
        ).hexdigest(),
        "blind_ids_sha256": "sha256:" + hashlib.sha256("\n".join(blind_ids).encode("utf-8")).hexdigest(),
        "output_path": str(output),
        "output_sha256": file_sha256(output),
        "annotator_a_template": str(annotator_a),
        "annotator_a_template_sha256": file_sha256(annotator_a),
        "annotator_b_template": str(annotator_b),
        "annotator_b_template_sha256": file_sha256(annotator_b),
        "taxonomy_version": "qy-taxonomy-v2-draft.1",
        "annotation_guideline_version": "qy-annotation-guideline-v0.1",
    }
    write_json(output.with_suffix(output.suffix + ".manifest.json"), manifest)
    return manifest
