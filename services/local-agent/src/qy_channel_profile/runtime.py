from __future__ import annotations

from copy import deepcopy
from typing import Any

from .agent_contract import to_agent_payload
from .contracts import (
    AnalysisPolicy,
    ChannelSnapshot,
    CommentPageRecord,
    ProfileAnalysisRequest,
    parse_datetime,
)
from .errors import ContractError, SnapshotError
from .processor import PROCESSOR_VERSION, ChannelProfileProcessor
from .snapshot_repository import SnapshotRepository


def _sanitize_comment_pages(snapshot_value: dict[str, Any]) -> dict[str, Any]:
    copied = deepcopy(snapshot_value)
    contents = copied.get("contents")
    if not isinstance(contents, list):
        return copied
    as_of = parse_datetime(copied.get("as_of"), field_name="snapshot.as_of")
    invalid: list[dict[str, str]] = []
    for row in contents:
        if not isinstance(row, dict) or row.get("comments_first_page") is None:
            continue
        content_id = str(row.get("source_content_id") or row.get("content_id") or "")
        page_value = row.get("comments_first_page")
        try:
            if not isinstance(page_value, dict):
                raise SnapshotError("comments_first_page must be an object")
            page = CommentPageRecord.from_mapping(page_value)
            if page.collected_at > as_of:
                raise SnapshotError("comments_first_page.collected_at is after snapshot.as_of")
        except SnapshotError as error:
            row.pop("comments_first_page", None)
            invalid.append({"content_id": content_id, "error": str(error)})
    if invalid:
        provenance = dict(copied.get("provenance") or {})
        provenance["invalid_comment_pages"] = [
            *(provenance.get("invalid_comment_pages") or []),
            *invalid,
        ]
        copied["provenance"] = provenance
    return copied


def snapshot_from_runtime(snapshot_value: dict[str, Any]) -> ChannelSnapshot:
    """Parse a live crawler snapshot while isolating malformed comment pages."""

    return ChannelSnapshot.from_mapping(_sanitize_comment_pages(snapshot_value))


def _analyze_snapshot(
    snapshot: ChannelSnapshot,
    processor: ChannelProfileProcessor,
    policy: AnalysisPolicy,
) -> dict[str, Any]:
    input_url = str(snapshot.channel.get("channel_url") or "")
    request = ProfileAnalysisRequest(
        channel_id=snapshot.channel_id,
        input_url=input_url,
        as_of=snapshot.as_of,
        policy=policy,
    )
    result = processor.analyze(request, snapshot)
    payload = to_agent_payload(result)
    return {
        "channel_id": result.channel_id,
        "input_url": payload["input_url"],
        "payload": payload,
        "analysis_result": result.to_dict(),
        "input_content_ids": [content.source_content_id for content in snapshot.contents],
        "processor_version": PROCESSOR_VERSION,
        "model_bundle_version": processor.model_bundle.version,
    }


def analyze_runtime_envelope(
    envelope: dict[str, Any],
    processor: ChannelProfileProcessor,
    policy: AnalysisPolicy = AnalysisPolicy.COMPLETE_ESTIMATE,
) -> dict[str, Any]:
    snapshot_value = envelope.get("snapshot") if isinstance(envelope.get("snapshot"), dict) else envelope
    snapshot = snapshot_from_runtime(snapshot_value)
    requested_channel_id = str(envelope.get("channel_id") or snapshot.channel_id)
    if requested_channel_id != snapshot.channel_id:
        raise SnapshotError("runtime envelope channel_id does not match snapshot")
    return _analyze_snapshot(snapshot, processor, policy)


def analyze_runtime_request(
    request: dict[str, Any],
    processor: ChannelProfileProcessor,
) -> dict[str, Any]:
    """Analyze a worker batch and keep one failed channel from aborting the rest."""

    try:
        policy = AnalysisPolicy(request.get("policy") or AnalysisPolicy.COMPLETE_ESTIMATE.value)
    except ValueError as error:
        raise ContractError(f"unsupported analysis policy: {request.get('policy')}") from error
    if policy is not AnalysisPolicy.COMPLETE_ESTIMATE:
        raise ContractError("runtime Agent replacement requires complete_estimate")

    envelopes = request.get("envelopes")
    if not isinstance(envelopes, list) or not envelopes:
        raise ContractError("runtime request envelopes must be a non-empty array")

    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for envelope in envelopes:
        if not isinstance(envelope, dict):
            errors.append({
                "channel_id": None,
                "input_url": None,
                "error": "runtime envelope must be an object",
            })
            continue
        channel_id = str(envelope.get("channel_id") or "")
        input_url = str(envelope.get("input_url") or "")
        try:
            results.append(analyze_runtime_envelope(envelope, processor, policy))
        except Exception as error:
            errors.append({
                "channel_id": channel_id or None,
                "input_url": input_url or None,
                "error": f"{type(error).__name__}: {error}",
            })
    return {
        "status": "ok" if results else "error",
        "policy": policy.value,
        "processor_version": PROCESSOR_VERSION,
        "model_bundle_version": processor.model_bundle.version,
        "results": results,
        "errors": errors,
    }


def analyze_database_request(
    request: dict[str, Any],
    processor: ChannelProfileProcessor,
    repository: SnapshotRepository,
    policy: AnalysisPolicy = AnalysisPolicy.COMPLETE_ESTIMATE,
) -> dict[str, Any]:
    """Analyze current crawler snapshots selected only by stable Channel ID."""

    unexpected = set(request).difference({"channel_ids"})
    if unexpected:
        raise ContractError(
            "database runtime accepts only channel_ids; unexpected: "
            + ", ".join(sorted(unexpected))
        )
    try:
        policy = AnalysisPolicy(policy)
    except ValueError as error:
        raise ContractError(f"unsupported analysis policy: {policy}") from error
    if policy is not AnalysisPolicy.COMPLETE_ESTIMATE:
        raise ContractError("runtime Agent replacement requires complete_estimate")

    raw_ids = request.get("channel_ids")
    if not isinstance(raw_ids, list) or not raw_ids:
        raise ContractError("database runtime channel_ids must be a non-empty array")
    channel_ids = list(dict.fromkeys(str(value).strip() for value in raw_ids if str(value).strip()))
    if len(channel_ids) != len(raw_ids):
        raise ContractError("database runtime channel_ids must be unique non-empty strings")
    if len(channel_ids) > 50:
        raise ContractError("database runtime cannot analyze more than 50 channels per batch")

    records = repository.load_many(channel_ids)
    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for channel_id in channel_ids:
        record = records.get(channel_id)
        if record is None:
            errors.append({
                "channel_id": channel_id,
                "error": "SnapshotNotFound: channel snapshot not found",
            })
            continue
        try:
            result = _analyze_snapshot(record.snapshot, processor, policy)
            result["source_latest_run_id"] = record.latest_run_id
            results.append(result)
        except Exception as error:
            errors.append({
                "channel_id": channel_id,
                "error": f"{type(error).__name__}: {error}",
            })
    return {
        "status": "ok" if results else "error",
        "policy": policy.value,
        "processor_version": PROCESSOR_VERSION,
        "model_bundle_version": processor.model_bundle.version,
        "results": results,
        "errors": errors,
    }
