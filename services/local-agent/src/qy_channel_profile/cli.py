from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Iterator

from .agent_contract import internal_result_to_agent_payload, to_agent_payload
from .audit import build_snapshot_audit
from .contracts import AnalysisPolicy, ChannelSnapshot, ProfileAnalysisRequest
from .evaluation import (
    build_agreement_report,
    build_detailed_comparison,
    write_detailed_comparison_outputs,
)
from .feature_store import build_feature_store
from .io import read_jsonl, write_json, write_json_array, write_jsonl
from .model_bundle import ModelBundle
from .priors import PriorCatalog
from .processor import ChannelProfileProcessor
from .quality_gate import build_deployment_bundle
from .runtime import analyze_database_request, analyze_runtime_request
from .sampling import (
    build_blind_annotation_pilot,
    build_deterministic_country_coverage_sample,
    build_deterministic_holdout,
)
from .snapshot_repository import PostgresSnapshotRepository
from .training import ChannelProfileModelBuilder, ModelBuildPlan


def _analysis_rows(
    input_path: str,
    processor: ChannelProfileProcessor,
    policy: AnalysisPolicy,
) -> Iterator[dict[str, Any]]:
    for envelope in read_jsonl(input_path):
        snapshot_value = envelope.get("snapshot") if isinstance(envelope.get("snapshot"), dict) else envelope
        snapshot = ChannelSnapshot.from_mapping(snapshot_value)
        input_url = str(envelope.get("input_url") or snapshot.channel.get("channel_url") or "")
        request = ProfileAnalysisRequest(
            channel_id=snapshot.channel_id,
            input_url=input_url,
            as_of=snapshot.as_of,
            policy=policy,
        )
        result = processor.analyze(request, snapshot)
        output = {"result": result.to_dict()}
        if isinstance(envelope.get("agent_reference"), dict):
            output["agent_reference"] = envelope["agent_reference"]
        yield output


def _agent_rows(
    input_path: str,
    processor: ChannelProfileProcessor,
    policy: AnalysisPolicy,
) -> Iterator[dict[str, Any]]:
    for envelope in read_jsonl(input_path):
        snapshot_value = envelope.get("snapshot") if isinstance(envelope.get("snapshot"), dict) else envelope
        snapshot = ChannelSnapshot.from_mapping(snapshot_value)
        input_url = str(envelope.get("input_url") or snapshot.channel.get("channel_url") or "")
        request = ProfileAnalysisRequest(
            channel_id=snapshot.channel_id,
            input_url=input_url,
            as_of=snapshot.as_of,
            policy=policy,
        )
        yield to_agent_payload(processor.analyze(request, snapshot))


def _model_bundle(path: str | None) -> ModelBundle | None:
    if not path:
        return None
    candidate = Path(path).expanduser().resolve()
    return ModelBundle.load(candidate / "manifest.json" if candidate.is_dir() else candidate)


def _analyze(args: argparse.Namespace) -> int:
    catalog = PriorCatalog.load(args.prior_catalog) if args.prior_catalog else PriorCatalog.load()
    processor = ChannelProfileProcessor(catalog, _model_bundle(args.model_bundle))
    policy = AnalysisPolicy(args.policy)
    if args.output_format == "agent-json":
        if policy is not AnalysisPolicy.COMPLETE_ESTIMATE:
            raise ValueError("agent-json output requires --policy complete_estimate")
        count = write_json_array(args.output, _agent_rows(args.input, processor, policy))
    else:
        count = write_jsonl(args.output, _analysis_rows(args.input, processor, policy))
    print(json.dumps({
        "status": "ok",
        "analyzed": count,
        "output": str(Path(args.output)),
        "output_format": args.output_format,
        "model_bundle": processor.model_bundle.version,
    }))
    return 0


def _features(args: argparse.Namespace) -> int:
    manifest = build_feature_store(
        args.input,
        args.output,
        language_model_path=args.language_model,
        prior_catalog_path=args.prior_catalog,
        workers=args.workers,
    )
    print(json.dumps({
        "status": "ok",
        "rows": manifest.row_count,
        "output": manifest.output_path,
        "manifest": str(Path(args.output).with_suffix(Path(args.output).suffix + ".manifest.json")),
    }))
    return 0


def _train(args: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    plan = ModelBuildPlan(
        feature_store_path=Path(args.features),
        output_directory=Path(args.output_directory),
        bundle_version=args.bundle_version,
        language_model_path=Path(args.language_model),
        holdout_snapshot_path=Path(args.holdout) if args.holdout else None,
        analytics_labels_path=Path(args.analytics_labels) if args.analytics_labels else None,
        random_seed=args.seed,
        minimum_total_samples=args.minimum_total_samples,
        minimum_class_samples=args.minimum_class_samples,
        minimum_country_classes=args.minimum_country_classes,
        minimum_multilabel_samples=args.minimum_multilabel_samples,
        minimum_age_samples=args.minimum_age_samples,
        target_evidence_precision=args.target_evidence_precision,
        maximum_topic_text_characters=args.maximum_topic_text_characters,
        maximum_identity_text_characters=args.maximum_identity_text_characters,
    )
    result = ChannelProfileModelBuilder().build(plan)
    print(json.dumps({
        "status": "ok",
        "bundle": str(result.bundle_path),
        "manifest": str(result.manifest_path),
        "artifacts": len(result.manifest.artifacts),
        "field_status": result.manifest.field_status,
        "skipped_artifacts": result.manifest.build_metadata.get("skipped_artifacts", {}),
    }, ensure_ascii=False))
    return 0


def _analyze_runtime(args: argparse.Namespace) -> int:
    raw = sys.stdin.read()
    try:
        request = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError as error:
        raise ValueError(f"runtime stdin must be a JSON object: {error}") from error
    if not isinstance(request, dict):
        raise ValueError("runtime stdin must be a JSON object")
    catalog = PriorCatalog.load(args.prior_catalog) if args.prior_catalog else PriorCatalog.load()
    processor = ChannelProfileProcessor(catalog, _model_bundle(args.model_bundle))
    if args.policy:
        request = {**request, "policy": args.policy}
    payload = analyze_runtime_request(request, processor)
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    sys.stdout.write("\n")
    return 0 if payload.get("results") else 1


def _analyze_db_runtime(args: argparse.Namespace) -> int:
    raw = sys.stdin.read()
    try:
        request = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError as error:
        raise ValueError(f"database runtime stdin must be a JSON object: {error}") from error
    if not isinstance(request, dict):
        raise ValueError("database runtime stdin must be a JSON object")
    catalog = PriorCatalog.load(args.prior_catalog) if args.prior_catalog else PriorCatalog.load()
    processor = ChannelProfileProcessor(catalog, _model_bundle(args.model_bundle))
    repository = PostgresSnapshotRepository.from_environment(
        content_limit=args.content_limit,
        include_comments=not args.exclude_comments,
        statement_timeout_seconds=args.statement_timeout_seconds,
    )
    policy = AnalysisPolicy(args.policy or AnalysisPolicy.COMPLETE_ESTIMATE.value)
    payload = analyze_database_request(request, processor, repository, policy=policy)
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    sys.stdout.write("\n")
    return 0 if payload.get("results") else 1


def _audit(args: argparse.Namespace) -> int:
    report = build_snapshot_audit(read_jsonl(args.input))
    write_json(args.output, report)
    print(json.dumps({
        "status": "ok",
        "rows": report.get("summary", {}).get("rows", 0),
        "output": str(Path(args.output)),
    }))
    return 0


def _gate(args: argparse.Namespace) -> int:
    result = build_deployment_bundle(
        args.source_bundle,
        args.output_directory,
        bundle_version=args.bundle_version,
        target_precision=args.target_precision,
        minimum_kappa=args.minimum_kappa,
        minimum_gold_test_rows=args.minimum_gold_test_rows,
    )
    print(json.dumps({
        "status": "ok",
        "bundle": str(result.bundle_path),
        "manifest": str(result.manifest_path),
        "active_artifacts": [artifact.artifact_id for artifact in result.manifest.artifacts],
        "excluded_artifacts": [
            item["artifact_id"]
            for item in result.manifest.build_metadata.get("excluded_candidate_artifacts", [])
        ],
    }))
    return 0


def _export_agent(args: argparse.Namespace) -> int:
    def rows() -> Iterator[dict[str, Any]]:
        for envelope in read_jsonl(args.input):
            result = envelope.get("result") if isinstance(envelope.get("result"), dict) else envelope
            yield internal_result_to_agent_payload(result)

    count = write_json_array(args.output, rows())
    print(json.dumps({
        "status": "ok",
        "exported": count,
        "output": str(Path(args.output)),
        "output_format": "agent-json",
    }))
    return 0


def _sample(args: argparse.Namespace) -> int:
    builder = (
        build_deterministic_country_coverage_sample
        if args.coverage == "country"
        else build_deterministic_holdout
    )
    manifest = builder(args.input, args.output, sample_size=args.size, seed=args.seed)
    print(json.dumps({
        "status": "ok",
        "sample_size": manifest["sample_size"],
        "source_rows": manifest["source_rows"],
        "output": manifest["output_path"],
        "manifest": str(Path(args.output).with_suffix(Path(args.output).suffix + ".manifest.json")),
    }))
    return 0


def _annotation_pilot(args: argparse.Namespace) -> int:
    manifest = build_blind_annotation_pilot(
        args.input,
        args.output,
        sample_size=args.size,
        challenge_fraction=args.challenge_fraction,
        seed=args.seed,
    )
    print(json.dumps({
        "status": "ok",
        "sample_size": manifest["sample_size"],
        "natural_sample_size": manifest["natural_sample_size"],
        "challenge_sample_size": manifest["challenge_sample_size"],
        "output": manifest["output_path"],
        "manifest": str(Path(args.output).with_suffix(Path(args.output).suffix + ".manifest.json")),
    }))
    return 0


def _benchmark(args: argparse.Namespace) -> int:
    report = build_agreement_report(read_jsonl(args.input))
    write_json(args.output, report)
    print(json.dumps({
        "status": "ok",
        "compared": report.get("summary", {}).get("compared_channels", 0),
        "output": str(Path(args.output)),
    }))
    return 0


def _compare(args: argparse.Namespace) -> int:
    report = build_detailed_comparison(read_jsonl(args.input))
    selection_manifest = None
    if args.sample_manifest:
        selection_manifest = json.loads(
            Path(args.sample_manifest).read_text(encoding="utf-8")
        )
    write_detailed_comparison_outputs(
        report,
        markdown_path=args.markdown,
        csv_path=args.csv,
        source_name=str(Path(args.input)),
        selection_manifest=selection_manifest,
    )
    print(json.dumps({
        "status": "ok",
        "channels": report["channels"],
        "comparisons": len(report["details"]),
        "markdown": str(Path(args.markdown)),
        "csv": str(Path(args.csv)),
    }))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qy-channel-profile")
    subparsers = parser.add_subparsers(dest="command", required=True)
    analyze = subparsers.add_parser("analyze", help="analyze crawler snapshot JSONL")
    analyze.add_argument("--input", required=True)
    analyze.add_argument("--output", required=True)
    analyze.add_argument(
        "--policy",
        choices=[policy.value for policy in AnalysisPolicy],
        default=AnalysisPolicy.COMPLETE_ESTIMATE.value,
    )
    analyze.add_argument("--prior-catalog")
    analyze.add_argument("--model-bundle", help="model bundle directory or manifest.json")
    analyze.add_argument(
        "--output-format",
        choices=("internal-jsonl", "agent-json"),
        default="internal-jsonl",
        help="internal evidence-rich JSONL or strict legacy Agent JSON array",
    )
    analyze.set_defaults(func=_analyze)

    analyze_runtime = subparsers.add_parser(
        "analyze-runtime",
        help="analyze a live worker JSON batch from stdin and emit Agent-compatible JSON",
    )
    analyze_runtime.add_argument("--policy", choices=[policy.value for policy in AnalysisPolicy])
    analyze_runtime.add_argument("--prior-catalog")
    analyze_runtime.add_argument("--model-bundle", help="model bundle directory or manifest.json")
    analyze_runtime.set_defaults(func=_analyze_runtime)

    analyze_db_runtime = subparsers.add_parser(
        "analyze-db-runtime",
        help="read current crawler snapshots by Channel ID and emit Agent-compatible JSON",
    )
    analyze_db_runtime.add_argument("--policy", choices=[policy.value for policy in AnalysisPolicy])
    analyze_db_runtime.add_argument("--prior-catalog")
    analyze_db_runtime.add_argument("--model-bundle", help="model bundle directory or manifest.json")
    analyze_db_runtime.add_argument("--content-limit", type=int, default=30)
    analyze_db_runtime.add_argument("--exclude-comments", action="store_true")
    analyze_db_runtime.add_argument("--statement-timeout-seconds", type=int, default=60)
    analyze_db_runtime.set_defaults(func=_analyze_db_runtime)

    features = subparsers.add_parser("features", help="build an Agent-free Parquet feature store")
    features.add_argument("--input", required=True)
    features.add_argument("--output", required=True)
    features.add_argument("--language-model", required=True)
    features.add_argument("--prior-catalog")
    features.add_argument("--workers", type=int, default=1)
    features.set_defaults(func=_features)

    train = subparsers.add_parser("train", help="build an immutable Agent-free model bundle")
    train.add_argument("--features", required=True)
    train.add_argument("--output-directory", required=True)
    train.add_argument("--bundle-version", required=True)
    train.add_argument("--language-model", required=True)
    train.add_argument("--holdout", help="snapshot JSONL whose channel IDs must be excluded from training")
    train.add_argument("--analytics-labels")
    train.add_argument("--seed", type=int, default=20260809)
    train.add_argument("--minimum-total-samples", type=int, default=200)
    train.add_argument("--minimum-class-samples", type=int, default=20)
    train.add_argument("--minimum-country-classes", type=int, default=5)
    train.add_argument("--minimum-multilabel-samples", type=int, default=20)
    train.add_argument("--minimum-age-samples", type=int, default=200)
    train.add_argument("--target-evidence-precision", type=float, default=0.85)
    train.add_argument("--maximum-topic-text-characters", type=int, default=12000)
    train.add_argument("--maximum-identity-text-characters", type=int, default=16000)
    train.set_defaults(func=_train)

    audit = subparsers.add_parser("audit", help="audit snapshots and historical Agent references")
    audit.add_argument("--input", required=True)
    audit.add_argument("--output", required=True)
    audit.set_defaults(func=_audit)

    gate = subparsers.add_parser("gate", help="create a compact deployment bundle from validated artifacts")
    gate.add_argument("--source-bundle", required=True)
    gate.add_argument("--output-directory", required=True)
    gate.add_argument("--bundle-version", required=True)
    gate.add_argument("--target-precision", type=float, default=0.85)
    gate.add_argument("--minimum-kappa", type=float, default=0.70)
    gate.add_argument("--minimum-gold-test-rows", type=int, default=200)
    gate.set_defaults(func=_gate)

    export_agent = subparsers.add_parser(
        "export-agent",
        help="project internal JSONL into the strict legacy 11-field JSON array",
    )
    export_agent.add_argument("--input", required=True)
    export_agent.add_argument("--output", required=True)
    export_agent.set_defaults(func=_export_agent)

    sample = subparsers.add_parser("sample", help="build a deterministic Agent-blind holdout sample")
    sample.add_argument("--input", required=True)
    sample.add_argument("--output", required=True)
    sample.add_argument("--size", required=True, type=int)
    sample.add_argument("--seed", default="qy-agent-free-holdout-v1")
    sample.add_argument("--coverage", choices=("none", "country"), default="none")
    sample.set_defaults(func=_sample)

    annotation_pilot = subparsers.add_parser(
        "annotation-pilot",
        help="build an Agent-blind taxonomy annotation pilot and empty annotator templates",
    )
    annotation_pilot.add_argument("--input", required=True)
    annotation_pilot.add_argument("--output", required=True)
    annotation_pilot.add_argument("--size", type=int, default=200)
    annotation_pilot.add_argument("--challenge-fraction", type=float, default=0.5)
    annotation_pilot.add_argument("--seed", default="qy-taxonomy-v2-pilot-v1")
    annotation_pilot.set_defaults(func=_annotation_pilot)

    benchmark = subparsers.add_parser("benchmark", help="compare local output with historical Agent reference")
    benchmark.add_argument("--input", required=True)
    benchmark.add_argument("--output", required=True)
    benchmark.set_defaults(func=_benchmark)

    compare = subparsers.add_parser(
        "compare",
        help="write detailed Markdown and CSV tables against historical Agent output",
    )
    compare.add_argument("--input", required=True)
    compare.add_argument("--markdown", required=True)
    compare.add_argument("--csv", required=True)
    compare.add_argument("--sample-manifest")
    compare.set_defaults(func=_compare)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except Exception as error:
        print(json.dumps({"status": "error", "type": type(error).__name__, "message": str(error)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
