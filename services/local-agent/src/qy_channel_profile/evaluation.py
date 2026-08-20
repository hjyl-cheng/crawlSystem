from __future__ import annotations

import math
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable

from .analyzers import project_tags_to_controlled_vocabulary


FIELDS = (
    "country",
    "creator_gender",
    "creator_age_range",
    "creator_language",
    "audience_region",
    "audience_age_gender",
    "audience_language",
    "active_subscriber_ratio",
    "channel_tags",
    "channel_categories",
)


def agent_values(reference: dict[str, Any]) -> dict[str, Any]:
    metrics = reference.get("metrics_json") if isinstance(reference, dict) else None
    if not isinstance(metrics, dict):
        metrics = reference
    wrapped = metrics.get("audience_profile_agent") if isinstance(metrics, dict) else None
    source = wrapped if isinstance(wrapped, dict) else (metrics if isinstance(metrics, dict) else {})
    values: dict[str, Any] = {}
    for field in FIELDS:
        raw = source.get(field)
        values[field] = raw.get("value") if isinstance(raw, dict) and "value" in raw else raw
    return values


def _local_values(result: dict[str, Any]) -> dict[str, Any]:
    facts = result.get("facts") or {}
    return {
        field: (facts.get(field) or {}).get("value")
        for field in FIELDS
    }


def _distribution(rows: Any, label_key: str, value_key: str = "percentage") -> dict[str, float]:
    if not isinstance(rows, list):
        return {}
    values: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict) or row.get(label_key) is None:
            continue
        try:
            values[str(row[label_key])] = max(0.0, float(row[value_key]))
        except (TypeError, ValueError, KeyError):
            continue
    total = sum(values.values())
    return {key: value / total for key, value in values.items()} if total else {}


def _age_gender_distribution(rows: Any) -> dict[str, float]:
    if not isinstance(rows, list):
        return {}
    values: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict) or not row.get("age_range"):
            continue
        for gender in ("male", "female"):
            try:
                values[f"{row['age_range']}_{gender}"] = max(0.0, float(row[gender]))
            except (TypeError, ValueError, KeyError):
                pass
    total = sum(values.values())
    return {key: value / total for key, value in values.items()} if total else {}


def _js_divergence(left: dict[str, float], right: dict[str, float]) -> float | None:
    keys = set(left) | set(right)
    if not keys or not left or not right:
        return None
    midpoint = {key: (left.get(key, 0.0) + right.get(key, 0.0)) / 2 for key in keys}

    def kl(source: dict[str, float]) -> float:
        return sum(
            value * math.log(value / midpoint[key], 2)
            for key, value in source.items()
            if value > 0 and midpoint[key] > 0
        )

    return (kl(left) + kl(right)) / 2


def _hellinger(left: dict[str, float], right: dict[str, float]) -> float | None:
    keys = set(left) | set(right)
    if not keys or not left or not right:
        return None
    return math.sqrt(sum((math.sqrt(left.get(key, 0.0)) - math.sqrt(right.get(key, 0.0))) ** 2 for key in keys)) / math.sqrt(2)


def _jaccard(left: Iterable[str], right: Iterable[str]) -> float | None:
    left_set = {str(value).casefold() for value in left if str(value).strip()}
    right_set = {str(value).casefold() for value in right if str(value).strip()}
    union = left_set | right_set
    return len(left_set & right_set) / len(union) if union else None


def _spearman(pairs: list[tuple[float, float]]) -> float | None:
    if len(pairs) < 2:
        return None

    def ranks(values: list[float]) -> list[float]:
        indexed = sorted(enumerate(values), key=lambda item: item[1])
        result = [0.0] * len(values)
        index = 0
        while index < len(indexed):
            end = index + 1
            while end < len(indexed) and indexed[end][1] == indexed[index][1]:
                end += 1
            rank = (index + end - 1) / 2 + 1
            for original, _ in indexed[index:end]:
                result[original] = rank
            index = end
        return result

    left_rank = ranks([left for left, _ in pairs])
    right_rank = ranks([right for _, right in pairs])
    left_mean = mean(left_rank)
    right_mean = mean(right_rank)
    numerator = sum((left - left_mean) * (right - right_mean) for left, right in zip(left_rank, right_rank))
    denominator = math.sqrt(
        sum((left - left_mean) ** 2 for left in left_rank)
        * sum((right - right_mean) ** 2 for right in right_rank)
    )
    return numerator / denominator if denominator else None


def build_agreement_report(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter()
    exact: defaultdict[str, list[bool]] = defaultdict(list)
    errors: defaultdict[str, list[float]] = defaultdict(list)
    distributions: defaultdict[str, list[tuple[float, float]]] = defaultdict(list)
    set_scores: defaultdict[str, list[float]] = defaultdict(list)
    active_pairs: list[tuple[float, float]] = []
    snapshot_quality = Counter()
    difference_samples: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)

    for envelope in rows:
        result = envelope.get("result") if isinstance(envelope.get("result"), dict) else envelope
        reference = envelope.get("agent_reference")
        if not isinstance(reference, dict):
            counts["without_agent_reference"] += 1
            continue
        counts["compared_channels"] += 1
        snapshot_quality[str((result.get("snapshot") or {}).get("quality") or "unknown")] += 1
        local = _local_values(result)
        agent = agent_values(reference)
        channel_id = result.get("channel_id")

        for field in ("country", "creator_gender", "creator_language"):
            if local[field] is None or agent[field] is None:
                continue
            same = str(local[field]).casefold() == str(agent[field]).casefold()
            exact[field].append(same)
            if not same and len(difference_samples[field]) < 20:
                difference_samples[field].append({"channel_id": channel_id, "local": local[field], "agent": agent[field]})

        for field in ("creator_age_range", "active_subscriber_ratio"):
            try:
                local_number = float(local[field])
                agent_number = float(agent[field])
            except (TypeError, ValueError):
                continue
            errors[field].append(abs(local_number - agent_number))
            if field == "active_subscriber_ratio":
                active_pairs.append((local_number, agent_number))

        for field, label in (("audience_region", "region"), ("audience_language", "language")):
            left = _distribution(local[field], label)
            right = _distribution(agent[field], label)
            js = _js_divergence(left, right)
            hellinger = _hellinger(left, right)
            if js is not None and hellinger is not None:
                distributions[field].append((js, hellinger))
        left_age = _age_gender_distribution(local["audience_age_gender"])
        right_age = _age_gender_distribution(agent["audience_age_gender"])
        js = _js_divergence(left_age, right_age)
        hellinger = _hellinger(left_age, right_age)
        if js is not None and hellinger is not None:
            distributions["audience_age_gender"].append((js, hellinger))

        local_tags = (local["channel_tags"] or {}).get("tags", []) if isinstance(local["channel_tags"], dict) else []
        agent_tags = (agent["channel_tags"] or {}).get("tags", []) if isinstance(agent["channel_tags"], dict) else []
        score = _jaccard(local_tags, agent_tags)
        if score is not None:
            set_scores["channel_tags"].append(score)
        controlled_score = _jaccard(
            project_tags_to_controlled_vocabulary(local_tags),
            project_tags_to_controlled_vocabulary(agent_tags),
        )
        if controlled_score is not None:
            set_scores["channel_tags.controlled_vocabulary"].append(controlled_score)
        local_categories = local["channel_categories"] if isinstance(local["channel_categories"], dict) else {}
        agent_categories = agent["channel_categories"] if isinstance(agent["channel_categories"], dict) else {}
        if local_categories and agent_categories:
            exact["channel_categories.level_1"].append(local_categories.get("level_1") == agent_categories.get("level_1"))
            score = _jaccard(local_categories.get("level_2", []), agent_categories.get("level_2", []))
            if score is not None:
                set_scores["channel_categories.level_2"].append(score)

    field_report: dict[str, Any] = {}
    for field, values in exact.items():
        field_report[field] = {
            "comparable": len(values),
            "agreement": round(sum(values) / len(values), 6) if values else None,
        }
    for field, values in errors.items():
        field_report[field] = {
            "comparable": len(values),
            "mean_absolute_difference": round(mean(values), 6) if values else None,
            "median_absolute_difference": round(median(values), 6) if values else None,
            **({"spearman": round(_spearman(active_pairs), 6) if _spearman(active_pairs) is not None else None} if field == "active_subscriber_ratio" else {}),
        }
    for field, values in distributions.items():
        field_report[field] = {
            "comparable": len(values),
            "mean_js_divergence": round(mean(value[0] for value in values), 6) if values else None,
            "mean_hellinger_distance": round(mean(value[1] for value in values), 6) if values else None,
        }
    for field, values in set_scores.items():
        field_report[field] = {
            "comparable": len(values),
            "mean_jaccard": round(mean(values), 6) if values else None,
        }

    return {
        "report_type": "frozen_agent_agreement_not_accuracy",
        "summary": dict(counts),
        "snapshot_quality": dict(snapshot_quality),
        "fields": field_report,
        "difference_samples": dict(difference_samples),
        "caveats": [
            "Historical Agent output is a reference, not ground truth.",
            "approximate_as_of snapshots are not strict same-input replays.",
            "Audience and active-subscriber fields cannot be validated as true without Analytics or an approved panel.",
        ],
    }


def _compact_value(field: str, value: Any) -> str:
    if value is None:
        return "unavailable"
    if field in {"country", "creator_gender", "creator_language"}:
        return str(value)
    if field in {"creator_age_range", "active_subscriber_ratio"}:
        return str(value)
    if field in {"audience_region", "audience_language"} and isinstance(value, list):
        label = "region" if field == "audience_region" else "language"
        return "; ".join(
            f"{row.get(label)} {row.get('percentage')}%"
            for row in value
            if isinstance(row, dict)
        )
    if field == "audience_age_gender" and isinstance(value, list):
        return "; ".join(
            f"{row.get('age_range')} M{row.get('male')}/F{row.get('female')}"
            for row in value
            if isinstance(row, dict)
        )
    if field == "channel_tags" and isinstance(value, dict):
        return ", ".join(str(item) for item in value.get("tags", []))
    if field == "channel_categories" and isinstance(value, dict):
        level_2 = ", ".join(str(item) for item in value.get("level_2", []))
        return f"{value.get('level_1')} > {level_2}" if level_2 else str(value.get("level_1"))
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _field_comparison(field: str, local: Any, agent: Any) -> tuple[str, float | None]:
    if local is None or agent is None:
        return "not comparable", None
    if field in {"country", "creator_gender", "creator_language"}:
        same = str(local).casefold() == str(agent).casefold()
        return ("match" if same else "different"), float(same)
    if field in {"creator_age_range", "active_subscriber_ratio"}:
        try:
            difference = abs(float(local) - float(agent))
        except (TypeError, ValueError):
            return "not comparable", None
        return f"absolute difference {difference:g}", difference
    if field in {"audience_region", "audience_language"}:
        label = "region" if field == "audience_region" else "language"
        distance = _js_divergence(_distribution(local, label), _distribution(agent, label))
        return (f"JS divergence {distance:.4f}" if distance is not None else "not comparable"), distance
    if field == "audience_age_gender":
        distance = _js_divergence(_age_gender_distribution(local), _age_gender_distribution(agent))
        return (f"JS divergence {distance:.4f}" if distance is not None else "not comparable"), distance
    if field == "channel_tags":
        local_tags = local.get("tags", []) if isinstance(local, dict) else []
        agent_tags = agent.get("tags", []) if isinstance(agent, dict) else []
        raw = _jaccard(local_tags, agent_tags)
        controlled = _jaccard(
            project_tags_to_controlled_vocabulary(local_tags),
            project_tags_to_controlled_vocabulary(agent_tags),
        )
        text = f"Jaccard {raw:.3f}" if raw is not None else "not comparable"
        if controlled is not None:
            text += f"; controlled {controlled:.3f}"
        return text, raw
    if field == "channel_categories":
        if not isinstance(local, dict) or not isinstance(agent, dict):
            return "not comparable", None
        level_1_match = local.get("level_1") == agent.get("level_1")
        level_2 = _jaccard(local.get("level_2", []), agent.get("level_2", []))
        suffix = f"; Level 2 Jaccard {level_2:.3f}" if level_2 is not None else ""
        return f"Level 1 {'match' if level_1_match else 'different'}{suffix}", float(level_1_match)
    return "not comparable", None


def build_detailed_comparison(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    envelopes = list(rows)
    summary = build_agreement_report(envelopes)
    details: list[dict[str, Any]] = []
    for sample_index, envelope in enumerate(envelopes, 1):
        result = envelope.get("result") if isinstance(envelope.get("result"), dict) else envelope
        reference = envelope.get("agent_reference")
        if not isinstance(reference, dict):
            continue
        local = _local_values(result)
        agent = agent_values(reference)
        result_snapshot = result.get("snapshot") or {}
        facts = result.get("facts") or {}
        for field in FIELDS:
            fact = facts.get(field) or {}
            comparison, metric = _field_comparison(field, local[field], agent[field])
            details.append({
                "sample_index": sample_index,
                "channel_id": str(result.get("channel_id") or ""),
                "channel_title": str(result_snapshot.get("channel_title") or ""),
                "input_url": str(result.get("input_url") or ""),
                "field": field,
                "local_value": _compact_value(field, local[field]),
                "agent_value": _compact_value(field, agent[field]),
                "comparison": comparison,
                "metric_value": metric,
                "local_source": str(fact.get("source_type") or ""),
                "local_evidence_strength": str(fact.get("evidence_strength") or ""),
                "local_confidence": fact.get("evidence_confidence"),
            })
    return {
        "report_type": "frozen_agent_detailed_comparison_not_accuracy",
        "channels": len({row["channel_id"] for row in details}),
        "summary": summary,
        "details": details,
    }


def _markdown_escape(value: Any) -> str:
    return str(value if value is not None else "").replace("|", "\\|").replace("\n", " ")


def _summary_rows(report: dict[str, Any]) -> list[tuple[str, str, str]]:
    fields = report["summary"].get("fields", {})
    rows: list[tuple[str, str, str]] = []
    for field in ("country", "creator_gender", "creator_language", "channel_categories.level_1"):
        value = fields.get(field, {})
        rows.append((field, "Exact agreement", f"{value.get('agreement', 0):.3f}"))
    for field in ("creator_age_range", "active_subscriber_ratio"):
        value = fields.get(field, {})
        extra = f"; Spearman {value.get('spearman'):.3f}" if value.get("spearman") is not None else ""
        rows.append((field, "Mean / median absolute difference", f"{value.get('mean_absolute_difference', 0):.2f} / {value.get('median_absolute_difference', 0):.2f}{extra}"))
    for field in ("audience_region", "audience_age_gender", "audience_language"):
        value = fields.get(field, {})
        rows.append((field, "Mean JS / Hellinger distance", f"{value.get('mean_js_divergence', 0):.4f} / {value.get('mean_hellinger_distance', 0):.4f}"))
    for field in ("channel_tags", "channel_tags.controlled_vocabulary", "channel_categories.level_2"):
        value = fields.get(field, {})
        rows.append((field, "Mean Jaccard", f"{value.get('mean_jaccard', 0):.3f}"))
    return rows


def render_detailed_comparison_markdown(
    report: dict[str, Any],
    *,
    source_name: str,
    selection_manifest: dict[str, Any] | None = None,
) -> str:
    lines = [
        f"# QY 频道画像：{report['channels']} 个频道本地模块与历史 Agent 对比",
        "",
        "## 阅读说明",
        "",
        f"- 本地结果来源：`{source_name}`",
        f"- 频道数：`{report['channels']}`；逐字段对比数：`{len(report['details'])}`。",
        "- 历史 Agent 输出只是冻结的参照结果，不是真值；下表是 agreement，不是 accuracy。",
        "- 三个受众分布和 `active_subscriber_ratio` 是未校准的公开信号估计，不是 YouTube Analytics 实测。",
        "- JS/Hellinger 越低表示两个分布越接近；Jaccard 越高表示集合重合越多。",
    ]
    if selection_manifest:
        lines.extend([
            f"- 抽样方法：`{selection_manifest.get('selection_method')}`；seed：`{selection_manifest.get('seed')}`。",
            f"- 抽样是否读取 Agent 值：`{selection_manifest.get('selection_uses_agent_values')}`；频道 ID 哈希：`{selection_manifest.get('selected_channel_ids_sha256')}`。",
        ])
    lines.extend([
        "",
        "## 汇总",
        "",
        "| 字段 | 对比指标 | 结果 |",
        "| --- | --- | ---: |",
    ])
    lines.extend(
        f"| `{field}` | {metric} | {value} |"
        for field, metric, value in _summary_rows(report)
    )
    by_field: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in report["details"]:
        by_field[row["field"]].append(row)
    for field in FIELDS:
        lines.extend([
            "",
            f"## {field}",
            "",
            "| # | 频道 | 本地模块 | 历史 Agent | 对比 | 本地证据 |",
            "| ---: | --- | --- | --- | --- | --- |",
        ])
        for row in by_field[field]:
            title = row["channel_title"] or row["channel_id"]
            channel = f"[{_markdown_escape(title)}]({_markdown_escape(row['input_url'])})<br>`{_markdown_escape(row['channel_id'])}`"
            confidence = row["local_confidence"]
            evidence = f"{row['local_source']} / {row['local_evidence_strength']}"
            if isinstance(confidence, (int, float)):
                evidence += f" / {confidence:.2f}"
            lines.append(
                "| {sample_index} | {channel} | {local} | {agent} | {comparison} | {evidence} |".format(
                    sample_index=row["sample_index"],
                    channel=channel,
                    local=_markdown_escape(row["local_value"]),
                    agent=_markdown_escape(row["agent_value"]),
                    comparison=_markdown_escape(row["comparison"]),
                    evidence=_markdown_escape(evidence),
                )
            )
    lines.extend([
        "",
        "## 结论边界",
        "",
        "Agreement 只能说明两种估计方法是否接近，不能证明哪一个正确。爬虫结构化国家字段属于观测值；没有明确自我介绍时，多数年龄和性别结果仍是低证据估计；受众与活跃度字段仍需要 Analytics 或独立实测样本才能做真实性校准。",
        "",
    ])
    return "\n".join(lines)


def write_detailed_comparison_outputs(
    report: dict[str, Any],
    *,
    markdown_path: str | Path,
    csv_path: str | Path,
    source_name: str,
    selection_manifest: dict[str, Any] | None = None,
) -> None:
    markdown = Path(markdown_path)
    csv_output = Path(csv_path)
    markdown.parent.mkdir(parents=True, exist_ok=True)
    csv_output.parent.mkdir(parents=True, exist_ok=True)
    markdown.write_text(
        render_detailed_comparison_markdown(
            report,
            source_name=source_name,
            selection_manifest=selection_manifest,
        ),
        encoding="utf-8",
    )
    columns = (
        "sample_index", "channel_id", "channel_title", "input_url", "field",
        "local_value", "agent_value", "comparison", "metric_value",
        "local_source", "local_evidence_strength", "local_confidence",
    )
    with csv_output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows({name: row.get(name) for name in columns} for row in report["details"])
