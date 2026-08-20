from __future__ import annotations

from typing import Any

from .analyzers import unavailable
from .contracts import AnalysisPolicy, FieldResult
from .ml_models import AGE_BUCKETS, AGE_BUCKET_INTERVALS
from .model_bundle import ModelBundle, ModelPrediction
from .taxonomy import CHANNEL_CATEGORY_TREE


MODEL_POLICY_VERSION = "trained-model-decision-v1"


def _accepted(
    prediction: ModelPrediction,
    field: str,
    policy: AnalysisPolicy,
    bundle: ModelBundle,
) -> bool:
    decision = bundle.decision_policy(field)
    if decision is None:
        return policy is AnalysisPolicy.COMPLETE_ESTIMATE
    if policy is AnalysisPolicy.COMPLETE_ESTIMATE:
        return prediction.probability >= decision.complete_min_probability
    return (
        prediction.probability >= decision.evidence_min_probability
        and prediction.margin >= decision.evidence_min_margin
    )


def _model_result(
    prediction: ModelPrediction,
    *,
    value: Any,
    field: str,
    bundle: ModelBundle,
    extra_metadata: dict[str, Any] | None = None,
) -> FieldResult:
    decision = bundle.decision_policy(field)
    strong = bool(
        decision
        and prediction.probability >= decision.evidence_min_probability
        and prediction.margin >= decision.evidence_min_margin
    )
    return FieldResult(
        value=value,
        source_type="public_signal_model",
        truth_status="estimated",
        evidence_strength="strong" if strong else "weak",
        model_confidence=round(prediction.probability, 6),
        evidence_confidence=round(min(0.85, prediction.probability * 0.8), 6),
        candidates=prediction.candidates,
        evidence_refs=(f"model_bundle:{bundle.version}:{prediction.metadata['artifact_id']}",),
        model_version=prediction.model_version,
        decision_policy_version=decision.version if decision else MODEL_POLICY_VERSION,
        metadata={**prediction.metadata, **(extra_metadata or {})},
    )


def resolve_country(
    baseline: FieldResult,
    prediction: ModelPrediction | None,
    policy: AnalysisPolicy,
    bundle: ModelBundle,
) -> FieldResult:
    if baseline.source_type == "observed" or baseline.evidence_strength in {"explicit", "strong"}:
        return baseline
    if prediction and _accepted(prediction, "country", policy, bundle):
        return _model_result(prediction, value=str(prediction.value), field="country", bundle=bundle)
    if baseline.value is not None:
        return baseline
    return unavailable("country_model_probability_below_field_policy")


def _has_protected_primary_gender(baseline: FieldResult) -> bool:
    if baseline.value not in {"male", "female"}:
        return False
    if baseline.metadata.get("evidence_tier") == "A":
        return True
    if baseline.evidence_strength in {"explicit", "strong"}:
        return True
    return baseline.metadata.get("consensus_gate_passed") is True


def resolve_gender(
    baseline: FieldResult,
    entity_prediction: ModelPrediction | None,
    gender_prediction: ModelPrediction | None,
    policy: AnalysisPolicy,
    bundle: ModelBundle,
) -> FieldResult:
    if _has_protected_primary_gender(baseline):
        return baseline
    if entity_prediction is None or not _accepted(entity_prediction, "creator_gender", policy, bundle):
        return baseline if baseline.value is not None else unavailable("creator_entity_model_below_field_policy")
    if entity_prediction.value == "brand_or_team":
        return _model_result(
            entity_prediction,
            value="brand_team",
            field="creator_gender",
            bundle=bundle,
            extra_metadata={"entity_type": "brand_or_team"},
        )
    if entity_prediction.value != "single_creator" or gender_prediction is None:
        return baseline if baseline.value is not None else unavailable("single_creator_gender_model_unavailable")
    if not _accepted(gender_prediction, "creator_gender", policy, bundle):
        return baseline if baseline.value is not None else unavailable("creator_gender_model_below_field_policy")
    combined_probability = min(entity_prediction.probability, gender_prediction.probability)
    combined = ModelPrediction(
        value=gender_prediction.value,
        probability=combined_probability,
        margin=min(entity_prediction.margin, gender_prediction.margin),
        candidates=gender_prediction.candidates,
        model_version=f"{entity_prediction.model_version}+{gender_prediction.model_version}",
        metadata={
            "artifact_id": "gender.entity_type+gender.single_creator",
            "entity_probability": entity_prediction.probability,
            "gender_probability": gender_prediction.probability,
            "label_quality": "explicit_fact",
        },
    )
    return _model_result(
        combined,
        value=str(gender_prediction.value),
        field="creator_gender",
        bundle=bundle,
        extra_metadata={"entity_type": "single_creator"},
    )


def resolve_age(
    baseline: FieldResult,
    prediction: ModelPrediction | None,
    policy: AnalysisPolicy,
    bundle: ModelBundle,
) -> FieldResult:
    if (
        baseline.evidence_strength in {"explicit", "strong"}
        or baseline.metadata.get("evidence_tier") == "A"
        or baseline.evidence_strength == "prior_only"
    ):
        return baseline
    if prediction is None or not _accepted(prediction, "creator_age_range", policy, bundle):
        return baseline if baseline.value is not None else unavailable("creator_age_model_below_field_policy")
    try:
        index = AGE_BUCKETS.index(str(prediction.value))
    except ValueError:
        return baseline if baseline.value is not None else unavailable("creator_age_model_returned_invalid_bucket")
    lower, upper = AGE_BUCKET_INTERVALS[index]
    compat_integer = round((lower + upper) / 2)
    return _model_result(
        prediction,
        value=compat_integer,
        field="creator_age_range",
        bundle=bundle,
        extra_metadata={
            "age_bucket": prediction.value,
            "age_interval": [lower, upper],
            "compatibility_integer_method": "bucket_midpoint",
        },
    )


def _rule_level1_holds(baseline: FieldResult) -> bool:
    if not isinstance(baseline.value, dict):
        return False
    level_1 = baseline.value.get("level_1")
    if not level_1 or level_1 == "Uncategorized":
        return False
    if baseline.evidence_strength in {"explicit", "strong"}:
        return True
    top_score = float((baseline.metadata or {}).get("top_score") or 0.0)
    return float(baseline.model_confidence or 0.0) >= 0.5 and top_score >= 4.0


def resolve_categories(
    baseline: FieldResult,
    level_1_prediction: ModelPrediction | None,
    level_2_prediction: ModelPrediction | None,
    policy: AnalysisPolicy,
    bundle: ModelBundle,
) -> FieldResult:
    if baseline.evidence_strength == "explicit":
        return baseline
    rule_level_1 = baseline.value.get("level_1") if isinstance(baseline.value, dict) else None
    if _rule_level1_holds(baseline) and (
        level_1_prediction is None or str(level_1_prediction.value) != rule_level_1
    ):
        return baseline
    if level_1_prediction is None or level_2_prediction is None:
        return baseline
    if not _accepted(level_1_prediction, "channel_categories", policy, bundle):
        return baseline
    level_1 = str(level_1_prediction.value)
    branch = CHANNEL_CATEGORY_TREE.get(level_1)
    if not branch:
        return baseline
    threshold = 0.0
    thresholds: dict[str, float] = {}
    descriptor = bundle.descriptor("categories.level2")
    if descriptor:
        threshold = float(descriptor.metadata.get("threshold", 0.5))
        thresholds = {
            str(label): float(value)
            for label, value in (descriptor.metadata.get("thresholds") or {}).items()
        }
    branch_candidates = [
        candidate for candidate in level_2_prediction.candidates
        if candidate["value"] in branch
    ]
    if not branch_candidates:
        return baseline
    selected = [
        str(candidate["value"])
        for candidate in branch_candidates
        if float(candidate["probability"])
        >= thresholds.get(str(candidate["value"]), threshold)
    ][:3]
    if not selected:
        selected = [str(branch_candidates[0]["value"])]
    combined_probability = min(
        level_1_prediction.probability,
        max(float(candidate["probability"]) for candidate in branch_candidates if candidate["value"] in selected),
    )
    combined = ModelPrediction(
        value=level_1,
        probability=combined_probability,
        margin=level_1_prediction.margin,
        candidates=level_1_prediction.candidates,
        model_version=f"{level_1_prediction.model_version}+{level_2_prediction.model_version}",
        metadata={
            "artifact_id": "categories.level1+categories.level2",
            "label_quality": "high_precision_weak_supervision",
            "branch_mask_applied": True,
            "level_2_candidates": branch_candidates[:5],
        },
    )
    return _model_result(
        combined,
        value={"level_1": level_1, "level_2": selected},
        field="channel_categories",
        bundle=bundle,
    )


def resolve_tags(
    baseline: FieldResult,
    prediction: ModelPrediction | None,
    policy: AnalysisPolicy,
    bundle: ModelBundle,
) -> FieldResult:
    if prediction is None or not isinstance(baseline.value, dict):
        return baseline
    if not _accepted(prediction, "channel_tags", policy, bundle):
        return baseline
    descriptor = bundle.descriptor("channel_tags.multilabel")
    threshold = float(descriptor.metadata.get("threshold", 0.5)) if descriptor else 0.5
    thresholds = {
        str(label): float(value)
        for label, value in ((descriptor.metadata.get("thresholds") or {}) if descriptor else {}).items()
    }
    baseline_tags = [str(value) for value in baseline.value.get("tags", [])]
    supported = {
        str(value).casefold()
        for value in baseline.metadata.get("supported_tags", [])
    }
    accepted_candidates = [
        candidate
        for candidate in prediction.candidates
        if float(candidate["probability"])
        >= thresholds.get(str(candidate["value"]), threshold)
        and str(candidate["value"]).casefold() in supported
    ]
    if not accepted_candidates:
        return baseline
    predicted = [str(candidate["value"]) for candidate in accepted_candidates]

    # Keep the evidence-ranked top five and its measured attribution stable.
    # The weak-supervision model may reorder the tail, but it cannot introduce
    # a topic that the current snapshot did not independently support.
    merged: list[str] = []
    for value in [*baseline_tags[:5], *predicted, *baseline_tags[5:]]:
        if value.casefold() not in {item.casefold() for item in merged}:
            merged.append(value)
        if len(merged) == 10:
            break
    if len(merged) != 10:
        return baseline
    value = {
        "tags": merged,
        "top_5_distribution": list(baseline.value.get("top_5_distribution", [])),
    }
    confirmed = ModelPrediction(
        value=predicted,
        probability=max(float(candidate["probability"]) for candidate in accepted_candidates),
        margin=prediction.margin,
        candidates=tuple(accepted_candidates),
        model_version=prediction.model_version,
        metadata=prediction.metadata,
    )
    return _model_result(
        confirmed,
        value=value,
        field="channel_tags",
        bundle=bundle,
        extra_metadata={
            **baseline.metadata,
            "rule_and_model_blend": True,
            "model_role": "evidence_constrained_tail_reranker",
            "model_confirmed_tags": predicted,
        },
    )
