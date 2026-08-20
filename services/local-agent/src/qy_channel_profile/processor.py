from __future__ import annotations

from datetime import timezone
from typing import Any

from .analyzers import (
    analyze_active_ratio,
    analyze_age,
    analyze_audience_age_gender,
    analyze_audience_markets,
    analyze_categories,
    analyze_country,
    analyze_gender,
    analyze_language,
    analyze_tags,
    primary_language_hint,
)
from .contracts import (
    FACT_FIELDS,
    AnalysisPolicy,
    ChannelSnapshot,
    ProfileAnalysisRequest,
    ProfileAnalysisResult,
)
from .errors import SnapshotError
from .features import build_channel_features
from .inference import (
    resolve_age,
    resolve_categories,
    resolve_country,
    resolve_gender,
    resolve_tags,
)
from .model_bundle import ModelBundle
from .priors import PriorCatalog
from .taxonomy import TAXONOMY_VERSION


PROCESSOR_VERSION = "agent-free-v0.7-identity-evidence"


class ChannelProfileProcessor:
    """Deterministic read-only processor for the ten profile facts."""

    def __init__(
        self,
        prior_catalog: PriorCatalog | None = None,
        model_bundle: ModelBundle | None = None,
    ) -> None:
        self.prior_catalog = prior_catalog or PriorCatalog.load()
        self.model_bundle = model_bundle or ModelBundle.empty()
        self.language_identifier = self.model_bundle.language_identifier()

    def analyze(
        self,
        request: ProfileAnalysisRequest,
        snapshot: ChannelSnapshot,
    ) -> ProfileAnalysisResult:
        if request.channel_id != snapshot.channel_id:
            raise SnapshotError("request channel_id does not match snapshot")
        if request.as_of.astimezone(timezone.utc) != snapshot.as_of:
            raise SnapshotError("request as_of must match the immutable snapshot as_of")
        hashes = snapshot.hashes()
        if request.expected_snapshot_hash and request.expected_snapshot_hash != hashes["snapshot_hash"]:
            raise SnapshotError("expected_snapshot_hash does not match snapshot")

        diagnostics: list[dict[str, Any]] = []
        if not self.prior_catalog.production_eligible:
            diagnostics.append({
                "code": "UNVALIDATED_BOOTSTRAP_PRIOR",
                "severity": "warning",
                "message": "Audience and fallback estimates use a shadow-only prior catalog.",
                "prior_catalog_version": self.prior_catalog.version,
            })
        if self.model_bundle.manifest.artifacts and not self.model_bundle.production_eligible:
            diagnostics.append({
                "code": "MODEL_BUNDLE_NOT_PRODUCTION_ELIGIBLE",
                "severity": "warning",
                "message": "Trained artifacts are usable for Shadow evaluation but the complete bundle has unmet truth-validation gates.",
                "model_bundle_version": self.model_bundle.version,
                "field_status": self.model_bundle.manifest.field_status,
            })
        if snapshot.replay_quality != "current_exact":
            diagnostics.append({
                "code": "APPROXIMATE_HISTORICAL_REPLAY",
                "severity": "warning",
                "message": "Historical crawler input is not an exact reconstruction of the old Agent request.",
                "replay_quality": snapshot.replay_quality,
            })

        features = build_channel_features(snapshot, self.language_identifier)
        if features.comments.has_comments:
            diagnostics.append({
                "code": "TOP_COMMENTS_SAMPLE_USED",
                "severity": "info",
                "message": "First-page Top comments were used as biased public evidence, not Analytics truth.",
                "comment_sample_count": int(features.comments.numeric["comment_sample_count"]),
                "comment_unique_author_count": int(features.comments.numeric["comment_unique_author_count"]),
                "comment_page_count": int(features.comments.numeric["comment_page_count"]),
            })
        language_evidence = features.language
        creator_language = analyze_language(
            language_evidence,
            request.policy,
            model_version=(
                f"{self.model_bundle.version}:creator_language.fasttext+lexical-v2"
                if self.language_identifier is not None
                else "language-heuristic-v1"
            ),
            pretrained_model=self.language_identifier is not None,
            fallback_language=primary_language_hint(snapshot.channel),
        )
        category_baseline, _ = analyze_categories(
            snapshot,
            request.policy,
            channel_text=features.channel_text,
        )
        level_1_prediction = self.model_bundle.predict_text(
            "categories.level1", features.model_text("channel_categories")
        )
        level_2_prediction = self.model_bundle.predict_multilabel(
            "categories.level2", features.model_text("channel_categories"), top_k=20
        )
        channel_categories = resolve_categories(
            category_baseline,
            level_1_prediction,
            level_2_prediction,
            request.policy,
            self.model_bundle,
        )
        tag_baseline = analyze_tags(
            snapshot,
            channel_categories,
            creator_language,
            request.policy,
            full_text=features.full_text,
            comments=features.comments,
        )
        tag_prediction = self.model_bundle.predict_multilabel(
            "channel_tags.multilabel", features.model_text("channel_tags"), top_k=10
        )
        channel_tags = resolve_tags(
            tag_baseline,
            tag_prediction,
            request.policy,
            self.model_bundle,
        )
        gender_baseline = analyze_gender(
            snapshot,
            request.policy,
            comments=features.comments,
            identity=features.creator_evidence,
        )
        entity_prediction = self.model_bundle.predict_text(
            "gender.entity_type", features.model_text("creator_gender")
        )
        gender_prediction = self.model_bundle.predict_text(
            "gender.single_creator", features.model_text("creator_gender")
        )
        creator_gender = resolve_gender(
            gender_baseline,
            entity_prediction,
            gender_prediction,
            request.policy,
            self.model_bundle,
        )
        age_baseline = analyze_age(
            snapshot,
            channel_categories.value.get("level_1", "default")
            if isinstance(channel_categories.value, dict)
            else "default",
            self.prior_catalog,
            request.policy,
            identity=features.creator_evidence,
        )
        age_prediction = self.model_bundle.predict_text(
            "creator_age.ordinal", features.model_text("creator_age_range")
        )
        creator_age = resolve_age(
            age_baseline,
            age_prediction,
            request.policy,
            self.model_bundle,
        )
        country_baseline = analyze_country(
            snapshot,
            language_evidence.probabilities,
            self.prior_catalog,
            request.policy,
            channel_text=features.channel_text,
        )
        country_prediction = self.model_bundle.predict_text(
            "country.text", features.model_text("country")
        )
        country = resolve_country(
            country_baseline,
            country_prediction,
            request.policy,
            self.model_bundle,
        )
        audience_language_probabilities = language_evidence.probabilities or (
            {str(creator_language.value): 1.0} if creator_language.value else {}
        )
        audience_region, audience_language = analyze_audience_markets(
            audience_language_probabilities,
            country,
            self.prior_catalog,
            request.policy,
            snapshot=snapshot,
            category=channel_categories,
            tags=channel_tags,
            comments=features.comments,
        )
        audience_age_gender = analyze_audience_age_gender(
            channel_categories,
            self.prior_catalog,
            request.policy,
            snapshot=snapshot,
            tags=channel_tags,
            comments=features.comments,
            creator_gender=creator_gender,
        )
        active_ratio = analyze_active_ratio(
            snapshot,
            self.prior_catalog,
            request.policy,
            comments=features.comments,
        )

        facts = {
            "country": country,
            "creator_gender": creator_gender,
            "creator_age_range": creator_age,
            "creator_language": creator_language,
            "audience_region": audience_region,
            "audience_age_gender": audience_age_gender,
            "audience_language": audience_language,
            "active_subscriber_ratio": active_ratio,
            "channel_tags": channel_tags,
            "channel_categories": channel_categories,
        }
        if tuple(facts) != FACT_FIELDS:
            raise AssertionError("processor fact order drifted from the public contract")
        unavailable_count = sum(result.value is None for result in facts.values())
        status = "partial_failure" if unavailable_count else "completed_with_estimates"
        latest_content = max(
            (content.published_at for content in snapshot.contents if content.published_at),
            default=None,
        )
        result = ProfileAnalysisResult(
            channel_id=request.channel_id,
            input_url=request.input_url,
            analysis_status=status,
            snapshot={
                "as_of": snapshot.as_of.isoformat().replace("+00:00", "Z"),
                "channel_title": str(snapshot.channel.get("title") or ""),
                "channel_handle": str(snapshot.channel.get("handle") or ""),
                **features.hashes,
                "latest_content_at": latest_content.isoformat().replace("+00:00", "Z") if latest_content else None,
                "quality": snapshot.replay_quality,
                "content_count": len(snapshot.contents),
                "comment_page_count": int(features.comments.numeric["comment_page_count"]),
                "comment_sample_count": int(features.comments.numeric["comment_sample_count"]),
                "comment_unique_author_count": int(features.comments.numeric["comment_unique_author_count"]),
                "provenance": snapshot.provenance,
            },
            facts=facts,
            processor={
                "version": PROCESSOR_VERSION,
                "model_bundle_version": self.model_bundle.version,
                "model_bundle_hash": self.model_bundle.content_hash,
                "taxonomy_version": TAXONOMY_VERSION,
                "prior_catalog_version": self.prior_catalog.version,
                "prior_catalog_hash": self.prior_catalog.content_hash,
            },
            diagnostics=tuple(diagnostics),
        )
        result.validate()
        return result
