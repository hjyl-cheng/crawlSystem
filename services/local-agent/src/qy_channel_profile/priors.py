from __future__ import annotations

import json
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any

from .errors import PriorCatalogError
from .hashing import sha256_json


@dataclass(frozen=True)
class PriorCatalog:
    data: dict[str, Any]

    @classmethod
    def load(cls, path: str | Path | None = None) -> "PriorCatalog":
        catalog_path = Path(path) if path else files("qy_channel_profile").joinpath(
            "catalogs/bootstrap_shadow_v1.json"
        )
        with catalog_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        catalog = cls(data=data)
        catalog.validate()
        return catalog

    @property
    def version(self) -> str:
        return str(self.data["version"])

    @property
    def production_eligible(self) -> bool:
        return bool(self.data.get("production_eligible"))

    @property
    def content_hash(self) -> str:
        return sha256_json(self.data)

    def validate(self) -> None:
        required = (
            "version", "production_eligible", "provenance", "language_markets",
            "age_gender", "audience_market_model", "age_gender_adjustments",
            "active_audience", "comment_evidence",
        )
        missing = [key for key in required if key not in self.data]
        if missing:
            raise PriorCatalogError(f"prior catalog missing: {', '.join(missing)}")
        provenance = self.data["provenance"]
        for key in ("source", "license", "quality_grade", "intended_use"):
            if not str(provenance.get(key) or "").strip():
                raise PriorCatalogError(f"prior provenance.{key} is required")
        for language, values in self.data["language_markets"].items():
            self._validate_distribution(f"language_markets.{language}", values)
        for category, values in self.data["age_gender"].items():
            self._validate_distribution(f"age_gender.{category}", values)
        for category, genders in (self.data.get("age_gender_by_creator_gender") or {}).items():
            if not isinstance(genders, dict):
                raise PriorCatalogError(f"age_gender_by_creator_gender.{category} must be an object")
            for gender, values in genders.items():
                self._validate_distribution(f"age_gender_by_creator_gender.{category}.{gender}", values)
        market = self.data["audience_market_model"]
        for language, values in market["language_propagation"].items():
            self._validate_distribution(f"audience_market_model.language_propagation.{language}", values)
        for region, values in market["region_languages"].items():
            self._validate_distribution(f"audience_market_model.region_languages.{region}", values)
        for name in (
            "content_language_weight", "region_language_weight", "bridge_language_weight",
            "regional_topic_max_blend",
        ):
            value = float(market[name])
            if not 0 <= value <= 1:
                raise PriorCatalogError(f"audience_market_model.{name} must be in [0, 1]")
        if abs(
            float(market["content_language_weight"])
            + float(market["region_language_weight"])
            + float(market["bridge_language_weight"])
            - 1.0
        ) > 1e-6:
            raise PriorCatalogError("audience market language blend weights must sum to 1")

        active = self.data["active_audience"]
        required_active = (
            "window_days", "maximum_content_age_days", "growth_curve_exponent",
            "maximum_projection_multiplier", "legacy_window_share",
            "subscribed_view_share", "additional_upload_unique_share",
            "cross_format_overlap", "winsor_mad_multiplier",
            "winsor_minimum_relative_spread", "size_adjustment",
            "cohort_prior_center", "shrinkage_observations",
            "estimated_subscriber_max_evidence_weight", "inactivity_half_life_days",
            "engagement_reference", "engagement_signal_weight",
            "engagement_adjustment_bounds", "minimum_ratio", "maximum_ratio",
        )
        missing_active = [key for key in required_active if key not in active]
        if missing_active:
            raise PriorCatalogError(
                "active_audience missing: " + ", ".join(missing_active)
            )
        for name in (
            "growth_curve_exponent", "legacy_window_share", "subscribed_view_share",
            "additional_upload_unique_share",
        ):
            values = active[name]
            if set(values) != {"short", "longform", "live"}:
                raise PriorCatalogError(f"active_audience.{name} must define all content formats")
            if any(float(value) < 0 for value in values.values()):
                raise PriorCatalogError(f"active_audience.{name} cannot contain negative values")
        for name in ("size_adjustment", "cohort_prior_center"):
            values = active[name]
            if set(values) != {"under_50000", "50000_to_500000", "over_500000"}:
                raise PriorCatalogError(f"active_audience.{name} must define all subscriber tiers")
            if any(float(value) <= 0 for value in values.values()):
                raise PriorCatalogError(f"active_audience.{name} values must be positive")
        for name in (
            "cross_format_overlap", "winsor_minimum_relative_spread",
            "estimated_subscriber_max_evidence_weight", "engagement_signal_weight",
        ):
            if not 0 <= float(active[name]) <= 1:
                raise PriorCatalogError(f"active_audience.{name} must be in [0, 1]")
        engagement_bounds = [float(value) for value in active["engagement_adjustment_bounds"]]
        if len(engagement_bounds) != 2 or not 0 < engagement_bounds[0] <= 1 <= engagement_bounds[1]:
            raise PriorCatalogError(
                "active_audience.engagement_adjustment_bounds must straddle 1"
            )
        if not 0 <= float(active["minimum_ratio"]) < float(active["maximum_ratio"]) <= 100:
            raise PriorCatalogError("active_audience ratio bounds are invalid")

        comments = self.data["comment_evidence"]
        if comments.get("calibration_status") != "uncalibrated_top_comments":
            raise PriorCatalogError("comment_evidence must explicitly remain uncalibrated")
        for section_name in ("audience_market", "age_gender", "active_audience"):
            if not isinstance(comments.get(section_name), dict):
                raise PriorCatalogError(f"comment_evidence.{section_name} is required")
        bounded = (
            ("audience_market", "language_max_blend"),
            ("audience_market", "region_max_blend"),
            ("age_gender", "age_max_blend"),
            ("age_gender", "gender_max_blend"),
            ("age_gender", "joint_max_blend"),
            ("active_audience", "maximum_evidence_weight"),
        )
        for section_name, key in bounded:
            value = float(comments[section_name][key])
            if not 0 <= value <= 1:
                raise PriorCatalogError(
                    f"comment_evidence.{section_name}.{key} must be in [0, 1]"
                )
        positive = (
            ("audience_market", "language_shrinkage_authors"),
            ("audience_market", "region_shrinkage_authors"),
            ("age_gender", "age_shrinkage_authors"),
            ("age_gender", "gender_shrinkage_authors"),
            ("age_gender", "joint_shrinkage_authors"),
            ("active_audience", "shrinkage_authors"),
            ("active_audience", "meaningful_ratio_reference"),
            ("active_audience", "returning_author_ratio_reference"),
            ("active_audience", "unique_author_ratio_reference"),
            ("active_audience", "component_log_cap"),
        )
        for section_name, key in positive:
            if float(comments[section_name][key]) <= 0:
                raise PriorCatalogError(
                    f"comment_evidence.{section_name}.{key} must be positive"
                )
        if float(comments["active_audience"]["maximum_adjustment_log"]) < 0:
            raise PriorCatalogError(
                "comment_evidence.active_audience.maximum_adjustment_log cannot be negative"
            )

    @staticmethod
    def _validate_distribution(name: str, values: Any) -> None:
        if not isinstance(values, dict) or not values:
            raise PriorCatalogError(f"{name} must be a non-empty object")
        parsed = [float(value) for value in values.values()]
        if any(value < 0 for value in parsed) or sum(parsed) <= 0:
            raise PriorCatalogError(f"{name} has invalid weights")

    def language_market(self, language: str) -> dict[str, float]:
        markets = self.data["language_markets"]
        selected = markets.get(language) or markets["Other"]
        return {str(key): float(value) for key, value in selected.items()}

    def has_creator_conditioned_age_gender(self, category: str, creator_gender: str | None) -> bool:
        if not creator_gender:
            return False
        selected = ((self.data.get("age_gender_by_creator_gender") or {}).get(category) or {}).get(creator_gender)
        return isinstance(selected, dict) and bool(selected)

    def age_gender(self, category: str, creator_gender: str | None = None) -> dict[str, float]:
        if self.has_creator_conditioned_age_gender(category, creator_gender):
            selected = self.data["age_gender_by_creator_gender"][category][creator_gender]
            return {str(key): float(value) for key, value in selected.items()}
        values = self.data["age_gender"].get(category) or self.data["age_gender"]["default"]
        return {str(key): float(value) for key, value in values.items()}

    def creator_age_interval(self, category: str) -> tuple[int, int]:
        intervals = self.data.get("creator_age_intervals", {})
        value = intervals.get(category) or intervals.get("default") or [25, 44]
        return int(value[0]), int(value[1])

    def audience_market(self) -> dict[str, Any]:
        return dict(self.data["audience_market_model"])

    def language_propagation(self, language: str) -> dict[str, float]:
        values = self.data["audience_market_model"]["language_propagation"]
        selected = values.get(language) or {
            language: 0.85,
            "English": 0.10,
            "Other": 0.05,
        }
        return {str(key): float(value) for key, value in selected.items()}

    def region_languages(self, region: str) -> dict[str, float]:
        values = self.data["audience_market_model"]["region_languages"]
        selected = values.get(region) or values["Other"]
        return {str(key): float(value) for key, value in selected.items()}

    def category_cross_language_reach(self, category: str) -> float:
        values = self.data["audience_market_model"]["category_cross_language_reach"]
        return float(values.get(category, values["default"]))

    def age_gender_adjustments(self) -> dict[str, Any]:
        return dict(self.data["age_gender_adjustments"])

    def active_audience(self) -> dict[str, Any]:
        return dict(self.data["active_audience"])

    def comment_evidence(self) -> dict[str, Any]:
        return dict(self.data["comment_evidence"])
