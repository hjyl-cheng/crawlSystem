from __future__ import annotations

from datetime import date, datetime, timezone
import unittest

from feature_engine.shared_features import (
    CollectionPrioritySignals,
    QuantileDistribution,
    ReferenceCatalog,
    SharedFeatureInputs,
    derive_recent_change_probability,
    derive_shared_features,
    subscriber_scale_cohort,
)


def distribution(
    feature_name: str,
    cohort_key: str,
    values: tuple[float, ...],
    *,
    sample_count: int = 100,
) -> QuantileDistribution:
    probabilities = tuple(index / (len(values) - 1) for index in range(len(values)))
    return QuantileDistribution(
        as_of_day=date(2026, 7, 20),
        cohort_key=cohort_key,
        feature_name=feature_name,
        sample_count=sample_count,
        probabilities=probabilities,
        values=values,
        method_version="v16-empirical-1",
    )


class QuantileDistributionTests(unittest.TestCase):
    def test_interpolates_percentiles_and_centers_ties(self) -> None:
        ranked = distribution("velocity", "all", (0.0, 10.0, 20.0))
        tied = distribution("velocity", "all", (5.0, 5.0, 5.0))

        self.assertEqual(ranked.percentile(5.0), 0.25)
        self.assertEqual(ranked.percentile(30.0), 1.0)
        self.assertEqual(tied.percentile(5.0), 0.5)

    def test_sparse_cohort_uses_the_global_distribution(self) -> None:
        catalog = ReferenceCatalog(
            (
                distribution("subscriber_velocity_ewma", "all", (0.0, 100.0, 200.0)),
                distribution(
                    "subscriber_velocity_ewma",
                    "subs:10k-100k",
                    (1000.0, 2000.0, 3000.0),
                    sample_count=3,
                ),
            ),
            minimum_cohort_size=20,
        )

        self.assertEqual(
            catalog.percentile(
                "subscriber_velocity_ewma",
                "subs:10k-100k",
                100.0,
            ),
            0.5,
        )


class SharedFeatureTests(unittest.TestCase):
    def test_subscriber_scale_cohorts_are_stable(self) -> None:
        self.assertEqual(subscriber_scale_cohort(None), "subs:unknown")
        self.assertEqual(subscriber_scale_cohort(999), "subs:0-1k")
        self.assertEqual(subscriber_scale_cohort(1_000), "subs:1k-10k")
        self.assertEqual(subscriber_scale_cohort(75_000), "subs:10k-100k")
        self.assertEqual(subscriber_scale_cohort(2_000_000), "subs:1m-10m")
        self.assertEqual(subscriber_scale_cohort(120_000_000), "subs:100m+")

    def test_derives_every_shared_feature_from_references_and_signals(self) -> None:
        cohort = "subs:10k-100k"
        catalog = ReferenceCatalog(
            (
                distribution("subscriber_count", "all", (0.0, 50_000.0, 100_000.0)),
                distribution("subscriber_velocity_ewma", "all", (0.0, 50.0, 100.0)),
                distribution("subscriber_velocity_ewma", cohort, (0.0, 50.0, 100.0)),
                distribution("view_velocity_ewma", "all", (0.0, 500.0, 1000.0)),
                distribution("view_velocity_ewma", cohort, (0.0, 500.0, 1000.0)),
            ),
        )
        result = derive_shared_features(
            SharedFeatureInputs(
                subscriber_count=80_000,
                subscriber_velocity_ewma=70.0,
                view_velocity_ewma=600.0,
                recent30_video_count=6,
                last_publish_at=datetime(2026, 7, 20, tzinfo=timezone.utc),
                about_identity_observed=True,
                about_observed=True,
                discovery_observed=True,
                recent_sampling_observed=False,
                agent_observed=False,
            ),
            references=catalog,
            signals=CollectionPrioritySignals(user_query_demand=0.6, manual_priority=0.9),
            observed_at=datetime(2026, 7, 21, tzinfo=timezone.utc),
        )

        self.assertAlmostEqual(result.subscriber_size_percentile or 0, 0.8)
        self.assertAlmostEqual(result.subscriber_growth_percentile or 0, 0.7)
        self.assertAlmostEqual(result.view_growth_percentile or 0, 0.6)
        self.assertAlmostEqual(result.growth_momentum or 0, 0.65)
        self.assertAlmostEqual(result.channel_activity, 0.7175)
        self.assertAlmostEqual(result.data_incompleteness, 0.4)
        self.assertAlmostEqual(result.collection_priority, 0.689375)
        self.assertEqual(result.reference_distribution_version, "2026-07-20:v16-empirical-1")

    def test_missing_references_use_explicit_neutral_fallbacks(self) -> None:
        result = derive_shared_features(
            SharedFeatureInputs(),
            references=ReferenceCatalog(()),
            signals=CollectionPrioritySignals(),
            observed_at=datetime(2026, 7, 21, tzinfo=timezone.utc),
        )

        self.assertIsNone(result.subscriber_growth_percentile)
        self.assertIsNone(result.view_growth_percentile)
        self.assertIsNone(result.growth_momentum)
        self.assertEqual(result.data_incompleteness, 1.0)
        self.assertAlmostEqual(result.channel_activity, 0.1875)
        self.assertAlmostEqual(result.collection_priority, 0.346875)
        self.assertIsNone(result.reference_distribution_version)

    def test_recent_change_probability_tracks_shared_activity(self) -> None:
        probability = derive_recent_change_probability(
            view_change=0.8,
            engagement_change=0.4,
            upload_change=0.2,
            channel_activity=0.6,
        )

        self.assertAlmostEqual(probability or 0.0, 0.56)
        self.assertIsNone(
            derive_recent_change_probability(
                view_change=None,
                engagement_change=None,
                upload_change=None,
                channel_activity=0.6,
            )
        )


if __name__ == "__main__":
    unittest.main()
