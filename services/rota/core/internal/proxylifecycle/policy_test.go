package proxylifecycle

import (
	"testing"
	"time"
)

func TestPolicyStartsHardFailureEpisodeAtFirstConclusiveFailure(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	decision := DefaultPolicy().Decide(now, Snapshot{Status: StatusActive}, Verdict{
		Kind:               FailureHardUnreachable,
		Conclusive:         true,
		ControlPathHealthy: true,
	})

	assertStatus(t, decision, StatusFailed)
	assertTime(t, "failed since", decision.FailedSince, now)
	assertTime(t, "continuous failed since", decision.ContinuousFailedSince, now)
	assertTime(t, "next check", decision.NextHealthCheckAt, now.Add(15*time.Minute))
	if decision.FailureKind != FailureHardUnreachable {
		t.Fatalf("failure kind = %q", decision.FailureKind)
	}
}

func TestPolicyUsesFixedRecheckPointsInsteadOfFailureCounts(t *testing.T) {
	started := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	now := started.Add(time.Hour)
	decision := DefaultPolicy().Decide(now, Snapshot{
		Status:      StatusFailed,
		FailedSince: &started,
		FailureKind: FailureHardUnreachable,
	}, Verdict{
		Kind:               FailureHardUnreachable,
		Conclusive:         true,
		ControlPathHealthy: true,
	})

	assertStatus(t, decision, StatusFailed)
	assertTime(t, "failed since", decision.FailedSince, started)
	assertTime(t, "next check", decision.NextHealthCheckAt, started.Add(6*time.Hour))
}

func TestPolicyClearsFailureEpisodeOnHealthSuccess(t *testing.T) {
	started := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	now := started.Add(2 * time.Hour)
	decision := DefaultPolicy().Decide(now, Snapshot{
		Status:      StatusFailed,
		FailedSince: &started,
		FailureKind: FailureSoftUnreachable,
	}, HealthyVerdict())

	assertStatus(t, decision, StatusActive)
	if decision.FailedSince != nil || decision.FailureKind != FailureNone || decision.NextHealthCheckAt != nil {
		t.Fatalf("healthy transition retained failure episode: %#v", decision)
	}
}

func TestPolicyRestartsObservationWhenFailureKindChanges(t *testing.T) {
	started := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	now := started.Add(5 * time.Hour)
	decision := DefaultPolicy().Decide(now, Snapshot{
		Status:                StatusFailed,
		FailedSince:           &started,
		ContinuousFailedSince: &started,
		FailureKind:           FailureHardUnreachable,
	}, Verdict{
		Kind:               FailureYouTubeUnusable,
		Conclusive:         true,
		ControlPathHealthy: true,
	})

	assertStatus(t, decision, StatusFailed)
	assertTime(t, "failed since", decision.FailedSince, now)
	assertTime(t, "continuous failed since", decision.ContinuousFailedSince, started)
	assertTime(t, "next check", decision.NextHealthCheckAt, now.Add(30*time.Minute))
}

func TestPolicyArchivesContinuousFailureEvenWhenFailureKindKeepsChanging(t *testing.T) {
	started := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	now := started.Add(72 * time.Hour)
	decision := DefaultPolicy().Decide(now, Snapshot{
		Status:                StatusFailed,
		FailedSince:           timePointer(now.Add(-time.Hour)),
		ContinuousFailedSince: &started,
		FailureKind:           FailureSoftUnreachable,
	}, Verdict{
		Kind:               FailureYouTubeUnusable,
		Conclusive:         true,
		ControlPathHealthy: true,
	})

	assertStatus(t, decision, StatusArchived)
	if decision.ArchiveReason != "continuous_unusable" {
		t.Fatalf("archive reason = %q", decision.ArchiveReason)
	}
}

func TestPolicyKeepsIncidentFenceAndSchedulesInconclusiveActiveRevalidation(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	decision := DefaultPolicy().Decide(now, Snapshot{
		Status:               StatusActive,
		RevalidationRequired: true,
	}, Verdict{})

	assertStatus(t, decision, StatusActive)
	if !decision.RevalidationRequired {
		t.Fatal("inconclusive probe cleared incident revalidation fence")
	}
	assertTime(t, "next check", decision.NextHealthCheckAt, now.Add(30*time.Minute))
}

func TestPolicyLeavesLifecycleUnchangedForInconclusiveProbe(t *testing.T) {
	started := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	now := started.Add(6 * time.Hour)
	decision := DefaultPolicy().Decide(now, Snapshot{
		Status:      StatusFailed,
		FailedSince: &started,
		FailureKind: FailureHardUnreachable,
	}, Verdict{Kind: FailureHardUnreachable, Conclusive: false})

	assertStatus(t, decision, StatusFailed)
	assertTime(t, "failed since", decision.FailedSince, started)
	assertTime(t, "next check", decision.NextHealthCheckAt, now.Add(30*time.Minute))
	if decision.ArchivedAt != nil {
		t.Fatal("inconclusive probe archived proxy")
	}
}

func TestPolicyRetriesInconclusivePendingValidation(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	decision := DefaultPolicy().Decide(now, Snapshot{Status: StatusIdle}, Verdict{})

	assertStatus(t, decision, StatusIdle)
	assertTime(t, "next check", decision.NextHealthCheckAt, now.Add(30*time.Minute))
}

func TestPolicyArchivesOnlyAfterWindowAndFinalConclusiveFailure(t *testing.T) {
	started := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	policy := DefaultPolicy()

	beforeFinal := policy.Decide(started.Add(6*time.Hour-time.Second), Snapshot{
		Status:      StatusFailed,
		FailedSince: &started,
		FailureKind: FailureHardUnreachable,
	}, Verdict{Kind: FailureHardUnreachable, Conclusive: true, ControlPathHealthy: true})
	assertStatus(t, beforeFinal, StatusFailed)

	finalAt := started.Add(6 * time.Hour)
	final := policy.Decide(finalAt, Snapshot{
		Status:      StatusFailed,
		FailedSince: &started,
		FailureKind: FailureHardUnreachable,
	}, Verdict{Kind: FailureHardUnreachable, Conclusive: true, ControlPathHealthy: true})
	assertStatus(t, final, StatusArchived)
	assertTime(t, "archived at", final.ArchivedAt, finalAt)
	if final.ArchiveReason != string(FailureHardUnreachable) {
		t.Fatalf("archive reason = %q", final.ArchiveReason)
	}
}

func TestPolicyDoesNotArchiveWithoutHealthyControlPath(t *testing.T) {
	started := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	now := started.Add(72 * time.Hour)
	decision := DefaultPolicy().Decide(now, Snapshot{
		Status:      StatusFailed,
		FailedSince: &started,
		FailureKind: FailureYouTubeUnusable,
	}, Verdict{Kind: FailureYouTubeUnusable, Conclusive: true, ControlPathHealthy: false})

	assertStatus(t, decision, StatusFailed)
	if decision.ArchivedAt != nil {
		t.Fatal("unhealthy control path archived proxy")
	}
}

func TestPolicyNeverAutomaticallyChangesArchivedProxy(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	decision := DefaultPolicy().Decide(now, Snapshot{Status: StatusArchived}, HealthyVerdict())

	assertStatus(t, decision, StatusArchived)
	if decision.NextHealthCheckAt != nil {
		t.Fatal("archived proxy was scheduled for another health check")
	}
}

func assertStatus(t *testing.T, decision Decision, want Status) {
	t.Helper()
	if decision.Status != want {
		t.Fatalf("status = %q, want %q (decision=%#v)", decision.Status, want, decision)
	}
}

func assertTime(t *testing.T, label string, got *time.Time, want time.Time) {
	t.Helper()
	if got == nil || !got.Equal(want) {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
}

func timePointer(value time.Time) *time.Time { return &value }
