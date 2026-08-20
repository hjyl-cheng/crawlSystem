package repository

import (
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/proxylifecycle"
)

func TestHealthEvidenceStartedBeforeRestoreBarrierIsStale(t *testing.T) {
	startedAt := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	restoredAt := startedAt.Add(time.Minute)
	completedAt := restoredAt.Add(time.Minute)
	evidence := proxylifecycle.HealthEvidence{StartedAt: startedAt, CheckedAt: completedAt}

	if !healthEvidenceIsStale(evidence, nil, &restoredAt) {
		t.Fatal("pre-restore health check was allowed to change restored lifecycle")
	}
}

func TestLaterStartedHealthEvidencePassesBarrier(t *testing.T) {
	restoredAt := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	evidence := proxylifecycle.HealthEvidence{
		StartedAt: restoredAt.Add(time.Second),
		CheckedAt: restoredAt.Add(2 * time.Second),
	}

	if healthEvidenceIsStale(evidence, nil, &restoredAt) {
		t.Fatal("post-restore health check was rejected")
	}
}
