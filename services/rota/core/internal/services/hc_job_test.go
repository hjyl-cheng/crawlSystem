package services

import (
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
)

func TestHCJobCleanupRetainsInFlightAndRecentJobs(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	old := now.Add(-time.Hour)
	recent := now.Add(-time.Minute)
	store := &HCJobStore{jobs: map[string]*HCJob{
		"pending":       {ID: "pending", Status: HCJobPending, StartedAt: old, UpdatedAt: old},
		"running":       {ID: "running", Status: HCJobRunning, StartedAt: old, UpdatedAt: old},
		"old-done":      {ID: "old-done", Status: HCJobDone, StartedAt: old, UpdatedAt: old, FinishedAt: &old},
		"recent-failed": {ID: "recent-failed", Status: HCJobFailed, StartedAt: old, UpdatedAt: recent, FinishedAt: &recent},
	}}

	store.mu.Lock()
	store.cleanupLocked(now)
	store.mu.Unlock()
	for _, retained := range []string{"pending", "running", "recent-failed"} {
		if _, exists := store.jobs[retained]; !exists {
			t.Fatalf("job %q was removed", retained)
		}
	}
	if _, exists := store.jobs["old-done"]; exists {
		t.Fatal("expired completed job was retained")
	}
}

func TestHCJobReadsReturnDeepSnapshots(t *testing.T) {
	finishedAt := time.Now()
	responseTime := 250
	errorMessage := "connection refused"
	failureKind := "connectivity"
	store := &HCJobStore{jobs: map[string]*HCJob{
		"job": {
			ID:         "job",
			PoolID:     5,
			Status:     HCJobDone,
			FinishedAt: &finishedAt,
			Results: []models.ProxyTestResult{{
				ID:           42,
				ResponseTime: &responseTime,
				Error:        &errorMessage,
				FailureKind:  &failureKind,
			}},
		},
	}}

	first, ok := store.Get("job")
	if !ok {
		t.Fatal("job not found")
	}
	first.Status = HCJobFailed
	first.Results[0].ID = 99
	*first.Results[0].ResponseTime = 999
	*first.Results[0].Error = "mutated"
	*first.Results[0].FailureKind = "mutated"
	*first.FinishedAt = time.Time{}

	second, _ := store.Get("job")
	if second.Status != HCJobDone || second.Results[0].ID != 42 || second.FinishedAt.IsZero() ||
		*second.Results[0].ResponseTime != 250 || *second.Results[0].Error != "connection refused" ||
		*second.Results[0].FailureKind != "connectivity" {
		t.Fatalf("stored job was mutated through snapshot: %#v", second)
	}
}

func TestHCJobCreateReturnsInitialTotal(t *testing.T) {
	store := &HCJobStore{jobs: make(map[string]*HCJob)}
	created := store.Create(7, "pool", "https://example.com", 4, 23)
	if created.Total != 23 {
		t.Fatalf("created Total = %d, want 23", created.Total)
	}
	stored, ok := store.Get(created.ID)
	if !ok || stored.Total != 23 {
		t.Fatalf("stored job = %#v, found=%t", stored, ok)
	}
}

func TestHCJobListIsNewestFirstAndReturnsSnapshots(t *testing.T) {
	store := &HCJobStore{jobs: map[string]*HCJob{
		"old": {ID: "old", PoolID: 1, StartedAt: time.Unix(1, 0)},
		"new": {ID: "new", PoolID: 1, StartedAt: time.Unix(2, 0)},
	}}
	jobs := store.ListByPool(1)
	if len(jobs) != 2 || jobs[0].ID != "new" || jobs[1].ID != "old" {
		t.Fatalf("jobs = %#v", jobs)
	}
	jobs[0].ID = "mutated"
	if original, _ := store.Get("new"); original.ID != "new" {
		t.Fatal("ListByPool returned mutable store state")
	}
}
