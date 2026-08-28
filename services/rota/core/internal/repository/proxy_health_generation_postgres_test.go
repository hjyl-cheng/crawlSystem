package repository

import (
	"context"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/proxylifecycle"
)

func TestAppliedHealthVerdictAdvancesGenerationButStaleEvidenceDoesNot(t *testing.T) {
	_, pool := newSourceInventoryPostgres(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		ALTER TABLE proxies
		  ADD COLUMN cooldown_until TIMESTAMPTZ,
		  ADD COLUMN last_health_check_at TIMESTAMPTZ,
		  ADD COLUMN last_health_success_at TIMESTAMPTZ,
		  ADD COLUMN last_health_verdict JSONB,
		  ADD COLUMN last_check TIMESTAMPTZ,
		  ADD COLUMN last_rota_youtube_status INTEGER,
		  ADD COLUMN last_rota_youtube_error TEXT,
		  ADD COLUMN last_rota_youtube_check TIMESTAMPTZ;
		CREATE TABLE proxy_health_checks (
		  id BIGSERIAL PRIMARY KEY,
		  proxy_id INTEGER NOT NULL REFERENCES proxies(id),
		  started_at TIMESTAMPTZ NOT NULL,
		  checked_at TIMESTAMPTZ NOT NULL,
		  base_result JSONB NOT NULL,
		  youtube_result JSONB NOT NULL,
		  verdict TEXT NOT NULL,
		  conclusive BOOLEAN NOT NULL,
		  control_path_healthy BOOLEAN NOT NULL,
		  previous_status TEXT NOT NULL,
		  resulting_status TEXT NOT NULL,
		  applied BOOLEAN NOT NULL,
		  transition_preserved BOOLEAN NOT NULL,
		  error TEXT
		)
	`); err != nil {
		t.Fatalf("extend health lifecycle fixture: %v", err)
	}

	repo := NewProxyRepository(&database.DB{Pool: pool})
	startedAt := time.Date(2026, 8, 26, 6, 50, 0, 0, time.UTC)
	evidence := proxylifecycle.HealthEvidence{
		StartedAt: startedAt,
		CheckedAt: startedAt.Add(3 * time.Second),
		Base: proxylifecycle.ProbeEvidence{
			Status: proxylifecycle.ProbeFailed,
			Error:  "closed pipe",
		},
		YouTube: proxylifecycle.ProbeEvidence{Status: proxylifecycle.ProbeNotRun},
		Verdict: proxylifecycle.Verdict{
			Kind:               proxylifecycle.FailureSoftUnreachable,
			Conclusive:         true,
			ControlPathHealthy: true,
		},
		Error: "closed pipe",
	}
	decision, applied, err := repo.ApplyHealthVerdict(
		ctx, 1, evidence, proxylifecycle.DefaultPolicy(),
	)
	if err != nil {
		t.Fatalf("apply health verdict: %v", err)
	}
	if !applied || decision.Status != proxylifecycle.StatusFailed {
		t.Fatalf("decision = %+v, applied = %v", decision, applied)
	}
	var generation int64
	if err := pool.QueryRow(ctx, `SELECT health_generation FROM proxies WHERE id=1`).Scan(&generation); err != nil {
		t.Fatalf("load health generation: %v", err)
	}
	if generation != 1 {
		t.Fatalf("health generation = %d, want 1", generation)
	}

	stale := evidence
	stale.StartedAt = startedAt.Add(-time.Minute)
	stale.CheckedAt = evidence.CheckedAt.Add(time.Minute)
	_, staleApplied, err := repo.ApplyHealthVerdict(
		ctx, 1, stale, proxylifecycle.DefaultPolicy(),
	)
	if err != nil {
		t.Fatalf("record stale health evidence: %v", err)
	}
	if staleApplied {
		t.Fatal("stale health evidence changed lifecycle state")
	}
	if err := pool.QueryRow(ctx, `SELECT health_generation FROM proxies WHERE id=1`).Scan(&generation); err != nil {
		t.Fatalf("reload health generation: %v", err)
	}
	if generation != 1 {
		t.Fatalf("health generation after stale evidence = %d, want 1", generation)
	}
}
