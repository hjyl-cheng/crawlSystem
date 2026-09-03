package repository

import (
	"context"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/proxylifecycle"
)

func TestHealthVerdictAdvancesGenerationButIgnoresStaleOrBoundEvidence(t *testing.T) {
	_, pool := newSourceInventoryPostgres(t)
	ctx := context.Background()
	extendLiveLeaseFixture(t, pool)
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

	if _, err := pool.Exec(ctx, `
		INSERT INTO proxy_running_slots (
		  slot_name,proxy_id,current_lease_id,lease_until
		) VALUES ('bound-health',2,'bound-health-lease',NOW()+INTERVAL '1 hour');
		INSERT INTO proxy_control_leases (lease_id,slot_name,status,lease_until)
		VALUES ('bound-health-lease','bound-health','active',NOW()+INTERVAL '1 hour')
	`); err != nil {
		t.Fatalf("bind proxy fixture: %v", err)
	}
	boundEvidence := evidence
	boundEvidence.StartedAt = evidence.CheckedAt.Add(time.Minute)
	boundEvidence.CheckedAt = boundEvidence.StartedAt.Add(time.Second)
	decision, boundApplied, err := repo.ApplyHealthVerdict(
		ctx, 2, boundEvidence, proxylifecycle.DefaultPolicy(),
	)
	if err != nil {
		t.Fatalf("record bound health evidence: %v", err)
	}
	if boundApplied || decision.Status != proxylifecycle.StatusActive {
		t.Fatalf("bound decision = %+v, applied = %v", decision, boundApplied)
	}
	var boundStatus string
	if err := pool.QueryRow(ctx, `
		SELECT status,health_generation FROM proxies WHERE id=2
	`).Scan(&boundStatus, &generation); err != nil {
		t.Fatalf("reload bound lifecycle: %v", err)
	}
	if boundStatus != "active" || generation != 0 {
		t.Fatalf("bound lifecycle = status %q generation %d", boundStatus, generation)
	}
}

func TestHealthVerdictObservesConcurrentSlotBindingAfterProxyLockWait(t *testing.T) {
	_, pool := newSourceInventoryPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	extendLiveLeaseFixture(t, pool)
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

	binding, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin slot binding: %v", err)
	}
	defer func() { _ = binding.Rollback(ctx) }()
	if _, err := binding.Exec(ctx, `
		INSERT INTO proxy_running_slots (
		  slot_name,proxy_id,current_lease_id,lease_until
		) VALUES ('binding-race',1,'binding-race-lease',NOW()+INTERVAL '1 hour');
		INSERT INTO proxy_control_leases (lease_id,slot_name,status,lease_until)
		VALUES ('binding-race-lease','binding-race','active',NOW()+INTERVAL '1 hour')
	`); err != nil {
		t.Fatalf("stage slot binding: %v", err)
	}

	type verdictResult struct {
		decision proxylifecycle.Decision
		applied  bool
		err      error
	}
	resultCh := make(chan verdictResult, 1)
	startedAt := time.Now().UTC()
	evidence := proxylifecycle.HealthEvidence{
		StartedAt: startedAt,
		CheckedAt: startedAt.Add(time.Second),
		Base: proxylifecycle.ProbeEvidence{
			Status: proxylifecycle.ProbeFailed,
			Error:  "connection refused",
		},
		YouTube: proxylifecycle.ProbeEvidence{Status: proxylifecycle.ProbeNotRun},
		Verdict: proxylifecycle.Verdict{
			Kind:               proxylifecycle.FailureHardUnreachable,
			Conclusive:         true,
			ControlPathHealthy: true,
		},
		Error: "connection refused",
	}
	repo := NewProxyRepository(&database.DB{Pool: pool})
	go func() {
		decision, applied, err := repo.ApplyHealthVerdict(
			ctx, 1, evidence, proxylifecycle.DefaultPolicy(),
		)
		resultCh <- verdictResult{decision: decision, applied: applied, err: err}
	}()

	select {
	case result := <-resultCh:
		t.Fatalf("health verdict passed an uncommitted slot binding: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}
	if err := binding.Commit(ctx); err != nil {
		t.Fatalf("commit slot binding: %v", err)
	}

	var result verdictResult
	select {
	case result = <-resultCh:
	case <-ctx.Done():
		t.Fatalf("health verdict did not finish after slot binding committed: %v", ctx.Err())
	}
	if result.err != nil {
		t.Fatalf("record health evidence after slot binding: %v", result.err)
	}
	if result.applied || result.decision.Status != proxylifecycle.StatusActive {
		t.Fatalf("bound verdict result = %+v", result)
	}

	var status string
	var generation int64
	if err := pool.QueryRow(ctx, `
		SELECT status,health_generation FROM proxies WHERE id=1
	`).Scan(&status, &generation); err != nil {
		t.Fatalf("load lifecycle after binding race: %v", err)
	}
	if status != "active" || generation != 0 {
		t.Fatalf("lifecycle after binding race = status %q generation %d", status, generation)
	}
}
