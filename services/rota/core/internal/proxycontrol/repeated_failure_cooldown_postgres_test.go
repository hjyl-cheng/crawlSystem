package proxycontrol

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/proxylifecycle"
	"github.com/jackc/pgx/v5/pgxpool"
)

func quarantineForTest(
	t *testing.T,
	manager *Manager,
	pool *pgxpool.Pool,
	proxyID int,
	kind proxylifecycle.FailureKind,
	eventKind string,
) time.Duration {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin quarantine: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := manager.quarantineProxy(ctx, tx, proxyID, kind, "test failure", eventKind); err != nil {
		t.Fatalf("quarantine proxy %d: %v", proxyID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit quarantine: %v", err)
	}
	var seconds float64
	if err := pool.QueryRow(ctx, `
		SELECT EXTRACT(EPOCH FROM next_health_check_at-NOW())::float8 FROM proxies WHERE id=$1
	`, proxyID).Scan(&seconds); err != nil {
		t.Fatalf("load next health check: %v", err)
	}
	return time.Duration(math.Round(seconds/60)) * time.Minute
}

func TestTaskObservationQuarantineEscalatesUntilTaskSuccess(t *testing.T) {
	manager, pool := newProxyControlPostgresWithOptions(t, func(options *Options) {
		options.NetworkCooldown = 5 * time.Minute
		options.FailureCooldown = 30 * time.Minute
		options.RepeatedFailureWindow = 24 * time.Hour
		options.MaxRepeatedFailureCooldown = 4 * time.Hour
	})
	ctx := context.Background()
	proxyID := insertControlProxy(t, pool, "repeat-failure.example:8080", 10)

	for index, want := range []time.Duration{5 * time.Minute, 10 * time.Minute, 20 * time.Minute} {
		got := quarantineForTest(t, manager, pool, proxyID,
			proxylifecycle.FailureHardUnreachable, taskObservationQuarantineEvent)
		if got != want {
			t.Fatalf("quarantine %d cooldown = %s, want %s", index+1, got, want)
		}
	}

	// A health probe verdict does not reset escalation; a successful Task does.
	if _, err := pool.Exec(ctx, `
		UPDATE proxies SET status='active',last_task_success_at=NOW() WHERE id=$1
	`, proxyID); err != nil {
		t.Fatalf("record task success: %v", err)
	}
	if got := quarantineForTest(t, manager, pool, proxyID,
		proxylifecycle.FailureHardUnreachable, taskObservationQuarantineEvent); got != 5*time.Minute {
		t.Fatalf("cooldown after task success = %s, want base 5m", got)
	}
	if got := quarantineForTest(t, manager, pool, proxyID,
		proxylifecycle.FailureYouTubeUnusable, taskObservationQuarantineEvent); got != time.Hour {
		t.Fatalf("second youtube failure after success = %s, want 1h", got)
	}
}

func TestTaskObservationQuarantineCapsAndIgnoresOldOrOtherFailures(t *testing.T) {
	manager, pool := newProxyControlPostgresWithOptions(t, func(options *Options) {
		options.NetworkCooldown = 5 * time.Minute
		options.RepeatedFailureWindow = time.Hour
		options.MaxRepeatedFailureCooldown = 4 * time.Hour
	})
	ctx := context.Background()
	oldFailures := insertControlProxy(t, pool, "repeat-old.example:8080", 10)
	manyFailures := insertControlProxy(t, pool, "repeat-many.example:8080", 10)

	if _, err := pool.Exec(ctx, `
		INSERT INTO proxy_lifecycle_events (proxy_id,occurred_at,event_kind,previous_status,resulting_status,reason)
		SELECT $1::int,NOW()-INTERVAL '2 hours',$3::text,'active','failed','old'
		FROM generate_series(1,5)
		UNION ALL
		SELECT $1::int,NOW()-INTERVAL '5 minutes','crawler_failure','active','failed','other kind'
		FROM generate_series(1,5)
		UNION ALL
		SELECT $2::int,NOW()-INTERVAL '5 minutes',$3::text,'active','failed','recent'
		FROM generate_series(1,10)
	`, oldFailures, manyFailures, taskObservationQuarantineEvent); err != nil {
		t.Fatalf("insert prior lifecycle events: %v", err)
	}

	if got := quarantineForTest(t, manager, pool, oldFailures,
		proxylifecycle.FailureHardUnreachable, taskObservationQuarantineEvent); got != 5*time.Minute {
		t.Fatalf("cooldown with only old or other failures = %s, want base 5m", got)
	}
	if got := quarantineForTest(t, manager, pool, manyFailures,
		proxylifecycle.FailureHardUnreachable, taskObservationQuarantineEvent); got != 4*time.Hour {
		t.Fatalf("cooldown after many recent failures = %s, want cap 4h", got)
	}
	// Crawler failure reports keep their existing fixed cooldown.
	if got := quarantineForTest(t, manager, pool, manyFailures,
		proxylifecycle.FailureHardUnreachable, "crawler_failure"); got != 5*time.Minute {
		t.Fatalf("crawler failure report cooldown = %s, want base 5m", got)
	}
}
