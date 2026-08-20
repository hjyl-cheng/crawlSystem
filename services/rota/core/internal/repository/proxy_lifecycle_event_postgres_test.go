package repository

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/proxylifecycle"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLifecycleEventAndTransitionMarkerCommitAtomically(t *testing.T) {
	pool := newLifecycleEventPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		healthCheckID := int64(1)
		return insertLifecycleEvent(
			ctx, tx, 1, &healthCheckID, time.Now().UTC(), "health_verdict",
			proxylifecycle.StatusIdle, proxylifecycle.StatusActive, "healthy", nil,
		)
	})
	if err != nil {
		t.Fatalf("insert lifecycle event: %v", err)
	}

	var preserved bool
	var eventCount int
	if err := pool.QueryRow(ctx, `
		SELECT transition_preserved FROM proxy_health_checks WHERE id=1
	`).Scan(&preserved); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM proxy_lifecycle_events WHERE health_check_id=1
	`).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if !preserved || eventCount != 1 {
		t.Fatalf("transition_preserved=%v events=%d", preserved, eventCount)
	}
}

func TestLifecycleEventMarkerMismatchRollsBackEvent(t *testing.T) {
	pool := newLifecycleEventPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		healthCheckID := int64(1)
		return insertLifecycleEvent(
			ctx, tx, 2, &healthCheckID, time.Now().UTC(), "health_verdict",
			proxylifecycle.StatusIdle, proxylifecycle.StatusActive, "healthy", nil,
		)
	})
	if err == nil {
		t.Fatal("mismatched proxy and health evidence unexpectedly committed")
	}

	var preserved bool
	var eventCount int
	if err := pool.QueryRow(ctx, `
		SELECT transition_preserved FROM proxy_health_checks WHERE id=1
	`).Scan(&preserved); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM proxy_lifecycle_events`).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if preserved || eventCount != 0 {
		t.Fatalf("rolled-back transition_preserved=%v events=%d", preserved, eventCount)
	}
}

func newLifecycleEventPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("ROTA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("ROTA_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL admin pool: %v", err)
	}
	schema := fmt.Sprintf("rota_lifecycle_event_test_%d", time.Now().UnixNano())
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		admin.Close()
		t.Fatalf("create lifecycle event test schema: %v", err)
	}

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		admin.Close()
		t.Fatalf("parse PostgreSQL config: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		admin.Close()
		t.Fatalf("open lifecycle event test pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE")
		admin.Close()
	})

	if _, err := pool.Exec(ctx, `
		CREATE TABLE proxies (id INTEGER PRIMARY KEY);
		CREATE TABLE proxy_health_checks (
		  id BIGINT PRIMARY KEY,
		  proxy_id INTEGER NOT NULL REFERENCES proxies(id),
		  transition_preserved BOOLEAN NOT NULL DEFAULT true
		);
		CREATE TABLE proxy_lifecycle_events (
		  id BIGSERIAL PRIMARY KEY,
		  proxy_id INTEGER NOT NULL REFERENCES proxies(id),
		  health_check_id BIGINT UNIQUE REFERENCES proxy_health_checks(id),
		  occurred_at TIMESTAMPTZ NOT NULL,
		  event_kind TEXT NOT NULL,
		  previous_status TEXT NOT NULL,
		  resulting_status TEXT NOT NULL,
		  reason TEXT,
		  details JSONB NOT NULL DEFAULT '{}'::jsonb
		);
		INSERT INTO proxies(id) VALUES (1),(2);
		INSERT INTO proxy_health_checks(id,proxy_id,transition_preserved) VALUES (1,1,false);
	`); err != nil {
		t.Fatalf("create lifecycle event fixture: %v", err)
	}
	return pool
}
