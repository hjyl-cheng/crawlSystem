package repository

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/sourceinventory"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSourceInventoryRequiresRepeatedCompleteRefreshesAndShadowDoesNotArchive(t *testing.T) {
	repo, pool := newSourceInventoryPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	shadow, _ := sourceinventory.NewPolicy("shadow", 3)
	enforce, _ := sourceinventory.NewPolicy("enforce", 3)
	start := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)

	for index, offset := range []time.Duration{0, 48 * time.Hour, 72 * time.Hour} {
		result, err := repo.ReconcileCompleteRefresh(ctx, sourceinventory.CompleteRefresh{
			SourceID:       1,
			NodeIdentities: []string{"seen"},
			CompletedAt:    start.Add(offset),
		}, shadow)
		if err != nil {
			t.Fatalf("shadow refresh %d: %v", index+1, err)
		}
		if result.ArchivedProxyCount != 0 || result.RetiredMembershipCount != 0 {
			t.Fatalf("shadow refresh mutated lifecycle: %#v", result)
		}
	}

	var status string
	var retiredAt *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT p.status, m.retired_at
		FROM proxies p JOIN proxy_source_memberships m ON m.proxy_id=p.id
		WHERE p.node_identity='missing'
	`).Scan(&status, &retiredAt); err != nil {
		t.Fatalf("query shadow state: %v", err)
	}
	if status != "active" || retiredAt != nil {
		t.Fatalf("shadow status=%s retired_at=%v", status, retiredAt)
	}

	enforcedAt := start.Add(96 * time.Hour)
	result, err := repo.ReconcileCompleteRefresh(ctx, sourceinventory.CompleteRefresh{
		SourceID:       1,
		NodeIdentities: []string{"seen"},
		CompletedAt:    enforcedAt,
	}, enforce)
	if err != nil {
		t.Fatalf("enforced refresh: %v", err)
	}
	if result.RetiredMembershipCount != 1 || result.ArchivedProxyCount != 1 {
		t.Fatalf("enforced result = %#v", result)
	}

	if err := pool.QueryRow(ctx, `SELECT status FROM proxies WHERE node_identity='missing'`).Scan(&status); err != nil {
		t.Fatalf("query archived proxy: %v", err)
	}
	if status != "archived" {
		t.Fatalf("status after enforce = %q", status)
	}

	result, err = repo.ReconcileCompleteRefresh(ctx, sourceinventory.CompleteRefresh{
		SourceID:       1,
		NodeIdentities: []string{"seen", "missing"},
		CompletedAt:    start.Add(120 * time.Hour),
	}, enforce)
	if err != nil {
		t.Fatalf("reappearance refresh: %v", err)
	}
	if result.ReactivatedProxyCount != 1 {
		t.Fatalf("reappearance result = %#v", result)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM proxies WHERE node_identity='missing'`).Scan(&status); err != nil {
		t.Fatalf("query reactivated proxy: %v", err)
	}
	if status != "idle" {
		t.Fatalf("reappeared proxy status = %q, want idle", status)
	}
}

func TestSourceInventoryShadowScale100K(t *testing.T) {
	if os.Getenv("ROTA_RUN_SCALE_TESTS") != "1" {
		t.Skip("ROTA_RUN_SCALE_TESTS is not enabled")
	}
	repo, pool := newSourceInventoryPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	if _, err := pool.Exec(ctx, `
		TRUNCATE proxy_lifecycle_events, proxy_inventory_reconciliation_runs,
		         proxy_running_slots, proxy_source_memberships, proxies, proxy_sources
		RESTART IDENTITY;
		INSERT INTO proxy_sources (cleanup_enabled,cleanup_days) VALUES (true,1);
		INSERT INTO proxies (node_identity,status)
		SELECT 'proxy-' || lpad(value::text,6,'0'), 'active'
		FROM generate_series(1,100000) AS value;
		INSERT INTO proxy_source_memberships (source_id,proxy_id)
		SELECT 1,id FROM proxies;
	`); err != nil {
		t.Fatalf("seed 100k source inventory: %v", err)
	}

	observed := make([]string, 0, 90000)
	for value := 1; value <= 100000; value++ {
		if value%10 == 0 {
			continue
		}
		observed = append(observed, fmt.Sprintf("proxy-%06d", value))
	}
	shadow, err := sourceinventory.NewPolicy("shadow", 3)
	if err != nil {
		t.Fatal(err)
	}

	startedAt := time.Now()
	result, err := repo.ReconcileCompleteRefresh(ctx, sourceinventory.CompleteRefresh{
		SourceID:       1,
		NodeIdentities: observed,
		CompletedAt:    time.Now().UTC(),
	}, shadow)
	elapsed := time.Since(startedAt)
	if err != nil {
		t.Fatalf("reconcile 100k source inventory: %v", err)
	}
	if result.ObservedCount != 90000 || result.NewlyMissingCount != 10000 {
		t.Fatalf("scale reconciliation result = %#v", result)
	}
	if result.RetiredMembershipCount != 0 || result.ArchivedProxyCount != 0 || result.ReactivatedProxyCount != 0 {
		t.Fatalf("shadow scale reconciliation changed lifecycle: %#v", result)
	}
	if elapsed > 15*time.Second {
		t.Fatalf("100k shadow reconciliation took %s, want <=15s", elapsed)
	}
	t.Logf("100k shadow reconciliation completed in %s", elapsed)
}

func newSourceInventoryPostgres(t *testing.T) (*SourceRepository, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("ROTA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("ROTA_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL admin pool: %v", err)
	}
	schema := fmt.Sprintf("rota_source_inventory_test_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open schema pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, "DROP SCHEMA "+quoted+" CASCADE")
		admin.Close()
	})
	if _, err := pool.Exec(ctx, sourceInventoryFixture); err != nil {
		t.Fatalf("create source inventory fixture: %v", err)
	}
	return NewSourceRepository(&database.DB{Pool: pool}), pool
}

const sourceInventoryFixture = `
CREATE TABLE proxy_sources (
  id SERIAL PRIMARY KEY,
  successful_refresh_generation BIGINT NOT NULL DEFAULT 0,
  last_complete_refresh_at TIMESTAMPTZ,
  cleanup_enabled BOOLEAN NOT NULL DEFAULT false,
  cleanup_days INTEGER NOT NULL DEFAULT 7,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE proxies (
  id SERIAL PRIMARY KEY,
  node_identity TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL,
  tags TEXT[] NOT NULL DEFAULT '{}',
  failed_since TIMESTAMPTZ,
  continuous_failed_since TIMESTAMPTZ,
  failure_episode_kind TEXT,
  next_health_check_at TIMESTAMPTZ,
  revalidation_required BOOLEAN NOT NULL DEFAULT false,
  health_generation BIGINT NOT NULL DEFAULT 0,
  health_check_not_before TIMESTAMPTZ,
  base_health_status TEXT,
  youtube_health_status TEXT,
  last_youtube_success TIMESTAMPTZ,
  last_error TEXT,
  archived_at TIMESTAMPTZ,
  archive_reason TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE proxy_source_memberships (
  source_id INTEGER NOT NULL REFERENCES proxy_sources(id),
  proxy_id INTEGER NOT NULL REFERENCES proxies(id),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_generation BIGINT NOT NULL DEFAULT 0,
  consecutive_absences INTEGER NOT NULL DEFAULT 0,
  missing_since TIMESTAMPTZ,
  retired_at TIMESTAMPTZ,
  retirement_reason TEXT,
  PRIMARY KEY (source_id, proxy_id)
);
CREATE INDEX idx_proxy_source_memberships_reconcile
  ON proxy_source_memberships(source_id,retired_at,missing_since,consecutive_absences);
CREATE TABLE proxy_running_slots (
  slot_name TEXT PRIMARY KEY,
  proxy_id INTEGER REFERENCES proxies(id)
);
CREATE TABLE proxy_inventory_reconciliation_runs (
  id BIGSERIAL PRIMARY KEY,
  source_id INTEGER NOT NULL REFERENCES proxy_sources(id),
  refresh_generation BIGINT NOT NULL,
  mode TEXT NOT NULL,
  observed_count INTEGER NOT NULL,
  newly_missing_count INTEGER NOT NULL,
  eligible_count INTEGER NOT NULL,
  retired_membership_count INTEGER NOT NULL,
  archived_proxy_count INTEGER NOT NULL,
  reactivated_proxy_count INTEGER NOT NULL,
  completed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(source_id, refresh_generation)
);
CREATE TABLE proxy_lifecycle_events (
  id BIGSERIAL PRIMARY KEY,
  proxy_id INTEGER NOT NULL REFERENCES proxies(id),
  health_check_id BIGINT UNIQUE,
  occurred_at TIMESTAMPTZ NOT NULL,
  event_kind TEXT NOT NULL,
  previous_status TEXT NOT NULL,
  resulting_status TEXT NOT NULL,
  reason TEXT,
  details JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO proxy_sources (cleanup_enabled,cleanup_days) VALUES (true,1);
INSERT INTO proxies (node_identity,status) VALUES ('seen','active'),('missing','active');
INSERT INTO proxy_source_memberships (source_id,proxy_id) VALUES (1,1),(1,2);
`
