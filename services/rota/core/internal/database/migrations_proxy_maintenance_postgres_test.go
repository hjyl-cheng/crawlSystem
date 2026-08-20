package database

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/pkg/logger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestProxyMaintenanceMigrationBackfillsEvidenceWithoutRepairingBusinessState(t *testing.T) {
	db, pool := newProxyMaintenanceMigrationPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("apply proxy maintenance migration: %v", err)
	}

	var status string
	var nextCheck *time.Time
	var continuousFailedSince *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT status, next_health_check_at, continuous_failed_since
		FROM proxies WHERE id=1
	`).Scan(&status, &nextCheck, &continuousFailedSince); err != nil {
		t.Fatalf("query migrated proxy: %v", err)
	}
	if status != "failed" || nextCheck != nil || continuousFailedSince == nil {
		t.Fatalf("migration changed business state: status=%s next=%v continuous=%v", status, nextCheck, continuousFailedSince)
	}

	var action string
	if err := pool.QueryRow(ctx, `
		SELECT recommended_action FROM proxy_lifecycle_repair_candidates WHERE proxy_id=1
	`).Scan(&action); err != nil {
		t.Fatalf("query repair candidate: %v", err)
	}
	if action != "restore_archive_projection" {
		t.Fatalf("recommended action = %q", action)
	}

	var eventCount, violationCount int
	var transitionPreserved bool
	var retentionIndex, transitionIndex bool
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM proxy_lifecycle_events WHERE proxy_id=1`).Scan(&eventCount); err != nil {
		t.Fatalf("count lifecycle events: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT transition_preserved FROM proxy_health_checks WHERE id=1`).Scan(&transitionPreserved); err != nil {
		t.Fatalf("query transition preservation marker: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM proxy_lifecycle_invariant_violations WHERE proxy_id=1
	`).Scan(&violationCount); err != nil {
		t.Fatalf("count lifecycle violations: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT to_regclass('idx_proxy_health_checks_retention') IS NOT NULL,
		       to_regclass('idx_proxy_health_checks_unpreserved_transitions') IS NOT NULL
	`).Scan(&retentionIndex, &transitionIndex); err != nil {
		t.Fatalf("query online maintenance indexes: %v", err)
	}
	if eventCount != 1 || !transitionPreserved || violationCount == 0 || !retentionIndex || !transitionIndex {
		t.Fatalf(
			"events=%d transition_preserved=%v violations=%d retention_index=%v transition_index=%v",
			eventCount, transitionPreserved, violationCount, retentionIndex, transitionIndex,
		)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO proxy_health_checks (
		  id,proxy_id,started_at,checked_at,base_result,youtube_result,verdict,
		  conclusive,control_path_healthy,previous_status,resulting_status,applied
		) VALUES
		  (2,1,NOW(),NOW(),'{}','{}','healthy',true,true,'active','active',true),
		  (3,1,NOW(),NOW(),'{}','{}','soft_unreachable',true,true,'active','failed',true)
	`); err != nil {
		t.Fatalf("insert legacy-writer health evidence: %v", err)
	}
	var ordinaryPreserved, transitionInitiallyPreserved bool
	if err := pool.QueryRow(ctx, `
		SELECT bool_and(transition_preserved) FILTER (WHERE id=2),
		       bool_and(transition_preserved) FILTER (WHERE id=3)
		FROM proxy_health_checks WHERE id IN (2,3)
	`).Scan(&ordinaryPreserved, &transitionInitiallyPreserved); err != nil {
		t.Fatalf("query legacy-writer transition defaults: %v", err)
	}
	if !ordinaryPreserved || transitionInitiallyPreserved {
		t.Fatalf(
			"legacy-writer defaults ordinary=%v transition=%v",
			ordinaryPreserved, transitionInitiallyPreserved,
		)
	}

	if err := db.Rollback(ctx); err != nil {
		t.Fatalf("rollback sparse transition index: %v", err)
	}
	if err := db.Rollback(ctx); err != nil {
		t.Fatalf("rollback retention index: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT to_regclass('idx_proxy_health_checks_retention') IS NOT NULL,
		       to_regclass('idx_proxy_health_checks_unpreserved_transitions') IS NOT NULL
	`).Scan(&retentionIndex, &transitionIndex); err != nil {
		t.Fatalf("query rolled-back maintenance indexes: %v", err)
	}
	if retentionIndex || transitionIndex {
		t.Fatalf("online indexes remained after rollback: retention=%v transition=%v", retentionIndex, transitionIndex)
	}
}

func TestRecoverInvalidConcurrentIndex(t *testing.T) {
	db, pool := newProxyMaintenanceMigrationPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if _, err := pool.Exec(ctx, `
		CREATE TABLE invalid_index_fixture (value INTEGER NOT NULL);
		INSERT INTO invalid_index_fixture(value) VALUES (1),(1);
	`); err != nil {
		t.Fatalf("create invalid-index fixture: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE UNIQUE INDEX CONCURRENTLY idx_invalid_concurrent_fixture
		ON invalid_index_fixture(value)
	`); err == nil {
		t.Fatal("duplicate data unexpectedly produced a valid unique index")
	}

	exists, valid, err := db.concurrentIndexState(ctx, "idx_invalid_concurrent_fixture")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || valid {
		t.Fatalf("failed concurrent index state exists=%v valid=%v", exists, valid)
	}
	if err := db.recoverInvalidConcurrentIndex(ctx, "idx_invalid_concurrent_fixture"); err != nil {
		t.Fatalf("recover invalid concurrent index: %v", err)
	}
	exists, valid, err = db.concurrentIndexState(ctx, "idx_invalid_concurrent_fixture")
	if err != nil {
		t.Fatal(err)
	}
	if exists || valid {
		t.Fatalf("invalid concurrent index remained exists=%v valid=%v", exists, valid)
	}
}

func newProxyMaintenanceMigrationPostgres(t *testing.T) (*DB, *pgxpool.Pool) {
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
	schema := fmt.Sprintf("rota_proxy_maintenance_migration_test_%d", time.Now().UnixNano())
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		admin.Close()
		t.Fatalf("create test schema: %v", err)
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
		t.Fatalf("open schema-scoped pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE")
		admin.Close()
	})

	if _, err := pool.Exec(ctx, proxyMaintenanceMigration1004Fixture); err != nil {
		t.Fatalf("create migration 1004 fixture: %v", err)
	}
	for _, migration := range migrations {
		if migration.Version >= 1005 && migration.Version <= 1008 {
			continue
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO schema_migrations (version, description) VALUES ($1,$2)
		`, migration.Version, migration.Description); err != nil {
			t.Fatalf("mark migration %d applied: %v", migration.Version, err)
		}
	}
	return &DB{Pool: pool, logger: logger.New("error")}, pool
}

const proxyMaintenanceMigration1004Fixture = `
CREATE TABLE schema_migrations (
  version INT PRIMARY KEY,
  description TEXT NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE proxy_sources (
  id SERIAL PRIMARY KEY,
  cleanup_enabled BOOLEAN NOT NULL DEFAULT false,
  cleanup_days INTEGER NOT NULL DEFAULT 7
);
CREATE TABLE proxies (
  id SERIAL PRIMARY KEY,
  node_identity TEXT,
  status TEXT NOT NULL,
  tags TEXT[] NOT NULL DEFAULT '{}',
  failed_since TIMESTAMPTZ,
  failure_episode_kind TEXT,
  next_health_check_at TIMESTAMPTZ,
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
  PRIMARY KEY (source_id, proxy_id)
);
CREATE TABLE proxy_running_slots (
  slot_name TEXT PRIMARY KEY,
  proxy_id INTEGER REFERENCES proxies(id)
);
CREATE TABLE proxy_health_checks (
  id BIGSERIAL PRIMARY KEY,
  proxy_id INTEGER NOT NULL REFERENCES proxies(id) ON DELETE CASCADE,
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
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO proxies (
  node_identity,status,failed_since,failure_episode_kind,next_health_check_at
) VALUES (
  'legacy-stuck','failed',NOW()-INTERVAL '2 days','soft_unreachable',NULL
);
INSERT INTO proxy_health_checks (
  proxy_id,started_at,checked_at,base_result,youtube_result,verdict,
  conclusive,control_path_healthy,previous_status,resulting_status,applied
) VALUES (
  1,NOW()-INTERVAL '1 day',NOW()-INTERVAL '1 day','{}','{}','soft_unreachable',
  true,true,'failed','archived',true
);
`
