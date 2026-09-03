package database

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/pkg/logger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestActiveHealthMigrationRevalidatesOnlyUnboundLegacyActiveProxies(t *testing.T) {
	db, pool := newActiveHealthMigrationPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("apply active health migration: %v", err)
	}

	tests := []struct {
		id         int
		wantStatus string
		wantDue    bool
	}{
		{id: 1, wantStatus: "idle", wantDue: true},
		{id: 2, wantStatus: "active", wantDue: true},
		{id: 3, wantStatus: "active", wantDue: true},
		{id: 4, wantStatus: "idle", wantDue: true},
		{id: 5, wantStatus: "failed", wantDue: true},
		{id: 6, wantStatus: "archived", wantDue: false},
	}
	for _, test := range tests {
		var status string
		var nextCheck *time.Time
		if err := pool.QueryRow(ctx, `
			SELECT status,next_health_check_at FROM proxies WHERE id=$1
		`, test.id).Scan(&status, &nextCheck); err != nil {
			t.Fatalf("load migrated proxy %d: %v", test.id, err)
		}
		if status != test.wantStatus || (nextCheck != nil) != test.wantDue {
			t.Fatalf("proxy %d status=%q due=%v, want %q/%v", test.id, status, nextCheck, test.wantStatus, test.wantDue)
		}
	}

	var interval int
	if err := pool.QueryRow(ctx, `
		SELECT (value->>'active_recheck_minutes')::int
		FROM settings WHERE key='proxy_lifecycle'
	`).Scan(&interval); err != nil {
		t.Fatalf("load active recheck setting: %v", err)
	}
	if interval != 120 {
		t.Fatalf("active recheck minutes = %d, want 120", interval)
	}

	var migrationEvents int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM proxy_lifecycle_events
		WHERE event_kind='active_evidence_migration'
	`).Scan(&migrationEvents); err != nil {
		t.Fatalf("count active evidence migration events: %v", err)
	}
	if migrationEvents != 2 {
		t.Fatalf("active evidence migration events = %d, want 2", migrationEvents)
	}

	var indexPredicate string
	if err := pool.QueryRow(ctx, `
		SELECT pg_get_expr(indpred,indrelid)
		FROM pg_index WHERE indexrelid='idx_proxies_health_check_due'::regclass
	`).Scan(&indexPredicate); err != nil {
		t.Fatalf("load due index predicate: %v", err)
	}
	if !strings.Contains(indexPredicate, "active") {
		t.Fatalf("active health due index predicate = %q", indexPredicate)
	}
}

func newActiveHealthMigrationPostgres(t *testing.T) (*DB, *pgxpool.Pool) {
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
	schema := fmt.Sprintf("rota_active_health_migration_test_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		admin.Close()
		t.Fatalf("parse PostgreSQL config: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		admin.Close()
		t.Fatalf("open schema pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, "DROP SCHEMA "+quoted+" CASCADE")
		admin.Close()
	})
	if _, err := pool.Exec(ctx, activeHealthMigrationFixture); err != nil {
		t.Fatalf("create active health migration fixture: %v", err)
	}
	for _, migration := range migrations {
		if migration.Version == 1012 {
			continue
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO schema_migrations (version,description) VALUES ($1,$2)
		`, migration.Version, migration.Description); err != nil {
			t.Fatalf("mark migration %d applied: %v", migration.Version, err)
		}
	}
	return &DB{Pool: pool, logger: logger.New("error")}, pool
}

const activeHealthMigrationFixture = `
CREATE TABLE schema_migrations (
  version INTEGER PRIMARY KEY,
  description TEXT NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE settings (
  key TEXT PRIMARY KEY,
  value JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE proxies (
  id INTEGER PRIMARY KEY,
  status TEXT NOT NULL,
  failed_since TIMESTAMPTZ,
  continuous_failed_since TIMESTAMPTZ,
  failure_episode_kind TEXT,
  next_health_check_at TIMESTAMPTZ,
  revalidation_required BOOLEAN NOT NULL DEFAULT false,
  health_generation BIGINT NOT NULL DEFAULT 0,
  health_check_not_before TIMESTAMPTZ,
  last_health_success_at TIMESTAMPTZ,
	base_health_status TEXT,
	youtube_health_status TEXT,
	cooldown_until TIMESTAMPTZ,
	last_error TEXT,
	archived_at TIMESTAMPTZ,
	archive_reason TEXT,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE proxy_running_slots (
  slot_name TEXT PRIMARY KEY,
  proxy_id INTEGER REFERENCES proxies(id),
  current_lease_id TEXT,
  lease_until TIMESTAMPTZ
);
CREATE TABLE proxy_control_leases (
  lease_id TEXT PRIMARY KEY,
  slot_name TEXT NOT NULL,
  status TEXT NOT NULL,
  lease_until TIMESTAMPTZ NOT NULL
);
CREATE TABLE proxy_health_checks (
  id BIGSERIAL PRIMARY KEY,
  proxy_id INTEGER NOT NULL REFERENCES proxies(id),
  applied BOOLEAN NOT NULL DEFAULT true,
  previous_status TEXT NOT NULL,
  resulting_status TEXT NOT NULL,
  transition_preserved BOOLEAN NOT NULL DEFAULT true
);
CREATE TABLE proxy_lifecycle_events (
  id BIGSERIAL PRIMARY KEY,
  proxy_id INTEGER NOT NULL REFERENCES proxies(id),
  occurred_at TIMESTAMPTZ NOT NULL,
  event_kind TEXT NOT NULL,
  previous_status TEXT NOT NULL,
  resulting_status TEXT NOT NULL,
  reason TEXT
);
CREATE INDEX idx_proxies_health_check_due ON proxies(next_health_check_at,id)
  WHERE next_health_check_at IS NOT NULL
    AND (status IN ('idle','failed') OR (status='active' AND revalidation_required));
INSERT INTO settings (key,value) VALUES (
  'proxy_lifecycle',
  '{"auto_archive_enabled":true,"hard_unreachable_after_hours":6,"soft_unreachable_after_hours":24,"youtube_unusable_after_hours":72}'
);
INSERT INTO proxies (
  id,status,next_health_check_at,last_health_success_at,
  base_health_status,youtube_health_status,cooldown_until,
  failed_since,continuous_failed_since,failure_episode_kind
) VALUES
  (1,'active',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
  (2,'active',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
  (3,'active',NULL,NOW()-INTERVAL '30 minutes','passed','passed',NULL,NULL,NULL,NULL),
  (4,'active',NULL,NOW()-INTERVAL '30 minutes','passed','passed',NOW()+INTERVAL '30 minutes',NULL,NULL,NULL),
  (5,'failed',NULL,NULL,'failed','not_run',NULL,NOW()-INTERVAL '1 hour',NOW()-INTERVAL '1 hour','soft_unreachable'),
  (6,'archived',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO proxy_running_slots (
  slot_name,proxy_id,current_lease_id,lease_until
) VALUES ('bound-legacy',2,'bound-legacy-lease',NOW()+INTERVAL '1 hour');
INSERT INTO proxy_control_leases (lease_id,slot_name,status,lease_until)
VALUES ('bound-legacy-lease','bound-legacy','active',NOW()+INTERVAL '1 hour');
ALTER TABLE proxies ADD CONSTRAINT proxies_scheduled_status_has_due
  CHECK (status NOT IN ('idle','failed') OR next_health_check_at IS NOT NULL) NOT VALID;
`
