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

func TestProxyControlTaskMigrationPreservesLegacyControlState(t *testing.T) {
	db, pool := newProxyControlMigrationPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("apply proxy control task migration: %v", err)
	}

	var slotCount, reportCount, migrationCount, activationColumns int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM proxy_running_slots WHERE slot_name='legacy-channel-01'`).Scan(&slotCount); err != nil {
		t.Fatalf("count preserved slots: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM proxy_control_reports WHERE incident_id='legacy-incident-1'`).Scan(&reportCount); err != nil {
		t.Fatalf("count preserved reports: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version IN (1004,1009,1010)`).Scan(&migrationCount); err != nil {
		t.Fatalf("count Proxy Control migrations: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_schema=current_schema() AND table_name='proxy_running_slots'
		  AND column_name IN (
		    'route_activation_old_username',
		    'route_activation_claim_id',
		    'route_activation_claim_until',
		    'route_activation_previous_claim_id'
		  )
	`).Scan(&activationColumns); err != nil {
		t.Fatalf("count Route activation Fence columns: %v", err)
	}
	if slotCount != 1 || reportCount != 1 || migrationCount != 3 || activationColumns != 4 {
		t.Fatalf(
			"preserved slots=%d reports=%d migration=%d activation_columns=%d",
			slotCount, reportCount, migrationCount, activationColumns,
		)
	}

	for _, table := range []string{
		"proxy_control_leases",
		"proxy_control_business_runs",
		"proxy_control_tasks",
		"proxy_control_observations",
		"proxy_control_incident_observations",
		"proxy_control_command_receipts",
		"proxy_identity_profile_epochs",
	} {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
			t.Fatalf("check table %s: %v", table, err)
		}
		if !exists {
			t.Fatalf("table %s was not created", table)
		}
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO proxy_control_tasks (
		  task_id, attempt_request_id, request_hash, workload_scope,
		  business_run_id, job_execution_id, attempt_number, slot_name,
		  worker_id, worker_instance_id, lease_id, route_generation,
		  task_kind, status
		) VALUES
		  ('task-a','request-a','hash-a','qy-test','run-a','job-a',1,
		   'legacy-channel-01','worker-a','instance-a','lease-a',1,'channel_full','active'),
		  ('task-b','request-b','hash-b','qy-test','run-a','job-b',2,
		   'legacy-channel-02','worker-b','instance-b','lease-b',1,'channel_full','active')
	`); err == nil {
		t.Fatal("two active tasks for one business run were accepted")
	}
}

func newProxyControlMigrationPostgres(t *testing.T) (*DB, *pgxpool.Pool) {
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
	schema := fmt.Sprintf("rota_proxy_control_migration_test_%d", time.Now().UnixNano())
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
	config.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+quotedSchema+" CASCADE")
		admin.Close()
		t.Fatalf("open schema-scoped pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
		admin.Close()
	})

	if _, err := pool.Exec(ctx, proxyControlMigration1003Fixture); err != nil {
		t.Fatalf("create migration 1003 fixture: %v", err)
	}
	for _, migration := range migrations {
		if migration.Version == 1004 || migration.Version == 1009 || migration.Version == 1010 {
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

const proxyControlMigration1003Fixture = `
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE schema_migrations (
  version INT PRIMARY KEY,
  description TEXT NOT NULL,
  applied_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE TABLE proxies (
  id SERIAL PRIMARY KEY,
  country_code VARCHAR(3),
  node_identity VARCHAR(64),
  status TEXT NOT NULL DEFAULT 'active',
  cooldown_until TIMESTAMPTZ
);
CREATE TABLE proxy_pools (id SERIAL PRIMARY KEY);
CREATE TABLE proxy_users (id SERIAL PRIMARY KEY);
CREATE TABLE proxy_running_slots (
  slot_name TEXT PRIMARY KEY,
  role TEXT NOT NULL CHECK (role IN ('discover','channel','detail')),
  slot_no INTEGER NOT NULL,
  pool_id INTEGER NOT NULL REFERENCES proxy_pools(id),
  user_id INTEGER NOT NULL REFERENCES proxy_users(id),
  proxy_id INTEGER REFERENCES proxies(id),
  assignment_version BIGINT NOT NULL DEFAULT 0,
  credential_generation BIGINT NOT NULL DEFAULT 0,
  assigned_at TIMESTAMPTZ,
  ready_after TIMESTAMPTZ,
  worker_id TEXT,
  lease_id TEXT,
  lease_until TIMESTAMPTZ,
  last_heartbeat_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(role, slot_no)
);
CREATE TABLE proxy_control_reports (
  id BIGSERIAL PRIMARY KEY,
  incident_id TEXT UNIQUE,
  proxy_id INTEGER REFERENCES proxies(id),
  proxy_user TEXT,
  outcome TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  result JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);
INSERT INTO proxies (country_code,node_identity) VALUES ('BR','legacy-node');
INSERT INTO proxy_pools DEFAULT VALUES;
INSERT INTO proxy_pools DEFAULT VALUES;
INSERT INTO proxy_users DEFAULT VALUES;
INSERT INTO proxy_users DEFAULT VALUES;
INSERT INTO proxy_running_slots (
  slot_name,role,slot_no,pool_id,user_id,proxy_id,assignment_version
) VALUES
  ('legacy-channel-01','channel',1,1,1,1,1),
  ('legacy-channel-02','channel',2,2,2,NULL,0);
INSERT INTO proxy_control_reports (incident_id,proxy_id,outcome)
VALUES ('legacy-incident-1',1,'failure');
`
