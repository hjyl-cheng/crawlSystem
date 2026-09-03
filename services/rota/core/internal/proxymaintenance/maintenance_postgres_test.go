package proxymaintenance

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/pkg/logger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLegacyRepairUsesAuthoritativeHistoryAndRetentionPreservesUncopiedTransitions(t *testing.T) {
	db, pool := newMaintenancePostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now().UTC()
	maintainer := New(db, logger.New("error"), Options{
		RepairBatchSize:    10,
		RepairSpread:       time.Hour,
		Retention:          14 * 24 * time.Hour,
		RetentionBatchSize: 100,
	})
	maintainer.now = func() time.Time { return now }

	repaired, err := maintainer.RepairLegacyBatch(ctx)
	if err != nil {
		t.Fatalf("RepairLegacyBatch: %v", err)
	}
	if repaired != 3 {
		t.Fatalf("repaired = %d, want 3", repaired)
	}
	if err := maintainer.EnsureLifecycleConstraints(ctx); err != nil {
		t.Fatalf("EnsureLifecycleConstraints: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxies
		SET status='failed', next_health_check_at=NULL,
		    failed_since=NULL, continuous_failed_since=NULL, failure_episode_kind=NULL
		WHERE id=2
	`); err == nil {
		t.Fatal("database accepted an incomplete failed lifecycle projection")
	}

	rows, err := pool.Query(ctx, `SELECT id,status,next_health_check_at FROM proxies ORDER BY id`)
	if err != nil {
		t.Fatalf("query repaired proxies: %v", err)
	}
	defer rows.Close()
	want := []string{"archived", "idle", "failed"}
	index := 0
	for rows.Next() {
		var id int
		var status string
		var due *time.Time
		if err := rows.Scan(&id, &status, &due); err != nil {
			t.Fatal(err)
		}
		if status != want[index] {
			t.Fatalf("proxy %d status=%q want=%q", id, status, want[index])
		}
		if status == "archived" && due != nil {
			t.Fatalf("archived proxy %d retained due time %v", id, due)
		}
		if status != "archived" && due == nil {
			t.Fatalf("scheduled proxy %d has no due time", id)
		}
		index++
	}

	deleted, err := maintainer.PruneHealthEvidenceBatch(ctx)
	if err != nil {
		t.Fatalf("PruneHealthEvidenceBatch: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
	var unpreserved, recent int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM proxy_health_checks WHERE id=6`).Scan(&unpreserved); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM proxy_health_checks WHERE id=7`).Scan(&recent); err != nil {
		t.Fatal(err)
	}
	if unpreserved != 1 || recent != 1 {
		t.Fatalf("unpreserved=%d recent=%d", unpreserved, recent)
	}
}

func newMaintenancePostgres(t *testing.T) (*database.DB, *pgxpool.Pool) {
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
	schema := fmt.Sprintf("rota_maintenance_test_%d", time.Now().UnixNano())
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
	if _, err := pool.Exec(ctx, maintenanceFixture); err != nil {
		t.Fatalf("create maintenance fixture: %v", err)
	}
	return &database.DB{Pool: pool}, pool
}

const maintenanceFixture = `
CREATE TABLE proxies (
  id SERIAL PRIMARY KEY,
  status TEXT NOT NULL,
  failed_since TIMESTAMPTZ,
  continuous_failed_since TIMESTAMPTZ,
  failure_episode_kind TEXT,
  next_health_check_at TIMESTAMPTZ,
  revalidation_required BOOLEAN NOT NULL DEFAULT false,
  health_generation BIGINT NOT NULL DEFAULT 0,
  health_check_not_before TIMESTAMPTZ,
  base_health_status TEXT,
  youtube_health_status TEXT,
  last_error TEXT,
  archived_at TIMESTAMPTZ,
  archive_reason TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE proxy_health_checks (
  id BIGSERIAL PRIMARY KEY,
  proxy_id INTEGER NOT NULL REFERENCES proxies(id) ON DELETE CASCADE,
  checked_at TIMESTAMPTZ NOT NULL,
  verdict TEXT NOT NULL,
  previous_status TEXT NOT NULL,
  resulting_status TEXT NOT NULL,
  applied BOOLEAN NOT NULL,
  transition_preserved BOOLEAN NOT NULL DEFAULT true
);
CREATE TABLE proxy_lifecycle_events (
  id BIGSERIAL PRIMARY KEY,
  proxy_id INTEGER NOT NULL REFERENCES proxies(id) ON DELETE CASCADE,
  health_check_id BIGINT UNIQUE REFERENCES proxy_health_checks(id) ON DELETE SET NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  event_kind TEXT NOT NULL,
  previous_status TEXT NOT NULL,
  resulting_status TEXT NOT NULL,
  reason TEXT,
  details JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE proxy_lifecycle_repair_actions (
  run_id TEXT NOT NULL,
  proxy_id INTEGER NOT NULL REFERENCES proxies(id),
  planned_action TEXT NOT NULL,
  evidence_health_check_id BIGINT REFERENCES proxy_health_checks(id) ON DELETE SET NULL,
  before_state JSONB NOT NULL,
  after_state JSONB NOT NULL,
  applied_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(run_id,proxy_id)
);
CREATE VIEW proxy_lifecycle_invariant_violations AS
SELECT id AS proxy_id, status, 'scheduled_state_without_due'::text AS violation
FROM proxies WHERE status IN ('idle','failed') AND next_health_check_at IS NULL
UNION ALL
SELECT id, status, 'incomplete_failure_episode'
FROM proxies WHERE status='failed' AND (
  failed_since IS NULL OR continuous_failed_since IS NULL OR failure_episode_kind IS NULL
)
UNION ALL
SELECT id, status, 'invalid_archive_projection'
FROM proxies WHERE status='archived' AND (
  archived_at IS NULL OR archive_reason IS NULL OR next_health_check_at IS NOT NULL
)
UNION ALL
SELECT checks.proxy_id, proxies.status, 'transition_evidence_not_preserved'
FROM proxy_health_checks checks
JOIN proxies ON proxies.id=checks.proxy_id
WHERE checks.applied=true
  AND checks.previous_status IS DISTINCT FROM checks.resulting_status
  AND checks.transition_preserved=false;
INSERT INTO proxies (status,failed_since,failure_episode_kind) VALUES
 ('failed',NOW()-INTERVAL '2 days','soft_unreachable'),
 ('failed',NOW()-INTERVAL '2 days','soft_unreachable'),
 ('failed',NOW()-INTERVAL '2 days','soft_unreachable');
INSERT INTO proxy_health_checks (
  id,proxy_id,checked_at,verdict,previous_status,resulting_status,applied,transition_preserved
) VALUES
 (1,1,NOW()-INTERVAL '1 day','soft_unreachable','failed','archived',true,false),
 (2,2,NOW()-INTERVAL '1 day','healthy','failed','active',true,false),
 (3,3,NOW()-INTERVAL '1 day','soft_unreachable','active','failed',true,false),
 (4,1,NOW()-INTERVAL '30 days','healthy','active','active',true,true),
 (5,1,NOW()-INTERVAL '30 days','soft_unreachable','active','failed',true,true),
 (6,2,NOW()-INTERVAL '30 days','soft_unreachable','active','failed',true,false),
 (7,3,NOW()-INTERVAL '2 days','healthy','active','active',true,true);
INSERT INTO proxy_lifecycle_events (
  proxy_id,health_check_id,occurred_at,event_kind,previous_status,resulting_status,reason
) VALUES (1,5,NOW()-INTERVAL '30 days','health_verdict','active','failed','soft_unreachable');
`
