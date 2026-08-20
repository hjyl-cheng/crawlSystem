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

func TestSourceDefaultTagsMigrationAppliesBelowLocalHighWaterMark(t *testing.T) {
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
	schema := fmt.Sprintf("rota_source_migration_test_%d", time.Now().UnixNano())
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

	if _, err := pool.Exec(ctx, `
		CREATE TABLE schema_migrations (
			version INT PRIMARY KEY,
			description TEXT NOT NULL,
			applied_at TIMESTAMP NOT NULL DEFAULT NOW()
		);
		CREATE TABLE proxy_sources (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL
		);
		INSERT INTO proxy_sources (name) VALUES ('existing source');
	`); err != nil {
		t.Fatalf("create migration fixtures: %v", err)
	}
	for _, migration := range migrations {
		if migration.Version == 25 {
			continue
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO schema_migrations (version, description) VALUES ($1, $2)
		`, migration.Version, migration.Description); err != nil {
			t.Fatalf("mark migration %d applied: %v", migration.Version, err)
		}
	}

	db := &DB{Pool: pool, logger: logger.New("error")}
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("apply pending migration: %v", err)
	}

	var migrationCount int
	var defaultTags []string
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM schema_migrations WHERE version = 25
	`).Scan(&migrationCount); err != nil {
		t.Fatalf("query migration 25: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT default_tags FROM proxy_sources`).Scan(&defaultTags); err != nil {
		t.Fatalf("query source default tags: %v", err)
	}
	if migrationCount != 1 || len(defaultTags) != 0 {
		t.Fatalf("migration count = %d, default tags = %#v", migrationCount, defaultTags)
	}
}
