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

func TestMixedXrayNodeMigrationPreservesConfigurationsAndSourceMembership(t *testing.T) {
	dsn := os.Getenv("ROTA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("ROTA_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL admin pool: %v", err)
	}
	schema := fmt.Sprintf("rota_xray_migration_test_%d", time.Now().UnixNano())
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

	if _, err := pool.Exec(ctx, `
		CREATE TABLE schema_migrations (
			version INT PRIMARY KEY,
			description TEXT NOT NULL,
			applied_at TIMESTAMP NOT NULL DEFAULT NOW()
		);
		CREATE TABLE proxy_sources (
			id SERIAL PRIMARY KEY,
			last_supported INTEGER NOT NULL DEFAULT 0,
			last_skipped INTEGER NOT NULL DEFAULT 0
		);
		ALTER TABLE proxy_sources DROP COLUMN last_supported;
		ALTER TABLE proxy_sources DROP COLUMN last_skipped;
		CREATE TABLE proxies (
			id SERIAL PRIMARY KEY,
			address TEXT NOT NULL,
			protocol TEXT NOT NULL,
			password TEXT,
			source_id INTEGER REFERENCES proxy_sources(id) ON DELETE SET NULL,
			last_seen_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			CONSTRAINT unique_proxy_address_protocol UNIQUE (address, protocol)
		);
		CREATE TABLE settings (
			key VARCHAR(255) PRIMARY KEY,
			value JSONB NOT NULL
		);
		INSERT INTO proxy_sources DEFAULT VALUES;
		INSERT INTO proxies (address, protocol, password, source_id)
		VALUES
			('shared.example:443', 'vless', 'vless://first-canonical', 1),
			('ordinary.example:8080', 'http', NULL, NULL);
		INSERT INTO settings (key, value)
		VALUES ('rotation', '{"allowed_protocols":["http"]}'::jsonb);
	`); err != nil {
		t.Fatalf("create migration fixtures: %v", err)
	}
	for _, migration := range migrations {
		if migration.Version == 1002 {
			continue
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO schema_migrations (version, description) VALUES ($1, $2)`,
			migration.Version, migration.Description,
		); err != nil {
			t.Fatalf("mark migration %d applied: %v", migration.Version, err)
		}
	}

	db := &DB{Pool: pool, logger: logger.New("error")}
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("apply migration 1002: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO proxies (address, protocol, password, node_identity)
		VALUES (
			'shared.example:443',
			'vless',
			'vless://second-canonical',
			encode(digest('vless://second-canonical', 'sha256'), 'hex')
		)
	`); err != nil {
		t.Fatalf("insert second configuration on same endpoint: %v", err)
	}

	var proxyCount, membershipCount int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM proxies WHERE address = 'shared.example:443' AND protocol = 'vless'`,
	).Scan(&proxyCount); err != nil {
		t.Fatalf("count same-endpoint configurations: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM proxy_source_memberships WHERE source_id = 1`,
	).Scan(&membershipCount); err != nil {
		t.Fatalf("count migrated memberships: %v", err)
	}
	if proxyCount != 2 || membershipCount != 1 {
		t.Fatalf("same-endpoint proxies = %d, migrated memberships = %d", proxyCount, membershipCount)
	}

	var protocols []string
	if err := pool.QueryRow(ctx,
		`SELECT ARRAY(SELECT jsonb_array_elements_text(value->'allowed_protocols')) FROM settings WHERE key = 'rotation'`,
	).Scan(&protocols); err != nil {
		t.Fatalf("read rotation protocols: %v", err)
	}
	if len(protocols) != 1 || protocols[0] != "http" {
		t.Fatalf("rotation protocols = %#v, want existing selection preserved", protocols)
	}
}
