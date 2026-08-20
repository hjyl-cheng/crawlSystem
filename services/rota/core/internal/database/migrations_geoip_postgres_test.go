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

func TestManagedGeoIPMigrationAppliesBelowLocalHighWaterMark(t *testing.T) {
	tests := []struct {
		name             string
		existingValue    string
		wantProvider     string
		wantLicenseKey   string
		wantDatabasePath string
	}{
		{
			name:             "inserts defaults",
			wantProvider:     "local",
			wantDatabasePath: "/app/geoip/managed/GeoLite2-City.mmdb",
		},
		{
			name:             "preserves existing managed settings",
			existingValue:    `{"provider":"maxmind","maxmind_license_key":"existing-secret","maxmind_db_path":"/app/geoip/managed/custom.mmdb","auto_update":true,"update_interval_hours":24}`,
			wantProvider:     "maxmind",
			wantLicenseKey:   "existing-secret",
			wantDatabasePath: "/app/geoip/managed/custom.mmdb",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, pool := newGeoIPMigrationPostgres(t)
			if test.existingValue != "" {
				if _, err := pool.Exec(context.Background(),
					`INSERT INTO settings (key, value) VALUES ('geoip', $1::jsonb)`, test.existingValue,
				); err != nil {
					t.Fatalf("insert existing GeoIP settings: %v", err)
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := db.Migrate(ctx); err != nil {
				t.Fatalf("apply pending migration: %v", err)
			}

			var migrationCount int
			if err := pool.QueryRow(ctx,
				`SELECT COUNT(*) FROM schema_migrations WHERE version = 22`,
			).Scan(&migrationCount); err != nil {
				t.Fatalf("query migration 22: %v", err)
			}
			if migrationCount != 1 {
				t.Fatalf("migration 22 count = %d, want 1", migrationCount)
			}

			var provider, licenseKey, databasePath string
			if err := pool.QueryRow(ctx, `
				SELECT value->>'provider',
				       COALESCE(value->>'maxmind_license_key', ''),
				       value->>'maxmind_db_path'
				FROM settings
				WHERE key = 'geoip'
			`).Scan(&provider, &licenseKey, &databasePath); err != nil {
				t.Fatalf("query GeoIP settings: %v", err)
			}
			if provider != test.wantProvider || licenseKey != test.wantLicenseKey || databasePath != test.wantDatabasePath {
				t.Fatalf(
					"GeoIP settings = provider %q, license %q, path %q",
					provider, licenseKey, databasePath,
				)
			}
		})
	}
}

func newGeoIPMigrationPostgres(t *testing.T) (*DB, *pgxpool.Pool) {
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
	schema := fmt.Sprintf("rota_geoip_migration_test_%d", time.Now().UnixNano())
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
		CREATE TABLE settings (
			key VARCHAR(255) PRIMARY KEY,
			value JSONB NOT NULL,
			updated_at TIMESTAMP NOT NULL DEFAULT NOW()
		);
	`); err != nil {
		t.Fatalf("create migration fixtures: %v", err)
	}
	for _, migration := range migrations {
		if migration.Version == 22 {
			continue
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO schema_migrations (version, description)
			VALUES ($1, $2)
		`, migration.Version, migration.Description); err != nil {
			t.Fatalf("mark migration %d applied: %v", migration.Version, err)
		}
	}

	return &DB{Pool: pool, logger: logger.New("error")}, pool
}
