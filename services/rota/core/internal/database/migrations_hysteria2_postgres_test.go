package database

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestHysteria2MigrationPreservesCustomProtocolSelections(t *testing.T) {
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
	schema := fmt.Sprintf("rota_hysteria2_migration_test_%d", time.Now().UnixNano())
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		admin.Close()
		t.Fatalf("create test schema: %v", err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+quotedSchema+" CASCADE")
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE")
		admin.Close()
	})
	if _, err := pool.Exec(ctx, `
		CREATE TABLE settings (
			key VARCHAR(255) PRIMARY KEY,
			value JSONB NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		t.Fatal(err)
	}

	var migrationUp, migrationDown string
	for _, migration := range migrations {
		if migration.Version == 1014 {
			migrationUp = migration.Up
			migrationDown = migration.Down
			break
		}
	}
	if migrationUp == "" || migrationDown == "" {
		t.Fatal("migration 1014 is missing")
	}
	oldDefaults := []string{
		"http", "https", "socks4", "socks4a", "socks5",
		"vless", "vmess", "trojan", "shadowsocks",
	}
	newDefaults := append(append([]string{}, oldDefaults...), "hysteria2")
	tests := []struct {
		name    string
		value   string
		expects []string
	}{
		{name: "old defaults gain Hysteria2", value: `{"allowed_protocols":["http","https","socks4","socks4a","socks5","vless","vmess","trojan","shadowsocks"]}`, expects: newDefaults},
		{name: "custom selection is unchanged", value: `{"allowed_protocols":["http","vless"]}`, expects: []string{"http", "vless"}},
		{name: "missing selection receives new defaults", value: `{"method":"random"}`, expects: newDefaults},
		{name: "already upgraded selection is unchanged", value: `{"allowed_protocols":["http","https","socks4","socks4a","socks5","vless","vmess","trojan","shadowsocks","hysteria2"]}`, expects: newDefaults},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, `
				INSERT INTO settings(key,value) VALUES ('rotation',$1::jsonb)
				ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value
			`, test.value); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, migrationUp); err != nil {
				t.Fatalf("apply migration: %v", err)
			}
			var protocols []string
			if err := pool.QueryRow(ctx, `
				SELECT ARRAY(SELECT jsonb_array_elements_text(value->'allowed_protocols'))
				FROM settings WHERE key='rotation'
			`).Scan(&protocols); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(protocols, test.expects) {
				t.Fatalf("protocols = %#v, want %#v", protocols, test.expects)
			}
		})
	}

	if _, err := pool.Exec(ctx, `UPDATE settings SET value=$1::jsonb WHERE key='rotation'`,
		`{"allowed_protocols":["http","hysteria2","vless"]}`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, migrationDown); err != nil {
		t.Fatalf("roll back migration: %v", err)
	}
	var rolledBack []string
	if err := pool.QueryRow(ctx, `
		SELECT ARRAY(SELECT jsonb_array_elements_text(value->'allowed_protocols'))
		FROM settings WHERE key='rotation'
	`).Scan(&rolledBack); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rolledBack, []string{"http", "vless"}) {
		t.Fatalf("rolled-back protocols = %#v", rolledBack)
	}
}
