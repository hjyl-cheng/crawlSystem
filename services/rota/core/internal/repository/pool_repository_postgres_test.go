package repository

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPoolFilterReplacementIsAtomicPostgres(t *testing.T) {
	repo, db := newPoolRepositoryPostgres(t)
	poolID := insertTestPool(t, db, "atomic-filters")

	tests := []struct {
		name    string
		seedSQL string
		set     func() error
		readSQL string
		want    string
	}{
		{
			name:    "geo",
			seedSQL: `INSERT INTO pool_geo_filters (pool_id, country_code, city_name) VALUES ($1, 'US', 'Old City')`,
			set: func() error {
				return repo.SetGeoFilters(context.Background(), poolID, []models.GeoFilter{{CountryCode: "TOO-LONG"}})
			},
			readSQL: `SELECT country_code || ':' || city_name FROM pool_geo_filters WHERE pool_id=$1`,
			want:    "US:Old City",
		},
		{
			name:    "isp",
			seedSQL: `INSERT INTO pool_isp_filters (pool_id, isp) VALUES ($1, 'Old ISP')`,
			set: func() error {
				return repo.SetISPFilters(context.Background(), poolID, []string{strings.Repeat("x", 17)})
			},
			readSQL: `SELECT isp FROM pool_isp_filters WHERE pool_id=$1`,
			want:    "Old ISP",
		},
		{
			name:    "tag",
			seedSQL: `INSERT INTO pool_tag_filters (pool_id, tag) VALUES ($1, 'origin:paid')`,
			set: func() error {
				return repo.SetTagFilters(context.Background(), poolID, []string{strings.Repeat("x", 17)})
			},
			readSQL: `SELECT tag FROM pool_tag_filters WHERE pool_id=$1`,
			want:    "origin:paid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := db.Exec(context.Background(), tt.seedSQL, poolID); err != nil {
				t.Fatalf("seed old filter: %v", err)
			}
			if err := tt.set(); err == nil {
				t.Fatal("filter replacement unexpectedly succeeded")
			}
			var got string
			if err := db.QueryRow(context.Background(), tt.readSQL, poolID).Scan(&got); err != nil {
				t.Fatalf("read retained filter: %v", err)
			}
			if got != tt.want {
				t.Fatalf("retained filter = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRebuildMembershipIsAtomicPostgres(t *testing.T) {
	repo, db := newPoolRepositoryPostgres(t)
	poolID := insertTestPool(t, db, "atomic-membership")
	oldID := insertTestProxy(t, db, "old.example:80", "active", "US", "Old ISP", []string{"origin:paid"})
	newID := insertTestProxy(t, db, "new.example:80", "active", "GB", "New ISP", []string{"origin:free"})

	if _, err := db.Exec(context.Background(),
		`INSERT INTO pool_proxies (pool_id, proxy_id) VALUES ($1, $2)`, poolID, oldID); err != nil {
		t.Fatalf("seed pool membership: %v", err)
	}
	triggerSQL := fmt.Sprintf(`
		CREATE FUNCTION reject_test_pool_member() RETURNS trigger AS $$
		BEGIN
			IF NEW.proxy_id = %d THEN
				RAISE EXCEPTION 'rejected test proxy';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER reject_test_pool_member
		BEFORE INSERT ON pool_proxies
		FOR EACH ROW EXECUTE FUNCTION reject_test_pool_member();`, newID)
	if _, err := db.Exec(context.Background(), triggerSQL); err != nil {
		t.Fatalf("create failure trigger: %v", err)
	}

	if _, err := repo.rebuildMembership(context.Background(), poolID, []int{newID}); err == nil {
		t.Fatal("membership rebuild unexpectedly succeeded")
	}
	if got := poolMemberIDs(t, db, poolID); !reflect.DeepEqual(got, []int{oldID}) {
		t.Fatalf("membership after rollback = %v, want [%d]", got, oldID)
	}
}

func TestAddProxiesSkipsMissingIDsAndIsIdempotentPostgres(t *testing.T) {
	repo, db := newPoolRepositoryPostgres(t)
	poolID := insertTestPool(t, db, "bulk-add")
	proxyID := insertTestProxy(t, db, "bulk-add.example:80", "archived", "US", "Example ISP", nil)

	ids := []int{proxyID, proxyID + 1000000}
	if err := repo.AddProxies(context.Background(), poolID, ids); err != nil {
		t.Fatalf("add proxies: %v", err)
	}
	if err := repo.AddProxies(context.Background(), poolID, ids); err != nil {
		t.Fatalf("repeat add proxies: %v", err)
	}
	if got := poolMemberIDs(t, db, poolID); !reflect.DeepEqual(got, []int{proxyID}) {
		t.Fatalf("pool members = %v, want [%d]", got, proxyID)
	}
}

func TestSyncPoolByFiltersExcludesArchivedPostgres(t *testing.T) {
	repo, db := newPoolRepositoryPostgres(t)
	poolID := insertTestPool(t, db, "lifecycle-filtering")

	if err := repo.SetGeoFilters(context.Background(), poolID, []models.GeoFilter{{CountryCode: "US"}}); err != nil {
		t.Fatalf("set geo filter: %v", err)
	}
	if err := repo.SetISPFilters(context.Background(), poolID, []string{"Acme"}); err != nil {
		t.Fatalf("set ISP filter: %v", err)
	}
	if err := repo.SetTagFilters(context.Background(), poolID, []string{"origin:free", "provider:test"}); err != nil {
		t.Fatalf("set tag filter: %v", err)
	}

	activeGeo := insertTestProxy(t, db, "active-geo.example:80", "active", "US", "Other", nil)
	archivedGeo := insertTestProxy(t, db, "archived-geo.example:80", "archived", "US", "Other", nil)
	failedISP := insertTestProxy(t, db, "failed-isp.example:80", "failed", "CA", "Acme Transit", nil)
	archivedISP := insertTestProxy(t, db, "archived-isp.example:80", "archived", "CA", "Acme Transit", nil)
	idleTag := insertTestProxy(t, db, "idle-tag.example:80", "idle", "DE", "Other", []string{"origin:free", "provider:test"})
	archivedTag := insertTestProxy(t, db, "archived-tag.example:80", "archived", "FR", "Other", []string{"origin:free", "provider:test"})

	total, newIDs, err := repo.SyncPoolByFilters(context.Background(), models.ProxyPool{ID: poolID, SyncMode: "auto"})
	if err != nil {
		t.Fatalf("sync pool: %v", err)
	}
	want := sortedInts(activeGeo, failedISP, idleTag)
	if total != len(want) {
		t.Fatalf("sync total = %d, want %d", total, len(want))
	}
	if got := sortedInts(newIDs...); !reflect.DeepEqual(got, want) {
		t.Fatalf("new IDs = %v, want %v", got, want)
	}
	if got := poolMemberIDs(t, db, poolID); !reflect.DeepEqual(got, want) {
		t.Fatalf("pool members = %v, want %v", got, want)
	}

	if _, err := db.Exec(context.Background(), `UPDATE proxies SET status='idle' WHERE id=$1`, archivedGeo); err != nil {
		t.Fatalf("restore archived proxy: %v", err)
	}
	total, newIDs, err = repo.SyncPoolByFilters(context.Background(), models.ProxyPool{ID: poolID, SyncMode: "auto"})
	if err != nil {
		t.Fatalf("sync after restore: %v", err)
	}
	if total != 4 || !reflect.DeepEqual(sortedInts(newIDs...), []int{archivedGeo}) {
		t.Fatalf("sync after restore = total %d, new IDs %v", total, sortedInts(newIDs...))
	}
	if got := poolMemberIDs(t, db, poolID); !reflect.DeepEqual(got, sortedInts(activeGeo, archivedGeo, failedISP, idleTag)) {
		t.Fatalf("pool members after restore = %v", got)
	}

	for _, archivedID := range []int{archivedISP, archivedTag} {
		for _, memberID := range poolMemberIDs(t, db, poolID) {
			if memberID == archivedID {
				t.Fatalf("archived proxy %d was added to the automatic pool", archivedID)
			}
		}
	}
}

func TestAlertRuleMutationsAreScopedToPoolPostgres(t *testing.T) {
	repo, db := newPoolRepositoryPostgres(t)
	ownerPoolID := insertTestPool(t, db, "rule-owner")
	otherPoolID := insertTestPool(t, db, "other-pool")

	rule, err := repo.CreateAlertRule(context.Background(), ownerPoolID, models.CreatePoolAlertRuleRequest{
		Enabled:          true,
		MinActiveProxies: 2,
		WebhookURL:       "https://owner.example/hook",
		WebhookMethod:    "POST",
		CooldownMinutes:  30,
	})
	if err != nil {
		t.Fatalf("create alert rule: %v", err)
	}

	updated, err := repo.UpdateAlertRule(context.Background(), otherPoolID, rule.ID, models.CreatePoolAlertRuleRequest{
		Enabled:          true,
		MinActiveProxies: 99,
		WebhookURL:       "https://attacker.example/hook",
		WebhookMethod:    "POST",
		CooldownMinutes:  1,
	})
	if err != nil {
		t.Fatalf("cross-pool update: %v", err)
	}
	if updated != nil {
		t.Fatalf("cross-pool update returned rule %#v", updated)
	}
	if err := repo.DeleteAlertRule(context.Background(), otherPoolID, rule.ID); err != nil {
		t.Fatalf("cross-pool delete: %v", err)
	}

	var url string
	if err := db.QueryRow(context.Background(),
		`SELECT webhook_url FROM pool_alert_rules WHERE id=$1`, rule.ID).Scan(&url); err != nil {
		t.Fatalf("read retained rule: %v", err)
	}
	if url != "https://owner.example/hook" {
		t.Fatalf("cross-pool mutation changed webhook URL to %q", url)
	}

	updated, err = repo.UpdateAlertRule(context.Background(), ownerPoolID, rule.ID, models.CreatePoolAlertRuleRequest{
		Enabled:          true,
		MinActiveProxies: 3,
		WebhookURL:       "https://owner.example/new-hook",
		WebhookMethod:    "POST",
		CooldownMinutes:  15,
	})
	if err != nil || updated == nil || updated.PoolID != ownerPoolID {
		t.Fatalf("owner update = rule %#v, err %v", updated, err)
	}
	if err := repo.DeleteAlertRule(context.Background(), ownerPoolID, rule.ID); err != nil {
		t.Fatalf("owner delete: %v", err)
	}
	var count int
	if err := db.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM pool_alert_rules WHERE id=$1`, rule.ID).Scan(&count); err != nil {
		t.Fatalf("count deleted rule: %v", err)
	}
	if count != 0 {
		t.Fatalf("owner delete retained %d rules", count)
	}
}

func newPoolRepositoryPostgres(t *testing.T) (*PoolRepository, *pgxpool.Pool) {
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
	schema := fmt.Sprintf("rota_pool_repository_test_%d", time.Now().UnixNano())
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
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+quotedSchema+" CASCADE")
		admin.Close()
		t.Fatalf("open schema-scoped pool: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
		admin.Close()
	})

	const schemaSQL = `
		CREATE TABLE proxies (
			id SERIAL PRIMARY KEY,
			address VARCHAR(255) NOT NULL UNIQUE,
			protocol VARCHAR(20) NOT NULL DEFAULT 'http',
			status VARCHAR(20) NOT NULL DEFAULT 'idle',
			country_code VARCHAR(3),
			city_name VARCHAR(100),
			isp TEXT,
			tags TEXT[] NOT NULL DEFAULT '{}'
		);
		CREATE TABLE proxy_pools (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			description TEXT,
			country_code VARCHAR(3),
			region_name VARCHAR(100),
			city_name VARCHAR(100),
			rotation_method VARCHAR(30) NOT NULL DEFAULT 'roundrobin',
			stick_count INTEGER NOT NULL DEFAULT 10,
			health_check_url TEXT NOT NULL DEFAULT 'https://example.com/health',
			health_check_cron VARCHAR(100) NOT NULL DEFAULT '*/30 * * * *',
			health_check_enabled BOOLEAN NOT NULL DEFAULT true,
			auto_sync BOOLEAN NOT NULL DEFAULT true,
			sync_mode VARCHAR(10) NOT NULL DEFAULT 'auto',
			enabled BOOLEAN NOT NULL DEFAULT true,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP NOT NULL DEFAULT NOW()
		);
		CREATE TABLE pool_proxies (
			pool_id INTEGER NOT NULL REFERENCES proxy_pools(id) ON DELETE CASCADE,
			proxy_id INTEGER NOT NULL REFERENCES proxies(id) ON DELETE CASCADE,
			added_at TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY (pool_id, proxy_id)
		);
		CREATE TABLE pool_geo_filters (
			id SERIAL PRIMARY KEY,
			pool_id INTEGER NOT NULL REFERENCES proxy_pools(id) ON DELETE CASCADE,
			country_code VARCHAR(3),
			city_name VARCHAR(100),
			UNIQUE (pool_id, country_code, city_name)
		);
		CREATE TABLE pool_isp_filters (
			id SERIAL PRIMARY KEY,
			pool_id INTEGER NOT NULL REFERENCES proxy_pools(id) ON DELETE CASCADE,
			isp TEXT NOT NULL CHECK (char_length(isp) <= 16),
			UNIQUE (pool_id, isp)
		);
		CREATE TABLE pool_tag_filters (
			id SERIAL PRIMARY KEY,
			pool_id INTEGER NOT NULL REFERENCES proxy_pools(id) ON DELETE CASCADE,
			tag TEXT NOT NULL CHECK (char_length(tag) <= 16),
			UNIQUE (pool_id, tag)
		);
		CREATE TABLE pool_alert_rules (
			id SERIAL PRIMARY KEY,
			pool_id INTEGER NOT NULL REFERENCES proxy_pools(id) ON DELETE CASCADE,
			enabled BOOLEAN NOT NULL DEFAULT true,
			min_active_proxies INTEGER NOT NULL DEFAULT 5,
			webhook_url TEXT NOT NULL,
			webhook_method VARCHAR(10) NOT NULL DEFAULT 'POST',
			last_fired_at TIMESTAMP,
			cooldown_minutes INTEGER NOT NULL DEFAULT 30,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP NOT NULL DEFAULT NOW()
		);`
	if _, err := db.Exec(ctx, schemaSQL); err != nil {
		t.Fatalf("create repository test tables: %v", err)
	}

	return NewPoolRepository(&database.DB{Pool: db}), db
}

func insertTestPool(t *testing.T, db *pgxpool.Pool, name string) int {
	t.Helper()
	var id int
	if err := db.QueryRow(context.Background(),
		`INSERT INTO proxy_pools (name) VALUES ($1) RETURNING id`, name).Scan(&id); err != nil {
		t.Fatalf("insert test pool: %v", err)
	}
	return id
}

func insertTestProxy(t *testing.T, db *pgxpool.Pool, address, status, country, isp string, tags []string) int {
	t.Helper()
	if tags == nil {
		tags = []string{}
	}
	var id int
	if err := db.QueryRow(context.Background(), `
		INSERT INTO proxies (address, status, country_code, isp, tags)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id`, address, status, country, isp, tags).Scan(&id); err != nil {
		t.Fatalf("insert test proxy: %v", err)
	}
	return id
}

func poolMemberIDs(t *testing.T, db *pgxpool.Pool, poolID int) []int {
	t.Helper()
	rows, err := db.Query(context.Background(),
		`SELECT proxy_id FROM pool_proxies WHERE pool_id=$1 ORDER BY proxy_id`, poolID)
	if err != nil {
		t.Fatalf("query pool members: %v", err)
	}
	defer rows.Close()
	ids := []int{}
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan pool member: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate pool members: %v", err)
	}
	return ids
}

func sortedInts(values ...int) []int {
	sorted := append([]int(nil), values...)
	sort.Ints(sorted)
	return sorted
}
