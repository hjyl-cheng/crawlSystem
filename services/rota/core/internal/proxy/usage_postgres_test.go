package proxy

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/repository"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresUsageBatchPreservesLifecycleAndReadsActualSchema(t *testing.T) {
	repo, db := newProxyPackagePostgres(t)
	checkedAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)

	var archivedID int
	if err := db.QueryRow(context.Background(), `
		INSERT INTO proxies (
			address, status, requests, successful_requests, failed_requests,
			avg_response_time, last_check, last_error
		) VALUES ('archived.example:80', 'archived', 10, 4, 7, 100, $1, 'health evidence')
		RETURNING id
	`, checkedAt).Scan(&archivedID); err != nil {
		t.Fatalf("insert archived proxy: %v", err)
	}
	var failedID int
	if err := db.QueryRow(context.Background(), `
		INSERT INTO proxies (
			address, status, requests, successful_requests, failed_requests,
			avg_response_time, last_check, last_error
		) VALUES ('failed.example:80', 'failed', 2, 1, 2, 50, $1, 'youtube evidence')
		RETURNING id
	`, checkedAt).Scan(&failedID); err != nil {
		t.Fatalf("insert failed proxy: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	records := []RequestRecord{
		{ProxyID: archivedID, ProxyAddress: "archived.example:80", Method: "GET", RequestedURL: "https://example.com/1", Success: false, ErrorMessage: "first", Timestamp: now},
		{ProxyID: failedID, ProxyAddress: "failed.example:80", Method: "GET", RequestedURL: "https://example.com/2", Success: true, ResponseTime: 150, StatusCode: 200, Timestamp: now.Add(time.Millisecond)},
		{ProxyID: archivedID, ProxyAddress: "archived.example:80", Method: "GET", RequestedURL: "https://example.com/3", Success: true, ResponseTime: 300, StatusCode: 204, Timestamp: now.Add(2 * time.Millisecond)},
		{ProxyID: archivedID, ProxyAddress: "archived.example:80", Method: "GET", RequestedURL: "https://example.com/4", Success: false, ErrorMessage: "tail", Timestamp: now.Add(3 * time.Millisecond)},
	}
	writer := &postgresUsageBatchWriter{repo: repo}
	if err := writer.WriteBatch(context.Background(), records); err != nil {
		t.Fatalf("write usage batch: %v", err)
	}

	assertProxyUsageRow(t, db, archivedID, "archived", 13, 5, 1, 140, checkedAt, "health evidence")
	assertProxyUsageRow(t, db, failedID, "failed", 3, 2, 0, 100, checkedAt, "youtube evidence")

	var requestCount int
	if err := db.QueryRow(context.Background(), `SELECT COUNT(*) FROM proxy_requests`).Scan(&requestCount); err != nil {
		t.Fatalf("count proxy requests: %v", err)
	}
	if requestCount != len(records) {
		t.Fatalf("proxy request count = %d, want %d", requestCount, len(records))
	}

	tracker := &UsageTracker{repo: repo}
	recent, err := tracker.GetRecentRequests(context.Background(), archivedID, 10)
	if err != nil {
		t.Fatalf("get recent requests: %v", err)
	}
	if len(recent) != 3 {
		t.Fatalf("recent request count = %d, want 3", len(recent))
	}
	if recent[0].ProxyAddress != "archived.example:80" ||
		recent[0].StatusCode != 0 ||
		recent[0].Success ||
		recent[0].ErrorMessage != "tail" {
		t.Fatalf("newest recent request = %+v", recent[0])
	}
	if recent[1].StatusCode != 204 || !recent[1].Success || recent[1].ErrorMessage != "" {
		t.Fatalf("successful recent request = %+v", recent[1])
	}
}

func TestPostgresUsageBatchRollsBackHistoryWhenStatsUpdateFails(t *testing.T) {
	repo, db := newProxyPackagePostgres(t)
	proxyID := insertSelectorProxy(t, db, "rollback.example:80", "active")
	if _, err := db.Exec(context.Background(), `
		CREATE FUNCTION reject_usage_stats_update() RETURNS trigger AS $$
		BEGIN
			RAISE EXCEPTION 'rejected usage stats update';
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER reject_usage_stats_update
		BEFORE UPDATE ON proxies
		FOR EACH ROW EXECUTE FUNCTION reject_usage_stats_update();
	`); err != nil {
		t.Fatalf("create rejection trigger: %v", err)
	}

	writer := &postgresUsageBatchWriter{repo: repo}
	err := writer.WriteBatch(context.Background(), []RequestRecord{{
		ProxyID:      proxyID,
		ProxyAddress: "rollback.example:80",
		Method:       "GET",
		RequestedURL: "https://example.com/rollback",
		Success:      true,
		ResponseTime: 25,
		StatusCode:   200,
		Timestamp:    time.Now().UTC(),
	}})
	if err == nil {
		t.Fatal("usage batch unexpectedly succeeded")
	}

	var historyCount int
	if err := db.QueryRow(context.Background(), `SELECT COUNT(*) FROM proxy_requests`).Scan(&historyCount); err != nil {
		t.Fatalf("count request history: %v", err)
	}
	if historyCount != 0 {
		t.Fatalf("request history count after rollback = %d, want 0", historyCount)
	}
	var requests int64
	if err := db.QueryRow(context.Background(), `SELECT requests FROM proxies WHERE id=$1`, proxyID).Scan(&requests); err != nil {
		t.Fatalf("read proxy requests: %v", err)
	}
	if requests != 0 {
		t.Fatalf("proxy request count after rollback = %d, want 0", requests)
	}
}

func TestLeastConnectionsRefreshRetainsAndPrunesCountsPostgres(t *testing.T) {
	repo, db := newProxyPackagePostgres(t)
	firstID := insertSelectorProxy(t, db, "a.example:80", "active")
	secondID := insertSelectorProxy(t, db, "b.example:80", "active")
	selector := NewLeastConnectionsSelector(repo, &models.RotationSettings{})
	if err := selector.Refresh(context.Background()); err != nil {
		t.Fatalf("initial refresh: %v", err)
	}

	first, err := selector.Select(context.Background())
	if err != nil {
		t.Fatalf("first select: %v", err)
	}
	second, err := selector.Select(context.Background())
	if err != nil {
		t.Fatalf("second select: %v", err)
	}
	if first.ID != firstID || second.ID != secondID {
		t.Fatalf("initial selections = [%d %d], want [%d %d]", first.ID, second.ID, firstID, secondID)
	}

	if _, err := db.Exec(context.Background(), `UPDATE proxies SET status='archived' WHERE id=$1`, firstID); err != nil {
		t.Fatalf("archive first proxy: %v", err)
	}
	thirdID := insertSelectorProxy(t, db, "c.example:80", "active")
	if err := selector.Refresh(context.Background()); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	selected, err := selector.Select(context.Background())
	if err != nil {
		t.Fatalf("select after refresh: %v", err)
	}
	if selected.ID != thirdID {
		t.Fatalf("selection after refresh = %d, want new proxy %d", selected.ID, thirdID)
	}
	if _, exists := selector.counts[firstID]; exists {
		t.Fatalf("removed proxy %d retained a selection counter", firstID)
	}
	if selector.counts[secondID] != 1 {
		t.Fatalf("surviving proxy count = %d, want 1", selector.counts[secondID])
	}
}

func assertProxyUsageRow(
	t *testing.T,
	db *pgxpool.Pool,
	proxyID int,
	wantStatus string,
	wantRequests, wantSuccessful, wantFailed int64,
	wantAverage int,
	wantLastCheck time.Time,
	wantLastError string,
) {
	t.Helper()
	var status, lastError string
	var requests, successful, failed int64
	var average int
	var lastCheck time.Time
	if err := db.QueryRow(context.Background(), `
		SELECT status, requests, successful_requests, failed_requests,
		       avg_response_time, last_check, last_error
		FROM proxies WHERE id=$1
	`, proxyID).Scan(&status, &requests, &successful, &failed, &average, &lastCheck, &lastError); err != nil {
		t.Fatalf("read proxy %d stats: %v", proxyID, err)
	}
	if status != wantStatus || requests != wantRequests || successful != wantSuccessful ||
		failed != wantFailed || average != wantAverage || !lastCheck.Equal(wantLastCheck) ||
		lastError != wantLastError {
		t.Fatalf(
			"proxy %d = status %s, requests %d, successful %d, failed %d, avg %d, last_check %s, last_error %q",
			proxyID, status, requests, successful, failed, average, lastCheck, lastError,
		)
	}
}

func insertSelectorProxy(t *testing.T, db *pgxpool.Pool, address, status string) int {
	t.Helper()
	var id int
	if err := db.QueryRow(context.Background(),
		`INSERT INTO proxies (address, status) VALUES ($1, $2) RETURNING id`,
		address, status,
	).Scan(&id); err != nil {
		t.Fatalf("insert selector proxy: %v", err)
	}
	return id
}

func newProxyPackagePostgres(t *testing.T) (*repository.ProxyRepository, *pgxpool.Pool) {
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
	schema := fmt.Sprintf("rota_proxy_package_test_%d", time.Now().UnixNano())
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
			username VARCHAR(255),
			password TEXT,
			status VARCHAR(20) NOT NULL DEFAULT 'idle',
			requests BIGINT NOT NULL DEFAULT 0,
			successful_requests BIGINT NOT NULL DEFAULT 0,
			failed_requests BIGINT NOT NULL DEFAULT 0,
			avg_response_time INTEGER NOT NULL DEFAULT 0,
			last_check TIMESTAMP,
			last_error TEXT,
			cooldown_until TIMESTAMPTZ,
			revalidation_required BOOLEAN NOT NULL DEFAULT false,
			last_health_success_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			base_health_status TEXT NOT NULL DEFAULT 'passed',
			youtube_health_status TEXT NOT NULL DEFAULT 'passed',
			next_health_check_at TIMESTAMPTZ NOT NULL DEFAULT (NOW()+INTERVAL '2 hours'),
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP NOT NULL DEFAULT NOW()
		);
		CREATE TABLE proxy_requests (
			id BIGSERIAL,
			timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
			proxy_id INTEGER REFERENCES proxies(id) ON DELETE CASCADE,
			proxy_address VARCHAR(255) NOT NULL,
			method VARCHAR(10) NOT NULL,
			url TEXT,
			status_code INTEGER,
			response_time INTEGER,
			success BOOLEAN NOT NULL,
			error TEXT
		);`
	if _, err := db.Exec(ctx, schemaSQL); err != nil {
		t.Fatalf("create proxy package test tables: %v", err)
	}
	return repository.NewProxyRepository(&database.DB{Pool: db}), db
}
