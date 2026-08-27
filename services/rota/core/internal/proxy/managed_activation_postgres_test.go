package proxy

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/repository"
	"github.com/alpkeskin/rota/core/pkg/logger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

func TestActivateProxyUserRetiresOldTunnelsAndWarmsExactNewRoute(t *testing.T) {
	db := newManagedActivationPostgres(t)
	ctx := context.Background()

	var targetProxyID, otherProxyID, poolID int
	if err := db.QueryRow(ctx, `
		INSERT INTO proxies (address,status) VALUES ('target.example:8080','active') RETURNING id
	`).Scan(&targetProxyID); err != nil {
		t.Fatalf("insert target proxy: %v", err)
	}
	if err := db.QueryRow(ctx, `
		INSERT INTO proxies (address,status) VALUES ('other.example:8080','active') RETURNING id
	`).Scan(&otherProxyID); err != nil {
		t.Fatalf("insert other proxy: %v", err)
	}
	if err := db.QueryRow(ctx, `
		INSERT INTO proxy_pools (name) VALUES ('managed-slot') RETURNING id
	`).Scan(&poolID); err != nil {
		t.Fatalf("insert managed pool: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO pool_proxies (pool_id,proxy_id) VALUES ($1,$2)`, poolID, targetProxyID); err != nil {
		t.Fatalf("bind target proxy: %v", err)
	}
	password := "managed-password"
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("hash managed password: %v", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO proxy_users (
		  username,password_hash,enabled,main_pool_id,fallback_pool_ids,max_retries
		) VALUES ('managed-new',$1,true,$2,'{}',1)
	`, string(passwordHash), poolID); err != nil {
		t.Fatalf("insert managed proxy user: %v", err)
	}

	databaseHandle := &database.DB{Pool: db}
	log := logger.New("error")
	middleware := NewUserAuthMiddleware(
		repository.NewUserRepository(databaseHandle),
		repository.NewPoolRepository(databaseHandle),
		databaseHandle,
		NewAuthMiddleware(models.AuthenticationSettings{}),
		&models.RotationSettings{},
		log,
	)
	handler := NewUpstreamProxyHandler(nil, nil, &models.RotationSettings{}, log)
	server := &Server{userAuthMw: middleware, handler: handler}
	server.RequireRouteActivationRegistry()
	if err := server.RebuildRouteActivationRegistry(ctx, nil); err != nil {
		t.Fatalf("initialize Route activation registry: %v", err)
	}

	oldClient, oldPeer := net.Pipe()
	oldUpstream, oldUpstreamPeer := net.Pipe()
	defer oldPeer.Close()
	defer oldUpstreamPeer.Close()
	oldTunnel, accepted := handler.beginTunnel(oldClient, oldUpstream, "managed-old")
	if !accepted {
		t.Fatal("old managed tunnel was rejected")
	}
	oldDone := make(chan struct{})
	go func() {
		var buffer [1]byte
		_, _ = oldClient.Read(buffer[:])
		handler.endTunnel(oldTunnel)
		close(oldDone)
	}()

	activateCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if _, err := server.BeginProxyUserActivation(
		activateCtx, "managed-old", "managed-new", targetProxyID, "", "claim-1",
	); err != nil {
		t.Fatalf("activate managed proxy user: %v", err)
	}
	if err := server.CommitProxyUserActivation(activateCtx, "managed-new", "claim-1"); err != nil {
		t.Fatalf("commit managed proxy user activation: %v", err)
	}
	select {
	case <-oldDone:
	default:
		t.Fatal("activation returned before old tunnel retired")
	}

	middleware.mu.RLock()
	entry, prepared := middleware.cache["managed-new"]
	middleware.mu.RUnlock()
	if !prepared || entry.chain == nil || !entry.chain.HasExactProxy(targetProxyID) {
		t.Fatalf("prepared managed chain = %#v", entry.chain)
	}
	if err := server.RetireProxyUser(ctx, "managed-new"); err != nil {
		t.Fatalf("retire new proxy user after uncertain Finalize: %v", err)
	}
	if _, err := server.BeginProxyUserActivation(
		ctx, "", "managed-new", targetProxyID, "", "claim-2",
	); err != nil {
		t.Fatalf("reactivate proxy user after transient Finalize failure: %v", err)
	}
	if err := server.CommitProxyUserActivation(ctx, "managed-new", "claim-2"); err != nil {
		t.Fatalf("commit reactivated proxy user: %v", err)
	}
	retryClient, retryPeer := net.Pipe()
	retryUpstream, retryUpstreamPeer := net.Pipe()
	retriedTunnel, accepted := handler.beginTunnel(retryClient, retryUpstream, "managed-new")
	if !accepted {
		t.Fatal("reactivated managed proxy user remained permanently retired")
	}
	handler.endTunnel(retriedTunnel)
	_ = retryClient.Close()
	_ = retryPeer.Close()
	_ = retryUpstream.Close()
	_ = retryUpstreamPeer.Close()

	if _, err := db.Exec(ctx, `INSERT INTO pool_proxies (pool_id,proxy_id) VALUES ($1,$2)`, poolID, otherProxyID); err != nil {
		t.Fatalf("add illegal second proxy: %v", err)
	}
	if _, err := server.BeginProxyUserActivation(
		ctx, "", "managed-new", targetProxyID, "claim-2", "claim-3",
	); err == nil {
		t.Fatal("activation accepted a managed Pool with more than one route")
	}
}

func newManagedActivationPostgres(t *testing.T) *pgxpool.Pool {
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
	schema := fmt.Sprintf("rota_managed_activation_test_%d", time.Now().UnixNano())
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
		  address TEXT NOT NULL,
		  protocol TEXT NOT NULL DEFAULT 'http',
		  username TEXT,
		  password TEXT,
		  status TEXT NOT NULL,
		  requests BIGINT NOT NULL DEFAULT 0,
		  successful_requests BIGINT NOT NULL DEFAULT 0,
		  failed_requests BIGINT NOT NULL DEFAULT 0,
		  avg_response_time INTEGER NOT NULL DEFAULT 0,
		  last_check TIMESTAMPTZ,
		  last_error TEXT,
		  cooldown_until TIMESTAMPTZ,
		  revalidation_required BOOLEAN NOT NULL DEFAULT false,
		  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE TABLE proxy_pools (
		  id SERIAL PRIMARY KEY,
		  name TEXT NOT NULL,
		  description TEXT NOT NULL DEFAULT '',
		  country_code TEXT,
		  region_name TEXT,
		  city_name TEXT,
		  rotation_method TEXT NOT NULL DEFAULT 'roundrobin',
		  stick_count INTEGER NOT NULL DEFAULT 1,
		  health_check_url TEXT NOT NULL DEFAULT '',
		  health_check_cron TEXT NOT NULL DEFAULT '',
		  health_check_enabled BOOLEAN NOT NULL DEFAULT false,
		  auto_sync BOOLEAN NOT NULL DEFAULT false,
		  sync_mode TEXT NOT NULL DEFAULT 'manual',
		  enabled BOOLEAN NOT NULL DEFAULT true,
		  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE TABLE pool_proxies (
		  pool_id INTEGER NOT NULL REFERENCES proxy_pools(id),
		  proxy_id INTEGER NOT NULL REFERENCES proxies(id),
		  added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		  PRIMARY KEY (pool_id,proxy_id)
		);
		CREATE TABLE proxy_users (
		  id SERIAL PRIMARY KEY,
		  username TEXT NOT NULL UNIQUE,
		  password_hash TEXT NOT NULL,
		  enabled BOOLEAN NOT NULL DEFAULT true,
		  main_pool_id INTEGER REFERENCES proxy_pools(id),
		  fallback_pool_ids INTEGER[] NOT NULL DEFAULT '{}',
		  max_retries INTEGER NOT NULL DEFAULT 1,
		  requests_per_minute INTEGER NOT NULL DEFAULT 0,
		  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE TABLE proxy_running_slots (
		  pool_id INTEGER NOT NULL REFERENCES proxy_pools(id),
		  proxy_id INTEGER REFERENCES proxies(id),
		  worker_id TEXT,
		  lease_until TIMESTAMPTZ
		);`
	if _, err := db.Exec(ctx, schemaSQL); err != nil {
		t.Fatalf("create managed activation schema: %v", err)
	}
	return db
}
