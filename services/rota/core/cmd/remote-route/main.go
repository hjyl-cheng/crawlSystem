// Dedicated read-only access to the current Rota assignment. This process does
// not run migrations, allocation, reconciliation, or the proxy data plane.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alpkeskin/rota/core/internal/api"
	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/proxycontrol"
	"github.com/jackc/pgx/v5/pgxpool"
)

func router(reader api.RemoteRouteReader, token string, ping func(context.Context) error) (http.Handler, error) {
	handler, err := api.NewRemoteRouteHandler(reader, token)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/internal/v1/remote-route", handler)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		w.Header().Set("Cache-Control", "no-store")
		if ping(ctx) != nil {
			http.Error(w, "unhealthy", 503)
			return
		}
		w.WriteHeader(204)
	})
	return mux, nil
}

func run(ctx context.Context) error {
	dsn, scope := os.Getenv("REMOTE_ROUTE_DATABASE_URL"), os.Getenv("REMOTE_ROUTE_WORKLOAD_SCOPE")
	expected := os.Getenv("REMOTE_ROUTE_EXPECTED_DATABASE")
	if dsn == "" || scope == "" || expected == "" {
		return errors.New("explicit database identity and scope required")
	}
	tokenBytes, err := os.ReadFile(os.Getenv("REMOTE_ROUTE_TOKEN_FILE"))
	if err != nil {
		return errors.New("route credential unavailable")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return errors.New("invalid database configuration")
	}
	cfg.MaxConns = 4
	cfg.MinConns = 0
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "2500"
	cfg.ConnConfig.RuntimeParams["application_name"] = "remote-route-reader"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return errors.New("database unavailable")
	}
	defer pool.Close()
	var actual string
	if err = pool.QueryRow(ctx, "SELECT current_database()").Scan(&actual); err != nil || actual != expected {
		return errors.New("database identity check failed")
	}
	manager := proxycontrol.New(&database.DB{Pool: pool}, nil, nil, proxycontrol.Options{Enabled: true, WorkloadScope: scope}, nil)
	handler, err := router(manager, strings.TrimSpace(string(tokenBytes)), pool.Ping)
	if err != nil {
		return errors.New("invalid route credential")
	}
	server := &http.Server{Addr: ":3188", Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16384}
	stopped := make(chan error, 1)
	go func() { stopped <- server.ListenAndServe() }()
	fmt.Println("remote_route_reader_started")
	select {
	case err = <-stopped:
		if !errors.Is(err, http.ErrServerClosed) {
			return errors.New("route listener failed")
		}
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err = server.Shutdown(shutdown); err != nil {
			return errors.New("route shutdown failed")
		}
	}
	return nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
