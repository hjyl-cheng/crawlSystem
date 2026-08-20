package proxy

import (
	"bufio"
	"context"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/repository"
	"github.com/alpkeskin/rota/core/pkg/logger"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

func TestManagedRouteActivationSwitchesCONNECTAndSurvivesDataPlaneRestart(t *testing.T) {
	db := newManagedActivationPostgres(t)
	ctx := context.Background()
	exitA := newMarkedCONNECTProxy(t, "exit-a:")
	exitB := newMarkedCONNECTProxy(t, "exit-b:")

	proxyA := insertManagedRouteProxy(t, db, exitA.Address())
	proxyB := insertManagedRouteProxy(t, db, exitB.Address())
	poolID := insertManagedRoutePool(t, db, proxyA)
	const (
		oldUsername = "bullmq-channel-01-g1"
		newUsername = "bullmq-channel-01-g2"
		password    = "managed-route-password"
	)
	insertManagedRouteUser(t, db, oldUsername, password, poolID)

	dataPlane, proxyAddress := newManagedRouteDataPlane(t, db)
	oldTunnel := openManagedCONNECT(t, proxyAddress, oldUsername, password)
	assertMarkedTunnel(t, oldTunnel, "exit-a:", "first")
	secondTunnel := openManagedCONNECT(t, proxyAddress, oldUsername, password)
	assertMarkedTunnel(t, secondTunnel, "exit-a:", "second")
	_ = secondTunnel.Close()

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin replacement route: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE proxy_users SET username=$1,updated_at=NOW() WHERE username=$2
	`, newUsername, oldUsername); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("rotate managed username: %v", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM pool_proxies WHERE pool_id=$1`, poolID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("remove previous managed route: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO pool_proxies (pool_id,proxy_id) VALUES ($1,$2)
	`, poolID, proxyB); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("bind replacement managed route: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit replacement route: %v", err)
	}
	activationCtx, cancelActivation := context.WithTimeout(ctx, 2*time.Second)
	defer cancelActivation()
	if err := dataPlane.ActivateProxyUser(
		activationCtx, oldUsername, newUsername, proxyB,
	); err != nil {
		t.Fatalf("activate replacement route: %v", err)
	}
	_ = oldTunnel.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
	if _, err := oldTunnel.Read(make([]byte, 1)); err == nil {
		t.Fatal("old managed tunnel remained usable after route activation")
	}
	_ = oldTunnel.Close()

	assertCONNECTStatus(t, proxyAddress, oldUsername, password, http.StatusProxyAuthRequired)
	newTunnel := openManagedCONNECT(t, proxyAddress, newUsername, password)
	assertMarkedTunnel(t, newTunnel, "exit-b:", "third")
	_ = newTunnel.Close()
	fourthTunnel := openManagedCONNECT(t, proxyAddress, newUsername, password)
	assertMarkedTunnel(t, fourthTunnel, "exit-b:", "fourth")
	_ = fourthTunnel.Close()

	dataPlane.testServer.Close()
	restartedDataPlane, restartedAddress := newManagedRouteDataPlane(t, db)
	assertCONNECTStatus(t, restartedAddress, oldUsername, password, http.StatusProxyAuthRequired)
	restartedTunnel := openManagedCONNECT(t, restartedAddress, newUsername, password)
	assertMarkedTunnel(t, restartedTunnel, "exit-b:", "after-restart")
	_ = restartedTunnel.Close()
	restartedDataPlane.testServer.Close()
}

type managedRouteDataPlane struct {
	*Server
	testServer *httptest.Server
}

func newManagedRouteDataPlane(t *testing.T, pool *pgxpool.Pool) (*managedRouteDataPlane, string) {
	t.Helper()
	db := &database.DB{Pool: pool}
	log := logger.New("error")
	rotation := &models.RotationSettings{Timeout: 1}
	userAuth := NewUserAuthMiddleware(
		repository.NewUserRepository(db),
		repository.NewPoolRepository(db),
		db,
		NewAuthMiddleware(models.AuthenticationSettings{}),
		rotation,
		log,
	)
	tracker := newUsageTracker(nil, &recordingUsageWriter{}, log, testUsageTrackerConfig())
	cleanupUsageTracker(t, tracker)
	handler := NewUpstreamProxyHandler(nil, tracker, rotation, log)
	router := &proxyRouter{
		upstream:    handler,
		userAuthMw:  userAuth,
		rateLimitMw: NewRateLimitMiddleware(models.RateLimitSettings{}),
		logger:      log,
	}
	testServer := httptest.NewServer(router)
	t.Cleanup(testServer.Close)
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = log.Shutdown(shutdownCtx)
	})
	return &managedRouteDataPlane{
		Server:     &Server{userAuthMw: userAuth, handler: handler},
		testServer: testServer,
	}, strings.TrimPrefix(testServer.URL, "http://")
}

type markedCONNECTProxy struct {
	listener net.Listener
	marker   string
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	done     chan struct{}
	wg       sync.WaitGroup
}

func newMarkedCONNECTProxy(t *testing.T, marker string) *markedCONNECTProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for %s proxy: %v", marker, err)
	}
	proxy := &markedCONNECTProxy{
		listener: listener,
		marker:   marker,
		conns:    make(map[net.Conn]struct{}),
		done:     make(chan struct{}),
	}
	proxy.wg.Add(1)
	go proxy.accept()
	t.Cleanup(proxy.Close)
	return proxy
}

func (p *markedCONNECTProxy) Address() string { return p.listener.Addr().String() }

func (p *markedCONNECTProxy) accept() {
	defer p.wg.Done()
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			select {
			case <-p.done:
				return
			default:
				continue
			}
		}
		p.mu.Lock()
		p.conns[conn] = struct{}{}
		p.mu.Unlock()
		p.wg.Add(1)
		go p.handle(conn)
	}
}

func (p *markedCONNECTProxy) handle(conn net.Conn) {
	defer p.wg.Done()
	defer func() {
		_ = conn.Close()
		p.mu.Lock()
		delete(p.conns, conn)
		p.mu.Unlock()
	}()
	reader := bufio.NewReader(conn)
	request, err := http.ReadRequest(reader)
	if err != nil || request.Method != http.MethodConnect {
		_, _ = io.WriteString(conn, "HTTP/1.1 400 Bad Request\r\n\r\n")
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
	buffer := make([]byte, 1024)
	for {
		n, readErr := reader.Read(buffer)
		if n > 0 {
			if _, err := io.WriteString(conn, p.marker+string(buffer[:n])); err != nil {
				return
			}
		}
		if readErr != nil {
			return
		}
	}
}

func (p *markedCONNECTProxy) Close() {
	select {
	case <-p.done:
		return
	default:
		close(p.done)
	}
	_ = p.listener.Close()
	p.mu.Lock()
	for conn := range p.conns {
		_ = conn.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

func insertManagedRouteProxy(t *testing.T, db *pgxpool.Pool, address string) int {
	t.Helper()
	var proxyID int
	if err := db.QueryRow(context.Background(), `
		INSERT INTO proxies (address,protocol,status) VALUES ($1,'http','active') RETURNING id
	`, address).Scan(&proxyID); err != nil {
		t.Fatalf("insert managed route proxy: %v", err)
	}
	return proxyID
}

func insertManagedRoutePool(t *testing.T, db *pgxpool.Pool, proxyID int) int {
	t.Helper()
	var poolID int
	if err := db.QueryRow(context.Background(), `
		INSERT INTO proxy_pools (name) VALUES ('managed-route-e2e') RETURNING id
	`).Scan(&poolID); err != nil {
		t.Fatalf("insert managed route pool: %v", err)
	}
	if _, err := db.Exec(context.Background(), `
		INSERT INTO pool_proxies (pool_id,proxy_id) VALUES ($1,$2)
	`, poolID, proxyID); err != nil {
		t.Fatalf("bind initial managed route: %v", err)
	}
	return poolID
}

func insertManagedRouteUser(
	t *testing.T,
	db *pgxpool.Pool,
	username, password string,
	poolID int,
) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("hash managed route password: %v", err)
	}
	if _, err := db.Exec(context.Background(), `
		INSERT INTO proxy_users (
		  username,password_hash,enabled,main_pool_id,fallback_pool_ids,max_retries
		) VALUES ($1,$2,true,$3,'{}',1)
	`, username, string(hash), poolID); err != nil {
		t.Fatalf("insert managed route user: %v", err)
	}
}

func openManagedCONNECT(t *testing.T, proxyAddress, username, password string) net.Conn {
	t.Helper()
	conn, status := dialManagedCONNECT(t, proxyAddress, username, password)
	if status != http.StatusOK {
		_ = conn.Close()
		t.Fatalf("CONNECT status = %d, want %d", status, http.StatusOK)
	}
	return conn
}

func assertCONNECTStatus(t *testing.T, proxyAddress, username, password string, expected int) {
	t.Helper()
	conn, status := dialManagedCONNECT(t, proxyAddress, username, password)
	_ = conn.Close()
	if status != expected {
		t.Fatalf("CONNECT status = %d, want %d", status, expected)
	}
}

func dialManagedCONNECT(t *testing.T, proxyAddress, username, password string) (net.Conn, int) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", proxyAddress, time.Second)
	if err != nil {
		t.Fatalf("dial managed proxy: %v", err)
	}
	auth := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	request := strings.Join([]string{
		"CONNECT integration.invalid:443 HTTP/1.1",
		"Host: integration.invalid:443",
		"Proxy-Authorization: Basic " + auth,
		"",
		"",
	}, "\r\n")
	if _, err := io.WriteString(conn, request); err != nil {
		_ = conn.Close()
		t.Fatalf("write managed CONNECT: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	response, err := readCONNECTResponse(conn)
	if err != nil {
		_ = conn.Close()
		t.Fatalf("read managed CONNECT response: %v", err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	return conn, response.StatusCode
}

func assertMarkedTunnel(t *testing.T, conn net.Conn, marker, payload string) {
	t.Helper()
	if _, err := io.WriteString(conn, payload); err != nil {
		t.Fatalf("write tunnel payload: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	got := make([]byte, len(marker)+len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read tunnel marker: %v", err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	if string(got) != marker+payload {
		t.Fatalf("tunnel response = %q, want %q", got, marker+payload)
	}
}
