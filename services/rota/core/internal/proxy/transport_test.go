package proxy

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
)

func TestClearTransportCacheClosesIdleConnectionsAndDropsEntries(t *testing.T) {
	ClearTransportCache()
	t.Cleanup(ClearTransportCache)

	idle := make(chan struct{}, 1)
	closed := make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateIdle:
			select {
			case idle <- struct{}{}:
			default:
			}
		case http.StateClosed:
			select {
			case closed <- struct{}{}:
			default:
			}
		}
	}
	server.Start()
	defer server.Close()

	proxyModel := &models.Proxy{Address: "127.0.0.1:8180", Protocol: "http"}
	oldTransport := &http.Transport{}
	transportCache.Store(transportCacheKey(proxyModel), oldTransport)
	client := &http.Client{Transport: oldTransport}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("make request: %v", err)
	}
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		response.Body.Close()
		t.Fatalf("read response: %v", err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatalf("close response: %v", err)
	}

	select {
	case <-idle:
	case <-time.After(2 * time.Second):
		t.Fatal("connection did not become idle")
	}

	ClearTransportCache()

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("cached transport's idle connection was not closed")
	}
	newTransport, err := GetOrCreateTransport(proxyModel)
	if err != nil {
		t.Fatalf("recreate transport: %v", err)
	}
	if newTransport == oldTransport {
		t.Fatal("cache returned the cleared transport")
	}
}

func TestGetOrCreateTransport_CachesPerProxy(t *testing.T) {
	p := &models.Proxy{
		ID:       1,
		Address:  "127.0.0.1:8080",
		Protocol: "http",
	}

	t1, err := GetOrCreateTransport(p)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}

	t2, err := GetOrCreateTransport(p)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}

	if t1 != t2 {
		t.Fatal("expected same transport instance from cache")
	}
}

func TestGetOrCreateTransport_DifferentProxies(t *testing.T) {
	p1 := &models.Proxy{
		ID:       1,
		Address:  "127.0.0.1:8081",
		Protocol: "http",
	}
	p2 := &models.Proxy{
		ID:       2,
		Address:  "127.0.0.1:8082",
		Protocol: "http",
	}

	t1, err := GetOrCreateTransport(p1)
	if err != nil {
		t.Fatalf("proxy1: %v", err)
	}

	t2, err := GetOrCreateTransport(p2)
	if err != nil {
		t.Fatalf("proxy2: %v", err)
	}

	if t1 == t2 {
		t.Fatal("expected different transport instances for different proxies")
	}
}

func TestGetOrCreateTransport_DifferentCredentials(t *testing.T) {
	firstPassword := "first"
	secondPassword := "second"
	p1 := &models.Proxy{Address: "127.0.0.1:8181", Protocol: "http", Password: &firstPassword}
	p2 := &models.Proxy{Address: "127.0.0.1:8181", Protocol: "http", Password: &secondPassword}

	t1, err := GetOrCreateTransport(p1)
	if err != nil {
		t.Fatalf("first credential: %v", err)
	}
	t2, err := GetOrCreateTransport(p2)
	if err != nil {
		t.Fatalf("second credential: %v", err)
	}
	if t1 == t2 {
		t.Fatal("expected credentials to participate in the transport cache key")
	}
}

func TestCreateProxyTransport_HTTP(t *testing.T) {
	p := &models.Proxy{
		Address:  "127.0.0.1:3128",
		Protocol: "http",
	}
	tr, err := CreateProxyTransport(p)
	if err != nil {
		t.Fatalf("CreateProxyTransport: %v", err)
	}
	if tr.Proxy == nil {
		t.Fatal("HTTP proxy transport should have Proxy function set")
	}
}

func TestCreateProxyTransport_SOCKS5(t *testing.T) {
	p := &models.Proxy{
		Address:  "127.0.0.1:1080",
		Protocol: "socks5",
	}
	tr, err := CreateProxyTransport(p)
	if err != nil {
		t.Fatalf("CreateProxyTransport: %v", err)
	}
	if tr.Dial == nil {
		t.Fatal("SOCKS5 transport should have Dial function set")
	}
}

func TestCreateProxyTransport_UnsupportedProtocol(t *testing.T) {
	p := &models.Proxy{
		Address:  "127.0.0.1:9999",
		Protocol: "ftp",
	}
	_, err := CreateProxyTransport(p)
	if err == nil {
		t.Fatal("expected error for unsupported protocol")
	}
}

func TestCreateProxyTransport_VLESSRequiresCredential(t *testing.T) {
	p := &models.Proxy{Address: "node.example.com:443", Protocol: "vless"}
	_, err := CreateProxyTransport(p)
	if err == nil {
		t.Fatal("expected missing VLESS credential error")
	}
}

func TestVLESSDialContextRejectsEndpointMismatchWithoutLeakingCredential(t *testing.T) {
	credential := testVLESSURI
	p := &models.Proxy{
		Address:  "other.example.com:443",
		Protocol: "vless",
		Password: &credential,
	}
	_, err := shareNodeDialContext(p)
	if err == nil {
		t.Fatal("expected endpoint mismatch error")
	}
	if strings.Contains(err.Error(), credential) || strings.Contains(err.Error(), "11111111-1111-4111-8111-111111111111") {
		t.Fatalf("error leaked credential: %v", err)
	}
}

func TestCreateProxyTransport_WithAuth(t *testing.T) {
	user := "myuser"
	pass := "mypass"
	p := &models.Proxy{
		Address:  "127.0.0.1:3128",
		Protocol: "http",
		Username: &user,
		Password: &pass,
	}
	tr, err := CreateProxyTransport(p)
	if err != nil {
		t.Fatalf("CreateProxyTransport with auth: %v", err)
	}
	if tr.Proxy == nil {
		t.Fatal("should have Proxy set")
	}
}
