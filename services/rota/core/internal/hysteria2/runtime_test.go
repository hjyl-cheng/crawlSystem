package hysteria2

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	hyserver "github.com/apernet/hysteria/core/v2/server"
)

type fakeClient struct {
	mu       sync.Mutex
	dials    int
	closes   int
	dialFunc func(string) (net.Conn, error)
}

func (c *fakeClient) TCP(address string) (net.Conn, error) {
	c.mu.Lock()
	c.dials++
	c.mu.Unlock()
	if c.dialFunc != nil {
		return c.dialFunc(address)
	}
	client, server := net.Pipe()
	_ = server.Close()
	return client, nil
}

func (c *fakeClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closes++
	return nil
}

func TestClientConfigUsesPortHoppingTLSAndCertificatePin(t *testing.T) {
	certificate := []byte("test certificate")
	hash := sha256.Sum256(certificate)
	pin := hex.EncodeToString(hash[:])
	node, err := Parse("hysteria2://paid-secret@127.0.0.1:443/?mport=20000-20002&sni=cdn.example.com&pinSHA256=" + pin)
	if err != nil {
		t.Fatal(err)
	}
	config, err := node.clientConfig()
	if err != nil {
		t.Fatalf("clientConfig: %v", err)
	}
	if config.ServerAddr.Network() != "udphop" || config.Auth != "paid-secret" {
		t.Fatalf("server=%v auth was not retained", config.ServerAddr)
	}
	if config.TLSConfig.ServerName != "cdn.example.com" || config.TLSConfig.VerifyPeerCertificate == nil {
		t.Fatalf("TLS config = %#v", config.TLSConfig)
	}
	if err := config.TLSConfig.VerifyPeerCertificate([][]byte{certificate}, nil); err != nil {
		t.Fatalf("matching certificate pin failed: %v", err)
	}
	if err := config.TLSConfig.VerifyPeerCertificate([][]byte{[]byte("other")}, nil); err == nil {
		t.Fatal("mismatched certificate pin passed")
	}
}

func TestRuntimeCacheSharesClientAndClosesIt(t *testing.T) {
	client := &fakeClient{}
	starts := 0
	cache := newRuntimeCache(2, func(Node) (*runtime, error) {
		starts++
		return &runtime{client: client}, nil
	})
	node := Node{identity: "shared"}
	for range 2 {
		conn, err := cache.dialer(node)(context.Background(), "tcp", "example.com:443")
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if starts != 1 {
		t.Fatalf("runtime starts = %d, want 1", starts)
	}
	if err := cache.CloseAll(); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.dials != 2 || client.closes != 1 {
		t.Fatalf("client calls = dials:%d closes:%d", client.dials, client.closes)
	}
}

func TestDialContextReturnsWhenCallerTimesOut(t *testing.T) {
	release := make(chan struct{})
	client := &fakeClient{dialFunc: func(string) (net.Conn, error) {
		<-release
		return nil, errors.New("dial stopped")
	}}
	cache := newRuntimeCache(1, func(Node) (*runtime, error) {
		return &runtime{client: client}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := cache.dialer(Node{identity: "timeout"})(ctx, "tcp", "example.com:443")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dial error = %v", err)
	}
	close(release)
}

func TestRuntimeErrorsRedactAuthenticationAndObfsSecrets(t *testing.T) {
	node := Node{
		identity:     "redaction",
		credential:   "hysteria2://top-secret@example.com:443/",
		auth:         "top-secret",
		obfsPassword: "obfs-secret",
	}
	cache := newRuntimeCache(1, func(Node) (*runtime, error) {
		return &runtime{client: &fakeClient{dialFunc: func(string) (net.Conn, error) {
			return nil, errors.New("top-secret obfs-secret")
		}}}, nil
	})
	_, err := cache.dialer(node)(context.Background(), "tcp", "example.com:443")
	if err == nil || strings.Contains(err.Error(), "top-secret") || strings.Contains(err.Error(), "obfs-secret") {
		t.Fatalf("error was not redacted: %v", err)
	}
}

type staticAuthenticator string

func (a staticAuthenticator) Authenticate(_ net.Addr, auth string, _ uint64) (bool, string) {
	return auth == string(a), "test-client"
}

func TestDialContextTraversesRealHysteria2Tunnel(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "through-hysteria2")
	}))
	defer target.Close()

	certificateSource := httptest.NewTLSServer(http.NotFoundHandler())
	certificate := certificateSource.TLS.Certificates[0]
	certificateSource.Close()
	certificateHash := sha256.Sum256(certificate.Certificate[0])

	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	server, err := hyserver.NewServer(&hyserver.Config{
		TLSConfig:     hyserver.TLSConfig{Certificates: []tls.Certificate{certificate}},
		Conn:          udpConn,
		DisableUDP:    true,
		Authenticator: staticAuthenticator("test-auth"),
	})
	if err != nil {
		udpConn.Close()
		t.Fatalf("start Hysteria2 server: %v", err)
	}
	defer server.Close()
	go func() { _ = server.Serve() }()

	serverAddress := udpConn.LocalAddr().String()
	_, serverPort, err := net.SplitHostPort(serverAddress)
	if err != nil {
		t.Fatal(err)
	}
	node, err := Parse(fmt.Sprintf(
		"hysteria2://test-auth@%s/?mport=%s&sni=localhost&insecure=1&pinSHA256=%s",
		serverAddress, serverPort, hex.EncodeToString(certificateHash[:]),
	))
	if err != nil {
		t.Fatal(err)
	}
	cache := newRuntimeCache(1, startRuntime)
	defer cache.CloseAll()
	transport := &http.Transport{DialContext: cache.dialer(node)}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}

	response, err := client.Get(target.URL)
	if err != nil {
		t.Fatalf("request through Hysteria2: %v", err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || string(body) != "through-hysteria2" {
		t.Fatalf("response = status:%d body:%q", response.StatusCode, body)
	}
}
