package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
)

// TestConnectViaHTTPStandalone tests the HTTP CONNECT handshake against a mock proxy.
func TestConnectViaHTTPStandalone_Success(t *testing.T) {
	// Start a mock HTTP CONNECT proxy.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		line, _ := reader.ReadString('\n')
		if !strings.HasPrefix(line, "CONNECT") {
			conn.Write([]byte("HTTP/1.1 400 Bad Request\r\n\r\n"))
			return
		}
		for {
			hdr, _ := reader.ReadString('\n')
			if strings.TrimSpace(hdr) == "" {
				break
			}
		}
		// Send 200 OK response.
		conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))

		// After tunnel is established, echo back what client sends.
		io.Copy(conn, conn)
	}()

	proxy := &models.Proxy{
		Address:  ln.Addr().String(),
		Protocol: "http",
	}

	conn, err := connectViaHTTPStandalone(proxy, "example.com:443", 10*time.Second)
	if err != nil {
		t.Fatalf("connectViaHTTPStandalone: %v", err)
	}
	defer conn.Close()

	// Tunnel is established — test bidirectional communication.
	msg := "hello tunnel"
	conn.Write([]byte(msg))

	buf := make([]byte, 256)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf[:n]) != msg {
		t.Fatalf("got %q, want %q", buf[:n], msg)
	}
}

func TestConnectViaHTTPStandalone_HTTPSProxy(t *testing.T) {
	proxyServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "CONNECT required", http.StatusMethodNotAllowed)
			return
		}
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Error("test server does not support hijacking")
			return
		}
		conn, rw, err := hijacker.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer conn.Close()
		if _, err := rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			t.Errorf("write CONNECT response: %v", err)
			return
		}
		if err := rw.Flush(); err != nil {
			t.Errorf("flush CONNECT response: %v", err)
			return
		}
		_, _ = io.Copy(conn, conn)
	}))
	defer proxyServer.Close()

	proxy := &models.Proxy{
		Address:  proxyServer.Listener.Addr().String(),
		Protocol: "https",
	}
	conn, err := connectViaHTTPStandalone(proxy, "example.com:443", 10*time.Second)
	if err != nil {
		t.Fatalf("connectViaHTTPStandalone: %v", err)
	}
	defer conn.Close()

	const message = "hello TLS proxy tunnel"
	if _, err := conn.Write([]byte(message)); err != nil {
		t.Fatalf("write tunnel: %v", err)
	}
	buf := make([]byte, len(message))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read tunnel: %v", err)
	}
	if string(buf) != message {
		t.Fatalf("got %q, want %q", buf, message)
	}
}

func TestConnectViaHTTPStandalone_Rejected(t *testing.T) {
	// Mock proxy that rejects CONNECT.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		reader.ReadString('\n') // consume CONNECT line
		for {
			hdr, _ := reader.ReadString('\n')
			if strings.TrimSpace(hdr) == "" {
				break
			}
		}
		conn.Write([]byte("HTTP/1.1 403 Forbidden\r\n\r\n"))
	}()

	proxy := &models.Proxy{
		Address:  ln.Addr().String(),
		Protocol: "http",
	}

	_, err = connectViaHTTPStandalone(proxy, "example.com:443", 10*time.Second)
	if err == nil {
		t.Fatal("expected error for rejected CONNECT")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Fatalf("expected 403 in error, got: %v", err)
	}
}

func TestConnectViaHTTPStandalone_WithAuth(t *testing.T) {
	// Mock proxy that checks Proxy-Authorization.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		reader.ReadString('\n') // CONNECT line

		hasAuth := false
		for {
			hdr, _ := reader.ReadString('\n')
			if strings.HasPrefix(hdr, "Proxy-Authorization:") {
				hasAuth = true
			}
			if strings.TrimSpace(hdr) == "" {
				break
			}
		}

		if hasAuth {
			conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
		} else {
			conn.Write([]byte("HTTP/1.1 407 Proxy Authentication Required\r\n\r\n"))
		}
	}()

	user := "testuser"
	pass := "testpass"
	proxy := &models.Proxy{
		Address:  ln.Addr().String(),
		Protocol: "http",
		Username: &user,
		Password: &pass,
	}

	conn, err := connectViaHTTPStandalone(proxy, "example.com:443", 10*time.Second)
	if err != nil {
		t.Fatalf("expected success with auth, got: %v", err)
	}
	conn.Close()
}

func TestConnectViaHTTPStandalone_ConnectionRefused(t *testing.T) {
	proxy := &models.Proxy{
		Address:  "127.0.0.1:1", // nothing listening
		Protocol: "http",
	}

	_, err := connectViaHTTPStandalone(proxy, "example.com:443", 2*time.Second)
	if err == nil {
		t.Fatal("expected error for connection refused")
	}
}

// TestConnectViaSocks5 tests SOCKS5 connection against a mock server.
// Note: This is a basic test — a real SOCKS5 handshake mock is non-trivial.
func TestConnectViaSocks5_ConnectionRefused(t *testing.T) {
	proxy := &models.Proxy{
		Address:  "127.0.0.1:1", // nothing listening
		Protocol: "socks5",
	}

	_, err := connectViaSocks5(proxy, "example.com:443")
	if err == nil {
		t.Fatal("expected error for unreachable SOCKS5 proxy")
	}
}

// TestConnectViaProxyStandalone_UnsupportedProtocol tests the protocol switch.
func TestConnectViaProxyStandalone_UnsupportedProtocol(t *testing.T) {
	proxy := &models.Proxy{
		Address:  "127.0.0.1:9999",
		Protocol: "ftp",
	}
	settings := &models.RotationSettings{Timeout: 5}

	_, err := connectViaProxyStandalone(context.Background(), proxy, "example.com:443", settings)
	if err == nil {
		t.Fatal("expected error for unsupported protocol")
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("expected 'unsupported' in error, got: %v", err)
	}
}

// TestConnectViaProxyStandalone_RoutesHTTP verifies HTTP protocol goes to HTTP handler.
func TestConnectViaProxyStandalone_RoutesHTTP(t *testing.T) {
	// Start mock that accepts CONNECT.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		for {
			line, _ := reader.ReadString('\n')
			if strings.TrimSpace(line) == "" {
				break
			}
		}
		fmt.Fprint(conn, "HTTP/1.1 200 OK\r\n\r\n")
	}()

	proxy := &models.Proxy{
		Address:  ln.Addr().String(),
		Protocol: "http",
	}
	settings := &models.RotationSettings{Timeout: 10}

	conn, err := connectViaProxyStandalone(context.Background(), proxy, "example.com:443", settings)
	if err != nil {
		t.Fatalf("expected success: %v", err)
	}
	conn.Close()
}

func TestConnectViaHTTPStandalone_DoesNotOverReadTunnelBytes(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	const tunnelData = "\x16\x03\x03\x00\x2aTLS-SERVER-HELLO-BYTES"
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		for {
			line, readErr := reader.ReadString('\n')
			if readErr != nil || strings.TrimSpace(line) == "" {
				break
			}
		}
		_, _ = conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n" + tunnelData))
	}()

	proxy := &models.Proxy{Address: listener.Addr().String(), Protocol: "http"}
	conn, err := connectViaHTTPStandalone(proxy, "example.com:443", 30*time.Second)
	if err != nil {
		t.Fatalf("connectViaHTTPStandalone: %v", err)
	}
	defer conn.Close()

	got := make([]byte, len(tunnelData))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read tunnel bytes: %v", err)
	}
	if string(got) != tunnelData {
		t.Fatalf("tunnel bytes = %q, want %q", got, tunnelData)
	}
}

func TestConnectViaHTTPStandalone_RejectsMalformedStatus(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		for {
			line, readErr := reader.ReadString('\n')
			if readErr != nil || strings.TrimSpace(line) == "" {
				break
			}
		}
		_, _ = conn.Write([]byte("HTTP/1.1 1200 Not Really OK\r\n\r\n"))
	}()

	proxy := &models.Proxy{Address: listener.Addr().String(), Protocol: "http"}
	if _, err := connectViaHTTPStandalone(proxy, "example.com:443", 30*time.Second); err == nil {
		t.Fatal("malformed status was accepted")
	}
}

func TestReadCONNECTResponseRequiresCompleteHeaderBlock(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	go func() {
		_, _ = server.Write([]byte("HTTP/1.1 200 Connection Established\r\nX-Test: value\r\n"))
		_ = server.Close()
	}()

	if _, err := readCONNECTResponse(client); err == nil {
		t.Fatal("incomplete CONNECT response was accepted")
	}
}

func TestConnectViaSocks4_PerformsSOCKS4Handshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverErr <- acceptErr
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		fixed := make([]byte, 8)
		if _, readErr := io.ReadFull(reader, fixed); readErr != nil {
			serverErr <- readErr
			return
		}
		if fixed[0] != 4 || fixed[1] != 1 {
			serverErr <- fmt.Errorf("unexpected SOCKS4 command: %v", fixed[:2])
			return
		}
		if _, readErr := reader.ReadString(0); readErr != nil {
			serverErr <- readErr
			return
		}
		_, writeErr := conn.Write([]byte{0, 90, fixed[2], fixed[3], fixed[4], fixed[5], fixed[6], fixed[7]})
		serverErr <- writeErr
	}()

	username := "rota"
	proxy := &models.Proxy{Address: listener.Addr().String(), Protocol: "socks4", Username: &username}
	conn, err := connectViaSocks4(proxy, "127.0.0.1:443")
	if err != nil {
		t.Fatalf("connectViaSocks4: %v", err)
	}
	conn.Close()
	if err := <-serverErr; err != nil {
		t.Fatalf("SOCKS4 server: %v", err)
	}
}
