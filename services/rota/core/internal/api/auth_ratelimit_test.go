package api

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientIPIgnoresForwardedHeadersByDefault(t *testing.T) {
	request := httptest.NewRequest("GET", "http://example.test", nil)
	request.RemoteAddr = "198.51.100.10:4321"
	request.Header.Set("X-Forwarded-For", "203.0.113.20")
	request.Header.Set("X-Real-IP", "203.0.113.21")

	limiter := &authRateLimiter{}
	if got := limiter.clientIP(request); got != "198.51.100.10" {
		t.Fatalf("clientIP() = %q, want direct peer", got)
	}
}

func TestClientIPUsesValidatedForwardedHeadersWhenTrusted(t *testing.T) {
	request := httptest.NewRequest("GET", "http://example.test", nil)
	request.RemoteAddr = "198.51.100.10:4321"
	request.Header.Set("X-Forwarded-For", " 203.0.113.20, 198.51.100.10")

	limiter := &authRateLimiter{trustProxyHeaders: true}
	if got := limiter.clientIP(request); got != "203.0.113.20" {
		t.Fatalf("clientIP() = %q, want forwarded client", got)
	}
}

func TestClientIPFallsBackWhenForwardedHeadersAreInvalid(t *testing.T) {
	request := httptest.NewRequest("GET", "http://example.test", nil)
	request.RemoteAddr = "[2001:db8::10]:4321"
	request.Header.Set("X-Forwarded-For", "not-an-ip")

	limiter := &authRateLimiter{trustProxyHeaders: true}
	if got := limiter.clientIP(request); got != "2001:db8::10" {
		t.Fatalf("clientIP() = %q, want direct IPv6 peer", got)
	}
}

func TestAuthRateLimiterRunStopsWithOwner(t *testing.T) {
	limiter := &authRateLimiter{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		limiter.Run(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("rate limiter cleanup did not stop")
	}
}
