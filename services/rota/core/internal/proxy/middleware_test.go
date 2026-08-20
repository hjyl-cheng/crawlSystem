package proxy

import (
	"encoding/base64"
	"net/http"
	"sync"
	"testing"

	"github.com/alpkeskin/rota/core/internal/models"
)

func basicAuth(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

// ── AuthMiddleware ──────────────────────────────────────────────────────────

func TestAuthMiddleware_Disabled(t *testing.T) {
	m := NewAuthMiddleware(models.AuthenticationSettings{Enabled: false})
	req, _ := http.NewRequest("GET", "http://example.com", nil)

	_, resp := m.HandleRequest(req)
	if resp != nil {
		t.Fatalf("expected nil reject, got status %d", resp.StatusCode)
	}
}

func TestAuthMiddleware_ValidCredentials(t *testing.T) {
	m := NewAuthMiddleware(models.AuthenticationSettings{
		Enabled:  true,
		Username: "admin",
		Password: "secret",
	})

	req, _ := http.NewRequest("GET", "http://example.com", nil)
	req.Header.Set("Proxy-Authorization", basicAuth("admin", "secret"))

	_, resp := m.HandleRequest(req)
	if resp != nil {
		t.Fatalf("expected auth to pass, got status %d", resp.StatusCode)
	}
}

func TestAuthMiddleware_InvalidCredentials(t *testing.T) {
	m := NewAuthMiddleware(models.AuthenticationSettings{
		Enabled:  true,
		Username: "admin",
		Password: "secret",
	})

	req, _ := http.NewRequest("GET", "http://example.com", nil)
	req.Header.Set("Proxy-Authorization", basicAuth("admin", "wrong"))

	_, resp := m.HandleRequest(req)
	if resp == nil || resp.StatusCode != http.StatusProxyAuthRequired {
		t.Fatalf("expected 407, got %v", resp)
	}
}

func TestAuthMiddleware_MissingHeader(t *testing.T) {
	m := NewAuthMiddleware(models.AuthenticationSettings{
		Enabled:  true,
		Username: "admin",
		Password: "secret",
	})

	req, _ := http.NewRequest("GET", "http://example.com", nil)

	_, resp := m.HandleRequest(req)
	if resp == nil || resp.StatusCode != http.StatusProxyAuthRequired {
		t.Fatalf("expected 407, got %v", resp)
	}
}

func TestAuthMiddleware_UpdateSettings(t *testing.T) {
	m := NewAuthMiddleware(models.AuthenticationSettings{
		Enabled:  true,
		Username: "admin",
		Password: "old",
	})

	// Old password works
	req, _ := http.NewRequest("GET", "http://example.com", nil)
	req.Header.Set("Proxy-Authorization", basicAuth("admin", "old"))
	_, resp := m.HandleRequest(req)
	if resp != nil {
		t.Fatal("old password should work")
	}

	// Update credentials
	m.UpdateSettings(models.AuthenticationSettings{
		Enabled:  true,
		Username: "admin",
		Password: "new",
	})

	// Old password fails
	req2, _ := http.NewRequest("GET", "http://example.com", nil)
	req2.Header.Set("Proxy-Authorization", basicAuth("admin", "old"))
	_, resp2 := m.HandleRequest(req2)
	if resp2 == nil || resp2.StatusCode != http.StatusProxyAuthRequired {
		t.Fatal("old password should fail after update")
	}

	// New password works
	req3, _ := http.NewRequest("GET", "http://example.com", nil)
	req3.Header.Set("Proxy-Authorization", basicAuth("admin", "new"))
	_, resp3 := m.HandleRequest(req3)
	if resp3 != nil {
		t.Fatal("new password should work")
	}
}

func TestAuthMiddleware_StripHeader(t *testing.T) {
	m := NewAuthMiddleware(models.AuthenticationSettings{
		Enabled:  true,
		Username: "admin",
		Password: "secret",
	})

	req, _ := http.NewRequest("GET", "http://example.com", nil)
	req.Header.Set("Proxy-Authorization", basicAuth("admin", "secret"))

	req, _ = m.HandleRequest(req)
	if req.Header.Get("Proxy-Authorization") != "" {
		t.Fatal("Proxy-Authorization header should be stripped after auth")
	}
}

// ── RateLimitMiddleware ─────────────────────────────────────────────────────

func TestRateLimitMiddleware_Disabled(t *testing.T) {
	m := NewRateLimitMiddleware(models.RateLimitSettings{Enabled: false})
	req, _ := http.NewRequest("GET", "http://example.com", nil)

	_, resp := m.HandleRequest(req)
	if resp != nil {
		t.Fatalf("expected nil reject, got status %d", resp.StatusCode)
	}
}

func TestRateLimitMiddleware_AllowsUnderLimit(t *testing.T) {
	m := NewRateLimitMiddleware(models.RateLimitSettings{
		Enabled:     true,
		Interval:    1,   // 1 second
		MaxRequests: 100, // 100 per second
	})

	req, _ := http.NewRequest("GET", "http://example.com", nil)
	req.RemoteAddr = "192.168.1.1:12345"

	// First 10 requests should pass easily
	for i := 0; i < 10; i++ {
		_, resp := m.HandleRequest(req)
		if resp != nil {
			t.Fatalf("request %d should be allowed, got status %d", i, resp.StatusCode)
		}
	}
}

func TestRateLimitMiddleware_BlocksOverLimit(t *testing.T) {
	m := NewRateLimitMiddleware(models.RateLimitSettings{
		Enabled:     true,
		Interval:    1, // 1 second
		MaxRequests: 5, // only 5 burst
	})

	req, _ := http.NewRequest("GET", "http://example.com", nil)
	req.RemoteAddr = "10.0.0.1:12345"

	// Exhaust the burst
	for i := 0; i < 5; i++ {
		m.HandleRequest(req)
	}

	// Next request should be blocked
	_, resp := m.HandleRequest(req)
	if resp == nil || resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %v", resp)
	}
}

func TestRateLimitMiddleware_PerIP(t *testing.T) {
	m := NewRateLimitMiddleware(models.RateLimitSettings{
		Enabled:     true,
		Interval:    1,
		MaxRequests: 2, // tiny burst
	})

	// Exhaust limit for IP1
	req1, _ := http.NewRequest("GET", "http://example.com", nil)
	req1.RemoteAddr = "1.1.1.1:1111"
	m.HandleRequest(req1)
	m.HandleRequest(req1)
	_, resp1 := m.HandleRequest(req1)
	if resp1 == nil || resp1.StatusCode != http.StatusTooManyRequests {
		t.Fatal("IP1 should be rate-limited")
	}

	// IP2 should still be allowed
	req2, _ := http.NewRequest("GET", "http://example.com", nil)
	req2.RemoteAddr = "2.2.2.2:2222"
	_, resp2 := m.HandleRequest(req2)
	if resp2 != nil {
		t.Fatal("IP2 should not be rate-limited")
	}
}

func TestRateLimitMiddleware_Cleanup(t *testing.T) {
	m := NewRateLimitMiddleware(models.RateLimitSettings{
		Enabled:     true,
		Interval:    1,
		MaxRequests: 100,
	})

	// Add many IPs
	for i := 0; i < 100; i++ {
		req, _ := http.NewRequest("GET", "http://example.com", nil)
		req.RemoteAddr = "10.0.0.1:12345"
		m.HandleRequest(req)
	}

	// Cleanup should not panic or error
	m.CleanupLimiters()
}

func TestRateLimitMiddleware_UsesSocketPeerAndIgnoresForwardedHeaders(t *testing.T) {
	m := NewRateLimitMiddleware(models.RateLimitSettings{
		Enabled:     true,
		Interval:    60,
		MaxRequests: 1,
	})

	first, _ := http.NewRequest("GET", "http://example.com", nil)
	first.RemoteAddr = "192.0.2.10:4567"
	first.Header.Set("X-Forwarded-For", "198.51.100.1")
	if _, resp := m.HandleRequest(first); resp != nil {
		t.Fatalf("first request rejected: %v", resp)
	}

	second, _ := http.NewRequest("GET", "http://example.com", nil)
	second.RemoteAddr = "192.0.2.10:9999"
	second.Header.Set("X-Forwarded-For", "203.0.113.99")
	second.Header.Set("X-Real-IP", "203.0.113.100")
	if _, resp := m.HandleRequest(second); resp == nil || resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("forwarded headers created a new limiter identity: %v", resp)
	}
}

func TestRateLimitMiddleware_ClientIPParsing(t *testing.T) {
	m := NewRateLimitMiddleware(models.RateLimitSettings{})
	tests := []struct {
		name       string
		remoteAddr string
		want       string
	}{
		{name: "IPv4 with port", remoteAddr: "192.0.2.1:1234", want: "192.0.2.1"},
		{name: "bracketed IPv6 with port", remoteAddr: "[2001:db8::1]:1234", want: "2001:db8::1"},
		{name: "bare IPv6", remoteAddr: "2001:db8::2", want: "2001:db8::2"},
		{name: "host without port", remoteAddr: "proxy-client", want: "proxy-client"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", "http://example.com", nil)
			req.RemoteAddr = tt.remoteAddr
			if got := m.getClientIP(req); got != tt.want {
				t.Fatalf("client IP = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRateLimitMiddleware_InvalidLimitsDoNotPanicOrReject(t *testing.T) {
	m := NewRateLimitMiddleware(models.RateLimitSettings{Enabled: true})
	req, _ := http.NewRequest("GET", "http://example.com", nil)
	req.RemoteAddr = "192.0.2.1:1234"
	if _, resp := m.HandleRequest(req); resp != nil {
		t.Fatalf("non-positive limit should be effectively disabled: %v", resp)
	}
}

func TestProxyMiddlewareSettingsCanUpdateConcurrently(t *testing.T) {
	auth := NewAuthMiddleware(models.AuthenticationSettings{Enabled: true, Username: "user", Password: "pass"})
	limiter := NewRateLimitMiddleware(models.RateLimitSettings{Enabled: true, Interval: 1, MaxRequests: 1000})

	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for iteration := 0; iteration < 200; iteration++ {
				if worker%2 == 0 {
					auth.UpdateSettings(models.AuthenticationSettings{Enabled: iteration%2 == 0, Username: "user", Password: "pass"})
					limiter.UpdateSettings(models.RateLimitSettings{Enabled: iteration%2 == 0, Interval: 1, MaxRequests: 1000})
					continue
				}
				req, _ := http.NewRequest("GET", "http://example.com", nil)
				req.RemoteAddr = "192.0.2.1:1234"
				req.Header.Set("Proxy-Authorization", basicAuth("user", "pass"))
				auth.HandleRequest(req)
				limiter.HandleRequest(req)
			}
		}(worker)
	}
	wg.Wait()
}
