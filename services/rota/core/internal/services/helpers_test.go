package services

import (
	"errors"
	"net/url"
	"strings"
	"testing"
)

func TestRedactURLForLogRemovesEveryCredentialBearingPart(t *testing.T) {
	raw := "https://user:pass@panel.example.com/api/sub/secret-token?key=query-secret#fragment-secret"
	redacted := redactURLForLog(raw)
	if redacted != "https://panel.example.com" {
		t.Fatalf("redacted URL = %q", redacted)
	}
	for _, secret := range []string{"user", "pass", "secret-token", "query-secret", "fragment-secret"} {
		if strings.Contains(redacted, secret) {
			t.Fatalf("redacted URL retained %q", secret)
		}
	}

	errorText := redactURLInError(raw, errors.New(`Get "`+raw+`": connection refused`))
	if strings.Contains(errorText, "secret") || strings.Contains(errorText, "user:pass") {
		t.Fatalf("redacted error leaked URL credentials: %q", errorText)
	}
}

func TestRedactURLInErrorHandlesNetURLErrorFormatting(t *testing.T) {
	raw := "https://user:pass@panel.example.com/api/sub/secret-token?key=query-secret"
	err := &url.Error{
		Op:  "Get",
		URL: "https://user:pass@panel.example.com/api/sub/secret-token?key=query-secret",
		Err: errors.New("request for secret-token with query-secret failed"),
	}
	redacted := redactURLInError(raw, err)
	for _, secret := range []string{"user", "pass", "secret-token", "query-secret", "/api/sub"} {
		if strings.Contains(redacted, secret) {
			t.Fatalf("redacted error retained %q: %q", secret, redacted)
		}
	}
}
