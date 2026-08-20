package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestRESTJWTMiddlewareRejectsQueryTokens(t *testing.T) {
	const secret = "rest-query-token-secret"
	token := signMiddlewareTestToken(t, secret)
	handler := JWTMiddleware(secret)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodGet, "/api/v1/proxies/export?token="+token, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("query-token response = %d, want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestRESTJWTMiddlewareAcceptsBearerToken(t *testing.T) {
	const secret = "rest-bearer-token-secret"
	token := signMiddlewareTestToken(t, secret)
	handler := JWTMiddleware(secret)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodGet, "/api/v1/proxies/export", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("bearer-token response = %d, want %d", response.Code, http.StatusNoContent)
	}
}

func TestWebSocketJWTMiddlewareAcceptsQueryToken(t *testing.T) {
	const secret = "websocket-query-token-secret"
	token := signMiddlewareTestToken(t, secret)
	handler := WebSocketJWTMiddleware(secret)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodGet, "/ws/dashboard?token="+token, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("WebSocket query-token response = %d, want %d", response.Code, http.StatusNoContent)
	}
}

func signMiddlewareTestToken(t *testing.T, secret string) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "test-admin",
		"exp": time.Now().Add(time.Minute).Unix(),
	})
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return signed
}
