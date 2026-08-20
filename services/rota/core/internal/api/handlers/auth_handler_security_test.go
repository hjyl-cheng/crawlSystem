package handlers

import (
	"net/http/httptest"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

type permissiveSigningMethod struct{}

func (permissiveSigningMethod) Alg() string { return "permissive-test-only" }

func (permissiveSigningMethod) Verify(string, []byte, interface{}) error { return nil }

func (permissiveSigningMethod) Sign(string, interface{}) ([]byte, error) {
	return []byte("accepted"), nil
}

func TestUsernameFromRequestRejectsNonHMACSigningMethod(t *testing.T) {
	method := permissiveSigningMethod{}
	jwt.RegisterSigningMethod(method.Alg(), func() jwt.SigningMethod { return method })
	token := jwt.NewWithClaims(method, jwt.MapClaims{"username": "attacker"})
	tokenString, err := token.SignedString([]byte("ignored"))
	if err != nil {
		t.Fatalf("SignedString() error = %v", err)
	}

	request := httptest.NewRequest("POST", "/api/v1/auth/change-password", nil)
	request.Header.Set("Authorization", "Bearer "+tokenString)
	handler := &AuthHandler{jwtSecret: []byte("secret")}

	if username, err := handler.usernameFromRequest(request); err == nil {
		t.Fatalf("usernameFromRequest() = %q, want signing method rejection", username)
	}
}

func TestUsernameFromRequestAcceptsValidHMACToken(t *testing.T) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"username": "admin"})
	tokenString, err := token.SignedString([]byte("secret"))
	if err != nil {
		t.Fatalf("SignedString() error = %v", err)
	}
	request := httptest.NewRequest("GET", "/api/v1/auth/me", nil)
	request.Header.Set("Authorization", "Bearer "+tokenString)
	handler := &AuthHandler{jwtSecret: []byte("secret")}

	username, err := handler.usernameFromRequest(request)
	if err != nil {
		t.Fatalf("usernameFromRequest() error = %v", err)
	}
	if username != "admin" {
		t.Fatalf("usernameFromRequest() = %q, want admin", username)
	}
}
