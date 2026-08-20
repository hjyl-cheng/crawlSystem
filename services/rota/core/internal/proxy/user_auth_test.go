package proxy

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/pkg/logger"
)

type userAuthStoreStub struct {
	hasUsers bool
	hasErr   error
	authUser *models.ProxyUser
	authErr  error
	hasCalls int
}

func (s *userAuthStoreStub) HasAny(context.Context) (bool, error) {
	s.hasCalls++
	return s.hasUsers, s.hasErr
}

func (s *userAuthStoreStub) Authenticate(context.Context, string, string) (*models.ProxyUser, error) {
	if s.authErr != nil {
		return nil, s.authErr
	}
	return s.authUser, nil
}

func (s *userAuthStoreStub) GetByUsername(context.Context, string) (*models.ProxyUser, error) {
	if s.authErr != nil {
		return nil, s.authErr
	}
	return s.authUser, nil
}

func testUserAuthMiddleware(store userAuthStore, legacy *AuthMiddleware) *UserAuthMiddleware {
	return &UserAuthMiddleware{
		userRepo: store,
		legacy:   legacy,
		logger:   logger.New("error"),
		cache:    make(map[string]userEntry),
	}
}

func TestUserAuthMiddleware_NoCredentialMatrix(t *testing.T) {
	databaseFailure := errors.New("database unavailable")
	tests := []struct {
		name      string
		store     *userAuthStoreStub
		legacy    models.AuthenticationSettings
		wantAllow bool
	}{
		{name: "open only when no authentication exists", store: &userAuthStoreStub{}, wantAllow: true},
		{name: "proxy users configured", store: &userAuthStoreStub{hasUsers: true}},
		{name: "database decision fails closed", store: &userAuthStoreStub{hasErr: databaseFailure}},
		{name: "legacy authentication enabled", store: &userAuthStoreStub{}, legacy: models.AuthenticationSettings{Enabled: true, Username: "legacy", Password: "secret"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			middleware := testUserAuthMiddleware(tt.store, NewAuthMiddleware(tt.legacy))
			req, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
			_, rejection := middleware.HandleRequest(req)
			if tt.wantAllow && rejection != nil {
				t.Fatalf("request rejected with %d", rejection.StatusCode)
			}
			if !tt.wantAllow && (rejection == nil || rejection.StatusCode != http.StatusProxyAuthRequired) {
				t.Fatalf("request was not rejected: %v", rejection)
			}
		})
	}
}

func TestUserAuthMiddleware_ResolutionFailureFallsBackOnlyToEnabledLegacyAuth(t *testing.T) {
	store := &userAuthStoreStub{authErr: errors.New("invalid proxy user")}

	withoutLegacy := testUserAuthMiddleware(store, NewAuthMiddleware(models.AuthenticationSettings{}))
	request, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
	request.Header.Set("Proxy-Authorization", basicAuth("someone", "wrong"))
	if _, rejection := withoutLegacy.HandleRequest(request); rejection == nil || rejection.StatusCode != http.StatusProxyAuthRequired {
		t.Fatalf("invalid Proxy User passed disabled legacy auth: %v", rejection)
	}

	withLegacy := testUserAuthMiddleware(store, NewAuthMiddleware(models.AuthenticationSettings{
		Enabled: true, Username: "legacy", Password: "secret",
	}))
	legacyRequest, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
	legacyRequest.Header.Set("Proxy-Authorization", basicAuth("legacy", "secret"))
	if _, rejection := withLegacy.HandleRequest(legacyRequest); rejection != nil {
		t.Fatalf("valid enabled legacy credentials rejected with %d", rejection.StatusCode)
	}
}

func TestUserAuthMiddleware_ValidProxyUserAttachesPoolChain(t *testing.T) {
	store := &userAuthStoreStub{authUser: &models.ProxyUser{Username: "alice", MaxRetries: 3}}
	middleware := testUserAuthMiddleware(store, NewAuthMiddleware(models.AuthenticationSettings{}))
	request, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
	request.Header.Set("Proxy-Authorization", basicAuth("alice", "secret"))

	authenticated, rejection := middleware.HandleRequest(request)
	if rejection != nil {
		t.Fatalf("valid Proxy User rejected with %d", rejection.StatusCode)
	}
	if authenticated.Context().Value(UserChainContextKey) == nil {
		t.Fatal("authenticated request has no PoolChain")
	}
	if authenticated.Header.Get("Proxy-Authorization") != "" {
		t.Fatal("Proxy-Authorization was not stripped")
	}
}

func TestUserAuthMiddleware_InvalidationExpiresOpenProxyDecisionImmediately(t *testing.T) {
	store := &userAuthStoreStub{hasUsers: true}
	middleware := testUserAuthMiddleware(store, NewAuthMiddleware(models.AuthenticationSettings{}))
	middleware.usersConfigured = false
	middleware.usersCheckedUntil = time.Now().Add(time.Hour)

	request, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
	if _, rejection := middleware.HandleRequest(request); rejection != nil {
		t.Fatalf("cached open decision unexpectedly rejected: %v", rejection)
	}
	if store.hasCalls != 0 {
		t.Fatalf("fresh cache queried store %d times", store.hasCalls)
	}

	middleware.InvalidateUser("alice")
	request, _ = http.NewRequest(http.MethodGet, "http://example.com", nil)
	if _, rejection := middleware.HandleRequest(request); rejection == nil || rejection.StatusCode != http.StatusProxyAuthRequired {
		t.Fatalf("new Proxy User did not close open mode: %v", rejection)
	}
	if store.hasCalls != 1 {
		t.Fatalf("invalidated decision queried store %d times", store.hasCalls)
	}
}

func TestUserAuthMiddleware_LiveChainsEvictsExpiredEntries(t *testing.T) {
	now := time.Now()
	live := &PoolChain{}
	middleware := testUserAuthMiddleware(&userAuthStoreStub{}, nil)
	middleware.cache["expired"] = userEntry{chain: &PoolChain{}, expiresAt: now.Add(-time.Second)}
	middleware.cache["live"] = userEntry{chain: live, expiresAt: now.Add(time.Second)}

	chains := middleware.liveChains(now)
	if len(chains) != 1 || chains[0] != live {
		t.Fatalf("live chains = %#v", chains)
	}
	if _, exists := middleware.cache["expired"]; exists {
		t.Fatal("expired authentication entry was retained")
	}
}
