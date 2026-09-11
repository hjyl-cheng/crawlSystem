package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alpkeskin/rota/core/internal/proxycontrol"
	"github.com/go-chi/chi/v5"
)

type remoteRouteStub struct {
	calls int
	err   error
}

func (s *remoteRouteStub) ReadRemoteRoute(_ context.Context, r proxycontrol.RemoteRouteRequest) (proxycontrol.RemoteRoute, error) {
	s.calls++
	return proxycontrol.RemoteRoute{RemoteRouteRequest: r, OK: true, Upstream: proxycontrol.RemoteUpstream{Protocol: "http", Address: "assigned:8080", Username: "user", Password: "test-upstream-secret"}}, s.err
}

func TestRemoteRouteHandlerRequiresDedicatedCredentialAndRedactsFailures(t *testing.T) {
	stub := &remoteRouteStub{}
	token := strings.Repeat("center-only-", 3)
	h, err := NewRemoteRouteHandler(stub, token)
	if err != nil {
		t.Fatal(err)
	}
	call := func(auth, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/internal/v1/remote-route", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+auth)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := call("ordinary-worker-control-token", "{}"); w.Code != 401 || stub.calls != 0 {
		t.Fatal("ordinary worker reached credential reader")
	}
	for _, body := range []string{`{"proxy_id":123}`, `{} {}`, strings.Repeat(" ", 9000) + `{}`} {
		if w := call(token, body); w.Code != 400 {
			t.Fatalf("body validation: %d", w.Code)
		}
	}
	if stub.calls != 0 {
		t.Fatal("malformed request reached reader")
	}
	w := call(token, `{"slot_name":"slot-a"}`)
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("credential response missing cache protection")
	}
	var data struct {
		Upstream map[string]string `json:"upstream"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &data); err != nil || data.Upstream["password"] != "test-upstream-secret" {
		t.Fatal("explicit transport omitted credential")
	}
	stub.err = errors.New("fake DB error with test-upstream-secret")
	w = call(token, `{}`)
	if w.Code != 500 || strings.Contains(w.Body.String(), "test-upstream-secret") {
		t.Fatal("failure leaked credential")
	}
}

func TestRemoteRouteEndpointIsOptIn(t *testing.T) {
	router := chi.NewRouter()
	s := &Server{router: router, proxyControlEnabled: true, proxyControl: &proxycontrol.Manager{}, proxyControlToken: strings.Repeat("worker-", 5)}
	r := httptest.NewRequest(http.MethodPost, "/internal/v1/remote-route", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	if w.Code != 404 {
		t.Fatal("remote credentials enabled by default")
	}
	if err := s.EnableRemoteRouteRead(s.proxyControlToken); err == nil {
		t.Fatal("ordinary worker token accepted for upstream export")
	}
	if err := s.EnableRemoteRouteRead(strings.Repeat("center-", 5)); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	router.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("explicit enable did not protect endpoint")
	}
}
