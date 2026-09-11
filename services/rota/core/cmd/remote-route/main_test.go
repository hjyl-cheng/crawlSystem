package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alpkeskin/rota/core/internal/proxycontrol"
)

type readerStub struct{ calls int }

func (s *readerStub) ReadRemoteRoute(context.Context, proxycontrol.RemoteRouteRequest) (proxycontrol.RemoteRoute, error) {
	s.calls++
	return proxycontrol.RemoteRoute{}, proxycontrol.ErrTaskConflict
}
func TestDedicatedReaderOnlyExposesHealthAndAuthenticatedRead(t *testing.T) {
	reader := &readerStub{}
	var pingError error
	h, err := router(reader, strings.Repeat("dedicated-", 4), func(context.Context) error { return pingError })
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, path, token string
		status              int
	}{
		{"GET", "/healthz", "", 204},
		{"POST", "/internal/v1/remote-route", "", 401},
		{"POST", "/internal/v1/remote-route", "ordinary-control-token", 401},
		{"POST", "/api/v1/proxy-control/claim", "", 404},
		{"POST", "/internal/v1/remote-route", strings.Repeat("dedicated-", 4), 409},
	} {
		r := httptest.NewRequest(tc.method, tc.path, strings.NewReader("{}"))
		r.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s: status %d", tc.path, w.Code)
		}
	}
	if reader.calls != 1 {
		t.Fatal("unauthenticated request reached reader")
	}
	pingError = errors.New("private-database-detail")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != 503 || strings.Contains(w.Body.String(), "private") {
		t.Fatal("health failure not sanitized")
	}
}
