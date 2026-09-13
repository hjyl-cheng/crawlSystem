package api

import (
	"context"
	"github.com/alpkeskin/rota/core/internal/proxycontrol"
	"github.com/go-chi/chi/v5"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type capacityControlStub struct {
	proxyControlStub
	requested []proxycontrol.EnsureCapacityRequest
}

func (s *capacityControlStub) EnsureCapacity(_ context.Context, r proxycontrol.EnsureCapacityRequest) (proxycontrol.EnsureCapacityResult, error) {
	s.requested = append(s.requested, r)
	return proxycontrol.EnsureCapacityResult{OK: true, Role: r.Role, Provisioned: r.MinimumSlots}, nil
}
func TestEnsureCapacityRequiresControlTokenAndStrictRequest(t *testing.T) {
	stub := &capacityControlStub{}
	h := NewProxyControlHandler(stub)
	r := chi.NewRouter()
	r.With(ProxyControlTokenMiddleware("capacity-control-secret")).Post("/capacity/ensure", h.EnsureCapacity)
	for _, tc := range []struct {
		token, body string
		status      int
	}{{"", `{"role":"channel","minimum_slots":66}`, 401}, {"wrong", `{"role":"channel","minimum_slots":66}`, 401}, {"capacity-control-secret", `{"role":"channel","minimum_slots":66,"command":"shell"}`, 400}, {"capacity-control-secret", `{"role":"channel","minimum_slots":66} {}`, 400}, {"capacity-control-secret", `{"role":"channel","minimum_slots":66}`, 200}} {
		req := httptest.NewRequest(http.MethodPost, "/capacity/ensure", strings.NewReader(tc.body))
		req.Header.Set("Authorization", "Bearer "+tc.token)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code != tc.status {
			t.Fatalf("status=%d want=%d", rec.Code, tc.status)
		}
	}
	if len(stub.requested) != 1 || stub.requested[0].MinimumSlots != 66 {
		t.Fatalf("unauthorized expansion: %+v", stub.requested)
	}
}
