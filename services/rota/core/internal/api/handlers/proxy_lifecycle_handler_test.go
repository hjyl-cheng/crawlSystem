package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

type proxyLifecycleStoreStub struct {
	archivedIDs []int
	restoredIDs []int
	reason      string
}

func (s *proxyLifecycleStoreStub) Archive(_ context.Context, ids []int, reason string) (int, error) {
	s.archivedIDs = append([]int(nil), ids...)
	s.reason = reason
	return len(ids), nil
}

func (s *proxyLifecycleStoreStub) Restore(_ context.Context, ids []int) (int, error) {
	s.restoredIDs = append([]int(nil), ids...)
	return len(ids), nil
}

func TestArchiveProxyAcceptsAnExplicitReason(t *testing.T) {
	store := &proxyLifecycleStoreStub{}
	handler := &ProxyHandler{lifecycleStore: store}
	request := requestWithProxyID(http.MethodPost, "/api/v1/proxies/42/archive", "42", `{"reason":"retired by operator"}`)
	response := httptest.NewRecorder()

	handler.Archive(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if len(store.archivedIDs) != 1 || store.archivedIDs[0] != 42 || store.reason != "retired by operator" {
		t.Fatalf("archive call = ids %#v, reason %q", store.archivedIDs, store.reason)
	}
}

func TestBulkRestoreReturnsArchivedProxiesToPendingValidation(t *testing.T) {
	store := &proxyLifecycleStoreStub{}
	handler := &ProxyHandler{lifecycleStore: store}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/proxies/bulk-restore", strings.NewReader(`{"ids":[7,9]}`))
	response := httptest.NewRecorder()

	handler.BulkRestore(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if len(store.restoredIDs) != 2 || store.restoredIDs[0] != 7 || store.restoredIDs[1] != 9 {
		t.Fatalf("restored ids = %#v", store.restoredIDs)
	}
}

func TestLongOperationContextSurvivesRequestButStopsWithAPI(t *testing.T) {
	ownerCtx, stopOwner := context.WithCancel(context.Background())
	requestCtx, stopRequest := context.WithCancel(context.Background())
	handler := &ProxyHandler{operationCtx: ownerCtx}
	operationCtx, cancel := handler.longOperationContext(requestCtx, time.Minute)
	defer cancel()

	stopRequest()
	select {
	case <-operationCtx.Done():
		t.Fatalf("request cancellation stopped detached operation: %v", operationCtx.Err())
	default:
	}
	stopOwner()
	select {
	case <-operationCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("API cancellation did not stop long operation")
	}
}

func requestWithProxyID(method, target, id, body string) *http.Request {
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("id", id)
	return request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeContext))
}
