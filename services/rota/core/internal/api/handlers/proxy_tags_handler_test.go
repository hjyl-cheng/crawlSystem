package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBulkTagRejectsRequestsWithoutProxyIDs(t *testing.T) {
	handler := &ProxyHandler{}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/proxies/bulk-tags", strings.NewReader(`{"add":["origin:free"]}`))
	response := httptest.NewRecorder()

	handler.BulkTag(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "At least one proxy ID is required") {
		t.Fatalf("body = %s", response.Body.String())
	}
}

func TestBulkTagRejectsRequestsWithoutEffectiveTags(t *testing.T) {
	handler := &ProxyHandler{}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/proxies/bulk-tags", strings.NewReader(`{"ids":[7],"add":["  "],"remove":[]}`))
	response := httptest.NewRecorder()

	handler.BulkTag(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "At least one tag to add or remove is required") {
		t.Fatalf("body = %s", response.Body.String())
	}
}
