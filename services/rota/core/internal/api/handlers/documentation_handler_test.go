package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServeDocumentationUsesEmbeddedSpecWithoutRequestHostFetch(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "https://unreachable.example/docs", nil)
	response := httptest.NewRecorder()

	NewDocumentationHandler().ServeDocumentation(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	body := response.Body.String()
	if !strings.Contains(body, "Rota Proxy API Documentation") {
		t.Fatal("documentation HTML does not contain the configured title")
	}
	if strings.Contains(body, "unreachable.example") || strings.Contains(body, "/api/v1/swagger.json") {
		t.Fatal("documentation HTML retained a request-host Swagger URL")
	}
}
