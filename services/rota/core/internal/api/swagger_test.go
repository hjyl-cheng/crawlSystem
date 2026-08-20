package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alpkeskin/rota/core/docs"
)

func TestServeSwaggerJSONUsesEmbeddedDocument(t *testing.T) {
	if len(docs.SwaggerJSON) == 0 {
		t.Fatal("embedded Swagger document is empty")
	}
	if !json.Valid(docs.SwaggerJSON) {
		t.Fatal("embedded Swagger document is not valid JSON")
	}

	request := httptest.NewRequest(http.MethodGet, "/api/v1/swagger.json", nil)
	response := httptest.NewRecorder()
	server := &Server{}
	server.serveSwaggerJSON(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if got := response.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if !bytes.Equal(response.Body.Bytes(), docs.SwaggerJSON) {
		t.Fatal("response body differs from embedded Swagger document")
	}
}

func TestEmbeddedSwaggerIncludesLocalAndGeoIPContracts(t *testing.T) {
	var document struct {
		Paths       map[string]json.RawMessage `json:"paths"`
		Definitions map[string]json.RawMessage `json:"definitions"`
	}
	if err := json.Unmarshal(docs.SwaggerJSON, &document); err != nil {
		t.Fatalf("decode embedded Swagger document: %v", err)
	}
	if _, ok := document.Paths["/proxies/bulk-tags"]; !ok {
		t.Fatal("embedded Swagger document is missing /proxies/bulk-tags")
	}
	for _, path := range []string{"/settings/geoip/status", "/settings/geoip/update-db"} {
		if _, ok := document.Paths[path]; !ok {
			t.Fatalf("embedded Swagger document is missing %s", path)
		}
	}
	for _, definition := range []string{
		"github_com_alpkeskin_rota_core_internal_models.BulkTagProxyRequest",
		"github_com_alpkeskin_rota_core_internal_models.GeoIPSettings",
		"github_com_alpkeskin_rota_core_internal_models.GeoIPStatus",
	} {
		if _, ok := document.Definitions[definition]; !ok {
			t.Fatalf("embedded Swagger document is missing %s", definition)
		}
	}

	var proxyList struct {
		Get struct {
			Parameters []struct {
				Name string `json:"name"`
			} `json:"parameters"`
		} `json:"get"`
	}
	if err := json.Unmarshal(document.Paths["/proxies"], &proxyList); err != nil {
		t.Fatalf("decode /proxies Swagger path: %v", err)
	}
	foundTagFilter := false
	for _, parameter := range proxyList.Get.Parameters {
		if parameter.Name == "tag" {
			foundTagFilter = true
			break
		}
	}
	if !foundTagFilter {
		t.Fatal("embedded Swagger /proxies path is missing the tag query parameter")
	}

	var proxyDefinition struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	const proxyDefinitionName = "github_com_alpkeskin_rota_core_internal_models.Proxy"
	if err := json.Unmarshal(document.Definitions[proxyDefinitionName], &proxyDefinition); err != nil {
		t.Fatalf("decode Proxy Swagger definition: %v", err)
	}
	for _, property := range []string{"tags", "archived_at", "archive_reason"} {
		if _, ok := proxyDefinition.Properties[property]; !ok {
			t.Fatalf("embedded Swagger Proxy definition is missing %s", property)
		}
	}

	var createProxyDefinition struct {
		Properties map[string]struct {
			Enum []string `json:"enum"`
		} `json:"properties"`
	}
	const createProxyDefinitionName = "github_com_alpkeskin_rota_core_internal_models.CreateProxyRequest"
	if err := json.Unmarshal(document.Definitions[createProxyDefinitionName], &createProxyDefinition); err != nil {
		t.Fatalf("decode CreateProxyRequest Swagger definition: %v", err)
	}
	protocols := make(map[string]bool)
	for _, protocol := range createProxyDefinition.Properties["protocol"].Enum {
		protocols[protocol] = true
	}
	for _, protocol := range []string{
		"http", "https", "socks4", "socks4a", "socks5",
		"vless", "vmess", "trojan", "shadowsocks",
	} {
		if !protocols[protocol] {
			t.Fatalf("embedded Swagger CreateProxyRequest protocol enum is missing %s", protocol)
		}
	}
}
