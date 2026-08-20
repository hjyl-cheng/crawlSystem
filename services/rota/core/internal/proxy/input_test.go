package proxy

import (
	"strings"
	"testing"

	"github.com/alpkeskin/rota/core/internal/models"
)

const testVLESSURI = "vless://11111111-1111-4111-8111-111111111111@node.example.com:443?encryption=none&flow=xtls-rprx-vision&type=tcp&security=reality&sni=www.example.com&fp=chrome&pbk=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA&sid=0a0b#HK"

func TestNormalizeCreateRequestMapsVLESSToExistingStorage(t *testing.T) {
	username := "must-be-cleared"
	req := models.CreateProxyRequest{
		Address:  testVLESSURI,
		Protocol: "VLESS",
		Username: &username,
	}
	if err := NormalizeCreateRequest(&req); err != nil {
		t.Fatalf("NormalizeCreateRequest: %v", err)
	}
	if req.Address != "node.example.com:443" {
		t.Fatalf("Address = %q", req.Address)
	}
	if req.Username != nil {
		t.Fatal("Username was not cleared")
	}
	if req.Password == nil || !strings.HasPrefix(*req.Password, "vless://") || strings.Contains(*req.Password, "#") {
		t.Fatalf("Password was not canonicalized: %v", req.Password)
	}
	if len(req.Tags) != 1 || req.Tags[0] != "HK" {
		t.Fatalf("Tags = %#v", req.Tags)
	}
}

func TestNormalizeCreateRequestAcceptsStoredVLESSShape(t *testing.T) {
	credential := testVLESSURI
	req := models.CreateProxyRequest{
		Address:  "node.example.com:443",
		Protocol: "vless",
		Password: &credential,
	}
	if err := NormalizeCreateRequest(&req); err != nil {
		t.Fatalf("NormalizeCreateRequest: %v", err)
	}
	if req.Address != "node.example.com:443" || req.Password == nil {
		t.Fatalf("normalized request = %#v", req)
	}
}

func TestNormalizeCreateRequestDoesNotLeakVLESSCredential(t *testing.T) {
	bad := strings.Replace(testVLESSURI, "type=tcp", "type=ws", 1)
	req := models.CreateProxyRequest{Address: bad, Protocol: "vless"}
	err := NormalizeCreateRequest(&req)
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "11111111-1111-4111-8111-111111111111") || strings.Contains(err.Error(), bad) {
		t.Fatalf("error leaked credential: %v", err)
	}
}

func TestNormalizeCreateRequestDoesNotReflectUnsupportedProtocol(t *testing.T) {
	req := models.CreateProxyRequest{Address: "node.example.com:443", Protocol: testVLESSURI}
	err := NormalizeCreateRequest(&req)
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), testVLESSURI) || strings.Contains(err.Error(), "11111111-1111-4111-8111-111111111111") {
		t.Fatalf("error reflected credential: %v", err)
	}
}

func TestNormalizeUpdateRequestPreservesHiddenVLESSCredential(t *testing.T) {
	existing := &models.Proxy{Address: "node.example.com:443", Protocol: "vless"}
	req := models.UpdateProxyRequest{Address: existing.Address, Protocol: "vless"}
	if err := NormalizeUpdateRequest(existing, &req); err != nil {
		t.Fatalf("NormalizeUpdateRequest: %v", err)
	}
	if req.Address != "" || req.Password != nil || req.Username == nil || *req.Username != "" {
		t.Fatalf("hidden fields would not be preserved: %#v", req)
	}

	req = models.UpdateProxyRequest{Address: "other.example.com:443", Protocol: "vless"}
	if err := NormalizeUpdateRequest(existing, &req); err == nil {
		t.Fatal("expected endpoint-only change to be rejected")
	}
}

func TestSupportedSourceProtocolsIncludeAutoAndShareNodes(t *testing.T) {
	for _, protocol := range []string{
		"auto", "http", "https", "socks4", "socks4a", "socks5",
		"vless", "vmess", "trojan", "shadowsocks",
	} {
		if !IsSupportedSourceProtocol(protocol) {
			t.Fatalf("source protocol %q is not supported", protocol)
		}
	}
	if IsSupportedProtocol("auto") {
		t.Fatal("auto must not be accepted as a runnable proxy protocol")
	}
}
