package vless

import (
	"context"
	"strings"
	"testing"
)

const testURI = "vless://11111111-1111-4111-8111-111111111111@Example.COM:443?security=reality&type=tcp&pbk=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA&fp=chrome&sni=www.example.com&flow=xtls-rprx-vision&encryption=none&sid=0A0B#Hong%20Kong"

func TestParseNormalizesSupportedNode(t *testing.T) {
	node, err := Parse(testURI)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got, want := node.Address(), "example.com:443"; got != want {
		t.Fatalf("Address = %q, want %q", got, want)
	}
	if got, want := node.Name(), "Hong Kong"; got != want {
		t.Fatalf("Name = %q, want %q", got, want)
	}
	if strings.Contains(node.Credential(), "#") || strings.Contains(node.Credential(), "Hong") {
		t.Fatalf("Credential retained fragment: %q", node.Credential())
	}
	for _, part := range []string{
		"vless://11111111-1111-4111-8111-111111111111@example.com:443?",
		"encryption=none",
		"flow=xtls-rprx-vision",
		"fp=chrome",
		"security=reality",
		"sid=0a0b",
		"sni=www.example.com",
		"type=tcp",
	} {
		if !strings.Contains(node.Credential(), part) {
			t.Errorf("Credential %q does not contain %q", node.Credential(), part)
		}
	}
}

func TestParseRejectsUnsupportedVariantsWithoutLeakingCredentials(t *testing.T) {
	const id = "11111111-1111-4111-8111-111111111111"
	tests := []struct {
		name string
		uri  string
	}{
		{name: "websocket", uri: strings.Replace(testURI, "type=tcp", "type=ws", 1)},
		{name: "tls", uri: strings.Replace(testURI, "security=reality", "security=tls", 1)},
		{name: "non vision flow", uri: strings.Replace(testURI, "flow=xtls-rprx-vision", "flow=", 1)},
		{name: "non chrome fingerprint", uri: strings.Replace(testURI, "fp=chrome", "fp=firefox", 1)},
		{name: "unknown query", uri: strings.Replace(testURI, "#", "&spx=%2F#", 1)},
		{name: "duplicate query", uri: strings.Replace(testURI, "#", "&fp=chrome#", 1)},
		{name: "bad public key", uri: strings.Replace(testURI, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "secret-key", 1)},
		{name: "bad short id", uri: strings.Replace(testURI, "sid=0A0B", "sid=xyz", 1)},
		{name: "userinfo password", uri: strings.Replace(testURI, id+"@", id+":password@", 1)},
		{name: "path", uri: strings.Replace(testURI, ":443?", ":443/path?", 1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.uri)
			if err == nil {
				t.Fatal("expected error")
			}
			if strings.Contains(err.Error(), id) || strings.Contains(err.Error(), tt.uri) {
				t.Fatalf("error leaked credential: %v", err)
			}
		})
	}
}

func TestNewDialerBuildsRuntimeThroughPublicInterface(t *testing.T) {
	node, err := Parse(testURI)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	dial, err := NewDialer(node.Credential())
	if err != nil {
		t.Fatalf("NewDialer: %v", err)
	}
	if dial == nil {
		t.Fatal("NewDialer returned nil")
	}
	if _, err := dial(context.Background(), "udp", "example.com:53"); err == nil {
		t.Fatal("expected UDP destination to be rejected")
	}
	if _, err := NewDialer(node.Credential()); err != nil {
		t.Fatalf("cached NewDialer: %v", err)
	}
}
