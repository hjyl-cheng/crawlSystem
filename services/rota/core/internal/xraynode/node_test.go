package xraynode

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"

	legacyvless "github.com/alpkeskin/rota/core/internal/vless"
)

const testVLESSReality = "vless://11111111-1111-4111-8111-111111111111@Example.COM:443?encryption=none&flow=xtls-rprx-vision&type=tcp&security=reality&sni=www.example.com&fp=chrome&pbk=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA&sid=0A0B#Hong%20Kong"

func TestParseSupportedShareURIs(t *testing.T) {
	vmessPayload, err := json.Marshal(map[string]any{
		"v": "2", "ps": "VMess WS", "add": "vmess.example.com", "port": "443",
		"id": "22222222-2222-4222-8222-222222222222", "aid": "0", "scy": "auto",
		"net": "ws", "type": "none", "host": "cdn.example.com", "path": "/socket",
		"tls": "tls", "sni": "cdn.example.com", "fp": "firefox",
	})
	if err != nil {
		t.Fatal(err)
	}
	vmessURI := "vmess://" + base64.RawStdEncoding.EncodeToString(vmessPayload)

	legacyAuthority := base64.RawURLEncoding.EncodeToString([]byte(":33333333-3333-4333-8333-333333333333@legacy.example.com:443"))
	legacyVLESS := "vless://" + legacyAuthority + "?remarks=Legacy&tls=1&peer=www.example.com&xtls=2&pbk=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA&fingerprint=chrome"

	tests := []struct {
		name         string
		uri          string
		protocol     string
		address      string
		nameTag      string
		transportKey string
	}{
		{
			name:         "VLESS Reality TCP",
			uri:          testVLESSReality,
			protocol:     "vless",
			address:      "example.com:443",
			nameTag:      "Hong Kong",
			transportKey: "tcpSettings",
		},
		{
			name:         "VLESS TLS WebSocket",
			uri:          "vless://44444444-4444-4444-8444-444444444444@ws.example.com:443?encryption=none&type=ws&security=tls&sni=cdn.example.com&host=cdn.example.com&path=%2Fws&fp=ios#WS",
			protocol:     "vless",
			address:      "ws.example.com:443",
			nameTag:      "WS",
			transportKey: "wsSettings",
		},
		{
			name:         "legacy encoded VLESS",
			uri:          legacyVLESS,
			protocol:     "vless",
			address:      "legacy.example.com:443",
			nameTag:      "Legacy",
			transportKey: "tcpSettings",
		},
		{
			name:         "VMess TLS WebSocket",
			uri:          vmessURI,
			protocol:     "vmess",
			address:      "vmess.example.com:443",
			nameTag:      "VMess WS",
			transportKey: "wsSettings",
		},
		{
			name:         "Trojan TLS WebSocket",
			uri:          "trojan://top-secret@trojan.example.com:443?security=tls&type=ws&sni=cdn.example.com&host=cdn.example.com&path=%2Fassignment#Trojan",
			protocol:     "trojan",
			address:      "trojan.example.com:443",
			nameTag:      "Trojan",
			transportKey: "wsSettings",
		},
		{
			name:         "Shadowsocks SIP002",
			uri:          "ss://" + base64.RawURLEncoding.EncodeToString([]byte("chacha20-ietf-poly1305:ss-secret")) + "@192.0.2.10:8388#SS%20Node",
			protocol:     "shadowsocks",
			address:      "192.0.2.10:8388",
			nameTag:      "SS Node",
			transportKey: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node, err := Parse(test.uri)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if node.Protocol() != test.protocol || node.Address() != test.address || node.Name() != test.nameTag {
				t.Fatalf("node = protocol=%q address=%q name=%q", node.Protocol(), node.Address(), node.Name())
			}
			if len(node.Identity()) != 64 {
				t.Fatalf("identity = %q", node.Identity())
			}
			if strings.Contains(node.Credential(), "#") || strings.Contains(node.Credential(), test.nameTag) {
				t.Fatalf("credential retained display name: %q", node.Credential())
			}
			if test.transportKey != "" {
				stream, ok := node.outbound["streamSettings"].(map[string]any)
				if !ok || stream[test.transportKey] == nil {
					t.Fatalf("stream settings = %#v", node.outbound["streamSettings"])
				}
			}
		})
	}
}

func TestParseSupportsXHTTPAndGRPCReality(t *testing.T) {
	tests := []struct {
		uri     string
		setting string
	}{
		{
			uri:     "vless://55555555-5555-4555-8555-555555555555@xhttp.example.com:443?encryption=none&type=xhttp&security=tls&sni=cdn.example.com&fp=firefox&host=cdn.example.com&path=%2Fsplit&mode=auto&extra=%7B%22headers%22%3A%7B%22Xhttp-Verify%22%3A%22token%22%7D%7D",
			setting: "xhttpSettings",
		},
		{
			uri:     "trojan://password@grpc.example.com:443?type=grpc&security=reality&sni=www.example.com&fp=chrome&pbk=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA&sid=0a0b&serviceName=service",
			setting: "grpcSettings",
		},
	}
	for _, test := range tests {
		node, err := Parse(test.uri)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		stream := node.outbound["streamSettings"].(map[string]any)
		if stream[test.setting] == nil {
			t.Fatalf("stream = %#v", stream)
		}
	}
}

func TestParseTreatsXUDPPacketEncodingAsTCPOnlyMetadata(t *testing.T) {
	baseURI := "vless://55555555-5555-4555-8555-555555555555@tcp.example.com:443?encryption=none&type=tcp&security=none"
	withoutPacketEncoding, err := Parse(baseURI)
	if err != nil {
		t.Fatal(err)
	}
	withPacketEncoding, err := Parse(baseURI + "&packetEncoding=xudp")
	if err != nil {
		t.Fatal(err)
	}
	if withPacketEncoding.Identity() != withoutPacketEncoding.Identity() {
		t.Fatal("TCP-only packet encoding metadata changed node identity")
	}
	if _, err := Parse(baseURI + "&packetEncoding=unknown"); err == nil {
		t.Fatal("unknown packet encoding was accepted")
	}
}

func TestParseIgnoresFingerprintWithoutTransportSecurity(t *testing.T) {
	vmessPayload, err := json.Marshal(map[string]any{
		"v": "2", "add": "vmess.example.com", "port": "80",
		"id": "55555555-5555-4555-8555-555555555555", "aid": "0",
		"net": "ws", "path": "/vmess", "tls": "", "fp": "chrome",
	})
	if err != nil {
		t.Fatal(err)
	}
	node, err := Parse("vmess://" + base64.RawStdEncoding.EncodeToString(vmessPayload))
	if err != nil {
		t.Fatal(err)
	}
	stream := node.outbound["streamSettings"].(map[string]any)
	if stream["security"] != "none" {
		t.Fatalf("stream security = %v, want none", stream["security"])
	}
}

func TestIdentityPreservesMultipleConfigurationsOnOneEndpoint(t *testing.T) {
	first, err := Parse(testVLESSReality)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Parse(strings.Replace(testVLESSReality,
		"11111111-1111-4111-8111-111111111111",
		"66666666-6666-4666-8666-666666666666", 1))
	if err != nil {
		t.Fatal(err)
	}
	alias, err := Parse(strings.Replace(testVLESSReality, "Hong%20Kong", "Alias", 1))
	if err != nil {
		t.Fatal(err)
	}
	if first.Address() != second.Address() || first.Identity() == second.Identity() {
		t.Fatalf("different credentials were not distinguished")
	}
	if first.Identity() != alias.Identity() {
		t.Fatalf("display-only alias changed identity")
	}
}

func TestVLESSCanonicalCredentialMatchesLegacyParser(t *testing.T) {
	legacy, err := legacyvless.Parse(testVLESSReality)
	if err != nil {
		t.Fatalf("legacy Parse: %v", err)
	}
	current, err := Parse(testVLESSReality)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if current.Credential() != legacy.Credential() {
		t.Fatal("VLESS canonical credential changed from the legacy parser")
	}
}

func TestParseRejectsInvalidShareURIsWithoutLeakingSecrets(t *testing.T) {
	vmessAlterID, err := json.Marshal(map[string]any{
		"add": "vmess.example.com", "port": 443,
		"id": "88888888-8888-4888-8888-888888888888", "aid": 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []string{
		"vless://secret-uuid@example.com:443?type=ws&security=reality&pbk=secret-public-key",
		"vless://11111111-1111-4111-8111-111111111111@example.com:443?type=ws&flow=xtls-rprx-vision&security=tls",
		"vless://11111111-1111-4111-8111-111111111111@example.com:443?type=tcp&amp;security=tls",
		"vmess://" + base64.RawStdEncoding.EncodeToString(vmessAlterID),
		"trojan://secret-password@example.com:443?type=ws&security=reality&pbk=secret-public-key",
		"trojan://secret-password@example.com:443?type=tcp&security=tls&flow=xtls-rprx-vision",
		"trojan://secret-password@example.com:443?type=grpc&security=tls&mode=guna",
		"trojan://secret-password@example.com:443?type=xhttp&security=tls&mode=unknown",
		"vless://11111111-1111-4111-8111-111111111111@example.com:443?type=tcp&security=tls&fp=unknown-client",
		"vless://11111111-1111-4111-8111-111111111111@example.com:443?type=tcp&security=reality&sni=www.example.com&fp=unsafe&pbk=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"ss://" + base64.RawURLEncoding.EncodeToString([]byte("unsupported:secret-password")) + "@example.com:8388",
		"ss://" + base64.RawURLEncoding.EncodeToString([]byte("2022-blake3-aes-128-gcm:secret-password")) + "@example.com:8388",
		"ss://" + base64.RawURLEncoding.EncodeToString([]byte("2022-blake3-aes-128-gcm:QUFBQUFBQUFBQUFBQUFB")) + "@example.com:8388",
	}
	for _, raw := range tests {
		_, err := Parse(raw)
		if err == nil {
			t.Fatalf("Parse(%q) succeeded", raw)
		}
		for _, secret := range []string{"secret-uuid", "secret-password", "secret-public-key", raw} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("error leaked secret: %v", err)
			}
		}
	}
}

func TestRuntimeConfigurationStartsForEachProtocol(t *testing.T) {
	vmessPayload, _ := json.Marshal(map[string]any{
		"v": "2", "add": "vmess.example.com", "port": "443",
		"id": "77777777-7777-4777-8777-777777777777", "scy": "auto",
		"net": "ws", "host": "cdn.example.com", "path": "/ws",
		"tls": "tls", "sni": "cdn.example.com", "fp": "chrome",
	})
	credentials := []string{
		testVLESSReality,
		"vmess://" + base64.RawStdEncoding.EncodeToString(vmessPayload),
		"trojan://password@trojan.example.com:443?security=tls&type=tcp&sni=www.example.com&fp=chrome",
		"ss://" + base64.RawURLEncoding.EncodeToString([]byte("2022-blake3-aes-128-gcm:AAAAAAAAAAAAAAAAAAAAAA==")) + "@192.0.2.20:8388",
	}
	for _, credential := range credentials {
		node, err := Parse(credential)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		runtime, err := startRuntime(node)
		if err != nil {
			t.Fatalf("startRuntime(%s): %v", node.Protocol(), err)
		}
		if err := runtime.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}
}

func TestLiveMixedSubscriptionRuntimeConfigurations(t *testing.T) {
	path := os.Getenv("ROTA_MIXED_SUBSCRIPTION_FILE")
	if path == "" {
		t.Skip("ROTA_MIXED_SUBSCRIPTION_FILE is not set")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read mixed subscription: %v", err)
	}
	decoded, err := decodeBase64Any(string(data))
	if err != nil {
		decoded = data
	}

	unique := make(map[string]Node)
	scanner := bufio.NewScanner(strings.NewReader(string(decoded)))
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if _, supported := SchemeProtocol(line); !supported {
			continue
		}
		node, parseErr := Parse(line)
		if parseErr != nil {
			continue
		}
		unique[node.Identity()] = node
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan mixed subscription: %v", err)
	}

	for identity, node := range unique {
		runtime, startErr := startRuntime(node)
		if startErr != nil {
			t.Fatalf("start %s runtime for node %.12s: %v", node.Protocol(), identity, startErr)
		}
		if err := runtime.Close(); err != nil {
			t.Fatalf("close %s runtime for node %.12s: %v", node.Protocol(), identity, err)
		}
	}
	if expectedText := os.Getenv("ROTA_MIXED_EXPECTED_UNIQUE"); expectedText != "" {
		expected, err := strconv.Atoi(expectedText)
		if err != nil {
			t.Fatal("ROTA_MIXED_EXPECTED_UNIQUE is invalid")
		}
		if len(unique) != expected {
			t.Fatalf("runtime configurations = %d, want %d", len(unique), expected)
		}
	}
	t.Logf("started and closed %d unique runtime configurations", len(unique))
}
