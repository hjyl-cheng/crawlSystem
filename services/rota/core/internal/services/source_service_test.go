package services

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
)

type proxyUpserterStub struct {
	results []proxyUpsertResult
	calls   int
}

type proxyUpsertResult struct {
	status string
	err    error
}

func (s *proxyUpserterStub) Upsert(context.Context, models.CreateProxyRequest) (int, string, error) {
	result := s.results[s.calls]
	s.calls++
	return s.calls, result.status, result.err
}

func strp(s string) *string { return &s }

func deref(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

func TestBulkUpsertInvalidatesTransportCacheOnceForExistingEndpoints(t *testing.T) {
	tests := []struct {
		name              string
		results           []proxyUpsertResult
		wantCreated       int
		wantUpdated       int
		wantFailed        int
		wantInvalidations int
	}{
		{
			name:        "new endpoints preserve warm transports",
			results:     []proxyUpsertResult{{status: "created"}, {status: "created"}},
			wantCreated: 2,
		},
		{
			name:              "multiple updates invalidate once",
			results:           []proxyUpsertResult{{status: "updated"}, {status: "created"}, {status: "updated"}},
			wantCreated:       1,
			wantUpdated:       2,
			wantInvalidations: 1,
		},
		{
			name:       "failed batch does not invalidate",
			results:    []proxyUpsertResult{{status: "failed", err: errors.New("write failed")}},
			wantFailed: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upserter := &proxyUpserterStub{results: test.results}
			invalidations := 0
			service := &SourceService{
				proxyUpserter: upserter,
				invalidateTransportCache: func() {
					invalidations++
				},
			}
			requests := make([]models.CreateProxyRequest, len(test.results))

			created, updated, failed := service.bulkUpsert(context.Background(), requests)
			if created != test.wantCreated || updated != test.wantUpdated || failed != test.wantFailed {
				t.Fatalf("bulkUpsert = (%d, %d, %d), want (%d, %d, %d)",
					created, updated, failed, test.wantCreated, test.wantUpdated, test.wantFailed)
			}
			if invalidations != test.wantInvalidations {
				t.Fatalf("invalidations = %d, want %d", invalidations, test.wantInvalidations)
			}
		})
	}
}

func TestParseProxyLine(t *testing.T) {
	tests := []struct {
		name string
		line string
		ok   bool
		want parsedProxy
	}{
		// ── existing formats (regression) ──────────────────────────
		{
			name: "bare host:port (IPv4)",
			line: "1.2.3.4:8080",
			ok:   true,
			want: parsedProxy{address: "1.2.3.4:8080"},
		},
		{
			name: "bare host:port (hostname)",
			line: "proxy.example.com:3128",
			ok:   true,
			want: parsedProxy{address: "proxy.example.com:3128"},
		},
		{
			name: "userinfo@host:port",
			line: "bob:secret@1.2.3.4:8080",
			ok:   true,
			want: parsedProxy{
				address:  "1.2.3.4:8080",
				username: strp("bob"),
				password: strp("secret"),
			},
		},
		{
			name: "scheme + host:port",
			line: "http://1.2.3.4:8080",
			ok:   true,
			want: parsedProxy{address: "1.2.3.4:8080", protocol: "http"},
		},
		{
			name: "scheme + userinfo + host:port",
			line: "socks5://alice:p4ss@host.example.com:1080",
			ok:   true,
			want: parsedProxy{
				address:  "host.example.com:1080",
				protocol: "socks5",
				username: strp("alice"),
				password: strp("p4ss"),
			},
		},

		// ── NEW: host:port:user:pass ───────────────────────────────
		{
			name: "colon-separated creds (IPv4)",
			line: "10.1.2.4:6511:username:password",
			ok:   true,
			want: parsedProxy{
				address:  "10.1.2.4:6511",
				username: strp("username"),
				password: strp("password"),
			},
		},
		{
			name: "colon-separated creds (hostname)",
			line: "proxy.example.com:3128:bob:secret",
			ok:   true,
			want: parsedProxy{
				address:  "proxy.example.com:3128",
				username: strp("bob"),
				password: strp("secret"),
			},
		},
		{
			name: "colon-separated creds with ':' in password",
			line: "10.1.2.4:6511:bob:pa:ss:word",
			ok:   true,
			want: parsedProxy{
				address:  "10.1.2.4:6511",
				username: strp("bob"),
				password: strp("pa:ss:word"),
			},
		},
		{
			name: "scheme + colon-separated creds",
			line: "http://10.1.2.4:6511:bob:secret",
			ok:   true,
			want: parsedProxy{
				address:  "10.1.2.4:6511",
				protocol: "http",
				username: strp("bob"),
				password: strp("secret"),
			},
		},

		// ── rejections / fallthrough ───────────────────────────────
		{name: "empty", line: "", ok: false},
		{name: "comment", line: "# 1.2.3.4:8080", ok: false},
		{name: "host without port", line: "hostonly", ok: false},
		{name: "unsupported URI scheme", line: "anytls://secret@node.example.com:443", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok, err := parseProxyLine(tt.line)
			if err != nil {
				t.Fatalf("parseProxyLine: %v", err)
			}
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v (got=%+v)", ok, tt.ok, got)
			}
			if !ok {
				return
			}
			if got.address != tt.want.address {
				t.Errorf("address = %q, want %q", got.address, tt.want.address)
			}
			if got.protocol != tt.want.protocol {
				t.Errorf("protocol = %q, want %q", got.protocol, tt.want.protocol)
			}
			if deref(got.username) != deref(tt.want.username) {
				t.Errorf("username = %s, want %s", deref(got.username), deref(tt.want.username))
			}
			if deref(got.password) != deref(tt.want.password) {
				t.Errorf("password = %s, want %s", deref(got.password), deref(tt.want.password))
			}
		})
	}
}

func TestParseProxyListDecodesAndDeduplicatesVLESSSubscription(t *testing.T) {
	first := "vless://11111111-1111-4111-8111-111111111111@node.example.com:443?encryption=none&flow=xtls-rprx-vision&type=tcp&security=reality&sni=www.example.com&fp=chrome&pbk=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA&sid=0a0b#Alias%20One"
	second := strings.Replace(first, "Alias%20One", "Alias%20Two", 1)
	encoded := base64.StdEncoding.EncodeToString([]byte(first + "\n" + second + "\n"))

	got, err := parseProxyList(strings.NewReader(encoded))
	if err != nil {
		t.Fatalf("parseProxyList: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].address != "node.example.com:443" || got[0].protocol != "vless" {
		t.Fatalf("proxy = %#v", got[0])
	}
	if got[0].password == nil || strings.Contains(*got[0].password, "#") {
		t.Fatalf("credential was not canonicalized: %v", got[0].password)
	}
	if len(got[0].tags) != 2 || got[0].tags[0] != "Alias One" || got[0].tags[1] != "Alias Two" {
		t.Fatalf("tags = %#v", got[0].tags)
	}
}

func TestParseProxyListImportsAndDeduplicatesHysteria2Aliases(t *testing.T) {
	first := "hysteria2://paid-secret@hy2.example.com:443/?mport=20000-20010&sni=cdn.example.com&insecure=false&pinSHA256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa#Alias%20One"
	second := strings.Replace(strings.Replace(first, "hysteria2://", "hy2://", 1), "Alias%20One", "Alias%20Two", 1)
	encoded := base64.StdEncoding.EncodeToString([]byte(first + "\n" + second + "\n"))

	parsed, err := parseProxyListWithStats(strings.NewReader(encoded))
	if err != nil {
		t.Fatalf("parseProxyListWithStats: %v", err)
	}
	if parsed.total != 2 || parsed.supported != 2 || parsed.skipped != 0 || len(parsed.proxies) != 1 {
		t.Fatalf("stats = total:%d supported:%d skipped:%d proxies:%d",
			parsed.total, parsed.supported, parsed.skipped, len(parsed.proxies))
	}
	node := parsed.proxies[0]
	if node.address != "hy2.example.com:443" || node.protocol != "hysteria2" || node.password == nil {
		t.Fatalf("proxy = %#v", node)
	}
	if len(node.tags) != 2 || node.tags[0] != "Alias One" || node.tags[1] != "Alias Two" {
		t.Fatalf("tags = %#v", node.tags)
	}
}

func TestParseProxyListReadsSupportedClashYAML(t *testing.T) {
	yaml := `
proxies:
  - name: TLS HTTP One
    type: http
    server: 192.0.2.10
    port: 9002
    tls: true
  - name: TLS HTTP Alias
    type: http
    server: 192.0.2.10
    port: 9002
    tls: true
  - name: Authenticated SOCKS
    type: socks5
    server: socks.example.com
    port: 1080
    username: alice
    password: secret
  - name: Unsupported
    type: vmess
    server: ignored.example.com
    port: 443
proxy-groups:
  - name: Do Not Import
    type: select
`

	got, err := parseProxyList(strings.NewReader(yaml))
	if err != nil {
		t.Fatalf("parseProxyList: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].address != "192.0.2.10:9002" || got[0].protocol != "https" {
		t.Fatalf("TLS HTTP proxy = %#v", got[0])
	}
	if strings.Join(got[0].tags, ",") != "TLS HTTP One,TLS HTTP Alias" {
		t.Fatalf("deduplicated tags = %#v", got[0].tags)
	}
	if got[1].address != "socks.example.com:1080" || got[1].protocol != "socks5" {
		t.Fatalf("SOCKS proxy = %#v", got[1])
	}
	if deref(got[1].username) != "alice" || deref(got[1].password) != "secret" {
		t.Fatal("Clash proxy credentials were not preserved")
	}
}

func TestParseProxyListReadsHysteria2ClashYAML(t *testing.T) {
	yaml := `
proxies:
  - name: Paid HY2
    type: hysteria2
    server: hy2.example.com
    port: 443
    password: paid-secret
    ports: 20000-20010
    sni: cdn.example.com
    skip-cert-verify: false
    fingerprint: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    obfs: salamander
    obfs-password: obfs-secret
  - name: Paid HY2 Alias
    type: hy2
    server: hy2.example.com
    port: 443
    password: paid-secret
    ports: 20000-20010
    sni: cdn.example.com
    fingerprint: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    obfs: salamander
    obfs-password: obfs-secret
`

	got, err := parseProxyList(strings.NewReader(yaml))
	if err != nil {
		t.Fatalf("parseProxyList: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	node := got[0]
	if node.protocol != "hysteria2" || node.address != "hy2.example.com:443" || node.password == nil || node.nodeIdentity == "" {
		t.Fatalf("proxy = %#v", node)
	}
	if strings.Contains(*node.password, "#") || !strings.Contains(*node.password, "mport=20000-20010") {
		t.Fatal("Hysteria2 Clash configuration was not canonicalized")
	}
	if strings.Join(node.tags, ",") != "Paid HY2,Paid HY2 Alias" {
		t.Fatalf("tags = %#v", node.tags)
	}
}

func TestParseProxyListRejectsMalformedClashProxyCollection(t *testing.T) {
	_, err := parseProxyList(strings.NewReader("proxies: not-a-list\n"))
	if err == nil || !strings.Contains(err.Error(), "must be a YAML list") {
		t.Fatalf("error = %v", err)
	}
}

func TestSourceProxyRequestIncludesDefaultAndNodeTags(t *testing.T) {
	source := &models.ProxySource{
		ID:          42,
		Protocol:    "http",
		DefaultTags: []string{"origin:free", "source:example"},
	}
	request, err := sourceProxyRequest(source, parsedProxy{
		address:  "192.0.2.20:8080",
		protocol: "https",
		tags:     []string{"node-name", "origin:paid"},
	})
	if err != nil {
		t.Fatalf("sourceProxyRequest: %v", err)
	}
	if request.Protocol != "https" || request.SourceID == nil || *request.SourceID != source.ID {
		t.Fatalf("request identity = %#v", request)
	}
	if strings.Join(request.Tags, ",") != "node-name,origin:paid,origin:free,source:example" {
		t.Fatalf("tags = %#v", request.Tags)
	}
}

func TestParseProxyListSkipsMalformedShareURIWithoutAbortingSource(t *testing.T) {
	const id = "11111111-1111-4111-8111-111111111111"
	line := "vless://" + id + "@node.example.com:443?type=ws&security=reality&pbk=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA&sni=www.example.com&fp=chrome"
	parsed, err := parseProxyListWithStats(strings.NewReader("# heading\n" + line + "\nhttp://192.0.2.1:8080\n"))
	if err != nil {
		t.Fatalf("parse source: %v", err)
	}
	if parsed.total != 2 || parsed.supported != 1 || parsed.skipped != 1 || len(parsed.proxies) != 1 {
		t.Fatalf("stats = total:%d supported:%d skipped:%d proxies:%d",
			parsed.total, parsed.supported, parsed.skipped, len(parsed.proxies))
	}
}

func TestParseProxyListSkipsUnknownURISchemes(t *testing.T) {
	parsed, err := parseProxyListWithStats(strings.NewReader(
		"anytls://secret@node.example.com:443\nhttp://192.0.2.1:8080\n",
	))
	if err != nil {
		t.Fatalf("parse source: %v", err)
	}
	if parsed.total != 2 || parsed.supported != 1 || parsed.skipped != 1 || len(parsed.proxies) != 1 {
		t.Fatalf("stats = total:%d supported:%d skipped:%d proxies:%d",
			parsed.total, parsed.supported, parsed.skipped, len(parsed.proxies))
	}
}

func TestParseProxyListRejectsOversizeResponseAndLine(t *testing.T) {
	_, err := parseProxyList(strings.NewReader(strings.Repeat("#", maxProxyListBytes+1)))
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversize response error = %v", err)
	}

	_, err = parseProxyList(strings.NewReader(strings.Repeat("x", maxProxyLineBytes+1)))
	if err == nil {
		t.Fatal("oversize line was accepted")
	}
}

func TestSourceDueFetchAdmissionDoesNotHoldMutexAcrossWork(t *testing.T) {
	service := &SourceService{}
	if !service.beginDueFetch() {
		t.Fatal("first due fetch was rejected")
	}

	locked := make(chan struct{})
	go func() {
		service.mu.Lock()
		service.mu.Unlock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("fetch admission retained the mutex across work")
	}
	if service.beginDueFetch() {
		t.Fatal("overlapping due fetch was admitted")
	}
	service.endDueFetch()
	if !service.beginDueFetch() {
		t.Fatal("new due fetch was rejected after completion")
	}
	service.endDueFetch()
}

func TestLiveVLESSSubscriptionParsing(t *testing.T) {
	path := os.Getenv("ROTA_VLESS_SUBSCRIPTION_FILE")
	if path == "" {
		t.Skip("ROTA_VLESS_SUBSCRIPTION_FILE is not set")
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open subscription: %v", err)
	}
	defer file.Close()

	got, err := parseProxyList(file)
	if err != nil {
		t.Fatalf("parse subscription: %v", err)
	}
	if expectedText := os.Getenv("ROTA_VLESS_EXPECTED_NODES"); expectedText != "" {
		expected, err := strconv.Atoi(expectedText)
		if err != nil {
			t.Fatal("invalid ROTA_VLESS_EXPECTED_NODES")
		}
		if len(got) != expected {
			t.Fatalf("unique nodes = %d, want %d", len(got), expected)
		}
	}

	for _, node := range got {
		if node.protocol != "vless" || node.address == "" || node.password == nil {
			t.Fatal("subscription entry was not normalized for VLESS storage")
		}
		if strings.Contains(*node.password, "#") {
			t.Fatal("stored VLESS credential retained a fragment")
		}
	}
}

func TestLiveProxySourceParsing(t *testing.T) {
	path := os.Getenv("ROTA_PROXY_SOURCE_FILE")
	if path == "" {
		t.Skip("ROTA_PROXY_SOURCE_FILE is not set")
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open proxy source: %v", err)
	}
	defer file.Close()

	got, err := parseProxyList(file)
	if err != nil {
		t.Fatalf("parse proxy source: %v", err)
	}
	if expectedText := os.Getenv("ROTA_PROXY_SOURCE_EXPECTED_NODES"); expectedText != "" {
		expected, err := strconv.Atoi(expectedText)
		if err != nil {
			t.Fatal("invalid ROTA_PROXY_SOURCE_EXPECTED_NODES")
		}
		if len(got) != expected {
			t.Fatalf("unique nodes = %d, want %d", len(got), expected)
		}
	}
	if expectedProtocol := os.Getenv("ROTA_PROXY_SOURCE_EXPECTED_PROTOCOL"); expectedProtocol != "" {
		for _, node := range got {
			if node.protocol != expectedProtocol {
				t.Fatalf("protocol = %q for %q, want %q", node.protocol, node.address, expectedProtocol)
			}
		}
	}
}

func TestLiveMixedShareSubscriptionParsing(t *testing.T) {
	path := os.Getenv("ROTA_MIXED_SUBSCRIPTION_FILE")
	if path == "" {
		t.Skip("ROTA_MIXED_SUBSCRIPTION_FILE is not set")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open mixed subscription: %v", err)
	}
	defer file.Close()

	parsed, err := parseProxyListWithStats(file)
	if err != nil {
		t.Fatalf("parse mixed subscription: %v", err)
	}
	counts := make(map[string]int)
	for _, node := range parsed.proxies {
		counts[node.protocol]++
		if node.address == "" || node.password == nil || node.nodeIdentity == "" {
			t.Fatalf("share node was not normalized: %#v", node)
		}
	}
	t.Logf("total=%d supported=%d skipped=%d unique=%d protocols=%v",
		parsed.total, parsed.supported, parsed.skipped, len(parsed.proxies), counts)

	data, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("read mixed subscription: %v", readErr)
	}
	errorCounts := make(map[string]int)
	for _, line := range strings.Split(string(decodeBase64Subscription(data)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		_, ok, parseErr := parseProxyLine(line)
		if parseErr != nil {
			errorCounts[parseErr.Error()]++
		} else if !ok {
			errorCounts["unsupported line format"]++
		}
	}
	t.Logf("skip reasons=%v", errorCounts)

	if expectedText := os.Getenv("ROTA_MIXED_MIN_SUPPORTED"); expectedText != "" {
		expected, err := strconv.Atoi(expectedText)
		if err != nil {
			t.Fatal("invalid ROTA_MIXED_MIN_SUPPORTED")
		}
		if parsed.supported < expected {
			t.Fatalf("supported nodes = %d, want at least %d", parsed.supported, expected)
		}
	}
}
