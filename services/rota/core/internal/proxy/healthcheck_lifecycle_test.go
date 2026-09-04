package proxy

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/proxylifecycle"
)

type healthSettingsStoreStub struct {
	settings models.Settings
}

func (s *healthSettingsStoreStub) GetAll(context.Context) (*models.Settings, error) {
	settings := s.settings
	return &settings, nil
}

type healthLifecycleStoreStub struct {
	status     proxylifecycle.Status
	evidence   proxylifecycle.HealthEvidence
	claimLimit int
	applied    *bool
}

type healthBatchStoreStub struct {
	proxies    []*models.Proxy
	claimLimit int
}

func (s *healthBatchStoreStub) ApplyHealthVerdict(
	_ context.Context,
	_ int,
	evidence proxylifecycle.HealthEvidence,
	_ proxylifecycle.Policy,
) (proxylifecycle.Decision, bool, error) {
	status := proxylifecycle.StatusFailed
	if evidence.Verdict.Healthy {
		status = proxylifecycle.StatusActive
	}
	return proxylifecycle.Decision{Status: status}, true, nil
}

func (s *healthBatchStoreStub) ClaimDueHealthChecks(_ context.Context, limit int) ([]*models.Proxy, error) {
	s.claimLimit = limit
	if limit < len(s.proxies) {
		return s.proxies[:limit], nil
	}
	return s.proxies, nil
}

func (s *healthLifecycleStoreStub) ApplyHealthVerdict(
	_ context.Context,
	_ int,
	evidence proxylifecycle.HealthEvidence,
	policy proxylifecycle.Policy,
) (proxylifecycle.Decision, bool, error) {
	s.evidence = evidence
	decision := policy.Decide(evidence.CheckedAt, proxylifecycle.Snapshot{Status: s.status}, evidence.Verdict)
	s.status = decision.Status
	applied := true
	if s.applied != nil {
		applied = *s.applied
	}
	return decision, applied, nil
}

func (s *healthLifecycleStoreStub) ClaimDueHealthChecks(_ context.Context, limit int) ([]*models.Proxy, error) {
	s.claimLimit = limit
	return nil, nil
}

func TestPeriodicHealthCheckUsesBoundedBatch(t *testing.T) {
	store := &healthLifecycleStoreStub{status: proxylifecycle.StatusIdle}
	checker := NewHealthChecker(store, &healthSettingsStoreStub{}, nil)

	if _, err := checker.CheckAllProxies(context.Background()); err != nil {
		t.Fatalf("CheckAllProxies: %v", err)
	}
	if store.claimLimit != 20 {
		t.Fatalf("ClaimDueHealthChecks limit = %d, want 20", store.claimLimit)
	}
}

func TestPeriodicHealthCheckUsesIndependentWorkerCap(t *testing.T) {
	var current atomic.Int32
	var maximum atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		inFlight := current.Add(1)
		defer current.Add(-1)
		for {
			observed := maximum.Load()
			if inFlight <= observed || maximum.CompareAndSwap(observed, inFlight) {
				break
			}
		}
		time.Sleep(25 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)

	proxies := make([]*models.Proxy, 8)
	for index := range proxies {
		proxies[index] = httpProxyModel(upstream.URL)
		proxies[index].ID = index + 1
	}
	store := &healthBatchStoreStub{proxies: proxies}
	settings := healthSettings(t, "http://base.invalid/", "http://youtube.invalid/youtube")
	settings.settings.HealthCheck.Workers = 50
	checker := NewHealthChecker(store, settings, nil)

	if _, err := checker.CheckAllProxies(context.Background()); err != nil {
		t.Fatalf("CheckAllProxies: %v", err)
	}
	if got := maximum.Load(); got != defaultPeriodicWorkers {
		t.Fatalf("maximum concurrent probes = %d, want %d", got, defaultPeriodicWorkers)
	}
}

func TestPeriodicHealthCheckLoopWaitsAfterEveryCompletedBatch(t *testing.T) {
	trace := make([]string, 0, 5)
	waits := 0
	runPeriodicHealthCheckLoop(
		context.Background(),
		time.Minute,
		func(context.Context) error {
			trace = append(trace, "check")
			return nil
		},
		func(context.Context, time.Duration) bool {
			trace = append(trace, "wait")
			waits++
			return waits < 3
		},
	)

	want := []string{"wait", "check", "wait", "check", "wait"}
	if strings.Join(trace, ",") != strings.Join(want, ",") {
		t.Fatalf("trace = %v, want %v", trace, want)
	}
}

func TestHealthCheckerUsesOneRandomYouTubeSearchRequest(t *testing.T) {
	var proxyConnections atomic.Int32
	var youtubeRequests atomic.Int32
	var requestPath, searchQuery, requestHost string
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isYouTubeHost(strings.Split(r.Host, ":")[0]) {
			youtubeRequests.Add(1)
			requestHost = r.Host
			requestPath = r.URL.Path
			searchQuery = r.URL.Query().Get("search_query")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<script>var ytInitialData = {};</script>`))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(upstream.Close)
	proxyServer := connectTunnelProxy(t, upstream.URL, &proxyConnections)

	store := &healthLifecycleStoreStub{status: proxylifecycle.StatusIdle}
	settings := &healthSettingsStoreStub{settings: models.Settings{
		HealthCheck: models.HealthCheckSettings{
			Timeout:    60,
			Workers:    1,
			BaseURL:    "https://www.google.com/generate_204",
			BaseStatus: http.StatusNoContent,
			URL:        "https://www.youtube.com/watch?v=legacy",
			Status:     http.StatusOK,
		},
		ProxyLifecycle: models.ProxyLifecycleSettings{
			AutoArchiveEnabled:        true,
			HardUnreachableAfterHours: 6,
			SoftUnreachableAfterHours: 24,
			YouTubeUnusableAfterHours: 72,
		},
	}}
	checker := NewHealthChecker(store, settings, nil)

	result, err := checker.CheckProxyAgainst(
		context.Background(),
		httpProxyModel(proxyServer.URL),
		"https://www.youtube.com/watch?v=legacy-pool-target",
	)
	if err != nil {
		t.Fatalf("CheckProxy: %v", err)
	}
	if result.Status != string(proxylifecycle.StatusActive) || !result.Conclusive {
		t.Fatalf("result = %#v", result)
	}
	if got := proxyConnections.Load(); got != 1 {
		t.Fatalf("proxy connections = %d, want exactly one", got)
	}
	if got := youtubeRequests.Load(); got != 1 {
		t.Fatalf("YouTube requests = %d, want exactly one", got)
	}
	if requestHost != "www.youtube.com" || requestPath != "/results" {
		t.Fatalf("YouTube target = host %q path %q", requestHost, requestPath)
	}
	if !regexp.MustCompile(`^[1-9][0-9]{0,5}$`).MatchString(searchQuery) {
		t.Fatalf("search_query = %q, want a 1-6 digit positive number", searchQuery)
	}
	if store.evidence.Base.Status != proxylifecycle.ProbeNotRun ||
		store.evidence.YouTube.Status != proxylifecycle.ProbePassed {
		t.Fatalf("evidence = %#v", store.evidence)
	}
}

func TestHealthCheckTimeoutIsCappedAtFifteenSeconds(t *testing.T) {
	for _, configured := range []int{0, 15, 60, 300} {
		settings := normalizedHealthSettings(models.HealthCheckSettings{Timeout: configured})
		if settings.Timeout != 15 {
			t.Fatalf("normalized timeout for %d = %d, want 15", configured, settings.Timeout)
		}
	}
}

func TestHealthCheckerTreatsRequestTimeoutAsSoftFailure(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(1500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<script>var ytInitialData = {};</script>`))
	}))
	t.Cleanup(upstream.Close)
	var connections atomic.Int32
	proxyServer := connectTunnelProxy(t, upstream.URL, &connections)
	settings := healthSettings(t, "", "")
	settings.settings.HealthCheck.Timeout = 1
	store := &healthLifecycleStoreStub{status: proxylifecycle.StatusActive}
	checker := NewHealthChecker(store, settings, nil)

	startedAt := time.Now()
	result, err := checker.CheckProxy(context.Background(), httpProxyModel(proxyServer.URL))
	if err != nil {
		t.Fatalf("CheckProxy: %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed >= 3*time.Second {
		t.Fatalf("timed-out health request returned after %s", elapsed)
	}
	if result.Status != string(proxylifecycle.StatusFailed) || !result.Conclusive ||
		result.FailureKind == nil || *result.FailureKind != string(proxylifecycle.FailureSoftUnreachable) {
		t.Fatalf("timeout result = %#v", result)
	}
	if store.evidence.Base.Status != proxylifecycle.ProbeNotRun || connections.Load() != 1 {
		t.Fatalf("timeout evidence = %#v, proxy connections = %d", store.evidence, connections.Load())
	}
}

func TestClosedPipeIsAConclusiveSoftEndpointFailure(t *testing.T) {
	for _, err := range []error{
		io.ErrClosedPipe,
		io.EOF,
		io.ErrUnexpectedEOF,
		errors.New("proxy dial failed: closed pipe"),
	} {
		if got := classifyConnectionFailure(err); got != proxylifecycle.FailureSoftUnreachable {
			t.Fatalf("classifyConnectionFailure(%q) = %q, want %q", err, got, proxylifecycle.FailureSoftUnreachable)
		}
	}

	verdict := transportCreationVerdict(io.ErrClosedPipe)
	if verdict.Kind != proxylifecycle.FailureSoftUnreachable || !verdict.Conclusive || verdict.ControlPathHealthy {
		t.Fatalf("transport creation verdict = %#v", verdict)
	}
}

func TestYouTubeFailureClassification(t *testing.T) {
	tests := []struct {
		name       string
		probe      proxylifecycle.ProbeEvidence
		wantKind   proxylifecycle.FailureKind
		conclusive bool
	}{
		{
			name: "timeout", probe: failedProbe("context deadline exceeded (Client.Timeout exceeded)"),
			wantKind: proxylifecycle.FailureSoftUnreachable, conclusive: true,
		},
		{
			name: "connection refused", probe: failedProbe("dial tcp: connection refused"),
			wantKind: proxylifecycle.FailureHardUnreachable, conclusive: true,
		},
		{
			name: "forbidden", probe: proxylifecycle.ProbeEvidence{
				Status: proxylifecycle.ProbeFailed, HTTPStatus: intPtr(http.StatusForbidden),
			},
			wantKind: proxylifecycle.FailureYouTubeUnusable, conclusive: true,
		},
		{
			name: "proxy CONNECT forbidden", probe: failedProbe("proxyconnect tcp: 403 Forbidden"),
			wantKind: proxylifecycle.FailureYouTubeUnusable, conclusive: true,
		},
		{
			name: "proxy CONNECT rate limited", probe: failedProbe("proxyconnect tcp: 429 Too Many Requests"),
			wantKind: proxylifecycle.FailureYouTubeUnusable, conclusive: true,
		},
		{
			name: "proxy authentication", probe: proxylifecycle.ProbeEvidence{
				Status: proxylifecycle.ProbeFailed, HTTPStatus: intPtr(http.StatusProxyAuthRequired),
			},
			wantKind: proxylifecycle.FailureHardUnreachable, conclusive: true,
		},
		{
			name: "caller canceled", probe: failedProbe("context canceled"),
			wantKind: proxylifecycle.FailureNone, conclusive: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			verdict := youtubeFailureVerdict(test.probe)
			if verdict.Kind != test.wantKind || verdict.Conclusive != test.conclusive {
				t.Fatalf("verdict = %#v, want kind %q conclusive %v", verdict, test.wantKind, test.conclusive)
			}
		})
	}
}

func TestHealthCheckerActivatesProxyWhenYouTubePasses(t *testing.T) {
	upstream := youtubeStatusProxy(t, http.StatusOK)
	store := &healthLifecycleStoreStub{status: proxylifecycle.StatusIdle}
	checker := NewHealthChecker(store, healthSettings(t, "", ""), nil)

	result, err := checker.CheckProxy(context.Background(), httpProxyModel(upstream.URL))
	if err != nil {
		t.Fatalf("CheckProxy: %v", err)
	}
	if result.Status != "active" || !result.Conclusive {
		t.Fatalf("result = %#v", result)
	}
	if store.evidence.Base.Status != proxylifecycle.ProbeNotRun ||
		store.evidence.YouTube.Status != proxylifecycle.ProbePassed {
		t.Fatalf("evidence = %#v", store.evidence)
	}
}

func TestEveryAppliedHealthVerdictNotifiesProxyControlAfterPersistence(t *testing.T) {
	tests := []struct {
		name           string
		initialStatus  proxylifecycle.Status
		youtubeStatus  int
		wantStatus     string
		wantConclusive bool
	}{
		{
			name: "healthy", initialStatus: proxylifecycle.StatusIdle,
			youtubeStatus: http.StatusOK, wantStatus: "active", wantConclusive: true,
		},
		{
			name: "failed", initialStatus: proxylifecycle.StatusActive,
			youtubeStatus: http.StatusForbidden, wantStatus: "failed", wantConclusive: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upstream := youtubeStatusProxy(t, test.youtubeStatus)
			store := &healthLifecycleStoreStub{status: test.initialStatus}
			checker := NewHealthChecker(store, healthSettings(t, "", ""), nil)
			var events []HealthVerdictEvent
			checker.SetOnVerdictApplied(func(event HealthVerdictEvent) {
				events = append(events, event)
			})
			proxy := httpProxyModel(upstream.URL)
			proxy.ID = 785

			result, err := checker.CheckProxy(context.Background(), proxy)
			if err != nil {
				t.Fatalf("CheckProxy: %v", err)
			}
			if result.Status != test.wantStatus || result.Conclusive != test.wantConclusive {
				t.Fatalf("result = %+v, want status=%s conclusive=%v", result, test.wantStatus, test.wantConclusive)
			}
			if len(events) != 1 {
				t.Fatalf("health Verdict events = %v, want one", events)
			}
			event := events[0]
			if event.ProxyID != proxy.ID || event.ResultingStatus != result.Status ||
				event.Conclusive != result.Conclusive || event.CheckedAt.IsZero() {
				t.Fatalf("health Verdict event = %+v, result = %+v", event, result)
			}
		})
	}
}

func TestUnappliedHealthVerdictDoesNotNotifyProxyControl(t *testing.T) {
	upstream := youtubeStatusProxy(t, http.StatusOK)
	applied := false
	store := &healthLifecycleStoreStub{status: proxylifecycle.StatusIdle, applied: &applied}
	checker := NewHealthChecker(store, healthSettings(t, "", ""), nil)
	notified := false
	checker.SetOnVerdictApplied(func(HealthVerdictEvent) { notified = true })

	if _, err := checker.CheckProxy(context.Background(), httpProxyModel(upstream.URL)); err != nil {
		t.Fatalf("CheckProxy: %v", err)
	}
	if notified {
		t.Fatal("unapplied health evidence emitted a control-plane notification")
	}
}

func TestHealthCheckerClassifiesYouTubeBlockSeparately(t *testing.T) {
	upstream := youtubeStatusProxy(t, http.StatusForbidden)
	store := &healthLifecycleStoreStub{status: proxylifecycle.StatusActive}
	checker := NewHealthChecker(store, healthSettings(t, "", ""), nil)

	result, err := checker.CheckProxy(context.Background(), httpProxyModel(upstream.URL))
	if err != nil {
		t.Fatalf("CheckProxy: %v", err)
	}
	if result.Status != "failed" || result.FailureKind == nil ||
		*result.FailureKind != string(proxylifecycle.FailureYouTubeUnusable) {
		t.Fatalf("result = %#v", result)
	}
	if result.ControlPathHealthy || !result.Conclusive {
		t.Fatalf("control verdict = %#v", result)
	}
}

func TestHealthCheckerTreatsProxySpecificFiveHundredAsConclusive(t *testing.T) {
	upstream := youtubeStatusProxy(t, http.StatusBadGateway)
	store := &healthLifecycleStoreStub{status: proxylifecycle.StatusActive}
	checker := NewHealthChecker(store, healthSettings(t, "", ""), nil)

	result, err := checker.CheckProxy(context.Background(), httpProxyModel(upstream.URL))
	if err != nil {
		t.Fatalf("CheckProxy: %v", err)
	}
	if result.Status != "failed" || !result.Conclusive || result.FailureKind == nil ||
		*result.FailureKind != string(proxylifecycle.FailureYouTubeUnusable) {
		t.Fatalf("result = %#v", result)
	}
}

func TestHealthCheckerTreatsTooManyRequestsAsYouTubeUnusable(t *testing.T) {
	upstream := youtubeStatusProxy(t, http.StatusTooManyRequests)
	store := &healthLifecycleStoreStub{status: proxylifecycle.StatusActive}
	checker := NewHealthChecker(store, healthSettings(t, "", ""), nil)

	result, err := checker.CheckProxy(context.Background(), httpProxyModel(upstream.URL))
	if err != nil {
		t.Fatalf("CheckProxy: %v", err)
	}
	if result.Status != "failed" || !result.Conclusive || result.FailureKind == nil ||
		*result.FailureKind != string(proxylifecycle.FailureYouTubeUnusable) {
		t.Fatalf("result = %#v", result)
	}
}

func TestHealthCheckTLSModes(t *testing.T) {
	target := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	target.Config.ErrorLog = log.New(io.Discard, "", 0)
	target.StartTLS()
	t.Cleanup(target.Close)

	tests := []struct {
		name       string
		strict     bool
		wantStatus proxylifecycle.ProbeStatus
	}{
		{name: "permissive accepts self-signed certificate", strict: false, wantStatus: proxylifecycle.ProbePassed},
		{name: "strict rejects self-signed certificate", strict: true, wantStatus: proxylifecycle.ProbeFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS10}
			configureHealthCheckTLS(transport, tt.strict)
			t.Cleanup(transport.CloseIdleConnections)
			if transport.TLSClientConfig.MinVersion != 0 || transport.TLSClientConfig.MaxVersion != 0 {
				t.Fatalf("TLS versions were not reset: %#v", transport.TLSClientConfig)
			}
			if transport.TLSClientConfig.InsecureSkipVerify == tt.strict {
				t.Fatalf("InsecureSkipVerify = %t for strict=%t", transport.TLSClientConfig.InsecureSkipVerify, tt.strict)
			}
			probe := runProbe(
				context.Background(),
				&http.Client{Transport: transport, Timeout: time.Second},
				target.URL,
				http.StatusOK,
				nil,
			)
			if probe.Status != tt.wantStatus {
				t.Fatalf("probe = %#v, want status %s", probe, tt.wantStatus)
			}
		})
	}
}

func TestTLSCertificateFailureUsesSoftLifecycleWindow(t *testing.T) {
	verdict := youtubeFailureVerdict(
		failedProbe("tls: failed to verify certificate: x509: certificate signed by unknown authority"),
	)
	if verdict.Kind != proxylifecycle.FailureSoftUnreachable || !verdict.Conclusive || verdict.ControlPathHealthy {
		t.Fatalf("verdict = %#v", verdict)
	}
	decision := proxylifecycle.DefaultPolicy().Decide(
		time.Now(),
		proxylifecycle.Snapshot{Status: proxylifecycle.StatusActive},
		verdict,
	)
	if decision.Status != proxylifecycle.StatusFailed || decision.ArchivedAt != nil {
		t.Fatalf("strict TLS failure bypassed the observation window: %#v", decision)
	}
}

func TestValidateYouTubeResponseAcceptsPlayableWatchPage(t *testing.T) {
	watchURL := mustParseURL(t, "https://www.youtube.com/watch?v=test")
	body := []byte(`<script>var player = {"playabilityStatus":{"status":"OK"},"videoDetails":{"videoId":"test"}};</script>`)

	if err := validateYouTubeResponse(watchURL.String(), watchURL, body); err != nil {
		t.Fatalf("validateYouTubeResponse: %v", err)
	}
}

func TestValidateYouTubeResponseRejectsChallengePageWithHTTP200(t *testing.T) {
	watchURL := mustParseURL(t, "https://www.youtube.com/watch?v=test")
	body := []byte(`<html><body>Sign in to confirm you're not a bot</body></html>`)

	err := validateYouTubeResponse(watchURL.String(), watchURL, body)
	if err == nil || !strings.Contains(err.Error(), "challenge") {
		t.Fatalf("error = %v", err)
	}
}

func TestValidateYouTubeResponseRejectsUnavailableVideo(t *testing.T) {
	watchURL := mustParseURL(t, "https://www.youtube.com/watch?v=test")
	body := []byte(`<script>var player = {"playabilityStatus":{"status":"ERROR","reason":"Video unavailable"}};</script>`)

	err := validateYouTubeResponse(watchURL.String(), watchURL, body)
	if err == nil || !strings.Contains(err.Error(), "status ERROR") {
		t.Fatalf("error = %v", err)
	}
}

func TestValidateYouTubeResponseLeavesCustomTargetsUnchanged(t *testing.T) {
	targetURL := mustParseURL(t, "https://example.com/health")
	if err := validateYouTubeResponse(targetURL.String(), targetURL, nil); err != nil {
		t.Fatalf("validateYouTubeResponse: %v", err)
	}
}

func healthSettings(t *testing.T, baseURL, youtubeURL string) *healthSettingsStoreStub {
	t.Helper()
	return &healthSettingsStoreStub{settings: models.Settings{
		HealthCheck: models.HealthCheckSettings{
			Timeout:    2,
			Workers:    1,
			BaseURL:    baseURL,
			BaseStatus: http.StatusOK,
			URL:        youtubeURL,
			Status:     http.StatusOK,
		},
		ProxyLifecycle: models.ProxyLifecycleSettings{
			AutoArchiveEnabled:        true,
			HardUnreachableAfterHours: 6,
			SoftUnreachableAfterHours: 24,
			YouTubeUnusableAfterHours: 72,
		},
	}}
}

func statusServer(t *testing.T, status int) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	t.Cleanup(server.Close)
	return server
}

func statusProxy(t *testing.T, baseStatus, youtubeStatus int) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "youtube") {
			w.WriteHeader(youtubeStatus)
			return
		}
		w.WriteHeader(baseStatus)
	}))
	t.Cleanup(server.Close)
	return server
}

func youtubeStatusProxy(t *testing.T, status int) *httptest.Server {
	t.Helper()
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		if status == http.StatusOK {
			_, _ = w.Write([]byte(`<script>var ytInitialData = {};</script>`))
		}
	}))
	t.Cleanup(upstream.Close)
	var connections atomic.Int32
	return connectTunnelProxy(t, upstream.URL, &connections)
}

func connectTunnelProxy(t *testing.T, upstreamURL string, connections *atomic.Int32) *httptest.Server {
	t.Helper()
	upstream, err := url.Parse(upstreamURL)
	if err != nil {
		t.Fatalf("parse tunnel upstream: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connections.Add(1)
		if r.Method != http.MethodConnect {
			http.Error(w, "CONNECT required", http.StatusMethodNotAllowed)
			return
		}
		upstreamConn, dialErr := net.DialTimeout("tcp", upstream.Host, time.Second)
		if dialErr != nil {
			http.Error(w, dialErr.Error(), http.StatusBadGateway)
			return
		}
		clientConn, buffered, hijackErr := w.(http.Hijacker).Hijack()
		if hijackErr != nil {
			_ = upstreamConn.Close()
			return
		}
		defer clientConn.Close()
		defer upstreamConn.Close()
		_, _ = buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
		if flushErr := buffered.Flush(); flushErr != nil {
			return
		}

		upstreamDone := make(chan struct{})
		go func() {
			_, _ = io.Copy(upstreamConn, clientConn)
			_ = upstreamConn.(*net.TCPConn).CloseWrite()
			close(upstreamDone)
		}()
		_, _ = io.Copy(clientConn, upstreamConn)
		<-upstreamDone
	}))
	t.Cleanup(server.Close)
	return server
}

func httpProxyModel(proxyURL string) *models.Proxy {
	return &models.Proxy{
		ID:       42,
		Address:  strings.TrimPrefix(proxyURL, "http://"),
		Protocol: "http",
		Status:   "idle",
	}
}

func mustParseURL(t *testing.T, rawURL string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("url.Parse(%q): %v", rawURL, err)
	}
	return parsed
}
