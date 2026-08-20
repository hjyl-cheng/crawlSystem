package proxy

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/proxylifecycle"
	"github.com/alpkeskin/rota/core/pkg/logger"
	"github.com/gammazero/workerpool"
)

const (
	defaultBaseHealthURL    = "https://www.google.com/generate_204"
	defaultYouTubeHealthURL = "https://www.youtube.com/watch?v=_xXsXvsYAhA"
	controlCacheTTL         = 30 * time.Second
	maxHealthResponseBody   = 1024 * 1024
	defaultPeriodicBatch    = 20
	defaultPeriodicWorkers  = 4
)

var ErrArchivedProxy = errors.New("archived proxy must be restored before testing")

type HealthLifecycleStore interface {
	ApplyHealthVerdict(
		ctx context.Context,
		proxyID int,
		evidence proxylifecycle.HealthEvidence,
		policy proxylifecycle.Policy,
	) (proxylifecycle.Decision, bool, error)
	ClaimDueHealthChecks(ctx context.Context, limit int) ([]*models.Proxy, error)
}

type HealthSettingsStore interface {
	GetAll(ctx context.Context) (*models.Settings, error)
}

type controlCacheEntry struct {
	healthy   bool
	expiresAt time.Time
}

// HealthChecker performs a base-connectivity probe followed by a YouTube
// probe, then submits one structured verdict to the lifecycle repository.
type HealthChecker struct {
	proxyStore    HealthLifecycleStore
	settingsStore HealthSettingsStore
	logger        *logger.Logger
	probeGate     chan struct{}

	controlMu    sync.Mutex
	controlCache map[string]controlCacheEntry
}

func NewHealthChecker(
	proxyStore HealthLifecycleStore,
	settingsStore HealthSettingsStore,
	log *logger.Logger,
) *HealthChecker {
	return NewHealthCheckerWithLimit(proxyStore, settingsStore, log, defaultPeriodicWorkers)
}

// NewHealthCheckerWithLimit creates the single process-wide probe executor.
// Every periodic, incident, pool, and operator-triggered probe using this
// checker shares the same concurrency budget.
func NewHealthCheckerWithLimit(
	proxyStore HealthLifecycleStore,
	settingsStore HealthSettingsStore,
	log *logger.Logger,
	maxConcurrency int,
) *HealthChecker {
	if maxConcurrency <= 0 {
		maxConcurrency = defaultPeriodicWorkers
	}
	return &HealthChecker{
		proxyStore:    proxyStore,
		settingsStore: settingsStore,
		logger:        log,
		probeGate:     make(chan struct{}, maxConcurrency),
		controlCache:  make(map[string]controlCacheEntry),
	}
}

func (h *HealthChecker) CheckProxy(ctx context.Context, p *models.Proxy) (*models.ProxyTestResult, error) {
	settings, err := h.settingsStore.GetAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("load health settings: %w", err)
	}
	return h.checkProxyWithSettings(ctx, p, settings.HealthCheck, settings.ProxyLifecycle)
}

// CheckProxyAgainst runs the standard base and YouTube probes. A pool may
// select a different YouTube watch URL, but arbitrary targets cannot replace
// the authoritative YouTube lifecycle probe.
func (h *HealthChecker) CheckProxyAgainst(ctx context.Context, p *models.Proxy, targetURL string) (*models.ProxyTestResult, error) {
	settings, err := h.settingsStore.GetAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("load health settings: %w", err)
	}
	if parsed, parseErr := url.Parse(strings.TrimSpace(targetURL)); parseErr == nil && isYouTubeHost(parsed.Hostname()) {
		settings.HealthCheck.URL = strings.TrimSpace(targetURL)
	}
	return h.checkProxyWithSettings(ctx, p, settings.HealthCheck, settings.ProxyLifecycle)
}

func (h *HealthChecker) checkProxyWithSettings(
	ctx context.Context,
	p *models.Proxy,
	healthSettings models.HealthCheckSettings,
	lifecycleSettings models.ProxyLifecycleSettings,
) (*models.ProxyTestResult, error) {
	if p == nil {
		return nil, fmt.Errorf("proxy is required")
	}
	if p.Status == string(proxylifecycle.StatusArchived) {
		return nil, ErrArchivedProxy
	}
	if err := h.acquireProbe(ctx); err != nil {
		return nil, err
	}
	defer h.releaseProbe()

	healthSettings = normalizedHealthSettings(healthSettings)
	startedAt := time.Now()
	evidence := proxylifecycle.HealthEvidence{
		StartedAt: startedAt,
		Base:      proxylifecycle.ProbeEvidence{Status: proxylifecycle.ProbeNotRun},
		YouTube:   proxylifecycle.ProbeEvidence{Status: proxylifecycle.ProbeNotRun},
	}

	transport, err := CreateProxyTransport(p)
	if err != nil {
		evidence.Base = failedProbe(sanitizeHealthError(p, err.Error()))
		evidence.Verdict = h.transportCreationVerdict(ctx, err, healthSettings)
		evidence.Error = evidence.Base.Error
		return h.applyEvidence(ctx, p, startedAt, evidence, lifecycleSettings)
	}
	defer transport.CloseIdleConnections()
	configureHealthCheckTLS(transport, healthSettings.StrictTLS)

	client := &http.Client{
		Transport: transport,
		Timeout:   time.Duration(healthSettings.Timeout) * time.Second,
	}

	evidence.Base = runProbe(ctx, client, healthSettings.BaseURL, healthSettings.BaseStatus, healthSettings.Headers)
	evidence.Base.Error = sanitizeHealthError(p, evidence.Base.Error)
	if evidence.Base.Status != proxylifecycle.ProbePassed {
		evidence.Verdict = h.baseFailureVerdict(ctx, evidence.Base, healthSettings)
		evidence.Error = evidence.Base.Error
		return h.applyEvidence(ctx, p, startedAt, evidence, lifecycleSettings)
	}

	evidence.YouTube = runProbe(ctx, client, healthSettings.URL, healthSettings.Status, healthSettings.Headers)
	evidence.YouTube.Error = sanitizeHealthError(p, evidence.YouTube.Error)
	if evidence.YouTube.Status != proxylifecycle.ProbePassed {
		evidence.Verdict = h.youtubeFailureVerdict(ctx, evidence.YouTube, healthSettings)
		evidence.Error = evidence.YouTube.Error
		return h.applyEvidence(ctx, p, startedAt, evidence, lifecycleSettings)
	}

	evidence.Verdict = proxylifecycle.HealthyVerdict()
	return h.applyEvidence(ctx, p, startedAt, evidence, lifecycleSettings)
}

func (h *HealthChecker) acquireProbe(ctx context.Context) error {
	select {
	case h.probeGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *HealthChecker) releaseProbe() {
	<-h.probeGate
}

func configureHealthCheckTLS(transport *http.Transport, strict bool) {
	if transport == nil {
		return
	}
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	}
	config := transport.TLSClientConfig
	// Reset the shared transport's TLS 1.0 compatibility override. A zero
	// value follows Go's current default minimum and cipher suite policy.
	config.MinVersion = 0
	config.MaxVersion = 0
	config.CipherSuites = nil
	if strict {
		config.InsecureSkipVerify = false
		config.VerifyPeerCertificate = nil
		return
	}
	config.InsecureSkipVerify = true
	config.VerifyPeerCertificate = func([][]byte, [][]*x509.Certificate) error {
		return nil
	}
}

func (h *HealthChecker) applyEvidence(
	ctx context.Context,
	p *models.Proxy,
	startedAt time.Time,
	evidence proxylifecycle.HealthEvidence,
	lifecycleSettings models.ProxyLifecycleSettings,
) (*models.ProxyTestResult, error) {
	evidence.CheckedAt = time.Now()
	policy := proxylifecycle.PolicyFromHours(
		lifecycleSettings.AutoArchiveEnabled,
		lifecycleSettings.HardUnreachableAfterHours,
		lifecycleSettings.SoftUnreachableAfterHours,
		lifecycleSettings.YouTubeUnusableAfterHours,
	)
	decision, _, err := h.proxyStore.ApplyHealthVerdict(ctx, p.ID, evidence, policy)
	if err != nil {
		return nil, fmt.Errorf("record health verdict: %w", err)
	}

	result := &models.ProxyTestResult{
		ID:                  p.ID,
		Address:             p.Address,
		Status:              string(decision.Status),
		TestedAt:            evidence.CheckedAt,
		Conclusive:          evidence.Verdict.Conclusive,
		ControlPathHealthy:  evidence.Verdict.ControlPathHealthy,
		BaseHealthStatus:    string(evidence.Base.Status),
		YouTubeHealthStatus: string(evidence.YouTube.Status),
	}
	if evidence.Verdict.Conclusive && evidence.Verdict.Kind != proxylifecycle.FailureNone {
		kind := string(evidence.Verdict.Kind)
		result.FailureKind = &kind
	}
	if evidence.Error != "" {
		errMessage := evidence.Error
		result.Error = &errMessage
	}
	if evidence.Verdict.Healthy {
		duration := int(evidence.CheckedAt.Sub(startedAt).Milliseconds())
		result.ResponseTime = &duration
	}
	return result, nil
}

func (h *HealthChecker) transportCreationVerdict(
	ctx context.Context,
	err error,
	settings models.HealthCheckSettings,
) proxylifecycle.Verdict {
	kind := classifyConnectionFailure(err)
	if endpointConfigurationFailure(err) {
		kind = proxylifecycle.FailureHardUnreachable
	}
	controlHealthy := h.controlPathHealthy(ctx, settings.BaseURL, settings.BaseStatus, settings)
	return proxylifecycle.Verdict{
		Kind:               kind,
		Conclusive:         kind != proxylifecycle.FailureNone && controlHealthy,
		ControlPathHealthy: controlHealthy,
	}
}

func (h *HealthChecker) baseFailureVerdict(
	ctx context.Context,
	probe proxylifecycle.ProbeEvidence,
	settings models.HealthCheckSettings,
) proxylifecycle.Verdict {
	kind := baseFailureKind(probe)
	controlHealthy := h.controlPathHealthy(ctx, settings.BaseURL, settings.BaseStatus, settings)
	return proxylifecycle.Verdict{
		Kind:               kind,
		Conclusive:         kind != proxylifecycle.FailureNone && controlHealthy,
		ControlPathHealthy: controlHealthy,
	}
}

func (h *HealthChecker) youtubeFailureVerdict(
	ctx context.Context,
	probe proxylifecycle.ProbeEvidence,
	settings models.HealthCheckSettings,
) proxylifecycle.Verdict {
	controlHealthy := h.controlPathHealthy(ctx, settings.URL, settings.Status, settings)
	return proxylifecycle.Verdict{
		Kind:               proxylifecycle.FailureYouTubeUnusable,
		Conclusive:         controlHealthy,
		ControlPathHealthy: controlHealthy,
	}
}

func (h *HealthChecker) controlPathHealthy(
	ctx context.Context,
	targetURL string,
	expectedStatus int,
	settings models.HealthCheckSettings,
) bool {
	key := fmt.Sprintf("%s|%d|%s", targetURL, expectedStatus, strings.Join(settings.Headers, "\n"))
	now := time.Now()
	h.controlMu.Lock()
	entry, found := h.controlCache[key]
	if found && now.Before(entry.expiresAt) {
		h.controlMu.Unlock()
		return entry.healthy
	}
	h.controlMu.Unlock()

	directTransport := &http.Transport{}
	if defaultTransport, ok := http.DefaultTransport.(*http.Transport); ok {
		directTransport = defaultTransport.Clone()
	}
	directTransport.Proxy = nil
	defer directTransport.CloseIdleConnections()
	timeout := time.Duration(settings.Timeout) * time.Second
	if timeout <= 0 || timeout > 15*time.Second {
		timeout = 15 * time.Second
	}
	client := &http.Client{Transport: directTransport, Timeout: timeout}
	probe := runProbe(ctx, client, targetURL, expectedStatus, settings.Headers)
	healthy := probe.Status == proxylifecycle.ProbePassed

	h.controlMu.Lock()
	h.controlCache[key] = controlCacheEntry{healthy: healthy, expiresAt: now.Add(controlCacheTTL)}
	h.controlMu.Unlock()
	return healthy
}

func runProbe(
	ctx context.Context,
	client *http.Client,
	targetURL string,
	expectedStatus int,
	headers []string,
) proxylifecycle.ProbeEvidence {
	startedAt := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return failedProbe(fmt.Sprintf("create health request: %v", err))
	}
	for _, header := range headers {
		parts := strings.SplitN(header, ":", 2)
		if len(parts) == 2 {
			req.Header.Set(strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
		}
	}

	resp, err := client.Do(req)
	duration := int(time.Since(startedAt).Milliseconds())
	if err != nil {
		probe := failedProbe(err.Error())
		probe.ResponseTimeMS = &duration
		return probe
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxHealthResponseBody))

	probe := proxylifecycle.ProbeEvidence{
		HTTPStatus:     intPtr(resp.StatusCode),
		ResponseTimeMS: &duration,
	}
	if readErr != nil {
		probe.Status = proxylifecycle.ProbeFailed
		probe.Error = fmt.Sprintf("read health response: %v", readErr)
		return probe
	}
	if resp.StatusCode != expectedStatus {
		probe.Status = proxylifecycle.ProbeFailed
		probe.Error = fmt.Sprintf("unexpected status code: got %d, expected %d", resp.StatusCode, expectedStatus)
		return probe
	}
	if err := validateYouTubeResponse(targetURL, resp.Request.URL, body); err != nil {
		probe.Status = proxylifecycle.ProbeFailed
		probe.Error = err.Error()
		return probe
	}
	probe.Status = proxylifecycle.ProbePassed
	return probe
}

func validateYouTubeResponse(targetURL string, finalURL *url.URL, body []byte) error {
	requestedURL, err := url.Parse(targetURL)
	if err != nil || !isYouTubeHost(requestedURL.Hostname()) {
		return nil
	}
	if finalURL == nil || !isYouTubeHost(finalURL.Hostname()) {
		return fmt.Errorf("YouTube health request redirected outside YouTube")
	}

	lowerBody := strings.ToLower(string(body))
	for _, marker := range []string{
		"/sorry/index",
		"detected unusual traffic",
		"confirm you're not a bot",
		"confirm you&#39;re not a bot",
		"verify you are human",
		"before you continue to youtube",
	} {
		if strings.Contains(lowerBody, marker) {
			return fmt.Errorf("YouTube health request returned a challenge or consent page")
		}
	}

	if finalURL.Path != "/watch" {
		if !bytes.Contains(body, []byte("ytInitialData")) {
			return fmt.Errorf("YouTube page marker is missing")
		}
		return nil
	}

	status, ok := youtubePlayabilityStatus(body)
	if !ok {
		return fmt.Errorf("YouTube watch page playability status is missing")
	}
	if !strings.EqualFold(status, "OK") {
		return fmt.Errorf("YouTube video is not playable: status %s", status)
	}
	return nil
}

func isYouTubeHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	return host == "youtube.com" || strings.HasSuffix(host, ".youtube.com") || host == "youtu.be"
}

func youtubePlayabilityStatus(body []byte) (string, bool) {
	const marker = `"playabilityStatus"`
	for searchFrom := 0; searchFrom < len(body); {
		markerAt := bytes.Index(body[searchFrom:], []byte(marker))
		if markerAt < 0 {
			return "", false
		}
		markerAt += searchFrom
		valueStart := markerAt + len(marker)
		colonAt := bytes.IndexByte(body[valueStart:], ':')
		if colonAt < 0 {
			return "", false
		}
		valueStart += colonAt + 1
		for valueStart < len(body) && (body[valueStart] == ' ' || body[valueStart] == '\n' || body[valueStart] == '\r' || body[valueStart] == '\t') {
			valueStart++
		}
		object, ok := extractJSONObject(body[valueStart:])
		if ok {
			var playability struct {
				Status string `json:"status"`
			}
			if json.Unmarshal(object, &playability) == nil && playability.Status != "" {
				return playability.Status, true
			}
		}
		searchFrom = markerAt + len(marker)
	}
	return "", false
}

func extractJSONObject(value []byte) ([]byte, bool) {
	if len(value) == 0 || value[0] != '{' {
		return nil, false
	}
	depth := 0
	inString := false
	escaped := false
	for i, char := range value {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if char == '\\' {
				escaped = true
				continue
			}
			if char == '"' {
				inString = false
			}
			continue
		}
		switch char {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return value[:i+1], true
			}
		}
	}
	return nil, false
}

func failedProbe(message string) proxylifecycle.ProbeEvidence {
	return proxylifecycle.ProbeEvidence{Status: proxylifecycle.ProbeFailed, Error: message}
}

func baseFailureKind(probe proxylifecycle.ProbeEvidence) proxylifecycle.FailureKind {
	if probe.HTTPStatus != nil {
		if *probe.HTTPStatus == http.StatusProxyAuthRequired {
			return proxylifecycle.FailureHardUnreachable
		}
		return proxylifecycle.FailureSoftUnreachable
	}
	return classifyConnectionFailure(errors.New(probe.Error))
}

func classifyConnectionFailure(err error) proxylifecycle.FailureKind {
	if err == nil {
		return proxylifecycle.FailureNone
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH) {
		return proxylifecycle.FailureSoftUnreachable
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return proxylifecycle.FailureHardUnreachable
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return proxylifecycle.FailureSoftUnreachable
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return proxylifecycle.FailureSoftUnreachable
	}

	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"timeout", "timed out", "network is unreachable", "no route to host",
		"temporary failure", "server misbehaving", "name resolution", "x509:",
		"failed to verify certificate", "certificate signed by unknown authority",
		"certificate has expired", "certificate is not yet valid",
		"closed pipe", "unexpected eof",
	} {
		if strings.Contains(message, marker) {
			return proxylifecycle.FailureSoftUnreachable
		}
	}
	for _, marker := range []string{
		"connection refused", "proxy authentication required", "authentication failed",
		"bad handshake", "handshake failure", "socks connect",
	} {
		if strings.Contains(message, marker) {
			return proxylifecycle.FailureHardUnreachable
		}
	}
	return proxylifecycle.FailureNone
}

func endpointConfigurationFailure(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "failed to start ") && strings.Contains(message, " runtime for ") {
		return false
	}
	for _, marker := range []string{
		"unsupported proxy protocol", "invalid proxy url", "share node credential",
		"share uri", "failed to create socks5 dialer", "endpoint does not match",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func sanitizeHealthError(p *models.Proxy, message string) string {
	if message == "" || p == nil {
		return message
	}
	for _, secret := range []*string{p.Username, p.Password} {
		if secret != nil && *secret != "" {
			message = strings.ReplaceAll(message, *secret, "[redacted]")
		}
	}
	return message
}

func normalizedHealthSettings(settings models.HealthCheckSettings) models.HealthCheckSettings {
	if settings.Timeout <= 0 {
		settings.Timeout = 60
	}
	if settings.Workers <= 0 {
		settings.Workers = 20
	}
	if strings.TrimSpace(settings.BaseURL) == "" {
		settings.BaseURL = defaultBaseHealthURL
	}
	if settings.BaseStatus <= 0 {
		settings.BaseStatus = http.StatusNoContent
	}
	if strings.TrimSpace(settings.URL) == "" || settings.URL == "https://api.ipify.org" {
		settings.URL = defaultYouTubeHealthURL
	}
	if settings.Status <= 0 {
		settings.Status = http.StatusOK
	}
	return settings
}

func (h *HealthChecker) CheckAllProxies(ctx context.Context) ([]models.ProxyTestResult, error) {
	return h.checkDueHealthChecks(ctx, defaultPeriodicBatch, defaultPeriodicWorkers)
}

func (h *HealthChecker) checkDueHealthChecks(
	ctx context.Context,
	batchSize int,
	workers int,
) ([]models.ProxyTestResult, error) {
	batchSize, workers = normalizePeriodicHealthCheckLimits(batchSize, workers)
	startedAt := time.Now()
	settings, err := h.settingsStore.GetAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("load health settings: %w", err)
	}
	healthSettings := normalizedHealthSettings(settings.HealthCheck)
	proxies, err := h.proxyStore.ClaimDueHealthChecks(ctx, batchSize)
	if err != nil {
		return nil, err
	}
	if len(proxies) == 0 {
		return []models.ProxyTestResult{}, nil
	}

	if h.logger != nil {
		h.logger.Info("starting due proxy health-check batch", "proxy_count", len(proxies), "workers", workers)
	}
	pool := workerpool.New(workers)
	results := make([]models.ProxyTestResult, len(proxies))
	for index, item := range proxies {
		index, item := index, item
		pool.Submit(func() {
			result, checkErr := h.checkProxyWithSettings(ctx, item, healthSettings, settings.ProxyLifecycle)
			if checkErr != nil {
				errMessage := checkErr.Error()
				results[index] = models.ProxyTestResult{
					ID:       item.ID,
					Address:  item.Address,
					Status:   item.Status,
					Error:    &errMessage,
					TestedAt: time.Now(),
				}
				return
			}
			results[index] = *result
		})
	}
	pool.StopWait()
	if h.logger != nil {
		active, failed, inconclusive := summarizeHealthCheckResults(results)
		h.logger.Info(
			"periodic proxy health-check batch completed",
			"claimed", len(results),
			"active", active,
			"conclusive_failed", failed,
			"inconclusive", inconclusive,
			"duration_ms", time.Since(startedAt).Milliseconds(),
		)
	}
	return results, nil
}

func normalizePeriodicHealthCheckLimits(batchSize, workers int) (int, int) {
	if batchSize <= 0 {
		batchSize = defaultPeriodicBatch
	}
	if workers <= 0 {
		workers = defaultPeriodicWorkers
	}
	if workers > batchSize {
		workers = batchSize
	}
	return batchSize, workers
}

func summarizeHealthCheckResults(results []models.ProxyTestResult) (active, failed, inconclusive int) {
	for _, result := range results {
		if !result.Conclusive {
			inconclusive++
			continue
		}
		if result.Status == string(proxylifecycle.StatusActive) {
			active++
			continue
		}
		failed++
	}
	return active, failed, inconclusive
}

func (h *HealthChecker) StartPeriodicHealthCheck(
	ctx context.Context,
	interval time.Duration,
	batchSize int,
	workers int,
) {
	batchSize, workers = normalizePeriodicHealthCheckLimits(batchSize, workers)
	runPeriodicHealthCheckLoop(
		ctx,
		interval,
		func(ctx context.Context) error {
			_, err := h.checkDueHealthChecks(ctx, batchSize, workers)
			if err != nil && h.logger != nil {
				h.logger.Error("periodic health check failed", "error", err)
			}
			return err
		},
		waitForPeriodicHealthCheck,
	)
}

func runPeriodicHealthCheckLoop(
	ctx context.Context,
	interval time.Duration,
	check func(context.Context) error,
	wait func(context.Context, time.Duration) bool,
) {
	if interval <= 0 {
		interval = time.Minute
	}
	if wait == nil {
		wait = waitForPeriodicHealthCheck
	}
	for {
		if !wait(ctx, interval) {
			return
		}
		_ = check(ctx)
	}
}

func waitForPeriodicHealthCheck(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func intPtr(value int) *int {
	return &value
}
