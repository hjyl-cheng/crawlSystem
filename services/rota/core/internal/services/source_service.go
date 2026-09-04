package services

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/alpkeskin/rota/core/internal/models"
	proxycore "github.com/alpkeskin/rota/core/internal/proxy"
	"github.com/alpkeskin/rota/core/internal/repository"
	"github.com/alpkeskin/rota/core/internal/sharenode"
	"github.com/alpkeskin/rota/core/internal/sourceinventory"
	"github.com/alpkeskin/rota/core/pkg/logger"
	"go.yaml.in/yaml/v3"
)

// parsedProxy holds the extracted fields from a single proxy list line.
type parsedProxy struct {
	address      string  // host:port
	protocol     string  // empty means "use source default"
	username     *string // nil if not present
	password     *string // nil if not present
	tags         []string
	nodeIdentity string
}

type parsedProxyList struct {
	proxies   []parsedProxy
	total     int
	supported int
	skipped   int
}

type proxyUpserter interface {
	Upsert(context.Context, models.CreateProxyRequest) (int, string, error)
}

// Supported formats (auth is always optional):
//
//	host:port
//	host:port:user:pass
//	user:pass@host:port
//	protocol://host:port
//	protocol://host:port:user:pass
//	protocol://user:pass@host:port
func parseProxyLine(line string) (parsedProxy, bool, error) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return parsedProxy{}, false, nil
	}

	if _, shareURI := sharenode.SchemeProtocol(line); shareURI {
		node, err := sharenode.Parse(line)
		if err != nil {
			return parsedProxy{}, false, err
		}
		credential := node.Credential()
		p := parsedProxy{
			address:      node.Address(),
			protocol:     node.Protocol(),
			password:     &credential,
			nodeIdentity: node.Identity(),
		}
		if node.Name() != "" {
			p.tags = []string{node.Name()}
		}
		return p, true, nil
	}

	var proto string
	var user, pass *string

	// ── 1. Strip protocol scheme if present ──────────────────────────────
	if idx := strings.Index(line, "://"); idx != -1 {
		scheme := strings.ToLower(line[:idx])
		switch scheme {
		case "http", "https", "socks4", "socks4a", "socks5":
			proto = scheme
		default:
			return parsedProxy{}, false, nil
		}
		line = line[idx+3:]
	}

	// ── 1.5 host:port:user:pass — colon-separated creds ──────────────────
	// Disambiguated by validating the port: only triggers when the segment
	// between the first two ':' parses as a 1–65535 integer. Skipped for
	// inputs containing '@' (handled by step 2) and bracketed IPv6 hosts.
	if !strings.ContainsRune(line, '@') && !strings.HasPrefix(line, "[") {
		if i1 := strings.IndexByte(line, ':'); i1 > 0 {
			rest := line[i1+1:]
			if i2 := strings.IndexByte(rest, ':'); i2 > 0 {
				portStr := rest[:i2]
				host := line[:i1]

				if port, err := strconv.Atoi(portStr); err == nil && port >= 1 && port <= 65535 && isValidProxyHost(host) {
					tail := rest[i2+1:]
					if iu := strings.IndexByte(tail, ':'); iu > 0 {
						u := tail[:iu]
						p := tail[iu+1:]

						return parsedProxy{
							address:  host + ":" + portStr,
							protocol: proto,
							username: &u,
							password: &p,
						}, true, nil
					}
					// No second ':' in tail → not the 4-segment form; fall through.
				}
			}
		}
	}

	// ── 2. Try url.Parse for user:pass@host:port ─────────────────────────
	// Wrap with a fake scheme so url.Parse handles the userinfo correctly.
	parsed, err := url.Parse("x://" + line)
	if err == nil && parsed.Host != "" {
		if ui := parsed.User; ui != nil {
			u := ui.Username()
			if u != "" {
				user = &u
			}
			if p, ok := ui.Password(); ok && p != "" {
				pass = &p
			}
		}

		host := parsed.Host
		// url.Parse puts host:port in Host
		if !strings.Contains(host, ":") {
			return parsedProxy{}, false, nil // no port — unusable
		}

		return parsedProxy{
			address:  host,
			protocol: proto,
			username: user,
			password: pass,
		}, true, nil
	}

	// ── 3. Fallback: bare host:port (no userinfo) ─────────────────────────
	if strings.Contains(line, ":") {
		return parsedProxy{address: line, protocol: proto}, true, nil
	}

	return parsedProxy{}, false, nil
}

// isValidProxyHost reports whether s looks like a usable proxy host:
// a non-empty IPv4/IPv6 literal or a hostname-shaped token (letters,
// digits, dots, hyphens — no spaces or URL metacharacters).
func isValidProxyHost(s string) bool {
	if s == "" || strings.ContainsAny(s, " \t\r\n/?#@") {
		return false
	}

	if net.ParseIP(s) != nil {
		return true
	}

	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '.', r == '_':
		default:
			return false
		}
	}

	return true
}

// SourceService fetches proxy lists from remote URLs and imports them into the DB.
type SourceService struct {
	sourceRepo               *repository.SourceRepository
	proxyRepo                *repository.ProxyRepository
	proxyUpserter            proxyUpserter
	poolRepo                 *repository.PoolRepository
	geoSvc                   *GeoIPService
	logger                   *logger.Logger
	client                   *http.Client
	invalidateTransportCache func()
	inventoryPolicy          sourceinventory.Policy

	mu       sync.Mutex
	fetching bool
}

// NewSourceService creates a new SourceService.
func NewSourceService(
	sourceRepo *repository.SourceRepository,
	proxyRepo *repository.ProxyRepository,
	poolRepo *repository.PoolRepository,
	geoSvc *GeoIPService,
	log *logger.Logger,
) *SourceService {
	policy, _ := sourceinventory.NewPolicy("off", 3)
	return &SourceService{
		sourceRepo:      sourceRepo,
		proxyRepo:       proxyRepo,
		proxyUpserter:   proxyRepo,
		poolRepo:        poolRepo,
		geoSvc:          geoSvc,
		logger:          log,
		client:          &http.Client{Timeout: 30 * time.Second},
		inventoryPolicy: policy,
	}
}

func (s *SourceService) SetInventoryPolicy(policy sourceinventory.Policy) {
	s.inventoryPolicy = policy
}

// SetTransportCacheInvalidator wires source credential updates to the proxy
// engine without coupling this package to the transport cache implementation.
func (s *SourceService) SetTransportCacheInvalidator(invalidate func()) {
	s.invalidateTransportCache = invalidate
}

// Run checks for due sources until the owning background Group is cancelled.
func (s *SourceService) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	s.logger.Info("source service started")
	s.enrichMissingGeo(ctx)
	for {
		select {
		case <-ticker.C:
			s.fetchDueSources(ctx)
		case <-ctx.Done():
			s.logger.Info("source service stopped")
			return
		}
	}
}

func (s *SourceService) enrichMissingGeo(ctx context.Context) {
	enriched, err := s.EnrichAll(ctx)
	if err != nil {
		s.logger.Warn("automatic geoip enrichment failed", "error", err)
		return
	}
	if enriched > 0 {
		s.logger.Info("automatically enriched proxy geoip", "proxies", enriched)
	}
}

// FetchNow fetches a single source immediately (called from API handler).
func (s *SourceService) FetchNow(ctx context.Context, sourceID int) (*models.ProxySource, int, error) {
	src, err := s.sourceRepo.GetByID(ctx, sourceID)
	if err != nil || src == nil {
		return nil, 0, fmt.Errorf("source not found: %w", err)
	}
	result, fetchErr := s.fetchAndImport(ctx, src)
	_ = s.sourceRepo.UpdateFetchResult(
		ctx, src.ID, result.created, result.total, result.supported, result.skipped, fetchErr,
	)
	if fetchErr != nil {
		return src, 0, fetchErr
	}
	updated, _ := s.sourceRepo.GetByID(ctx, src.ID)
	return updated, result.created, nil
}

// fetchDueSources finds all sources that are overdue and fetches them.
func (s *SourceService) fetchDueSources(ctx context.Context) {
	if !s.beginDueFetch() {
		return
	}
	defer s.endDueFetch()

	sources, err := s.sourceRepo.GetDueForFetch(ctx)
	if err != nil {
		s.logger.Error("failed to get due sources", "error", err)
		return
	}
	for _, src := range sources {
		srcCopy := src
		result, fetchErr := s.fetchAndImport(ctx, &srcCopy)
		if updateErr := s.sourceRepo.UpdateFetchResult(
			ctx, src.ID, result.created, result.total, result.supported, result.skipped, fetchErr,
		); updateErr != nil {
			s.logger.Error("failed to update source fetch result", "source_id", src.ID, "error", updateErr)
		}
		if fetchErr != nil {
			s.logger.Error("failed to fetch source",
				"source_id", src.ID,
				"url", redactURLForLog(src.URL),
				"error", fetchErr,
			)
		} else {
			s.logger.Info("fetched source",
				"source_id", src.ID, "name", src.Name,
				"imported", result.created,
				"total", result.total,
				"supported", result.supported,
				"skipped", result.skipped)
		}
	}

	// Pool synchronization is part of this owned batch, not detached work.
	s.syncAllPools(ctx)
}

func (s *SourceService) beginDueFetch() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fetching {
		return false
	}
	s.fetching = true
	return true
}

func (s *SourceService) endDueFetch() {
	s.mu.Lock()
	s.fetching = false
	s.mu.Unlock()
}

// syncAllPools re-syncs all auto_sync pools — called after a fetch batch completes
func (s *SourceService) syncAllPools(ctx context.Context) {
	synced, err := s.poolRepo.SyncAllAutoSyncPools(ctx)
	if err != nil {
		s.logger.Error("auto pool sync after fetch failed", "error", err)
	} else if synced > 0 {
		s.logger.Info("auto-synced pools after fetch", "pools", synced)
	}
}

type sourceFetchResult struct {
	created   int
	total     int
	supported int
	skipped   int
}

// fetchAndImport parses each node independently and upserts every unique,
// supported configuration while retaining skipped-line statistics.
func (s *SourceService) fetchAndImport(ctx context.Context, src *models.ProxySource) (sourceFetchResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.URL, nil)
	if err != nil {
		return sourceFetchResult{}, fmt.Errorf("build source request for %s: %s", redactURLForLog(src.URL), redactURLInError(src.URL, err))
	}
	req.Header.Set("User-Agent", "Rota-SourceFetcher/1.0")

	resp, err := s.client.Do(req)
	if err != nil {
		return sourceFetchResult{}, fmt.Errorf("fetch source %s: %s", redactURLForLog(src.URL), redactURLInError(src.URL, err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return sourceFetchResult{}, fmt.Errorf("unexpected HTTP %d from %s", resp.StatusCode, redactURLForLog(src.URL))
	}

	parsed, err := parseProxyListWithStats(resp.Body)
	if err != nil {
		return sourceFetchResult{}, fmt.Errorf("parse failed: %w", err)
	}
	result := sourceFetchResult{
		total:     parsed.total,
		supported: parsed.supported,
		skipped:   parsed.skipped,
	}
	if len(parsed.proxies) == 0 && src.LastSupported > 0 {
		return result, fmt.Errorf(
			"source returned no supported proxies after previously returning %d; inventory reconciliation withheld",
			src.LastSupported,
		)
	}

	// Build upsert requests — protocol from line takes priority over source default
	requests := make([]models.CreateProxyRequest, 0, len(parsed.proxies))
	identities := make([]string, 0, len(parsed.proxies))
	addresses := make([]string, 0, len(parsed.proxies))
	for _, p := range parsed.proxies {
		request, err := sourceProxyRequest(src, p)
		if err != nil {
			result.supported--
			result.skipped++
			continue
		}
		requests = append(requests, request)
		identities = append(identities, request.NodeIdentity)
		addresses = append(addresses, request.Address)
	}

	var failed int
	result.created, _, failed = s.bulkUpsert(ctx, requests)
	if failed > 0 {
		return result, fmt.Errorf("%d supported source proxies failed to upsert; inventory reconciliation withheld", failed)
	}

	reconciliation, err := s.sourceRepo.ReconcileCompleteRefresh(ctx, sourceinventory.CompleteRefresh{
		SourceID:       src.ID,
		NodeIdentities: identities,
		CompletedAt:    time.Now().UTC(),
	}, s.inventoryPolicy)
	if err != nil {
		return result, fmt.Errorf("reconcile complete source refresh: %w", err)
	}
	if s.inventoryPolicy.Mode != sourceinventory.ModeOff {
		s.logger.Info("source inventory reconciliation completed",
			"source_id", src.ID,
			"mode", string(s.inventoryPolicy.Mode),
			"generation", reconciliation.Generation,
			"observed", reconciliation.ObservedCount,
			"newly_missing", reconciliation.NewlyMissingCount,
			"eligible", reconciliation.EligibleCount,
			"retired_memberships", reconciliation.RetiredMembershipCount,
			"archived_proxies", reconciliation.ArchivedProxyCount,
			"reactivated_proxies", reconciliation.ReactivatedProxyCount,
		)
	}

	// Enrichment is best-effort, but remains owned by the fetch context.
	if _, err := s.EnrichAddresses(ctx, addresses); err != nil {
		s.logger.Warn("proxy geoip enrichment failed", "error", err)
	}

	return result, nil
}

func sourceProxyRequest(src *models.ProxySource, parsed parsedProxy) (models.CreateProxyRequest, error) {
	protocol := src.Protocol
	if parsed.protocol != "" {
		protocol = parsed.protocol
	}
	request := models.CreateProxyRequest{
		Address:  parsed.address,
		Protocol: protocol,
		Username: parsed.username,
		Password: parsed.password,
		Tags:     append(append([]string{}, parsed.tags...), src.DefaultTags...),
		SourceID: &src.ID,
	}
	if err := proxycore.NormalizeCreateRequest(&request); err != nil {
		return models.CreateProxyRequest{}, err
	}
	return request, nil
}

// bulkUpsert upserts proxies and invalidates warm transports once if any
// existing endpoint was updated. Returns (created, updated, failed).
// Uses Upsert so that username/password from the list update existing entries.
func (s *SourceService) bulkUpsert(ctx context.Context, proxies []models.CreateProxyRequest) (int, int, int) {
	created := 0
	updated := 0
	failed := 0
	for _, req := range proxies {
		_, status, err := s.proxyUpserter.Upsert(ctx, req)
		if err != nil {
			failed++
		} else if status == "created" {
			created++
		} else if status == "updated" {
			updated++
		}
	}
	if updated > 0 && s.invalidateTransportCache != nil {
		s.invalidateTransportCache()
	}
	return created, updated, failed
}

// EnrichAddresses attempts geo detection only for addresses that have never been checked.
func (s *SourceService) EnrichAddresses(ctx context.Context, addresses []string) (int, error) {
	if len(addresses) == 0 {
		return 0, nil
	}
	if !s.geoSvc.Configured() {
		return 0, fmt.Errorf("geoip provider is not configured")
	}

	rows, err := s.proxyRepo.GetDB().Pool.Query(ctx, `
		UPDATE proxies
		SET geo_updated_at = NOW()
		WHERE geo_updated_at IS NULL
		  AND address = ANY($1::text[])
		RETURNING address
	`, addresses)
	if err != nil {
		return 0, fmt.Errorf("failed to claim proxies for geoip: %w", err)
	}

	missing := make([]string, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))
	for rows.Next() {
		var address string
		if err := rows.Scan(&address); err != nil {
			rows.Close()
			return 0, fmt.Errorf("failed to scan proxy missing geoip: %w", err)
		}
		if _, exists := seen[address]; !exists {
			seen[address] = struct{}{}
			missing = append(missing, address)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("failed to iterate proxies missing geoip: %w", err)
	}
	rows.Close()

	geos := s.geoSvc.EnrichProxies(ctx, missing)

	var updateErrors []error
	updated := 0
	for addr, geo := range geos {
		result, err := s.proxyRepo.GetDB().Pool.Exec(ctx, `
			UPDATE proxies SET
				country_code   = $1,
				country_name   = $2,
				region_name    = $3,
				city_name      = $4,
				latitude       = $5,
				longitude      = $6,
				isp            = $7
			WHERE address = $8
		`, geo.CountryCode, geo.CountryName, geo.RegionName, geo.CityName,
			geo.Latitude, geo.Longitude, geo.ISP, addr,
		)
		if err != nil {
			s.logger.Warn("failed to update geo for proxy", "address", addr, "error", err)
			updateErrors = append(updateErrors, err)
			continue
		}
		updated += int(result.RowsAffected())
	}

	if updated > 0 {
		s.syncAllPools(ctx)
	}

	return updated, errors.Join(updateErrors...)
}

// EnrichAll performs the one-time backfill for proxies that have never been checked.
func (s *SourceService) EnrichAll(ctx context.Context) (int, error) {
	rows, err := s.proxyRepo.GetDB().Pool.Query(ctx,
		`SELECT address FROM proxies WHERE geo_updated_at IS NULL ORDER BY id`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var addresses []string
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			return 0, fmt.Errorf("failed to scan proxy for geoip backfill: %w", err)
		}
		addresses = append(addresses, addr)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("failed to iterate proxies for geoip backfill: %w", err)
	}
	rows.Close()

	if len(addresses) == 0 {
		return 0, nil
	}

	return s.EnrichAddresses(ctx, addresses)
}

const (
	maxProxyListBytes = 10 << 20
	maxProxyLineBytes = 1 << 20
)

// parseProxyList parses plain-text or Base64 subscription content. Malformed
// and unsupported node lines are skipped independently so one bad entry cannot
// prevent a mixed subscription from refreshing.
func parseProxyList(r io.Reader) ([]parsedProxy, error) {
	parsed, err := parseProxyListWithStats(r)
	return parsed.proxies, err
}

func parseProxyListWithStats(r io.Reader) (parsedProxyList, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxProxyListBytes+1))
	if err != nil {
		return parsedProxyList{}, err
	}
	if len(data) > maxProxyListBytes {
		return parsedProxyList{}, fmt.Errorf("proxy list exceeds %d byte limit", maxProxyListBytes)
	}
	data = decodeBase64Subscription(data)
	if proxies, recognized, err := parseClashProxyList(data); recognized {
		if err != nil {
			return parsedProxyList{}, err
		}
		return parsedProxyList{proxies: proxies, total: len(proxies), supported: len(proxies)}, nil
	}

	var proxies []parsedProxy
	seen := make(map[string]int)
	total := 0
	supported := 0
	skipped := 0
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Buffer(make([]byte, 64*1024), maxProxyLineBytes)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		total++
		p, ok, err := parseProxyLine(line)
		if err != nil {
			if _, shareURI := sharenode.SchemeProtocol(line); shareURI {
				skipped++
				continue
			}
			return parsedProxyList{}, fmt.Errorf("line %d: %w", lineNumber, err)
		}
		if !ok {
			skipped++
			continue
		}
		supported++

		key := p.nodeIdentity
		if key == "" {
			key = p.protocol + "\x00" + p.address
		}
		if index, exists := seen[key]; exists {
			for _, tag := range p.tags {
				proxies[index].tags = appendProxyTag(proxies[index].tags, tag)
			}
			continue
		}
		seen[key] = len(proxies)
		proxies = append(proxies, p)
	}
	if err := scanner.Err(); err != nil {
		return parsedProxyList{}, err
	}
	return parsedProxyList{proxies: proxies, total: total, supported: supported, skipped: skipped}, nil
}

type clashProxy struct {
	Name           string `yaml:"name"`
	Type           string `yaml:"type"`
	Server         string `yaml:"server"`
	Port           int    `yaml:"port"`
	Username       string `yaml:"username"`
	Password       string `yaml:"password"`
	TLS            bool   `yaml:"tls"`
	SNI            string `yaml:"sni"`
	SkipCertVerify bool   `yaml:"skip-cert-verify"`
	Insecure       bool   `yaml:"insecure"`
	Fingerprint    string `yaml:"fingerprint"`
	PinSHA256      string `yaml:"pinSHA256"`
	Ports          string `yaml:"ports"`
	MPort          string `yaml:"mport"`
	Obfs           string `yaml:"obfs"`
	ObfsPassword   string `yaml:"obfs-password"`
}

func parseClashProxyList(data []byte) ([]parsedProxy, bool, error) {
	var document struct {
		Proxies yaml.Node `yaml:"proxies"`
	}
	if err := yaml.Unmarshal(data, &document); err != nil {
		return nil, false, nil
	}
	if document.Proxies.Kind == 0 {
		return nil, false, nil
	}
	if document.Proxies.Kind != yaml.SequenceNode {
		return nil, true, fmt.Errorf("Clash proxies must be a YAML list")
	}

	var entries []clashProxy
	if err := document.Proxies.Decode(&entries); err != nil {
		return nil, true, fmt.Errorf("decode Clash proxies: %w", err)
	}

	proxies := make([]parsedProxy, 0, len(entries))
	seen := make(map[string]int, len(entries))
	for _, entry := range entries {
		protocol := strings.ToLower(strings.TrimSpace(entry.Type))
		if protocol == "hysteria2" || protocol == "hy2" {
			proxy, ok := parseClashHysteria2(entry)
			if !ok {
				continue
			}
			if index, exists := seen[proxy.nodeIdentity]; exists {
				for _, tag := range proxy.tags {
					proxies[index].tags = appendProxyTag(proxies[index].tags, tag)
				}
				continue
			}
			seen[proxy.nodeIdentity] = len(proxies)
			proxies = append(proxies, proxy)
			continue
		}
		if protocol == "http" && entry.TLS {
			protocol = "https"
		}
		switch protocol {
		case "http", "https", "socks4", "socks4a", "socks5":
		default:
			continue
		}

		host := strings.TrimSpace(entry.Server)
		if !isValidProxyHost(host) || entry.Port < 1 || entry.Port > 65535 {
			continue
		}
		proxy := parsedProxy{
			address:  net.JoinHostPort(host, strconv.Itoa(entry.Port)),
			protocol: protocol,
		}
		if username := strings.TrimSpace(entry.Username); username != "" {
			proxy.username = &username
		}
		if entry.Password != "" {
			password := entry.Password
			proxy.password = &password
		}
		if name := strings.TrimSpace(entry.Name); name != "" {
			proxy.tags = []string{name}
		}

		key := proxy.protocol + "\x00" + proxy.address
		if index, exists := seen[key]; exists {
			for _, tag := range proxy.tags {
				proxies[index].tags = appendProxyTag(proxies[index].tags, tag)
			}
			continue
		}
		seen[key] = len(proxies)
		proxies = append(proxies, proxy)
	}
	return proxies, true, nil
}

func parseClashHysteria2(entry clashProxy) (parsedProxy, bool) {
	host := strings.TrimSpace(entry.Server)
	if !isValidProxyHost(host) || entry.Port < 1 || entry.Port > 65535 {
		return parsedProxy{}, false
	}
	query := url.Values{}
	if sni := strings.TrimSpace(entry.SNI); sni != "" {
		query.Set("sni", sni)
	}
	if entry.SkipCertVerify || entry.Insecure {
		query.Set("insecure", "1")
	}
	pin := strings.TrimSpace(entry.PinSHA256)
	if pin == "" {
		pin = strings.TrimSpace(entry.Fingerprint)
	}
	if pin != "" {
		query.Set("pinSHA256", pin)
	}
	ports := strings.TrimSpace(entry.Ports)
	if ports == "" {
		ports = strings.TrimSpace(entry.MPort)
	}
	if ports != "" {
		query.Set("mport", ports)
	}
	if obfsType := strings.TrimSpace(entry.Obfs); obfsType != "" {
		query.Set("obfs", obfsType)
		query.Set("obfs-password", entry.ObfsPassword)
	}
	var user *url.Userinfo
	if entry.Password != "" {
		user = url.User(entry.Password)
	}
	raw := (&url.URL{
		Scheme:   "hysteria2",
		User:     user,
		Host:     net.JoinHostPort(host, strconv.Itoa(entry.Port)),
		Fragment: strings.TrimSpace(entry.Name),
		RawQuery: query.Encode(),
	}).String()
	node, err := sharenode.Parse(raw)
	if err != nil {
		return parsedProxy{}, false
	}
	credential := node.Credential()
	proxy := parsedProxy{
		address:      node.Address(),
		protocol:     node.Protocol(),
		password:     &credential,
		nodeIdentity: node.Identity(),
	}
	if node.Name() != "" {
		proxy.tags = []string{node.Name()}
	}
	return proxy, true
}

func decodeBase64Subscription(data []byte) []byte {
	compact := strings.Map(func(r rune) rune {
		if r == ' ' || r == '\t' || r == '\r' || r == '\n' {
			return -1
		}
		return r
	}, strings.TrimSpace(string(data)))
	if compact == "" {
		return data
	}

	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding} {
		decoded, err := encoding.DecodeString(compact)
		if err == nil && utf8.Valid(decoded) && looksLikeDecodedSubscription(decoded) {
			return decoded
		}
	}
	return data
}

func looksLikeDecodedSubscription(data []byte) bool {
	text := strings.ToLower(strings.TrimSpace(string(data)))
	return strings.HasPrefix(text, "vless://") ||
		strings.HasPrefix(text, "vmess://") ||
		strings.HasPrefix(text, "trojan://") ||
		strings.HasPrefix(text, "ss://") ||
		(strings.Contains(text, "://") && strings.ContainsAny(text, "\r\n"))
}

func appendProxyTag(tags []string, tag string) []string {
	for _, existing := range tags {
		if existing == tag {
			return tags
		}
	}
	return append(tags, tag)
}
