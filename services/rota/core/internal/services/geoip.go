package services

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/pkg/logger"
	"github.com/oschwald/maxminddb-golang"
)

// ipAPIResponse is the response from ip-api.com batch endpoint
type ipAPIResponse struct {
	Status      string  `json:"status"`
	Country     string  `json:"country"`
	CountryCode string  `json:"countryCode"`
	Region      string  `json:"regionName"`
	City        string  `json:"city"`
	ISP         string  `json:"isp"`
	Lat         float64 `json:"lat"`
	Lon         float64 `json:"lon"`
	Query       string  `json:"query"`
}

type cacheEntry struct {
	geo      models.GeoInfo
	cachedAt time.Time
}

type mmdbCountry struct {
	ISOCode string            `maxminddb:"iso_code"`
	Names   map[string]string `maxminddb:"names"`
}

type mmdbCountryRecord struct {
	Country           mmdbCountry `maxminddb:"country"`
	RegisteredCountry mmdbCountry `maxminddb:"registered_country"`
	Subdivisions      []struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"subdivisions"`
	City struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"city"`
	Location struct {
		Latitude  float64 `maxminddb:"latitude"`
		Longitude float64 `maxminddb:"longitude"`
	} `maxminddb:"location"`
}

func (r mmdbCountryRecord) geoInfo() (models.GeoInfo, bool) {
	country := r.Country
	if country.ISOCode == "" {
		country = r.RegisteredCountry
	}
	if country.ISOCode == "" {
		return models.GeoInfo{}, false
	}

	name := country.Names["en"]
	if name == "" {
		name = country.ISOCode
	}
	geo := models.GeoInfo{
		CountryCode: country.ISOCode,
		CountryName: name,
		CityName:    r.City.Names["en"],
		Latitude:    r.Location.Latitude,
		Longitude:   r.Location.Longitude,
	}
	if len(r.Subdivisions) > 0 {
		geo.RegionName = r.Subdivisions[0].Names["en"]
	}
	return geo, true
}

type GeoIPSettingsStore interface {
	GetAll(ctx context.Context) (*models.Settings, error)
	SetGeoIPLastUpdated(ctx context.Context, updatedAt time.Time) error
}

type geoDBReader interface {
	Lookup(ip net.IP, result any) error
	Close() error
}

type openedGeoDatabase struct {
	reader       geoDBReader
	databaseType string
	buildTime    *time.Time
}

var ErrGeoIPUpdateInProgress = errors.New("geoip database update is already running")

// GeoIPService performs local MMDB lookups and caches results for 24 hours.
// A licensed remote batch endpoint can be configured explicitly as a fallback provider.
type GeoIPService struct {
	client         *http.Client
	downloadClient *http.Client
	batchURL       string
	localDBPath    string
	cache          map[string]cacheEntry
	mu             sync.RWMutex
	logger         *logger.Logger
	cacheTTL       time.Duration
	cacheMax       int

	readerMu           sync.RWMutex
	geoDB              geoDBReader
	activeDBPath       string
	activeDBSource     string
	activeDatabaseType string
	activeDBBuildTime  *time.Time
	openDatabase       func([]byte) (*openedGeoDatabase, error)

	configMu           sync.RWMutex
	reloadMu           sync.Mutex
	settings           models.GeoIPSettings
	settingsGeneration uint64
	lastError          string
	updating           bool
	settingsStore      GeoIPSettingsStore
	updateSlot         chan struct{}
	updateWake         chan struct{}
	managedDir         string

	remoteSlot chan struct{}
	lastRemote time.Time
	remoteGap  time.Duration

	closeOnce sync.Once
	closeErr  error
}

const (
	defaultGeoCacheMax      = 10_000
	maxGeoResponseBytes     = 2 << 20
	maxGeoDownloadBytes     = 256 << 20
	maxGeoDatabaseBytes     = 256 << 20
	defaultGeoRemoteGap     = 1500 * time.Millisecond
	geoCacheSweepInterval   = time.Hour
	geoUpdateCheckInterval  = time.Hour
	geoDownloadTimeout      = 5 * time.Minute
	defaultManagedGeoIPPath = "/app/geoip/managed/GeoLite2-City.mmdb"
	defaultGeoUpdateHours   = 168
)

// NewGeoIPService creates a new GeoIPService
func NewGeoIPService(log *logger.Logger) *GeoIPService {
	return newGeoIPService(nil, log)
}

// NewManagedGeoIPService creates a GeoIP service backed by persisted settings.
func NewManagedGeoIPService(settingsStore GeoIPSettingsStore, log *logger.Logger) *GeoIPService {
	return newGeoIPService(settingsStore, log)
}

func newGeoIPService(settingsStore GeoIPSettingsStore, log *logger.Logger) *GeoIPService {
	service := &GeoIPService{
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
		downloadClient: &http.Client{
			Timeout: geoDownloadTimeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return fmt.Errorf("too many geoip download redirects")
				}
				if req.URL.Scheme != "https" {
					return fmt.Errorf("geoip download redirect must use HTTPS")
				}
				req.Header.Del("Referer")
				return nil
			},
		},
		batchURL:      strings.TrimSpace(os.Getenv("GEOIP_API_BATCH_URL")),
		localDBPath:   strings.TrimSpace(os.Getenv("GEOIP_DATABASE_PATH")),
		cache:         make(map[string]cacheEntry),
		logger:        log,
		cacheTTL:      24 * time.Hour,
		cacheMax:      defaultGeoCacheMax,
		settingsStore: settingsStore,
		settings:      defaultGeoIPSettings(),
		updateSlot:    make(chan struct{}, 1),
		updateWake:    make(chan struct{}, 1),
		managedDir:    filepath.Dir(defaultManagedGeoIPPath),
		remoteGap:     defaultGeoRemoteGap,
		remoteSlot:    make(chan struct{}, 1),
		openDatabase:  openGeoDatabase,
	}

	if settingsStore != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		persisted, err := settingsStore.GetAll(ctx)
		cancel()
		if err != nil && log != nil {
			log.Warn("failed to load geoip settings; using local defaults", "error", err)
		} else if persisted != nil {
			service.settings = NormalizeGeoIPSettings(persisted.GeoIP)
		}
	}

	if service.settings.Provider == models.GeoIPProviderMaxMind {
		if err := service.loadDatabase(service.settings.MaxMindDBPath, "managed"); err == nil {
			return service
		} else if log != nil {
			log.Warn("managed geoip database unavailable; retaining local fallback", "error", err)
		}
	}
	if service.localDBPath != "" {
		if err := service.loadDatabase(service.localDBPath, "local"); err != nil && log != nil {
			log.Error("failed to load local geoip database", "path", service.localDBPath, "error", err)
		}
	}
	return service
}

func defaultGeoIPSettings() models.GeoIPSettings {
	return models.GeoIPSettings{
		Provider:            models.GeoIPProviderLocal,
		MaxMindDBPath:       defaultManagedGeoIPPath,
		UpdateIntervalHours: defaultGeoUpdateHours,
	}
}

// NormalizeGeoIPSettings fills stable defaults without changing credentials.
func NormalizeGeoIPSettings(settings models.GeoIPSettings) models.GeoIPSettings {
	if settings.Provider == "" {
		settings.Provider = models.GeoIPProviderLocal
	}
	if strings.TrimSpace(settings.MaxMindDBPath) == "" {
		settings.MaxMindDBPath = defaultManagedGeoIPPath
	}
	if settings.UpdateIntervalHours == 0 {
		settings.UpdateIntervalHours = defaultGeoUpdateHours
	}
	settings.MaxMindDBPath = filepath.Clean(strings.TrimSpace(settings.MaxMindDBPath))
	settings.MaxMindURL = strings.TrimSpace(settings.MaxMindURL)
	settings.MaxMindLicenseKey = strings.TrimSpace(settings.MaxMindLicenseKey)
	return settings
}

func openGeoDatabase(databaseBytes []byte) (*openedGeoDatabase, error) {
	reader, err := maxminddb.FromBytes(databaseBytes)
	if err != nil {
		return nil, err
	}
	if err := reader.Verify(); err != nil {
		_ = reader.Close()
		return nil, err
	}
	databaseType := reader.Metadata.DatabaseType
	if !strings.Contains(strings.ToLower(databaseType), "city") &&
		!strings.Contains(strings.ToLower(databaseType), "country") {
		_ = reader.Close()
		return nil, fmt.Errorf("unsupported geoip database type %q", databaseType)
	}
	var buildTime *time.Time
	if reader.Metadata.BuildEpoch > 0 {
		builtAt := time.Unix(int64(reader.Metadata.BuildEpoch), 0).UTC()
		buildTime = &builtAt
	}
	return &openedGeoDatabase{reader: reader, databaseType: databaseType, buildTime: buildTime}, nil
}

func (g *GeoIPService) loadDatabase(databasePath, source string) error {
	opened, cleanedPath, err := g.prepareDatabase(databasePath)
	if err != nil {
		return err
	}
	g.swapDatabase(opened, cleanedPath, source)
	if g.logger != nil {
		g.logger.Info("loaded geoip database", "path", cleanedPath, "source", source, "database_type", opened.databaseType)
	}
	return nil
}

func (g *GeoIPService) prepareDatabase(databasePath string) (*openedGeoDatabase, string, error) {
	databasePath = filepath.Clean(strings.TrimSpace(databasePath))
	if databasePath == "." || databasePath == "" {
		return nil, "", fmt.Errorf("geoip database path is empty")
	}
	info, err := os.Stat(databasePath)
	if err != nil {
		return nil, "", fmt.Errorf("stat geoip database: %w", err)
	}
	if info.Size() <= 0 || info.Size() > maxGeoDatabaseBytes {
		return nil, "", fmt.Errorf("geoip database size %d is outside the allowed range", info.Size())
	}
	databaseBytes, err := os.ReadFile(databasePath)
	if err != nil {
		return nil, "", fmt.Errorf("read geoip database: %w", err)
	}
	opened, err := g.openDatabase(databaseBytes)
	if err != nil {
		return nil, "", fmt.Errorf("validate geoip database: %w", err)
	}
	return opened, databasePath, nil
}

func (g *GeoIPService) swapDatabase(opened *openedGeoDatabase, databasePath, source string) {
	g.readerMu.Lock()
	oldReader := g.geoDB
	g.geoDB = opened.reader
	g.activeDBPath = databasePath
	g.activeDBSource = source
	g.activeDatabaseType = opened.databaseType
	g.activeDBBuildTime = opened.buildTime
	g.readerMu.Unlock()
	if oldReader != nil {
		_ = oldReader.Close()
	}

	g.mu.Lock()
	clear(g.cache)
	g.mu.Unlock()
}

// Run evicts expired cache entries until the owning background Group stops.
func (g *GeoIPService) Run(ctx context.Context) {
	cacheTicker := time.NewTicker(geoCacheSweepInterval)
	updateTicker := time.NewTicker(geoUpdateCheckInterval)
	defer cacheTicker.Stop()
	defer updateTicker.Stop()

	g.runScheduledUpdate(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-cacheTicker.C:
			g.sweepCache(now)
		case <-updateTicker.C:
			g.runScheduledUpdate(ctx)
		case <-g.updateWake:
			g.runScheduledUpdate(ctx)
		}
	}
}

// Close releases the local MMDB reader after all lookups have drained.
func (g *GeoIPService) Close() error {
	g.closeOnce.Do(func() {
		g.readerMu.Lock()
		defer g.readerMu.Unlock()
		if g.geoDB != nil {
			g.closeErr = g.geoDB.Close()
			g.geoDB = nil
		}
	})
	return g.closeErr
}

func (g *GeoIPService) Configured() bool {
	g.readerMu.RLock()
	configured := g.geoDB != nil
	g.readerMu.RUnlock()
	return configured || g.batchURL != ""
}

// ReloadSettings applies persisted GeoIP settings while retaining the last
// known good database if the newly selected file cannot be opened.
func (g *GeoIPService) ReloadSettings(ctx context.Context) error {
	if g.settingsStore == nil {
		return nil
	}
	g.reloadMu.Lock()
	defer g.reloadMu.Unlock()

	allSettings, err := g.settingsStore.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load geoip settings: %w", err)
	}
	settings := NormalizeGeoIPSettings(allSettings.GeoIP)
	if err := g.ValidateSettings(settings); err != nil {
		return err
	}

	g.configMu.Lock()
	g.settings = settings
	g.settingsGeneration++
	generation := g.settingsGeneration
	g.lastError = ""
	g.configMu.Unlock()

	var loadErr error
	var source string
	var databasePath string
	if settings.Provider == models.GeoIPProviderMaxMind {
		databasePath, source = settings.MaxMindDBPath, "managed"
	} else if g.localDBPath != "" {
		databasePath, source = g.localDBPath, "local"
	}
	if databasePath != "" {
		var opened *openedGeoDatabase
		var cleanedPath string
		opened, cleanedPath, loadErr = g.prepareDatabase(databasePath)
		if loadErr == nil {
			g.configMu.Lock()
			if g.settingsGeneration != generation {
				loadErr = fmt.Errorf("geoip settings changed while loading database")
				_ = opened.reader.Close()
			} else {
				g.swapDatabase(opened, cleanedPath, source)
				if g.logger != nil {
					g.logger.Info("loaded geoip database", "path", cleanedPath, "source", source, "database_type", opened.databaseType)
				}
			}
			g.configMu.Unlock()
		}
	}
	if loadErr != nil {
		g.recordUpdateError(loadErr)
	}
	select {
	case g.updateWake <- struct{}{}:
	default:
	}
	return loadErr
}

// ValidateSettings checks managed paths and download sources before persistence.
func (g *GeoIPService) ValidateSettings(settings models.GeoIPSettings) error {
	settings = NormalizeGeoIPSettings(settings)
	if settings.Provider != models.GeoIPProviderLocal && settings.Provider != models.GeoIPProviderMaxMind {
		return fmt.Errorf("geoip.provider must be either %q or %q", models.GeoIPProviderLocal, models.GeoIPProviderMaxMind)
	}
	if settings.UpdateIntervalHours < 1 || settings.UpdateIntervalHours > 8760 {
		return fmt.Errorf("geoip.update_interval_hours must be between 1 and 8760")
	}
	if settings.Provider == models.GeoIPProviderMaxMind {
		if err := validateManagedDatabasePath(settings.MaxMindDBPath, g.managedDir); err != nil {
			return err
		}
		if settings.MaxMindURL != "" {
			if err := validateGeoIPDownloadURL(settings.MaxMindURL); err != nil {
				return err
			}
		}
		if settings.AutoUpdate && settings.MaxMindLicenseKey == "" && settings.MaxMindURL == "" {
			return fmt.Errorf("geoip auto-update requires a MaxMind license key or explicit HTTPS download URL")
		}
	}
	return nil
}

func validateManagedDatabasePath(databasePath, managedDir string) error {
	cleaned := filepath.Clean(strings.TrimSpace(databasePath))
	if !filepath.IsAbs(cleaned) {
		return fmt.Errorf("geoip.maxmind_db_path must be an absolute path")
	}
	if filepath.Ext(cleaned) != ".mmdb" {
		return fmt.Errorf("geoip.maxmind_db_path must end in .mmdb")
	}
	managedDir = filepath.Clean(managedDir)
	relative, err := filepath.Rel(managedDir, cleaned)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("geoip.maxmind_db_path must be inside %s", managedDir)
	}
	return nil
}

func validateGeoIPDownloadURL(rawURL string) error {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Host == "" {
		return fmt.Errorf("geoip.maxmind_url must be a valid HTTPS URL")
	}
	if parsed.Scheme != "https" {
		return fmt.Errorf("geoip.maxmind_url must use HTTPS")
	}
	return nil
}

// Status returns a credential-free snapshot of current GeoIP state.
func (g *GeoIPService) Status() models.GeoIPStatus {
	g.configMu.RLock()
	settings := g.settings
	lastError := g.lastError
	updating := g.updating
	g.configMu.RUnlock()

	g.readerMu.RLock()
	databaseLoaded := g.geoDB != nil
	activeDBPath := g.activeDBPath
	activeDBSource := g.activeDBSource
	databaseType := g.activeDatabaseType
	buildTime := g.activeDBBuildTime
	g.readerMu.RUnlock()

	return models.GeoIPStatus{
		Provider:            settings.Provider,
		Configured:          databaseLoaded || g.batchURL != "",
		DatabaseLoaded:      databaseLoaded,
		DatabasePath:        settings.MaxMindDBPath,
		ActiveDatabasePath:  activeDBPath,
		DatabaseType:        databaseType,
		DatabaseBuildTime:   buildTime,
		Source:              activeDBSource,
		LicenseConfigured:   settings.MaxMindLicenseKey != "",
		AutoUpdate:          settings.AutoUpdate,
		UpdateIntervalHours: settings.UpdateIntervalHours,
		LastUpdatedAt:       settings.LastUpdatedAt,
		Updating:            updating,
		LastError:           lastError,
	}
}

func (g *GeoIPService) runScheduledUpdate(ctx context.Context) {
	g.configMu.RLock()
	settings := g.settings
	g.configMu.RUnlock()
	if settings.Provider != models.GeoIPProviderMaxMind || !settings.AutoUpdate {
		return
	}
	lastUpdate := settings.LastUpdatedAt
	if info, err := os.Stat(settings.MaxMindDBPath); err == nil && (lastUpdate == nil || info.ModTime().After(*lastUpdate)) {
		modifiedAt := info.ModTime()
		lastUpdate = &modifiedAt
	}
	if lastUpdate != nil && time.Since(*lastUpdate) < time.Duration(settings.UpdateIntervalHours)*time.Hour {
		return
	}
	if err := g.DownloadAndUpdateDB(ctx); err != nil && !errors.Is(err, ErrGeoIPUpdateInProgress) && !errors.Is(err, context.Canceled) {
		g.recordUpdateError(err)
		if g.logger != nil {
			g.logger.Error("scheduled geoip database update failed", "error", err)
		}
	}
}

func (g *GeoIPService) recordUpdateError(err error) {
	if err == nil {
		return
	}
	g.configMu.Lock()
	g.lastError = err.Error()
	g.configMu.Unlock()
}

// DownloadAndUpdateDB downloads, validates, atomically replaces, and activates
// the configured managed MaxMind database.
func (g *GeoIPService) DownloadAndUpdateDB(ctx context.Context) (updateErr error) {
	var generation uint64
	select {
	case g.updateSlot <- struct{}{}:
		defer func() {
			<-g.updateSlot
			g.configMu.RLock()
			settingsChanged := g.settingsGeneration != generation
			g.configMu.RUnlock()
			if settingsChanged {
				select {
				case g.updateWake <- struct{}{}:
				default:
				}
			}
		}()
	default:
		return ErrGeoIPUpdateInProgress
	}

	g.configMu.Lock()
	g.updating = true
	g.lastError = ""
	settings := g.settings
	generation = g.settingsGeneration
	g.configMu.Unlock()
	defer func() {
		g.configMu.Lock()
		g.updating = false
		if updateErr != nil && g.settingsGeneration == generation {
			g.lastError = updateErr.Error()
		}
		g.configMu.Unlock()
	}()

	if settings.Provider != models.GeoIPProviderMaxMind {
		return fmt.Errorf("managed GeoIP updates require the maxmind provider")
	}
	if err := g.ValidateSettings(settings); err != nil {
		return err
	}
	downloadURL, err := maxMindDownloadURL(settings)
	if err != nil {
		return err
	}

	downloadCtx, cancel := context.WithTimeout(ctx, geoDownloadTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(downloadCtx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return fmt.Errorf("build geoip download request for %s: %s", redactURLForLog(downloadURL), redactURLInError(downloadURL, err))
	}
	if g.logger != nil {
		g.logger.Info("downloading managed geoip database", "source", redactURLForLog(downloadURL), "timeout", geoDownloadTimeout.String())
	}
	resp, err := g.downloadClient.Do(req)
	if err != nil {
		return fmt.Errorf("download geoip database from %s: %s", redactURLForLog(downloadURL), redactURLInError(downloadURL, err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("geoip download returned HTTP %d", resp.StatusCode)
	}

	targetPath := settings.MaxMindDBPath
	targetDir := filepath.Dir(targetPath)
	if err := os.MkdirAll(targetDir, 0o750); err != nil {
		return fmt.Errorf("create geoip directory: %w", err)
	}
	downloadFile, err := os.CreateTemp(targetDir, ".geoip-download-*")
	if err != nil {
		return fmt.Errorf("create geoip download file: %w", err)
	}
	downloadPath := downloadFile.Name()
	defer os.Remove(downloadPath)
	if err := copyWithLimit(downloadFile, resp.Body, maxGeoDownloadBytes); err != nil {
		_ = downloadFile.Close()
		return fmt.Errorf("save geoip download: %w", err)
	}
	if err := downloadFile.Sync(); err != nil {
		_ = downloadFile.Close()
		return fmt.Errorf("sync geoip download: %w", err)
	}
	if _, err := downloadFile.Seek(0, io.SeekStart); err != nil {
		_ = downloadFile.Close()
		return fmt.Errorf("rewind geoip download: %w", err)
	}

	databaseFile, err := os.CreateTemp(targetDir, ".geoip-database-*")
	if err != nil {
		_ = downloadFile.Close()
		return fmt.Errorf("create geoip database file: %w", err)
	}
	databasePath := databaseFile.Name()
	defer os.Remove(databasePath)
	if err := extractGeoIPDatabase(downloadFile, databaseFile); err != nil {
		_ = downloadFile.Close()
		_ = databaseFile.Close()
		return fmt.Errorf("extract geoip database: %w", err)
	}
	_ = downloadFile.Close()
	if err := databaseFile.Chmod(0o640); err != nil {
		_ = databaseFile.Close()
		return fmt.Errorf("set geoip database permissions: %w", err)
	}
	if err := databaseFile.Sync(); err != nil {
		_ = databaseFile.Close()
		return fmt.Errorf("sync geoip database: %w", err)
	}
	if err := databaseFile.Close(); err != nil {
		return fmt.Errorf("close geoip database: %w", err)
	}

	databaseBytes, err := os.ReadFile(databasePath)
	if err != nil {
		return fmt.Errorf("read extracted geoip database: %w", err)
	}
	opened, err := g.openDatabase(databaseBytes)
	if err != nil {
		return fmt.Errorf("downloaded geoip database is invalid: %w", err)
	}
	g.configMu.Lock()
	if g.settingsGeneration != generation || g.settings.Provider != models.GeoIPProviderMaxMind || g.settings.MaxMindDBPath != targetPath {
		g.configMu.Unlock()
		_ = opened.reader.Close()
		return fmt.Errorf("geoip settings changed during database update")
	}
	if err := os.Rename(databasePath, targetPath); err != nil {
		g.configMu.Unlock()
		_ = opened.reader.Close()
		return fmt.Errorf("replace geoip database: %w", err)
	}
	g.swapDatabase(opened, targetPath, "managed")

	now := time.Now().UTC()
	g.settings.LastUpdatedAt = &now
	g.lastError = ""
	g.configMu.Unlock()
	if g.settingsStore != nil {
		if err := g.settingsStore.SetGeoIPLastUpdated(ctx, now); err != nil && g.logger != nil {
			g.logger.Warn("geoip database updated but timestamp persistence failed", "error", err)
		}
	}
	if g.logger != nil {
		g.logger.Info("managed geoip database updated", "path", targetPath, "database_type", opened.databaseType, "updated_at", now)
	}
	return nil
}

func maxMindDownloadURL(settings models.GeoIPSettings) (string, error) {
	if settings.MaxMindURL != "" {
		if err := validateGeoIPDownloadURL(settings.MaxMindURL); err != nil {
			return "", err
		}
		return settings.MaxMindURL, nil
	}
	if settings.MaxMindLicenseKey == "" {
		return "", fmt.Errorf("a MaxMind license key or explicit HTTPS download URL is required")
	}
	values := url.Values{
		"edition_id":  {"GeoLite2-City"},
		"license_key": {settings.MaxMindLicenseKey},
		"suffix":      {"tar.gz"},
	}
	return "https://download.maxmind.com/app/geoip_download?" + values.Encode(), nil
}

func copyWithLimit(dst io.Writer, src io.Reader, limit int64) error {
	written, err := io.Copy(dst, io.LimitReader(src, limit+1))
	if err != nil {
		return err
	}
	if written == 0 {
		return fmt.Errorf("download is empty")
	}
	if written > limit {
		return fmt.Errorf("download exceeds %d byte limit", limit)
	}
	return nil
}

func extractGeoIPDatabase(download io.ReadSeeker, destination io.Writer) error {
	return extractGeoIPDatabaseWithLimit(download, destination, maxGeoDatabaseBytes)
}

func extractGeoIPDatabaseWithLimit(download io.ReadSeeker, destination io.Writer, limit int64) error {
	if _, err := download.Seek(0, io.SeekStart); err != nil {
		return err
	}
	buffered := bufio.NewReader(download)
	header, _ := buffered.Peek(2)
	if len(header) != 2 || header[0] != 0x1f || header[1] != 0x8b {
		return copyWithLimit(destination, buffered, limit)
	}

	gzipReader, err := gzip.NewReader(buffered)
	if err != nil {
		return err
	}
	defer gzipReader.Close()
	decompressed := bufio.NewReader(io.LimitReader(gzipReader, limit+1))
	tarHeader, _ := decompressed.Peek(512)
	if !looksLikeTar(tarHeader) {
		return copyWithLimit(destination, decompressed, limit)
	}

	tarReader := tar.NewReader(decompressed)
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("archive does not contain an .mmdb database")
		}
		if err != nil {
			return err
		}
		if header.Typeflag == tar.TypeReg && strings.EqualFold(filepath.Ext(header.Name), ".mmdb") {
			return copyWithLimit(destination, tarReader, limit)
		}
	}
}

func looksLikeTar(header []byte) bool {
	return len(header) >= 262 && string(header[257:262]) == "ustar"
}

func (g *GeoIPService) sweepCache(now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for key, entry := range g.cache {
		if now.Sub(entry.cachedAt) >= g.cacheTTL {
			delete(g.cache, key)
		}
	}
	g.enforceCacheCapLocked()
}

func (g *GeoIPService) cacheResults(results map[string]models.GeoInfo, now time.Time) {
	g.mu.Lock()
	for host, geo := range results {
		g.cache[host] = cacheEntry{geo: geo, cachedAt: now}
	}
	g.enforceCacheCapLocked()
	g.mu.Unlock()
}

func (g *GeoIPService) enforceCacheCapLocked() {
	limit := g.cacheMax
	if limit <= 0 {
		limit = defaultGeoCacheMax
	}
	for len(g.cache) > limit {
		var oldestKey string
		var oldest time.Time
		for key, entry := range g.cache {
			if oldestKey == "" || entry.cachedAt.Before(oldest) {
				oldestKey = key
				oldest = entry.cachedAt
			}
		}
		delete(g.cache, oldestKey)
	}
}

// extractIP parses "host:port" and returns just the host IP.
func extractIP(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		// maybe no port
		return strings.TrimSpace(address)
	}
	return strings.TrimSpace(host)
}

// LookupOne returns GeoInfo for a single proxy address ("host:port" or bare IP).
func (g *GeoIPService) LookupOne(ctx context.Context, address string) (*models.GeoInfo, error) {
	ip := extractIP(address)
	if ip == "" {
		return nil, fmt.Errorf("empty address")
	}

	// Check cache first
	g.mu.RLock()
	if entry, ok := g.cache[ip]; ok && time.Since(entry.cachedAt) < g.cacheTTL {
		g.mu.RUnlock()
		geo := entry.geo
		return &geo, nil
	}
	g.mu.RUnlock()

	results, err := g.lookupBatch(ctx, []string{ip})
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no result for %s", ip)
	}
	return &results[0], nil
}

// LookupBatch resolves GeoInfo for up to 100 addresses at once.
// Returns map[address] -> GeoInfo.
func (g *GeoIPService) LookupBatch(ctx context.Context, addresses []string) map[string]models.GeoInfo {
	result := make(map[string]models.GeoInfo)

	// deduplicate & separate cached vs needed
	ipToAddresses := make(map[string][]string)
	neededSet := make(map[string]struct{})
	var needed []string

	g.mu.RLock()
	for _, addr := range addresses {
		ip := extractIP(addr)
		if ip == "" {
			continue
		}
		ipToAddresses[ip] = append(ipToAddresses[ip], addr)
		if entry, ok := g.cache[ip]; ok && time.Since(entry.cachedAt) < g.cacheTTL {
			result[addr] = entry.geo
		} else if _, exists := neededSet[ip]; !exists {
			neededSet[ip] = struct{}{}
			needed = append(needed, ip)
		}
	}
	g.mu.RUnlock()

	if len(needed) == 0 {
		return result
	}

	// Remote batch providers supported by this API shape accept at most 100 IPs.
	const batchSize = 100
	for i := 0; i < len(needed); i += batchSize {
		end := i + batchSize
		if end > len(needed) {
			end = len(needed)
		}
		batch := needed[i:end]

		rawGeos, err := g.lookupBatchRaw(ctx, batch)
		if err != nil {
			g.logger.Warn("geoip batch lookup failed", "error", err)
			continue
		}
		for ip, geo := range rawGeos {
			for _, addr := range ipToAddresses[ip] {
				result[addr] = geo
			}
		}
	}

	return result
}

// lookupBatch fetches geo data for a slice of IPs (max 100)
func (g *GeoIPService) lookupBatch(ctx context.Context, ips []string) ([]models.GeoInfo, error) {
	raw, err := g.lookupBatchRaw(ctx, ips)
	if err != nil {
		return nil, err
	}
	var out []models.GeoInfo
	for _, v := range raw {
		out = append(out, v)
	}
	return out, nil
}

// lookupBatchRaw resolves geo data and returns map[ip] -> GeoInfo.
func (g *GeoIPService) lookupBatchRaw(ctx context.Context, ips []string) (map[string]models.GeoInfo, error) {
	if len(ips) == 0 {
		return nil, nil
	}
	if local, available := g.lookupLocal(ctx, ips); available {
		return local, nil
	}
	if g.batchURL == "" {
		return nil, fmt.Errorf("geoip provider is not configured")
	}
	return g.lookupRemoteBatch(ctx, ips)
}

func (g *GeoIPService) lookupLocal(ctx context.Context, hosts []string) (map[string]models.GeoInfo, bool) {
	result := make(map[string]models.GeoInfo, len(hosts))
	now := time.Now()
	resolved := make(map[string]net.IP, len(hosts))
	g.readerMu.RLock()
	available := g.geoDB != nil
	g.readerMu.RUnlock()
	if !available {
		return nil, false
	}

	for _, host := range hosts {
		ip, err := resolveLookupIP(ctx, host)
		if err != nil {
			g.logger.Debug("failed to resolve proxy host for geoip", "host", host, "error", err)
			continue
		}
		resolved[host] = ip
	}

	g.readerMu.RLock()
	defer g.readerMu.RUnlock()
	if g.geoDB == nil {
		return nil, false
	}
	for host, ip := range resolved {
		var record mmdbCountryRecord
		if err := g.geoDB.Lookup(ip, &record); err != nil {
			g.logger.Debug("local geoip lookup failed", "host", host, "error", err)
			continue
		}
		geo, ok := record.geoInfo()
		if !ok {
			continue
		}
		result[host] = geo
	}

	g.cacheResults(result, now)
	return result, true
}

func resolveLookupIP(ctx context.Context, host string) (net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		return ip, nil
	}

	resolved, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(resolved) == 0 {
		return nil, fmt.Errorf("no IP address found for %s", host)
	}
	return resolved[0], nil
}

func (g *GeoIPService) lookupRemoteBatch(ctx context.Context, ips []string) (map[string]models.GeoInfo, error) {
	if err := g.acquireRemoteSlot(ctx); err != nil {
		return nil, err
	}
	defer g.releaseRemoteSlot()

	// A concurrent caller may have populated these entries while this call was
	// waiting for the serialized remote slot.
	result := make(map[string]models.GeoInfo, len(ips))
	needed := make([]string, 0, len(ips))
	now := time.Now()
	g.mu.RLock()
	for _, host := range ips {
		if entry, ok := g.cache[host]; ok && now.Sub(entry.cachedAt) < g.cacheTTL {
			result[host] = entry.geo
		} else {
			needed = append(needed, host)
		}
	}
	g.mu.RUnlock()
	if len(needed) == 0 {
		return result, nil
	}

	// Build JSON body: [{"query":"1.2.3.4","fields":"..."}, ...]
	type reqItem struct {
		Query  string `json:"query"`
		Fields string `json:"fields"`
	}
	items := make([]reqItem, len(needed))
	fields := "status,country,countryCode,regionName,city,isp,lat,lon,query"
	for i, ip := range needed {
		items[i] = reqItem{Query: ip, Fields: fields}
	}

	body, err := json.Marshal(items)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal geoip request: %w", err)
	}

	responses, err := g.doRemoteBatch(ctx, body)
	if err != nil {
		return result, err
	}

	fetched := make(map[string]models.GeoInfo, len(responses))
	for _, r := range responses {
		if r.Status != "success" {
			continue
		}
		geo := models.GeoInfo{
			CountryCode: r.CountryCode,
			CountryName: r.Country,
			RegionName:  r.Region,
			CityName:    r.City,
			ISP:         r.ISP,
			Latitude:    r.Lat,
			Longitude:   r.Lon,
		}
		result[r.Query] = geo
		fetched[r.Query] = geo
	}
	g.cacheResults(fetched, time.Now())
	return result, nil
}

func (g *GeoIPService) acquireRemoteSlot(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case g.remoteSlot <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *GeoIPService) releaseRemoteSlot() {
	<-g.remoteSlot
}

func (g *GeoIPService) doRemoteBatch(ctx context.Context, body []byte) ([]ipAPIResponse, error) {
	const maxAttempts = 3
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := g.waitForRemoteSlot(ctx); err != nil {
			return nil, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.batchURL, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("build geoip request for %s: %s", redactURLForLog(g.batchURL), redactURLInError(g.batchURL, err))
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := g.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("geoip request to %s: %s", redactURLForLog(g.batchURL), redactURLInError(g.batchURL, err))
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now(), 2*g.effectiveRemoteGap())
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("geoip api returned HTTP 429")
			if g.logger != nil {
				g.logger.Warn("geoip remote provider rate limited", "wait", wait.String())
			}
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
				continue
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("geoip api returned HTTP %d", resp.StatusCode)
		}

		encoded, readErr := io.ReadAll(io.LimitReader(resp.Body, maxGeoResponseBytes+1))
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read geoip response: %w", readErr)
		}
		if len(encoded) > maxGeoResponseBytes {
			return nil, fmt.Errorf("geoip response exceeds %d byte limit", maxGeoResponseBytes)
		}
		var responses []ipAPIResponse
		if err := json.Unmarshal(encoded, &responses); err != nil {
			return nil, fmt.Errorf("decode geoip response: %w", err)
		}
		return responses, nil
	}
	return nil, lastErr
}

func (g *GeoIPService) effectiveRemoteGap() time.Duration {
	if g.remoteGap < 0 {
		return 0
	}
	return g.remoteGap
}

func (g *GeoIPService) waitForRemoteSlot(ctx context.Context) error {
	gap := g.effectiveRemoteGap()
	if wait := gap - time.Since(g.lastRemote); !g.lastRemote.IsZero() && wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		}
	}
	g.lastRemote = time.Now()
	return nil
}

func parseRetryAfter(value string, now time.Time, fallback time.Duration) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if retryAt, err := http.ParseTime(strings.TrimSpace(value)); err == nil && retryAt.After(now) {
		return retryAt.Sub(now)
	}
	return fallback
}

// EnrichProxies resolves all addresses and returns map[address] -> GeoInfo.
func (g *GeoIPService) EnrichProxies(ctx context.Context, addresses []string) map[string]models.GeoInfo {
	return g.LookupBatch(ctx, addresses)
}
