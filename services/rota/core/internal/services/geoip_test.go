package services

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/pkg/logger"
)

type geoReaderStub struct {
	lookupStarted chan struct{}
	lookupRelease chan struct{}
	startedOnce   sync.Once
	closed        atomic.Bool
}

func (r *geoReaderStub) Lookup(_ net.IP, result any) error {
	if r.lookupStarted != nil {
		r.startedOnce.Do(func() { close(r.lookupStarted) })
	}
	if r.lookupRelease != nil {
		<-r.lookupRelease
	}
	if record, ok := result.(*mmdbCountryRecord); ok {
		record.Country = mmdbCountry{ISOCode: "US", Names: map[string]string{"en": "United States"}}
	}
	return nil
}

func (r *geoReaderStub) Close() error {
	r.closed.Store(true)
	return nil
}

type geoSettingsStoreStub struct {
	settings  models.Settings
	updatedAt time.Time
}

func (s *geoSettingsStoreStub) GetAll(context.Context) (*models.Settings, error) {
	settings := s.settings
	return &settings, nil
}

func (s *geoSettingsStoreStub) SetGeoIPLastUpdated(_ context.Context, updatedAt time.Time) error {
	s.updatedAt = updatedAt
	return nil
}

func TestExtractIP(t *testing.T) {
	tests := map[string]string{
		"1.2.3.4:8080":       "1.2.3.4",
		"[2001:db8::1]:1080": "2001:db8::1",
		"proxy.example.com":  "proxy.example.com",
		" 1.2.3.4 ":          "1.2.3.4",
	}

	for address, want := range tests {
		if got := extractIP(address); got != want {
			t.Errorf("extractIP(%q) = %q, want %q", address, got, want)
		}
	}
}

func TestMMDBCountryRecordFallsBackToRegisteredCountry(t *testing.T) {
	record := mmdbCountryRecord{
		RegisteredCountry: mmdbCountry{
			ISOCode: "DE",
			Names:   map[string]string{"en": "Germany"},
		},
	}

	geo, ok := record.geoInfo()
	if !ok {
		t.Fatal("expected country data")
	}
	if geo.CountryCode != "DE" || geo.CountryName != "Germany" {
		t.Fatalf("unexpected geo data: %+v", geo)
	}
}

func TestLookupBatchFetchesEachIPOnceAndCachesResult(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)

		var items []struct {
			Query string `json:"query"`
		}
		if err := json.NewDecoder(r.Body).Decode(&items); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		response := make([]ipAPIResponse, 0, len(items))
		for _, item := range items {
			response = append(response, ipAPIResponse{
				Status:      "success",
				Country:     "United States",
				CountryCode: "US",
				Query:       item.Query,
			})
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	service := NewGeoIPService(logger.New("error"))
	service.batchURL = server.URL

	addresses := []string{"1.2.3.4:8000", "1.2.3.4:8080", "5.6.7.8:9000"}
	got := service.LookupBatch(context.Background(), addresses)
	if len(got) != 3 {
		t.Fatalf("got %d geo results, want 3", len(got))
	}
	if got["1.2.3.4:8000"].CountryCode != "US" {
		t.Fatalf("unexpected geo result: %+v", got["1.2.3.4:8000"])
	}
	if got["1.2.3.4:8080"].CountryCode != "US" {
		t.Fatalf("same IP with another port was not mapped: %+v", got["1.2.3.4:8080"])
	}
	if requests.Load() != 1 {
		t.Fatalf("requests = %d, want 1", requests.Load())
	}

	service.LookupBatch(context.Background(), addresses)
	if requests.Load() != 1 {
		t.Fatalf("cached lookup made another request; requests = %d", requests.Load())
	}
}

func TestConcurrentRemoteLookupsRecheckCacheInsideSerializedSlot(t *testing.T) {
	var requests atomic.Int32
	firstStarted := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			close(firstStarted)
			<-release
		}
		_ = json.NewEncoder(w).Encode([]ipAPIResponse{{
			Status: "success", CountryCode: "US", Query: "1.2.3.4",
		}})
	}))
	defer server.Close()

	service := NewGeoIPService(logger.New("error"))
	service.batchURL = server.URL
	service.remoteGap = 0
	var wg sync.WaitGroup
	results := make([]map[string]models.GeoInfo, 2)
	for index := range results {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			results[index] = service.LookupBatch(context.Background(), []string{"1.2.3.4:8000"})
		}(index)
	}
	<-firstStarted
	close(release)
	wg.Wait()

	if got := requests.Load(); got != 1 {
		t.Fatalf("remote requests = %d, want 1", got)
	}
	for index, result := range results {
		if result["1.2.3.4:8000"].CountryCode != "US" {
			t.Fatalf("result %d = %#v", index, result)
		}
	}
}

func TestRemoteSlotWaitHonorsCancellation(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		_ = json.NewEncoder(w).Encode([]ipAPIResponse{{
			Status: "success", CountryCode: "US", Query: "1.2.3.4",
		}})
	}))
	defer server.Close()

	service := NewGeoIPService(logger.New("error"))
	service.batchURL = server.URL
	service.remoteGap = 0
	firstDone := make(chan struct{})
	go func() {
		service.LookupBatch(context.Background(), []string{"1.2.3.4:8000"})
		close(firstDone)
	}()
	<-firstStarted

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	startedAt := time.Now()
	result := service.LookupBatch(ctx, []string{"5.6.7.8:8000"})
	if len(result) != 0 {
		t.Fatalf("cancelled lookup returned %#v", result)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("remote slot cancellation took %s", elapsed)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests = %d, cancelled waiter reached provider", got)
	}
	close(releaseFirst)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first lookup did not finish")
	}
}

func TestGeoIPRateLimitBackoffHonorsCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	service := NewGeoIPService(logger.New("error"))
	service.batchURL = server.URL
	service.remoteGap = 0
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	startedAt := time.Now()
	result := service.LookupBatch(ctx, []string{"1.2.3.4:8000"})
	if len(result) != 0 {
		t.Fatalf("rate-limited lookup returned %#v", result)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("cancellation took %s", elapsed)
	}
}

func TestGeoIPCacheEnforcesTTLAndHardCap(t *testing.T) {
	service := NewGeoIPService(logger.New("error"))
	service.cacheMax = 2
	service.cacheTTL = time.Hour
	now := time.Now()
	service.cacheResults(map[string]models.GeoInfo{"old": {CountryCode: "AA"}}, now.Add(-3*time.Minute))
	service.cacheResults(map[string]models.GeoInfo{"middle": {CountryCode: "BB"}}, now.Add(-2*time.Minute))
	service.cacheResults(map[string]models.GeoInfo{"new": {CountryCode: "CC"}}, now.Add(-time.Minute))

	service.mu.RLock()
	_, retainedOldest := service.cache["old"]
	cacheSize := len(service.cache)
	service.mu.RUnlock()
	if retainedOldest || cacheSize != 2 {
		t.Fatalf("cache size = %d, retained oldest = %v", cacheSize, retainedOldest)
	}

	service.cacheResults(map[string]models.GeoInfo{"expired": {CountryCode: "DD"}}, now.Add(-2*time.Hour))
	service.sweepCache(now)
	service.mu.RLock()
	_, retainedExpired := service.cache["expired"]
	service.mu.RUnlock()
	if retainedExpired {
		t.Fatal("expired cache entry was retained")
	}
}

func TestParseRetryAfterSupportsSecondsAndHTTPDate(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	if got := parseRetryAfter("7", now, time.Second); got != 7*time.Second {
		t.Fatalf("delta retry = %s", got)
	}
	retryAt := now.Add(12 * time.Second)
	if got := parseRetryAfter(retryAt.Format(http.TimeFormat), now, time.Second); got != 12*time.Second {
		t.Fatalf("date retry = %s", got)
	}
}

func TestGeoIPRunStopsWithOwnerAndCloseIsIdempotent(t *testing.T) {
	service := NewGeoIPService(logger.New("error"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		service.Run(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("GeoIP worker did not stop")
	}
	if err := service.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := service.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestGeoIPSettingsRequireManagedHTTPSPath(t *testing.T) {
	managedDir := t.TempDir()
	service := NewGeoIPService(logger.New("error"))
	service.managedDir = managedDir

	valid := models.GeoIPSettings{
		Provider:            models.GeoIPProviderMaxMind,
		MaxMindDBPath:       filepath.Join(managedDir, "GeoLite2-City.mmdb"),
		MaxMindURL:          "https://downloads.example.test/GeoLite2-City.mmdb",
		UpdateIntervalHours: 168,
	}
	if err := service.ValidateSettings(valid); err != nil {
		t.Fatalf("valid settings: %v", err)
	}

	outside := valid
	outside.MaxMindDBPath = filepath.Join(filepath.Dir(managedDir), "outside.mmdb")
	if err := service.ValidateSettings(outside); err == nil || !strings.Contains(err.Error(), "must be inside") {
		t.Fatalf("outside path error = %v", err)
	}

	insecure := valid
	insecure.MaxMindURL = "http://downloads.example.test/GeoLite2-City.mmdb"
	if err := service.ValidateSettings(insecure); err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("insecure URL error = %v", err)
	}
}

func TestMaxMindDownloadURLSecretsAreRedacted(t *testing.T) {
	downloadURL, err := maxMindDownloadURL(models.GeoIPSettings{MaxMindLicenseKey: "super-secret-key"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(downloadURL, "super-secret-key") {
		t.Fatal("official request URL is missing the license key")
	}
	if redacted := redactURLForLog(downloadURL); strings.Contains(redacted, "super-secret-key") || strings.Contains(redacted, "license_key") {
		t.Fatalf("redacted URL leaked credentials: %q", redacted)
	}
	errorText := redactURLInError(downloadURL, errors.New("request failed for "+downloadURL))
	if strings.Contains(errorText, "super-secret-key") {
		t.Fatalf("redacted error leaked credentials: %q", errorText)
	}
}

func TestExtractGeoIPDatabaseSupportsRawGzipAndTarGzip(t *testing.T) {
	raw := []byte("test-mmdb-payload")
	var gzipPayload bytes.Buffer
	gzipWriter := gzip.NewWriter(&gzipPayload)
	if _, err := gzipWriter.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}

	var tarGzipPayload bytes.Buffer
	tarGzipWriter := gzip.NewWriter(&tarGzipPayload)
	tarWriter := tar.NewWriter(tarGzipWriter)
	if err := tarWriter.WriteHeader(&tar.Header{Name: "GeoLite2-City/GeoLite2-City.mmdb", Mode: 0o600, Size: int64(len(raw))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tarGzipWriter.Close(); err != nil {
		t.Fatal(err)
	}

	for name, payload := range map[string][]byte{
		"raw":      raw,
		"gzip":     gzipPayload.Bytes(),
		"tar-gzip": tarGzipPayload.Bytes(),
	} {
		t.Run(name, func(t *testing.T) {
			source, err := os.CreateTemp(t.TempDir(), "source-*")
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			if _, err := source.Write(payload); err != nil {
				t.Fatal(err)
			}
			var destination bytes.Buffer
			if err := extractGeoIPDatabase(source, &destination); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(destination.Bytes(), raw) {
				t.Fatalf("extracted %q, want %q", destination.Bytes(), raw)
			}
		})
	}
}

func TestExtractGeoIPDatabaseBoundsExpandedTarScan(t *testing.T) {
	var payload bytes.Buffer
	gzipWriter := gzip.NewWriter(&payload)
	tarWriter := tar.NewWriter(gzipWriter)
	junk := bytes.Repeat([]byte("x"), 1024)
	if err := tarWriter.WriteHeader(&tar.Header{Name: "padding.bin", Mode: 0o600, Size: int64(len(junk))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write(junk); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.WriteHeader(&tar.Header{Name: "GeoLite2-City.mmdb", Mode: 0o600, Size: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write([]byte("mmdb")); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}

	source, err := os.CreateTemp(t.TempDir(), "source-*")
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Write(payload.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := extractGeoIPDatabaseWithLimit(source, io.Discard, 700); err == nil {
		t.Fatal("oversized expanded archive was accepted")
	}
}

func TestCopyWithLimitRejectsEmptyAndOversizedDownloads(t *testing.T) {
	if err := copyWithLimit(io.Discard, strings.NewReader(""), 3); err == nil {
		t.Fatal("empty download was accepted")
	}
	if err := copyWithLimit(io.Discard, strings.NewReader("four"), 3); err == nil {
		t.Fatal("oversized download was accepted")
	}
	if err := copyWithLimit(io.Discard, strings.NewReader("ok"), 3); err != nil {
		t.Fatalf("bounded download: %v", err)
	}
}

func TestDatabaseSwapWaitsForLookupBeforeClosingOldReader(t *testing.T) {
	oldReader := &geoReaderStub{lookupStarted: make(chan struct{}), lookupRelease: make(chan struct{})}
	newReader := &geoReaderStub{}
	service := NewGeoIPService(logger.New("error"))
	service.geoDB = oldReader

	lookupDone := make(chan struct{})
	go func() {
		service.lookupLocal(context.Background(), []string{"1.2.3.4"})
		close(lookupDone)
	}()
	<-oldReader.lookupStarted

	swapDone := make(chan struct{})
	go func() {
		service.swapDatabase(&openedGeoDatabase{reader: newReader, databaseType: "GeoLite2-City"}, "/managed/new.mmdb", "managed")
		close(swapDone)
	}()
	select {
	case <-swapDone:
		t.Fatal("database swapped while a lookup was still using the old reader")
	case <-time.After(20 * time.Millisecond):
	}
	if oldReader.closed.Load() {
		t.Fatal("old reader closed during an active lookup")
	}

	close(oldReader.lookupRelease)
	select {
	case <-lookupDone:
	case <-time.After(time.Second):
		t.Fatal("lookup did not finish")
	}
	select {
	case <-swapDone:
	case <-time.After(time.Second):
		t.Fatal("database swap did not finish")
	}
	if !oldReader.closed.Load() {
		t.Fatal("old reader was not closed after the lookup drained")
	}
}

func TestInvalidDownloadKeepsLastKnownGoodDatabase(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "not-a-valid-mmdb")
	}))
	defer server.Close()

	managedDir := t.TempDir()
	targetPath := filepath.Join(managedDir, "GeoLite2-City.mmdb")
	oldContents := []byte("last-known-good")
	if err := os.WriteFile(targetPath, oldContents, 0o640); err != nil {
		t.Fatal(err)
	}
	oldReader := &geoReaderStub{}
	service := NewGeoIPService(logger.New("error"))
	service.managedDir = managedDir
	service.downloadClient = server.Client()
	service.geoDB = oldReader
	service.settings = models.GeoIPSettings{
		Provider:            models.GeoIPProviderMaxMind,
		MaxMindDBPath:       targetPath,
		MaxMindURL:          server.URL,
		UpdateIntervalHours: 168,
	}
	service.openDatabase = func([]byte) (*openedGeoDatabase, error) {
		return nil, errors.New("invalid database")
	}

	if err := service.DownloadAndUpdateDB(context.Background()); err == nil {
		t.Fatal("invalid download was accepted")
	}
	contents, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(contents, oldContents) {
		t.Fatalf("working database changed to %q", contents)
	}
	if oldReader.closed.Load() || service.geoDB != oldReader {
		t.Fatal("last known good reader was replaced or closed")
	}
	if status := service.Status(); status.LastError == "" || status.Updating {
		t.Fatalf("unexpected status after failure: %+v", status)
	}
}

func TestSuccessfulDownloadAtomicallyActivatesAndPersistsTimestamp(t *testing.T) {
	payload := []byte("valid-test-mmdb")
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	managedDir := t.TempDir()
	targetPath := filepath.Join(managedDir, "GeoLite2-City.mmdb")
	oldReader := &geoReaderStub{}
	newReader := &geoReaderStub{}
	store := &geoSettingsStoreStub{}
	service := NewGeoIPService(logger.New("error"))
	service.managedDir = managedDir
	service.settingsStore = store
	service.downloadClient = server.Client()
	service.geoDB = oldReader
	service.settings = models.GeoIPSettings{
		Provider:            models.GeoIPProviderMaxMind,
		MaxMindDBPath:       targetPath,
		MaxMindURL:          server.URL,
		UpdateIntervalHours: 168,
	}
	service.openDatabase = func(database []byte) (*openedGeoDatabase, error) {
		if !bytes.Equal(database, payload) {
			t.Fatalf("validated payload %q", database)
		}
		return &openedGeoDatabase{reader: newReader, databaseType: "GeoLite2-City"}, nil
	}

	if err := service.DownloadAndUpdateDB(context.Background()); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(contents, payload) {
		t.Fatalf("database contents %q", contents)
	}
	if !oldReader.closed.Load() || service.geoDB != newReader {
		t.Fatal("new database was not activated cleanly")
	}
	if store.updatedAt.IsZero() || service.Status().LastUpdatedAt == nil {
		t.Fatal("successful update timestamp was not persisted")
	}
}

func TestConcurrentGeoIPUpdateIsRejected(t *testing.T) {
	requestStarted := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(requestStarted)
		<-release
		_, _ = io.WriteString(w, "payload")
	}))
	defer server.Close()

	managedDir := t.TempDir()
	service := NewGeoIPService(logger.New("error"))
	service.managedDir = managedDir
	service.downloadClient = server.Client()
	service.settings = models.GeoIPSettings{
		Provider:            models.GeoIPProviderMaxMind,
		MaxMindDBPath:       filepath.Join(managedDir, "GeoLite2-City.mmdb"),
		MaxMindURL:          server.URL,
		UpdateIntervalHours: 168,
	}
	service.openDatabase = func([]byte) (*openedGeoDatabase, error) {
		return &openedGeoDatabase{reader: &geoReaderStub{}, databaseType: "GeoLite2-City"}, nil
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- service.DownloadAndUpdateDB(context.Background()) }()
	<-requestStarted
	if err := service.DownloadAndUpdateDB(context.Background()); !errors.Is(err, ErrGeoIPUpdateInProgress) {
		t.Fatalf("concurrent update error = %v", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestSettingsChangeRejectsInFlightGeoIPDownload(t *testing.T) {
	requestStarted := make(chan struct{})
	release := make(chan struct{})
	payload := []byte("valid-test-mmdb")
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(requestStarted)
		<-release
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	managedDir := t.TempDir()
	targetPath := filepath.Join(managedDir, "GeoLite2-City.mmdb")
	oldReader := &geoReaderStub{}
	candidateReader := &geoReaderStub{}
	store := &geoSettingsStoreStub{}
	service := NewGeoIPService(logger.New("error"))
	service.managedDir = managedDir
	service.settingsStore = store
	service.downloadClient = server.Client()
	service.geoDB = oldReader
	service.settings = models.GeoIPSettings{
		Provider:            models.GeoIPProviderMaxMind,
		MaxMindDBPath:       targetPath,
		MaxMindURL:          server.URL,
		UpdateIntervalHours: 168,
	}
	service.openDatabase = func(database []byte) (*openedGeoDatabase, error) {
		if !bytes.Equal(database, payload) {
			t.Fatalf("validated payload %q", database)
		}
		return &openedGeoDatabase{reader: candidateReader, databaseType: "GeoLite2-City"}, nil
	}

	downloadDone := make(chan error, 1)
	go func() { downloadDone <- service.DownloadAndUpdateDB(context.Background()) }()
	<-requestStarted

	store.settings.GeoIP = models.GeoIPSettings{Provider: models.GeoIPProviderLocal}
	if err := service.ReloadSettings(context.Background()); err != nil {
		t.Fatalf("reload settings: %v", err)
	}
	select {
	case <-service.updateWake:
	default:
		t.Fatal("settings reload did not signal the update worker")
	}
	close(release)

	err := <-downloadDone
	if err == nil || !strings.Contains(err.Error(), "settings changed") {
		t.Fatalf("in-flight update error = %v", err)
	}
	if _, err := os.Stat(targetPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale download was written to %s: %v", targetPath, err)
	}
	if !candidateReader.closed.Load() {
		t.Fatal("rejected download reader was not closed")
	}
	if oldReader.closed.Load() || service.geoDB != oldReader {
		t.Fatal("stale download replaced or closed the active reader")
	}
	if status := service.Status(); status.Provider != models.GeoIPProviderLocal || status.Updating || status.LastError != "" {
		t.Fatalf("unexpected status after settings change: %+v", status)
	}
	select {
	case <-service.updateWake:
	default:
		t.Fatal("settings change was not re-signalled after the old update released its slot")
	}
}
