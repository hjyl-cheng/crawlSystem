package models

import "time"

const (
	YouTubeSearchURLPrefix       = "https://www.youtube.com/results?search_query="
	MaxHealthCheckTimeoutSeconds = 15
)

// Settings represents system configuration
type Settings struct {
	Authentication AuthenticationSettings `json:"authentication"`
	Rotation       RotationSettings       `json:"rotation"`
	RateLimit      RateLimitSettings      `json:"rate_limit"`
	HealthCheck    HealthCheckSettings    `json:"healthcheck"`
	LogRetention   LogRetentionSettings   `json:"log_retention"`
	ProxyCleanup   ProxyCleanupSettings   `json:"proxy_cleanup"`
	ProxyLifecycle ProxyLifecycleSettings `json:"proxy_lifecycle"`
	GeoIP          GeoIPSettings          `json:"geoip"`
}

// AuthenticationSettings represents proxy server authentication configuration
// This controls authentication for incoming requests to the PROXY server (port 8000)
// NOT for dashboard/API login (which uses ROTA_ADMIN_USER/ROTA_ADMIN_PASSWORD)
type AuthenticationSettings struct {
	Enabled  bool   `json:"enabled"`  // Enable authentication for proxy requests
	Username string `json:"username"` // Username for proxy authentication
	Password string `json:"password"` // Password for proxy authentication (write-only)
}

// RotationSettings represents proxy rotation configuration
type RotationSettings struct {
	Method             string            `json:"method"`
	TimeBased          TimeBasedSettings `json:"time_based,omitempty"`
	RemoveUnhealthy    bool              `json:"remove_unhealthy"`
	Fallback           bool              `json:"fallback"`
	FallbackMaxRetries int               `json:"fallback_max_retries"`
	FollowRedirect     bool              `json:"follow_redirect"`
	Timeout            int               `json:"timeout"`
	Retries            int               `json:"retries"`
	AllowedProtocols   []string          `json:"allowed_protocols"` // Empty means all supported protocols.
	MaxResponseTime    int               `json:"max_response_time"` // in milliseconds, 0 means no limit
	MinSuccessRate     float64           `json:"min_success_rate"`  // 0-100, 0 means no minimum
}

// TimeBasedSettings represents time-based rotation settings
type TimeBasedSettings struct {
	Interval int `json:"interval"` // in seconds
}

// RateLimitSettings represents rate limiting configuration
type RateLimitSettings struct {
	Enabled     bool `json:"enabled"`
	Interval    int  `json:"interval"` // in seconds
	MaxRequests int  `json:"max_requests"`
}

// HealthCheckSettings represents health check configuration
type HealthCheckSettings struct {
	Timeout int `json:"timeout" minimum:"1" maximum:"15" default:"15"`
	Workers int `json:"workers"`
	// BaseURL and BaseStatus are retained for settings API compatibility; the probe does not use them.
	BaseURL    string `json:"base_url"`
	BaseStatus int    `json:"base_status"`
	// URL and Status expose the fixed YouTube search prefix and expected HTTP 200 contract.
	URL       string   `json:"url" default:"https://www.youtube.com/results?search_query="`
	Status    int      `json:"status" default:"200"`
	Headers   []string `json:"headers"`
	StrictTLS bool     `json:"strict_tls"`
}

// LogRetentionSettings represents log retention and cleanup configuration
type LogRetentionSettings struct {
	Enabled              bool `json:"enabled"`                // Enable automatic log cleanup
	RetentionDays        int  `json:"retention_days"`         // Days to keep logs (7, 15, 30, 60, 90)
	CompressionAfterDays int  `json:"compression_after_days"` // Compress logs older than X days (1, 3, 7, 14)
	CleanupIntervalHours int  `json:"cleanup_interval_hours"` // How often to run cleanup (1, 6, 12, 24)
}

// ProxyCleanupSettings represents dead proxy auto-removal configuration
type ProxyCleanupSettings struct {
	Enabled              bool    `json:"enabled"`                // Enable automatic dead proxy cleanup
	MaxFailedDays        int     `json:"max_failed_days"`        // Remove proxies failed for more than N days
	MinSuccessRate       float64 `json:"min_success_rate"`       // Remove proxies with success rate below X% (0 = disabled)
	CleanupIntervalHours int     `json:"cleanup_interval_hours"` // How often to run cleanup
}

// ProxyLifecycleSettings controls failure observation and automatic archival.
type ProxyLifecycleSettings struct {
	AutoArchiveEnabled        bool `json:"auto_archive_enabled"`
	ActiveRecheckMinutes      int  `json:"active_recheck_minutes"`
	HardUnreachableAfterHours int  `json:"hard_unreachable_after_hours"`
	SoftUnreachableAfterHours int  `json:"soft_unreachable_after_hours"`
	YouTubeUnusableAfterHours int  `json:"youtube_unusable_after_hours"`
}

const (
	GeoIPProviderLocal   = "local"
	GeoIPProviderMaxMind = "maxmind"
)

// GeoIPSettings controls the current lookup database and optional managed updates.
type GeoIPSettings struct {
	Provider            string     `json:"provider"`
	MaxMindLicenseKey   string     `json:"maxmind_license_key"` // Write-only.
	MaxMindDBPath       string     `json:"maxmind_db_path"`
	MaxMindURL          string     `json:"maxmind_url"`
	AutoUpdate          bool       `json:"auto_update"`
	UpdateIntervalHours int        `json:"update_interval_hours"`
	LastUpdatedAt       *time.Time `json:"last_updated_at,omitempty"`
}

// GeoIPStatus reports effective runtime state without exposing download credentials.
type GeoIPStatus struct {
	Provider            string     `json:"provider"`
	Configured          bool       `json:"configured"`
	DatabaseLoaded      bool       `json:"database_loaded"`
	DatabasePath        string     `json:"database_path"`
	ActiveDatabasePath  string     `json:"active_database_path,omitempty"`
	DatabaseType        string     `json:"database_type,omitempty"`
	DatabaseBuildTime   *time.Time `json:"database_build_time,omitempty"`
	Source              string     `json:"source,omitempty"`
	LicenseConfigured   bool       `json:"license_configured"`
	AutoUpdate          bool       `json:"auto_update"`
	UpdateIntervalHours int        `json:"update_interval_hours"`
	LastUpdatedAt       *time.Time `json:"last_updated_at,omitempty"`
	Updating            bool       `json:"updating"`
	LastError           string     `json:"last_error,omitempty"`
}

// SettingRecord represents a settings database record
type SettingRecord struct {
	Key       string         `json:"key"`
	Value     map[string]any `json:"value"`
	UpdatedAt time.Time      `json:"updated_at"`
}
