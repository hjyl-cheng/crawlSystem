package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config holds all application configuration
type Config struct {
	ProxyPort int
	APIPort   int
	LogLevel  string
	Database  DatabaseConfig
	AdminUser string
	AdminPass string

	// ProxyControl owns running slots, warm standby allocation, worker leases,
	// and crawler outcome reports. It is disabled unless explicitly enabled or
	// a control token is configured.
	ProxyControl ProxyControlConfig

	// PeriodicHealthCheck bounds the background proxy lifecycle probe. These
	// limits are intentionally independent from the operator-triggered pool
	// health-check settings stored in the database.
	PeriodicHealthCheck PeriodicHealthCheckConfig

	// ProxyMaintenance keeps lifecycle repair, source inventory reconciliation,
	// and health-evidence retention off the request path. Inventory defaults to
	// shadow mode; destructive maintenance remains explicitly opt-in.
	ProxyMaintenance ProxyMaintenanceConfig

	// CORSAllowedOrigins controls browser API and WebSocket origins.
	CORSAllowedOrigins []string

	// TrustProxyHeaders permits forwarded client IP headers only when Rota is
	// deployed behind a trusted reverse proxy.
	TrustProxyHeaders bool

	// Auth brute-force protection
	// Per-IP: after AuthIPMaxAttempts failures within AuthIPWindowMinutes,
	// that IP is blocked for AuthIPBlockMinutes.
	// Global: if total login attempts across all IPs exceed AuthGlobalMaxPerMinute
	// in a 1-minute window, the login endpoint is locked for AuthGlobalLockoutMin.
	AuthIPMaxAttempts      int // failed attempts before IP block       (AUTH_IP_MAX_ATTEMPTS, default 10)
	AuthIPWindowMinutes    int // sliding window to count attempts      (AUTH_IP_WINDOW_MINUTES, default 10)
	AuthIPBlockMinutes     int // how long to block an IP               (AUTH_IP_BLOCK_MINUTES, default 30)
	AuthGlobalMaxPerMinute int // max total attempts/min before lockout (AUTH_GLOBAL_MAX_PER_MINUTE, default 1000)
	AuthGlobalLockoutMin   int // global lockout duration in minutes    (AUTH_GLOBAL_LOCKOUT_MINUTES, default 1)
}

// ProxyControlConfig configures Rota's worker-facing proxy control module.
type ProxyControlConfig struct {
	Enabled             bool
	Token               string
	WorkerPassword      string
	WorkloadScope       string
	DiscoverSlots       int
	ChannelSlots        int
	QueryQualitySlots   int
	DetailSlots         int
	LeaseSeconds        int
	ReconcileIntervalMS int
	ResourceSyncMinutes int
	MinReservePercent   int
	MinReserveCount     int
	FailureCooldownMin  int
	NetworkCooldownMin  int
}

// PeriodicHealthCheckConfig configures the bounded background health-check
// loop. Database health-check worker settings continue to apply to explicit
// pool jobs and cannot raise this loop's concurrency.
type PeriodicHealthCheckConfig struct {
	Enabled         bool
	IntervalSeconds int
	BatchSize       int
	Workers         int
	MaxConcurrency  int
}

// ProxyMaintenanceConfig controls bounded background maintenance. Source
// retirement requires both InventoryMode=enforce and per-source opt-in.
type ProxyMaintenanceConfig struct {
	InventoryMode                 string
	InventoryMinimumMisses        int
	LifecycleAuditIntervalSeconds int
	LifecycleRepairEnabled        bool
	LifecycleRepairBatchSize      int
	LifecycleRepairSpreadSeconds  int
	LifecycleConstraintsEnabled   bool
	HealthEvidenceRetention       bool
	HealthEvidenceRetentionDays   int
	HealthEvidenceBatchSize       int
	HealthEvidenceIntervalSeconds int
}

// DatabaseConfig holds database configuration
type DatabaseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Name     string
	SSLMode  string
}

// DSN returns the database connection string
func (d *DatabaseConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		d.Host, d.Port, d.User, d.Password, d.Name, d.SSLMode,
	)
}

// Load reads configuration from environment variables
func Load() (*Config, error) {
	controlToken := firstNonEmptyEnv("ROTA_PROXY_CONTROL_TOKEN")
	workerPassword := firstNonEmptyEnv("ROTA_BULLMQ_PROXY_PASSWORD")
	cfg := &Config{
		ProxyPort: getEnvAsInt("PROXY_PORT", 8000),
		APIPort:   getEnvAsInt("API_PORT", 8001),
		LogLevel:  getEnv("LOG_LEVEL", "info"),
		Database: DatabaseConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     getEnvAsInt("DB_PORT", 5432),
			User:     getEnv("DB_USER", "rota"),
			Password: getEnv("DB_PASSWORD", "rota_password"),
			Name:     getEnv("DB_NAME", "rota"),
			SSLMode:  getEnv("DB_SSLMODE", "disable"),
		},
		AdminUser: getEnv("ROTA_ADMIN_USER", "admin"),
		AdminPass: getEnv("ROTA_ADMIN_PASSWORD", "admin"),
		ProxyControl: ProxyControlConfig{
			Enabled:             getEnvAsBool("ROTA_PROXY_CONTROL_ENABLED", controlToken != ""),
			Token:               controlToken,
			WorkerPassword:      workerPassword,
			WorkloadScope:       getEnv("ROTA_WORKLOAD_SCOPE", "qy-production"),
			DiscoverSlots:       getEnvAsInt("ROTA_DISCOVER_SLOTS", 0),
			ChannelSlots:        getEnvAsInt("ROTA_CHANNEL_SLOTS", 20),
			QueryQualitySlots:   getEnvAsInt("ROTA_QUERY_QUALITY_SLOTS", 0),
			DetailSlots:         getEnvAsInt("ROTA_DETAIL_SLOTS", 0),
			LeaseSeconds:        getEnvAsInt("ROTA_PROXY_LEASE_SECONDS", 60),
			ReconcileIntervalMS: getEnvAsInt("ROTA_PROXY_RECONCILE_INTERVAL_MS", 5000),
			ResourceSyncMinutes: getEnvAsInt("ROTA_PROXY_RESOURCE_SYNC_MINUTES", 10),
			MinReservePercent:   getEnvAsInt("ROTA_PROXY_MIN_RESERVE_PERCENT", 25),
			MinReserveCount:     getEnvAsInt("ROTA_PROXY_MIN_RESERVE_COUNT", 3),
			FailureCooldownMin:  getEnvAsInt("ROTA_PROXY_FAILURE_COOLDOWN_MINUTES", 30),
			NetworkCooldownMin:  getEnvAsInt("ROTA_PROXY_NETWORK_COOLDOWN_MINUTES", 5),
		},
		PeriodicHealthCheck: PeriodicHealthCheckConfig{
			Enabled:         getEnvAsBool("ROTA_PERIODIC_HEALTH_CHECK_ENABLED", true),
			IntervalSeconds: getEnvAsInt("ROTA_PERIODIC_HEALTH_CHECK_INTERVAL_SECONDS", 60),
			BatchSize:       getEnvAsInt("ROTA_PERIODIC_HEALTH_CHECK_BATCH_SIZE", 20),
			Workers:         getEnvAsInt("ROTA_PERIODIC_HEALTH_CHECK_WORKERS", 4),
			MaxConcurrency:  getEnvAsInt("ROTA_HEALTH_PROBE_MAX_CONCURRENCY", 4),
		},
		ProxyMaintenance: ProxyMaintenanceConfig{
			InventoryMode:                 strings.ToLower(strings.TrimSpace(getEnv("ROTA_SOURCE_INVENTORY_MODE", "shadow"))),
			InventoryMinimumMisses:        getEnvAsInt("ROTA_SOURCE_INVENTORY_MINIMUM_MISSES", 3),
			LifecycleAuditIntervalSeconds: getEnvAsInt("ROTA_LIFECYCLE_AUDIT_INTERVAL_SECONDS", 300),
			LifecycleRepairEnabled:        getEnvAsBool("ROTA_LIFECYCLE_REPAIR_ENABLED", false),
			LifecycleRepairBatchSize:      getEnvAsInt("ROTA_LIFECYCLE_REPAIR_BATCH_SIZE", 25),
			LifecycleRepairSpreadSeconds:  getEnvAsInt("ROTA_LIFECYCLE_REPAIR_SPREAD_SECONDS", 3600),
			LifecycleConstraintsEnabled:   getEnvAsBool("ROTA_LIFECYCLE_CONSTRAINTS_ENABLED", false),
			HealthEvidenceRetention:       getEnvAsBool("ROTA_HEALTH_EVIDENCE_RETENTION_ENABLED", false),
			HealthEvidenceRetentionDays:   getEnvAsInt("ROTA_HEALTH_EVIDENCE_RETENTION_DAYS", 14),
			HealthEvidenceBatchSize:       getEnvAsInt("ROTA_HEALTH_EVIDENCE_RETENTION_BATCH_SIZE", 2000),
			HealthEvidenceIntervalSeconds: getEnvAsInt("ROTA_HEALTH_EVIDENCE_RETENTION_INTERVAL_SECONDS", 60),
		},
		CORSAllowedOrigins: splitAndTrim(
			getEnv("CORS_ALLOWED_ORIGINS", "*"),
		),
		TrustProxyHeaders: getEnvAsBool("TRUST_PROXY_HEADERS", false),

		AuthIPMaxAttempts:      getEnvAsInt("AUTH_IP_MAX_ATTEMPTS", 10),
		AuthIPWindowMinutes:    getEnvAsInt("AUTH_IP_WINDOW_MINUTES", 10),
		AuthIPBlockMinutes:     getEnvAsInt("AUTH_IP_BLOCK_MINUTES", 30),
		AuthGlobalMaxPerMinute: getEnvAsInt("AUTH_GLOBAL_MAX_PER_MINUTE", 1000),
		AuthGlobalLockoutMin:   getEnvAsInt("AUTH_GLOBAL_LOCKOUT_MINUTES", 1),
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.ProxyPort < 1 || c.ProxyPort > 65535 {
		return fmt.Errorf("invalid proxy port: %d", c.ProxyPort)
	}
	if c.APIPort < 1 || c.APIPort > 65535 {
		return fmt.Errorf("invalid API port: %d", c.APIPort)
	}
	if c.ProxyPort == c.APIPort {
		return fmt.Errorf("proxy port and API port cannot be the same: %d", c.ProxyPort)
	}

	validLogLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}
	if !validLogLevels[c.LogLevel] {
		return fmt.Errorf("invalid log level: %s (must be debug, info, warn, or error)", c.LogLevel)
	}

	if c.ProxyControl.Enabled {
		if strings.TrimSpace(c.ProxyControl.WorkloadScope) == "" {
			return fmt.Errorf("ROTA_WORKLOAD_SCOPE cannot be empty")
		}
		if len(c.ProxyControl.Token) < 12 {
			return fmt.Errorf("ROTA_PROXY_CONTROL_TOKEN must contain at least 12 characters")
		}
		if len(c.ProxyControl.WorkerPassword) < 12 {
			return fmt.Errorf("ROTA_BULLMQ_PROXY_PASSWORD must contain at least 12 characters")
		}
		if c.ProxyControl.LeaseSeconds < 15 || c.ProxyControl.LeaseSeconds > 600 {
			return fmt.Errorf("ROTA_PROXY_LEASE_SECONDS must be between 15 and 600")
		}
		if c.ProxyControl.ReconcileIntervalMS < 1000 || c.ProxyControl.ReconcileIntervalMS > 300000 {
			return fmt.Errorf("ROTA_PROXY_RECONCILE_INTERVAL_MS must be between 1000 and 300000")
		}
		if c.ProxyControl.ResourceSyncMinutes < 1 || c.ProxyControl.ResourceSyncMinutes > 1440 {
			return fmt.Errorf("ROTA_PROXY_RESOURCE_SYNC_MINUTES must be between 1 and 1440")
		}
		if c.ProxyControl.MinReservePercent < 0 || c.ProxyControl.MinReservePercent > 100 {
			return fmt.Errorf("ROTA_PROXY_MIN_RESERVE_PERCENT must be between 0 and 100")
		}
		if c.ProxyControl.DiscoverSlots > 100 || c.ProxyControl.ChannelSlots > 500 ||
			c.ProxyControl.QueryQualitySlots > 100 || c.ProxyControl.DetailSlots > 500 {
			return fmt.Errorf("proxy control slot counts exceed their supported limits")
		}
		if c.ProxyControl.FailureCooldownMin < 1 || c.ProxyControl.FailureCooldownMin > 1440 {
			return fmt.Errorf("ROTA_PROXY_FAILURE_COOLDOWN_MINUTES must be between 1 and 1440")
		}
		if c.ProxyControl.NetworkCooldownMin < 1 || c.ProxyControl.NetworkCooldownMin > 1440 {
			return fmt.Errorf("ROTA_PROXY_NETWORK_COOLDOWN_MINUTES must be between 1 and 1440")
		}
	}

	if c.PeriodicHealthCheck.Enabled {
		if c.PeriodicHealthCheck.IntervalSeconds < 10 || c.PeriodicHealthCheck.IntervalSeconds > 86400 {
			return fmt.Errorf("ROTA_PERIODIC_HEALTH_CHECK_INTERVAL_SECONDS must be between 10 and 86400")
		}
		if c.PeriodicHealthCheck.BatchSize < 1 || c.PeriodicHealthCheck.BatchSize > 1000 {
			return fmt.Errorf("ROTA_PERIODIC_HEALTH_CHECK_BATCH_SIZE must be between 1 and 1000")
		}
		if c.PeriodicHealthCheck.Workers < 1 || c.PeriodicHealthCheck.Workers > 100 {
			return fmt.Errorf("ROTA_PERIODIC_HEALTH_CHECK_WORKERS must be between 1 and 100")
		}
		if c.PeriodicHealthCheck.Workers > c.PeriodicHealthCheck.BatchSize {
			return fmt.Errorf("ROTA_PERIODIC_HEALTH_CHECK_WORKERS cannot exceed ROTA_PERIODIC_HEALTH_CHECK_BATCH_SIZE")
		}
		if c.PeriodicHealthCheck.MaxConcurrency < 1 || c.PeriodicHealthCheck.MaxConcurrency > 100 {
			return fmt.Errorf("ROTA_HEALTH_PROBE_MAX_CONCURRENCY must be between 1 and 100")
		}
	}

	switch c.ProxyMaintenance.InventoryMode {
	case "off", "shadow", "enforce":
	default:
		return fmt.Errorf("ROTA_SOURCE_INVENTORY_MODE must be off, shadow, or enforce")
	}
	if c.ProxyMaintenance.InventoryMinimumMisses < 2 || c.ProxyMaintenance.InventoryMinimumMisses > 100 {
		return fmt.Errorf("ROTA_SOURCE_INVENTORY_MINIMUM_MISSES must be between 2 and 100")
	}
	if c.ProxyMaintenance.LifecycleAuditIntervalSeconds < 30 || c.ProxyMaintenance.LifecycleAuditIntervalSeconds > 86400 {
		return fmt.Errorf("ROTA_LIFECYCLE_AUDIT_INTERVAL_SECONDS must be between 30 and 86400")
	}
	if c.ProxyMaintenance.LifecycleRepairBatchSize < 1 || c.ProxyMaintenance.LifecycleRepairBatchSize > 500 {
		return fmt.Errorf("ROTA_LIFECYCLE_REPAIR_BATCH_SIZE must be between 1 and 500")
	}
	if c.ProxyMaintenance.LifecycleRepairSpreadSeconds < 60 || c.ProxyMaintenance.LifecycleRepairSpreadSeconds > 86400 {
		return fmt.Errorf("ROTA_LIFECYCLE_REPAIR_SPREAD_SECONDS must be between 60 and 86400")
	}
	if c.ProxyMaintenance.HealthEvidenceRetentionDays < 4 || c.ProxyMaintenance.HealthEvidenceRetentionDays > 3650 {
		return fmt.Errorf("ROTA_HEALTH_EVIDENCE_RETENTION_DAYS must be between 4 and 3650")
	}
	if c.ProxyMaintenance.HealthEvidenceBatchSize < 1 || c.ProxyMaintenance.HealthEvidenceBatchSize > 10000 {
		return fmt.Errorf("ROTA_HEALTH_EVIDENCE_RETENTION_BATCH_SIZE must be between 1 and 10000")
	}
	if c.ProxyMaintenance.HealthEvidenceIntervalSeconds < 10 || c.ProxyMaintenance.HealthEvidenceIntervalSeconds > 86400 {
		return fmt.Errorf("ROTA_HEALTH_EVIDENCE_RETENTION_INTERVAL_SECONDS must be between 10 and 86400")
	}

	return nil
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func firstNonEmptyEnv(keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return ""
}

// getEnvAsInt retrieves a non-negative integer or returns the default value.
func getEnvAsInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	intValue, err := strconv.Atoi(value)
	if err != nil || intValue < 0 {
		fmt.Fprintf(os.Stderr, "config: invalid integer for %s=%q; using default %d\n", key, value, defaultValue)
		return defaultValue
	}
	return intValue
}

func getEnvAsBool(key string, defaultValue bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	switch strings.ToLower(value) {
	case "1", "t", "true", "yes", "on":
		return true
	case "0", "f", "false", "no", "off":
		return false
	default:
		fmt.Fprintf(os.Stderr, "config: invalid boolean for %s=%q; using default %t\n", key, value, defaultValue)
		return defaultValue
	}
}

func splitAndTrim(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			result = append(result, part)
		}
	}
	if len(result) == 0 {
		return []string{"*"}
	}
	return result
}
