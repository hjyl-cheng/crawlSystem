package config

import (
	"reflect"
	"strings"
	"testing"
)

func TestLoadSecurityBoundaryConfig(t *testing.T) {
	t.Setenv("PROXY_PORT", "8000")
	t.Setenv("API_PORT", "8001")
	t.Setenv("LOG_LEVEL", "info")
	t.Setenv("CORS_ALLOWED_ORIGINS", " https://one.example,https://two.example , ")
	t.Setenv("TRUST_PROXY_HEADERS", "yes")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	wantOrigins := []string{"https://one.example", "https://two.example"}
	if !reflect.DeepEqual(cfg.CORSAllowedOrigins, wantOrigins) {
		t.Fatalf("CORSAllowedOrigins = %#v, want %#v", cfg.CORSAllowedOrigins, wantOrigins)
	}
	if !cfg.TrustProxyHeaders {
		t.Fatal("TrustProxyHeaders = false, want true")
	}
}

func TestLoadPeriodicHealthCheckSafetyLimits(t *testing.T) {
	t.Setenv("ROTA_PERIODIC_HEALTH_CHECK_ENABLED", "true")
	t.Setenv("ROTA_PERIODIC_HEALTH_CHECK_INTERVAL_SECONDS", "90")
	t.Setenv("ROTA_PERIODIC_HEALTH_CHECK_BATCH_SIZE", "12")
	t.Setenv("ROTA_PERIODIC_HEALTH_CHECK_WORKERS", "3")
	t.Setenv("ROTA_HEALTH_PROBE_MAX_CONCURRENCY", "5")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.PeriodicHealthCheck.Enabled || cfg.PeriodicHealthCheck.IntervalSeconds != 90 ||
		cfg.PeriodicHealthCheck.BatchSize != 12 || cfg.PeriodicHealthCheck.Workers != 3 ||
		cfg.PeriodicHealthCheck.MaxConcurrency != 5 {
		t.Fatalf("PeriodicHealthCheck = %#v", cfg.PeriodicHealthCheck)
	}
}

func TestLoadProxyMaintenanceDefaultsToShadowWithoutAutomaticMutation(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ProxyMaintenance.InventoryMode != "shadow" {
		t.Fatalf("InventoryMode = %q", cfg.ProxyMaintenance.InventoryMode)
	}
	if cfg.ProxyMaintenance.LifecycleRepairEnabled {
		t.Fatal("legacy lifecycle repair unexpectedly enabled by default")
	}
	if cfg.ProxyMaintenance.HealthEvidenceRetention {
		t.Fatal("health evidence deletion unexpectedly enabled by default")
	}
}

func TestLoadRejectsDestructiveSourceInventoryMode(t *testing.T) {
	t.Setenv("ROTA_SOURCE_INVENTORY_MODE", "delete")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "ROTA_SOURCE_INVENTORY_MODE") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestProxyControlRequiresDedicatedCredentials(t *testing.T) {
	t.Setenv("ROTA_PROXY_CONTROL_ENABLED", "true")
	t.Setenv("ROTA_PROXY_CONTROL_TOKEN", "control-token-secret")
	t.Setenv("ROTA_BULLMQ_PROXY_PASSWORD", "worker-password-secret")
	t.Setenv("ROTA_WORKLOAD_SCOPE", "qy-production")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.ProxyControl.Enabled || cfg.ProxyControl.Token != "control-token-secret" ||
		cfg.ProxyControl.WorkloadScope != "qy-production" {
		t.Fatalf("ProxyControl = %#v", cfg.ProxyControl)
	}
}

func TestProxyControlRejectsShortCredentials(t *testing.T) {
	t.Setenv("ROTA_PROXY_CONTROL_ENABLED", "true")
	t.Setenv("ROTA_PROXY_CONTROL_TOKEN", "short")
	t.Setenv("ROTA_BULLMQ_PROXY_PASSWORD", "worker-password-secret")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "ROTA_PROXY_CONTROL_TOKEN") {
		t.Fatalf("Load() error = %v, want proxy control token error", err)
	}
}

func TestProxyPasswordAloneDoesNotEnableControlPlane(t *testing.T) {
	t.Setenv("ROTA_BULLMQ_PROXY_PASSWORD", "worker-password-secret")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ProxyControl.Enabled || cfg.ProxyControl.Token != "" {
		t.Fatalf("ProxyControl = %#v, want disabled without dedicated token", cfg.ProxyControl)
	}
}

func TestEnvironmentParsersRejectInvalidValues(t *testing.T) {
	t.Setenv("TEST_NEGATIVE_INT", "-1")
	if got := getEnvAsInt("TEST_NEGATIVE_INT", 7); got != 7 {
		t.Fatalf("getEnvAsInt() = %d, want 7", got)
	}

	t.Setenv("TEST_INVALID_BOOL", "sometimes")
	if got := getEnvAsBool("TEST_INVALID_BOOL", true); !got {
		t.Fatal("getEnvAsBool() = false, want default true")
	}
}
