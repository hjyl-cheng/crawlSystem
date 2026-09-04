package handlers

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
)

func TestMergeWriteOnlySettingsPreservesSecretsAndServiceTimestamp(t *testing.T) {
	updatedAt := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	current := &models.Settings{
		Authentication: models.AuthenticationSettings{Password: "proxy-secret"},
		ProxyLifecycle: models.ProxyLifecycleSettings{ActiveRecheckMinutes: 90},
		GeoIP: models.GeoIPSettings{
			Provider:          models.GeoIPProviderMaxMind,
			MaxMindLicenseKey: "license-secret",
			LastUpdatedAt:     &updatedAt,
		},
	}
	incoming := &models.Settings{
		GeoIP: models.GeoIPSettings{Provider: models.GeoIPProviderMaxMind},
	}

	mergeWriteOnlySettings(incoming, current)

	if incoming.Authentication.Password != "proxy-secret" {
		t.Fatalf("proxy password = %q", incoming.Authentication.Password)
	}
	if incoming.ProxyLifecycle.ActiveRecheckMinutes != 90 {
		t.Fatalf("active recheck minutes = %d", incoming.ProxyLifecycle.ActiveRecheckMinutes)
	}
	if incoming.GeoIP.MaxMindLicenseKey != "license-secret" {
		t.Fatalf("license key = %q", incoming.GeoIP.MaxMindLicenseKey)
	}
	if incoming.GeoIP.LastUpdatedAt == nil || !incoming.GeoIP.LastUpdatedAt.Equal(updatedAt) {
		t.Fatalf("last update = %v", incoming.GeoIP.LastUpdatedAt)
	}
}

func TestRedactSettingsSecretsDoesNotMutateOtherGeoIPState(t *testing.T) {
	settings := &models.Settings{
		Authentication: models.AuthenticationSettings{Password: "proxy-secret"},
		GeoIP: models.GeoIPSettings{
			Provider:          models.GeoIPProviderMaxMind,
			MaxMindLicenseKey: "license-secret",
			MaxMindDBPath:     "/app/geoip/managed/GeoLite2-City.mmdb",
		},
	}

	redactSettingsSecrets(settings)

	if settings.Authentication.Password != "" || settings.GeoIP.MaxMindLicenseKey != "" {
		t.Fatalf("secrets were not redacted: %+v", settings)
	}
	if settings.GeoIP.Provider != models.GeoIPProviderMaxMind || settings.GeoIP.MaxMindDBPath == "" {
		t.Fatalf("non-secret GeoIP state changed: %+v", settings.GeoIP)
	}
}

func TestNormalizeHealthCheckSettingsUsesFixedYouTubeSearchContract(t *testing.T) {
	settings := &models.Settings{HealthCheck: models.HealthCheckSettings{
		URL:    "https://www.youtube.com/watch?v=legacy",
		Status: http.StatusNoContent,
	}}

	normalizeHealthCheckSettings(settings)

	if settings.HealthCheck.URL != models.YouTubeSearchURLPrefix ||
		settings.HealthCheck.Status != http.StatusOK {
		t.Fatalf("health settings = %+v", settings.HealthCheck)
	}
}

func TestValidateSettingsRejectsHealthTimeoutAboveFifteenSeconds(t *testing.T) {
	settings := validSettingsForValidation()
	settings.HealthCheck.Timeout = models.MaxHealthCheckTimeoutSeconds + 1

	err := (&SettingsHandler{}).validateSettings(settings)
	if err == nil || !strings.Contains(err.Error(), "between 1 and 15") {
		t.Fatalf("validation error = %v", err)
	}

	settings.HealthCheck.Timeout = models.MaxHealthCheckTimeoutSeconds
	if err := (&SettingsHandler{}).validateSettings(settings); err != nil {
		t.Fatalf("15-second timeout rejected: %v", err)
	}
}

func validSettingsForValidation() *models.Settings {
	return &models.Settings{
		Rotation:    models.RotationSettings{Timeout: 30, Retries: 1},
		HealthCheck: models.HealthCheckSettings{Timeout: 15, Workers: 1},
		ProxyLifecycle: models.ProxyLifecycleSettings{
			ActiveRecheckMinutes:      120,
			HardUnreachableAfterHours: 6,
			SoftUnreachableAfterHours: 24,
			YouTubeUnusableAfterHours: 72,
		},
	}
}
