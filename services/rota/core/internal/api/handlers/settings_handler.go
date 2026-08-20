package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/repository"
	"github.com/alpkeskin/rota/core/internal/services"
	"github.com/alpkeskin/rota/core/pkg/logger"
)

// SettingsHandler handles settings endpoints
type SettingsHandler struct {
	settingsRepo     *repository.SettingsRepository
	logger           *logger.Logger
	onSettingsUpdate func(ctx context.Context) // called after settings are persisted
	geoSvc           *services.GeoIPService
}

// SetGeoIPService enables GeoIP settings validation and management endpoints.
func (h *SettingsHandler) SetGeoIPService(geoSvc *services.GeoIPService) {
	h.geoSvc = geoSvc
}

// NewSettingsHandler creates a new SettingsHandler.
// onUpdate is an optional callback invoked after settings are saved (e.g. to reload the proxy server).
func NewSettingsHandler(settingsRepo *repository.SettingsRepository, log *logger.Logger, onUpdate func(ctx context.Context)) *SettingsHandler {
	return &SettingsHandler{
		settingsRepo:     settingsRepo,
		logger:           log,
		onSettingsUpdate: onUpdate,
	}
}

// SetOnUpdate sets the callback invoked after settings are persisted.
func (h *SettingsHandler) SetOnUpdate(fn func(ctx context.Context)) {
	h.onSettingsUpdate = fn
}

// Get handles getting current configuration
//
//	@Summary		Get settings
//	@Description	Get current system configuration
//	@Tags			settings
//	@Produce		json
//	@Success		200	{object}	models.Settings	"Current settings"
//	@Failure		500	{object}	models.ErrorResponse
//	@Router			/settings [get]
func (h *SettingsHandler) Get(w http.ResponseWriter, r *http.Request) {
	settings, err := h.settingsRepo.GetAll(r.Context())
	if err != nil {
		h.logger.Error("failed to get settings", "error", err)
		h.errorResponse(w, http.StatusInternalServerError, "Failed to get settings")
		return
	}

	redactSettingsSecrets(settings)

	h.jsonResponse(w, http.StatusOK, settings)
}

// Update handles updating configuration
//
//	@Summary		Update settings
//	@Description	Update system configuration
//	@Tags			settings
//	@Accept			json
//	@Produce		json
//	@Param			request	body		models.Settings			true	"Updated settings"
//	@Success		200		{object}	map[string]interface{}	"Update confirmation"
//	@Failure		400		{object}	models.ErrorResponse
//	@Failure		500		{object}	models.ErrorResponse
//	@Router			/settings [put]
func (h *SettingsHandler) Update(w http.ResponseWriter, r *http.Request) {
	var settings models.Settings
	if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
		h.errorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	currentSettings, err := h.settingsRepo.GetAll(r.Context())
	if err != nil {
		h.logger.Error("failed to load current settings", "error", err)
		h.errorResponse(w, http.StatusInternalServerError, "Failed to load current settings")
		return
	}
	mergeWriteOnlySettings(&settings, currentSettings)
	settings.GeoIP = services.NormalizeGeoIPSettings(settings.GeoIP)

	// Validate settings
	if err := h.validateSettings(&settings); err != nil {
		h.errorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	// Update settings
	// Note: These settings are for PROXY server authentication (port 8000)
	// Dashboard/API authentication uses ROTA_ADMIN_USER/ROTA_ADMIN_PASSWORD (environment)
	if err := h.settingsRepo.UpdateAll(r.Context(), &settings); err != nil {
		h.logger.Error("failed to update settings", "error", err)
		h.errorResponse(w, http.StatusInternalServerError, "Failed to update settings")
		return
	}

	// Get updated settings
	updatedSettings, err := h.settingsRepo.GetAll(r.Context())
	if err != nil {
		h.logger.Error("failed to get updated settings", "error", err)
		h.errorResponse(w, http.StatusInternalServerError, "Failed to get updated settings")
		return
	}

	// Never expose proxy password
	redactSettingsSecrets(updatedSettings)

	response := map[string]interface{}{
		"message": "Configuration updated successfully",
		"config":  updatedSettings,
	}

	h.logger.Info("settings updated successfully")

	// Reload proxy server so changes take effect immediately
	if h.onSettingsUpdate != nil {
		h.onSettingsUpdate(r.Context())
	}

	h.jsonResponse(w, http.StatusOK, response)
}

// Reset handles resetting configuration to defaults
//
//	@Summary		Reset settings
//	@Description	Reset configuration to default values
//	@Tags			settings
//	@Produce		json
//	@Success		200	{object}	map[string]interface{}	"Reset confirmation"
//	@Failure		500	{object}	models.ErrorResponse
//	@Router			/settings/reset [post]
func (h *SettingsHandler) Reset(w http.ResponseWriter, r *http.Request) {
	if err := h.settingsRepo.Reset(r.Context()); err != nil {
		h.logger.Error("failed to reset settings", "error", err)
		h.errorResponse(w, http.StatusInternalServerError, "Failed to reset settings")
		return
	}

	settings, err := h.settingsRepo.GetAll(r.Context())
	if err != nil {
		h.logger.Error("failed to get settings after reset", "error", err)
		h.errorResponse(w, http.StatusInternalServerError, "Failed to get settings")
		return
	}
	redactSettingsSecrets(settings)
	if h.onSettingsUpdate != nil {
		h.onSettingsUpdate(r.Context())
	}

	response := map[string]interface{}{
		"message": "Configuration reset to defaults",
		"config":  settings,
	}

	h.jsonResponse(w, http.StatusOK, response)
}

// GetGeoIPStatus returns the active database and updater state.
//
//	@Summary		Get GeoIP database status
//	@Description	Get the effective GeoIP provider, active database, and updater state
//	@Tags			settings
//	@Produce		json
//	@Success		200	{object}	models.GeoIPStatus
//	@Failure		503	{object}	models.ErrorResponse
//	@Router			/settings/geoip/status [get]
func (h *SettingsHandler) GetGeoIPStatus(w http.ResponseWriter, _ *http.Request) {
	if h.geoSvc == nil {
		h.errorResponse(w, http.StatusServiceUnavailable, "GeoIP service not configured")
		return
	}
	h.jsonResponse(w, http.StatusOK, h.geoSvc.Status())
}

// UpdateGeoIPDB triggers a bounded, validated managed database update.
//
//	@Summary		Update managed GeoIP database
//	@Description	Download, validate, atomically replace, and activate the configured MaxMind database
//	@Tags			settings
//	@Produce		json
//	@Success		200	{object}	map[string]interface{}
//	@Failure		400	{object}	models.ErrorResponse
//	@Failure		409	{object}	models.ErrorResponse
//	@Failure		503	{object}	models.ErrorResponse
//	@Router			/settings/geoip/update-db [post]
func (h *SettingsHandler) UpdateGeoIPDB(w http.ResponseWriter, r *http.Request) {
	if h.geoSvc == nil {
		h.errorResponse(w, http.StatusServiceUnavailable, "GeoIP service not configured")
		return
	}
	if err := h.geoSvc.DownloadAndUpdateDB(r.Context()); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, services.ErrGeoIPUpdateInProgress) {
			status = http.StatusConflict
		}
		h.logger.Error("managed geoip database update failed", "error", err)
		h.errorResponse(w, status, err.Error())
		return
	}
	h.jsonResponse(w, http.StatusOK, map[string]interface{}{
		"message": "GeoIP database updated successfully",
		"status":  h.geoSvc.Status(),
	})
}

func mergeWriteOnlySettings(incoming, current *models.Settings) {
	if incoming == nil || current == nil {
		return
	}
	if incoming.Authentication.Password == "" {
		incoming.Authentication.Password = current.Authentication.Password
	}
	if incoming.GeoIP.Provider == "" {
		incoming.GeoIP = current.GeoIP
		return
	}
	if incoming.GeoIP.MaxMindLicenseKey == "" {
		incoming.GeoIP.MaxMindLicenseKey = current.GeoIP.MaxMindLicenseKey
	}
	incoming.GeoIP.LastUpdatedAt = current.GeoIP.LastUpdatedAt
}

func redactSettingsSecrets(settings *models.Settings) {
	if settings == nil {
		return
	}
	settings.Authentication.Password = ""
	settings.GeoIP.MaxMindLicenseKey = ""
}

// validateSettings validates settings configuration
func (h *SettingsHandler) validateSettings(s *models.Settings) error {
	// Validate rotation timeout
	if s.Rotation.Timeout < 1 || s.Rotation.Timeout > 300 {
		return fmt.Errorf("rotation.timeout must be between 1 and 300")
	}

	// Validate rotation retries
	if s.Rotation.Retries < 0 || s.Rotation.Retries > 10 {
		return fmt.Errorf("rotation.retries must be between 0 and 10")
	}

	// Validate healthcheck timeout
	if s.HealthCheck.Timeout < 1 || s.HealthCheck.Timeout > 300 {
		return fmt.Errorf("healthcheck.timeout must be between 1 and 300")
	}

	// Validate healthcheck workers
	if s.HealthCheck.Workers < 1 || s.HealthCheck.Workers > 100 {
		return fmt.Errorf("healthcheck.workers must be between 1 and 100")
	}

	if s.ProxyLifecycle.HardUnreachableAfterHours < 1 || s.ProxyLifecycle.HardUnreachableAfterHours > 168 {
		return fmt.Errorf("proxy_lifecycle.hard_unreachable_after_hours must be between 1 and 168")
	}
	if s.ProxyLifecycle.SoftUnreachableAfterHours < 1 || s.ProxyLifecycle.SoftUnreachableAfterHours > 336 {
		return fmt.Errorf("proxy_lifecycle.soft_unreachable_after_hours must be between 1 and 336")
	}
	if s.ProxyLifecycle.YouTubeUnusableAfterHours < 1 || s.ProxyLifecycle.YouTubeUnusableAfterHours > 720 {
		return fmt.Errorf("proxy_lifecycle.youtube_unusable_after_hours must be between 1 and 720")
	}
	if h.geoSvc != nil {
		if err := h.geoSvc.ValidateSettings(s.GeoIP); err != nil {
			return err
		}
	}

	return nil
}

// jsonResponse sends a JSON response
func (h *SettingsHandler) jsonResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

// errorResponse sends an error JSON response
func (h *SettingsHandler) errorResponse(w http.ResponseWriter, statusCode int, message string) {
	response := models.ErrorResponse{
		Error: message,
	}
	h.jsonResponse(w, statusCode, response)
}
