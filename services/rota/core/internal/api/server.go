package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/alpkeskin/rota/core/docs"
	"github.com/alpkeskin/rota/core/internal/api/handlers"
	"github.com/alpkeskin/rota/core/internal/background"
	"github.com/alpkeskin/rota/core/internal/config"
	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/proxy"
	"github.com/alpkeskin/rota/core/internal/proxycontrol"
	"github.com/alpkeskin/rota/core/internal/proxymaintenance"
	"github.com/alpkeskin/rota/core/internal/repository"
	"github.com/alpkeskin/rota/core/internal/services"
	"github.com/alpkeskin/rota/core/internal/sourceinventory"
	"github.com/alpkeskin/rota/core/pkg/logger"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"

	// Import for swagger documentation
	_ "github.com/alpkeskin/rota/core/internal/models"
)

// ProxyServer interface for reloading proxy pool
type ProxyServer interface {
	ReloadSettings(ctx context.Context) error
	RefreshProxyUser(username string)
	RetireProxyUser(ctx context.Context, username string) error
	ActivateProxyUser(ctx context.Context, oldUsername, newUsername string, expectedProxyID int) error
}

// Server represents the API server
type Server struct {
	router            *chi.Mux
	server            *http.Server
	logger            *logger.Logger
	db                *database.DB
	port              int
	jwtSecret         string
	authRL            *authRateLimiter
	corsOrigins       []string
	trustProxyHeaders bool
	background        *background.Group
	closeServices     func() error

	// Proxy server reference for reloading
	proxyServer         ProxyServer
	proxyControl        *proxycontrol.Manager
	proxyControlToken   string
	proxyControlEnabled bool

	// Handlers
	authHandler          *handlers.AuthHandler
	healthHandler        *handlers.HealthHandler
	dashboardHandler     *handlers.DashboardHandler
	proxyHandler         *handlers.ProxyHandler
	logsHandler          *handlers.LogsHandler
	settingsHandler      *handlers.SettingsHandler
	websocketHandler     *handlers.WebSocketHandler
	metricsHandler       *handlers.MetricsHandler
	documentationHandler *handlers.DocumentationHandler
	sourceHandler        *handlers.SourceHandler
	poolHandler          *handlers.PoolHandler
	userHandler          *handlers.UserHandler
	proxyControlHandler  *ProxyControlHandler
}

// New creates a new API server instance
func New(
	cfg *config.Config,
	log *logger.Logger,
	db *database.DB,
	identityPolicies map[string]proxycontrol.IdentityPolicy,
	identityPolicyCatalogVersion int,
	identityPolicyCatalogDigest string,
) *Server {
	// Initialize repositories
	proxyRepo := repository.NewProxyRepository(db)
	logRepo := repository.NewLogRepository(db)
	settingsRepo := repository.NewSettingsRepository(db)
	dashboardRepo := repository.NewDashboardRepository(db)
	sourceRepo := repository.NewSourceRepository(db)
	poolRepo := repository.NewPoolRepository(db)
	userRepo := repository.NewUserRepository(db)
	adminRepo := repository.NewAdminRepository(db)

	// Seed admin credentials from env on first start (no-op if already seeded)
	if err := adminRepo.Seed(context.Background(), cfg.AdminUser, cfg.AdminPass); err != nil {
		log.Warn("failed to seed admin credentials", "error", err)
	}

	// Generate random JWT secret on startup
	// This ensures all previous tokens become invalid on restart
	jwtSecret := generateJWTSecret()
	log.Info("generated new JWT secret for this session", "length", len(jwtSecret))

	// Create health checker for testing proxies
	healthChecker := proxy.NewHealthCheckerWithLimit(
		proxyRepo, settingsRepo, log, cfg.PeriodicHealthCheck.MaxConcurrency,
	)
	serviceGroup := background.New(context.Background())

	// GeoIP + source + pool services
	geoSvc := services.NewManagedGeoIPService(settingsRepo, log)
	sourceSvc := services.NewSourceService(sourceRepo, proxyRepo, poolRepo, geoSvc, log)
	inventoryPolicy, inventoryPolicyErr := sourceinventory.NewPolicy(
		cfg.ProxyMaintenance.InventoryMode,
		cfg.ProxyMaintenance.InventoryMinimumMisses,
	)
	if inventoryPolicyErr != nil {
		log.Error("source inventory policy is invalid; inventory reconciliation disabled", "error", inventoryPolicyErr)
		inventoryPolicy, _ = sourceinventory.NewPolicy("off", 3)
	}
	sourceSvc.SetInventoryPolicy(inventoryPolicy)
	sourceSvc.SetTransportCacheInvalidator(proxy.ClearTransportCache)
	poolSvc := services.NewPoolService(poolRepo, proxyRepo, healthChecker, serviceGroup, log)
	control := proxycontrol.New(
		db,
		proxyRepo,
		healthChecker,
		proxycontrol.Options{
			Enabled:              cfg.ProxyControl.Enabled,
			WorkloadScope:        cfg.ProxyControl.WorkloadScope,
			CatalogVersion:       identityPolicyCatalogVersion,
			CatalogDigest:        identityPolicyCatalogDigest,
			WorkerPassword:       cfg.ProxyControl.WorkerPassword,
			DiscoverSlots:        cfg.ProxyControl.DiscoverSlots,
			ChannelSlots:         cfg.ProxyControl.ChannelSlots,
			QueryQualitySlots:    cfg.ProxyControl.QueryQualitySlots,
			DetailSlots:          cfg.ProxyControl.DetailSlots,
			LeaseDuration:        time.Duration(cfg.ProxyControl.LeaseSeconds) * time.Second,
			ReconcileInterval:    time.Duration(cfg.ProxyControl.ReconcileIntervalMS) * time.Millisecond,
			ResourceSyncInterval: time.Duration(cfg.ProxyControl.ResourceSyncMinutes) * time.Minute,
			MinReservePercent:    cfg.ProxyControl.MinReservePercent,
			MinReserveCount:      cfg.ProxyControl.MinReserveCount,
			FailureCooldown:      time.Duration(cfg.ProxyControl.FailureCooldownMin) * time.Minute,
			NetworkCooldown:      time.Duration(cfg.ProxyControl.NetworkCooldownMin) * time.Minute,
			IdentityPolicies:     identityPolicies,
		},
		log,
	)
	healthChecker.SetOnVerdictApplied(func(event proxy.HealthVerdictEvent) {
		control.NotifyHealthIncident(event.ProxyID)
	})

	// Initialize handlers
	authHandler := handlers.NewAuthHandler(settingsRepo, adminRepo, log, jwtSecret, cfg.AdminUser, cfg.AdminPass)
	healthHandler := handlers.NewHealthHandler(db, proxyRepo, log)
	dashboardHandler := handlers.NewDashboardHandler(dashboardRepo, proxyRepo, log)
	proxyHandler := handlers.NewProxyHandler(proxyRepo, healthChecker, sourceSvc, log)
	proxyHandler.SetTransportCacheInvalidator(proxy.ClearTransportCache)
	proxyHandler.SetOperationContext(serviceGroup.Context())
	logsHandler := handlers.NewLogsHandler(logRepo, log)
	settingsHandler := handlers.NewSettingsHandler(settingsRepo, log, nil) // onUpdate set below
	settingsHandler.SetGeoIPService(geoSvc)
	websocketHandler := handlers.NewWebSocketHandler(dashboardRepo, proxyRepo, logRepo, log, cfg.CORSAllowedOrigins)
	metricsHandler := handlers.NewMetricsHandler(log)
	documentationHandler := handlers.NewDocumentationHandler()
	sourceHandler := handlers.NewSourceHandler(sourceRepo, sourceSvc, log)
	poolHandler := handlers.NewPoolHandler(poolRepo, poolSvc, log)
	userHandler := handlers.NewUserHandler(userRepo, poolRepo, log)
	proxyControlHandler := NewProxyControlHandler(control)

	// Auth rate limiter (per-IP block + global lockout)
	authRL := newAuthRateLimiter(
		cfg.AuthIPMaxAttempts,
		cfg.AuthIPWindowMinutes,
		cfg.AuthIPBlockMinutes,
		cfg.AuthGlobalMaxPerMinute,
		cfg.AuthGlobalLockoutMin,
		cfg.TrustProxyHeaders,
		log,
	)

	s := &Server{
		router:               chi.NewRouter(),
		logger:               log,
		db:                   db,
		port:                 cfg.APIPort,
		jwtSecret:            jwtSecret,
		authRL:               authRL,
		corsOrigins:          cfg.CORSAllowedOrigins,
		trustProxyHeaders:    cfg.TrustProxyHeaders,
		background:           serviceGroup,
		closeServices:        geoSvc.Close,
		proxyControl:         control,
		proxyControlToken:    cfg.ProxyControl.Token,
		proxyControlEnabled:  cfg.ProxyControl.Enabled,
		authHandler:          authHandler,
		healthHandler:        healthHandler,
		dashboardHandler:     dashboardHandler,
		proxyHandler:         proxyHandler,
		logsHandler:          logsHandler,
		settingsHandler:      settingsHandler,
		websocketHandler:     websocketHandler,
		metricsHandler:       metricsHandler,
		documentationHandler: documentationHandler,
		sourceHandler:        sourceHandler,
		poolHandler:          poolHandler,
		userHandler:          userHandler,
		proxyControlHandler:  proxyControlHandler,
	}

	// Wire settings reload: when settings are updated via API, reload live services.
	settingsHandler.SetOnUpdate(func(ctx context.Context) {
		if err := geoSvc.ReloadSettings(ctx); err != nil {
			log.Warn("failed to activate updated geoip settings; retaining last working database", "error", err)
		}
		if s.proxyServer != nil {
			if err := s.proxyServer.ReloadSettings(ctx); err != nil {
				log.Error("failed to reload proxy settings after update", "error", err)
			} else {
				log.Info("proxy settings reloaded after update")
			}
		}
	})

	// Alert watcher + due lifecycle checks
	alertWatcher := services.NewAlertWatcher(poolRepo, log)
	proxyMaintainer := proxymaintenance.New(db, log, proxymaintenance.Options{
		AuditInterval:      time.Duration(cfg.ProxyMaintenance.LifecycleAuditIntervalSeconds) * time.Second,
		RepairEnabled:      cfg.ProxyMaintenance.LifecycleRepairEnabled,
		RepairBatchSize:    cfg.ProxyMaintenance.LifecycleRepairBatchSize,
		RepairSpread:       time.Duration(cfg.ProxyMaintenance.LifecycleRepairSpreadSeconds) * time.Second,
		ConstraintsEnabled: cfg.ProxyMaintenance.LifecycleConstraintsEnabled,
		RetentionEnabled:   cfg.ProxyMaintenance.HealthEvidenceRetention,
		Retention:          time.Duration(cfg.ProxyMaintenance.HealthEvidenceRetentionDays) * 24 * time.Hour,
		RetentionBatchSize: cfg.ProxyMaintenance.HealthEvidenceBatchSize,
		RetentionInterval:  time.Duration(cfg.ProxyMaintenance.HealthEvidenceIntervalSeconds) * time.Second,
	})

	serviceGroup.Go(sourceSvc.Run)
	serviceGroup.Go(poolSvc.Run)
	serviceGroup.Go(alertWatcher.Run)
	serviceGroup.Go(geoSvc.Run)
	serviceGroup.Go(authRL.Run)
	serviceGroup.Go(proxyMaintainer.Run)
	if cfg.PeriodicHealthCheck.Enabled {
		serviceGroup.Go(func(ctx context.Context) {
			healthChecker.StartPeriodicHealthCheck(
				ctx,
				time.Duration(cfg.PeriodicHealthCheck.IntervalSeconds)*time.Second,
				cfg.PeriodicHealthCheck.BatchSize,
				cfg.PeriodicHealthCheck.Workers,
			)
		})
	} else {
		log.Info("periodic proxy health check disabled")
	}
	if cfg.ProxyControl.Enabled {
		serviceGroup.Go(control.Run)
	}

	s.setupMiddleware()
	s.setupRoutes()

	s.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", s.port),
		Handler:      s.router,
		BaseContext:  func(net.Listener) context.Context { return serviceGroup.Context() },
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 10 * time.Minute, // health checks on large pools can take several minutes
		IdleTimeout:  120 * time.Second,
	}

	return s
}

// setupMiddleware configures middleware for the API server
func (s *Server) setupMiddleware() {
	origins := s.corsOrigins
	if len(origins) == 0 {
		origins = []string{"*"}
	}
	s.router.Use(cors.Handler(cors.Options{
		AllowedOrigins:   origins,
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	s.router.Use(middleware.RequestID)
	if s.trustProxyHeaders {
		s.router.Use(middleware.RealIP)
	}
	s.router.Use(LoggerMiddleware(s.logger))
	s.router.Use(middleware.Recoverer)
	// No global timeout — health-check routes need minutes; individual routes handle their own timeouts
}

// setupRoutes configures all API routes
func (s *Server) setupRoutes() {
	// ── Fully public routes ────────────────────────────────────────────────
	s.router.Get("/health", s.healthHandler.Health)

	// API Documentation (public — read-only reference)
	s.router.Get("/docs", s.documentationHandler.ServeDocumentation)
	s.router.Get("/api/v1/swagger.json", s.serveSwaggerJSON)

	// Auth: only login is public; everything else requires a valid JWT
	// Auth rate limiter wraps the login handler — per-IP block + global lockout
	s.router.With(s.authRL.Middleware()).Post("/api/v1/auth/login", s.authHandler.Login)

	// Worker control uses a stable shared token because dashboard JWTs are
	// intentionally regenerated on every Rota restart.
	if s.proxyControlEnabled && s.proxyControlHandler != nil {
		s.router.Route("/api/v1/proxy-control", func(r chi.Router) {
			r.Use(ProxyControlTokenMiddleware(s.proxyControlToken))
			r.Post("/claim", s.proxyControlHandler.Claim)
			r.Post("/renew", s.proxyControlHandler.Renew)
			r.Post("/tasks/begin", s.proxyControlHandler.BeginTask)
			r.Post("/tasks/observe", s.proxyControlHandler.Observe)
			r.Post("/tasks/complete", s.proxyControlHandler.CompleteTask)
			r.Post("/report", s.proxyControlHandler.Report)
			r.Post("/swap", s.proxyControlHandler.Swap)
			r.Post("/release", s.proxyControlHandler.Release)
			r.Get("/capacity", s.proxyControlHandler.Capacity)
		})
	}

	// ── Protected routes (JWT required) ────────────────────────────────────
	s.router.Route("/api/v1", func(r chi.Router) {
		r.Use(JWTMiddleware(s.jwtSecret))

		// Auth (require token — change-password, whoami)
		r.Post("/auth/change-password", s.authHandler.ChangePassword)
		r.Get("/auth/me", s.authHandler.GetAdminInfo)

		// Health & Status
		r.Get("/status", s.healthHandler.Status)
		r.Get("/database/health", s.healthHandler.DatabaseHealth)
		r.Get("/database/stats", s.healthHandler.DatabaseStats)

		// System Metrics
		r.Get("/metrics/system", s.metricsHandler.GetSystemMetrics)

		// Dashboard endpoints
		r.Get("/dashboard/stats", s.dashboardHandler.GetStats)
		r.Get("/dashboard/charts/response-time", s.dashboardHandler.GetResponseTimeChart)
		r.Get("/dashboard/charts/success-rate", s.dashboardHandler.GetSuccessRateChart)

		// Proxy management
		r.Get("/proxies", s.proxyHandler.List)
		r.Post("/proxies", s.proxyHandler.Create)
		r.Post("/proxies/bulk", s.proxyHandler.BulkCreate)
		r.Post("/proxies/bulk-delete", s.proxyHandler.BulkDelete)
		r.Post("/proxies/bulk-tags", s.proxyHandler.BulkTag)
		r.Post("/proxies/bulk-archive", s.proxyHandler.BulkArchive)
		r.Post("/proxies/bulk-restore", s.proxyHandler.BulkRestore)
		r.Delete("/proxies", s.proxyHandler.DeleteAll)
		r.Get("/proxies/export", s.proxyHandler.Export)
		r.Put("/proxies/{id}", s.proxyHandler.Update)
		r.Delete("/proxies/{id}", s.proxyHandler.Delete)
		r.Post("/proxies/{id}/test", s.proxyHandler.Test)
		r.Post("/proxies/{id}/archive", s.proxyHandler.Archive)
		r.Post("/proxies/{id}/restore", s.proxyHandler.Restore)
		r.Post("/proxies/reload", s.ReloadProxyPool)

		// System logs
		r.Get("/logs", s.logsHandler.List)
		r.Get("/logs/export", s.logsHandler.Export)

		// Settings
		r.Get("/settings", s.settingsHandler.Get)
		r.Put("/settings", s.settingsHandler.Update)
		r.Post("/settings/reset", s.settingsHandler.Reset)
		r.Get("/settings/geoip/status", s.settingsHandler.GetGeoIPStatus)
		r.Post("/settings/geoip/update-db", s.settingsHandler.UpdateGeoIPDB)

		// Proxy Sources
		r.Get("/sources", s.sourceHandler.List)
		r.Post("/sources", s.sourceHandler.Create)
		r.Put("/sources/{id}", s.sourceHandler.Update)
		r.Delete("/sources/{id}", s.sourceHandler.Delete)
		r.Post("/sources/{id}/fetch", s.sourceHandler.FetchNow)
		r.Post("/sources/enrich-geo", s.sourceHandler.EnrichGeo)

		// Proxy Users (per-user pool authentication)
		r.Get("/proxy-users", s.userHandler.List)
		r.Post("/proxy-users", s.userHandler.Create)
		r.Post("/proxy-users/refresh", s.RefreshProxyUser)
		r.Get("/proxy-users/{id}", s.userHandler.Get)
		r.Put("/proxy-users/{id}", s.userHandler.Update)
		r.Delete("/proxy-users/{id}", s.userHandler.Delete)

		// Proxy Pools
		r.Get("/pools", s.poolHandler.List)
		r.Post("/pools", s.poolHandler.Create)
		r.Get("/pools/geo-summary", s.poolHandler.GeoSummary)
		r.Get("/pools/geo-countries", s.poolHandler.GeoByCountry)
		r.Get("/pools/geo-cities/{country_code}", s.poolHandler.GeoCitiesByCountry)
		r.Get("/pools/isp-list", s.poolHandler.GetISPList)
		r.Get("/pools/tag-list", s.poolHandler.GetTagList)
		r.Get("/pools/{id}", s.poolHandler.Get)
		r.Put("/pools/{id}", s.poolHandler.Update)
		r.Delete("/pools/{id}", s.poolHandler.Delete)
		r.Get("/pools/{id}/proxies", s.poolHandler.GetProxies)
		r.Post("/pools/{id}/proxies", s.poolHandler.AddProxies)
		r.Delete("/pools/{id}/proxies", s.poolHandler.RemoveProxies)
		r.Post("/pools/{id}/sync", s.poolHandler.Sync)
		r.Get("/pools/{id}/export", s.poolHandler.Export)
		r.Post("/pools/{id}/health-check", s.poolHandler.HealthCheck)
		r.Get("/pools/{id}/health-check/jobs", s.poolHandler.HealthCheckJobs)
		r.Get("/pools/{id}/health-check/{job_id}", s.poolHandler.HealthCheckStatus)
		// Alert rules
		r.Get("/pools/{id}/alert-rules", s.poolHandler.ListAlertRules)
		r.Post("/pools/{id}/alert-rules", s.poolHandler.CreateAlertRule)
		r.Put("/pools/{id}/alert-rules/{rule_id}", s.poolHandler.UpdateAlertRule)
		r.Delete("/pools/{id}/alert-rules/{rule_id}", s.poolHandler.DeleteAlertRule)
	})

	// Browser WebSocket handshakes use the route-scoped query-token exception.
	s.router.With(WebSocketJWTMiddleware(s.jwtSecret)).Get("/ws/dashboard", s.websocketHandler.DashboardWebSocket)
	s.router.With(WebSocketJWTMiddleware(s.jwtSecret)).Get("/ws/logs", s.websocketHandler.LogsWebSocket)
}

// Start starts the API server
func (s *Server) Start() error {
	s.logger.Info("starting API server", "port", s.port)

	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("API server failed: %w", err)
	}

	return nil
}

// Shutdown gracefully shuts down the API server
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("shutting down API server")
	if s.background != nil {
		s.background.Cancel()
	}
	serverErr := s.server.Shutdown(ctx)
	var websocketErr error
	if s.websocketHandler != nil {
		websocketErr = s.websocketHandler.Shutdown(ctx)
	}
	var backgroundErr error
	if s.background != nil {
		backgroundErr = s.background.Wait(ctx)
	}
	var closeErr error
	if serverErr == nil && websocketErr == nil && backgroundErr == nil && s.closeServices != nil {
		closeErr = s.closeServices()
	}
	return errors.Join(serverErr, websocketErr, backgroundErr, closeErr)
}

// SetProxyServer sets the proxy server reference after initialization
func (s *Server) SetProxyServer(ps ProxyServer) {
	s.proxyServer = ps
	if s.proxyControl != nil {
		if ps == nil {
			s.proxyControl.SetDataPlaneController(nil)
		} else {
			s.proxyControl.SetDataPlaneController(ps)
		}
	}
	if s.userHandler == nil {
		return
	}
	if ps == nil {
		s.userHandler.SetOnChange(nil)
		return
	}
	s.userHandler.SetOnChange(ps.RefreshProxyUser)
}

// RefreshProxyUser invalidates one cached pool chain. The next request from
// that proxy user rebuilds the chain from the latest pool membership.
func (s *Server) RefreshProxyUser(w http.ResponseWriter, r *http.Request) {
	if s.proxyServer == nil {
		http.Error(w, `{"error":"proxy server unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	var body struct {
		Username string `json:"username"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.Username) == "" {
		http.Error(w, `{"error":"username is required"}`, http.StatusBadRequest)
		return
	}
	username := strings.TrimSpace(body.Username)
	s.proxyServer.RefreshProxyUser(username)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(fmt.Sprintf(`{"ok":true,"username":%q}`, username)))
}

// ReloadProxyPool reloads the proxy pool from database
//
//	@Summary		Reload proxy pool
//	@Description	Reload proxy pool from database
//	@Tags			proxies
//	@Produce		json
//	@Success		200	{object}	map[string]interface{}	"Reload confirmation"
//	@Failure		500	{object}	models.ErrorResponse
//	@Failure		503	{object}	models.ErrorResponse
//	@Router			/proxies/reload [post]
func (s *Server) ReloadProxyPool(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if s.proxyServer == nil {
		s.logger.Error("proxy server not initialized")
		http.Error(w, "proxy server not available", http.StatusServiceUnavailable)
		return
	}

	s.logger.Info("reloading proxy pool via API request")

	if err := s.proxyServer.ReloadSettings(ctx); err != nil {
		s.logger.Error("failed to reload proxy pool", "error", err)
		http.Error(w, fmt.Sprintf("failed to reload proxy pool: %v", err), http.StatusInternalServerError)
		return
	}

	s.logger.Info("proxy pool reloaded successfully")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"success","message":"Proxy pool reloaded successfully"}`))
}

// serveSwaggerJSON serves the OpenAPI document embedded in the binary.
func (s *Server) serveSwaggerJSON(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if len(docs.SwaggerJSON) == 0 {
		http.Error(w, "swagger spec unavailable", http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(docs.SwaggerJSON)
}

// generateJWTSecret generates a cryptographically secure random JWT secret
func generateJWTSecret() string {
	// Generate 32 random bytes (256 bits)
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to timestamp-based random if crypto/rand fails
		return fmt.Sprintf("fallback-secret-%d", time.Now().UnixNano())
	}

	// Convert to hex string (64 characters)
	return hex.EncodeToString(bytes)
}
