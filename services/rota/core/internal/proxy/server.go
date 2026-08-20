package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/alpkeskin/rota/core/internal/background"
	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/repository"
	"github.com/alpkeskin/rota/core/pkg/logger"
)

// proxyRouter is the core HTTP handler that dispatches incoming proxy requests.
// It replaces the goproxy library with a minimal, zero-dependency implementation
// that supports both HTTP forwarding and HTTPS CONNECT tunneling.
type proxyRouter struct {
	upstream    *UpstreamProxyHandler
	userAuthMw  *UserAuthMiddleware
	rateLimitMw *RateLimitMiddleware
	logger      *logger.Logger
}

// ServeHTTP dispatches incoming requests through the middleware chain
// and routes them to the appropriate handler based on method.
func (p *proxyRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 1. User auth middleware (sets PoolChain in context or falls back to legacy)
	r, reject := p.userAuthMw.HandleRequest(r)
	if reject != nil {
		writeHTTPResponse(w, reject)
		return
	}

	// 2. Rate limit middleware
	r, reject = p.rateLimitMw.HandleRequest(r)
	if reject != nil {
		writeHTTPResponse(w, reject)
		return
	}

	// 3. Dispatch based on method
	if r.Method == http.MethodConnect {
		p.upstream.HandleConnectRequest(w, r)
	} else {
		p.upstream.HandleHTTPRequest(w, r)
	}
}

// writeHTTPResponse translates a middleware-returned *http.Response into
// http.ResponseWriter calls. This bridges the middleware return convention
// (returning *http.Response for reject) with the stdlib interface.
func writeHTTPResponse(w http.ResponseWriter, resp *http.Response) {
	if resp == nil {
		return
	}
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	if resp.Body != nil {
		io.Copy(w, resp.Body) //nolint:errcheck
		resp.Body.Close()
	}
}

// Server represents the proxy server
type Server struct {
	router         *proxyRouter
	server         *http.Server
	logger         *logger.Logger
	port           int
	tracker        *UsageTracker
	handler        *UpstreamProxyHandler
	authMiddleware *AuthMiddleware
	userAuthMw     *UserAuthMiddleware
	rateLimitMw    *RateLimitMiddleware
	proxyRepo      *repository.ProxyRepository
	settingsRepo   *repository.SettingsRepository
	background     *background.Group
	startOnce      sync.Once
}

// New creates a new proxy server instance
func New(
	port int,
	log *logger.Logger,
	db *database.DB,
	proxyRepo *repository.ProxyRepository,
	poolRepo *repository.PoolRepository,
	userRepo *repository.UserRepository,
	settingsRepo *repository.SettingsRepository,
) (*Server, error) {
	// Load settings
	ctx := context.Background()
	settings, err := settingsRepo.GetAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load settings: %w", err)
	}

	// Create proxy selector based on rotation settings
	selector, err := NewProxySelector(proxyRepo, &settings.Rotation)
	if err != nil {
		return nil, fmt.Errorf("failed to create proxy selector: %w", err)
	}

	// Initial refresh of proxy list
	if err := selector.Refresh(ctx); err != nil {
		log.Warn("no proxies available at startup - server will start but requests will fail until proxies are added", "error", err)
	} else {
		log.Info("proxy server initialized successfully")
	}

	// Create usage tracker
	tracker := NewUsageTracker(proxyRepo, log)

	// Create upstream proxy handler
	handler := NewUpstreamProxyHandler(selector, tracker, &settings.Rotation, log)

	// Create middlewares
	authMiddleware := NewAuthMiddleware(settings.Authentication)
	rateLimitMw := NewRateLimitMiddleware(settings.RateLimit)

	// Create user-aware auth middleware (pool-based routing)
	userAuthMw := NewUserAuthMiddleware(userRepo, poolRepo, db, authMiddleware, &settings.Rotation, log)

	// Create the proxy router
	router := &proxyRouter{
		upstream:    handler,
		userAuthMw:  userAuthMw,
		rateLimitMw: rateLimitMw,
		logger:      log,
	}
	serverGroup := background.New(context.Background())

	// WriteTimeout must be 0 for CONNECT tunnels (they are long-lived).
	// HTTP path enforces timeouts via context.
	httpServer := &http.Server{
		Addr:        fmt.Sprintf(":%d", port),
		Handler:     router,
		BaseContext: func(net.Listener) context.Context { return serverGroup.Context() },
		ReadTimeout: time.Duration(settings.Rotation.Timeout) * time.Second,
		IdleTimeout: 60 * time.Second,
	}

	s := &Server{
		router:         router,
		server:         httpServer,
		logger:         log,
		port:           port,
		tracker:        tracker,
		handler:        handler,
		authMiddleware: authMiddleware,
		userAuthMw:     userAuthMw,
		rateLimitMw:    rateLimitMw,
		proxyRepo:      proxyRepo,
		settingsRepo:   settingsRepo,
		background:     serverGroup,
	}

	return s, nil
}

// startBackgroundTasks starts periodic background tasks
func (s *Server) startBackgroundTasks() {
	s.background.Go(func(ctx context.Context) {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				refreshCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				if err := s.handler.refreshSelector(refreshCtx); err != nil {
					s.logger.Error("failed to refresh proxy list", "error", err)
				} else {
					s.logger.Debug("proxy list refreshed")
				}
				cancel()
			case <-ctx.Done():
				return
			}
		}
	})

	s.background.Go(func(ctx context.Context) {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.rateLimitMw.CleanupLimiters()
				s.logger.Debug("cleaned up rate limiters")
			case <-ctx.Done():
				return
			}
		}
	})

	s.background.Go(s.userAuthMw.Run)
}

// Start starts the proxy server
func (s *Server) Start() error {
	s.logger.Info("starting proxy server", "port", s.port)
	s.startOnce.Do(s.startBackgroundTasks)

	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("proxy server failed: %w", err)
	}

	return nil
}

// Shutdown gracefully shuts down the proxy server
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("shutting down proxy server")

	if s.background != nil {
		s.background.Cancel()
	}
	serverErr := s.server.Shutdown(ctx)
	var tunnelErr error
	if s.handler != nil {
		tunnelErr = s.handler.ShutdownTunnels(ctx)
	}
	var backgroundErr error
	if s.background != nil {
		backgroundErr = s.background.Wait(ctx)
	}
	var trackerErr error
	if serverErr == nil && tunnelErr == nil && s.tracker != nil {
		trackerErr = s.tracker.Stop(ctx)
	}
	return errors.Join(serverErr, tunnelErr, backgroundErr, trackerErr)
}

// ReloadSettings reloads settings from database and updates components
func (s *Server) ReloadSettings(ctx context.Context) error {
	settings, err := s.settingsRepo.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to load settings: %w", err)
	}

	// Update middleware settings
	s.authMiddleware.UpdateSettings(settings.Authentication)
	s.rateLimitMw.UpdateSettings(settings.RateLimit)

	// Recreate selector if rotation method changed
	newSelector, err := NewProxySelector(s.proxyRepo, &settings.Rotation)
	if err != nil {
		return fmt.Errorf("failed to create new selector: %w", err)
	}

	if err := newSelector.Refresh(ctx); err != nil {
		return fmt.Errorf("failed to refresh new selector: %w", err)
	}

	s.handler.updateRouting(newSelector, settings.Rotation)

	s.logger.Info("settings reloaded successfully")
	return nil
}

// RefreshProxyUser drops one user-specific PoolChain so the next request sees
// a newly bound proxy immediately instead of waiting for the 30-second refresh.
func (s *Server) RefreshProxyUser(username string) {
	s.userAuthMw.InvalidateUser(username)
}

func (s *Server) RetireProxyUser(ctx context.Context, username string) error {
	s.userAuthMw.InvalidateUser(username)
	return s.handler.RetireProxyUser(ctx, username)
}

// ActivateProxyUser fences the old credential generation and prepares the new
// generation before the control plane advertises the Route as ready.
func (s *Server) ActivateProxyUser(
	ctx context.Context,
	oldUsername string,
	newUsername string,
	expectedProxyID int,
) error {
	if s == nil || s.userAuthMw == nil || s.handler == nil {
		return fmt.Errorf("proxy data plane is unavailable")
	}
	if strings.TrimSpace(oldUsername) != "" {
		if err := s.RetireProxyUser(ctx, oldUsername); err != nil {
			return fmt.Errorf("retire old proxy user: %w", err)
		}
	}
	if err := s.userAuthMw.PrepareManagedUser(ctx, newUsername, expectedProxyID); err != nil {
		return fmt.Errorf("prepare new proxy user: %w", err)
	}
	return nil
}
