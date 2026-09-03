package proxycontrol

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/pkg/logger"
)

const controlAdvisoryLock int64 = 82_642_117

type ProxyStore interface {
	GetByID(context.Context, int) (*models.Proxy, error)
}

type HealthChecker interface {
	CheckProxy(context.Context, *models.Proxy) (*models.ProxyTestResult, error)
}

type RouteActivationPhase string

const (
	RouteActivationActivating RouteActivationPhase = "activating"
	RouteActivationCommitted  RouteActivationPhase = "committed"
)

type RouteActivationRegistryEntry struct {
	Username string
	ClaimID  string
	Phase    RouteActivationPhase
	Blocked  bool
}

type RouteActivationBeginResult struct {
	AlreadyCommitted bool
}

type DataPlaneController interface {
	RefreshProxyUser(string)
	RetireProxyUser(context.Context, string) error
	RequireRouteActivationRegistry()
	RebuildRouteActivationRegistry(context.Context, []RouteActivationRegistryEntry) error
	BeginProxyUserActivation(
		context.Context,
		string,
		string,
		int,
		string,
		string,
	) (RouteActivationBeginResult, error)
	CommitProxyUserActivation(context.Context, string, string) error
	RetireProxyUserIfClaim(context.Context, string, string) (bool, error)
}

type Manager struct {
	db      *database.DB
	options Options
	logger  *logger.Logger

	invalidateMu sync.RWMutex
	invalidate   func(string)
	dataPlane    DataPlaneController

	reconcileMu       sync.Mutex
	reconcileRequests chan struct{}
}

func (m *Manager) policyForRole(role string) (IdentityPolicy, bool) {
	var resolved IdentityPolicy
	found := false
	for _, policy := range m.options.IdentityPolicies {
		if policy.Role != role {
			continue
		}
		if found {
			return IdentityPolicy{}, false
		}
		resolved = policy
		found = true
	}
	return resolved, found
}

func New(
	db *database.DB,
	_ ProxyStore,
	_ HealthChecker,
	options Options,
	log *logger.Logger,
) *Manager {
	if options.WorkloadScope == "" {
		options.WorkloadScope = "default"
	}
	if options.LeaseDuration <= 0 {
		options.LeaseDuration = time.Minute
	}
	if options.ReconcileInterval <= 0 {
		options.ReconcileInterval = 5 * time.Second
	}
	if options.ResourceSyncInterval <= 0 {
		options.ResourceSyncInterval = 10 * time.Minute
	}
	if options.FailureCooldown <= 0 {
		options.FailureCooldown = 30 * time.Minute
	}
	if options.NetworkCooldown <= 0 {
		options.NetworkCooldown = 5 * time.Minute
	}
	if options.MaxRouteSwitchesPerExecution <= 0 {
		options.MaxRouteSwitchesPerExecution = 2
	}
	if options.MaxNetworkAttemptsPerBusinessRun <= 0 {
		options.MaxNetworkAttemptsPerBusinessRun = 9
	}
	return &Manager{
		db:                db,
		options:           options,
		logger:            log,
		reconcileRequests: make(chan struct{}, 1),
	}
}

// SetCacheInvalidator installs the in-process adapter that drops a Proxy
// User's cached Pool chain after a committed Route assignment change.
func (m *Manager) SetCacheInvalidator(invalidate func(string)) {
	m.invalidateMu.Lock()
	m.invalidate = invalidate
	m.invalidateMu.Unlock()
	m.requestReconcile()
}

func (m *Manager) SetDataPlaneController(dataPlane DataPlaneController) error {
	if dataPlane == nil {
		m.invalidateMu.Lock()
		m.dataPlane = nil
		m.invalidateMu.Unlock()
		return nil
	}
	if err := m.requireEnabled(); err != nil {
		return err
	}
	dataPlane.RequireRouteActivationRegistry()
	ctx, cancel := context.WithTimeout(context.Background(), routeActivationAttemptTimeout)
	defer cancel()
	if err := m.syncResources(ctx); err != nil {
		return fmt.Errorf("initialize Route activation resources: %w", err)
	}
	registry, err := m.loadRouteActivationRegistry(ctx)
	if err != nil {
		return err
	}
	if err := dataPlane.RebuildRouteActivationRegistry(ctx, registry); err != nil {
		return fmt.Errorf("rebuild Route activation registry: %w", err)
	}
	m.invalidateMu.Lock()
	m.dataPlane = dataPlane
	m.invalidateMu.Unlock()
	m.requestReconcile()
	return nil
}

func (m *Manager) Run(ctx context.Context) {
	if !m.options.Enabled {
		return
	}

	if err := m.syncResources(ctx); err != nil {
		m.logError("initial proxy control resource sync failed", err)
	} else if _, err := m.reconcile(ctx); err != nil {
		m.logError("initial proxy control reconciliation failed", err)
	}

	reconcileTicker := time.NewTicker(m.options.ReconcileInterval)
	resourceTicker := time.NewTicker(m.options.ResourceSyncInterval)
	defer reconcileTicker.Stop()
	defer resourceTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-reconcileTicker.C:
			if _, err := m.reconcile(ctx); err != nil {
				m.logError("proxy control reconciliation failed", err)
			}
		case <-m.reconcileRequests:
			if _, err := m.reconcile(ctx); err != nil {
				m.logError("requested proxy control reconciliation failed", err)
			}
		case <-resourceTicker.C:
			if err := m.syncResources(ctx); err != nil {
				m.logError("proxy control resource sync failed", err)
				continue
			}
			if _, err := m.reconcile(ctx); err != nil {
				m.logError("post-sync proxy control reconciliation failed", err)
			}
		}
	}
}

func (m *Manager) requestReconcile() {
	select {
	case m.reconcileRequests <- struct{}{}:
	default:
	}
}

func (m *Manager) NotifyHealthVerdictApplied(proxyID int) {
	if proxyID > 0 {
		m.requestReconcile()
	}
}

func (m *Manager) invalidateUser(username string) bool {
	m.invalidateMu.RLock()
	invalidate := m.invalidate
	dataPlane := m.dataPlane
	m.invalidateMu.RUnlock()
	if dataPlane != nil {
		dataPlane.RefreshProxyUser(username)
		return true
	}
	if invalidate == nil {
		return false
	}
	invalidate(username)
	return true
}

func (m *Manager) retireUser(ctx context.Context, username string) bool {
	m.invalidateMu.RLock()
	dataPlane := m.dataPlane
	invalidate := m.invalidate
	m.invalidateMu.RUnlock()
	if dataPlane != nil {
		if err := dataPlane.RetireProxyUser(ctx, username); err != nil {
			m.logError("retire proxy user failed", err, "proxy_user", username)
			return false
		}
		return true
	}
	if invalidate == nil {
		return false
	}
	invalidate(username)
	return true
}

func (m *Manager) beginUserActivation(
	ctx context.Context,
	oldUsername string,
	newUsername string,
	expectedProxyID int,
	previousClaimID string,
	claimID string,
) (RouteActivationBeginResult, bool) {
	m.invalidateMu.RLock()
	dataPlane := m.dataPlane
	m.invalidateMu.RUnlock()
	if dataPlane == nil {
		return RouteActivationBeginResult{}, false
	}
	result, err := dataPlane.BeginProxyUserActivation(
		ctx, oldUsername, newUsername, expectedProxyID, previousClaimID, claimID,
	)
	if err != nil {
		m.logError(
			"begin proxy user activation failed", err,
			"old_proxy_user", oldUsername,
			"new_proxy_user", newUsername,
			"proxy_id", expectedProxyID,
			"claim_id", claimID,
		)
		return RouteActivationBeginResult{}, false
	}
	return result, true
}

func (m *Manager) commitUserActivation(
	ctx context.Context,
	username string,
	claimID string,
) bool {
	m.invalidateMu.RLock()
	dataPlane := m.dataPlane
	m.invalidateMu.RUnlock()
	if dataPlane == nil {
		return false
	}
	if err := dataPlane.CommitProxyUserActivation(ctx, username, claimID); err != nil {
		m.logError(
			"commit proxy user activation failed", err,
			"proxy_user", username,
			"claim_id", claimID,
		)
		return false
	}
	return true
}

func (m *Manager) retireUserIfClaim(
	ctx context.Context,
	username string,
	claimID string,
) bool {
	m.invalidateMu.RLock()
	dataPlane := m.dataPlane
	m.invalidateMu.RUnlock()
	if dataPlane == nil {
		return false
	}
	retired, err := dataPlane.RetireProxyUserIfClaim(ctx, username, claimID)
	if err != nil {
		m.logError(
			"conditionally retire proxy user failed", err,
			"proxy_user", username,
			"claim_id", claimID,
		)
		return false
	}
	return retired
}

func (m *Manager) requireEnabled() error {
	if !m.options.Enabled {
		return ErrDisabled
	}
	if m.db == nil || m.db.Pool == nil {
		return fmt.Errorf("%w: database is unavailable", ErrDisabled)
	}
	return nil
}

func (m *Manager) logError(message string, err error, attrs ...any) {
	if m.logger == nil {
		return
	}
	values := append([]any{"error", err}, attrs...)
	m.logger.Error(message, values...)
}
