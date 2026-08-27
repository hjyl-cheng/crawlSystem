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

type DataPlaneController interface {
	RefreshProxyUser(string)
	RetireProxyUser(context.Context, string) error
	ActivateProxyUser(context.Context, string, string, int) error
}

type Manager struct {
	db            *database.DB
	proxyStore    ProxyStore
	healthChecker HealthChecker
	options       Options
	logger        *logger.Logger

	invalidateMu sync.RWMutex
	invalidate   func(string)
	dataPlane    DataPlaneController

	reconcileRequests chan struct{}
	healthRequests    chan int
	healthMu          sync.Mutex
	healthPending     map[int]struct{}
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
	proxyStore ProxyStore,
	healthChecker HealthChecker,
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
		proxyStore:        proxyStore,
		healthChecker:     healthChecker,
		options:           options,
		logger:            log,
		reconcileRequests: make(chan struct{}, 1),
		healthRequests:    make(chan int, 1024),
		healthPending:     make(map[int]struct{}),
	}
}

// SetCacheInvalidator installs the in-process adapter that drops a Proxy
// User's cached Pool chain after a committed binding change.
func (m *Manager) SetCacheInvalidator(invalidate func(string)) {
	m.invalidateMu.Lock()
	m.invalidate = invalidate
	m.invalidateMu.Unlock()
	m.requestReconcile()
}

func (m *Manager) SetDataPlaneController(dataPlane DataPlaneController) {
	m.invalidateMu.Lock()
	m.dataPlane = dataPlane
	m.invalidateMu.Unlock()
	m.requestReconcile()
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

	var healthWorkers sync.WaitGroup
	for range 5 {
		healthWorkers.Add(1)
		go func() {
			defer healthWorkers.Done()
			m.runHealthWorker(ctx)
		}()
	}
	defer healthWorkers.Wait()
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

func (m *Manager) requestHealthCheck(proxyID int) bool {
	if proxyID <= 0 || m.healthChecker == nil || m.proxyStore == nil {
		return false
	}
	m.healthMu.Lock()
	if _, found := m.healthPending[proxyID]; found {
		m.healthMu.Unlock()
		return true
	}
	m.healthPending[proxyID] = struct{}{}
	m.healthMu.Unlock()

	select {
	case m.healthRequests <- proxyID:
		return true
	default:
		m.healthMu.Lock()
		delete(m.healthPending, proxyID)
		m.healthMu.Unlock()
		return false
	}
}

func (m *Manager) runHealthWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case proxyID := <-m.healthRequests:
			m.runRequestedHealthCheck(ctx, proxyID)
			m.healthMu.Lock()
			delete(m.healthPending, proxyID)
			m.healthMu.Unlock()
			m.requestReconcile()
		}
	}
}

func (m *Manager) runRequestedHealthCheck(ctx context.Context, proxyID int) {
	proxy, err := m.proxyStore.GetByID(ctx, proxyID)
	if err != nil {
		m.logError("load proxy for requested self-check failed", err, "proxy_id", proxyID)
		return
	}
	if proxy == nil || proxy.Status == "archived" {
		return
	}
	if _, err := m.healthChecker.CheckProxy(ctx, proxy); err != nil {
		m.logError("requested proxy self-check failed", err, "proxy_id", proxyID)
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

func (m *Manager) activateUser(
	ctx context.Context,
	oldUsername string,
	newUsername string,
	expectedProxyID int,
) bool {
	m.invalidateMu.RLock()
	dataPlane := m.dataPlane
	m.invalidateMu.RUnlock()
	if dataPlane == nil {
		return false
	}
	if err := dataPlane.ActivateProxyUser(
		ctx, oldUsername, newUsername, expectedProxyID,
	); err != nil {
		m.logError(
			"activate proxy user failed", err,
			"old_proxy_user", oldUsername,
			"new_proxy_user", newUsername,
			"proxy_id", expectedProxyID,
		)
		return false
	}
	return true
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
