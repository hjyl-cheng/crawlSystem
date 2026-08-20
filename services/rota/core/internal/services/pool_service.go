package services

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/alpkeskin/rota/core/internal/background"
	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/repository"
	"github.com/alpkeskin/rota/core/pkg/logger"
	"github.com/gammazero/workerpool"
)

// PoolService manages proxy pools: auto-sync by geo, health checks, rotation state
type PoolService struct {
	poolRepo      *repository.PoolRepository
	proxyRepo     *repository.ProxyRepository
	healthChecker PoolHealthChecker
	logger        *logger.Logger
	tasks         *background.Group

	// per-pool rotation state (roundrobin index, stick counters)
	mu         sync.Mutex
	rrIndex    map[int]int          // pool_id -> current roundrobin index
	stickCur   map[int]int          // pool_id -> current proxy index in stick mode
	stickCount map[int]int          // pool_id -> requests served on current proxy
	schedules  map[int]poolSchedule // pool_id -> scheduled health-check state
	now        func() time.Time
}

type poolSchedule struct {
	cron     string
	nextDue  time.Time
	inFlight bool
}

const (
	poolHealthTaskTimeout = 10 * time.Minute
	newMemberTaskTimeout  = 3 * time.Minute
)

type PoolHealthChecker interface {
	CheckProxyAgainst(ctx context.Context, proxy *models.Proxy, targetURL string) (*models.ProxyTestResult, error)
}

// NewPoolService creates a new PoolService
func NewPoolService(
	poolRepo *repository.PoolRepository,
	proxyRepo *repository.ProxyRepository,
	healthChecker PoolHealthChecker,
	tasks *background.Group,
	log *logger.Logger,
) *PoolService {
	return &PoolService{
		poolRepo:      poolRepo,
		proxyRepo:     proxyRepo,
		healthChecker: healthChecker,
		logger:        log,
		tasks:         tasks,
		rrIndex:       make(map[int]int),
		stickCur:      make(map[int]int),
		stickCount:    make(map[int]int),
		schedules:     make(map[int]poolSchedule),
		now:           time.Now,
	}
}

// Run performs scheduled health checks and Pool synchronization until its owner
// cancels the shared background Group.
func (ps *PoolService) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	ps.logger.Info("pool service started")
	for {
		select {
		case <-ticker.C:
			ps.runScheduledHealthChecks(ctx)
			ps.runAutoSync(ctx)
		case <-ctx.Done():
			ps.logger.Info("pool service stopped")
			return
		}
	}
}

// runScheduledHealthChecks fires health checks for pools whose cron is due
func (ps *PoolService) runScheduledHealthChecks(ctx context.Context) {
	pools, err := ps.poolRepo.GetAllEnabledWithHC(ctx)
	if err != nil {
		ps.logger.Error("failed to load pools for scheduled health check", "error", err)
		return
	}
	livePools := make(map[int]struct{}, len(pools))
	now := ps.now()
	for _, pool := range pools {
		livePools[pool.ID] = struct{}{}
		if !ps.claimScheduledRun(pool.ID, pool.HealthCheckCron, now) {
			continue
		}
		poolCopy := pool
		if !ps.goTask(func(taskCtx context.Context) {
			defer ps.finishScheduledRun(poolCopy.ID)
			checkCtx, cancel := context.WithTimeout(taskCtx, poolHealthTaskTimeout)
			defer cancel()
			if _, err := ps.HealthCheckPool(checkCtx, poolCopy.ID, poolCopy.HealthCheckURL, 20); err != nil {
				ps.logger.Error("scheduled pool health check failed", "pool_id", poolCopy.ID, "error", err)
			}
		}) {
			ps.finishScheduledRun(pool.ID)
		}
	}
	ps.pruneSchedules(livePools)
}

// runAutoSync re-builds membership of auto_sync pools from geo/isp/tag filters
func (ps *PoolService) runAutoSync(ctx context.Context) {
	pools, err := ps.poolRepo.List(ctx)
	if err != nil {
		return
	}
	for _, pool := range pools {
		if pool.AutoSync && pool.Enabled && pool.SyncMode != "manual" {
			poolCopy := pool
			total, newIDs, err := ps.poolRepo.SyncPoolByFilters(ctx, poolCopy)
			if err != nil {
				ps.logger.Warn("auto-sync pool failed", "pool_id", poolCopy.ID, "error", err)
				continue
			}
			// If new proxies joined the pool during this sync, test them
			// immediately instead of waiting up to ~30min for the next scheduled
			// pool health check. Without this, a newly-bought proxy that lands
			// in a pool stays 'idle' for half an hour.
			if len(newIDs) > 0 {
				ps.logger.Info("auto-sync added new proxies to pool",
					"pool_id", poolCopy.ID, "added", len(newIDs), "total", total)
				newCopy := append([]int(nil), newIDs...)
				ps.goTask(func(taskCtx context.Context) {
					hcCtx, cancel := context.WithTimeout(taskCtx, newMemberTaskTimeout)
					defer cancel()
					if err := ps.checkProxiesByIDs(hcCtx, poolCopy.HealthCheckURL, newCopy, 20); err != nil {
						ps.logger.Warn("auto-HC on new pool members failed",
							"pool_id", poolCopy.ID, "error", err)
					}
				})
			}
		}
	}
}

// SyncPool re-builds the membership of a single pool from all its filters (geo+isp+tag).
// Returns total pool size after sync. Newly-added members get an immediate
// scoped health check in the background so they're not left as `idle` when
// the user expands the pool (e.g. adds a new country filter).
func (ps *PoolService) SyncPool(ctx context.Context, poolID int) (int, error) {
	pool, err := ps.poolRepo.GetByID(ctx, poolID)
	if err != nil || pool == nil {
		return 0, fmt.Errorf("pool not found")
	}
	total, newIDs, err := ps.poolRepo.SyncPoolByFilters(ctx, *pool)
	if err != nil {
		return total, err
	}
	if len(newIDs) > 0 {
		ps.logger.Info("manual sync added new proxies to pool",
			"pool_id", poolID, "added", len(newIDs), "total", total)
		newCopy := append([]int(nil), newIDs...)
		poolCopy := *pool
		ps.goTask(func(taskCtx context.Context) {
			hcCtx, cancel := context.WithTimeout(taskCtx, newMemberTaskTimeout)
			defer cancel()
			if err := ps.checkProxiesByIDs(hcCtx, poolCopy.HealthCheckURL, newCopy, 20); err != nil {
				ps.logger.Warn("auto-HC on new pool members failed (manual sync)",
					"pool_id", poolCopy.ID, "error", err)
			}
		})
	}
	return total, nil
}

// checkProxiesByIDs runs a health check on the specified proxy IDs only.
// Used by auto-sync to test newly-added pool members immediately instead of
// waiting for the next scheduled */30 cron tick.
func (ps *PoolService) checkProxiesByIDs(ctx context.Context, checkURL string, proxyIDs []int, workers int) error {
	if len(proxyIDs) == 0 {
		return nil
	}
	if workers <= 0 {
		workers = 20
	}
	if checkURL == "" {
		checkURL = "https://www.youtube.com/watch?v=_xXsXvsYAhA"
	}

	// Load the proxy rows for the given IDs
	rows, err := ps.proxyRepo.GetDB().Pool.Query(ctx, `
		SELECT id, address, protocol, username, password, status
		FROM proxies
		WHERE id = ANY($1::int[])
		  AND status <> 'archived'
	`, proxyIDs)
	if err != nil {
		return fmt.Errorf("failed to load proxies by ids: %w", err)
	}
	defer rows.Close()

	var proxies []*models.Proxy
	for rows.Next() {
		var p models.Proxy
		if err := rows.Scan(&p.ID, &p.Address, &p.Protocol, &p.Username, &p.Password, &p.Status); err != nil {
			return err
		}
		proxies = append(proxies, &p)
	}
	if len(proxies) == 0 {
		return nil
	}

	wp := workerpool.New(workers)
	for _, p := range proxies {
		p := p
		wp.Submit(func() {
			ps.checkOneProxy(ctx, p, checkURL)
		})
	}
	wp.StopWait()

	ps.logger.Info("auto-HC on new pool members completed",
		"count", len(proxies), "url", checkURL)
	return nil
}

// HealthCheckPool tests all proxies in a pool against the pool's custom URL
func (ps *PoolService) HealthCheckPool(ctx context.Context, poolID int, checkURL string, workers int) (*models.PoolHealthCheckResult, error) {
	pool, err := ps.poolRepo.GetByID(ctx, poolID)
	if err != nil || pool == nil {
		return nil, fmt.Errorf("pool not found")
	}

	url := checkURL
	if url == "" {
		url = pool.HealthCheckURL
	}
	if workers <= 0 {
		workers = 20
	}

	proxies, err := ps.poolRepo.GetProxies(ctx, poolID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pool proxies: %w", err)
	}

	eligible := make([]models.PoolProxy, 0, len(proxies))
	for _, candidate := range proxies {
		if candidate.Status == "active" {
			eligible = append(eligible, candidate)
		}
	}
	startedAt := time.Now()
	wp := workerpool.New(workers)
	type resultSlot struct {
		result models.ProxyTestResult
	}
	slots := make([]resultSlot, len(eligible))

	for i, pp := range eligible {
		i := i
		pp := pp
		wp.Submit(func() {
			res := ps.checkOneProxy(ctx, pp.ToProxy(), url)
			slots[i].result = res
		})
	}
	wp.StopWait()

	result := &models.PoolHealthCheckResult{
		PoolID:     poolID,
		PoolName:   pool.Name,
		Checked:    len(eligible),
		StartedAt:  startedAt,
		FinishedAt: time.Now(),
	}
	for _, s := range slots {
		result.Results = append(result.Results, s.result)
		if s.result.Status == "active" {
			result.Active++
		} else {
			result.Failed++
		}
	}

	ps.logger.Info("pool health check done",
		"pool_id", poolID, "checked", result.Checked,
		"active", result.Active, "failed", result.Failed)
	return result, nil
}

// checkOneProxy delegates to Rota's single structured lifecycle checker.
func (ps *PoolService) checkOneProxy(ctx context.Context, p *models.Proxy, targetURL string) models.ProxyTestResult {
	return ps.checkOneProxyTimeout(ctx, p, targetURL, 0)
}

func (ps *PoolService) checkOneProxyTimeout(ctx context.Context, p *models.Proxy, targetURL string, _ time.Duration) models.ProxyTestResult {
	result, err := ps.healthChecker.CheckProxyAgainst(ctx, p, targetURL)
	if err == nil {
		return *result
	}
	message := err.Error()
	return models.ProxyTestResult{
		ID:       p.ID,
		Address:  p.Address,
		Status:   p.Status,
		Error:    &message,
		TestedAt: time.Now(),
	}
}

// HealthCheckPoolWithProgress is like HealthCheckPool but calls progressFn after each proxy finishes.
// progressFn receives (checked_so_far, active_so_far, failed_so_far).
func (ps *PoolService) HealthCheckPoolWithProgress(
	ctx context.Context,
	poolID int,
	checkURL string,
	workers int,
	progressFn func(checked, active, failed int),
) (*models.PoolHealthCheckResult, error) {
	pool, err := ps.poolRepo.GetByID(ctx, poolID)
	if err != nil || pool == nil {
		return nil, fmt.Errorf("pool not found")
	}

	url := checkURL
	if url == "" {
		url = pool.HealthCheckURL
	}
	if workers <= 0 {
		workers = 20
	}

	proxies, err := ps.poolRepo.GetProxies(ctx, poolID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pool proxies: %w", err)
	}

	eligible := make([]models.PoolProxy, 0, len(proxies))
	for _, candidate := range proxies {
		if candidate.Status != "archived" {
			eligible = append(eligible, candidate)
		}
	}
	startedAt := time.Now()
	wp := workerpool.New(workers)
	slots := make([]models.ProxyTestResult, len(eligible))

	var mu sync.Mutex
	checked, active, failed := 0, 0, 0

	for i, pp := range eligible {
		i, pp := i, pp
		wp.Submit(func() {
			res := ps.checkOneProxyTimeout(ctx, pp.ToProxy(), url, 10*time.Second)
			slots[i] = res

			mu.Lock()
			checked++
			if res.Status == "active" {
				active++
			} else {
				failed++
			}
			c, a, f := checked, active, failed
			mu.Unlock()

			if progressFn != nil {
				progressFn(c, a, f)
			}
		})
	}
	wp.StopWait()

	result := &models.PoolHealthCheckResult{
		PoolID:     poolID,
		PoolName:   pool.Name,
		Checked:    len(eligible),
		Active:     active,
		Failed:     failed,
		Results:    slots,
		StartedAt:  startedAt,
		FinishedAt: time.Now(),
	}

	ps.logger.Info("pool health check done",
		"pool_id", poolID, "checked", result.Checked,
		"active", result.Active, "failed", result.Failed,
		"url", url)
	return result, nil
}

func (ps *PoolService) goTask(fn func(context.Context)) bool {
	return ps.tasks != nil && ps.tasks.Go(fn)
}

// cronInterval supports the existing every-N-minutes form and uses the same
// 30-minute fallback for unsupported expressions.
func cronInterval(cron string) time.Duration {
	cron = strings.TrimSpace(cron)
	if strings.HasPrefix(cron, "*/") {
		parts := strings.Fields(cron)
		if len(parts) == 5 {
			var n int
			if _, err := fmt.Sscanf(parts[0][2:], "%d", &n); err == nil && n > 0 {
				return time.Duration(n) * time.Minute
			}
		}
	}
	return 30 * time.Minute
}

func nextCronBoundary(now time.Time, interval time.Duration) time.Time {
	minuteStep := int(interval / time.Minute)
	if minuteStep <= 0 {
		minuteStep = 30
	}
	hour := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, now.Location())
	if now.Second() == 0 && now.Nanosecond() == 0 && now.Minute()%minuteStep == 0 {
		return now
	}
	nextMinute := (now.Minute()/minuteStep + 1) * minuteStep
	if nextMinute >= 60 {
		return hour.Add(time.Hour)
	}
	return hour.Add(time.Duration(nextMinute) * time.Minute)
}

func (ps *PoolService) claimScheduledRun(poolID int, cron string, now time.Time) bool {
	interval := cronInterval(cron)
	ps.mu.Lock()
	defer ps.mu.Unlock()
	state, exists := ps.schedules[poolID]
	if !exists || state.cron != cron {
		state.cron = cron
		state.nextDue = nextCronBoundary(now, interval)
	}
	if state.inFlight || now.Before(state.nextDue) {
		ps.schedules[poolID] = state
		return false
	}
	state.inFlight = true
	state.nextDue = nextCronBoundary(now.Add(time.Nanosecond), interval)
	ps.schedules[poolID] = state
	return true
}

func (ps *PoolService) finishScheduledRun(poolID int) {
	ps.mu.Lock()
	state, exists := ps.schedules[poolID]
	if exists {
		state.inFlight = false
		ps.schedules[poolID] = state
	}
	ps.mu.Unlock()
}

func (ps *PoolService) pruneSchedules(livePools map[int]struct{}) {
	ps.mu.Lock()
	for poolID, state := range ps.schedules {
		if _, exists := livePools[poolID]; !exists && !state.inFlight {
			delete(ps.schedules, poolID)
		}
	}
	ps.mu.Unlock()
}
