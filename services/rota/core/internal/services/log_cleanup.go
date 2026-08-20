package services

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/alpkeskin/rota/core/internal/background"
	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/pkg/logger"
)

type logCleanupSettingsStore interface {
	GetAll(ctx context.Context) (*models.Settings, error)
}

// LogCleanupService handles automatic log cleanup and retention.
type LogCleanupService struct {
	db           *database.DB
	settingsRepo logCleanupSettingsStore
	logger       *logger.Logger

	mu      sync.Mutex
	group   *background.Group
	reload  chan struct{}
	started bool
}

func NewLogCleanupService(
	db *database.DB,
	settingsRepo logCleanupSettingsStore,
	log *logger.Logger,
) *LogCleanupService {
	return &LogCleanupService{
		db:           db,
		settingsRepo: settingsRepo,
		logger:       log,
		reload:       make(chan struct{}, 1),
	}
}

// Start validates initial settings and starts one owned scheduling worker.
func (s *LogCleanupService) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	settings, err := s.settingsRepo.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to get settings: %w", err)
	}

	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	s.started = true
	s.group = background.New(ctx)
	group := s.group
	s.mu.Unlock()

	s.logger.Info("starting log cleanup service")
	group.Go(func(runCtx context.Context) { s.worker(runCtx, settings) })
	return nil
}

// Shutdown cancels and drains the cleanup worker. It is idempotent.
func (s *LogCleanupService) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	group := s.group
	s.mu.Unlock()
	if group == nil {
		return nil
	}
	s.logger.Info("stopping log cleanup service")
	return group.Shutdown(ctx)
}

func (s *LogCleanupService) worker(ctx context.Context, settings *models.Settings) {
	for {
		interval := cleanupInterval(settings.LogRetention.CleanupIntervalHours)
		if settings.LogRetention.Enabled {
			if err := s.runCleanupWithSettings(ctx, settings.LogRetention); err != nil {
				s.logger.Error("cleanup job failed", "error", err)
			}
		} else {
			s.logger.Info("log cleanup is disabled")
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			stopAndDrainTimer(timer)
			s.logger.Info("log cleanup worker context cancelled")
			return
		case <-timer.C:
		case <-s.reload:
			stopAndDrainTimer(timer)
		}

		updated, err := s.settingsRepo.GetAll(ctx)
		if err != nil {
			s.logger.Error("failed to reload log cleanup settings", "error", err)
			continue
		}
		settings = updated
	}
}

func stopAndDrainTimer(timer *time.Timer) {
	if timer != nil && !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func cleanupInterval(hours int) time.Duration {
	if hours <= 0 {
		return time.Hour
	}
	return time.Duration(hours) * time.Hour
}

func (s *LogCleanupService) runCleanup(ctx context.Context) error {
	settings, err := s.settingsRepo.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to get settings: %w", err)
	}
	if !settings.LogRetention.Enabled {
		return nil
	}
	return s.runCleanupWithSettings(ctx, settings.LogRetention)
}

func (s *LogCleanupService) runCleanupWithSettings(ctx context.Context, settings models.LogRetentionSettings) error {
	s.logger.Info("running log cleanup")
	var updateErrors []error
	if err := s.updateRetentionPolicy(ctx, settings); err != nil {
		updateErrors = append(updateErrors, err)
	}
	if err := s.updateCompressionPolicy(ctx, settings); err != nil {
		updateErrors = append(updateErrors, err)
	}
	if err := errors.Join(updateErrors...); err != nil {
		return err
	}
	s.logger.Info("log cleanup completed",
		"retention_days", settings.RetentionDays,
		"compression_after_days", settings.CompressionAfterDays,
	)
	return nil
}

func (s *LogCleanupService) updateRetentionPolicy(ctx context.Context, config models.LogRetentionSettings) error {
	query := `
		SELECT remove_retention_policy('logs', if_exists => true);
		SELECT add_retention_policy('logs', INTERVAL '%d days', if_not_exists => true);
	`
	sql := fmt.Sprintf(query, config.RetentionDays)
	if _, err := s.db.Pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("failed to update retention policy: %w", err)
	}
	s.logger.Info("updated retention policy", "retention_days", config.RetentionDays)
	return nil
}

func (s *LogCleanupService) updateCompressionPolicy(ctx context.Context, config models.LogRetentionSettings) error {
	removeQuery := `SELECT remove_compression_policy('logs', if_exists => true);`
	if _, err := s.db.Pool.Exec(ctx, removeQuery); err != nil {
		return fmt.Errorf("failed to remove compression policy: %w", err)
	}

	addQuery := fmt.Sprintf(`
		SELECT add_compression_policy('logs', INTERVAL '%d days', if_not_exists => true);
	`, config.CompressionAfterDays)
	if _, err := s.db.Pool.Exec(ctx, addQuery); err != nil {
		return fmt.Errorf("failed to add compression policy: %w", err)
	}
	s.logger.Info("updated compression policy", "compression_after_days", config.CompressionAfterDays)
	return nil
}

// UpdateSettings validates the new settings and coalesces a worker wake-up.
func (s *LogCleanupService) UpdateSettings(ctx context.Context) error {
	if _, err := s.settingsRepo.GetAll(ctx); err != nil {
		return fmt.Errorf("failed to get settings: %w", err)
	}
	select {
	case s.reload <- struct{}{}:
	default:
	}
	return nil
}
