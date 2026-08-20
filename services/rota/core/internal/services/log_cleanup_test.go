package services

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/pkg/logger"
)

type logCleanupSettingsStub struct {
	mu       sync.Mutex
	settings models.Settings
	err      error
	calls    int
}

func (s *logCleanupSettingsStub) GetAll(context.Context) (*models.Settings, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	copy := s.settings
	return &copy, nil
}

func (s *logCleanupSettingsStub) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestLogCleanupLifecycleIsOwnedAndIdempotent(t *testing.T) {
	store := &logCleanupSettingsStub{settings: models.Settings{
		LogRetention: models.LogRetentionSettings{Enabled: false, CleanupIntervalHours: 0},
	}}
	service := NewLogCleanupService(nil, store, logger.New("error"))
	if err := service.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatalf("second start: %v", err)
	}
	if err := service.UpdateSettings(context.Background()); err != nil {
		t.Fatalf("update settings: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for store.callCount() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if calls := store.callCount(); calls < 3 {
		t.Fatalf("settings calls = %d, worker did not consume reload", calls)
	}

	if err := service.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := service.Shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}

func TestLogCleanupStartPropagatesInitialSettingsFailure(t *testing.T) {
	wantErr := errors.New("database unavailable")
	service := NewLogCleanupService(nil, &logCleanupSettingsStub{err: wantErr}, logger.New("error"))
	if err := service.Start(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("start error = %v", err)
	}
	if err := service.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown after failed start: %v", err)
	}
}

func TestCleanupIntervalUsesSafeDefault(t *testing.T) {
	if got := cleanupInterval(0); got != time.Hour {
		t.Fatalf("zero interval = %s", got)
	}
	if got := cleanupInterval(-1); got != time.Hour {
		t.Fatalf("negative interval = %s", got)
	}
	if got := cleanupInterval(6); got != 6*time.Hour {
		t.Fatalf("configured interval = %s", got)
	}
}
