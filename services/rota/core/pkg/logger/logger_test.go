package logger

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHookQueueIsBoundedAndShutdownDrainsAcceptedCalls(t *testing.T) {
	log := New("error")
	release := make(chan struct{})
	started := make(chan struct{}, hookWorkerCount)
	var completed atomic.Int64
	log.AddHook(func(context.Context, string, string, map[string]any) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		completed.Add(1)
	})

	for range hookWorkerCount {
		log.callHooks("info", "blocking", nil)
	}
	for range hookWorkerCount {
		<-started
	}
	for range hookQueueSize + 1 {
		log.callHooks("info", "queued", nil)
	}
	if got := log.dropped.Load(); got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}

	close(release)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := log.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if got := completed.Load(); got != hookWorkerCount+hookQueueSize {
		t.Fatalf("completed = %d, want %d", got, hookWorkerCount+hookQueueSize)
	}
	log.callHooks("info", "after shutdown", nil)
}

func TestHookShutdownDeadlineCancelsInFlightAdapter(t *testing.T) {
	log := New("error")
	started := make(chan struct{})
	log.AddHook(func(ctx context.Context, _, _ string, _ map[string]any) {
		close(started)
		<-ctx.Done()
	})
	log.callHooks("info", "blocking", nil)
	<-started

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := log.Shutdown(shutdownCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error = %v", err)
	}
	if err := log.Shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}

func TestHooksCanRegisterAndLogConcurrently(t *testing.T) {
	log := New("error")
	log.AddHook(func(context.Context, string, string, map[string]any) {})
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for range 50 {
				if worker == 0 {
					log.AddHook(func(context.Context, string, string, map[string]any) {})
				}
				log.callHooks("info", "concurrent", []any{"worker", worker})
			}
		}(worker)
	}
	wg.Wait()
	if err := log.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}
