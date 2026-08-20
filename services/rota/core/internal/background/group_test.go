package background

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGroupShutdownCancelsAndWaitsForAcceptedWork(t *testing.T) {
	group := New(context.Background())
	started := make(chan struct{})
	finished := make(chan struct{})
	if !group.Go(func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(finished)
	}) {
		t.Fatal("initial work was rejected")
	}
	<-started

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := group.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("shutdown returned before accepted work finished")
	}
	if group.Go(func(context.Context) {}) {
		t.Fatal("work was accepted after shutdown")
	}
}

func TestGroupWaitHonorsCallerDeadline(t *testing.T) {
	group := New(context.Background())
	release := make(chan struct{})
	if !group.Go(func(context.Context) { <-release }) {
		t.Fatal("work was rejected")
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := group.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait error = %v", err)
	}
	close(release)
	if err := group.Wait(context.Background()); err != nil {
		t.Fatalf("second wait: %v", err)
	}
}

func TestGroupContextIsCancelledByShutdown(t *testing.T) {
	group := New(context.Background())
	groupCtx := group.Context()
	if err := group.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case <-groupCtx.Done():
	default:
		t.Fatal("group context remained active after shutdown")
	}
}
