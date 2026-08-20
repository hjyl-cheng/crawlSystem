package services

import (
	"context"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/background"
)

func newScheduleTestPoolService() *PoolService {
	return &PoolService{
		tasks:     background.New(context.Background()),
		schedules: make(map[int]poolSchedule),
	}
}

func TestCronIntervalAndBoundary(t *testing.T) {
	if got := cronInterval("*/7 * * * *"); got != 7*time.Minute {
		t.Fatalf("cron interval = %s", got)
	}
	if got := cronInterval("not-supported"); got != 30*time.Minute {
		t.Fatalf("fallback interval = %s", got)
	}

	tests := []struct {
		name     string
		now      time.Time
		interval time.Duration
		want     time.Time
	}{
		{
			name:     "divisor of an hour",
			now:      time.Date(2026, 8, 11, 12, 7, 10, 0, time.UTC),
			interval: 30 * time.Minute,
			want:     time.Date(2026, 8, 11, 12, 30, 0, 0, time.UTC),
		},
		{
			name:     "non-divisor stays aligned to cron minute field",
			now:      time.Date(2026, 8, 11, 12, 7, 10, 0, time.UTC),
			interval: 7 * time.Minute,
			want:     time.Date(2026, 8, 11, 12, 14, 0, 0, time.UTC),
		},
		{
			name:     "non-divisor rolls over at the hour",
			now:      time.Date(2026, 8, 11, 12, 56, 10, 0, time.UTC),
			interval: 7 * time.Minute,
			want:     time.Date(2026, 8, 11, 13, 0, 0, 0, time.UTC),
		},
		{
			name:     "exact boundary is retained",
			now:      time.Date(2026, 8, 11, 12, 14, 0, 0, time.UTC),
			interval: 7 * time.Minute,
			want:     time.Date(2026, 8, 11, 12, 14, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nextCronBoundary(tt.now, tt.interval); !got.Equal(tt.want) {
				t.Fatalf("next boundary = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestScheduledPoolDoesNotFireImmediatelyOrOverlap(t *testing.T) {
	service := newScheduleTestPoolService()
	beforeBoundary := time.Date(2026, 8, 11, 12, 29, 0, 0, time.UTC)
	if service.claimScheduledRun(7, "*/30 * * * *", beforeBoundary) {
		t.Fatal("new schedule fired before its wall-clock boundary")
	}

	atBoundary := time.Date(2026, 8, 11, 12, 30, 0, 0, time.UTC)
	if !service.claimScheduledRun(7, "*/30 * * * *", atBoundary) {
		t.Fatal("schedule did not fire at its boundary")
	}
	if service.claimScheduledRun(7, "*/30 * * * *", atBoundary.Add(time.Minute)) {
		t.Fatal("overlapping schedule was admitted")
	}
	service.finishScheduledRun(7)
	if service.claimScheduledRun(7, "*/30 * * * *", atBoundary.Add(time.Minute)) {
		t.Fatal("completed schedule fired again before the next interval")
	}
	if !service.claimScheduledRun(7, "*/30 * * * *", atBoundary.Add(30*time.Minute)) {
		t.Fatal("schedule did not fire at the next interval")
	}
}

func TestPoolScheduleStatePrunesOnlyIdleDeletedPools(t *testing.T) {
	service := newScheduleTestPoolService()
	service.schedules[1] = poolSchedule{}
	service.schedules[2] = poolSchedule{}
	service.schedules[3] = poolSchedule{inFlight: true}
	service.pruneSchedules(map[int]struct{}{2: {}})
	if _, exists := service.schedules[1]; exists {
		t.Fatal("deleted Pool schedule was retained")
	}
	if _, exists := service.schedules[2]; !exists {
		t.Fatal("live Pool schedule was removed")
	}
	if _, exists := service.schedules[3]; !exists {
		t.Fatal("in-flight Pool schedule was removed")
	}
	service.finishScheduledRun(3)
	service.pruneSchedules(map[int]struct{}{2: {}})
	if _, exists := service.schedules[3]; exists {
		t.Fatal("finished deleted Pool schedule was retained")
	}
}

func TestPoolTasksAreOwnedByBackgroundGroup(t *testing.T) {
	service := newScheduleTestPoolService()
	started := make(chan struct{})
	finished := make(chan struct{})
	if !service.goTask(func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(finished)
	}) {
		t.Fatal("Pool task was rejected")
	}
	<-started
	if err := service.tasks.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("owner returned before Pool task finished")
	}
	if service.goTask(func(context.Context) {}) {
		t.Fatal("Pool task was accepted after owner shutdown")
	}
}
