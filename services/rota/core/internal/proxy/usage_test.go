package proxy

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

type recordingUsageWriter struct {
	mu        sync.Mutex
	batches   [][]RequestRecord
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
	err       error
}

func (w *recordingUsageWriter) WriteBatch(ctx context.Context, records []RequestRecord) error {
	if w.started != nil {
		w.startOnce.Do(func() { close(w.started) })
	}
	if w.release != nil {
		select {
		case <-w.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if w.err != nil {
		return w.err
	}
	batch := append([]RequestRecord(nil), records...)
	w.mu.Lock()
	w.batches = append(w.batches, batch)
	w.mu.Unlock()
	return nil
}

func (w *recordingUsageWriter) records() []RequestRecord {
	w.mu.Lock()
	defer w.mu.Unlock()
	var records []RequestRecord
	for _, batch := range w.batches {
		records = append(records, batch...)
	}
	return records
}

func testUsageTrackerConfig() usageTrackerConfig {
	return usageTrackerConfig{
		queueSize:       8,
		maxBatch:        100,
		flushInterval:   time.Hour,
		writeTimeout:    time.Second,
		warningInterval: time.Hour,
	}
}

func cleanupUsageTracker(t *testing.T, tracker *UsageTracker) {
	t.Helper()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = tracker.Stop(stopCtx)
	})
}

func TestAggregateUsageRecords(t *testing.T) {
	records := []RequestRecord{
		{ProxyID: 7, Success: false},
		{ProxyID: 3, Success: true, ResponseTime: 25},
		{ProxyID: 7, Success: false},
		{ProxyID: 7, Success: true, ResponseTime: 100},
		{ProxyID: 3, Success: true, ResponseTime: 75},
		{ProxyID: 7, Success: false},
	}

	order, aggregates := aggregateUsageRecords(records)
	if !reflect.DeepEqual(order, []int{7, 3}) {
		t.Fatalf("first-seen order = %v, want [7 3]", order)
	}
	if got := aggregates[7]; got.requestDelta != 4 ||
		got.successfulDelta != 1 ||
		got.successfulResponseTimeMS != 100 ||
		got.trailingFailures != 1 ||
		!got.hadSuccess {
		t.Fatalf("proxy 7 aggregate = %+v", got)
	}
	if got := aggregates[3]; got.requestDelta != 2 ||
		got.successfulDelta != 2 ||
		got.successfulResponseTimeMS != 100 ||
		got.trailingFailures != 0 ||
		!got.hadSuccess {
		t.Fatalf("proxy 3 aggregate = %+v", got)
	}
}

func TestUsageTrackerStopFlushesAcceptedRecords(t *testing.T) {
	writer := &recordingUsageWriter{}
	tracker := newUsageTracker(nil, writer, nil, testUsageTrackerConfig())
	cleanupUsageTracker(t, tracker)

	for proxyID := 1; proxyID <= 3; proxyID++ {
		if err := tracker.RecordRequest(RequestRecord{ProxyID: proxyID}); err != nil {
			t.Fatalf("record proxy %d: %v", proxyID, err)
		}
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tracker.Stop(stopCtx); err != nil {
		t.Fatalf("stop tracker: %v", err)
	}

	got := writer.records()
	if len(got) != 3 || got[0].ProxyID != 1 || got[1].ProxyID != 2 || got[2].ProxyID != 3 {
		t.Fatalf("flushed records = %+v", got)
	}
	if err := tracker.RecordRequest(RequestRecord{ProxyID: 4}); !errors.Is(err, ErrUsageTrackerStopped) {
		t.Fatalf("record after stop error = %v, want %v", err, ErrUsageTrackerStopped)
	}
	if err := tracker.Stop(stopCtx); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

func TestUsageTrackerQueueOverflowIsNonBlockingAndCounted(t *testing.T) {
	writer := &recordingUsageWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	config := testUsageTrackerConfig()
	config.queueSize = 1
	config.maxBatch = 1
	config.writeTimeout = 5 * time.Second
	tracker := newUsageTracker(nil, writer, nil, config)

	var releaseOnce sync.Once
	releaseWriter := func() { releaseOnce.Do(func() { close(writer.release) }) }
	t.Cleanup(func() {
		releaseWriter()
		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = tracker.Stop(stopCtx)
	})

	if err := tracker.RecordRequest(RequestRecord{ProxyID: 1}); err != nil {
		t.Fatalf("record first: %v", err)
	}
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("writer did not start")
	}
	if err := tracker.RecordRequest(RequestRecord{ProxyID: 2}); err != nil {
		t.Fatalf("record second: %v", err)
	}

	started := time.Now()
	err := tracker.RecordRequest(RequestRecord{ProxyID: 3})
	if !errors.Is(err, ErrUsageQueueFull) {
		t.Fatalf("overflow error = %v, want %v", err, ErrUsageQueueFull)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("overflow submission blocked for %s", elapsed)
	}
	if got := tracker.droppedRecords.Load(); got != 1 {
		t.Fatalf("dropped records = %d, want 1", got)
	}

	releaseWriter()
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tracker.Stop(stopCtx); err != nil {
		t.Fatalf("stop tracker: %v", err)
	}
	got := writer.records()
	if len(got) != 2 || got[0].ProxyID != 1 || got[1].ProxyID != 2 {
		t.Fatalf("accepted records = %+v", got)
	}
}

func TestUsageTrackerStopCancelsWriterAtDeadline(t *testing.T) {
	writer := &recordingUsageWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	config := testUsageTrackerConfig()
	config.maxBatch = 1
	config.writeTimeout = time.Minute
	tracker := newUsageTracker(nil, writer, nil, config)
	cleanupUsageTracker(t, tracker)

	if err := tracker.RecordRequest(RequestRecord{ProxyID: 1}); err != nil {
		t.Fatalf("record request: %v", err)
	}
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("writer did not start")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := tracker.Stop(stopCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop error = %v, want deadline exceeded", err)
	}
	select {
	case <-tracker.done:
	default:
		t.Fatal("tracker worker remained active after Stop returned")
	}
}
