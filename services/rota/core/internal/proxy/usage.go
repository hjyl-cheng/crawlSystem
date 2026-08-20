package proxy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alpkeskin/rota/core/internal/repository"
	"github.com/alpkeskin/rota/core/pkg/logger"
	"github.com/jackc/pgx/v5"
)

const (
	usageQueueSize       = 8192
	usageMaxBatch        = 500
	usageFlushInterval   = time.Second
	usageWriteTimeout    = 15 * time.Second
	usageWarningInterval = 30 * time.Second
)

var (
	ErrUsageQueueFull      = errors.New("usage queue is full")
	ErrUsageTrackerStopped = errors.New("usage tracker is stopped")
)

type usageTrackerConfig struct {
	queueSize       int
	maxBatch        int
	flushInterval   time.Duration
	writeTimeout    time.Duration
	warningInterval time.Duration
}

func defaultUsageTrackerConfig() usageTrackerConfig {
	return usageTrackerConfig{
		queueSize:       usageQueueSize,
		maxBatch:        usageMaxBatch,
		flushInterval:   usageFlushInterval,
		writeTimeout:    usageWriteTimeout,
		warningInterval: usageWarningInterval,
	}
}

// RequestRecord represents a single proxy request.
type RequestRecord struct {
	ProxyID      int
	ProxyAddress string
	RequestedURL string
	Method       string
	Success      bool
	ResponseTime int // milliseconds
	StatusCode   int
	ErrorMessage string
	Timestamp    time.Time
}

// usageBatchWriter is the tracker's private persistence seam. Production uses
// PostgreSQL while tests use an in-memory adapter.
type usageBatchWriter interface {
	WriteBatch(ctx context.Context, records []RequestRecord) error
}

// UsageTracker accepts request statistics without blocking the proxy hot path
// and owns their bounded batching and shutdown drain.
type UsageTracker struct {
	repo   *repository.ProxyRepository
	writer usageBatchWriter
	logger *logger.Logger
	config usageTrackerConfig

	recordCh     chan RequestRecord
	done         chan struct{}
	workerCtx    context.Context
	cancelWorker context.CancelFunc

	acceptMu  sync.RWMutex
	accepting bool
	stopOnce  sync.Once

	droppedRecords atomic.Int64
	failedRecords  atomic.Int64
	lastDropWarn   atomic.Int64
	lastWriteWarn  atomic.Int64

	errMu        sync.Mutex
	lastWriteErr error
}

// NewUsageTracker creates and starts a bounded usage tracker.
func NewUsageTracker(repo *repository.ProxyRepository, log *logger.Logger) *UsageTracker {
	return newUsageTracker(
		repo,
		&postgresUsageBatchWriter{repo: repo},
		log,
		defaultUsageTrackerConfig(),
	)
}

func newUsageTracker(
	repo *repository.ProxyRepository,
	writer usageBatchWriter,
	log *logger.Logger,
	config usageTrackerConfig,
) *UsageTracker {
	config = normalizeUsageTrackerConfig(config)
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	t := &UsageTracker{
		repo:         repo,
		writer:       writer,
		logger:       log,
		config:       config,
		recordCh:     make(chan RequestRecord, config.queueSize),
		done:         make(chan struct{}),
		workerCtx:    workerCtx,
		cancelWorker: cancelWorker,
		accepting:    true,
	}
	go t.run()
	return t
}

func normalizeUsageTrackerConfig(config usageTrackerConfig) usageTrackerConfig {
	defaults := defaultUsageTrackerConfig()
	if config.queueSize <= 0 {
		config.queueSize = defaults.queueSize
	}
	if config.maxBatch <= 0 {
		config.maxBatch = defaults.maxBatch
	}
	if config.flushInterval <= 0 {
		config.flushInterval = defaults.flushInterval
	}
	if config.writeTimeout <= 0 {
		config.writeTimeout = defaults.writeTimeout
	}
	if config.warningInterval <= 0 {
		config.warningInterval = defaults.warningInterval
	}
	return config
}

// RecordRequest submits one record without blocking. A full or stopped tracker
// returns a sentinel error and never falls back to synchronous database I/O.
func (t *UsageTracker) RecordRequest(record RequestRecord) error {
	t.acceptMu.RLock()
	if !t.accepting {
		t.acceptMu.RUnlock()
		return ErrUsageTrackerStopped
	}
	select {
	case t.recordCh <- record:
		t.acceptMu.RUnlock()
		return nil
	default:
		t.acceptMu.RUnlock()
	}

	total := t.droppedRecords.Add(1)
	t.warnRateLimited(
		&t.lastDropWarn,
		"usage queue full; request statistics dropped",
		"dropped_records", total,
		"queue_capacity", t.config.queueSize,
	)
	return ErrUsageQueueFull
}

// Stop stops intake, drains all accepted records, and waits for the worker. If
// the deadline expires, the in-flight database write is canceled before return.
func (t *UsageTracker) Stop(ctx context.Context) error {
	t.stopOnce.Do(func() {
		t.acceptMu.Lock()
		t.accepting = false
		close(t.recordCh)
		t.acceptMu.Unlock()
	})

	select {
	case <-t.done:
		t.cancelWorker()
		return t.persistenceError()
	case <-ctx.Done():
		t.cancelWorker()
		<-t.done
		return errors.Join(ctx.Err(), t.persistenceError())
	}
}

func (t *UsageTracker) run() {
	defer close(t.done)
	ticker := time.NewTicker(t.config.flushInterval)
	defer ticker.Stop()

	batch := make([]RequestRecord, 0, t.config.maxBatch)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		writeCtx, cancel := context.WithTimeout(t.workerCtx, t.config.writeTimeout)
		err := t.writer.WriteBatch(writeCtx, batch)
		cancel()
		if err != nil {
			t.recordWriteFailure(len(batch), err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case record, ok := <-t.recordCh:
			if !ok {
				flush()
				return
			}
			batch = append(batch, record)
			if len(batch) >= t.config.maxBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (t *UsageTracker) recordWriteFailure(recordCount int, err error) {
	total := t.failedRecords.Add(int64(recordCount))
	t.errMu.Lock()
	t.lastWriteErr = err
	t.errMu.Unlock()
	t.warnRateLimited(
		&t.lastWriteWarn,
		"failed to persist usage batch",
		"error", err,
		"batch_records", recordCount,
		"failed_records", total,
	)
}

func (t *UsageTracker) persistenceError() error {
	failed := t.failedRecords.Load()
	if failed == 0 {
		return nil
	}
	t.errMu.Lock()
	lastErr := t.lastWriteErr
	t.errMu.Unlock()
	return fmt.Errorf("failed to persist %d usage records: %w", failed, lastErr)
}

func (t *UsageTracker) warnRateLimited(lastWarn *atomic.Int64, message string, attrs ...any) {
	if t.logger == nil {
		return
	}
	now := time.Now().UnixNano()
	last := lastWarn.Load()
	if last != 0 && time.Duration(now-last) < t.config.warningInterval {
		return
	}
	if lastWarn.CompareAndSwap(last, now) {
		t.logger.Warn(message, attrs...)
	}
}

type proxyUsageAggregate struct {
	requestDelta             int64
	successfulDelta          int64
	successfulResponseTimeMS int64
	trailingFailures         int64
	hadSuccess               bool
}

func aggregateUsageRecords(records []RequestRecord) ([]int, map[int]*proxyUsageAggregate) {
	order := make([]int, 0, len(records))
	aggregates := make(map[int]*proxyUsageAggregate, len(records))
	for _, record := range records {
		aggregate, ok := aggregates[record.ProxyID]
		if !ok {
			aggregate = &proxyUsageAggregate{}
			aggregates[record.ProxyID] = aggregate
			order = append(order, record.ProxyID)
		}
		aggregate.requestDelta++
		if record.Success {
			aggregate.successfulDelta++
			aggregate.successfulResponseTimeMS += int64(record.ResponseTime)
			aggregate.trailingFailures = 0
			aggregate.hadSuccess = true
		} else {
			aggregate.trailingFailures++
		}
	}
	return order, aggregates
}

const updateUsageStatsSQL = `
	UPDATE proxies SET
		requests = requests + $2,
		successful_requests = successful_requests + $3,
		failed_requests = CASE
			WHEN $5 THEN $6
			ELSE failed_requests + $6
		END,
		avg_response_time = CASE
			WHEN $3 > 0 THEN (
				(avg_response_time::BIGINT * successful_requests + $4)
				/ (successful_requests + $3)
			)::INTEGER
			ELSE avg_response_time
		END,
		updated_at = NOW()
	WHERE id = $1
`

type postgresUsageBatchWriter struct {
	repo *repository.ProxyRepository
}

func (w *postgresUsageBatchWriter) WriteBatch(ctx context.Context, records []RequestRecord) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := w.repo.GetDB().Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin usage transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows := make([][]any, 0, len(records))
	for _, record := range records {
		var statusCode *int
		if record.StatusCode > 0 {
			statusCode = &record.StatusCode
		}
		var errorMessage *string
		if record.ErrorMessage != "" {
			errorMessage = &record.ErrorMessage
		}
		rows = append(rows, []any{
			record.ProxyID,
			record.ProxyAddress,
			record.Method,
			record.RequestedURL,
			statusCode,
			record.Success,
			record.ResponseTime,
			errorMessage,
			record.Timestamp,
		})
	}

	inserted, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"proxy_requests"},
		[]string{
			"proxy_id",
			"proxy_address",
			"method",
			"url",
			"status_code",
			"success",
			"response_time",
			"error",
			"timestamp",
		},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copy proxy requests: %w", err)
	}
	if inserted != int64(len(records)) {
		return fmt.Errorf("copy proxy requests: inserted %d of %d rows", inserted, len(records))
	}

	order, aggregates := aggregateUsageRecords(records)
	statements := &pgx.Batch{}
	for _, proxyID := range order {
		aggregate := aggregates[proxyID]
		statements.Queue(
			updateUsageStatsSQL,
			proxyID,
			aggregate.requestDelta,
			aggregate.successfulDelta,
			aggregate.successfulResponseTimeMS,
			aggregate.hadSuccess,
			aggregate.trailingFailures,
		)
	}

	results := tx.SendBatch(ctx, statements)
	for _, proxyID := range order {
		commandTag, execErr := results.Exec()
		if execErr != nil {
			_ = results.Close()
			return fmt.Errorf("update usage stats for proxy %d: %w", proxyID, execErr)
		}
		if commandTag.RowsAffected() != 1 {
			_ = results.Close()
			return fmt.Errorf("update usage stats for proxy %d: updated %d rows", proxyID, commandTag.RowsAffected())
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("close usage statistics batch: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit usage transaction: %w", err)
	}
	return nil
}

// GetRecentRequests retrieves recent requests for a proxy.
func (t *UsageTracker) GetRecentRequests(ctx context.Context, proxyID int, limit int) ([]RequestRecord, error) {
	query := `
		SELECT
			proxy_id, proxy_address, method, url, COALESCE(status_code, 0),
			success, COALESCE(response_time, 0), COALESCE(error, ''), timestamp
		FROM proxy_requests
		WHERE proxy_id = $1
		ORDER BY timestamp DESC
		LIMIT $2
	`

	rows, err := t.repo.GetDB().Pool.Query(ctx, query, proxyID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]RequestRecord, 0, limit)
	for rows.Next() {
		var record RequestRecord
		if err := rows.Scan(
			&record.ProxyID,
			&record.ProxyAddress,
			&record.Method,
			&record.RequestedURL,
			&record.StatusCode,
			&record.Success,
			&record.ResponseTime,
			&record.ErrorMessage,
			&record.Timestamp,
		); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}
