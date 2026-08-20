package logger

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
)

// LogHook is a function that gets called when a log is written
type LogHook func(ctx context.Context, level, message string, attrs map[string]any)

type hookCall struct {
	level   string
	message string
	attrs   map[string]any
}

const (
	hookQueueSize   = 1024
	hookWorkerCount = 2
)

// Logger wraps slog.Logger with additional functionality
type Logger struct {
	*slog.Logger

	mu         sync.RWMutex
	hooks      []LogHook
	hookCh     chan hookCall
	hookCtx    context.Context
	hookCancel context.CancelFunc
	hookWG     sync.WaitGroup
	waitOnce   sync.Once
	done       chan struct{}
	closed     bool

	dropped atomic.Uint64
}

// New creates a new logger with the specified level
func New(level string) *Logger {
	var logLevel slog.Level

	switch level {
	case "debug":
		logLevel = slog.LevelDebug
	case "info":
		logLevel = slog.LevelInfo
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: logLevel,
	}

	handler := slog.NewJSONHandler(os.Stdout, opts)
	logger := slog.New(handler)

	return &Logger{
		Logger: logger,
		hooks:  []LogHook{},
		done:   make(chan struct{}),
	}
}

// AddHook adds a hook that will be called for each log message
func (l *Logger) AddHook(hook LogHook) {
	if hook == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	if l.hookCh == nil {
		l.hookCtx, l.hookCancel = context.WithCancel(context.Background())
		l.hookCh = make(chan hookCall, hookQueueSize)
		for range hookWorkerCount {
			l.hookWG.Add(1)
			go l.hookWorker()
		}
	}
	l.hooks = append(l.hooks, hook)
}

func (l *Logger) hookWorker() {
	defer l.hookWG.Done()
	for {
		select {
		case <-l.hookCtx.Done():
			return
		case call, ok := <-l.hookCh:
			if !ok {
				return
			}
			l.mu.RLock()
			hooks := append([]LogHook(nil), l.hooks...)
			l.mu.RUnlock()
			for _, hook := range hooks {
				if l.hookCtx.Err() != nil {
					return
				}
				hook(l.hookCtx, call.level, call.message, call.attrs)
			}
		}
	}
}

// callHooks queues registered hooks without blocking the logging caller.
func (l *Logger) callHooks(level, message string, args []any) {
	l.mu.RLock()
	if l.closed || len(l.hooks) == 0 || l.hookCh == nil {
		l.mu.RUnlock()
		return
	}

	// Convert args to map
	attrs := make(map[string]any)
	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			if key, ok := args[i].(string); ok {
				attrs[key] = args[i+1]
			}
		}
	}

	select {
	case l.hookCh <- hookCall{level: level, message: message, attrs: attrs}:
	default:
		if dropped := l.dropped.Add(1); dropped%100 == 1 {
			_, _ = fmt.Fprintf(os.Stderr, "logger: hook queue full, dropped %d hook event(s)\n", dropped)
		}
	}
	l.mu.RUnlock()
}

// Shutdown stops accepting hook events and drains accepted work. When ctx
// expires, the hook context is cancelled so context-aware adapters can abort.
func (l *Logger) Shutdown(ctx context.Context) error {
	l.mu.Lock()
	if !l.closed {
		l.closed = true
		if l.hookCh != nil {
			close(l.hookCh)
		}
	}
	l.mu.Unlock()

	l.waitOnce.Do(func() {
		go func() {
			l.hookWG.Wait()
			close(l.done)
		}()
	})
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-l.done:
		if l.hookCancel != nil {
			l.hookCancel()
		}
		return nil
	case <-ctx.Done():
		if l.hookCancel != nil {
			l.hookCancel()
		}
		return ctx.Err()
	}
}

// Info logs an info message
func (l *Logger) Info(msg string, args ...any) {
	l.Logger.Info(msg, args...)
	l.callHooks("info", msg, args)
}

// Warn logs a warning message
func (l *Logger) Warn(msg string, args ...any) {
	l.Logger.Warn(msg, args...)
	l.callHooks("warning", msg, args)
}

// Error logs an error message
func (l *Logger) Error(msg string, args ...any) {
	l.Logger.Error(msg, args...)
	l.callHooks("error", msg, args)
}

// Debug logs a debug message
func (l *Logger) Debug(msg string, args ...any) {
	l.Logger.Debug(msg, args...)
	l.callHooks("info", msg, args)
}

// InfoContext logs an info message with context
func (l *Logger) InfoContext(ctx context.Context, msg string, args ...any) {
	l.Logger.InfoContext(ctx, msg, args...)
	l.callHooks("info", msg, args)
}

// WarnContext logs a warning message with context
func (l *Logger) WarnContext(ctx context.Context, msg string, args ...any) {
	l.Logger.WarnContext(ctx, msg, args...)
	l.callHooks("warning", msg, args)
}

// ErrorContext logs an error message with context
func (l *Logger) ErrorContext(ctx context.Context, msg string, args ...any) {
	l.Logger.ErrorContext(ctx, msg, args...)
	l.callHooks("error", msg, args)
}

// DebugContext logs a debug message with context
func (l *Logger) DebugContext(ctx context.Context, msg string, args ...any) {
	l.Logger.DebugContext(ctx, msg, args...)
	l.callHooks("info", msg, args)
}
