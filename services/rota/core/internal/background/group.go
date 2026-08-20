package background

import (
	"context"
	"sync"
)

// Group owns a set of context-aware background functions. Cancellation closes
// admission before signalling existing functions, so Wait never races a new
// WaitGroup addition.
type Group struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu        sync.Mutex
	accepting bool
	wg        sync.WaitGroup
	waitOnce  sync.Once
	done      chan struct{}
}

func New(parent context.Context) *Group {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &Group{
		ctx:       ctx,
		cancel:    cancel,
		accepting: true,
		done:      make(chan struct{}),
	}
}

// Context returns the Group lifetime context for synchronous work that must
// survive a request disconnect but still stop with the owning service.
func (g *Group) Context() context.Context {
	return g.ctx
}

// Go starts fn when the Group is still accepting work. The Group context is
// the only lifetime context exposed to fn.
func (g *Group) Go(fn func(context.Context)) bool {
	if fn == nil {
		return false
	}

	g.mu.Lock()
	if !g.accepting || g.ctx.Err() != nil {
		g.mu.Unlock()
		return false
	}
	g.wg.Add(1)
	g.mu.Unlock()

	go func() {
		defer g.wg.Done()
		fn(g.ctx)
	}()
	return true
}

// Cancel stops admission before cancelling every accepted function.
func (g *Group) Cancel() {
	g.mu.Lock()
	g.accepting = false
	g.cancel()
	g.mu.Unlock()
}

// Wait cancels the Group if needed and waits for every accepted function.
func (g *Group) Wait(ctx context.Context) error {
	g.Cancel()
	g.waitOnce.Do(func() {
		go func() {
			g.wg.Wait()
			close(g.done)
		}()
	})

	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-g.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown cancels the Group and waits for it to drain.
func (g *Group) Shutdown(ctx context.Context) error {
	return g.Wait(ctx)
}
