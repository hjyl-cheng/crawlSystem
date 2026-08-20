package xraynode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	_ "github.com/xtls/xray-core/app/dispatcher"
	_ "github.com/xtls/xray-core/app/log"
	_ "github.com/xtls/xray-core/app/policy"
	_ "github.com/xtls/xray-core/app/proxyman/inbound"
	_ "github.com/xtls/xray-core/app/proxyman/outbound"
	_ "github.com/xtls/xray-core/app/router"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	_ "github.com/xtls/xray-core/main/json"
	_ "github.com/xtls/xray-core/proxy/shadowsocks"
	_ "github.com/xtls/xray-core/proxy/shadowsocks_2022"
	_ "github.com/xtls/xray-core/proxy/trojan"
	_ "github.com/xtls/xray-core/proxy/vless/outbound"
	_ "github.com/xtls/xray-core/proxy/vmess/outbound"
	_ "github.com/xtls/xray-core/transport/internet/grpc"
	_ "github.com/xtls/xray-core/transport/internet/reality"
	_ "github.com/xtls/xray-core/transport/internet/splithttp"
	_ "github.com/xtls/xray-core/transport/internet/tcp"
	_ "github.com/xtls/xray-core/transport/internet/tls"
	_ "github.com/xtls/xray-core/transport/internet/websocket"
)

// ContextDialer matches http.Transport.DialContext.
type ContextDialer func(context.Context, string, string) (net.Conn, error)

type runtime struct {
	instance *core.Instance
	dialFn   ContextDialer
	closeFn  func() error
	secrets  []string

	closeOnce sync.Once
	closeErr  error
}

type runtimeEntry struct {
	runtime  *runtime
	startErr error
	ready    chan struct{}
	starting bool
	leases   int
	lastUsed time.Time
}

type runtimeCache struct {
	mu         sync.Mutex
	entries    map[string]*runtimeEntry
	maxEntries int
	start      func(Node) (*runtime, error)
	now        func() time.Time
	closed     bool

	closeOnce sync.Once
	closeErr  error
}

const maxCachedRuntimes = 64

var ErrRuntimeCapacity = errors.New("Xray node runtime capacity reached")
var errRuntimeCacheClosed = errors.New("Xray node runtime cache is closed")
var runtimes = newRuntimeCache(maxCachedRuntimes, startRuntime)

func newRuntimeCache(maxEntries int, start func(Node) (*runtime, error)) *runtimeCache {
	return &runtimeCache{
		entries:    make(map[string]*runtimeEntry),
		maxEntries: maxEntries,
		start:      start,
		now:        time.Now,
	}
}

// NewDialer validates a credential and returns a stable cached Xray dialer.
func NewDialer(credential string) (ContextDialer, error) {
	node, err := Parse(credential)
	if err != nil {
		return nil, err
	}
	return runtimes.dialer(node), nil
}

// NewNodeDialer avoids reparsing a Node already returned by Parse.
func NewNodeDialer(node Node) ContextDialer {
	return runtimes.dialer(node)
}

func (c *runtimeCache) dialer(node Node) ContextDialer {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		if network != "tcp" && network != "tcp4" && network != "tcp6" {
			return nil, fmt.Errorf("Xray share nodes only support TCP destinations")
		}
		entry, err := c.acquire(ctx, node)
		if err != nil {
			return nil, err
		}
		conn, err := entry.runtime.dialContext(ctx, network, address)
		if err != nil {
			c.release(entry)
			return nil, err
		}
		return &leasedConn{Conn: conn, release: func() { c.release(entry) }}, nil
	}
}

func (c *runtimeCache) acquire(ctx context.Context, node Node) (*runtimeEntry, error) {
	key := node.cacheKey()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errRuntimeCacheClosed
	}
	if entry := c.entries[key]; entry != nil {
		entry.leases++
		entry.lastUsed = c.now()
		c.mu.Unlock()
		return c.waitForEntry(ctx, entry)
	}

	limit := c.maxEntries
	if limit <= 0 {
		limit = 1
	}
	var evicted *runtimeEntry
	if len(c.entries) >= limit {
		var oldestKey string
		var oldestEntry *runtimeEntry
		for candidateKey, candidate := range c.entries {
			if candidate.starting || candidate.leases != 0 {
				continue
			}
			if oldestEntry == nil || candidate.lastUsed.Before(oldestEntry.lastUsed) {
				oldestKey = candidateKey
				oldestEntry = candidate
			}
		}
		if oldestEntry == nil {
			c.mu.Unlock()
			return nil, ErrRuntimeCapacity
		}
		delete(c.entries, oldestKey)
		evicted = oldestEntry
	}

	entry := &runtimeEntry{ready: make(chan struct{}), starting: true, leases: 1, lastUsed: c.now()}
	c.entries[key] = entry
	c.mu.Unlock()

	if evicted != nil {
		if err := evicted.runtime.Close(); err != nil {
			startErr := fmt.Errorf("close evicted Xray node runtime: %w", err)
			c.completeStart(key, entry, nil, startErr)
			return nil, startErr
		}
	}

	rt, startErr := c.start(node)
	if startErr == nil && rt == nil {
		startErr = errors.New("Xray node runtime factory returned nil")
	}
	c.completeStart(key, entry, rt, startErr)
	return c.waitForEntry(ctx, entry)
}

func (c *runtimeCache) completeStart(key string, entry *runtimeEntry, rt *runtime, startErr error) {
	c.mu.Lock()
	entry.runtime = rt
	entry.startErr = startErr
	entry.starting = false
	if c.closed && entry.startErr == nil {
		entry.startErr = errRuntimeCacheClosed
	}
	if entry.startErr != nil && c.entries[key] == entry {
		delete(c.entries, key)
	}
	close(entry.ready)
	c.mu.Unlock()
}

func (c *runtimeCache) waitForEntry(ctx context.Context, entry *runtimeEntry) (*runtimeEntry, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-entry.ready:
	case <-ctx.Done():
		c.release(entry)
		return nil, ctx.Err()
	}
	if entry.startErr != nil {
		c.release(entry)
		return nil, entry.startErr
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		c.release(entry)
		return nil, errRuntimeCacheClosed
	}
	return entry, nil
}

func (c *runtimeCache) release(entry *runtimeEntry) {
	if entry == nil {
		return
	}
	c.mu.Lock()
	if entry.leases > 0 {
		entry.leases--
	}
	entry.lastUsed = c.now()
	c.mu.Unlock()
}

func (c *runtimeCache) CloseAll() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		entries := make([]*runtimeEntry, 0, len(c.entries))
		for key, entry := range c.entries {
			entries = append(entries, entry)
			delete(c.entries, key)
		}
		c.mu.Unlock()

		var closeErrors []error
		for _, entry := range entries {
			<-entry.ready
			if entry.runtime != nil {
				closeErrors = append(closeErrors, entry.runtime.Close())
			}
		}
		c.closeErr = errors.Join(closeErrors...)
	})
	return c.closeErr
}

// CloseAllRuntimes releases every embedded Xray instance during shutdown.
func CloseAllRuntimes() error { return runtimes.CloseAll() }

type leasedConn struct {
	net.Conn
	release func()
	once    sync.Once
}

func (c *leasedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

func (r *runtime) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if r.dialFn != nil {
		return r.dialFn(ctx, network, address)
	}
	destination, err := xnet.ParseDestination("tcp:" + address)
	if err != nil {
		return nil, fmt.Errorf("invalid Xray node destination")
	}
	return core.Dial(ctx, r.instance, destination)
}

func (r *runtime) Close() error {
	r.closeOnce.Do(func() {
		if r.closeFn != nil {
			r.closeErr = r.closeFn()
		} else if r.instance != nil {
			r.closeErr = r.instance.Close()
		}
		if r.closeErr != nil {
			message := r.closeErr.Error()
			for _, secret := range r.secrets {
				if secret != "" {
					message = strings.ReplaceAll(message, secret, "[redacted]")
				}
			}
			r.closeErr = errors.New(message)
		}
	})
	return r.closeErr
}

func startRuntime(node Node) (*runtime, error) {
	config := map[string]any{
		"log":       map[string]any{"loglevel": "none"},
		"outbounds": []any{node.outbound},
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to build Xray node runtime configuration")
	}
	instance, err := core.StartInstance("json", encoded)
	if err != nil {
		message := err.Error()
		for _, secret := range node.secrets {
			if secret != "" {
				message = strings.ReplaceAll(message, secret, "[redacted]")
			}
		}
		return nil, fmt.Errorf("failed to start %s runtime for %s: %s", node.protocol, node.address, message)
	}
	return &runtime{instance: instance, closeFn: instance.Close, secrets: node.secrets}, nil
}
