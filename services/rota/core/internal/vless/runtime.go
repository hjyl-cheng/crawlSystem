package vless

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
	_ "github.com/xtls/xray-core/proxy/vless/outbound"
	_ "github.com/xtls/xray-core/transport/internet/reality"
	_ "github.com/xtls/xray-core/transport/internet/tcp"
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

var ErrRuntimeCapacity = errors.New("VLESS runtime capacity reached")

var errRuntimeCacheClosed = errors.New("VLESS runtime cache is closed")

var runtimes = newRuntimeCache(maxCachedRuntimes, startRuntime)

func newRuntimeCache(maxEntries int, start func(Node) (*runtime, error)) *runtimeCache {
	return &runtimeCache{
		entries:    make(map[string]*runtimeEntry),
		maxEntries: maxEntries,
		start:      start,
		now:        time.Now,
	}
}

// NewDialer returns a stable dialer that leases the current cached Xray runtime
// on every TCP connection. Raw credentials are never map keys or error text.
func NewDialer(credential string) (ContextDialer, error) {
	node, err := Parse(credential)
	if err != nil {
		return nil, err
	}
	return runtimes.dialer(node), nil
}

func (c *runtimeCache) dialer(node Node) ContextDialer {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		if network != "tcp" && network != "tcp4" && network != "tcp6" {
			return nil, fmt.Errorf("VLESS only supports TCP destinations")
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

	entry := &runtimeEntry{
		ready:    make(chan struct{}),
		starting: true,
		leases:   1,
		lastUsed: c.now(),
	}
	c.entries[key] = entry
	c.mu.Unlock()

	if evicted != nil {
		if err := evicted.runtime.Close(); err != nil {
			startErr := fmt.Errorf("close evicted VLESS runtime: %w", err)
			c.completeStart(key, entry, nil, startErr)
			return nil, startErr
		}
	}

	rt, startErr := c.start(node)
	if startErr == nil && rt == nil {
		startErr = errors.New("VLESS runtime factory returned nil")
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
			if entry.runtime == nil {
				continue
			}
			if err := entry.runtime.Close(); err != nil {
				closeErrors = append(closeErrors, err)
			}
		}
		c.closeErr = errors.Join(closeErrors...)
	})
	return c.closeErr
}

// CloseAllRuntimes closes every cached Xray instance during process shutdown.
func CloseAllRuntimes() error {
	return runtimes.CloseAll()
}

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
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("VLESS only supports TCP destinations")
	}
	destination, err := xnet.ParseDestination("tcp:" + address)
	if err != nil {
		return nil, fmt.Errorf("invalid VLESS destination")
	}
	if r.dialFn != nil {
		return r.dialFn(ctx, network, address)
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

type xrayConfig struct {
	Log       logConfig        `json:"log"`
	Outbounds []outboundConfig `json:"outbounds"`
}

type logConfig struct {
	LogLevel string `json:"loglevel"`
}

type outboundConfig struct {
	Protocol       string         `json:"protocol"`
	Settings       vlessSettings  `json:"settings"`
	StreamSettings streamSettings `json:"streamSettings"`
}

type vlessSettings struct {
	VNext []vnext `json:"vnext"`
}

type vnext struct {
	Address string      `json:"address"`
	Port    int         `json:"port"`
	Users   []vlessUser `json:"users"`
}

type vlessUser struct {
	ID         string `json:"id"`
	Encryption string `json:"encryption"`
	Flow       string `json:"flow"`
}

type streamSettings struct {
	Network         string          `json:"network"`
	Security        string          `json:"security"`
	RealitySettings realitySettings `json:"realitySettings"`
}

type realitySettings struct {
	ServerName  string `json:"serverName"`
	Fingerprint string `json:"fingerprint"`
	PublicKey   string `json:"publicKey"`
	ShortID     string `json:"shortId,omitempty"`
}

func startRuntime(node Node) (*runtime, error) {
	config := xrayConfig{
		Log: logConfig{LogLevel: "none"},
		Outbounds: []outboundConfig{{
			Protocol: "vless",
			Settings: vlessSettings{VNext: []vnext{{
				Address: node.host,
				Port:    node.port,
				Users: []vlessUser{{
					ID:         node.id,
					Encryption: requiredEncryption,
					Flow:       requiredFlow,
				}},
			}}},
			StreamSettings: streamSettings{
				Network:  requiredNetwork,
				Security: requiredSecurity,
				RealitySettings: realitySettings{
					ServerName:  node.sni,
					Fingerprint: requiredFingerprint,
					PublicKey:   node.publicKey,
					ShortID:     node.shortID,
				},
			},
		}},
	}

	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to build VLESS runtime configuration")
	}
	instance, err := core.StartInstance("json", encoded)
	if err != nil {
		message := err.Error()
		for _, secret := range []string{node.credential, node.id} {
			message = strings.ReplaceAll(message, secret, "[redacted]")
		}
		return nil, fmt.Errorf("failed to start VLESS runtime for %s: %s", node.address, message)
	}
	return &runtime{
		instance: instance,
		closeFn:  instance.Close,
		secrets:  []string{node.credential, node.id},
	}, nil
}
