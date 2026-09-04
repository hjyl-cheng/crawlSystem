package hysteria2

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	hyclient "github.com/apernet/hysteria/core/v2/client"
	"github.com/apernet/hysteria/extras/v2/obfs"
	"github.com/apernet/hysteria/extras/v2/transport/udphop"
)

type ContextDialer func(context.Context, string, string) (net.Conn, error)

type tcpClient interface {
	TCP(string) (net.Conn, error)
	Close() error
}

type runtime struct {
	client  tcpClient
	secrets []string

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

var ErrRuntimeCapacity = errors.New("Hysteria2 runtime capacity reached")
var errRuntimeCacheClosed = errors.New("Hysteria2 runtime cache is closed")
var runtimes = newRuntimeCache(maxCachedRuntimes, startRuntime)

func newRuntimeCache(maxEntries int, start func(Node) (*runtime, error)) *runtimeCache {
	return &runtimeCache{
		entries:    make(map[string]*runtimeEntry),
		maxEntries: maxEntries,
		start:      start,
		now:        time.Now,
	}
}

func NewDialer(credential string) (ContextDialer, error) {
	node, err := Parse(credential)
	if err != nil {
		return nil, err
	}
	return runtimes.dialer(node), nil
}

func NewNodeDialer(node Node) ContextDialer {
	return runtimes.dialer(node)
}

func (c *runtimeCache) dialer(node Node) ContextDialer {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		if network != "tcp" && network != "tcp4" && network != "tcp6" {
			return nil, fmt.Errorf("Hysteria2 nodes only support TCP destinations")
		}
		entry, err := c.acquire(ctx, node)
		if err != nil {
			return nil, err
		}
		conn, err := entry.runtime.dialContext(ctx, address)
		if err != nil {
			c.release(entry)
			return nil, sanitizeError(err, node)
		}
		return &leasedConn{Conn: conn, release: func() { c.release(entry) }}, nil
	}
}

func (c *runtimeCache) acquire(ctx context.Context, node Node) (*runtimeEntry, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errRuntimeCacheClosed
	}
	if entry := c.entries[node.cacheKey()]; entry != nil {
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
	c.entries[node.cacheKey()] = entry
	c.mu.Unlock()

	if evicted != nil {
		if err := evicted.runtime.Close(); err != nil {
			startErr := fmt.Errorf("close evicted Hysteria2 runtime: %w", err)
			c.completeStart(node.cacheKey(), entry, nil, startErr)
			return nil, startErr
		}
	}
	rt, startErr := c.start(node)
	if startErr == nil && rt == nil {
		startErr = errors.New("Hysteria2 runtime factory returned nil")
	}
	c.completeStart(node.cacheKey(), entry, rt, startErr)
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

func (r *runtime) dialContext(ctx context.Context, address string) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	type result struct {
		conn net.Conn
		err  error
	}
	results := make(chan result)
	go func() {
		conn, err := r.client.TCP(address)
		select {
		case results <- result{conn: conn, err: err}:
		case <-ctx.Done():
			if conn != nil {
				_ = conn.Close()
			}
		}
	}()
	select {
	case outcome := <-results:
		return outcome.conn, outcome.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *runtime) Close() error {
	r.closeOnce.Do(func() {
		if r.client != nil {
			r.closeErr = r.client.Close()
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
	c, err := hyclient.NewReconnectableClient(func() (*hyclient.Config, error) {
		return node.clientConfig()
	}, nil, true)
	if err != nil {
		return nil, sanitizeError(err, node)
	}
	return &runtime{client: c, secrets: node.secrets()}, nil
}

func (n Node) clientConfig() (*hyclient.Config, error) {
	serverAddress := n.address
	if n.portSet != "" {
		serverAddress = net.JoinHostPort(n.host, n.portSet)
	}
	var serverAddr net.Addr
	var factory hyclient.ConnFactory
	if n.portSet == "" {
		resolved, err := net.ResolveUDPAddr("udp", serverAddress)
		if err != nil {
			return nil, fmt.Errorf("resolve Hysteria2 server: %w", err)
		}
		serverAddr = resolved
		factory = connFactoryFunc(func(net.Addr) (net.PacketConn, error) {
			return n.wrapPacketConn(net.ListenUDP("udp", nil))
		})
	} else {
		resolved, err := udphop.ResolveUDPHopAddr(serverAddress)
		if err != nil {
			return nil, fmt.Errorf("resolve Hysteria2 port hopping server: %w", err)
		}
		serverAddr = resolved
		factory = connFactoryFunc(func(net.Addr) (net.PacketConn, error) {
			conn, err := udphop.NewUDPHopPacketConn(resolved, udphop.HopIntervalConfig{}, func() (net.PacketConn, error) {
				return net.ListenUDP("udp", nil)
			})
			return n.wrapPacketConn(conn, err)
		})
	}

	serverName := n.sni
	if serverName == "" {
		serverName = n.host
	}
	config := &hyclient.Config{
		ConnFactory: factory,
		ServerAddr:  serverAddr,
		Auth:        n.auth,
		TLSConfig: hyclient.TLSConfig{
			ServerName:         serverName,
			InsecureSkipVerify: n.insecure,
			ECHConfigList:      append([]byte(nil), n.echConfig...),
		},
	}
	if n.pinSHA256 != "" {
		pin := n.pinSHA256
		config.TLSConfig.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("Hysteria2 server did not provide a certificate")
			}
			hash := sha256.Sum256(rawCerts[0])
			if hex.EncodeToString(hash[:]) != pin {
				return errors.New("Hysteria2 server certificate pin does not match")
			}
			return nil
		}
	}
	return config, nil
}

func (n Node) wrapPacketConn(conn net.PacketConn, err error) (net.PacketConn, error) {
	if err != nil {
		return nil, err
	}
	switch n.obfsType {
	case "":
		return conn, nil
	case "salamander":
		wrapped, wrapErr := obfs.WrapPacketConnSalamander(conn, []byte(n.obfsPassword))
		if wrapErr != nil {
			_ = conn.Close()
			return nil, wrapErr
		}
		return wrapped, nil
	case "gecko":
		wrapped, wrapErr := obfs.WrapPacketConnGecko(conn, obfs.GeckoOptions{Password: []byte(n.obfsPassword)})
		if wrapErr != nil {
			_ = conn.Close()
			return nil, wrapErr
		}
		return wrapped, nil
	default:
		_ = conn.Close()
		return nil, errors.New("unsupported Hysteria2 obfuscation type")
	}
}

func (n Node) secrets() []string {
	return []string{n.credential, n.auth, n.obfsPassword}
}

func sanitizeError(err error, node Node) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	message := err.Error()
	for _, secret := range node.secrets() {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[redacted]")
		}
	}
	return errors.New(message)
}

type connFactoryFunc func(net.Addr) (net.PacketConn, error)

func (f connFactoryFunc) New(addr net.Addr) (net.PacketConn, error) { return f(addr) }
