package proxy

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/xraynode"
	"github.com/alpkeskin/rota/core/pkg/logger"
	"github.com/google/uuid"
	proxyDialer "golang.org/x/net/proxy"
)

type handlerRoutingState struct {
	selector ProxySelector
	settings models.RotationSettings
}

type activeTunnel struct {
	client   net.Conn
	upstream net.Conn
	username string
	done     chan struct{}
	doneOnce sync.Once
}

// UpstreamProxyHandler handles requests with upstream proxy rotation
type UpstreamProxyHandler struct {
	routingMu sync.RWMutex
	routing   handlerRoutingState
	tracker   *UsageTracker
	logger    *logger.Logger

	tunnelMu      sync.Mutex
	tunnels       map[*activeTunnel]struct{}
	retiredUsers  map[string]struct{}
	tunnelWG      sync.WaitGroup
	tunnelWait    sync.Once
	tunnelsDone   chan struct{}
	tunnelsClosed bool
}

func (h *UpstreamProxyHandler) routingSnapshot() handlerRoutingState {
	h.routingMu.RLock()
	defer h.routingMu.RUnlock()
	return h.routing
}

func (h *UpstreamProxyHandler) updateRouting(selector ProxySelector, settings models.RotationSettings) {
	h.routingMu.Lock()
	h.routing = handlerRoutingState{selector: selector, settings: settings}
	h.routingMu.Unlock()
}

func (h *UpstreamProxyHandler) refreshSelector(ctx context.Context) error {
	selector := h.routingSnapshot().selector
	if selector == nil {
		return fmt.Errorf("proxy selector is unavailable")
	}
	return selector.Refresh(ctx)
}

// NewUpstreamProxyHandler creates a new upstream proxy handler
func NewUpstreamProxyHandler(
	selector ProxySelector,
	tracker *UsageTracker,
	settings *models.RotationSettings,
	log *logger.Logger,
) *UpstreamProxyHandler {
	var rotationSettings models.RotationSettings
	if settings != nil {
		rotationSettings = *settings
	}
	return &UpstreamProxyHandler{
		routing: handlerRoutingState{
			selector: selector,
			settings: rotationSettings,
		},
		tracker:      tracker,
		logger:       log,
		tunnels:      make(map[*activeTunnel]struct{}),
		retiredUsers: make(map[string]struct{}),
		tunnelsDone:  make(chan struct{}),
	}
}

func (h *UpstreamProxyHandler) beginTunnel(client, upstream net.Conn, username string) (*activeTunnel, bool) {
	tunnel := &activeTunnel{client: client, upstream: upstream, username: username, done: make(chan struct{})}
	h.tunnelMu.Lock()
	defer h.tunnelMu.Unlock()
	if h.tunnelsClosed {
		return nil, false
	}
	if _, retired := h.retiredUsers[username]; username != "" && retired {
		return nil, false
	}
	if h.tunnels == nil {
		h.tunnels = make(map[*activeTunnel]struct{})
	}
	if h.tunnelsDone == nil {
		h.tunnelsDone = make(chan struct{})
	}
	h.tunnels[tunnel] = struct{}{}
	h.tunnelWG.Add(1)
	return tunnel, true
}

func (h *UpstreamProxyHandler) endTunnel(tunnel *activeTunnel) {
	h.tunnelMu.Lock()
	delete(h.tunnels, tunnel)
	h.tunnelMu.Unlock()
	tunnel.doneOnce.Do(func() { close(tunnel.done) })
	h.tunnelWG.Done()
}

func (h *UpstreamProxyHandler) RetireProxyUser(ctx context.Context, username string) error {
	username = strings.TrimSpace(username)
	if username == "" {
		return nil
	}
	h.tunnelMu.Lock()
	if h.retiredUsers == nil {
		h.retiredUsers = make(map[string]struct{})
	}
	h.retiredUsers[username] = struct{}{}
	tunnels := make([]*activeTunnel, 0)
	for tunnel := range h.tunnels {
		if tunnel.username == username {
			tunnels = append(tunnels, tunnel)
		}
	}
	h.tunnelMu.Unlock()

	for _, tunnel := range tunnels {
		_ = tunnel.client.Close()
		_ = tunnel.upstream.Close()
	}
	for _, tunnel := range tunnels {
		select {
		case <-tunnel.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (h *UpstreamProxyHandler) restoreProxyUser(username string) {
	username = strings.TrimSpace(username)
	if username == "" {
		return
	}
	h.tunnelMu.Lock()
	delete(h.retiredUsers, username)
	h.tunnelMu.Unlock()
}

// ShutdownTunnels closes hijacked CONNECT connections and waits for both copy
// directions and their handler to finish.
func (h *UpstreamProxyHandler) ShutdownTunnels(ctx context.Context) error {
	h.tunnelMu.Lock()
	h.tunnelsClosed = true
	if h.tunnelsDone == nil {
		h.tunnelsDone = make(chan struct{})
	}
	done := h.tunnelsDone
	tunnels := make([]*activeTunnel, 0, len(h.tunnels))
	for tunnel := range h.tunnels {
		tunnels = append(tunnels, tunnel)
	}
	h.tunnelMu.Unlock()

	for _, tunnel := range tunnels {
		_ = tunnel.client.Close()
		_ = tunnel.upstream.Close()
	}
	h.tunnelWait.Do(func() {
		go func() {
			h.tunnelWG.Wait()
			close(done)
		}()
	})
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// HandleHTTPRequest handles HTTP requests (non-CONNECT) with upstream proxy rotation.
// It writes the proxied response directly to w.
func (h *UpstreamProxyHandler) HandleHTTPRequest(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	requestID := uuid.New().String()

	h.logger.Debug("handling proxy request",
		"source", "proxy",
		"request_id", requestID,
		"method", r.Method,
		"url", r.URL.String(),
	)

	// Remove hop-by-hop headers
	stripHopByHopHeaders(r.Header)
	routing := h.routingSnapshot()

	// --- Pool-aware path: if a PoolChain was attached by UserAuthMiddleware, use it ---
	reqCtx := r.Context()
	if chain, ok := reqCtx.Value(UserChainContextKey).(*PoolChain); ok && chain != nil {
		resp, proxyID, err := chain.SendWithRetry(r, reqCtx, &routing.settings, h.logger)
		duration := int(time.Since(startTime).Milliseconds())
		if proxyID > 0 {
			h.recordResult(proxyID, "", r.URL.String(), r.Method, resp, err, duration, startTime)
		}
		if err != nil {
			h.logger.Error("pool-chain request failed", "request_id", requestID, "error", err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		copyResponse(w, resp)
		return
	}

	// --- Legacy path: global proxy pool ---
	resp, proxyID, err := h.sendWithRetry(r, r.Context(), routing)
	duration := int(time.Since(startTime).Milliseconds())

	// Record the request
	if proxyID > 0 {
		record := RequestRecord{
			ProxyID:      proxyID,
			ProxyAddress: "",
			RequestedURL: r.URL.String(),
			Method:       r.Method,
			Success:      err == nil && resp != nil,
			ResponseTime: duration,
			Timestamp:    startTime,
		}
		if resp != nil {
			record.StatusCode = resp.StatusCode
		}
		if err != nil {
			record.ErrorMessage = err.Error()
		}
		h.record(record)
	}

	if err != nil {
		h.logger.Error("proxy request failed",
			"source", "proxy",
			"request_id", requestID,
			"error", err,
			"duration_ms", duration,
		)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	h.logger.Debug("proxy request completed",
		"source", "proxy",
		"request_id", requestID,
		"status", resp.StatusCode,
		"duration_ms", duration,
	)

	copyResponse(w, resp)
}

// HandleConnectRequest handles HTTPS CONNECT requests.
// It hijacks the client connection, establishes an upstream tunnel,
// and copies data bidirectionally using splice(2) on Linux.
func (h *UpstreamProxyHandler) HandleConnectRequest(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	host := r.Host

	h.logger.Debug("handling CONNECT request",
		"source", "proxy",
		"host", host,
	)

	// Establish upstream connection (pool-chain or global)
	var upstreamConn net.Conn
	var proxyID int
	var err error
	routing := h.routingSnapshot()

	reqCtx := r.Context()
	if chain, ok := reqCtx.Value(UserChainContextKey).(*PoolChain); ok && chain != nil {
		upstreamConn, proxyID, err = chain.ConnectWithRetry(host, reqCtx, &routing.settings, h.logger)
	} else {
		upstreamConn, proxyID, err = h.connectThroughProxy(host, reqCtx, routing)
	}

	if err != nil {
		h.logger.Error("CONNECT upstream failed",
			"source", "proxy",
			"host", host,
			"error", err,
		)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer upstreamConn.Close()

	// Hijack the client connection from the HTTP server.
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		h.logger.Error("ResponseWriter does not support Hijack")
		http.Error(w, "hijack not supported", http.StatusInternalServerError)
		return
	}

	clientConn, clientBuf, err := hijacker.Hijack()
	if err != nil {
		h.logger.Error("hijack failed", "error", err)
		return
	}
	defer clientConn.Close()
	proxyUser, _ := reqCtx.Value(ProxyUserContextKey).(string)
	tunnel, accepted := h.beginTunnel(clientConn, upstreamConn, proxyUser)
	if !accepted {
		return
	}
	defer func() {
		_ = clientConn.Close()
		_ = upstreamConn.Close()
		h.endTunnel(tunnel)
	}()

	// Send 200 Connection Established to the client.
	if _, err := clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		h.logger.Error("failed to write CONNECT response", "error", err)
		return
	}

	// Drain any buffered data the HTTP server read ahead.
	if clientBuf != nil && clientBuf.Reader.Buffered() > 0 {
		buffered := make([]byte, clientBuf.Reader.Buffered())
		if _, err := io.ReadFull(clientBuf.Reader, buffered); err == nil {
			upstreamConn.Write(buffered) //nolint:errcheck
		}
	}

	// Record the successful CONNECT.
	duration := int(time.Since(startTime).Milliseconds())
	if proxyID > 0 {
		h.record(RequestRecord{
			ProxyID:      proxyID,
			ProxyAddress: "",
			RequestedURL: "CONNECT://" + host,
			Method:       "CONNECT",
			Success:      true,
			ResponseTime: duration,
			StatusCode:   200,
			Timestamp:    startTime,
		})
	}

	// Bidirectional copy — uses splice(2) on Linux for zero-copy.
	BidirectionalCopy(clientConn, upstreamConn)
}

var hopByHopHeaders = []string{
	"Connection",
	"Proxy-Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

func stripHopByHopHeaders(header http.Header) {
	for _, value := range header.Values("Connection") {
		for _, name := range strings.Split(value, ",") {
			if name = strings.TrimSpace(name); name != "" {
				header.Del(name)
			}
		}
	}
	for _, name := range hopByHopHeaders {
		header.Del(name)
	}
}

func closeResponseBody(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}

// copyResponse writes an *http.Response to an http.ResponseWriter.
func copyResponse(w http.ResponseWriter, resp *http.Response) {
	if resp == nil {
		http.Error(w, "empty upstream response", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	stripHopByHopHeaders(resp.Header)
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}

	w.WriteHeader(resp.StatusCode)

	// Use pooled buffer for the body copy
	buf := bufPool.Get().([]byte)
	defer bufPool.Put(buf)
	io.CopyBuffer(w, resp.Body, buf) //nolint:errcheck
}

// sendWithRetry attempts to send the request with retry and fallback logic
func (h *UpstreamProxyHandler) sendWithRetry(req *http.Request, ctx context.Context, routing handlerRoutingState) (*http.Response, int, error) {
	settings := routing.settings
	maxFallbackRetries := settings.FallbackMaxRetries
	if !settings.Fallback {
		maxFallbackRetries = 1
	}

	perProxyRetries := settings.Retries
	if perProxyRetries <= 0 {
		perProxyRetries = 1
	}

	h.logger.Debug("starting proxy selection",
		"source", "proxy",
		"max_fallback_retries", maxFallbackRetries,
		"per_proxy_retries", perProxyRetries,
	)

	var lastErr error
	triedProxies := make(map[int]bool)

	for fallbackAttempt := 0; fallbackAttempt < maxFallbackRetries; fallbackAttempt++ {
		selectedProxy, err := routing.selector.Select(ctx)
		if err != nil {
			return nil, 0, fmt.Errorf("no proxy available: %w", err)
		}

		if triedProxies[selectedProxy.ID] {
			continue
		}
		triedProxies[selectedProxy.ID] = true

		h.logger.Debug("attempting request with proxy",
			"source", "proxy",
			"proxy_id", selectedProxy.ID,
			"proxy_address", selectedProxy.Address,
			"fallback_attempt", fallbackAttempt+1,
		)

		resp, err := h.tryProxyWithRetries(req, ctx, selectedProxy, perProxyRetries, settings)
		if err != nil {
			lastErr = fmt.Errorf("proxy %s failed after %d retries: %w", selectedProxy.Address, perProxyRetries, err)
			h.logger.Warn("proxy failed after all retries",
				"source", "proxy",
				"proxy_id", selectedProxy.ID,
				"error", err,
			)

			h.record(RequestRecord{
				ProxyID:      selectedProxy.ID,
				ProxyAddress: selectedProxy.Address,
				RequestedURL: req.URL.String(),
				Method:       req.Method,
				Success:      false,
				ResponseTime: 0,
				ErrorMessage: err.Error(),
				Timestamp:    time.Now(),
			})

			continue
		}

		return resp, selectedProxy.ID, nil
	}

	return nil, 0, fmt.Errorf("all proxies failed, last error: %w", lastErr)
}

// tryProxyWithRetries attempts to send request through a specific proxy with retries
func (h *UpstreamProxyHandler) tryProxyWithRetries(req *http.Request, ctx context.Context, selectedProxy *models.Proxy, maxRetries int, settings models.RotationSettings) (*http.Response, error) {
	var lastErr error

	for retry := 0; retry < maxRetries; retry++ {
		transport, err := GetOrCreateTransport(selectedProxy)
		if err != nil {
			lastErr = fmt.Errorf("failed to create transport: %w", err)
			continue
		}

		client := &http.Client{
			Transport: transport,
			Timeout:   time.Duration(settings.Timeout) * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if !settings.FollowRedirect {
					return http.ErrUseLastResponse
				}
				if len(via) >= 10 {
					return fmt.Errorf("stopped after 10 redirects")
				}
				return nil
			},
		}

		clonedReq := req.Clone(ctx)
		clonedReq.RequestURI = ""

		resp, err := client.Do(clonedReq)
		if err != nil {
			closeResponseBody(resp)
			lastErr = fmt.Errorf("proxy %s failed: %w", selectedProxy.Address, err)
			if retry < maxRetries-1 {
				continue
			}
		} else {
			return resp, nil
		}
	}

	return nil, lastErr
}

// connectThroughProxy establishes a connection through upstream proxy with retry logic
func (h *UpstreamProxyHandler) connectThroughProxy(host string, ctx context.Context, routing handlerRoutingState) (net.Conn, int, error) {
	startTime := time.Now()

	settings := routing.settings
	maxFallbackRetries := settings.FallbackMaxRetries
	if !settings.Fallback {
		maxFallbackRetries = 1
	}

	perProxyRetries := settings.Retries
	if perProxyRetries <= 0 {
		perProxyRetries = 1
	}

	var lastErr error
	triedProxies := make(map[int]bool)

	for fallbackAttempt := 0; fallbackAttempt < maxFallbackRetries; fallbackAttempt++ {
		selectedProxy, err := routing.selector.Select(ctx)
		if err != nil {
			return nil, 0, fmt.Errorf("no proxy available: %w", err)
		}

		if triedProxies[selectedProxy.ID] {
			continue
		}
		triedProxies[selectedProxy.ID] = true

		conn, err := h.tryConnectWithRetries(ctx, selectedProxy, host, perProxyRetries, settings)
		duration := int(time.Since(startTime).Milliseconds())

		if err != nil {
			lastErr = fmt.Errorf("proxy %s failed after %d retries: %w", selectedProxy.Address, perProxyRetries, err)

			h.record(RequestRecord{
				ProxyID:      selectedProxy.ID,
				ProxyAddress: selectedProxy.Address,
				RequestedURL: "CONNECT://" + host,
				Method:       "CONNECT",
				Success:      false,
				ResponseTime: duration,
				ErrorMessage: err.Error(),
				Timestamp:    startTime,
			})

			continue
		}

		return conn, selectedProxy.ID, nil
	}

	return nil, 0, fmt.Errorf("all proxies failed for CONNECT, last error: %w", lastErr)
}

// tryConnectWithRetries attempts to connect through a specific proxy with retries
func (h *UpstreamProxyHandler) tryConnectWithRetries(ctx context.Context, selectedProxy *models.Proxy, host string, maxRetries int, settings models.RotationSettings) (net.Conn, error) {
	var lastErr error

	for retry := 0; retry < maxRetries; retry++ {
		conn, err := h.connectViaProxy(ctx, selectedProxy, host, settings)
		if err != nil {
			lastErr = fmt.Errorf("proxy %s failed: %w", selectedProxy.Address, err)
			if retry < maxRetries-1 {
				continue
			}
		} else {
			return conn, nil
		}
	}

	return nil, lastErr
}

// connectViaProxy establishes a connection through a specific proxy
func (h *UpstreamProxyHandler) connectViaProxy(ctx context.Context, proxy *models.Proxy, host string, settings models.RotationSettings) (net.Conn, error) {
	switch proxy.Protocol {
	case "socks5":
		var dialer proxyDialer.Dialer
		var err error

		if proxy.Username != nil && *proxy.Username != "" {
			password := ""
			if proxy.Password != nil {
				password = *proxy.Password
			}
			auth := &proxyDialer.Auth{
				User:     *proxy.Username,
				Password: password,
			}
			dialer, err = proxyDialer.SOCKS5("tcp", proxy.Address, auth, proxyDialer.Direct)
		} else {
			dialer, err = proxyDialer.SOCKS5("tcp", proxy.Address, nil, proxyDialer.Direct)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to create SOCKS5 dialer: %w", err)
		}

		conn, err := dialer.Dial("tcp", host)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to %s via SOCKS5 proxy %s: %w", host, proxy.Address, err)
		}

		return conn, nil

	case "socks4", "socks4a":
		return connectViaSocks4(proxy, host)
	case "http", "https":
		return h.connectViaHTTPProxy(proxy, host, settings)
	default:
		if xraynode.IsProtocol(proxy.Protocol) {
			dialer, err := shareNodeDialContext(proxy)
			if err != nil {
				return nil, err
			}
			return dialer(ctx, "tcp", host)
		}
		return nil, fmt.Errorf("unsupported proxy protocol for CONNECT: %s", proxy.Protocol)
	}
}

// connectViaHTTPProxy establishes a connection through HTTP proxy using CONNECT method
func (h *UpstreamProxyHandler) connectViaHTTPProxy(proxy *models.Proxy, host string, settings models.RotationSettings) (net.Conn, error) {
	timeout := time.Duration(settings.Timeout) * time.Second
	if timeout < 60*time.Second {
		timeout = 60 * time.Second
	}

	conn, err := dialHTTPProxyEndpoint(proxy, timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to proxy %s: %w", proxy.Address, err)
	}

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set connection deadline: %w", err)
	}

	connectReq := fmt.Sprintf("CONNECT %s HTTP/1.1\r\n", host)
	connectReq += fmt.Sprintf("Host: %s\r\n", host)

	if proxy.Username != nil && *proxy.Username != "" {
		password := ""
		if proxy.Password != nil {
			password = *proxy.Password
		}
		auth := *proxy.Username + ":" + password
		encoded := base64.StdEncoding.EncodeToString([]byte(auth))
		connectReq += fmt.Sprintf("Proxy-Authorization: Basic %s\r\n", encoded)
	}

	connectReq += "User-Agent: Rota-Proxy/1.0\r\n"
	connectReq += "Proxy-Connection: Keep-Alive\r\n"
	connectReq += "\r\n"

	if _, err = conn.Write([]byte(connectReq)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to send CONNECT request: %w", err)
	}

	connectResponse, err := readCONNECTResponse(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to read CONNECT response: %w", err)
	}

	if connectResponse.StatusCode != http.StatusOK {
		conn.Close()
		return nil, fmt.Errorf("CONNECT request failed: %s", connectResponse.Status)
	}

	// Clear deadline for the tunnel phase.
	if err := conn.SetDeadline(time.Time{}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to clear connection deadline: %w", err)
	}

	return conn, nil
}

func (h *UpstreamProxyHandler) recordResult(proxyID int, proxyAddr, url, method string, resp *http.Response, reqErr error, duration int, ts time.Time) {
	record := RequestRecord{
		ProxyID:      proxyID,
		ProxyAddress: proxyAddr,
		RequestedURL: url,
		Method:       method,
		Success:      reqErr == nil && resp != nil,
		ResponseTime: duration,
		Timestamp:    ts,
	}
	if resp != nil {
		record.StatusCode = resp.StatusCode
	}
	if reqErr != nil {
		record.ErrorMessage = reqErr.Error()
	}
	h.record(record)
}

func (h *UpstreamProxyHandler) record(record RequestRecord) {
	if err := h.tracker.RecordRequest(record); err != nil &&
		!errors.Is(err, ErrUsageQueueFull) &&
		!errors.Is(err, ErrUsageTrackerStopped) {
		h.logger.Error("failed to record proxy usage", "error", err)
	}
}
