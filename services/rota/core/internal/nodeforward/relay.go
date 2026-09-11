package nodeforward

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const maxControlBytes = 32 * 1024

// Grant is signed by the center, not manufactured from a node's task payload.
// Epoch increases on each route change within a slot; Generation identifies the
// whole channel lease. Country describes the center's selected egress, not locale.
type Grant struct {
	Version       int      `json:"version"`
	Action        string   `json:"action"` // activate, renew, revoke
	NodeID        string   `json:"node_id"`
	BootID        string   `json:"boot_id"`
	Slot          string   `json:"slot"`
	Epoch         int64    `json:"epoch"`
	TaskID        string   `json:"task_id"`
	Generation    int64    `json:"generation"`
	RouteID       string   `json:"route_id"`
	IdentityID    string   `json:"identity_id"`
	EgressCountry string   `json:"egress_country"`
	ExpiresAt     int64    `json:"expires_at_ms"`
	ProxyToken    string   `json:"proxy_token"`
	Upstream      Upstream `json:"upstream"`
}

// Payload is the exact signed JSON byte sequence, base64-encoded. Signing does
// not encrypt credentials; only the node's protected control path may carry it.
type SignedGrant struct {
	Payload   string `json:"payload"`
	Signature string `json:"signature"`
}

type Config struct {
	NodeID            string
	PublicKey         ed25519.PublicKey
	ControlToken      string
	Slots             []string
	MaxTunnelsPerSlot int
}

type route struct {
	grant       Grant
	deadline    time.Time // monotonic lifetime; wall-clock corrections cannot prolong tunnels
	identity    [32]byte
	dial        Dialer
	ctx         context.Context
	cancel      context.CancelFunc
	timer       *time.Timer
	connections map[net.Conn]struct{}
	inFlight    int // includes dials which have not yet returned a socket
}

type slot struct {
	epoch     int64
	lastToken [32]byte
	active    *route
	lastRoute *route // keeps retired ownership until all in-flight handlers finish
}

type Relay struct {
	mu                           sync.Mutex
	nodeID, bootID, controlToken string
	key                          ed25519.PublicKey
	slots                        map[string]*slot
	maxTunnels                   int
	closed                       bool
}

func New(config Config) (*Relay, error) {
	if config.NodeID == "" || len(config.PublicKey) != ed25519.PublicKeySize || len(config.ControlToken) < 32 || len(config.Slots) == 0 || len(config.Slots) > 256 {
		return nil, errors.New("invalid node forward configuration")
	}
	nonce := make([]byte, 24)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	r := &Relay{nodeID: config.NodeID, bootID: hex.EncodeToString(nonce), controlToken: config.ControlToken,
		key: append(ed25519.PublicKey(nil), config.PublicKey...), slots: make(map[string]*slot), maxTunnels: config.MaxTunnelsPerSlot}
	if r.maxTunnels == 0 {
		r.maxTunnels = 64
	}
	if r.maxTunnels < 1 || r.maxTunnels > 1024 {
		return nil, errors.New("invalid tunnel limit")
	}
	for _, name := range config.Slots {
		if name == "" || len(name) > 100 || strings.ContainsAny(name, ":\r\n") || r.slots[name] != nil {
			return nil, errors.New("invalid slot")
		}
		r.slots[name] = &slot{}
	}
	return r, nil
}

func strictJSON(data []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func (r *Relay) verify(signed SignedGrant) (Grant, [32]byte, error) {
	var g Grant
	var identity [32]byte
	data, err := base64.StdEncoding.DecodeString(signed.Payload)
	if err != nil || len(data) > 16*1024 {
		return g, identity, errors.New("invalid grant")
	}
	signature, err := base64.StdEncoding.DecodeString(signed.Signature)
	if err != nil || !ed25519.Verify(r.key, data, signature) {
		return g, identity, errors.New("invalid grant signature")
	}
	if err := strictJSON(data, &g); err != nil {
		return g, identity, errors.New("invalid grant JSON")
	}
	remaining := time.Until(time.UnixMilli(g.ExpiresAt))
	if g.Version != 1 || g.NodeID != r.nodeID || g.BootID != r.bootID || g.Epoch < 1 || g.Epoch > 9007199254740991 || g.Generation < 1 || g.Generation > 9007199254740991 ||
		g.TaskID == "" || g.RouteID == "" || g.IdentityID == "" || len(g.ProxyToken) < 32 || len(g.ProxyToken) > 256 || remaining <= 0 || remaining > 5*time.Minute ||
		(g.Action != "activate" && g.Action != "renew" && g.Action != "revoke") ||
		(g.EgressCountry != "" && (len(g.EgressCountry) != 2 || g.EgressCountry[0] < 'A' || g.EgressCountry[0] > 'Z' || g.EgressCountry[1] < 'A' || g.EgressCountry[1] > 'Z')) {
		return g, identity, errors.New("invalid grant binding or expiry")
	}
	immutable := g
	immutable.Action = ""
	immutable.ExpiresAt = 0
	canonical, _ := json.Marshal(immutable)
	return g, sha256.Sum256(canonical), nil
}

// apply is only reached after control authentication and signature validation.
// Expired/revoked epochs remain tombstoned until process exit. A new boot nonce
// then requires fresh central authorization, so a restart cannot revive a grant.
func (r *Relay) apply(signed SignedGrant) (Grant, error) {
	g, identity, err := r.verify(signed)
	if err != nil {
		return Grant{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.slots[g.Slot]
	if r.closed || s == nil {
		return Grant{}, errors.New("slot unavailable")
	}
	old := s.active
	if old != nil && !time.Now().Before(old.deadline) {
		r.retireLocked(s)
		old = nil
	}
	if g.Action == "revoke" {
		if g.Epoch != s.epoch || (old != nil && old.identity != identity) {
			return Grant{}, errors.New("stale route")
		}
		r.retireLocked(s)
		return g, nil
	}
	if g.Epoch == s.epoch && old != nil && old.identity == identity {
		if g.Action == "renew" && g.ExpiresAt > old.grant.ExpiresAt {
			old.grant.ExpiresAt = g.ExpiresAt
			r.armExpiryLocked(g.Slot, old)
		} else if g.ExpiresAt != old.grant.ExpiresAt {
			return Grant{}, errors.New("stale renewal")
		}
		return g, nil
	}
	if g.Action != "activate" || g.Epoch <= s.epoch {
		return Grant{}, errors.New("stale route")
	}
	tokenHash := sha256.Sum256([]byte(g.ProxyToken))
	if s.epoch > 0 && tokenHash == s.lastToken {
		return Grant{}, errors.New("new route requires a fresh proxy token")
	}
	dial, err := newDialer(g.Upstream)
	if err != nil {
		return Grant{}, err
	}
	r.retireLocked(s)
	ctx, cancel := context.WithCancel(context.Background())
	current := &route{grant: g, identity: identity, dial: dial, ctx: ctx, cancel: cancel, connections: make(map[net.Conn]struct{})}
	s.epoch, s.active = g.Epoch, current
	s.lastRoute = current
	s.lastToken = tokenHash
	r.armExpiryLocked(g.Slot, current)
	return g, nil
}

func (r *Relay) armExpiryLocked(name string, current *route) {
	if current.timer != nil {
		current.timer.Stop()
	}
	current.deadline = time.Now().Add(time.Until(time.UnixMilli(current.grant.ExpiresAt)))
	current.timer = time.AfterFunc(time.Until(current.deadline), func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		s := r.slots[name]
		if s.active == current && !time.Now().Before(current.deadline) {
			r.retireLocked(s)
		}
	})
}

func (r *Relay) retireLocked(s *slot) {
	if s.active == nil {
		return
	}
	old := s.active
	s.active = nil
	old.cancel()
	if old.timer != nil {
		old.timer.Stop()
	}
	for conn := range old.connections {
		_ = conn.Close()
	}
}

func (r *Relay) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for _, s := range r.slots {
		r.retireLocked(s)
	}
}

func sameSecret(a, b string) bool {
	x, y := sha256.Sum256([]byte(a)), sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(x[:], y[:]) == 1
}

func (r *Relay) ControlHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if !sameSecret(request.Header.Get("Authorization"), "Bearer "+r.controlToken) {
			http.Error(w, "unauthorized", 401)
			return
		}
		if request.Method == "GET" && request.URL.Path == "/v1/boot" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"version": 1, "node_id": r.nodeID, "boot_id": r.bootID})
			return
		}
		if request.Method == "POST" && request.URL.Path == "/v1/retire" {
			r.retireHandler(w, request)
			return
		}
		if request.Method != "POST" || request.URL.Path != "/v1/route" {
			http.NotFound(w, request)
			return
		}
		data, err := io.ReadAll(http.MaxBytesReader(w, request.Body, maxControlBytes))
		if err != nil {
			http.Error(w, "control body too large", 413)
			return
		}
		var signed SignedGrant
		if strictJSON(data, &signed) != nil {
			http.Error(w, "invalid envelope", 400)
			return
		}
		g, err := r.apply(signed)
		if err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"version": 1, "action": g.Action, "slot": g.Slot, "epoch": g.Epoch,
			"task_id": g.TaskID, "generation": g.Generation, "route_id": g.RouteID, "identity_id": g.IdentityID,
			"egress_country": g.EgressCountry, "expires_at_ms": g.ExpiresAt})
	})
}

func allowedTarget(target string) bool {
	host, port, err := net.SplitHostPort(target)
	if err != nil || port != "443" || len(host) > 253 {
		return false
	}
	host = strings.ToLower(host)
	for _, suffix := range []string{"youtube.com", "youtube-nocookie.com", "googlevideo.com", "ytimg.com", "ggpht.com", "googleapis.com", "google.com"} {
		if host == suffix || strings.HasSuffix(host, "."+suffix) {
			for _, c := range host {
				if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '.' || c == '-') {
					return false
				}
			}
			return true
		}
	}
	return false
}

// HTTPS CONNECT only: there is no direct dial or center-proxy fallback.
func (r *Relay) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodConnect || !allowedTarget(request.Host) {
		http.Error(w, "target not allowed", 403)
		return
	}
	auth := request.Header.Get("Proxy-Authorization")
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(auth, "Basic "))
	name, password, ok := strings.Cut(string(decoded), ":")
	if !strings.HasPrefix(auth, "Basic ") || err != nil || !ok {
		http.Error(w, "proxy authentication required", 407)
		return
	}
	r.mu.Lock()
	s := r.slots[name]
	var current *route
	if s != nil {
		current = s.active
	}
	if r.closed || current == nil || !sameSecret(password, current.grant.ProxyToken) || !time.Now().Before(current.deadline) {
		r.mu.Unlock()
		http.Error(w, "route unavailable", 407)
		return
	}
	if current.inFlight >= r.maxTunnels {
		r.mu.Unlock()
		http.Error(w, "tunnel capacity reached", 429)
		return
	}
	current.inFlight++
	r.mu.Unlock()
	defer func() { r.mu.Lock(); current.inFlight--; r.mu.Unlock() }()
	ctx, cancel := context.WithTimeout(current.ctx, 20*time.Second)
	stop := context.AfterFunc(request.Context(), cancel)
	defer stop()
	upstream, err := current.dial(ctx, request.Host)
	cancel()
	if err != nil {
		http.Error(w, "upstream connection failed", 502)
		return
	}
	defer upstream.Close()
	r.mu.Lock()
	if s.active != current || current.ctx.Err() != nil || !time.Now().Before(current.deadline) {
		r.mu.Unlock()
		http.Error(w, "route retired", 409)
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		r.mu.Unlock()
		http.Error(w, "CONNECT unavailable", 500)
		return
	}
	client, buffer, err := hijacker.Hijack()
	if err != nil {
		r.mu.Unlock()
		return
	}
	current.connections[client] = struct{}{}
	current.connections[upstream] = struct{}{}
	r.mu.Unlock()
	defer func() {
		_ = client.Close()
		r.mu.Lock()
		delete(current.connections, client)
		delete(current.connections, upstream)
		r.mu.Unlock()
	}()
	if _, err = buffer.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	if err = buffer.Flush(); err != nil {
		return
	}
	// Copy buffered client bytes too; an eager client can pipeline its TLS hello.
	done := make(chan struct{})
	go func() { _, _ = io.Copy(upstream, buffer); _ = upstream.Close(); _ = client.Close(); close(done) }()
	_, _ = io.Copy(client, upstream)
	_ = upstream.Close()
	_ = client.Close()
	<-done
}
