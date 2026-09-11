package nodeforward

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type harness struct {
	relay          *Relay
	key            ed25519.PrivateKey
	proxy, control *httptest.Server
	grant          Grant
}

func setup(t *testing.T) *harness {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	r, err := New(Config{NodeID: "node-a", PublicKey: public, ControlToken: strings.Repeat("C", 32), Slots: []string{"worker-1", "worker-2"}, MaxTunnelsPerSlot: 2})
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{relay: r, key: private, proxy: httptest.NewServer(r), control: httptest.NewServer(r.ControlHandler())}
	h.grant = Grant{Version: 1, Action: "activate", NodeID: "node-a", BootID: r.bootID, Slot: "worker-1", Epoch: 1,
		TaskID: "channel-plan-a", Generation: 1, RouteID: "route-us", IdentityID: "identity-us", EgressCountry: "US",
		ExpiresAt: time.Now().Add(time.Minute).UnixMilli(), ProxyToken: strings.Repeat("A", 32)}
	t.Cleanup(func() { r.Close(); h.proxy.Close(); h.control.Close() })
	return h
}

func sign(t *testing.T, key ed25519.PrivateKey, g Grant) SignedGrant {
	t.Helper()
	data, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	return SignedGrant{Payload: base64.StdEncoding.EncodeToString(data), Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(key, data))}
}

func (h *harness) send(t *testing.T, signed SignedGrant, token string) (int, string) {
	t.Helper()
	body, _ := json.Marshal(signed)
	request, _ := http.NewRequest("POST", h.control.URL+"/v1/route", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(data)
}

func (h *harness) apply(t *testing.T, grant Grant, want int) {
	t.Helper()
	status, body := h.send(t, sign(t, h.key, grant), strings.Repeat("C", 32))
	if status != want {
		t.Fatalf("control status %d, want %d: %s", status, want, body)
	}
	if strings.Contains(body, grant.ProxyToken) || (grant.Upstream.Password != "" && strings.Contains(body, grant.Upstream.Password)) {
		t.Fatal("control response leaked credential")
	}
}

// A local proxy which never resolves or connects to YouTube. Its marker proves
// which upstream was used, including bytes pipelined after CONNECT's header.
func markedProxy(t *testing.T, marker string) (string, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	var sockets []net.Conn
	// One handler at a time is sufficient for these sequential fixture connects.
	var socketMu = make(chan struct{}, 1)
	socketMu <- struct{}{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "CONNECT" || r.Host != "www.youtube.com:443" {
			http.Error(w, "bad target", 400)
			return
		}
		if r.Header.Get("Proxy-Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte("test-user:p@ss:$word")) {
			http.Error(w, "bad auth", 407)
			return
		}
		c, buffer, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		<-socketMu
		sockets = append(sockets, c)
		socketMu <- struct{}{}
		defer c.Close()
		_, _ = buffer.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n" + marker)
		_ = buffer.Flush()
		_, _ = io.Copy(c, buffer)
	}))
	t.Cleanup(func() {
		<-socketMu
		for _, c := range sockets {
			_ = c.Close()
		}
		socketMu <- struct{}{}
		s.Close()
	})
	return strings.TrimPrefix(s.URL, "http://"), &calls
}

func upstream(address string) Upstream {
	return Upstream{Protocol: "http", Address: address, Username: "test-user", Password: "p@ss:$word"}
}

func connect(t *testing.T, proxyURL, slot, token, early string, want int) (net.Conn, *bufio.Reader) {
	t.Helper()
	c, err := net.DialTimeout("tcp", strings.TrimPrefix(proxyURL, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	_, err = fmt.Fprintf(c, "CONNECT www.youtube.com:443 HTTP/1.1\r\nHost: www.youtube.com:443\r\nProxy-Authorization: Basic %s\r\n\r\n%s", base64.StdEncoding.EncodeToString([]byte(slot+":"+token)), early)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(c)
	response, err := http.ReadResponse(reader, &http.Request{Method: "CONNECT"})
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != want {
		t.Fatalf("proxy status %d, want %d", response.StatusCode, want)
	}
	return c, reader
}

func readText(t *testing.T, r io.Reader, want string) {
	t.Helper()
	data := make([]byte, len(want))
	if _, err := io.ReadFull(r, data); err != nil {
		t.Fatal(err)
	}
	if string(data) != want {
		t.Fatalf("received %q, want %q", data, want)
	}
}

func expectClosed(t *testing.T, r io.Reader) {
	t.Helper()
	var b [1]byte
	_, err := r.Read(b[:])
	if err == nil {
		t.Fatal("retired tunnel remained open")
	}
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("retired tunnel only stopped on test timeout")
	}
}

func TestSelectedUpstreamSwitchAndLeaseBinding(t *testing.T) {
	h := setup(t)
	a, ca := markedProxy(t, "US")
	b, cb := markedProxy(t, "BR")
	g := h.grant
	g.Upstream = upstream(a)
	h.apply(t, g, 200)
	h.apply(t, g, 200) // lost activation acknowledgement
	_, reader := connect(t, h.proxy.URL, g.Slot, g.ProxyToken, "hello", 200)
	readText(t, reader, "UShello")
	old := g
	g.Epoch = 2
	h.apply(t, g, 409) // a fresh epoch must not reuse the old worker password
	g.RouteID = "route-br"
	g.IdentityID = "identity-br"
	g.EgressCountry = "BR"
	g.ProxyToken = strings.Repeat("B", 32)
	g.Upstream = upstream(b)
	h.apply(t, g, 200)
	expectClosed(t, reader)
	connect(t, h.proxy.URL, old.Slot, old.ProxyToken, "", 407)
	_, next := connect(t, h.proxy.URL, g.Slot, g.ProxyToken, "ok", 200)
	readText(t, next, "BRok")
	h.apply(t, old, 409)
	old.Action = "revoke"
	h.apply(t, old, 409) // old completion cannot retire new route
	if ca.Load() != 1 || cb.Load() != 1 {
		t.Fatalf("upstream calls US=%d BR=%d", ca.Load(), cb.Load())
	}
	g.Action = "revoke"
	h.apply(t, g, 200)
	h.apply(t, g, 200)
	expectClosed(t, next)
	g.Action = "activate"
	h.apply(t, g, 409) // no resurrection after revoke
}

func TestGrantAuthAndIsolation(t *testing.T) {
	h := setup(t)
	a, _ := markedProxy(t, "US")
	g := h.grant
	g.Upstream = upstream(a)
	if status, _ := h.send(t, sign(t, h.key, g), "wrong"); status != 401 {
		t.Fatal(status)
	}
	_, otherKey, _ := ed25519.GenerateKey(rand.Reader)
	if status, _ := h.send(t, sign(t, otherKey, g), strings.Repeat("C", 32)); status != 409 {
		t.Fatal(status)
	}
	for _, mutate := range []func(*Grant){
		func(g *Grant) { g.NodeID = "node-b" }, func(g *Grant) { g.BootID = "old-boot" },
		func(g *Grant) { g.ExpiresAt = time.Now().Add(-time.Second).UnixMilli() },
		func(g *Grant) { g.ExpiresAt = time.Now().Add(10 * time.Minute).UnixMilli() },
		func(g *Grant) { g.Slot = "unknown" }, func(g *Grant) { g.Upstream.Protocol = "direct" },
	} {
		bad := g
		mutate(&bad)
		h.apply(t, bad, 409)
	}
	h.apply(t, g, 200)
	for _, mutate := range []func(*Grant){
		func(g *Grant) { g.TaskID = "other-task" }, func(g *Grant) { g.Generation++ },
		func(g *Grant) { g.EgressCountry = "BR" }, func(g *Grant) { g.Upstream.Address = "127.0.0.1:1" },
	} {
		bad := g
		bad.Action = "renew"
		mutate(&bad)
		h.apply(t, bad, 409)
	}
	connect(t, h.proxy.URL, "worker-2", g.ProxyToken, "", 407)
	connect(t, h.proxy.URL, g.Slot, "wrong", "", 407)
	r2, err := New(Config{NodeID: "node-a", PublicKey: h.key.Public().(ed25519.PublicKey), ControlToken: strings.Repeat("C", 32), Slots: []string{"worker-1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if _, err = r2.apply(sign(t, h.key, g)); err == nil {
		t.Fatal("old boot grant accepted after restart")
	}
}

func TestRenewalKeepsTunnelAndExpiryClosesIt(t *testing.T) {
	h := setup(t)
	a, _ := markedProxy(t, "US")
	g := h.grant
	g.Upstream = upstream(a)
	g.ExpiresAt = time.Now().Add(200 * time.Millisecond).UnixMilli()
	h.apply(t, g, 200)
	c, r := connect(t, h.proxy.URL, g.Slot, g.ProxyToken, "", 200)
	readText(t, r, "US")
	g.Action = "renew"
	g.ExpiresAt = time.Now().Add(650 * time.Millisecond).UnixMilli()
	h.apply(t, g, 200)
	h.apply(t, g, 200)
	time.Sleep(250 * time.Millisecond)
	_, _ = io.WriteString(c, "alive")
	readText(t, r, "alive")
	expectClosed(t, r)
	connect(t, h.proxy.URL, g.Slot, g.ProxyToken, "", 407)
	g.ExpiresAt = time.Now().Add(time.Minute).UnixMilli()
	h.apply(t, g, 409)
}

func TestCapacityAndTargetRestrictions(t *testing.T) {
	h := setup(t)
	a, calls := markedProxy(t, "US")
	g := h.grant
	g.Upstream = upstream(a)
	h.apply(t, g, 200)
	for i := 0; i < 2; i++ {
		_, r := connect(t, h.proxy.URL, g.Slot, g.ProxyToken, "", 200)
		readText(t, r, "US")
	}
	connect(t, h.proxy.URL, g.Slot, g.ProxyToken, "", 429)
	if calls.Load() != 2 {
		t.Fatal("capacity did not include active connections")
	}
	for _, target := range []string{"example.com:443", "youtube.com:80", "127.0.0.1:443", "youtube.com.attacker.test:443", "x.youtube.com\r\n:443"} {
		if allowedTarget(target) {
			t.Fatalf("unsafe target accepted: %q", target)
		}
	}
	h.relay.Close()
	connect(t, h.proxy.URL, g.Slot, g.ProxyToken, "", 407)
}

func TestRevokeCancelsStalledUpstreamHandshake(t *testing.T) {
	h := setup(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := listener.Accept()
		if err == nil {
			accepted <- c
		}
	}()
	g := h.grant
	g.Upstream = upstream(listener.Addr().String())
	h.apply(t, g, 200)
	done := make(chan int, 1)
	go func() {
		c, err := net.Dial("tcp", strings.TrimPrefix(h.proxy.URL, "http://"))
		if err != nil {
			done <- 0
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		_, _ = fmt.Fprintf(c, "CONNECT www.youtube.com:443 HTTP/1.1\r\nHost: www.youtube.com:443\r\nProxy-Authorization: Basic %s\r\n\r\n", base64.StdEncoding.EncodeToString([]byte(g.Slot+":"+g.ProxyToken)))
		resp, err := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: "CONNECT"})
		if err != nil {
			done <- 0
		} else {
			done <- resp.StatusCode
		}
	}()
	select {
	case c := <-accepted:
		defer c.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("no upstream connection")
	}
	g.Action = "revoke"
	h.apply(t, g, 200)
	select {
	case status := <-done:
		if status != 502 {
			t.Fatal(status)
		}
	case <-time.After(time.Second):
		t.Fatal("revoke did not cancel pending dial")
	}
}

func TestLocalRetireQuiescesAndCannotCloseNewerOwnership(t *testing.T) {
	h := setup(t)
	address, _ := markedProxy(t, "BR")
	g := h.grant
	g.Upstream = upstream(address)
	h.apply(t, g, 200)
	_, first := connect(t, h.proxy.URL, g.Slot, g.ProxyToken, "", 200)
	readText(t, first, "BR")
	retire := func(expected retireRequest, want int) {
		data, _ := json.Marshal(expected)
		req, _ := http.NewRequest("POST", h.control.URL+"/v1/retire", bytes.NewReader(data))
		req.Header.Set("Authorization", "Bearer "+strings.Repeat("C", 32))
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != want {
			t.Fatalf("retire status %d, want %d", res.StatusCode, want)
		}
		if want == 200 {
			var ack struct {
				Retired  bool `json:"retired"`
				InFlight int  `json:"in_flight"`
			}
			if err := json.NewDecoder(res.Body).Decode(&ack); err != nil || !ack.Retired || ack.InFlight != 0 {
				t.Fatal("missing quiescence receipt")
			}
		}
	}
	old := retireRequest{BootID: g.BootID, Slot: g.Slot, Epoch: g.Epoch, TaskID: g.TaskID, Generation: g.Generation}
	retire(old, 200)
	expectClosed(t, first)
	retire(old, 200)
	g.Epoch++
	g.TaskID = "next-channel"
	g.ProxyToken = strings.Repeat("N", 32)
	h.apply(t, g, 200)
	c, next := connect(t, h.proxy.URL, g.Slot, g.ProxyToken, "", 200)
	readText(t, next, "BR")
	retire(old, 409)
	old.BootID = "previous-relay-process"
	retire(old, 200)
	_, _ = io.WriteString(c, "still-alive")
	readText(t, next, "still-alive")
	retire(retireRequest{BootID: g.BootID, Slot: g.Slot, Epoch: g.Epoch, TaskID: g.TaskID, Generation: g.Generation}, 200)
	expectClosed(t, next)
}
