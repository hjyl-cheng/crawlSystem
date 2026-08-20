package proxy

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/background"
	"github.com/alpkeskin/rota/core/pkg/logger"
)

func TestProxyShutdownDrainsHandlerUsageBeforeStoppingTracker(t *testing.T) {
	writer := &recordingUsageWriter{}
	tracker := newUsageTracker(nil, writer, nil, testUsageTrackerConfig())
	cleanupUsageTracker(t, tracker)

	requestStarted := make(chan struct{})
	shutdownStarted := make(chan struct{})
	allowRecord := make(chan struct{})
	recordErr := make(chan error, 1)
	httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(requestStarted)
		<-allowRecord
		recordErr <- tracker.RecordRequest(RequestRecord{ProxyID: 42})
		w.WriteHeader(http.StatusNoContent)
	})}
	httpServer.RegisterOnShutdown(func() { close(shutdownStarted) })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = httpServer.Close() })
	serveDone := make(chan error, 1)
	go func() { serveDone <- httpServer.Serve(listener) }()

	requestDone := make(chan error, 1)
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 5 * time.Second}
	go func() {
		response, requestErr := client.Get("http://" + listener.Addr().String())
		if requestErr == nil {
			requestErr = response.Body.Close()
		}
		requestDone <- requestErr
	}()
	select {
	case <-requestStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("request did not reach test server")
	}

	server := &Server{
		server:     httpServer,
		tracker:    tracker,
		logger:     logger.New("error"),
		background: background.New(context.Background()),
	}
	shutdownDone := make(chan error, 1)
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelShutdown()
	go func() { shutdownDone <- server.Shutdown(shutdownCtx) }()
	select {
	case <-shutdownStarted:
	case <-time.After(time.Second):
		t.Fatal("HTTP shutdown did not start")
	}
	close(allowRecord)

	select {
	case err := <-recordErr:
		if err != nil {
			t.Fatalf("handler usage submission: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not submit usage")
	}
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not complete")
	}

	if got := writer.records(); len(got) != 1 || got[0].ProxyID != 42 {
		t.Fatalf("flushed records = %+v", got)
	}
	select {
	case err := <-requestDone:
		if err != nil {
			t.Fatalf("request completion: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request did not complete")
	}
	select {
	case err := <-serveDone:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("Serve returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("HTTP server did not stop")
	}
}

func TestShutdownTunnelsClosesAndWaitsForHijackedConnections(t *testing.T) {
	handler := &UpstreamProxyHandler{
		tunnels:     make(map[*activeTunnel]struct{}),
		tunnelsDone: make(chan struct{}),
	}
	client, clientPeer := net.Pipe()
	upstream, upstreamPeer := net.Pipe()
	defer clientPeer.Close()
	defer upstreamPeer.Close()

	tunnel, accepted := handler.beginTunnel(client, upstream, "worker-a")
	if !accepted {
		t.Fatal("initial tunnel was rejected")
	}
	handlerDone := make(chan struct{})
	go func() {
		var buffer [1]byte
		_, _ = client.Read(buffer[:])
		handler.endTunnel(tunnel)
		close(handlerDone)
	}()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := handler.ShutdownTunnels(shutdownCtx); err != nil {
		t.Fatalf("shutdown tunnels: %v", err)
	}
	select {
	case <-handlerDone:
	default:
		t.Fatal("tunnel shutdown returned before the handler finished")
	}

	lateClient, lateClientPeer := net.Pipe()
	lateUpstream, lateUpstreamPeer := net.Pipe()
	defer lateClient.Close()
	defer lateClientPeer.Close()
	defer lateUpstream.Close()
	defer lateUpstreamPeer.Close()
	if _, accepted := handler.beginTunnel(lateClient, lateUpstream, "worker-a"); accepted {
		t.Fatal("tunnel was accepted after shutdown")
	}
}

func TestRetireProxyUserClosesOnlyThatUsersTunnels(t *testing.T) {
	handler := &UpstreamProxyHandler{
		tunnels:     make(map[*activeTunnel]struct{}),
		tunnelsDone: make(chan struct{}),
	}
	aClient, aPeer := net.Pipe()
	aUpstream, aUpstreamPeer := net.Pipe()
	bClient, bPeer := net.Pipe()
	bUpstream, bUpstreamPeer := net.Pipe()
	defer aPeer.Close()
	defer aUpstreamPeer.Close()
	defer bPeer.Close()
	defer bUpstreamPeer.Close()

	aTunnel, accepted := handler.beginTunnel(aClient, aUpstream, "worker-a-old")
	if !accepted {
		t.Fatal("worker A tunnel was rejected")
	}
	bTunnel, accepted := handler.beginTunnel(bClient, bUpstream, "worker-b-current")
	if !accepted {
		t.Fatal("worker B tunnel was rejected")
	}
	aDone := make(chan struct{})
	go func() {
		var buffer [1]byte
		_, _ = aClient.Read(buffer[:])
		handler.endTunnel(aTunnel)
		close(aDone)
	}()

	retireCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := handler.RetireProxyUser(retireCtx, "worker-a-old"); err != nil {
		t.Fatalf("retire worker A: %v", err)
	}
	select {
	case <-aDone:
	default:
		t.Fatal("retirement returned before worker A tunnel ended")
	}

	handler.tunnelMu.Lock()
	_, bStillActive := handler.tunnels[bTunnel]
	handler.tunnelMu.Unlock()
	if !bStillActive {
		t.Fatal("retiring worker A closed worker B tunnel")
	}
	bWrite := make(chan error, 1)
	go func() {
		_, err := bPeer.Write([]byte{1})
		bWrite <- err
	}()
	var received [1]byte
	if _, err := bClient.Read(received[:]); err != nil || received[0] != 1 {
		t.Fatalf("worker B tunnel read = %v, byte = %d", err, received[0])
	}
	if err := <-bWrite; err != nil {
		t.Fatalf("worker B tunnel is no longer usable: %v", err)
	}

	lateClient, latePeer := net.Pipe()
	lateUpstream, lateUpstreamPeer := net.Pipe()
	defer lateClient.Close()
	defer latePeer.Close()
	defer lateUpstream.Close()
	defer lateUpstreamPeer.Close()
	if _, accepted := handler.beginTunnel(lateClient, lateUpstream, "worker-a-old"); accepted {
		t.Fatal("retired worker A credential opened a new tunnel")
	}

	_ = bClient.Close()
	_ = bUpstream.Close()
	handler.endTunnel(bTunnel)
}
