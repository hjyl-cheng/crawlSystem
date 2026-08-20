package api

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

func TestShutdownCancelsServicesBeforeWaitingForHTTPHandlers(t *testing.T) {
	serviceGroup := background.New(context.Background())
	var serviceCtx context.Context
	serviceReady := make(chan struct{})
	serviceGroup.Go(func(ctx context.Context) {
		serviceCtx = ctx
		close(serviceReady)
		<-ctx.Done()
	})
	<-serviceReady
	requestStarted := make(chan struct{})
	requestDone := make(chan error, 1)

	httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(requestStarted)
		<-serviceCtx.Done()
		w.WriteHeader(http.StatusNoContent)
	})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = httpServer.Close() })
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- httpServer.Serve(listener)
	}()

	client := &http.Client{
		Transport: &http.Transport{Proxy: nil},
		Timeout:   5 * time.Second,
	}
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
		logger:     logger.New("error"),
		background: serviceGroup,
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	select {
	case err := <-requestDone:
		if err != nil {
			t.Fatalf("request completion: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request remained blocked after service cancellation")
	}
	select {
	case err := <-serveDone:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("Serve returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP server did not stop")
	}
}

func TestShutdownWaitsForOwnedServicesBeforeClosingResources(t *testing.T) {
	serviceGroup := background.New(context.Background())
	serviceCancelled := make(chan struct{})
	releaseService := make(chan struct{})
	serviceGroup.Go(func(ctx context.Context) {
		<-ctx.Done()
		close(serviceCancelled)
		<-releaseService
	})

	resourcesClosed := make(chan struct{})
	server := &Server{
		server:     &http.Server{},
		logger:     logger.New("error"),
		background: serviceGroup,
		closeServices: func() error {
			close(resourcesClosed)
			return nil
		},
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- server.Shutdown(context.Background()) }()
	<-serviceCancelled
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned before service drain: %v", err)
	case <-resourcesClosed:
		t.Fatal("resources closed while an owned service was running")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseService)
	if err := <-shutdownDone; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case <-resourcesClosed:
	default:
		t.Fatal("resources were not closed after service drain")
	}
}
