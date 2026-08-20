package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/pkg/logger"
	"github.com/gorilla/websocket"
)

func TestWebSocketOriginPolicy(t *testing.T) {
	tests := []struct {
		name           string
		origin         string
		host           string
		allowedOrigins []string
		want           bool
	}{
		{name: "non-browser client", host: "proxy.example", want: true},
		{name: "same origin", origin: "https://proxy.example", host: "proxy.example", want: true},
		{name: "allowlisted cross origin", origin: "https://admin.example", host: "proxy.example", allowedOrigins: []string{"https://admin.example"}, want: true},
		{name: "wildcard", origin: "https://other.example", host: "proxy.example", allowedOrigins: []string{"*"}, want: true},
		{name: "rejected cross origin", origin: "https://other.example", host: "proxy.example", allowedOrigins: []string{"https://admin.example"}, want: false},
		{name: "malformed origin", origin: "://bad", host: "proxy.example", allowedOrigins: []string{"https://admin.example"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest("GET", "http://"+tt.host+"/ws/dashboard", nil)
			request.Host = tt.host
			if tt.origin != "" {
				request.Header.Set("Origin", tt.origin)
			}
			handler := &WebSocketHandler{allowedOrigins: tt.allowedOrigins}
			if got := handler.checkOrigin(request); got != tt.want {
				t.Fatalf("checkOrigin() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestWebSocketShutdownClosesAndWaitsForHijackedSessions(t *testing.T) {
	handler := NewWebSocketHandler(nil, nil, nil, logger.New("error"), []string{"*"})
	sessionStarted := make(chan struct{})
	sessionDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := handler.upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		ctx, cancel, accepted := handler.beginSession(r, conn)
		if !accepted {
			_ = conn.Close()
			return
		}
		close(sessionStarted)
		defer func() {
			cancel()
			_ = conn.Close()
			handler.endSession(conn)
			close(sessionDone)
		}()
		<-ctx.Done()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()
	select {
	case <-sessionStarted:
	case <-time.After(time.Second):
		t.Fatal("websocket session did not start")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := handler.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case <-sessionDone:
	default:
		t.Fatal("shutdown returned before the session handler finished")
	}
	request := httptest.NewRequest(http.MethodGet, server.URL, nil)
	if _, _, accepted := handler.beginSession(request, conn); accepted {
		t.Fatal("websocket handler accepted a session after shutdown")
	}
}
