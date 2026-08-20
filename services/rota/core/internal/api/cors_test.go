package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alpkeskin/rota/core/pkg/logger"
	"github.com/go-chi/chi/v5"
)

func newBoundaryTestServer(origins []string, trustProxyHeaders bool) *Server {
	server := &Server{
		router:            chi.NewRouter(),
		logger:            logger.New("error"),
		corsOrigins:       origins,
		trustProxyHeaders: trustProxyHeaders,
	}
	server.setupMiddleware()
	server.router.Get("/resource", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Observed-Remote-Addr", r.RemoteAddr)
		w.WriteHeader(http.StatusNoContent)
	})
	return server
}

func TestCORSPreflightUsesConfiguredAllowlist(t *testing.T) {
	server := newBoundaryTestServer([]string{"https://allowed.example"}, false)

	allowed := httptest.NewRequest(http.MethodOptions, "/resource", nil)
	allowed.Header.Set("Origin", "https://allowed.example")
	allowed.Header.Set("Access-Control-Request-Method", http.MethodGet)
	allowedResponse := httptest.NewRecorder()
	server.router.ServeHTTP(allowedResponse, allowed)
	if got := allowedResponse.Header().Get("Access-Control-Allow-Origin"); got != "https://allowed.example" {
		t.Fatalf("allowed origin header = %q", got)
	}

	denied := httptest.NewRequest(http.MethodOptions, "/resource", nil)
	denied.Header.Set("Origin", "https://denied.example")
	denied.Header.Set("Access-Control-Request-Method", http.MethodGet)
	deniedResponse := httptest.NewRecorder()
	server.router.ServeHTTP(deniedResponse, denied)
	if got := deniedResponse.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("denied origin header = %q, want empty", got)
	}
}

func TestRealIPMiddlewareRequiresExplicitTrust(t *testing.T) {
	for _, tt := range []struct {
		name  string
		trust bool
		want  string
	}{
		{name: "direct peer", trust: false, want: "198.51.100.10:4321"},
		{name: "trusted proxy", trust: true, want: "203.0.113.20"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := newBoundaryTestServer([]string{"*"}, tt.trust)
			request := httptest.NewRequest(http.MethodGet, "/resource", nil)
			request.RemoteAddr = "198.51.100.10:4321"
			request.Header.Set("X-Forwarded-For", "203.0.113.20")
			response := httptest.NewRecorder()
			server.router.ServeHTTP(response, request)
			if got := response.Header().Get("X-Observed-Remote-Addr"); got != tt.want {
				t.Fatalf("observed remote address = %q, want %q", got, tt.want)
			}
		})
	}
}
