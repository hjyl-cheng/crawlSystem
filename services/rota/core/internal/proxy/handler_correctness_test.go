package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alpkeskin/rota/core/internal/models"
)

type generationSelector struct {
	generation int
}

func (s *generationSelector) Select(context.Context) (*models.Proxy, error) {
	return &models.Proxy{ID: s.generation}, nil
}

func (s *generationSelector) Refresh(context.Context) error { return nil }

func TestHandlerRoutingSnapshotIsPublishedAtomically(t *testing.T) {
	handler := NewUpstreamProxyHandler(
		&generationSelector{generation: 1},
		nil,
		&models.RotationSettings{Timeout: 1},
		nil,
	)

	var inconsistent atomic.Bool
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for generation := 2; generation < 2_000; generation++ {
			handler.updateRouting(
				&generationSelector{generation: generation},
				models.RotationSettings{Timeout: generation},
			)
		}
	}()
	go func() {
		defer wg.Done()
		for iteration := 0; iteration < 10_000; iteration++ {
			snapshot := handler.routingSnapshot()
			selector := snapshot.selector.(*generationSelector)
			if selector.generation != snapshot.settings.Timeout {
				inconsistent.Store(true)
				return
			}
		}
	}()
	wg.Wait()
	if inconsistent.Load() {
		t.Fatal("selector and settings came from different routing generations")
	}
}

func TestStripHopByHopHeadersIncludesConnectionTokens(t *testing.T) {
	header := make(http.Header)
	header.Add("Connection", "X-Private, X-Trace")
	header.Add("Connection", "X-Second")
	header.Set("X-Private", "secret")
	header.Set("X-Trace", "trace")
	header.Set("X-Second", "second")
	header.Set("Proxy-Connection", "keep-alive")
	header.Set("Trailer", "X-Trailer")
	header.Set("X-End-To-End", "preserve")

	stripHopByHopHeaders(header)
	for _, name := range []string{"Connection", "X-Private", "X-Trace", "X-Second", "Proxy-Connection", "Trailer"} {
		if value := header.Get(name); value != "" {
			t.Fatalf("header %s retained value %q", name, value)
		}
	}
	if got := header.Get("X-End-To-End"); got != "preserve" {
		t.Fatalf("end-to-end header = %q", got)
	}
}

func TestCopyResponseStripsDynamicHopByHopHeaders(t *testing.T) {
	response := &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Connection":      []string{"X-Upstream-Only"},
			"X-Upstream-Only": []string{"secret"},
			"X-End-To-End":    []string{"visible"},
		},
		Body: io.NopCloser(strings.NewReader("ok")),
	}
	recorder := httptest.NewRecorder()

	copyResponse(recorder, response)

	if got := recorder.Header().Get("X-Upstream-Only"); got != "" {
		t.Fatalf("dynamic hop-by-hop response header = %q", got)
	}
	if got := recorder.Header().Get("X-End-To-End"); got != "visible" {
		t.Fatalf("end-to-end response header = %q", got)
	}
}

type closeTrackingBody struct {
	closed bool
}

func (b *closeTrackingBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *closeTrackingBody) Close() error {
	b.closed = true
	return nil
}

func TestCloseResponseBodyHandlesResponseReturnedWithError(t *testing.T) {
	body := &closeTrackingBody{}
	closeResponseBody(&http.Response{Body: body})
	if !body.closed {
		t.Fatal("response body was not closed")
	}
}
