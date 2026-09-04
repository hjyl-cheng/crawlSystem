package hysteria2

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// Set ROTA_HYSTERIA2_SUBSCRIPTION_FILE to run real nodes through YouTube.
func TestLiveSubscriptionYouTube(t *testing.T) {
	path := os.Getenv("ROTA_HYSTERIA2_SUBSCRIPTION_FILE")
	if path == "" {
		t.Skip("ROTA_HYSTERIA2_SUBSCRIPTION_FILE is not set")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read subscription: %v", err)
	}
	decoded := decodeSubscription(data)
	var nodes []Node
	scanner := bufio.NewScanner(strings.NewReader(string(decoded)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if _, ok := SchemeProtocol(line); !ok {
			continue
		}
		node, err := Parse(line)
		if err != nil {
			t.Fatalf("parse Hysteria2 subscription entry without exposing it: %v", err)
		}
		nodes = append(nodes, node)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(nodes) == 0 {
		t.Fatal("subscription has no Hysteria2 nodes")
	}

	const concurrency = 10
	semaphore := make(chan struct{}, concurrency)
	errorsSeen := make(chan error, len(nodes))
	var wg sync.WaitGroup
	for index, node := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodGet,
				fmt.Sprintf("https://www.youtube.com/results?search_query=%d", index+1), nil)
			if err != nil {
				errorsSeen <- err
				return
			}
			transport := &http.Transport{DialContext: NewNodeDialer(node)}
			defer transport.CloseIdleConnections()
			response, err := transport.RoundTrip(request)
			if err != nil {
				errorsSeen <- fmt.Errorf("request failed: %w", sanitizeError(err, node))
				return
			}
			_, copyErr := io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
			closeErr := response.Body.Close()
			if copyErr != nil {
				errorsSeen <- fmt.Errorf("read failed: %w", sanitizeError(copyErr, node))
				return
			}
			if closeErr != nil {
				errorsSeen <- fmt.Errorf("close failed: %w", sanitizeError(closeErr, node))
				return
			}
			if response.StatusCode != http.StatusOK {
				errorsSeen <- fmt.Errorf("unexpected YouTube HTTP status %d", response.StatusCode)
				return
			}
			errorsSeen <- nil
		}()
	}
	wg.Wait()
	close(errorsSeen)

	passed := 0
	failures := make(map[string]int)
	for err := range errorsSeen {
		if err == nil {
			passed++
			continue
		}
		failures[liveFailureClass(err)]++
	}
	t.Logf("Hysteria2 YouTube checks: passed=%d failed=%d total=%d", passed, len(nodes)-passed, len(nodes))
	if passed == 0 {
		t.Fatalf("all Hysteria2 nodes failed without exposing endpoints: %v", failures)
	}
}

func liveFailureClass(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case strings.Contains(strings.ToLower(err.Error()), "certificate"):
		return "certificate"
	case strings.Contains(strings.ToLower(err.Error()), "auth"):
		return "authentication"
	case strings.Contains(strings.ToLower(err.Error()), "http status"):
		return "http_status"
	default:
		return "transport"
	}
}

func decodeSubscription(data []byte) []byte {
	trimmed := strings.TrimSpace(string(data))
	if strings.Contains(trimmed, "://") {
		return []byte(trimmed)
	}
	for _, encoding := range []*base64.Encoding{
		base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding,
	} {
		if decoded, err := encoding.DecodeString(trimmed); err == nil {
			return decoded
		}
	}
	return data
}
