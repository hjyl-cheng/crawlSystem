package vless

import (
	"bufio"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

const liveTestURL = "https://www.youtube.com/watch?v=_xXsXvsYAhA"

// TestLiveSubscription is opt-in because it performs real network requests.
// Set ROTA_VLESS_SUBSCRIPTION_FILE to a Base64 subscription file to run it.
func TestLiveSubscription(t *testing.T) {
	path := os.Getenv("ROTA_VLESS_SUBSCRIPTION_FILE")
	if path == "" {
		t.Skip("ROTA_VLESS_SUBSCRIPTION_FILE is not set")
	}

	nodes := readLiveNodes(t, path)
	if expectedText := os.Getenv("ROTA_VLESS_EXPECTED_NODES"); expectedText != "" {
		expected, err := strconv.Atoi(expectedText)
		if err != nil {
			t.Fatalf("invalid ROTA_VLESS_EXPECTED_NODES")
		}
		if len(nodes) != expected {
			t.Fatalf("unique nodes = %d, want %d", len(nodes), expected)
		}
	}
	if limitText := os.Getenv("ROTA_VLESS_TEST_LIMIT"); limitText != "" {
		limit, err := strconv.Atoi(limitText)
		if err != nil || limit < 1 {
			t.Fatalf("invalid ROTA_VLESS_TEST_LIMIT")
		}
		if limit < len(nodes) {
			nodes = nodes[:limit]
		}
	}

	semaphore := make(chan struct{}, 5)
	for _, node := range nodes {
		node := node
		t.Run(node.Address(), func(t *testing.T) {
			t.Parallel()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			dialer, err := NewDialer(node.Credential())
			if err != nil {
				t.Fatalf("NewDialer: %s", redactLiveError(node, err))
			}
			transport := &http.Transport{
				DialContext: dialer,
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true, // Match Rota's existing proxy health check.
				},
			}
			defer transport.CloseIdleConnections()

			client := &http.Client{Transport: transport, Timeout: 60 * time.Second}
			request, err := http.NewRequest(http.MethodGet, liveTestURL, nil)
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			request.Header.Set("User-Agent", "Rota-HealthCheck/1.0")

			response, err := client.Do(request)
			if err != nil {
				t.Fatalf("request failed: %s", redactLiveError(node, err))
			}
			response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", response.StatusCode)
			}
		})
	}
}

func readLiveNodes(t *testing.T, path string) []Node {
	t.Helper()
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read subscription: %v", err)
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(encoded)))
	if err != nil {
		t.Fatalf("decode subscription: %v", err)
	}

	seen := make(map[string]struct{})
	var nodes []Node
	scanner := bufio.NewScanner(strings.NewReader(string(decoded)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(strings.ToLower(line), "vless://") {
			continue
		}
		node, err := Parse(line)
		if err != nil {
			t.Fatalf("parse subscription line: %v", err)
		}
		if _, exists := seen[node.Address()]; exists {
			continue
		}
		seen[node.Address()] = struct{}{}
		nodes = append(nodes, node)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan subscription: %v", err)
	}
	if len(nodes) == 0 {
		t.Fatal("subscription has no VLESS nodes")
	}
	return nodes
}

func redactLiveError(node Node, err error) string {
	message := err.Error()
	for _, secret := range []string{node.Credential(), node.id} {
		message = strings.ReplaceAll(message, secret, "[redacted]")
	}
	return fmt.Sprintf("endpoint %s: %s", node.Address(), message)
}
