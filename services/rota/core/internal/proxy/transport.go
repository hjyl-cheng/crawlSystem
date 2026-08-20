package proxy

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/xraynode"
	proxyDialer "golang.org/x/net/proxy"
	"h12.io/socks"
)

// transportCache caches *http.Transport per proxy address+protocol to avoid
// creating a new transport (and new connection pool) for every request.
var transportCache sync.Map

// ClearTransportCache closes idle connections held by cached transports and
// removes every entry. Active requests may finish on their existing transport;
// subsequent requests build a transport from the latest proxy configuration.
func ClearTransportCache() {
	transportCache.Range(func(key, value any) bool {
		if transport, ok := value.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
		transportCache.Delete(key)
		return true
	})
}

// CloseRuntimeResources releases process-wide transport and Xray node caches.
// It is called only after both HTTP servers have stopped accepting work.
func CloseRuntimeResources() error {
	ClearTransportCache()
	return xraynode.CloseAllRuntimes()
}

// GetOrCreateTransport returns a cached transport for the given proxy,
// or creates and caches a new one.
func GetOrCreateTransport(p *models.Proxy) (*http.Transport, error) {
	key := transportCacheKey(p)
	if t, ok := transportCache.Load(key); ok {
		return t.(*http.Transport), nil
	}
	t, err := CreateProxyTransport(p)
	if err != nil {
		return nil, err
	}
	actual, _ := transportCache.LoadOrStore(key, t)
	return actual.(*http.Transport), nil
}

// CreateProxyTransport creates an HTTP transport configured for the given proxy
// This is shared between proxy handler and health checker
func CreateProxyTransport(p *models.Proxy) (*http.Transport, error) {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,             // Skip certificate verification for proxy connections
			MinVersion:         tls.VersionTLS10, // Support older TLS versions for compatibility
			MaxVersion:         0,                // Allow all TLS versions
			// Don't specify CipherSuites to accept all available ciphers for maximum compatibility
			// This is acceptable since InsecureSkipVerify is already true
		},
		// Timeouts for proxy connections
		// NOTE: Do NOT set DialContext here - it will override Proxy settings!
		// Let http.Transport handle proxy dialing automatically
		TLSHandshakeTimeout:   30 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
		ExpectContinueTimeout: 10 * time.Second,
	}
	if xraynode.IsProtocol(p.Protocol) {
		dialer, err := shareNodeDialContext(p)
		if err != nil {
			return nil, err
		}
		transport.DialContext = dialer
		return transport, nil
	}

	// Parse proxy URL
	var proxyURL string
	var authMasked string // For logging (hide credentials)

	if p.Username != nil && *p.Username != "" {
		// Username exists, include authentication
		if p.Password != nil && *p.Password != "" {
			// Both username and password
			proxyURL = fmt.Sprintf("%s://%s:%s@%s", p.Protocol, *p.Username, *p.Password, p.Address)
			authMasked = fmt.Sprintf("%s://[username]:[password]@%s", p.Protocol, p.Address)
		} else {
			// Only username (API key), password is empty
			proxyURL = fmt.Sprintf("%s://%s:@%s", p.Protocol, *p.Username, p.Address)
			authMasked = fmt.Sprintf("%s://[api_key]:@%s", p.Protocol, p.Address)
		}
	} else {
		// No authentication
		proxyURL = fmt.Sprintf("%s://%s", p.Protocol, p.Address)
		authMasked = proxyURL
	}

	parsedURL, err := url.Parse(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("invalid proxy URL %s: %w", authMasked, err)
	}

	switch p.Protocol {
	case "http", "https":
		// Set proxy URL - http.Transport will handle authentication headers automatically
		transport.Proxy = http.ProxyURL(parsedURL)
	case "socks4", "socks4a":
		// Create SOCKS4/SOCKS4A dialer using h12.io/socks
		// The Dial function accepts URI format: socks4://[user@]host:port
		transport.Dial = socks.Dial(proxyURL)
	case "socks5":
		// Create SOCKS5 dialer
		var auth *proxyDialer.Auth
		if p.Username != nil && *p.Username != "" {
			// Username exists, create auth
			password := ""
			if p.Password != nil {
				password = *p.Password
			}
			auth = &proxyDialer.Auth{
				User:     *p.Username,
				Password: password,
			}
		}

		dialer, err := proxyDialer.SOCKS5("tcp", p.Address, auth, proxyDialer.Direct)
		if err != nil {
			return nil, fmt.Errorf("failed to create SOCKS5 dialer: %w", err)
		}

		transport.Dial = dialer.Dial
	default:
		return nil, fmt.Errorf("unsupported proxy protocol: %s", p.Protocol)
	}

	return transport, nil
}

func transportCacheKey(p *models.Proxy) string {
	username, password := "", ""
	if p.Username != nil {
		username = *p.Username
	}
	if p.Password != nil {
		password = *p.Password
	}
	credentialHash := sha256.Sum256([]byte(username + "\x00" + password))
	return fmt.Sprintf("%s://%s#%x", p.Protocol, p.Address, credentialHash)
}

func shareNodeDialContext(p *models.Proxy) (func(context.Context, string, string) (net.Conn, error), error) {
	if p.Password == nil || strings.TrimSpace(*p.Password) == "" {
		return nil, fmt.Errorf("share node credential is missing")
	}
	node, err := xraynode.ParseForProtocol(p.Protocol, *p.Password)
	if err != nil {
		return nil, err
	}
	if node.Address() != p.Address {
		return nil, fmt.Errorf("share node credential endpoint does not match stored endpoint")
	}
	return xraynode.NewNodeDialer(node), nil
}
