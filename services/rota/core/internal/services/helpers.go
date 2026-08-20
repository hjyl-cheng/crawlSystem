package services

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	proxyDialer "golang.org/x/net/proxy"
	"h12.io/socks"
)

// redactURLForLog retains only enough origin information to identify a remote
// system. Userinfo, path, query, and fragment may all contain credentials.
func redactURLForLog(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "[redacted]"
	}
	return u.Scheme + "://" + u.Host
}

func redactURLInError(raw string, err error) string {
	if err == nil {
		return ""
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		cause := redactURLSecrets(raw, urlErr.Err.Error())
		cause = redactURLSecrets(urlErr.URL, cause)
		return fmt.Sprintf("%s %s: %s", urlErr.Op, redactURLForLog(urlErr.URL), cause)
	}
	return redactURLSecrets(raw, err.Error())
}

func redactURLSecrets(raw, message string) string {
	origin := redactURLForLog(raw)
	message = strings.ReplaceAll(message, raw, origin)
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return message
	}
	components := []string{u.EscapedPath(), u.Path, u.RawQuery, u.Fragment}
	for _, segment := range strings.Split(u.Path, "/") {
		if segment != "" {
			components = append(components, segment)
		}
	}
	if query, queryErr := url.ParseQuery(u.RawQuery); queryErr == nil {
		for _, values := range query {
			components = append(components, values...)
		}
	}
	if u.User != nil {
		components = append(components, u.User.String(), u.User.Username())
		if password, ok := u.User.Password(); ok {
			components = append(components, password)
		}
	}
	for _, component := range components {
		if component != "" && component != "/" {
			message = strings.ReplaceAll(message, component, "[redacted]")
		}
	}
	return message
}

// parseURL wraps url.Parse with a helpful error
func parseURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid URL %q: %w", raw, err)
	}
	return u, nil
}

// buildSocksDialer returns a DialContext-compatible function for SOCKS proxies
func buildSocksDialer(address, protocol string) (func(context.Context, string, string) (net.Conn, error), error) {
	proxyURL := fmt.Sprintf("%s://%s", protocol, address)
	switch protocol {
	case "socks4", "socks4a":
		dialFn := socks.Dial(proxyURL)
		return func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialFn(network, addr)
		}, nil
	case "socks5":
		dialer, err := proxyDialer.SOCKS5("tcp", address, nil, proxyDialer.Direct)
		if err != nil {
			return nil, fmt.Errorf("failed to create SOCKS5 dialer: %w", err)
		}
		dc, ok := dialer.(interface {
			DialContext(ctx context.Context, network, addr string) (net.Conn, error)
		})
		if ok {
			return dc.DialContext, nil
		}
		return func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.Dial(network, addr)
		}, nil
	}
	return nil, fmt.Errorf("unsupported socks protocol: %s", protocol)
}
