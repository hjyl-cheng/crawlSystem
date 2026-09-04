package sharenode

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/alpkeskin/rota/core/internal/hysteria2"
	"github.com/alpkeskin/rota/core/internal/xraynode"
)

type Node interface {
	Protocol() string
	Address() string
	Name() string
	Credential() string
	Identity() string
}

type ContextDialer func(context.Context, string, string) (net.Conn, error)

func IsProtocol(protocol string) bool {
	return xraynode.IsProtocol(protocol) || hysteria2.IsProtocol(protocol)
}

func SchemeProtocol(raw string) (string, bool) {
	if protocol, ok := xraynode.SchemeProtocol(raw); ok {
		return protocol, true
	}
	return hysteria2.SchemeProtocol(raw)
}

func Parse(raw string) (Node, error) {
	protocol, ok := SchemeProtocol(raw)
	if !ok {
		return nil, fmt.Errorf("share URI scheme is not supported")
	}
	if hysteria2.IsProtocol(protocol) {
		return hysteria2.Parse(raw)
	}
	return xraynode.Parse(raw)
}

func ParseForProtocol(protocol, raw string) (Node, error) {
	protocol = strings.ToLower(strings.TrimSpace(protocol))
	if hysteria2.IsProtocol(protocol) {
		return hysteria2.ParseForProtocol(protocol, raw)
	}
	return xraynode.ParseForProtocol(protocol, raw)
}

func NewDialer(protocol, credential string) (ContextDialer, error) {
	protocol = strings.ToLower(strings.TrimSpace(protocol))
	if hysteria2.IsProtocol(protocol) {
		dialer, err := hysteria2.NewDialer(credential)
		return ContextDialer(dialer), err
	}
	if xraynode.IsProtocol(protocol) {
		dialer, err := xraynode.NewDialer(credential)
		return ContextDialer(dialer), err
	}
	return nil, fmt.Errorf("share node protocol is not supported")
}

func CloseAllRuntimes() error {
	return joinErrors(xraynode.CloseAllRuntimes(), hysteria2.CloseAllRuntimes())
}

func joinErrors(first, second error) error {
	if first == nil {
		return second
	}
	if second == nil {
		return first
	}
	return fmt.Errorf("close share node runtimes: %v; %w", first, second)
}
