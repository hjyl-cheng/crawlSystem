package proxy

import (
	"fmt"
	"strings"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/proxyidentity"
	"github.com/alpkeskin/rota/core/internal/sharenode"
)

var supportedProtocols = map[string]struct{}{
	"http":        {},
	"https":       {},
	"socks4":      {},
	"socks4a":     {},
	"socks5":      {},
	"vless":       {},
	"vmess":       {},
	"trojan":      {},
	"shadowsocks": {},
	"hysteria2":   {},
}

// NormalizeCreateRequest validates external proxy input. Share nodes keep
// their public endpoint in address and their canonical URI in write-only password.
func NormalizeCreateRequest(req *models.CreateProxyRequest) error {
	req.Protocol = strings.ToLower(strings.TrimSpace(req.Protocol))
	if req.Protocol == "" {
		req.Protocol = "http"
	}
	if !IsSupportedProtocol(req.Protocol) {
		return fmt.Errorf("unsupported proxy protocol")
	}
	req.Address = strings.TrimSpace(req.Address)
	if req.Address == "" {
		return fmt.Errorf("address is required")
	}

	if !sharenode.IsProtocol(req.Protocol) {
		if _, shareURI := sharenode.SchemeProtocol(req.Address); shareURI {
			return fmt.Errorf("share URI requires its matching protocol")
		}
		req.NodeIdentity = proxyidentity.Endpoint(req.Protocol, req.Address)
		return nil
	}

	node, err := sharenode.ParseForProtocol(req.Protocol, shareCredentialInput(req.Address, req.Password))
	if err != nil {
		return err
	}
	credential := node.Credential()
	req.Address = node.Address()
	req.Username = nil
	req.Password = &credential
	req.NodeIdentity = node.Identity()
	req.Tags = appendUniqueTag(req.Tags, node.Name())
	return nil
}

// NormalizeUpdateRequest preserves hidden share credentials when an edit only
// contains the public endpoint. Endpoint changes require a complete share URI.
func NormalizeUpdateRequest(existing *models.Proxy, req *models.UpdateProxyRequest) error {
	targetProtocol := existing.Protocol
	if req.Protocol != "" {
		req.Protocol = strings.ToLower(strings.TrimSpace(req.Protocol))
		targetProtocol = req.Protocol
	}
	if !IsSupportedProtocol(targetProtocol) {
		return fmt.Errorf("unsupported proxy protocol")
	}

	existingShare := sharenode.IsProtocol(existing.Protocol)
	targetShare := sharenode.IsProtocol(targetProtocol)
	if existingShare && targetProtocol != existing.Protocol {
		return fmt.Errorf("a share node cannot be converted to another protocol")
	}
	if !targetShare {
		if _, shareURI := sharenode.SchemeProtocol(req.Address); shareURI {
			return fmt.Errorf("share URI requires its matching protocol")
		}
		address := strings.TrimSpace(req.Address)
		if address == "" {
			address = existing.Address
		}
		req.NodeIdentity = proxyidentity.Endpoint(targetProtocol, address)
		return nil
	}

	raw := ""
	if _, shareURI := sharenode.SchemeProtocol(req.Address); shareURI {
		raw = req.Address
	} else if req.Password != nil {
		if _, shareURI := sharenode.SchemeProtocol(*req.Password); shareURI {
			raw = *req.Password
		}
	}
	if raw != "" {
		node, err := sharenode.ParseForProtocol(targetProtocol, raw)
		if err != nil {
			return err
		}
		credential := node.Credential()
		emptyUsername := ""
		req.Address = node.Address()
		req.Protocol = node.Protocol()
		req.Username = &emptyUsername
		req.Password = &credential
		req.NodeIdentity = node.Identity()
		req.Tags = appendUniqueTag(req.Tags, node.Name())
		return nil
	}

	if !existingShare {
		return fmt.Errorf("a complete share URI is required")
	}
	if address := strings.TrimSpace(req.Address); address != "" && address != existing.Address {
		return fmt.Errorf("changing a share node endpoint requires a complete share URI")
	}
	if existing.Password != nil && strings.TrimSpace(*existing.Password) != "" {
		node, err := sharenode.ParseForProtocol(existing.Protocol, *existing.Password)
		if err != nil {
			return fmt.Errorf("stored share credential is invalid")
		}
		req.NodeIdentity = node.Identity()
	} else {
		req.NodeIdentity = existing.NodeIdentity
	}
	emptyUsername := ""
	req.Address = ""
	req.Username = &emptyUsername
	req.Password = nil
	return nil
}

func IsSupportedProtocol(protocol string) bool {
	_, ok := supportedProtocols[strings.ToLower(strings.TrimSpace(protocol))]
	return ok
}

func IsSupportedSourceProtocol(protocol string) bool {
	return strings.EqualFold(strings.TrimSpace(protocol), "auto") || IsSupportedProtocol(protocol)
}

func shareCredentialInput(address string, password *string) string {
	if _, ok := sharenode.SchemeProtocol(address); ok {
		return address
	}
	if password != nil && strings.TrimSpace(*password) != "" {
		return *password
	}
	return address
}

func appendUniqueTag(tags []string, tag string) []string {
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return tags
	}
	for _, existing := range tags {
		if existing == tag {
			return tags
		}
	}
	return append(tags, tag)
}
