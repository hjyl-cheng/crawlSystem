package proxyidentity

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const endpointSeparator = "\x1f"

// Endpoint preserves the historical one-record-per-protocol-and-endpoint
// identity used by ordinary HTTP and SOCKS proxies.
func Endpoint(protocol, address string) string {
	return digest("endpoint" + endpointSeparator +
		strings.ToLower(strings.TrimSpace(protocol)) + endpointSeparator +
		strings.TrimSpace(address))
}

// Credential identifies a canonical share node, including all credentials,
// security, and transport parameters that affect how it connects.
func Credential(canonical string) string {
	return digest(strings.TrimSpace(canonical))
}

// ForProxy returns the storage identity for either an ordinary endpoint or a
// canonical share credential. It is also used defensively by repositories
// whose tests or internal callers bypass request normalization.
func ForProxy(protocol, address string, credential *string) string {
	switch strings.ToLower(strings.TrimSpace(protocol)) {
	case "vless", "vmess", "trojan", "shadowsocks":
		if credential != nil && strings.TrimSpace(*credential) != "" {
			return Credential(*credential)
		}
	}
	return Endpoint(protocol, address)
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
