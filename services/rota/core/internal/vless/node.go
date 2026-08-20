package vless

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"unicode"

	"github.com/google/uuid"
)

const (
	requiredEncryption  = "none"
	requiredFlow        = "xtls-rprx-vision"
	requiredNetwork     = "tcp"
	requiredSecurity    = "reality"
	requiredFingerprint = "chrome"
)

var allowedQueryKeys = map[string]struct{}{
	"encryption": {},
	"flow":       {},
	"type":       {},
	"security":   {},
	"sni":        {},
	"fp":         {},
	"pbk":        {},
	"sid":        {},
}

// Node is a validated VLESS/TCP/Reality endpoint. Credential returns the
// canonical URI used for storage; callers must treat it as a password and
// never include it in responses or logs.
type Node struct {
	address    string
	name       string
	credential string
	host       string
	port       int
	id         string
	sni        string
	publicKey  string
	shortID    string
}

func (n Node) Address() string    { return n.address }
func (n Node) Name() string       { return n.name }
func (n Node) Credential() string { return n.credential }

func (n Node) cacheKey() string {
	sum := sha256.Sum256([]byte(n.credential))
	return hex.EncodeToString(sum[:])
}

// Parse validates the deliberately narrow VLESS variant supported by Rota:
// TCP + Reality + xtls-rprx-vision with a Chrome fingerprint.
func Parse(raw string) (Node, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Node{}, invalid("URI is required")
	}

	u, err := url.Parse(raw)
	if err != nil {
		return Node{}, invalid("URI cannot be parsed")
	}
	if !strings.EqualFold(u.Scheme, "vless") || u.Opaque != "" {
		return Node{}, invalid("scheme must be vless")
	}
	if u.Path != "" {
		return Node{}, invalid("path is not supported")
	}
	if u.User == nil || u.User.Username() == "" {
		return Node{}, invalid("UUID is required")
	}
	if _, hasPassword := u.User.Password(); hasPassword {
		return Node{}, invalid("userinfo password is not supported")
	}

	parsedID, err := uuid.Parse(u.User.Username())
	if err != nil {
		return Node{}, invalid("UUID is invalid")
	}
	id := parsedID.String()

	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	if host == "" || !validHost(host) {
		return Node{}, invalid("host is invalid")
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 {
		return Node{}, invalid("port must be between 1 and 65535")
	}
	address := net.JoinHostPort(host, strconv.Itoa(port))

	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return Node{}, invalid("query is invalid")
	}
	for key, values := range query {
		if _, ok := allowedQueryKeys[key]; !ok {
			return Node{}, invalid("query contains an unsupported parameter")
		}
		if len(values) != 1 {
			return Node{}, invalid(fmt.Sprintf("query parameter %q must occur once", key))
		}
	}

	if err := requireValue(query, "encryption", requiredEncryption); err != nil {
		return Node{}, err
	}
	if err := requireValue(query, "flow", requiredFlow); err != nil {
		return Node{}, err
	}
	if err := requireValue(query, "type", requiredNetwork); err != nil {
		return Node{}, err
	}
	if err := requireValue(query, "security", requiredSecurity); err != nil {
		return Node{}, err
	}
	if err := requireValue(query, "fp", requiredFingerprint); err != nil {
		return Node{}, err
	}

	sni := strings.ToLower(strings.TrimSpace(query.Get("sni")))
	if !validDNSName(sni) {
		return Node{}, invalid("sni must be a valid DNS name")
	}

	publicKey := strings.TrimRight(strings.TrimSpace(query.Get("pbk")), "=")
	decodedKey, err := base64.RawURLEncoding.DecodeString(publicKey)
	if err != nil || len(decodedKey) != 32 {
		return Node{}, invalid("pbk must be a 32-byte base64url public key")
	}

	shortID := strings.ToLower(strings.TrimSpace(query.Get("sid")))
	if shortID != "" {
		if len(shortID) > 16 || len(shortID)%2 != 0 {
			return Node{}, invalid("sid must contain up to 16 hexadecimal characters")
		}
		if _, err := hex.DecodeString(shortID); err != nil {
			return Node{}, invalid("sid must contain up to 16 hexadecimal characters")
		}
	}

	name := strings.TrimSpace(u.Fragment)
	if len(name) > 255 || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return Node{}, invalid("node name is invalid")
	}

	canonicalQuery := url.Values{
		"encryption": {requiredEncryption},
		"flow":       {requiredFlow},
		"fp":         {requiredFingerprint},
		"pbk":        {publicKey},
		"security":   {requiredSecurity},
		"sni":        {sni},
		"type":       {requiredNetwork},
	}
	if shortID != "" {
		canonicalQuery.Set("sid", shortID)
	}
	canonical := (&url.URL{
		Scheme:   "vless",
		User:     url.User(id),
		Host:     address,
		RawQuery: canonicalQuery.Encode(),
	}).String()

	return Node{
		address:    address,
		name:       name,
		credential: canonical,
		host:       host,
		port:       port,
		id:         id,
		sni:        sni,
		publicKey:  publicKey,
		shortID:    shortID,
	}, nil
}

func requireValue(query url.Values, key, want string) error {
	values, ok := query[key]
	if !ok || len(values) != 1 {
		return invalid(fmt.Sprintf("query parameter %q is required", key))
	}
	if values[0] != want {
		return invalid(fmt.Sprintf("query parameter %q must be %q", key, want))
	}
	return nil
}

func validHost(host string) bool {
	if ip := net.ParseIP(host); ip != nil {
		return true
	}
	return validDNSName(host)
}

func validDNSName(name string) bool {
	if name == "" || len(name) > 253 || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' {
				return false
			}
		}
	}
	return true
}

func invalid(reason string) error {
	return fmt.Errorf("invalid VLESS URI: %s", reason)
}
