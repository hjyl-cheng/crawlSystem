package hysteria2

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/alpkeskin/rota/core/internal/proxyidentity"
	"github.com/apernet/hysteria/extras/v2/utils"
)

const maxNodeNameBytes = 255

// Node is a validated Hysteria2 share link. Credential contains private
// connection material and must never be logged or exposed through the API.
type Node struct {
	protocol     string
	address      string
	name         string
	credential   string
	identity     string
	host         string
	portSet      string
	auth         string
	sni          string
	insecure     bool
	pinSHA256    string
	echConfig    []byte
	obfsType     string
	obfsPassword string
}

func (n Node) Protocol() string   { return n.protocol }
func (n Node) Address() string    { return n.address }
func (n Node) Name() string       { return n.name }
func (n Node) Credential() string { return n.credential }
func (n Node) Identity() string   { return n.identity }

func (n Node) cacheKey() string { return n.identity }

func IsProtocol(protocol string) bool {
	return strings.EqualFold(strings.TrimSpace(protocol), "hysteria2")
}

// SchemeProtocol maps both official Hysteria2 URI schemes to one storage name.
func SchemeProtocol(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	separator := strings.Index(raw, "://")
	if separator < 1 {
		return "", false
	}
	switch strings.ToLower(raw[:separator]) {
	case "hysteria2", "hy2":
		return "hysteria2", true
	default:
		return "", false
	}
}

func Parse(raw string) (Node, error) {
	if _, ok := SchemeProtocol(raw); !ok {
		return Node{}, invalid("share URI scheme is not supported")
	}
	if strings.Contains(raw, "&amp;") {
		return Node{}, invalid("query contains an HTML entity")
	}
	prepared, rawAuthorityPortSet, err := prepareShareURI(strings.TrimSpace(raw))
	if err != nil {
		return Node{}, err
	}
	u, err := url.Parse(prepared)
	if err != nil {
		return Node{}, invalid("share URI is malformed")
	}
	host, address, authorityPortSet, err := parseServerEndpoint(u)
	if err != nil {
		return Node{}, err
	}
	if rawAuthorityPortSet != "" {
		authorityPortSet = rawAuthorityPortSet
	}

	auth := ""
	if u.User != nil {
		auth = u.User.Username()
		if password, ok := u.User.Password(); ok {
			auth += ":" + password
		}
	}

	query := u.Query()
	portSet, err := parsePortSet(query)
	if err != nil {
		return Node{}, err
	}
	if authorityPortSet != "" {
		if portSet != "" && portSet != authorityPortSet {
			return Node{}, invalid("multiple port hopping values conflict")
		}
		portSet = authorityPortSet
	}
	sni := strings.ToLower(strings.TrimSpace(query.Get("sni")))
	if sni != "" && !validHost(sni) {
		return Node{}, invalid("TLS server name is invalid")
	}
	insecure, err := parseOptionalBool(query.Get("insecure"))
	if err != nil {
		return Node{}, invalid("TLS insecure value is invalid")
	}
	pin, err := parseCertificatePin(query.Get("pinSHA256"))
	if err != nil {
		return Node{}, err
	}
	echConfig, err := parseECHConfig(query.Get("ech"))
	if err != nil {
		return Node{}, err
	}

	obfsType := strings.ToLower(strings.TrimSpace(query.Get("obfs")))
	obfsPassword := query.Get("obfs-password")
	switch obfsType {
	case "", "plain":
		if obfsPassword != "" {
			return Node{}, invalid("obfuscation password requires an obfuscation type")
		}
		obfsType = ""
	case "salamander", "gecko":
		if obfsPassword == "" {
			return Node{}, invalid("obfuscation password is missing")
		}
	default:
		return Node{}, invalid("obfuscation type is not supported")
	}
	name, err := cleanName(u.Fragment)
	if err != nil {
		return Node{}, err
	}

	canonicalQuery := url.Values{}
	if portSet != "" {
		canonicalQuery.Set("mport", portSet)
	}
	if sni != "" {
		canonicalQuery.Set("sni", sni)
	}
	if insecure {
		canonicalQuery.Set("insecure", "1")
	}
	if pin != "" {
		canonicalQuery.Set("pinSHA256", pin)
	}
	if len(echConfig) != 0 {
		canonicalQuery.Set("ech", base64.StdEncoding.EncodeToString(echConfig))
	}
	if obfsType != "" {
		canonicalQuery.Set("obfs", obfsType)
		canonicalQuery.Set("obfs-password", obfsPassword)
	}
	var user *url.Userinfo
	if auth != "" {
		user = url.User(auth)
	}
	canonical := (&url.URL{
		Scheme:   "hysteria2",
		User:     user,
		Host:     address,
		Path:     "/",
		RawQuery: canonicalQuery.Encode(),
	}).String()

	return Node{
		protocol:     "hysteria2",
		address:      address,
		name:         name,
		credential:   canonical,
		identity:     proxyidentity.Credential(canonical),
		host:         host,
		portSet:      portSet,
		auth:         auth,
		sni:          sni,
		insecure:     insecure,
		pinSHA256:    pin,
		echConfig:    echConfig,
		obfsType:     obfsType,
		obfsPassword: obfsPassword,
	}, nil
}

// prepareShareURI makes the official host:port-range URI form parseable by
// net/url. The normalized range is retained separately and restored as mport
// in the private canonical credential.
func prepareShareURI(raw string) (string, string, error) {
	schemeEnd := strings.Index(raw, "://")
	if schemeEnd < 1 {
		return raw, "", nil
	}
	authorityStart := schemeEnd + 3
	authorityEnd := len(raw)
	if offset := strings.IndexAny(raw[authorityStart:], "/?#"); offset >= 0 {
		authorityEnd = authorityStart + offset
	}
	authority := raw[authorityStart:authorityEnd]
	hostStart := authorityStart
	if at := strings.LastIndexByte(authority, '@'); at >= 0 {
		hostStart += at + 1
	}
	hostPort := raw[hostStart:authorityEnd]
	portStart := -1
	if strings.HasPrefix(hostPort, "[") {
		if bracket := strings.IndexByte(hostPort, ']'); bracket >= 0 && bracket+1 < len(hostPort) && hostPort[bracket+1] == ':' {
			portStart = hostStart + bracket + 2
		}
	} else if colon := strings.LastIndexByte(hostPort, ':'); colon >= 0 {
		portStart = hostStart + colon + 1
	}
	if portStart < 0 {
		return raw, "", nil
	}
	portSpec := raw[portStart:authorityEnd]
	if !strings.ContainsAny(portSpec, "-,") {
		return raw, "", nil
	}
	portSet, firstPort, err := normalizePortSet(portSpec)
	if err != nil {
		return "", "", err
	}
	prepared := raw[:portStart] + strconv.Itoa(firstPort) + raw[authorityEnd:]
	return prepared, portSet, nil
}

func parseServerEndpoint(u *url.URL) (string, string, string, error) {
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	if !validHost(host) {
		return "", "", "", invalid("server host is invalid")
	}

	authority := strings.TrimSpace(u.Host)
	portSpec := "443"
	if !strings.EqualFold(authority, host) && !strings.EqualFold(authority, "["+host+"]") {
		authorityHost, rawPort, err := net.SplitHostPort(authority)
		if err != nil || !strings.EqualFold(strings.TrimSpace(authorityHost), host) || rawPort == "" {
			return "", "", "", invalid("server port is invalid")
		}
		portSpec = rawPort
	}

	if strings.ContainsAny(portSpec, "-,") {
		portSet, firstPort, err := normalizePortSet(portSpec)
		if err != nil {
			return "", "", "", err
		}
		return host, net.JoinHostPort(host, strconv.Itoa(firstPort)), portSet, nil
	}
	port, err := validPort(portSpec)
	if err != nil {
		return "", "", "", err
	}
	return host, net.JoinHostPort(host, strconv.Itoa(port)), "", nil
}

func ParseForProtocol(protocol, raw string) (Node, error) {
	if !IsProtocol(protocol) {
		return Node{}, invalid("share URI scheme does not match protocol")
	}
	return Parse(raw)
}

func parsePortSet(query url.Values) (string, error) {
	mport := strings.TrimSpace(query.Get("mport"))
	ports := strings.TrimSpace(query.Get("ports"))
	var normalizedMPort, normalizedPorts string
	if mport != "" {
		var err error
		normalizedMPort, _, err = normalizePortSet(mport)
		if err != nil {
			return "", err
		}
	}
	if ports != "" {
		var err error
		normalizedPorts, _, err = normalizePortSet(ports)
		if err != nil {
			return "", err
		}
	}
	if normalizedMPort != "" && normalizedPorts != "" && normalizedMPort != normalizedPorts {
		return "", invalid("multiple port hopping values conflict")
	}
	if normalizedMPort != "" {
		return normalizedMPort, nil
	}
	return normalizedPorts, nil
}

func normalizePortSet(raw string) (string, int, error) {
	union := utils.ParsePortUnion(raw)
	if len(union) == 0 {
		return "", 0, invalid("port hopping value is invalid")
	}
	parts := make([]string, 0, len(union))
	for _, portRange := range union {
		if portRange.Start == 0 {
			return "", 0, invalid("port hopping value is invalid")
		}
		if portRange.Start == portRange.End {
			parts = append(parts, strconv.Itoa(int(portRange.Start)))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", portRange.Start, portRange.End))
		}
	}
	return strings.Join(parts, ","), int(union[0].Start), nil
}

func validPort(raw string) (int, error) {
	port, err := strconv.Atoi(raw)
	if err != nil || port < 1 || port > 65535 {
		return 0, invalid("server port is invalid")
	}
	return port, nil
}

func parseOptionalBool(raw string) (bool, error) {
	if strings.TrimSpace(raw) == "" {
		return false, nil
	}
	return strconv.ParseBool(raw)
}

func parseCertificatePin(raw string) (string, error) {
	pin := strings.ToLower(strings.TrimSpace(raw))
	pin = strings.ReplaceAll(pin, ":", "")
	pin = strings.ReplaceAll(pin, "-", "")
	decoded, err := hex.DecodeString(pin)
	if pin != "" && (err != nil || len(decoded) != 32) {
		return "", invalid("TLS certificate pin must be a SHA-256 hash")
	}
	return pin, nil
}

func parseECHConfig(raw string) ([]byte, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	for _, encoding := range []*base64.Encoding{
		base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding,
	} {
		decoded, err := encoding.DecodeString(raw)
		if err == nil && len(decoded) != 0 {
			return decoded, nil
		}
	}
	return nil, invalid("ECH configuration is invalid")
}

func validHost(host string) bool {
	if host == "" || strings.ContainsAny(host, " \t\r\n/?#@") {
		return false
	}
	if net.ParseIP(host) != nil {
		return true
	}
	for _, r := range host {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '.' || r == '_') {
			return false
		}
	}
	return true
}

func cleanName(raw string) (string, error) {
	name := strings.TrimSpace(raw)
	if !utf8.ValidString(name) {
		return "", invalid("node name is not valid UTF-8")
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return "", invalid("node name contains control characters")
		}
	}
	if len(name) > maxNodeNameBytes {
		return "", invalid("node name is too long")
	}
	return name, nil
}

func invalid(message string) error {
	return fmt.Errorf("invalid Hysteria2 node: %s", message)
}
