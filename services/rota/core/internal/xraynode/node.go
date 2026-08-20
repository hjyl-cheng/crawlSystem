package xraynode

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"unicode"

	"github.com/alpkeskin/rota/core/internal/proxyidentity"
	"github.com/google/uuid"
	xraytls "github.com/xtls/xray-core/transport/internet/tls"
)

const maxNodeNameBytes = 255

var shareProtocols = map[string]struct{}{
	"vless":       {},
	"vmess":       {},
	"trojan":      {},
	"shadowsocks": {},
}

var shadowsocksMethods = map[string]struct{}{
	"aes-128-gcm":                   {},
	"aes-256-gcm":                   {},
	"chacha20-poly1305":             {},
	"chacha20-ietf-poly1305":        {},
	"xchacha20-poly1305":            {},
	"xchacha20-ietf-poly1305":       {},
	"2022-blake3-aes-128-gcm":       {},
	"2022-blake3-aes-256-gcm":       {},
	"2022-blake3-chacha20-poly1305": {},
}

var shadowsocks2022KeyLengths = map[string]int{
	"2022-blake3-aes-128-gcm":       16,
	"2022-blake3-aes-256-gcm":       32,
	"2022-blake3-chacha20-poly1305": 32,
}

// Node is a validated share link and the complete Xray outbound it describes.
// Credential is write-only storage material and must never be logged or exposed.
type Node struct {
	protocol   string
	address    string
	name       string
	credential string
	identity   string
	host       string
	port       int
	outbound   map[string]any
	secrets    []string
}

func (n Node) Protocol() string   { return n.protocol }
func (n Node) Address() string    { return n.address }
func (n Node) Name() string       { return n.name }
func (n Node) Credential() string { return n.credential }
func (n Node) Identity() string   { return n.identity }

func (n Node) cacheKey() string { return n.identity }

// IsProtocol reports whether protocol is backed by the embedded Xray runtime.
func IsProtocol(protocol string) bool {
	_, ok := shareProtocols[strings.ToLower(strings.TrimSpace(protocol))]
	return ok
}

// SchemeProtocol maps a supported share URI scheme to Rota's protocol name.
func SchemeProtocol(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	separator := strings.Index(raw, "://")
	if separator < 1 {
		return "", false
	}
	switch strings.ToLower(raw[:separator]) {
	case "vless":
		return "vless", true
	case "vmess":
		return "vmess", true
	case "trojan":
		return "trojan", true
	case "ss":
		return "shadowsocks", true
	default:
		return "", false
	}
}

// Parse validates and canonicalizes a supported share URI.
func Parse(raw string) (Node, error) {
	protocol, ok := SchemeProtocol(raw)
	if !ok {
		return Node{}, invalid("share URI scheme is not supported")
	}

	var node Node
	var err error
	switch protocol {
	case "vless":
		node, err = parseVLESS(raw)
	case "vmess":
		node, err = parseVMess(raw)
	case "trojan":
		node, err = parseTrojan(raw)
	case "shadowsocks":
		node, err = parseShadowsocks(raw)
	}
	if err != nil {
		return Node{}, err
	}
	node.identity = proxyidentity.Credential(node.credential)
	return node, nil
}

// ParseForProtocol additionally verifies the external protocol selection.
func ParseForProtocol(protocol, raw string) (Node, error) {
	node, err := Parse(raw)
	if err != nil {
		return Node{}, err
	}
	if node.Protocol() != strings.ToLower(strings.TrimSpace(protocol)) {
		return Node{}, invalid("share URI scheme does not match protocol")
	}
	return node, nil
}

func parseVLESS(raw string) (Node, error) {
	if strings.Contains(raw, "&amp;") {
		return Node{}, invalid("VLESS query contains an HTML entity")
	}
	u, err := parseURL(raw, "vless")
	if err != nil {
		return Node{}, err
	}
	if u.User == nil || u.User.Username() == "" || u.Port() == "" {
		u, err = decodeLegacyVLESSAuthority(u)
		if err != nil {
			return Node{}, err
		}
	}
	if u.User == nil || u.User.Username() == "" {
		return Node{}, invalid("VLESS UUID is missing")
	}
	if _, hasPassword := u.User.Password(); hasPassword {
		return Node{}, invalid("VLESS userinfo password is not supported")
	}
	id, err := canonicalUUID(u.User.Username())
	if err != nil {
		return Node{}, invalid("VLESS UUID is invalid")
	}
	host, port, address, err := endpoint(u)
	if err != nil {
		return Node{}, err
	}

	query := u.Query()
	network := lowerFirst(query, "type", "net", "network")
	if network == "" {
		network = "tcp"
	}
	security := lowerFirst(query, "security")
	publicKey := strings.TrimRight(first(query, "pbk", "publicKey"), "=")
	if security == "" && publicKey != "" {
		security = "reality"
	}
	if security == "" && truthy(first(query, "tls")) {
		security = "tls"
	}
	if security == "" {
		security = "none"
	}
	flow := first(query, "flow")
	if flow == "" && first(query, "xtls") == "2" {
		flow = "xtls-rprx-vision"
	}
	if flow != "" && flow != "xtls-rprx-vision" && flow != "xtls-rprx-vision-udp443" {
		return Node{}, invalid("VLESS flow is not supported")
	}
	if flow != "" && network != "tcp" && network != "raw" {
		return Node{}, invalid("VLESS Vision requires TCP transport")
	}
	encryption := lowerFirst(query, "encryption")
	if encryption == "" {
		encryption = "none"
	}
	if encryption != "none" {
		return Node{}, invalid("VLESS encryption is not supported")
	}
	packetEncoding := strings.ToLower(strings.TrimSpace(first(query, "packetEncoding")))
	if packetEncoding != "" && packetEncoding != "xudp" {
		return Node{}, invalid("VLESS packet encoding is not supported")
	}

	stream, canonicalStream, streamSecrets, err := parseStream(query, streamInput{
		Network:    network,
		Security:   security,
		ServerHost: host,
	})
	if err != nil {
		return Node{}, err
	}
	if security == "reality" && publicKey == "" {
		return Node{}, invalid("Reality public key is missing")
	}

	canonicalQuery := url.Values{
		"encryption": {encryption},
		"type":       {network},
		"security":   {security},
	}
	if flow != "" {
		canonicalQuery.Set("flow", flow)
	}
	mergeValues(canonicalQuery, canonicalStream)
	canonical := (&url.URL{
		Scheme:   "vless",
		User:     url.User(id),
		Host:     address,
		RawQuery: canonicalQuery.Encode(),
	}).String()

	settings := map[string]any{
		"vnext": []any{map[string]any{
			"address": host,
			"port":    port,
			"users": []any{map[string]any{
				"id":         id,
				"encryption": encryption,
				"flow":       flow,
			}},
		}},
	}
	return newNode("vless", address, nodeName(u, query), canonical, host, port,
		settings, stream, append([]string{id}, streamSecrets...)), nil
}

func parseVMess(raw string) (Node, error) {
	body := strings.TrimSpace(raw)[len("vmess://"):]
	decoded, err := decodeBase64Any(body)
	if err != nil {
		return Node{}, invalid("VMess payload is not valid Base64")
	}
	decoder := json.NewDecoder(strings.NewReader(string(decoded)))
	decoder.UseNumber()
	var document map[string]any
	if err := decoder.Decode(&document); err != nil {
		return Node{}, invalid("VMess payload is not valid JSON")
	}
	if aid := strings.TrimSpace(valueString(document["aid"])); aid != "" {
		value, err := strconv.Atoi(aid)
		if err != nil || value != 0 {
			return Node{}, invalid("VMess AlterID is not supported")
		}
	}

	host := strings.ToLower(strings.TrimSpace(valueString(document["add"])))
	if !validHost(host) {
		return Node{}, invalid("VMess host is invalid")
	}
	port, err := parsePort(valueString(document["port"]))
	if err != nil {
		return Node{}, invalid("VMess port is invalid")
	}
	address := net.JoinHostPort(host, strconv.Itoa(port))
	id, err := canonicalUUID(valueString(document["id"]))
	if err != nil {
		return Node{}, invalid("VMess UUID is invalid")
	}
	network := strings.ToLower(valueString(document["net"]))
	if network == "" {
		network = "tcp"
	}
	securityLayer := strings.ToLower(valueString(document["tls"]))
	if securityLayer == "" || securityLayer == "none" {
		securityLayer = "none"
	} else if securityLayer != "tls" {
		return Node{}, invalid("VMess transport security is not supported")
	}
	accountSecurity := strings.ToLower(valueString(document["scy"]))
	if accountSecurity == "" {
		accountSecurity = strings.ToLower(valueString(document["security"]))
	}
	switch accountSecurity {
	case "", "auto":
		accountSecurity = "auto"
	case "aes-128-gcm", "chacha20-poly1305", "none", "zero":
	case "null":
		accountSecurity = "none"
	default:
		return Node{}, invalid("VMess account security is not supported")
	}

	query := url.Values{}
	copyDocumentValue(query, document, "host", "host")
	copyDocumentValue(query, document, "path", "path")
	copyDocumentValue(query, document, "type", "headerType")
	copyDocumentValue(query, document, "sni", "sni")
	copyDocumentValue(query, document, "fp", "fp")
	copyDocumentValue(query, document, "alpn", "alpn")
	copyDocumentValue(query, document, "serviceName", "serviceName")
	copyDocumentValue(query, document, "authority", "authority")
	copyDocumentValue(query, document, "mode", "mode")
	if documentTruthy(document, "skip-cert-verify", "allowInsecure", "allowinsecure", "allow_insecure", "insecure") {
		query.Set("allowInsecure", "1")
	}
	stream, canonicalStream, streamSecrets, err := parseStream(query, streamInput{
		Network:    network,
		Security:   securityLayer,
		ServerHost: host,
	})
	if err != nil {
		return Node{}, err
	}

	canonicalDocument := map[string]any{
		"v":    "2",
		"add":  host,
		"port": strconv.Itoa(port),
		"id":   id,
		"net":  network,
		"tls":  emptyIfNone(securityLayer),
		"scy":  accountSecurity,
	}
	for key, values := range canonicalStream {
		if len(values) == 0 {
			continue
		}
		canonicalKey := key
		if key == "headerType" {
			canonicalKey = "type"
		}
		canonicalDocument[canonicalKey] = values[0]
	}
	encodedDocument, err := json.Marshal(canonicalDocument)
	if err != nil {
		return Node{}, invalid("VMess payload cannot be canonicalized")
	}
	canonical := "vmess://" + base64.RawStdEncoding.EncodeToString(encodedDocument)
	settings := map[string]any{
		"vnext": []any{map[string]any{
			"address": host,
			"port":    port,
			"users": []any{map[string]any{
				"id":       id,
				"security": accountSecurity,
			}},
		}},
	}
	name, err := cleanName(valueString(document["ps"]))
	if err != nil {
		return Node{}, err
	}
	return newNode("vmess", address, name, canonical, host, port,
		settings, stream, append([]string{id}, streamSecrets...)), nil
}

func parseTrojan(raw string) (Node, error) {
	if strings.Contains(raw, "&amp;") {
		return Node{}, invalid("Trojan query contains an HTML entity")
	}
	u, err := parseURL(raw, "trojan")
	if err != nil {
		return Node{}, err
	}
	if u.User == nil || u.User.Username() == "" {
		return Node{}, invalid("Trojan password is missing")
	}
	if _, hasPassword := u.User.Password(); hasPassword {
		return Node{}, invalid("Trojan userinfo format is invalid")
	}
	password := u.User.Username()
	host, port, address, err := endpoint(u)
	if err != nil {
		return Node{}, err
	}
	query := u.Query()
	if strings.TrimSpace(first(query, "flow")) != "" {
		return Node{}, invalid("Trojan flow is not supported")
	}
	network := lowerFirst(query, "type", "net", "network")
	if network == "" {
		network = "tcp"
	}
	security := lowerFirst(query, "security")
	if security == "" {
		if first(query, "pbk", "publicKey") != "" {
			security = "reality"
		} else {
			security = "tls"
		}
	}
	stream, canonicalStream, streamSecrets, err := parseStream(query, streamInput{
		Network:    network,
		Security:   security,
		ServerHost: host,
	})
	if err != nil {
		return Node{}, err
	}
	canonicalQuery := url.Values{"type": {network}, "security": {security}}
	mergeValues(canonicalQuery, canonicalStream)
	canonical := (&url.URL{
		Scheme:   "trojan",
		User:     url.User(password),
		Host:     address,
		RawQuery: canonicalQuery.Encode(),
	}).String()
	settings := map[string]any{
		"servers": []any{map[string]any{
			"address":  host,
			"port":     port,
			"password": password,
		}},
	}
	return newNode("trojan", address, nodeName(u, query), canonical, host, port,
		settings, stream, append([]string{password}, streamSecrets...)), nil
}

func parseShadowsocks(raw string) (Node, error) {
	raw = strings.TrimSpace(raw)
	withoutFragment := raw
	name := ""
	if index := strings.IndexByte(withoutFragment, '#'); index >= 0 {
		fragment, err := url.PathUnescape(withoutFragment[index+1:])
		if err != nil {
			return Node{}, invalid("Shadowsocks name is invalid")
		}
		name, err = cleanName(fragment)
		if err != nil {
			return Node{}, err
		}
		withoutFragment = withoutFragment[:index]
	}
	if index := strings.IndexByte(withoutFragment, '?'); index >= 0 {
		query, err := url.ParseQuery(withoutFragment[index+1:])
		if err != nil || query.Get("plugin") != "" {
			return Node{}, invalid("Shadowsocks plugins are not supported")
		}
		withoutFragment = withoutFragment[:index]
	}
	body := strings.TrimPrefix(withoutFragment, "ss://")
	userinfo, endpointText, ok := strings.Cut(body, "@")
	if !ok {
		decoded, err := decodeBase64Any(body)
		if err != nil {
			return Node{}, invalid("Shadowsocks payload is invalid")
		}
		userinfo, endpointText, ok = strings.Cut(string(decoded), "@")
		if !ok {
			return Node{}, invalid("Shadowsocks endpoint is missing")
		}
	}
	if !strings.Contains(userinfo, ":") {
		decoded, err := decodeBase64Any(userinfo)
		if err != nil {
			return Node{}, invalid("Shadowsocks credentials are invalid")
		}
		userinfo = string(decoded)
	} else if decoded, err := url.PathUnescape(userinfo); err == nil {
		userinfo = decoded
	}
	method, password, ok := strings.Cut(userinfo, ":")
	method = strings.ToLower(strings.TrimSpace(method))
	if !ok || password == "" {
		return Node{}, invalid("Shadowsocks credentials are invalid")
	}
	if _, ok := shadowsocksMethods[method]; !ok {
		return Node{}, invalid("Shadowsocks cipher is not supported")
	}
	if keyLength, ok := shadowsocks2022KeyLengths[method]; ok {
		key, err := base64.StdEncoding.DecodeString(password)
		if err != nil || len(key) != keyLength || base64.StdEncoding.EncodeToString(key) != password {
			return Node{}, invalid("Shadowsocks 2022 key is invalid")
		}
	}
	host, portText, err := net.SplitHostPort(endpointText)
	if err != nil || !validHost(strings.ToLower(host)) {
		return Node{}, invalid("Shadowsocks endpoint is invalid")
	}
	host = strings.ToLower(host)
	port, err := parsePort(portText)
	if err != nil {
		return Node{}, invalid("Shadowsocks port is invalid")
	}
	address := net.JoinHostPort(host, strconv.Itoa(port))
	encodedCredentials := base64.RawURLEncoding.EncodeToString([]byte(method + ":" + password))
	canonical := "ss://" + encodedCredentials + "@" + address
	settings := map[string]any{
		"servers": []any{map[string]any{
			"address":  host,
			"port":     port,
			"method":   method,
			"password": password,
		}},
	}
	return newNode("shadowsocks", address, name, canonical, host, port,
		settings, nil, []string{password}), nil
}

type streamInput struct {
	Network    string
	Security   string
	ServerHost string
}

func parseStream(query url.Values, input streamInput) (map[string]any, url.Values, []string, error) {
	network := strings.ToLower(strings.TrimSpace(input.Network))
	switch network {
	case "tcp", "raw":
		network = "tcp"
	case "ws", "websocket":
		network = "ws"
	case "grpc":
	case "xhttp", "splithttp":
		network = "xhttp"
	default:
		return nil, nil, nil, invalid("transport network is not supported")
	}
	security := strings.ToLower(strings.TrimSpace(input.Security))
	if security == "" {
		security = "none"
	}
	if security != "none" && security != "tls" && security != "reality" {
		return nil, nil, nil, invalid("transport security is not supported")
	}
	if security == "reality" && network == "ws" {
		return nil, nil, nil, invalid("Reality over WebSocket is not supported by Xray")
	}
	if first(query, "ech") != "" {
		return nil, nil, nil, invalid("ECH share parameters are not supported yet")
	}

	stream := map[string]any{"network": network, "security": security}
	canonical := url.Values{}
	secrets := []string{}
	host := strings.TrimSpace(first(query, "host"))
	path := strings.TrimSpace(first(query, "path"))
	headerType := strings.ToLower(strings.TrimSpace(first(query, "headerType")))
	serviceName := strings.TrimSpace(first(query, "serviceName"))
	authority := strings.TrimSpace(first(query, "authority"))
	mode := strings.ToLower(strings.TrimSpace(first(query, "mode")))

	switch network {
	case "tcp":
		if headerType == "" || headerType == "none" || headerType == "auto" || headerType == "---" {
			headerType = "none"
			stream["tcpSettings"] = map[string]any{"header": map[string]any{"type": "none"}}
		} else if headerType == "http" {
			if path == "" {
				path = "/"
			}
			request := map[string]any{"path": []string{path}}
			if host != "" {
				request["headers"] = map[string]any{"Host": []string{host}}
			}
			stream["tcpSettings"] = map[string]any{
				"header": map[string]any{"type": "http", "request": request},
			}
		} else {
			return nil, nil, nil, invalid("TCP header type is not supported")
		}
		if headerType == "http" {
			canonical.Set("headerType", headerType)
			canonical.Set("path", path)
			setIfNotEmpty(canonical, "host", host)
		}
	case "ws":
		if path == "" {
			path = "/"
		}
		stream["wsSettings"] = map[string]any{"path": path, "host": host}
		canonical.Set("path", path)
		setIfNotEmpty(canonical, "host", host)
	case "grpc":
		multiMode := false
		switch mode {
		case "", "gun":
		case "multi":
			multiMode = true
		default:
			return nil, nil, nil, invalid("gRPC mode is not supported")
		}
		stream["grpcSettings"] = map[string]any{
			"serviceName": serviceName,
			"authority":   authority,
			"multiMode":   multiMode,
		}
		setIfNotEmpty(canonical, "serviceName", serviceName)
		setIfNotEmpty(canonical, "authority", authority)
		setIfNotEmpty(canonical, "mode", mode)
	case "xhttp":
		if path == "" {
			path = "/"
		}
		if mode == "" {
			mode = "auto"
		}
		switch mode {
		case "auto", "packet-up", "stream-up", "stream-one":
		default:
			return nil, nil, nil, invalid("XHTTP mode is not supported")
		}
		settings := map[string]any{"path": path, "host": host, "mode": mode}
		if extra := strings.TrimSpace(first(query, "extra")); extra != "" {
			var extraDocument map[string]any
			if err := json.Unmarshal([]byte(extra), &extraDocument); err != nil {
				return nil, nil, nil, invalid("XHTTP extra configuration is invalid")
			}
			settings["extra"] = extraDocument
			canonical.Set("extra", extra)
		}
		stream["xhttpSettings"] = settings
		canonical.Set("path", path)
		canonical.Set("mode", mode)
		setIfNotEmpty(canonical, "host", host)
	}

	sni := strings.TrimSpace(first(query, "sni", "peer", "serverName"))
	if sni == "" && security == "tls" {
		sni = input.ServerHost
	}
	fingerprint := strings.ToLower(strings.TrimSpace(first(query, "fp", "fingerprint")))
	if fingerprint == "" && (security == "tls" || security == "reality") {
		fingerprint = "chrome"
	}
	if err := validateFingerprint(security, fingerprint); err != nil {
		return nil, nil, nil, err
	}
	alpn := splitCSV(first(query, "alpn"))
	insecure := truthy(first(query, "allowInsecure", "insecure", "skip-cert-verify"))

	switch security {
	case "tls":
		settings := map[string]any{
			"serverName":    sni,
			"fingerprint":   fingerprint,
			"allowInsecure": insecure,
		}
		if len(alpn) > 0 {
			settings["alpn"] = alpn
		}
		stream["tlsSettings"] = settings
		setIfNotEmpty(canonical, "sni", sni)
		setIfNotEmpty(canonical, "fp", fingerprint)
		if len(alpn) > 0 {
			canonical.Set("alpn", strings.Join(alpn, ","))
		}
		if insecure {
			canonical.Set("allowInsecure", "1")
		}
	case "reality":
		if sni == "" || !validHost(strings.ToLower(sni)) {
			return nil, nil, nil, invalid("Reality server name is invalid")
		}
		publicKey := strings.TrimRight(strings.TrimSpace(first(query, "pbk", "publicKey")), "=")
		decodedKey, err := base64.RawURLEncoding.DecodeString(publicKey)
		if err != nil || len(decodedKey) != 32 {
			return nil, nil, nil, invalid("Reality public key is invalid")
		}
		shortID := strings.ToLower(strings.TrimSpace(first(query, "sid", "shortId")))
		if len(shortID) > 16 || len(shortID)%2 != 0 {
			return nil, nil, nil, invalid("Reality short ID is invalid")
		}
		if _, err := hex.DecodeString(shortID); err != nil {
			return nil, nil, nil, invalid("Reality short ID is invalid")
		}
		spiderX := strings.TrimSpace(first(query, "spx"))
		if spiderX == "" {
			spiderX = "/"
		}
		if !strings.HasPrefix(spiderX, "/") {
			return nil, nil, nil, invalid("Reality spider path is invalid")
		}
		settings := map[string]any{
			"serverName":  strings.ToLower(sni),
			"fingerprint": fingerprint,
			"publicKey":   publicKey,
			"shortId":     shortID,
			"spiderX":     spiderX,
		}
		stream["realitySettings"] = settings
		canonical.Set("sni", strings.ToLower(sni))
		canonical.Set("fp", fingerprint)
		canonical.Set("pbk", publicKey)
		setIfNotEmpty(canonical, "sid", shortID)
		if spiderX != "/" {
			canonical.Set("spx", spiderX)
		}
		secrets = append(secrets, publicKey)
	}
	return stream, canonical, secrets, nil
}

func validateFingerprint(security, fingerprint string) error {
	if fingerprint == "" {
		return nil
	}
	switch security {
	case "none":
		return nil
	case "tls":
		if fingerprint == "unsafe" || xraytls.GetFingerprint(fingerprint) != nil {
			return nil
		}
	case "reality":
		if fingerprint != "unsafe" && fingerprint != "hellogolang" && xraytls.GetFingerprint(fingerprint) != nil {
			return nil
		}
	}
	return invalid("transport fingerprint is not supported")
}

func newNode(protocol, address, name, credential, host string, port int, settings map[string]any, stream map[string]any, secrets []string) Node {
	outbound := map[string]any{"protocol": protocol, "settings": settings}
	if protocol == "shadowsocks" {
		outbound["protocol"] = "shadowsocks"
	}
	if stream != nil {
		outbound["streamSettings"] = stream
	}
	return Node{
		protocol:   protocol,
		address:    address,
		name:       name,
		credential: credential,
		host:       host,
		port:       port,
		outbound:   outbound,
		secrets:    append([]string{credential}, secrets...),
	}
}

func parseURL(raw, scheme string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Opaque != "" || !strings.EqualFold(u.Scheme, scheme) {
		return nil, invalid(strings.ToUpper(scheme) + " URI cannot be parsed")
	}
	if u.Path != "" {
		return nil, invalid(strings.ToUpper(scheme) + " URI path is not supported")
	}
	return u, nil
}

func decodeLegacyVLESSAuthority(original *url.URL) (*url.URL, error) {
	if original == nil || original.Host == "" || original.User != nil || original.Port() != "" {
		return nil, invalid("VLESS endpoint is missing")
	}
	decoded, err := decodeBase64Any(original.Host)
	if err != nil {
		return nil, invalid("VLESS endpoint is missing")
	}
	authority := strings.TrimPrefix(strings.TrimSpace(string(decoded)), ":")
	legacy := "vless://" + authority
	if original.RawQuery != "" {
		legacy += "?" + original.RawQuery
	}
	if original.Fragment != "" {
		legacy += "#" + url.PathEscape(original.Fragment)
	}
	u, err := url.Parse(legacy)
	if err != nil || u.User == nil || u.Port() == "" {
		return nil, invalid("legacy VLESS endpoint is invalid")
	}
	return u, nil
}

func endpoint(u *url.URL) (string, int, string, error) {
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	if !validHost(host) {
		return "", 0, "", invalid("node host is invalid")
	}
	port, err := parsePort(u.Port())
	if err != nil {
		return "", 0, "", invalid("node port is invalid")
	}
	return host, port, net.JoinHostPort(host, strconv.Itoa(port)), nil
}

func parsePort(value string) (int, error) {
	port, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("invalid port")
	}
	return port, nil
}

func canonicalUUID(value string) (string, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(value))
	if err != nil {
		return "", err
	}
	return parsed.String(), nil
}

func validHost(host string) bool {
	if net.ParseIP(host) != nil {
		return true
	}
	if host == "" || len(host) > 253 || strings.HasPrefix(host, ".") || strings.HasSuffix(host, ".") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') &&
				(character < '0' || character > '9') && character != '-' && character != '_' {
				return false
			}
		}
	}
	return true
}

func nodeName(u *url.URL, query url.Values) string {
	name := strings.TrimSpace(u.Fragment)
	if name == "" {
		name = strings.TrimSpace(first(query, "remarks", "remark", "name", "ps"))
	}
	cleaned, _ := cleanName(name)
	return cleaned
}

func cleanName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if len(name) > maxNodeNameBytes || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return "", invalid("node name is invalid")
	}
	return name, nil
}

func decodeBase64Any(value string) ([]byte, error) {
	value = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, strings.TrimSpace(value))
	for _, encoding := range []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	} {
		if decoded, err := encoding.DecodeString(value); err == nil {
			return decoded, nil
		}
	}
	return nil, fmt.Errorf("invalid base64")
}

func first(values url.Values, keys ...string) string {
	for _, key := range keys {
		if value := values.Get(key); value != "" {
			return value
		}
	}
	return ""
}

func lowerFirst(values url.Values, keys ...string) string {
	return strings.ToLower(strings.TrimSpace(first(values, keys...)))
}

func setIfNotEmpty(values url.Values, key, value string) {
	if value != "" {
		values.Set(key, value)
	}
}

func mergeValues(destination, source url.Values) {
	for key, values := range source {
		for _, value := range values {
			destination.Add(key, value)
		}
	}
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func truthy(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on", "tls":
		return true
	default:
		return false
	}
}

func valueString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(typed)
	default:
		return ""
	}
}

func copyDocumentValue(values url.Values, document map[string]any, sourceKey, destinationKey string) {
	if value := strings.TrimSpace(valueString(document[sourceKey])); value != "" {
		values.Set(destinationKey, value)
	}
}

func documentTruthy(document map[string]any, keys ...string) bool {
	for _, key := range keys {
		if truthy(valueString(document[key])) {
			return true
		}
	}
	return false
}

func emptyIfNone(value string) string {
	if value == "none" {
		return ""
	}
	return value
}

func invalid(reason string) error {
	return fmt.Errorf("invalid share URI: %s", reason)
}
