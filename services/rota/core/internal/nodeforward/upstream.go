// Package nodeforward is an opt-in, database-free Rota data plane. It does not
// select proxies or own retry budgets; the center authorizes exactly one route.
package nodeforward

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/alpkeskin/rota/core/internal/sharenode"
	socks "golang.org/x/net/proxy"
)

// This credential DTO is only carried inside a signed, confidential control
// request. Never put it in status responses, errors, or ordinary task payloads.
type Upstream struct {
	Protocol string `json:"protocol"`
	Address  string `json:"address"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

type Dialer func(context.Context, string) (net.Conn, error)

func newDialer(p Upstream) (Dialer, error) {
	host, portText, err := net.SplitHostPort(p.Address)
	port, portErr := strconv.Atoi(portText)
	if err != nil || host == "" || portErr != nil || port < 1 || port > 65535 || strings.ContainsAny(host, "\r\n\x00") {
		return nil, errors.New("invalid upstream address")
	}
	if sharenode.IsProtocol(p.Protocol) {
		node, err := sharenode.ParseForProtocol(p.Protocol, p.Password)
		if err != nil || node.Address() != p.Address {
			return nil, errors.New("invalid share node credential")
		}
		dial, err := sharenode.NewDialer(p.Protocol, node.Credential())
		if err != nil {
			return nil, errors.New("unsupported share node")
		}
		return func(ctx context.Context, target string) (net.Conn, error) { return dial(ctx, "tcp", target) }, nil
	}
	switch p.Protocol {
	case "socks5":
		var auth *socks.Auth
		if p.Username != "" {
			auth = &socks.Auth{User: p.Username, Password: p.Password}
		}
		d, err := socks.SOCKS5("tcp", p.Address, auth, &net.Dialer{Timeout: 20 * time.Second})
		if err != nil {
			return nil, errors.New("invalid SOCKS5 configuration")
		}
		return func(ctx context.Context, target string) (net.Conn, error) {
			return d.(socks.ContextDialer).DialContext(ctx, "tcp", target)
		}, nil
	case "http", "https", "socks4", "socks4a":
		return func(ctx context.Context, target string) (net.Conn, error) {
			return dialHandshake(ctx, p, target)
		}, nil
	default:
		return nil, errors.New("unsupported upstream protocol")
	}
}

// Cancel closes even a handshake stalled before CONNECT/SOCKS has completed.
// Existing Rota's non-context SOCKS4 helper cannot provide this guarantee.
func dialHandshake(ctx context.Context, p Upstream, target string) (result net.Conn, err error) {
	conn, err := (&net.Dialer{Timeout: 20 * time.Second}).DialContext(ctx, "tcp", p.Address)
	if err != nil {
		return nil, err
	}
	cancelDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = conn.Close(); close(cancelDone) })
	defer func() {
		if !stop() {
			<-cancelDone
		}
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		if err != nil {
			_ = conn.Close()
			result = nil
		}
	}()
	deadline := time.Now().Add(20 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err = conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	var stream net.Conn = conn
	if p.Protocol == "https" {
		host, _, _ := net.SplitHostPort(p.Address)
		// Node forwarding requires a valid proxy certificate; no blanket TLS bypass.
		t := tls.Client(conn, &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12})
		if err = t.HandshakeContext(ctx); err != nil {
			return nil, err
		}
		stream = t
	}
	if p.Protocol == "socks4" || p.Protocol == "socks4a" {
		err = handshakeSOCKS4(ctx, stream, p.Protocol, target)
	} else {
		err = handshakeHTTP(stream, p, target)
	}
	if err != nil {
		return nil, err
	}
	if err = conn.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	return stream, nil
}

func handshakeHTTP(conn net.Conn, p Upstream, target string) error {
	request := "CONNECT " + target + " HTTP/1.1\r\nHost: " + target + "\r\n"
	if p.Username != "" {
		request += "Proxy-Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte(p.Username+":"+p.Password)) + "\r\n"
	}
	if _, err := io.WriteString(conn, request+"\r\n"); err != nil {
		return err
	}
	// Do not read beyond the header: upstream may pipeline tunnel bytes.
	header := make([]byte, 0, 512)
	one := []byte{0}
	for len(header) < 8192 {
		if _, err := io.ReadFull(conn, one); err != nil {
			return err
		}
		header = append(header, one[0])
		if bytes.HasSuffix(header, []byte("\r\n\r\n")) {
			response, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(header)), &http.Request{Method: http.MethodConnect})
			if err != nil {
				return errors.New("invalid upstream CONNECT response")
			}
			if response.StatusCode != 200 {
				return errors.New("upstream CONNECT rejected")
			}
			return nil
		}
	}
	return errors.New("upstream CONNECT headers too large")
}

func handshakeSOCKS4(ctx context.Context, conn net.Conn, protocol, target string) error {
	host, portText, err := net.SplitHostPort(target)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 || strings.ContainsRune(host, 0) {
		return errors.New("invalid SOCKS4 target")
	}
	ip := net.ParseIP(host).To4()
	if ip == nil && protocol == "socks4" {
		addresses, err := net.DefaultResolver.LookupIP(ctx, "ip4", host)
		if err != nil || len(addresses) == 0 {
			return errors.New("SOCKS4 target resolution failed")
		}
		ip = addresses[0].To4()
	}
	packet := []byte{4, 1, 0, 0, 0, 0, 0, 1, 0} // anonymous userid, as in Rota
	binary.BigEndian.PutUint16(packet[2:4], uint16(port))
	if ip != nil {
		copy(packet[4:8], ip)
	} else {
		packet = append(packet, append([]byte(host), 0)...)
	}
	if _, err := conn.Write(packet); err != nil {
		return err
	}
	response := make([]byte, 8)
	if _, err := io.ReadFull(conn, response); err != nil {
		return err
	}
	if response[0] != 0 || response[1] != 90 {
		return errors.New("upstream SOCKS4 rejected")
	}
	return nil
}
