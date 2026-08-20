package proxy

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
	proxyDialer "golang.org/x/net/proxy"
	"h12.io/socks"
)

const maxCONNECTResponseHeaderBytes = 8 * 1024

// connectViaSocks5 dials host through a SOCKS5 proxy.
func connectViaSocks5(p *models.Proxy, host string) (net.Conn, error) {
	var auth *proxyDialer.Auth
	if p.Username != nil && *p.Username != "" {
		pw := ""
		if p.Password != nil {
			pw = *p.Password
		}
		auth = &proxyDialer.Auth{User: *p.Username, Password: pw}
	}
	dialer, err := proxyDialer.SOCKS5("tcp", p.Address, auth, proxyDialer.Direct)
	if err != nil {
		return nil, fmt.Errorf("socks5 dialer: %w", err)
	}
	conn, err := dialer.Dial("tcp", host)
	if err != nil {
		return nil, fmt.Errorf("socks5 dial %s via %s: %w", host, p.Address, err)
	}
	return conn, nil
}

// connectViaSocks4 dials host through a SOCKS4/SOCKS4A proxy.
func connectViaSocks4(p *models.Proxy, host string) (net.Conn, error) {
	proxyURL := &url.URL{Scheme: p.Protocol, Host: p.Address}
	// h12.io/socks implements the anonymous SOCKS4/SOCKS4A form used by the
	// transport path. Its URI credentials are SOCKS5-only and are not a SOCKS4
	// user-id implementation.
	conn, err := socks.Dial(proxyURL.String())("tcp", host)
	if err != nil {
		return nil, fmt.Errorf("socks4 dial %s via %s: %w", host, p.Address, err)
	}
	return conn, nil
}

// connectViaHTTPStandalone sends a CONNECT request to an HTTP proxy.
func connectViaHTTPStandalone(p *models.Proxy, host string, timeout time.Duration) (net.Conn, error) {
	if timeout < 30*time.Second {
		timeout = 30 * time.Second
	}

	conn, err := dialHTTPProxyEndpoint(p, timeout)
	if err != nil {
		return nil, fmt.Errorf("dial proxy %s: %w", p.Address, err)
	}

	_ = conn.SetDeadline(time.Now().Add(timeout))

	req := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n", host, host)
	if p.Username != nil && *p.Username != "" {
		pw := ""
		if p.Password != nil {
			pw = *p.Password
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(*p.Username + ":" + pw))
		req += "Proxy-Authorization: Basic " + encoded + "\r\n"
	}
	req += "User-Agent: Rota-Proxy/1.0\r\nProxy-Connection: Keep-Alive\r\n\r\n"

	if _, err := conn.Write([]byte(req)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("send CONNECT to %s: %w", p.Address, err)
	}

	response, err := readCONNECTResponse(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read CONNECT response from %s: %w", p.Address, err)
	}
	if response.StatusCode != http.StatusOK {
		conn.Close()
		return nil, fmt.Errorf("CONNECT to %s rejected: %s", p.Address, response.Status)
	}

	if err := conn.SetDeadline(time.Time{}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("clear CONNECT deadline for %s: %w", p.Address, err)
	}
	return conn, nil
}

// readCONNECTResponse stops exactly at the end of the HTTP header block. It
// cannot consume tunnel bytes that an upstream proxy pipelines after its 200.
func readCONNECTResponse(conn net.Conn) (*http.Response, error) {
	header := make([]byte, 0, 512)
	oneByte := []byte{0}
	for len(header) < maxCONNECTResponseHeaderBytes {
		n, err := conn.Read(oneByte)
		if n > 0 {
			header = append(header, oneByte[0])
			if bytes.HasSuffix(header, []byte("\r\n\r\n")) {
				response, parseErr := http.ReadResponse(
					bufio.NewReader(bytes.NewReader(header)),
					&http.Request{Method: http.MethodConnect},
				)
				if parseErr != nil {
					return nil, fmt.Errorf("parse CONNECT response: %w", parseErr)
				}
				return response, nil
			}
		}
		if err != nil {
			return nil, fmt.Errorf("incomplete CONNECT response: %w", err)
		}
	}
	return nil, fmt.Errorf("CONNECT response headers exceed %d bytes", maxCONNECTResponseHeaderBytes)
}

func dialHTTPProxyEndpoint(p *models.Proxy, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", p.Address, timeout)
	if err != nil {
		return nil, err
	}
	if p.Protocol != "https" {
		return conn, nil
	}

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		conn.Close()
		return nil, err
	}
	tlsConn := tls.Client(conn, &tls.Config{
		InsecureSkipVerify: true, // Public HTTPS proxies commonly use self-signed or IP-mismatched certificates.
	})
	if err := tlsConn.Handshake(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("TLS handshake with proxy: %w", err)
	}
	return tlsConn, nil
}
