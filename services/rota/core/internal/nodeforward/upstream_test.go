package nodeforward

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSOCKSProtocolsPreserveDestinationAndCredentials(t *testing.T) {
	for _, protocol := range []string{"socks4", "socks4a", "socks5"} {
		t.Run(protocol, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan error, 1)
			go func() {
				c, err := listener.Accept()
				if err != nil {
					done <- err
					return
				}
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(2 * time.Second))
				reader := bufio.NewReader(c)
				if protocol == "socks5" {
					err = checkSOCKS5(reader, c)
				} else {
					err = checkSOCKS4(reader, c, protocol)
				}
				if err == nil {
					_, err = io.WriteString(c, "selected")
				}
				done <- err
			}()
			dial, err := newDialer(Upstream{Protocol: protocol, Address: listener.Addr().String(), Username: "name", Password: "p@ss"})
			if err != nil {
				t.Fatal(err)
			}
			target := "www.youtube.com:443"
			if protocol == "socks4" {
				target = "203.0.113.1:443"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			c, err := dial(ctx, target)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			readText(t, c, "selected")
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func checkSOCKS4(reader *bufio.Reader, c net.Conn, protocol string) error {
	head := make([]byte, 8)
	if _, err := io.ReadFull(reader, head); err != nil {
		return err
	}
	if head[0] != 4 || head[1] != 1 || binary.BigEndian.Uint16(head[2:4]) != 443 {
		return errors.New("invalid SOCKS4 target")
	}
	user, err := reader.ReadString(0)
	if err != nil || user != "\x00" {
		return errors.New("SOCKS4 userid mismatch")
	}
	if protocol == "socks4a" {
		host, err := reader.ReadString(0)
		if err != nil || host != "www.youtube.com\x00" || string(head[4:8]) != "\x00\x00\x00\x01" {
			return errors.New("SOCKS4a host mismatch")
		}
	} else if net.IP(head[4:8]).String() != "203.0.113.1" {
		return errors.New("SOCKS4 IP mismatch")
	}
	_, err = c.Write([]byte{0, 90, 0, 0, 0, 0, 0, 0})
	return err
}

func checkSOCKS5(reader *bufio.Reader, c net.Conn) error {
	header := make([]byte, 2)
	if _, err := io.ReadFull(reader, header); err != nil {
		return err
	}
	if header[0] != 5 {
		return errors.New("not SOCKS5")
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(reader, methods); err != nil {
		return err
	}
	if _, err := c.Write([]byte{5, 2}); err != nil {
		return err
	}
	if _, err := io.ReadFull(reader, header); err != nil {
		return err
	}
	username := make([]byte, int(header[1]))
	if _, err := io.ReadFull(reader, username); err != nil {
		return err
	}
	n, err := reader.ReadByte()
	if err != nil {
		return err
	}
	password := make([]byte, int(n))
	if _, err := io.ReadFull(reader, password); err != nil {
		return err
	}
	if header[0] != 1 || string(username) != "name" || string(password) != "p@ss" {
		return errors.New("SOCKS5 auth mismatch")
	}
	if _, err := c.Write([]byte{1, 0}); err != nil {
		return err
	}
	request := make([]byte, 5)
	if _, err := io.ReadFull(reader, request); err != nil {
		return err
	}
	if request[0] != 5 || request[1] != 1 || request[3] != 3 {
		return errors.New("SOCKS5 destination must stay remote DNS")
	}
	host := make([]byte, int(request[4]))
	if _, err := io.ReadFull(reader, host); err != nil {
		return err
	}
	if _, err := io.ReadFull(reader, header); err != nil {
		return err
	}
	if string(host) != "www.youtube.com" || binary.BigEndian.Uint16(header) != 443 {
		return fmt.Errorf("SOCKS5 target mismatch")
	}
	_, err = c.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 1})
	return err
}

func TestHTTPSUpstreamDoesNotBypassCertificateVerification(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("untrusted TLS upstream reached HTTP handler") }))
	defer server.Close()
	dial, err := newDialer(Upstream{Protocol: "https", Address: strings.TrimPrefix(server.URL, "https://")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := dial(ctx, "www.youtube.com:443")
	if c != nil {
		c.Close()
	}
	if err == nil {
		t.Fatal("untrusted proxy certificate accepted")
	}
}

func TestCONNECTHeaderBound(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go func() {
		reader := bufio.NewReader(b)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if line == "\r\n" {
				break
			}
		}
		_, _ = io.WriteString(b, "HTTP/1.1 200 OK\r\nX: "+strings.Repeat("x", 9000))
	}()
	_ = a.SetDeadline(time.Now().Add(time.Second))
	if err := handshakeHTTP(a, Upstream{}, "www.youtube.com:443"); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("header bound: %v", err)
	}
}
