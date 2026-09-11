// node-forward is an opt-in, loopback-only Rota data plane. Unlike cmd/server,
// it never opens a database, runs migrations, selects a proxy, or polls a pool.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alpkeskin/rota/core/internal/nodeforward"
	"github.com/alpkeskin/rota/core/internal/sharenode"
)

func loopback(address string) (net.Listener, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return nil, errors.New("listener must use a loopback IP")
	}
	return net.Listen("tcp", address)
}

func run() error {
	node := flag.String("node-id", "", "registered node ID")
	slots := flag.String("slots", "worker-1", "comma-separated local worker slot names")
	publicFile := flag.String("public-key-file", "", "center Ed25519 public key, PEM")
	tokenFile := flag.String("control-token-file", "", "local control token file")
	proxyAddress := flag.String("proxy-listen", "127.0.0.1:8000", "loopback proxy listener")
	controlAddress := flag.String("control-listen", "127.0.0.1:8001", "loopback control listener")
	flag.Parse()
	keyData, err := os.ReadFile(*publicFile)
	if err != nil {
		return errors.New("cannot read public key file")
	}
	block, rest := pem.Decode(keyData)
	if block == nil || len(strings.TrimSpace(string(rest))) != 0 {
		return errors.New("invalid public key PEM")
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return errors.New("invalid public key")
	}
	public, ok := key.(ed25519.PublicKey)
	if !ok {
		return errors.New("Ed25519 public key required")
	}
	info, err := os.Stat(*tokenFile)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return errors.New("control token must be an owner-only regular file")
	}
	token, err := os.ReadFile(*tokenFile)
	if err != nil || len(token) > 4096 {
		return errors.New("cannot read control token")
	}
	relay, err := nodeforward.New(nodeforward.Config{NodeID: *node, PublicKey: public, ControlToken: strings.TrimSpace(string(token)), Slots: strings.Split(*slots, ",")})
	if err != nil {
		return err
	}
	defer relay.Close()
	defer sharenode.CloseAllRuntimes()
	proxyListener, err := loopback(*proxyAddress)
	if err != nil {
		return err
	}
	defer proxyListener.Close()
	controlListener, err := loopback(*controlAddress)
	if err != nil {
		return err
	}
	defer controlListener.Close()
	proxy := &http.Server{Handler: relay, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	control := &http.Server{Handler: relay.ControlHandler(), ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	failures := make(chan error, 2)
	go func() { failures <- proxy.Serve(proxyListener) }()
	go func() { failures <- control.Serve(controlListener) }()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"event": "node_forward_ready", "node_id": *node,
		"proxy_address": proxyListener.Addr().String(), "control_address": controlListener.Addr().String()})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
	case err = <-failures:
	}
	relay.Close() // explicitly closes hijacked tunnels; http.Shutdown cannot.
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = proxy.Shutdown(shutdown)
	_ = control.Shutdown(shutdown)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
