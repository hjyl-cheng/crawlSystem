package proxy

import (
	"io"
	"net"
	"sync"
)

// BidirectionalCopy copies data between client and upstream in both directions
// concurrently. It returns when either direction encounters an error or EOF.
// On TCP connections, io.CopyBuffer delegates to Go's runtime-aware
// TCPConn.ReadFrom/WriteTo fast paths when the platform supports them.
func BidirectionalCopy(client, upstream net.Conn) error {
	var wg sync.WaitGroup
	var clientErr, upstreamErr error

	wg.Add(2)

	// upstream → client
	go func() {
		defer wg.Done()
		clientErr = copyOneDirection(client, upstream)
		// When upstream closes or errors, half-close the client write side
		// so the client knows there's no more data coming.
		if tc, ok := client.(*net.TCPConn); ok {
			tc.CloseWrite() //nolint:errcheck
		}
	}()

	// client → upstream
	go func() {
		defer wg.Done()
		upstreamErr = copyOneDirection(upstream, client)
		// When client closes or errors, half-close the upstream write side.
		if tc, ok := upstream.(*net.TCPConn); ok {
			tc.CloseWrite() //nolint:errcheck
		}
	}()

	wg.Wait()

	// Return whichever error is more meaningful
	if clientErr != nil {
		return clientErr
	}
	return upstreamErr
}

// copyOneDirection copies from src to dst using the most efficient method
// available on the current platform. Platform hooks may handle the transfer;
// otherwise io.CopyBuffer delegates to the standard library TCP fast paths.
func copyOneDirection(dst, src net.Conn) error {
	// Try a bounded platform-specific implementation when one is available.
	ok, err := trySplice(dst, src)
	if ok {
		return err
	}

	// Standard io.Copy uses TCP fast paths when possible and otherwise copies
	// through the pooled userspace buffer.
	buf := bufPool.Get().([]byte)
	defer bufPool.Put(buf)
	_, err = io.CopyBuffer(dst, src, buf)
	return err
}

// bufPool reuses 32KB buffers for io.CopyBuffer to reduce GC pressure.
var bufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 32*1024)
		return buf
	},
}
