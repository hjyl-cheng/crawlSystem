//go:build linux

package proxy

import (
	"net"

	"golang.org/x/sys/unix"
)

// trySplice delegates to io.CopyBuffer, which uses net.TCPConn's runtime-aware
// ReadFrom/WriteTo fast paths on Linux. A previous implementation ran an
// unbounded splice pump while holding SyscallConn Read/Write locks, causing a
// concurrent tunnel retirement to block in net.Conn.Close.
func trySplice(net.Conn, net.Conn) (bool, error) {
	return false, nil
}

func pollFDWithInterval(fd int, write bool, intervalMS int) error {
	events := int16(unix.POLLIN)
	if write {
		events = unix.POLLOUT
	}
	fds := []unix.PollFd{{Fd: int32(fd), Events: events}}
	for {
		n, err := unix.Poll(fds, intervalMS)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			return err
		}
		if n == 0 {
			continue
		}
		return nil
	}
}
