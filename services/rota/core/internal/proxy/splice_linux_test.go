//go:build linux

package proxy

import (
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestPollFDSurvivesIdleIntervalsUntilReady(t *testing.T) {
	pipeFDs := make([]int, 2)
	if err := unix.Pipe2(pipeFDs, unix.O_NONBLOCK|unix.O_CLOEXEC); err != nil {
		t.Fatalf("create pipe: %v", err)
	}
	defer unix.Close(pipeFDs[0])
	defer unix.Close(pipeFDs[1])

	done := make(chan error, 1)
	go func() {
		done <- pollFDWithInterval(pipeFDs[0], false, 5)
	}()

	time.Sleep(30 * time.Millisecond)
	if _, err := unix.Write(pipeFDs[1], []byte{1}); err != nil {
		t.Fatalf("make pipe readable: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("poll returned after an idle interval: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("poll did not wake after the descriptor became ready")
	}
}
