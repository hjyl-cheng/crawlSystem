package vless

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeRuntimeFactory struct {
	mu     sync.Mutex
	starts map[string]int
	closes map[string]int
}

func newFakeRuntimeFactory() *fakeRuntimeFactory {
	return &fakeRuntimeFactory{
		starts: make(map[string]int),
		closes: make(map[string]int),
	}
}

func (f *fakeRuntimeFactory) start(node Node) (*runtime, error) {
	credential := node.credential
	f.mu.Lock()
	f.starts[credential]++
	f.mu.Unlock()
	return &runtime{
		dialFn: func(context.Context, string, string) (net.Conn, error) {
			client, server := net.Pipe()
			_ = server.Close()
			return client, nil
		},
		closeFn: func() error {
			f.mu.Lock()
			f.closes[credential]++
			f.mu.Unlock()
			return nil
		},
	}, nil
}

func (f *fakeRuntimeFactory) counts(credential string) (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starts[credential], f.closes[credential]
}

func fakeNode(credential string) Node {
	return Node{credential: credential}
}

func dialAndClose(t *testing.T, dial ContextDialer) {
	t.Helper()
	conn, err := dial(context.Background(), "tcp", "example.com:443")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close connection: %v", err)
	}
}

func TestRuntimeCacheEvictsLeastRecentlyUsedUnleasedEntry(t *testing.T) {
	factory := newFakeRuntimeFactory()
	cache := newRuntimeCache(2, factory.start)
	now := time.Unix(1, 0)
	cache.now = func() time.Time { return now }
	node1, node2, node3 := fakeNode("one"), fakeNode("two"), fakeNode("three")

	dialAndClose(t, cache.dialer(node1))
	now = now.Add(time.Second)
	dialAndClose(t, cache.dialer(node2))
	now = now.Add(time.Second)
	dialAndClose(t, cache.dialer(node1))
	now = now.Add(time.Second)
	dialAndClose(t, cache.dialer(node3))

	_, closes1 := factory.counts("one")
	_, closes2 := factory.counts("two")
	if closes1 != 0 || closes2 != 1 {
		t.Fatalf("close counts: one=%d two=%d", closes1, closes2)
	}
}

func TestRuntimeCacheNeverEvictsLeasedRuntime(t *testing.T) {
	factory := newFakeRuntimeFactory()
	cache := newRuntimeCache(2, factory.start)
	node1, node2, node3 := fakeNode("one"), fakeNode("two"), fakeNode("three")
	conn1, err := cache.dialer(node1)(context.Background(), "tcp", "example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	conn2, err := cache.dialer(node2)(context.Background(), "tcp", "example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.dialer(node3)(context.Background(), "tcp", "example.com:443"); !errors.Is(err, ErrRuntimeCapacity) {
		t.Fatalf("capacity error = %v", err)
	}
	if err := conn1.Close(); err != nil {
		t.Fatal(err)
	}
	dialAndClose(t, cache.dialer(node3))
	if err := conn2.Close(); err != nil {
		t.Fatal(err)
	}
	_, closes1 := factory.counts("one")
	_, closes2 := factory.counts("two")
	if closes1 != 1 || closes2 != 0 {
		t.Fatalf("close counts: one=%d two=%d", closes1, closes2)
	}
}

func TestOldDialerReacquiresRuntimeAfterEviction(t *testing.T) {
	factory := newFakeRuntimeFactory()
	cache := newRuntimeCache(1, factory.start)
	node1, node2 := fakeNode("one"), fakeNode("two")
	dial1 := cache.dialer(node1)
	dialAndClose(t, dial1)
	dialAndClose(t, cache.dialer(node2))
	dialAndClose(t, dial1)

	starts, closes := factory.counts("one")
	if starts != 2 || closes != 1 {
		t.Fatalf("node one starts=%d closes=%d", starts, closes)
	}
}

func TestConcurrentDialsShareOneStartingRuntime(t *testing.T) {
	factory := newFakeRuntimeFactory()
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	var startOnce sync.Once
	cache := newRuntimeCache(2, func(node Node) (*runtime, error) {
		startOnce.Do(func() { close(startEntered) })
		<-releaseStart
		return factory.start(node)
	})
	node := fakeNode("shared")
	const callers = 12
	var wg sync.WaitGroup
	errorsSeen := make(chan error, callers)
	for range callers {
		wg.Go(func() {
			conn, err := cache.dialer(node)(context.Background(), "tcp", "example.com:443")
			if err == nil {
				err = conn.Close()
			}
			errorsSeen <- err
		})
	}
	<-startEntered
	close(releaseStart)
	wg.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatalf("concurrent dial: %v", err)
		}
	}
	starts, _ := factory.counts("shared")
	if starts != 1 {
		t.Fatalf("runtime starts = %d, want 1", starts)
	}
}

func TestRuntimeStartDoesNotHoldCacheLock(t *testing.T) {
	factory := newFakeRuntimeFactory()
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	cache := newRuntimeCache(2, func(node Node) (*runtime, error) {
		if node.credential == "slow" {
			close(startEntered)
			<-releaseStart
		}
		return factory.start(node)
	})

	conn, err := cache.dialer(fakeNode("existing"))(context.Background(), "tcp", "example.com:443")
	if err != nil {
		t.Fatalf("existing dial: %v", err)
	}
	slowDone := make(chan error, 1)
	go func() {
		slowConn, slowErr := cache.dialer(fakeNode("slow"))(context.Background(), "tcp", "example.com:443")
		if slowErr == nil {
			slowErr = slowConn.Close()
		}
		slowDone <- slowErr
	}()
	<-startEntered

	closeDone := make(chan error, 1)
	go func() { closeDone <- conn.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("close existing connection: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("connection release blocked behind runtime startup")
	}
	close(releaseStart)
	if err := <-slowDone; err != nil {
		t.Fatalf("slow dial: %v", err)
	}
}

func TestRuntimeCacheCloseWaitsForStartingRuntime(t *testing.T) {
	factory := newFakeRuntimeFactory()
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	cache := newRuntimeCache(1, func(node Node) (*runtime, error) {
		close(startEntered)
		<-releaseStart
		return factory.start(node)
	})
	node := fakeNode("starting")
	dialDone := make(chan error, 1)
	go func() {
		conn, err := cache.dialer(node)(context.Background(), "tcp", "example.com:443")
		if conn != nil {
			closeErr := conn.Close()
			if err == nil {
				err = closeErr
			}
		}
		dialDone <- err
	}()
	<-startEntered

	closeDone := make(chan error, 1)
	go func() { closeDone <- cache.CloseAll() }()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		cache.mu.Lock()
		closed := cache.closed
		cache.mu.Unlock()
		if closed {
			break
		}
		select {
		case <-poll.C:
		case <-deadline.C:
			t.Fatal("close did not mark the cache closed")
		}
	}
	select {
	case err := <-closeDone:
		t.Fatalf("close returned before startup finished: %v", err)
	default:
	}
	close(releaseStart)
	if err := <-closeDone; err != nil {
		t.Fatalf("close all: %v", err)
	}
	if err := <-dialDone; !errors.Is(err, errRuntimeCacheClosed) {
		t.Fatalf("dial error = %v, want cache closed", err)
	}
	_, closes := factory.counts("starting")
	if closes != 1 {
		t.Fatalf("runtime closes = %d, want 1", closes)
	}
}

func TestRuntimeCacheCloseAllIsIdempotentAndRejectsNewDials(t *testing.T) {
	factory := newFakeRuntimeFactory()
	cache := newRuntimeCache(2, factory.start)
	node1, node2 := fakeNode("one"), fakeNode("two")
	conn1, err := cache.dialer(node1)(context.Background(), "tcp", "example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	conn2, err := cache.dialer(node2)(context.Background(), "tcp", "example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.CloseAll(); err != nil {
		t.Fatalf("close all: %v", err)
	}
	if err := cache.CloseAll(); err != nil {
		t.Fatalf("second close all: %v", err)
	}
	if _, err := cache.dialer(node1)(context.Background(), "tcp", "example.com:443"); err == nil {
		t.Fatal("closed cache accepted a dial")
	}
	_ = conn1.Close()
	_ = conn2.Close()
	for _, credential := range []string{"one", "two"} {
		_, closes := factory.counts(credential)
		if closes != 1 {
			t.Fatalf("%s close count = %d", credential, closes)
		}
	}
}

func TestRuntimeCloseErrorRedactsSecrets(t *testing.T) {
	runtime := &runtime{
		secrets: []string{"secret-credential", "secret-uuid"},
		closeFn: func() error {
			return errors.New("failed for secret-credential and secret-uuid")
		},
	}
	err := runtime.Close()
	if err == nil {
		t.Fatal("expected close error")
	}
	if strings.Contains(err.Error(), "secret-credential") || strings.Contains(err.Error(), "secret-uuid") {
		t.Fatalf("close error leaked secret: %v", err)
	}
}
