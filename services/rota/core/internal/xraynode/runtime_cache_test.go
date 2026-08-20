package xraynode

import (
	"context"
	"net"
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
	return &fakeRuntimeFactory{starts: make(map[string]int), closes: make(map[string]int)}
}

func (f *fakeRuntimeFactory) start(node Node) (*runtime, error) {
	key := node.cacheKey()
	f.mu.Lock()
	f.starts[key]++
	f.mu.Unlock()
	return &runtime{
		dialFn: func(context.Context, string, string) (net.Conn, error) {
			client, server := net.Pipe()
			_ = server.Close()
			return client, nil
		},
		closeFn: func() error {
			f.mu.Lock()
			f.closes[key]++
			f.mu.Unlock()
			return nil
		},
	}, nil
}

func fakeNode(key string) Node { return Node{identity: key} }

func dialAndClose(t *testing.T, dial ContextDialer) {
	t.Helper()
	conn, err := dial(context.Background(), "tcp", "example.com:443")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestRuntimeCacheEvictsOnlyUnleasedLeastRecentlyUsedEntry(t *testing.T) {
	factory := newFakeRuntimeFactory()
	cache := newRuntimeCache(2, factory.start)
	now := time.Unix(1, 0)
	cache.now = func() time.Time { return now }
	one, two, three := fakeNode("one"), fakeNode("two"), fakeNode("three")

	conn, err := cache.dialer(one)(context.Background(), "tcp", "example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	dialAndClose(t, cache.dialer(two))
	dialAndClose(t, cache.dialer(three))
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	factory.mu.Lock()
	defer factory.mu.Unlock()
	if factory.closes["one"] != 0 || factory.closes["two"] != 1 {
		t.Fatalf("close counts = %#v", factory.closes)
	}
}

func TestRuntimeCacheConcurrentDialsShareStartup(t *testing.T) {
	factory := newFakeRuntimeFactory()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	cache := newRuntimeCache(2, func(node Node) (*runtime, error) {
		once.Do(func() { close(entered) })
		<-release
		return factory.start(node)
	})
	node := fakeNode("shared")
	const callers = 8
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
	<-entered
	close(release)
	wg.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	factory.mu.Lock()
	defer factory.mu.Unlock()
	if factory.starts["shared"] != 1 {
		t.Fatalf("starts = %#v", factory.starts)
	}
}
