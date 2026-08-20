package proxy

import (
	"testing"

	"github.com/alpkeskin/rota/core/internal/models"
)

func TestPoolChainSuppressesOnlyAfterConsecutiveFailureThreshold(t *testing.T) {
	selector := &PoolSelector{proxies: []*models.Proxy{{ID: 42}}}
	chain := &PoolChain{selectors: []*PoolSelector{selector}, failures: make(map[int]int)}

	for attempt := 1; attempt < chainFailureThreshold; attempt++ {
		chain.markFailed(0, 42)
		if !selector.HasActive() {
			t.Fatalf("proxy suppressed after only %d failures", attempt)
		}
	}

	chain.markFailed(0, 42)
	if selector.HasActive() {
		t.Fatalf("proxy remained after %d consecutive failures", chainFailureThreshold)
	}
}

func TestPoolChainSuccessResetsFailureMemory(t *testing.T) {
	selector := &PoolSelector{proxies: []*models.Proxy{{ID: 42}}}
	chain := &PoolChain{selectors: []*PoolSelector{selector}, failures: make(map[int]int)}

	chain.markFailed(0, 42)
	chain.markFailed(0, 42)
	chain.markSucceeded(42)
	chain.markFailed(0, 42)

	if !selector.HasActive() {
		t.Fatal("proxy was suppressed even though a success reset the failure sequence")
	}
}

func TestPoolChainRequiresExactlyOneExpectedProxyForManagedActivation(t *testing.T) {
	tests := []struct {
		name      string
		selectors []*PoolSelector
		expected  int
		want      bool
	}{
		{
			name: "one expected proxy",
			selectors: []*PoolSelector{{
				proxies: []*models.Proxy{{ID: 42}},
			}},
			expected: 42,
			want:     true,
		},
		{
			name: "wrong proxy",
			selectors: []*PoolSelector{{
				proxies: []*models.Proxy{{ID: 41}},
			}},
			expected: 42,
		},
		{
			name: "multiple proxies in main pool",
			selectors: []*PoolSelector{{
				proxies: []*models.Proxy{{ID: 42}, {ID: 43}},
			}},
			expected: 42,
		},
		{
			name: "fallback adds another route",
			selectors: []*PoolSelector{
				{proxies: []*models.Proxy{{ID: 42}}},
				{proxies: []*models.Proxy{{ID: 43}}},
			},
			expected: 42,
		},
		{name: "empty chain", expected: 42},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			chain := &PoolChain{selectors: test.selectors}
			if got := chain.HasExactProxy(test.expected); got != test.want {
				t.Fatalf("HasExactProxy(%d) = %v, want %v", test.expected, got, test.want)
			}
		})
	}
}
