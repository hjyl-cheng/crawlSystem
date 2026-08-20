package sourceinventory

import "testing"

func TestNewPolicyAcceptsOnlyExplicitModes(t *testing.T) {
	for _, mode := range []string{"off", "shadow", "enforce", " SHADOW "} {
		if _, err := NewPolicy(mode, 3); err != nil {
			t.Fatalf("NewPolicy(%q): %v", mode, err)
		}
	}
	if _, err := NewPolicy("delete", 3); err == nil {
		t.Fatal("invalid mode was accepted")
	}
}

func TestNewPolicyRejectsSingleRefreshRetirement(t *testing.T) {
	if _, err := NewPolicy("enforce", 1); err == nil {
		t.Fatal("single-miss retirement policy was accepted")
	}
}
