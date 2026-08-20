package sourceinventory

import (
	"fmt"
	"strings"
	"time"
)

type Mode string

const (
	ModeOff     Mode = "off"
	ModeShadow  Mode = "shadow"
	ModeEnforce Mode = "enforce"
)

type Policy struct {
	Mode          Mode
	MinimumMisses int
}

func NewPolicy(mode string, minimumMisses int) (Policy, error) {
	policy := Policy{
		Mode:          Mode(strings.ToLower(strings.TrimSpace(mode))),
		MinimumMisses: minimumMisses,
	}
	if policy.Mode != ModeOff && policy.Mode != ModeShadow && policy.Mode != ModeEnforce {
		return Policy{}, fmt.Errorf("invalid source inventory mode %q", mode)
	}
	if policy.MinimumMisses < 2 {
		return Policy{}, fmt.Errorf("source inventory minimum misses must be at least 2")
	}
	return policy, nil
}

type CompleteRefresh struct {
	SourceID       int
	NodeIdentities []string
	CompletedAt    time.Time
}

type Result struct {
	Generation             int64
	ObservedCount          int
	NewlyMissingCount      int
	EligibleCount          int
	RetiredMembershipCount int
	ArchivedProxyCount     int
	ReactivatedProxyCount  int
}
