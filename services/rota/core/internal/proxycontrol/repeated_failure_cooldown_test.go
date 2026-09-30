package proxycontrol

import (
	"testing"
	"time"
)

func TestRepeatedFailureCooldownDoublesUntilMaximum(t *testing.T) {
	for _, test := range []struct {
		name          string
		base          time.Duration
		priorFailures int
		maximum       time.Duration
		want          time.Duration
	}{
		{name: "first failure keeps base", base: 5 * time.Minute, priorFailures: 0, maximum: 4 * time.Hour, want: 5 * time.Minute},
		{name: "negative count keeps base", base: 5 * time.Minute, priorFailures: -3, maximum: 4 * time.Hour, want: 5 * time.Minute},
		{name: "second failure doubles", base: 5 * time.Minute, priorFailures: 1, maximum: 4 * time.Hour, want: 10 * time.Minute},
		{name: "fifth failure", base: 5 * time.Minute, priorFailures: 4, maximum: 4 * time.Hour, want: 80 * time.Minute},
		{name: "capped at maximum", base: 5 * time.Minute, priorFailures: 6, maximum: 4 * time.Hour, want: 4 * time.Hour},
		{name: "large count stays capped", base: 5 * time.Minute, priorFailures: 10_000, maximum: 4 * time.Hour, want: 4 * time.Hour},
		{name: "youtube base capped", base: 30 * time.Minute, priorFailures: 4, maximum: 4 * time.Hour, want: 4 * time.Hour},
		{name: "maximum below base keeps base", base: 30 * time.Minute, priorFailures: 3, maximum: 10 * time.Minute, want: 30 * time.Minute},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := repeatedFailureCooldown(test.base, test.priorFailures, test.maximum); got != test.want {
				t.Fatalf("repeatedFailureCooldown(%s, %d, %s) = %s, want %s",
					test.base, test.priorFailures, test.maximum, got, test.want)
			}
		})
	}
}
