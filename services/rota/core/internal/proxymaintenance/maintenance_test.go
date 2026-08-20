package proxymaintenance

import (
	"testing"
	"time"
)

func TestRecommendedRepairActionUsesLatestAuthoritativeProjection(t *testing.T) {
	archived := "archived"
	failed := "failed"
	if got := recommendedRepairAction(repairCandidate{EvidenceResultStatus: &archived}); got != "restore_archive_projection" {
		t.Fatalf("archived action = %q", got)
	}
	if got := recommendedRepairAction(repairCandidate{EvidenceResultStatus: &failed}); got != "schedule_failed_recovery" {
		t.Fatalf("failed action = %q", got)
	}
	if got := recommendedRepairAction(repairCandidate{}); got != "reset_pending_validation" {
		t.Fatalf("unknown action = %q", got)
	}
}

func TestDeterministicSpreadBoundsRepairDueTimes(t *testing.T) {
	spread := time.Hour
	first := deterministicSpread(558, spread)
	second := deterministicSpread(558, spread)
	if first != second {
		t.Fatalf("spread is not deterministic: %s != %s", first, second)
	}
	if first < 0 || first >= spread {
		t.Fatalf("spread = %s, want [0,%s)", first, spread)
	}
}
