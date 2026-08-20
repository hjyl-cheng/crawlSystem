package proxycontrol

import "testing"

func intPointer(value int) *int { return &value }

func TestPlanAssignmentsNeverChangesLockedSlot(t *testing.T) {
	plan := planAssignments([]slotState{
		{Name: "bullmq-channel-01", Role: RoleChannel, Number: 1, ProxyID: intPointer(84), Locked: true},
		{Name: "bullmq-channel-02", Role: RoleChannel, Number: 2, ProxyID: intPointer(109)},
	}, []candidate{
		{ID: 109, Protocol: "http", AverageResponseTime: 10},
		{ID: 112, Protocol: "http", AverageResponseTime: 20},
	})

	if got := plan.BySlot["bullmq-channel-01"]; got == nil || *got != 84 {
		t.Fatalf("locked assignment = %v, want 84", got)
	}
	if got := plan.BySlot["bullmq-channel-02"]; got == nil || *got != 109 {
		t.Fatalf("healthy assignment = %v, want 109", got)
	}
}

func TestPlanAssignmentsPreservesHealthyAssignmentsAndUsesReserveForGap(t *testing.T) {
	plan := planAssignments([]slotState{
		{Name: "bullmq-channel-01", Role: RoleChannel, Number: 1, ProxyID: intPointer(1)},
		{Name: "bullmq-channel-02", Role: RoleChannel, Number: 2, ProxyID: intPointer(99)},
	}, []candidate{
		{ID: 1, Protocol: "http", AverageResponseTime: 30},
		{ID: 2, Protocol: "http", AverageResponseTime: 10},
		{ID: 3, Protocol: "http", AverageResponseTime: 20},
	})

	if got := plan.BySlot["bullmq-channel-01"]; got == nil || *got != 1 {
		t.Fatalf("stable assignment = %v, want 1", got)
	}
	if got := plan.BySlot["bullmq-channel-02"]; got == nil || *got != 2 {
		t.Fatalf("replacement assignment = %v, want 2", got)
	}
	if len(plan.Reserve) != 1 || plan.Reserve[0] != 3 {
		t.Fatalf("reserve = %v, want [3]", plan.Reserve)
	}
}

func TestPlanAssignmentsHonorsRolePinsAndInlineDetailFallback(t *testing.T) {
	plan := planAssignments([]slotState{
		{Name: "bullmq-discover-01", Role: RoleDiscover, Number: 1},
		{Name: "bullmq-channel-01", Role: RoleChannel, Number: 1},
		{Name: "bullmq-channel-02", Role: RoleChannel, Number: 2},
	}, []candidate{
		{ID: 1, Protocol: "http", Tags: []string{"bullmq-detail"}, AverageResponseTime: 90},
		{ID: 2, Protocol: "http", AverageResponseTime: 10},
		{ID: 3, Protocol: "http", AverageResponseTime: 20},
	})

	if got := plan.BySlot["bullmq-channel-01"]; got == nil || *got != 1 {
		t.Fatalf("inline detail assignment = %v, want 1", got)
	}
	if got := plan.BySlot["bullmq-discover-01"]; got == nil || *got != 2 {
		t.Fatalf("discover assignment = %v, want 2", got)
	}
}

func TestPlanPolicyAssignmentsKeepsLockedRoutesAndNeverSharesAProxy(t *testing.T) {
	plan := planPolicyAssignments([]slotState{
		{Name: "bullmq-discover-01", Role: RoleDiscover, Number: 1, ProxyID: intPointer(7), Locked: true},
		{Name: "bullmq-channel-01", Role: RoleChannel, Number: 1},
		{Name: "bullmq-query_quality-01", Role: RoleQueryQuality, Number: 1},
	}, map[string][]candidate{
		RoleDiscover:     {{ID: 7}, {ID: 8}},
		RoleChannel:      {{ID: 7}, {ID: 8}, {ID: 9}},
		RoleQueryQuality: {{ID: 8}, {ID: 9}},
	})

	if got := plan.BySlot["bullmq-discover-01"]; got == nil || *got != 7 {
		t.Fatalf("locked discover assignment = %v, want 7", got)
	}
	if got := plan.BySlot["bullmq-channel-01"]; got == nil || *got != 8 {
		t.Fatalf("channel assignment = %v, want 8", got)
	}
	if got := plan.BySlot["bullmq-query_quality-01"]; got == nil || *got != 9 {
		t.Fatalf("query quality assignment = %v, want 9", got)
	}
	if len(plan.Reserve) != 0 {
		t.Fatalf("reserve = %v, want empty", plan.Reserve)
	}
}

func TestPlanAssignmentsUsesAnotherRoleTagAsAFallback(t *testing.T) {
	plan := planAssignments([]slotState{
		{Name: "bullmq-channel-01", Role: RoleChannel, Number: 1},
	}, []candidate{
		{ID: 17, Protocol: "http", Tags: []string{"role:discover"}},
	})

	if got := plan.BySlot["bullmq-channel-01"]; got == nil || *got != 17 {
		t.Fatalf("fallback assignment = %v, want proxy 17", got)
	}
}

func TestMinimumReserveWatermarkUsesCeiling(t *testing.T) {
	if got := minimumReserveWatermark(20, 25, 3); got != 5 {
		t.Fatalf("watermark = %d, want 5", got)
	}
	if got := minimumReserveWatermark(4, 25, 3); got != 3 {
		t.Fatalf("watermark = %d, want 3", got)
	}
}
