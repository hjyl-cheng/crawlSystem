package proxycontrol

import (
	"context"
	"testing"
	"time"
)

func TestBusinessRunBudgetReportsPersistedTaskAndExecutionLimits(t *testing.T) {
	manager, pool := newProxyControlPostgresWithOptions(t, func(options *Options) {
		options.MaxRouteSwitchesPerExecution = 2
		options.MaxNetworkAttemptsPerBusinessRun = 9
	})
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "budget-current.example:8080", 10)
	_ = insertControlProxy(t, pool, "budget-reserve.example:8080", 20)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-budget-status", "worker-budget-status", "instance-budget-status",
	))
	if err != nil {
		t.Fatalf("claim budget Route: %v", err)
	}
	if _, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration:  claim.AssignmentVersion,
		AttemptRequestID: "attempt-budget-status",
		BusinessRunID:    "business-budget-status",
		JobExecutionID:   "exec:v1:budget-status",
		TaskKind:         TaskKindChannelFull,
	}); err != nil {
		t.Fatalf("begin budget task: %v", err)
	}
	exhaustedAt := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_control_business_runs
		SET budget_exhausted_at=$1
		WHERE workload_scope='qy-test' AND business_run_id='business-budget-status'
	`, exhaustedAt); err != nil {
		t.Fatalf("mark budget exhausted: %v", err)
	}

	budget, err := manager.BusinessRunBudget(ctx, "business-budget-status")
	if err != nil {
		t.Fatalf("load Business Run budget: %v", err)
	}
	if !budget.OK || budget.WorkloadScope != "qy-test" ||
		budget.BusinessRunID != "business-budget-status" ||
		budget.BusinessTasksUsed != 1 || budget.BusinessTasksLimit != 9 ||
		budget.CurrentExecutionID != "exec:v1:budget-status" ||
		budget.ExecutionTasksUsed != 1 || budget.ExecutionTasksLimit != 3 ||
		budget.BudgetExhaustedAt == nil || !budget.BudgetExhaustedAt.Equal(exhaustedAt) {
		t.Fatalf("Business Run budget = %+v", budget)
	}
	if _, err := manager.BusinessRunBudget(ctx, "business-budget-missing"); err != ErrBusinessRunNotFound {
		t.Fatalf("missing Business Run error = %v, want %v", err, ErrBusinessRunNotFound)
	}
}
