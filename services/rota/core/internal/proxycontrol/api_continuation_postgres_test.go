package proxycontrol

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestAPIContinuationPreservesNetworkBudgetAndMonotonicTaskFences(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()
	insertControlProxy(t, pool, "api-continuation.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest("api-claim", "api-worker", "api-instance"))
	if err != nil {
		t.Fatal(err)
	}
	const runID = "api-continuation-run"
	for i := 1; i <= 30; i++ {
		task, err := manager.BeginTask(ctx, BeginTaskRequest{
			SlotName: claim.SlotName, WorkerID: claim.WorkerID, WorkerInstanceID: claim.WorkerInstanceID,
			LeaseID: claim.LeaseID, RouteGeneration: claim.AssignmentVersion,
			AttemptRequestID: fmt.Sprintf("api-attempt-%d", i), BusinessRunID: runID,
			JobExecutionID: fmt.Sprintf("api-execution-%d", i), TaskKind: TaskKindChannelFull,
		})
		if i == 30 {
			if !errors.Is(err, ErrBusinessRunBudget) {
				t.Fatalf("expected exhausted network budget after nine failures: %v", err)
			}
			break
		}
		if err != nil {
			t.Fatalf("begin attempt %d: %v", i, err)
		}
		if task.AttemptNumber != i {
			t.Fatalf("task fence sequence reused: %d != %d", task.AttemptNumber, i)
		}
		continuation := i <= 20
		outcome := TaskOutcomeFailed
		if continuation {
			outcome = TaskOutcomeSuccess
		}
		request := CompleteTaskRequest{
			CompletionRequestID: fmt.Sprintf("api-completion-%d", i), SlotName: claim.SlotName,
			WorkerID: claim.WorkerID, WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
			RouteGeneration: claim.AssignmentVersion, TaskID: task.TaskID, BusinessRunID: runID,
			Outcome: outcome, AttemptQuiesced: true, APIContinuation: continuation,
		}
		result, err := manager.CompleteTask(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if result.APIContinuation != continuation {
			t.Fatal("missing API continuation acknowledgement")
		}
		if _, err := manager.CompleteTask(ctx, request); err != nil {
			t.Fatalf("idempotent completion replay: %v", err)
		}
		changed := request
		changed.APIContinuation = !continuation
		changed.Outcome = TaskOutcomeSuccess
		if _, err := manager.CompleteTask(ctx, changed); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("completion replay must not change budget accounting: %v", err)
		}
		budget, err := manager.BusinessRunBudget(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		expected := 0
		if !continuation {
			expected = i - 20
		}
		if budget.BusinessTasksUsed != expected || budget.BusinessTasksLimit != 9 {
			t.Fatalf("after task %d budget=%+v expected used=%d", i, budget, expected)
		}
	}
}

func TestAPIContinuationCannotHideFailedOrUnquiescedWork(t *testing.T) {
	base := CompleteTaskRequest{APIContinuation: true, Outcome: TaskOutcomeSuccess,
		CompletionRequestID: "completion", SlotName: "slot", WorkerID: "worker", WorkerInstanceID: "instance",
		LeaseID: "lease", TaskID: "task", BusinessRunID: "run", AttemptQuiesced: true}
	if err := validateCompleteTaskRequest(base); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*CompleteTaskRequest){
		func(r *CompleteTaskRequest) { r.Outcome = TaskOutcomeFailed },
		func(r *CompleteTaskRequest) { r.BusinessComplete = true },
		func(r *CompleteTaskRequest) { r.ObservationIDs = []string{"network-failure"} },
		func(r *CompleteTaskRequest) { r.RecheckCountry = "BR" },
		func(r *CompleteTaskRequest) { r.AttemptQuiesced = false },
		func(r *CompleteTaskRequest) { r.ActiveManagedRequests = 1 },
	} {
		request := base
		mutate(&request)
		if err := validateCompleteTaskRequest(request); err == nil {
			t.Fatal("invalid API handoff was accepted")
		}
	}
}
