package proxycontrol

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestBeginTaskIsIdempotent(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	firstID := insertControlProxy(t, pool, "task-first.example:8080", 10)
	_ = insertControlProxy(t, pool, "task-reserve.example:8080", 20)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}

	claim, err := manager.Claim(ctx, testClaimRequest("claim-task-1", "worker-task-1", "instance-task-1"))
	if err != nil {
		t.Fatalf("claim proxy: %v", err)
	}
	if claim.ProxyID == nil || *claim.ProxyID != firstID {
		t.Fatalf("claim = %+v", claim)
	}

	request := BeginTaskRequest{
		SlotName:         claim.SlotName,
		WorkerID:         claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID,
		LeaseID:          claim.LeaseID,
		RouteGeneration:  claim.AssignmentVersion,
		AttemptRequestID: "attempt-request-1",
		BusinessRunID:    "channel-run-1",
		JobExecutionID:   "youtube-channel-crawl:channel-1:1",
		TaskKind:         "channel_full",
	}
	first, err := manager.BeginTask(ctx, request)
	if err != nil {
		t.Fatalf("begin first task: %v", err)
	}
	duplicate, err := manager.BeginTask(ctx, request)
	if err != nil {
		t.Fatalf("replay first task: %v", err)
	}

	if first.TaskID == "" || first.AttemptNumber != 1 {
		t.Fatalf("first task = %+v", first)
	}
	if duplicate != first {
		t.Fatalf("duplicate = %+v, want %+v", duplicate, first)
	}
	wrongInstance := request
	wrongInstance.AttemptRequestID = "attempt-request-wrong-instance"
	wrongInstance.WorkerInstanceID = "other-instance"
	if _, err := manager.BeginTask(ctx, wrongInstance); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("wrong worker instance begin error = %v, want lease conflict", err)
	}

	var taskCount, nextAttemptNumber int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM proxy_control_tasks
		WHERE workload_scope='qy-test' AND attempt_request_id='attempt-request-1'
	`).Scan(&taskCount); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT next_attempt_number FROM proxy_control_business_runs
		WHERE workload_scope='qy-test' AND business_run_id='channel-run-1'
	`).Scan(&nextAttemptNumber); err != nil {
		t.Fatalf("load next attempt number: %v", err)
	}
	if taskCount != 1 || nextAttemptNumber != 2 {
		t.Fatalf("task count = %d, next attempt = %d", taskCount, nextAttemptNumber)
	}
	var taskInstance, taskPolicyID, taskPolicyHash, activeTaskID, controlState string
	if err := pool.QueryRow(ctx, `
		SELECT t.worker_instance_id,t.identity_policy_id,t.identity_policy_hash,
		       s.active_task_id,s.control_state
		FROM proxy_control_tasks t JOIN proxy_running_slots s ON s.slot_name=t.slot_name
		WHERE t.task_id=$1
	`, first.TaskID).Scan(
		&taskInstance, &taskPolicyID, &taskPolicyHash, &activeTaskID, &controlState,
	); err != nil {
		t.Fatalf("load frozen task identity: %v", err)
	}
	if taskInstance != claim.WorkerInstanceID || taskPolicyID != claim.IdentityPolicyID ||
		taskPolicyHash != claim.IdentityPolicyHash || activeTaskID != first.TaskID || controlState != "active_task" {
		t.Fatalf("task identity=%q/%q/%q active=%q state=%q", taskInstance, taskPolicyID, taskPolicyHash, activeTaskID, controlState)
	}
}

func TestExpiredLeaseAbandonsActiveTaskBeforeSlotReclaim(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "task-expired-lease.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}

	firstClaim, err := manager.Claim(ctx, testClaimRequest(
		"claim-expired-task-1", "worker-expired-task-1", "instance-expired-task-1",
	))
	if err != nil {
		t.Fatalf("claim first worker: %v", err)
	}
	firstTask, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName:         firstClaim.SlotName,
		WorkerID:         firstClaim.WorkerID,
		WorkerInstanceID: firstClaim.WorkerInstanceID,
		LeaseID:          firstClaim.LeaseID,
		RouteGeneration:  firstClaim.AssignmentVersion,
		AttemptRequestID: "attempt-expired-task-1",
		BusinessRunID:    "channel-run-expired-task-1",
		JobExecutionID:   "youtube-channel-crawl:expired-task-1:1",
		TaskKind:         TaskKindChannelFull,
	})
	if err != nil {
		t.Fatalf("begin first task: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots SET lease_until=NOW()-interval '1 second'
		WHERE slot_name=$1 AND lease_id=$2
	`, firstClaim.SlotName, firstClaim.LeaseID); err != nil {
		t.Fatalf("expire first lease fixture: %v", err)
	}

	secondClaim, err := manager.Claim(ctx, testClaimRequest(
		"claim-expired-task-2", "worker-expired-task-2", "instance-expired-task-2",
	))
	if err != nil {
		t.Fatalf("claim replacement worker: %v", err)
	}
	if secondClaim.SlotName != firstClaim.SlotName || secondClaim.LeaseID == firstClaim.LeaseID {
		t.Fatalf("replacement claim = %+v, first claim = %+v", secondClaim, firstClaim)
	}
	if _, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName:         secondClaim.SlotName,
		WorkerID:         secondClaim.WorkerID,
		WorkerInstanceID: secondClaim.WorkerInstanceID,
		LeaseID:          secondClaim.LeaseID,
		RouteGeneration:  secondClaim.AssignmentVersion,
		AttemptRequestID: "attempt-expired-task-2",
		BusinessRunID:    "channel-run-expired-task-2",
		JobExecutionID:   "youtube-channel-crawl:expired-task-2:1",
		TaskKind:         TaskKindChannelFull,
	}); err != nil {
		t.Fatalf("begin replacement task: %v", err)
	}

	var status, outcome, failedStage string
	var completed bool
	if err := pool.QueryRow(ctx, `
		SELECT status,COALESCE(outcome,''),COALESCE(failed_stage,''),completed_at IS NOT NULL
		FROM proxy_control_tasks WHERE task_id=$1
	`, firstTask.TaskID).Scan(&status, &outcome, &failedStage, &completed); err != nil {
		t.Fatalf("load expired task: %v", err)
	}
	if status != "abandoned" || outcome != TaskOutcomeCancelled ||
		failedStage != "lease_expired" || !completed {
		t.Fatalf(
			"expired task status=%q outcome=%q failed_stage=%q completed=%t",
			status, outcome, failedStage, completed,
		)
	}
}

func TestClaimRepairsDetachedActiveTaskFromPriorLease(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "task-detached-lease.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}

	firstClaim, err := manager.Claim(ctx, testClaimRequest(
		"claim-detached-task-1", "worker-detached-task-1", "instance-detached-task-1",
	))
	if err != nil {
		t.Fatalf("claim first worker: %v", err)
	}
	firstTask, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName:         firstClaim.SlotName,
		WorkerID:         firstClaim.WorkerID,
		WorkerInstanceID: firstClaim.WorkerInstanceID,
		LeaseID:          firstClaim.LeaseID,
		RouteGeneration:  firstClaim.AssignmentVersion,
		AttemptRequestID: "attempt-detached-task-1",
		BusinessRunID:    "channel-run-detached-task-1",
		JobExecutionID:   "youtube-channel-crawl:detached-task-1:1",
		TaskKind:         TaskKindChannelFull,
	})
	if err != nil {
		t.Fatalf("begin first task: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_control_leases
		SET status='expired',released_at=NOW(),release_reason='lease_expired',updated_at=NOW()
		WHERE lease_id=$1
	`, firstClaim.LeaseID); err != nil {
		t.Fatalf("expire detached task lease fixture: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots
		SET worker_id=NULL,worker_instance_id=NULL,lease_id=NULL,current_lease_id=NULL,
		    lease_until=NULL,last_heartbeat_at=NULL,active_task_id=NULL,
		    active_task_started_at=NULL,control_state='unleased',updated_at=NOW()
		WHERE slot_name=$1
	`, firstClaim.SlotName); err != nil {
		t.Fatalf("detach active task fixture: %v", err)
	}

	secondClaim, err := manager.Claim(ctx, testClaimRequest(
		"claim-detached-task-2", "worker-detached-task-2", "instance-detached-task-2",
	))
	if err != nil {
		t.Fatalf("claim replacement worker: %v", err)
	}
	if _, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName:         secondClaim.SlotName,
		WorkerID:         secondClaim.WorkerID,
		WorkerInstanceID: secondClaim.WorkerInstanceID,
		LeaseID:          secondClaim.LeaseID,
		RouteGeneration:  secondClaim.AssignmentVersion,
		AttemptRequestID: "attempt-detached-task-2",
		BusinessRunID:    "channel-run-detached-task-2",
		JobExecutionID:   "youtube-channel-crawl:detached-task-2:1",
		TaskKind:         TaskKindChannelFull,
	}); err != nil {
		t.Fatalf("begin replacement task: %v", err)
	}

	var status, outcome, failedStage string
	if err := pool.QueryRow(ctx, `
		SELECT status,COALESCE(outcome,''),COALESCE(failed_stage,'')
		FROM proxy_control_tasks WHERE task_id=$1
	`, firstTask.TaskID).Scan(&status, &outcome, &failedStage); err != nil {
		t.Fatalf("load detached task: %v", err)
	}
	if status != "abandoned" || outcome != TaskOutcomeCancelled ||
		failedStage != "lease_expired" {
		t.Fatalf(
			"detached task status=%q outcome=%q failed_stage=%q",
			status, outcome, failedStage,
		)
	}
}

func TestBeginTaskRepairsDetachedActiveTaskWithLiveReplacementLease(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "task-live-replacement.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}

	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-live-replacement-1", "worker-live-replacement", "instance-live-replacement",
	))
	if err != nil {
		t.Fatalf("claim first worker: %v", err)
	}
	oldTask, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName:         claim.SlotName,
		WorkerID:         claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID,
		LeaseID:          claim.LeaseID,
		RouteGeneration:  claim.AssignmentVersion,
		AttemptRequestID: "attempt-live-replacement-1",
		BusinessRunID:    "channel-run-live-replacement-1",
		JobExecutionID:   "youtube-channel-crawl:live-replacement-1:1",
		TaskKind:         TaskKindChannelFull,
	})
	if err != nil {
		t.Fatalf("begin old task: %v", err)
	}

	replacementLeaseID := "lease-live-replacement-2"
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_control_leases
		SET status='expired',released_at=NOW(),release_reason='lease_replaced',updated_at=NOW()
		WHERE lease_id=$1
	`, claim.LeaseID); err != nil {
		t.Fatalf("expire prior lease fixture: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO proxy_control_leases (
		  lease_id,workload_scope,slot_name,role,worker_id,worker_instance_id,
		  identity_policy_id,identity_policy_version,identity_policy_hash,
		  status,claim_request_id,claim_request_hash,lease_until
		)
		SELECT $2,workload_scope,slot_name,role,worker_id,worker_instance_id,
		       identity_policy_id,identity_policy_version,identity_policy_hash,
		       'active','claim-live-replacement-2','hash-live-replacement-2',
		       NOW()+interval '1 minute'
		FROM proxy_control_leases WHERE lease_id=$1
	`, claim.LeaseID, replacementLeaseID); err != nil {
		t.Fatalf("insert replacement lease fixture: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots
		SET lease_id=$2,current_lease_id=$2,lease_until=NOW()+interval '1 minute',
		    active_task_id=NULL,active_task_started_at=NULL,control_state='leased_idle',updated_at=NOW()
		WHERE slot_name=$1
	`, claim.SlotName, replacementLeaseID); err != nil {
		t.Fatalf("detach old task from live replacement lease fixture: %v", err)
	}

	newTask, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName:         claim.SlotName,
		WorkerID:         claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID,
		LeaseID:          replacementLeaseID,
		RouteGeneration:  claim.AssignmentVersion,
		AttemptRequestID: "attempt-live-replacement-2",
		BusinessRunID:    "channel-run-live-replacement-2",
		JobExecutionID:   "youtube-channel-crawl:live-replacement-2:1",
		TaskKind:         TaskKindChannelFull,
	})
	if err != nil {
		t.Fatalf("begin task on live replacement lease: %v", err)
	}

	var oldStatus, oldOutcome, oldFailedStage, activeTaskID string
	if err := pool.QueryRow(ctx, `
		SELECT status,COALESCE(outcome,''),COALESCE(failed_stage,'')
		FROM proxy_control_tasks WHERE task_id=$1
	`, oldTask.TaskID).Scan(&oldStatus, &oldOutcome, &oldFailedStage); err != nil {
		t.Fatalf("load detached task: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(active_task_id,'') FROM proxy_running_slots WHERE slot_name=$1
	`, claim.SlotName).Scan(&activeTaskID); err != nil {
		t.Fatalf("load replacement slot task: %v", err)
	}
	if oldStatus != "abandoned" || oldOutcome != TaskOutcomeCancelled ||
		oldFailedStage != "lease_replaced" || activeTaskID != newTask.TaskID {
		t.Fatalf(
			"old task status=%q outcome=%q failed_stage=%q slot task=%q new task=%q",
			oldStatus, oldOutcome, oldFailedStage, activeTaskID, newTask.TaskID,
		)
	}
}

func TestBeginTaskRejectsFourthTaskWithoutAdvancingRun(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "task-budget.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}

	claim, err := manager.Claim(ctx, testClaimRequest("claim-task-budget", "worker-task-budget", "instance-task-budget"))
	if err != nil {
		t.Fatalf("claim proxy: %v", err)
	}

	base := BeginTaskRequest{
		SlotName:         claim.SlotName,
		WorkerID:         claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID,
		LeaseID:          claim.LeaseID,
		RouteGeneration:  claim.AssignmentVersion,
		BusinessRunID:    "channel-run-budget",
		JobExecutionID:   "youtube-channel-crawl:channel-budget:1",
		TaskKind:         "channel_full",
	}
	for attempt := 1; attempt <= 3; attempt++ {
		request := base
		request.AttemptRequestID = fmt.Sprintf("attempt-request-budget-%d", attempt)
		task, err := manager.BeginTask(ctx, request)
		if err != nil {
			t.Fatalf("begin task %d: %v", attempt, err)
		}
		if task.AttemptNumber != attempt {
			t.Fatalf("task %d attempt number = %d", attempt, task.AttemptNumber)
		}
		if _, err := pool.Exec(ctx, `
			WITH completed AS (
			  UPDATE proxy_control_tasks
			  SET status='completed', outcome='retryable_failure', completed_at=NOW()
			  WHERE task_id=$1 RETURNING slot_name
			)
			UPDATE proxy_running_slots
			SET active_task_id=NULL,active_task_started_at=NULL,control_state='leased_idle'
			WHERE slot_name=(SELECT slot_name FROM completed)
		`, task.TaskID); err != nil {
			t.Fatalf("complete task %d fixture: %v", attempt, err)
		}
	}

	rejected := base
	rejected.AttemptRequestID = "attempt-request-budget-4"
	if _, err := manager.BeginTask(ctx, rejected); !errors.Is(err, ErrExecutionBudget) {
		t.Fatalf("fourth begin error = %v, want execution budget", err)
	}

	var taskCount, nextAttemptNumber int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM proxy_control_tasks
		WHERE workload_scope='qy-test' AND job_execution_id=$1
	`, base.JobExecutionID).Scan(&taskCount); err != nil {
		t.Fatalf("count execution tasks: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT next_attempt_number FROM proxy_control_business_runs
		WHERE workload_scope='qy-test' AND business_run_id=$1
	`, base.BusinessRunID).Scan(&nextAttemptNumber); err != nil {
		t.Fatalf("load next attempt number: %v", err)
	}
	if taskCount != 3 || nextAttemptNumber != 4 {
		t.Fatalf("task count = %d, next attempt = %d; want 3 and 4", taskCount, nextAttemptNumber)
	}
}

func TestBeginTaskPrioritizesExhaustedBusinessRunOverExecution(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "task-run-budget.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-task-run-budget", "worker-task-run-budget", "instance-task-run-budget",
	))
	if err != nil {
		t.Fatalf("claim proxy: %v", err)
	}

	const businessRunID = "channel-run-total-budget"
	const jobExecutionID = "youtube-channel-crawl:channel-total-budget:1"
	if _, err := pool.Exec(ctx, `
		INSERT INTO proxy_control_business_runs (
		  workload_scope,business_run_id,next_attempt_number,retry_policy_id,retry_policy_version,
		  max_route_switches_per_execution,max_network_attempts_per_business_run
		) VALUES ('qy-test',$1,10,'default',1,2,9)
	`, businessRunID); err != nil {
		t.Fatalf("insert exhausted business run: %v", err)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		if _, err := pool.Exec(ctx, `
			INSERT INTO proxy_control_tasks (
			  task_id,attempt_request_id,request_hash,workload_scope,business_run_id,
			  job_execution_id,attempt_number,slot_name,worker_id,worker_instance_id,
			  lease_id,route_generation,task_kind,identity_policy_id,
			  identity_policy_version,identity_policy_hash,status,outcome,completed_at
			) VALUES (
			  $1,$2,$2,'qy-test',$3,$4,$5,$6,$7,$8,$9,$10,'channel_full',
			  'qy-test-channel-v1',1,'sha256:test-channel-v1','completed','failed',NOW()
			)
		`, fmt.Sprintf("task-total-budget-%d", attempt),
			fmt.Sprintf("attempt-total-budget-%d", attempt), businessRunID, jobExecutionID,
			attempt, claim.SlotName, claim.WorkerID, claim.WorkerInstanceID, claim.LeaseID,
			claim.AssignmentVersion); err != nil {
			t.Fatalf("insert completed task %d: %v", attempt, err)
		}
	}

	_, err = manager.BeginTask(ctx, BeginTaskRequest{
		SlotName:         claim.SlotName,
		WorkerID:         claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID,
		LeaseID:          claim.LeaseID,
		RouteGeneration:  claim.AssignmentVersion,
		AttemptRequestID: "attempt-total-budget-4",
		BusinessRunID:    businessRunID,
		JobExecutionID:   jobExecutionID,
		TaskKind:         "channel_full",
	})
	if !errors.Is(err, ErrBusinessRunBudget) {
		t.Fatalf("begin error = %v, want business run budget", err)
	}
	var exhausted bool
	if err := pool.QueryRow(ctx, `
		SELECT budget_exhausted_at IS NOT NULL
		FROM proxy_control_business_runs
		WHERE workload_scope='qy-test' AND business_run_id=$1
	`, businessRunID).Scan(&exhausted); err != nil {
		t.Fatalf("load business run budget marker: %v", err)
	}
	if !exhausted {
		t.Fatal("business run budget_exhausted_at was not recorded")
	}
}

func TestBeginTaskStopsAtBusinessRunBudgetAfterThreeExecutions(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "task-nine-attempts.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-task-nine-attempts", "worker-task-nine-attempts", "instance-task-nine-attempts",
	))
	if err != nil {
		t.Fatalf("claim proxy: %v", err)
	}

	const businessRunID = "channel-run-nine-attempts"
	for attempt := 1; attempt <= 9; attempt++ {
		execution := 1 + (attempt-1)/3
		task, err := manager.BeginTask(ctx, BeginTaskRequest{
			SlotName:         claim.SlotName,
			WorkerID:         claim.WorkerID,
			WorkerInstanceID: claim.WorkerInstanceID,
			LeaseID:          claim.LeaseID,
			RouteGeneration:  claim.AssignmentVersion,
			AttemptRequestID: fmt.Sprintf("attempt-nine-budget-%d", attempt),
			BusinessRunID:    businessRunID,
			JobExecutionID:   fmt.Sprintf("youtube-channel-crawl:nine-budget:%d", execution),
			TaskKind:         TaskKindChannelFull,
		})
		if err != nil {
			t.Fatalf("begin task %d in execution %d: %v", attempt, execution, err)
		}
		if task.AttemptNumber != attempt {
			t.Fatalf("task %d attempt number = %d", attempt, task.AttemptNumber)
		}
		if _, err := manager.CompleteTask(ctx, CompleteTaskRequest{
			CompletionRequestID:   fmt.Sprintf("completion-nine-budget-%d", attempt),
			SlotName:              claim.SlotName,
			WorkerID:              claim.WorkerID,
			WorkerInstanceID:      claim.WorkerInstanceID,
			LeaseID:               claim.LeaseID,
			RouteGeneration:       claim.AssignmentVersion,
			TaskID:                task.TaskID,
			BusinessRunID:         businessRunID,
			Outcome:               TaskOutcomeFailed,
			AttemptQuiesced:       true,
			ActiveManagedRequests: 0,
		}); err != nil {
			t.Fatalf("complete task %d: %v", attempt, err)
		}
	}

	for requestNumber := 10; requestNumber <= 11; requestNumber++ {
		_, err := manager.BeginTask(ctx, BeginTaskRequest{
			SlotName:         claim.SlotName,
			WorkerID:         claim.WorkerID,
			WorkerInstanceID: claim.WorkerInstanceID,
			LeaseID:          claim.LeaseID,
			RouteGeneration:  claim.AssignmentVersion,
			AttemptRequestID: fmt.Sprintf("attempt-nine-budget-%d", requestNumber),
			BusinessRunID:    businessRunID,
			JobExecutionID:   "youtube-channel-crawl:nine-budget:3",
			TaskKind:         TaskKindChannelFull,
		})
		if !errors.Is(err, ErrBusinessRunBudget) {
			t.Fatalf("begin request %d error = %v, want business run budget", requestNumber, err)
		}
	}

	var taskCount, nextAttemptNumber int
	var budgetExhaustedAt *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*),MAX(run.next_attempt_number),MAX(run.budget_exhausted_at)
		FROM proxy_control_business_runs run
		LEFT JOIN proxy_control_tasks task
		  ON task.workload_scope=run.workload_scope AND task.business_run_id=run.business_run_id
		WHERE run.workload_scope='qy-test' AND run.business_run_id=$1
	`, businessRunID).Scan(&taskCount, &nextAttemptNumber, &budgetExhaustedAt); err != nil {
		t.Fatalf("load exhausted business run: %v", err)
	}
	if taskCount != 9 || nextAttemptNumber != 10 || budgetExhaustedAt == nil {
		t.Fatalf(
			"business run tasks=%d next_attempt=%d budget_exhausted_at=%v; want 9/10/non-null",
			taskCount, nextAttemptNumber, budgetExhaustedAt,
		)
	}
}

func TestBeginTaskRejectsAnIneligibleLeasedProxy(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	proxyID := insertControlProxy(t, pool, "task-ineligible.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-task-ineligible", "worker-task-ineligible", "instance-task-ineligible",
	))
	if err != nil {
		t.Fatalf("claim proxy: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxies
		SET status='failed',base_health_status='failed',youtube_health_status='not_run'
		WHERE id=$1
	`, proxyID); err != nil {
		t.Fatalf("mark claimed proxy failed: %v", err)
	}

	_, err = manager.BeginTask(ctx, BeginTaskRequest{
		SlotName:         claim.SlotName,
		WorkerID:         claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID,
		LeaseID:          claim.LeaseID,
		RouteGeneration:  claim.AssignmentVersion,
		AttemptRequestID: "attempt-task-ineligible",
		BusinessRunID:    "channel-run-ineligible",
		JobExecutionID:   "youtube-channel-crawl:channel-ineligible:1",
		TaskKind:         "channel_full",
	})
	if !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("begin error = %v, want lease conflict", err)
	}
	var taskCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM proxy_control_tasks
		WHERE workload_scope='qy-test' AND business_run_id='channel-run-ineligible'
	`).Scan(&taskCount); err != nil {
		t.Fatalf("count ineligible proxy tasks: %v", err)
	}
	if taskCount != 0 {
		t.Fatalf("ineligible proxy created %d tasks", taskCount)
	}
}

func TestBeginTaskRejectsTaskKindOutsideClaimedRole(t *testing.T) {
	manager, pool := newProxyControlPostgresWithOptions(t, func(options *Options) {
		options.ChannelSlots = 1
		options.DiscoverSlots = 1
		options.QueryQualitySlots = 1
		options.IdentityPolicies = map[string]IdentityPolicy{
			"qy-test-channel-v1": {
				ID: "qy-test-channel-v1", Version: 1,
				Hash: "sha256:test-channel-v1", Role: RoleChannel,
			},
			"qy-test-discover-v1": {
				ID: "qy-test-discover-v1", Version: 1,
				Hash: "sha256:test-discover-v1", Role: RoleDiscover,
			},
			"qy-test-query-quality-v1": {
				ID: "qy-test-query-quality-v1", Version: 1,
				Hash: "sha256:test-query-quality-v1", Role: RoleQueryQuality,
			},
		}
	})
	ctx := context.Background()

	for index := 0; index < 3; index++ {
		insertControlProxy(t, pool, fmt.Sprintf("task-role-%d.example:8080", index), 10+index)
	}
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync role resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile role assignments: %v", err)
	}

	tests := []struct {
		role             string
		policyID         string
		allowedTaskKind  string
		rejectedTaskKind string
	}{
		{
			role: RoleChannel, policyID: "qy-test-channel-v1",
			allowedTaskKind: "channel_incremental", rejectedTaskKind: "discover_page",
		},
		{
			role: RoleDiscover, policyID: "qy-test-discover-v1",
			allowedTaskKind: "discover_page", rejectedTaskKind: "query_quality_chunk",
		},
		{
			role: RoleQueryQuality, policyID: "qy-test-query-quality-v1",
			allowedTaskKind: "query_quality_chunk", rejectedTaskKind: "channel_full",
		},
	}
	for index, test := range tests {
		claim, err := manager.Claim(ctx, ClaimRequest{
			ClaimRequestID:        fmt.Sprintf("claim-task-role-%d", index),
			ProtocolVersion:       ProtocolVersionV2,
			Role:                  test.role,
			WorkerID:              fmt.Sprintf("worker-task-role-%d", index),
			WorkerInstanceID:      fmt.Sprintf("instance-task-role-%d", index),
			IdentityPolicyID:      test.policyID,
			IdentityPolicyVersion: 1,
		})
		if err != nil {
			t.Fatalf("claim %s role: %v", test.role, err)
		}

		request := BeginTaskRequest{
			SlotName:         claim.SlotName,
			WorkerID:         claim.WorkerID,
			WorkerInstanceID: claim.WorkerInstanceID,
			LeaseID:          claim.LeaseID,
			RouteGeneration:  claim.AssignmentVersion,
			AttemptRequestID: fmt.Sprintf("attempt-task-role-rejected-%d", index),
			BusinessRunID:    fmt.Sprintf("business-task-role-%d", index),
			JobExecutionID:   fmt.Sprintf("queue:task-role-%d:1", index),
			TaskKind:         test.rejectedTaskKind,
		}
		if _, err := manager.BeginTask(ctx, request); !errors.Is(err, ErrPolicyRejected) {
			t.Fatalf("%s role rejected task error = %v, want policy rejected", test.role, err)
		}

		request.AttemptRequestID = fmt.Sprintf("attempt-task-role-allowed-%d", index)
		request.TaskKind = test.allowedTaskKind
		if _, err := manager.BeginTask(ctx, request); err != nil {
			t.Fatalf("begin allowed %s task %s: %v", test.role, test.allowedTaskKind, err)
		}
	}
}
