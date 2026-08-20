package proxycontrol

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReleaseV2ReplayCannotAffectNewLeaseOnReusedSlot(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "release-v2.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignment: %v", err)
	}
	firstLease, err := manager.Claim(ctx, testClaimRequest(
		"claim-release-v2-first", "worker-release-v2-first", "instance-release-v2-first",
	))
	if err != nil {
		t.Fatalf("claim first lease: %v", err)
	}

	request := ReleaseRequest{
		ReleaseRequestID:     "release-v2-1",
		SlotName:             firstLease.SlotName,
		WorkerID:             firstLease.WorkerID,
		WorkerInstanceID:     firstLease.WorkerInstanceID,
		LeaseID:              firstLease.LeaseID,
		KnownRouteGeneration: firstLease.AssignmentVersion,
		Reason:               "worker_shutdown",
	}
	first, err := manager.Release(ctx, request)
	if err != nil {
		t.Fatalf("first release: %v", err)
	}
	if !first.OK || !first.Released || first.Status != "released" ||
		first.ReleaseRequestID != request.ReleaseRequestID ||
		first.LeaseID != request.LeaseID || first.Reason != request.Reason ||
		first.ReleasedAt.IsZero() {
		t.Fatalf("first release = %+v", first)
	}

	secondLease, err := manager.Claim(ctx, testClaimRequest(
		"claim-release-v2-second", "worker-release-v2-second", "instance-release-v2-second",
	))
	if err != nil {
		t.Fatalf("claim replacement lease: %v", err)
	}
	if secondLease.SlotName != firstLease.SlotName || secondLease.LeaseID == firstLease.LeaseID {
		t.Fatalf("replacement lease = %+v, first = %+v", secondLease, firstLease)
	}
	var generationBeforeReplay int64
	if err := pool.QueryRow(ctx, `
		SELECT credential_generation FROM proxy_running_slots WHERE slot_name=$1
	`, secondLease.SlotName).Scan(&generationBeforeReplay); err != nil {
		t.Fatalf("load credential generation before replay: %v", err)
	}

	replayed, err := manager.Release(ctx, request)
	if err != nil {
		t.Fatalf("replay old release: %v", err)
	}
	if !replayed.Released || replayed.Status != first.Status ||
		!replayed.ReleasedAt.Equal(first.ReleasedAt) || replayed.Reason != first.Reason {
		t.Fatalf("replayed release = %+v, first = %+v", replayed, first)
	}
	var (
		currentWorker   string
		currentInstance string
		currentLease    string
		generationAfter int64
		receiptCount    int
	)
	if err := pool.QueryRow(ctx, `
		SELECT worker_id,worker_instance_id,current_lease_id,credential_generation
		FROM proxy_running_slots WHERE slot_name=$1
	`, secondLease.SlotName).Scan(
		&currentWorker, &currentInstance, &currentLease, &generationAfter,
	); err != nil {
		t.Fatalf("load slot after old release replay: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM proxy_control_command_receipts
		WHERE workload_scope='qy-test' AND command_kind='release' AND request_id=$1
	`, request.ReleaseRequestID).Scan(&receiptCount); err != nil {
		t.Fatalf("count release receipts: %v", err)
	}
	if currentWorker != secondLease.WorkerID || currentInstance != secondLease.WorkerInstanceID ||
		currentLease != secondLease.LeaseID || generationAfter != generationBeforeReplay ||
		receiptCount != 1 {
		t.Fatalf(
			"slot after replay = worker %q instance %q lease %q generation %d; receipt count %d",
			currentWorker, currentInstance, currentLease, generationAfter, receiptCount,
		)
	}

	changed := request
	changed.Reason = "process_exit"
	if _, err := manager.Release(ctx, changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed release error = %v, want idempotency conflict", err)
	}
}

func TestReleaseV2RequiresExactLiveLeaseAndNoActiveTask(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "release-fence.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignment: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-release-fence", "worker-release-fence", "instance-release-fence",
	))
	if err != nil {
		t.Fatalf("claim proxy: %v", err)
	}
	base := ReleaseRequest{
		ReleaseRequestID:     "release-fence-base",
		SlotName:             claim.SlotName,
		WorkerID:             claim.WorkerID,
		WorkerInstanceID:     claim.WorkerInstanceID,
		LeaseID:              claim.LeaseID,
		KnownRouteGeneration: claim.AssignmentVersion,
		Reason:               "worker_shutdown",
	}

	wrongInstance := base
	wrongInstance.ReleaseRequestID = "release-fence-wrong-instance"
	wrongInstance.WorkerInstanceID = "stale-instance"
	if _, err := manager.Release(ctx, wrongInstance); !errors.Is(err, ErrLeaseGone) {
		t.Fatalf("wrong instance release error = %v, want lease gone", err)
	}
	futureRoute := base
	futureRoute.ReleaseRequestID = "release-fence-future-route"
	futureRoute.KnownRouteGeneration++
	if _, err := manager.Release(ctx, futureRoute); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("future route release error = %v, want lease conflict", err)
	}

	task, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration: claim.AssignmentVersion, AttemptRequestID: "release-active-task-attempt",
		BusinessRunID: "release-active-task-run", JobExecutionID: "release-active-task-job:1",
		TaskKind: TaskKindChannelFull,
	})
	if err != nil {
		t.Fatalf("begin active task: %v", err)
	}
	activeTask := base
	activeTask.ReleaseRequestID = "release-fence-active-task"
	if _, err := manager.Release(ctx, activeTask); !errors.Is(err, ErrTaskConflict) {
		t.Fatalf("active task release error = %v, want task conflict", err)
	}
	var activeLeaseID, activeTaskID string
	if err := pool.QueryRow(ctx, `
		SELECT current_lease_id,active_task_id FROM proxy_running_slots WHERE slot_name=$1
	`, claim.SlotName).Scan(&activeLeaseID, &activeTaskID); err != nil {
		t.Fatalf("load slot after refused release: %v", err)
	}
	if activeLeaseID != claim.LeaseID || activeTaskID != task.TaskID {
		t.Fatalf("slot after refused release = lease %q task %q", activeLeaseID, activeTaskID)
	}
}

func TestReleaseV2ReturnsExpiredLeaseTerminalFactWithoutTouchingCurrentSlot(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "release-expired.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignment: %v", err)
	}
	expired, err := manager.Claim(ctx, testClaimRequest(
		"claim-release-expired", "worker-release-expired", "instance-release-expired",
	))
	if err != nil {
		t.Fatalf("claim expiring lease: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		WITH expired_slot AS (
			UPDATE proxy_running_slots
			SET lease_until=NOW()-interval '1 second'
			WHERE slot_name=$1
			RETURNING slot_name
		)
		UPDATE proxy_control_leases
		SET lease_until=NOW()-interval '1 second'
		WHERE lease_id=$2 AND EXISTS (SELECT 1 FROM expired_slot)
	`, expired.SlotName, expired.LeaseID); err != nil {
		t.Fatalf("expire lease fixture: %v", err)
	}
	current, err := manager.Claim(ctx, testClaimRequest(
		"claim-release-current", "worker-release-current", "instance-release-current",
	))
	if err != nil {
		t.Fatalf("claim current lease: %v", err)
	}

	result, err := manager.Release(ctx, ReleaseRequest{
		ReleaseRequestID:     "release-expired-terminal",
		SlotName:             expired.SlotName,
		WorkerID:             expired.WorkerID,
		WorkerInstanceID:     expired.WorkerInstanceID,
		LeaseID:              expired.LeaseID,
		KnownRouteGeneration: expired.AssignmentVersion,
		Reason:               "worker_shutdown",
	})
	if err != nil {
		t.Fatalf("release expired lease: %v", err)
	}
	if result.Released || result.Status != "expired" || result.Reason != "lease_expired" ||
		result.ReleasedAt.IsZero() || time.Since(result.ReleasedAt) > time.Minute {
		t.Fatalf("expired terminal result = %+v", result)
	}
	var currentLeaseID string
	if err := pool.QueryRow(ctx, `
		SELECT current_lease_id FROM proxy_running_slots WHERE slot_name=$1
	`, current.SlotName).Scan(&currentLeaseID); err != nil {
		t.Fatalf("load current lease after expired release: %v", err)
	}
	if currentLeaseID != current.LeaseID {
		t.Fatalf("current lease after expired release = %q, want %q", currentLeaseID, current.LeaseID)
	}
}
