package proxycontrol

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRenewV2IsFencedAndDoesNotExtendLeaseOnReplay(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "renew-v2.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignment: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-renew-v2", "worker-renew-v2", "instance-renew-v2",
	))
	if err != nil {
		t.Fatalf("claim proxy: %v", err)
	}

	request := RenewRequest{
		RenewRequestID:       "renew-v2-1",
		SlotName:             claim.SlotName,
		WorkerID:             claim.WorkerID,
		WorkerInstanceID:     claim.WorkerInstanceID,
		LeaseID:              claim.LeaseID,
		KnownRouteGeneration: claim.AssignmentVersion,
	}
	first, err := manager.Renew(ctx, request)
	if err != nil {
		t.Fatalf("first renew: %v", err)
	}
	if !first.Ready || first.RouteChanged || first.RenewSequence != 1 ||
		first.AssignmentVersion != claim.AssignmentVersion ||
		first.WorkerInstanceID != claim.WorkerInstanceID || first.LeaseRemainingMS <= 0 {
		t.Fatalf("first renewal = %+v", first)
	}
	var firstLeaseUntil time.Time
	if err := pool.QueryRow(ctx, `
		SELECT lease_until FROM proxy_control_leases WHERE lease_id=$1
	`, claim.LeaseID).Scan(&firstLeaseUntil); err != nil {
		t.Fatalf("load first renewed lease: %v", err)
	}

	time.Sleep(5 * time.Millisecond)
	replayed, err := manager.Renew(ctx, request)
	if err != nil {
		t.Fatalf("replay renew: %v", err)
	}
	var replayedLeaseUntil time.Time
	if err := pool.QueryRow(ctx, `
		SELECT lease_until FROM proxy_control_leases WHERE lease_id=$1
	`, claim.LeaseID).Scan(&replayedLeaseUntil); err != nil {
		t.Fatalf("load replayed lease: %v", err)
	}
	if replayed.RenewSequence != first.RenewSequence ||
		!replayedLeaseUntil.Equal(firstLeaseUntil) {
		t.Fatalf(
			"replay sequence/deadline = %d/%s, want %d/%s",
			replayed.RenewSequence, replayedLeaseUntil,
			first.RenewSequence, firstLeaseUntil,
		)
	}

	secondRequest := request
	secondRequest.RenewRequestID = "renew-v2-2"
	second, err := manager.Renew(ctx, secondRequest)
	if err != nil {
		t.Fatalf("second logical renew: %v", err)
	}
	if second.RenewSequence != 2 {
		t.Fatalf("second renew sequence = %d, want 2", second.RenewSequence)
	}
	lateReplay, err := manager.Renew(ctx, request)
	if err != nil {
		t.Fatalf("late replay of first renew: %v", err)
	}
	if lateReplay.RenewSequence != first.RenewSequence {
		t.Fatalf(
			"late replay sequence = %d, want original %d",
			lateReplay.RenewSequence, first.RenewSequence,
		)
	}

	changed := request
	changed.KnownRouteGeneration++
	if _, err := manager.Renew(ctx, changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed renew error = %v, want idempotency conflict", err)
	}
	wrongInstance := request
	wrongInstance.RenewRequestID = "renew-v2-wrong-instance"
	wrongInstance.WorkerInstanceID = "instance-renew-v2-stale"
	if _, err := manager.Renew(ctx, wrongInstance); !errors.Is(err, ErrLeaseGone) {
		t.Fatalf("wrong-instance renew error = %v, want lease gone", err)
	}

	var (
		lastSequence int64
		receiptCount int
	)
	if err := pool.QueryRow(ctx, `
		SELECT last_renew_sequence FROM proxy_control_leases WHERE lease_id=$1
	`, claim.LeaseID).Scan(&lastSequence); err != nil {
		t.Fatalf("load renew sequence: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM proxy_control_command_receipts
		WHERE workload_scope='qy-test' AND command_kind='renew' AND request_id=$1
	`, request.RenewRequestID).Scan(&receiptCount); err != nil {
		t.Fatalf("count renew receipts: %v", err)
	}
	if lastSequence != 2 || receiptCount != 1 {
		t.Fatalf("last sequence = %d, receipt count = %d", lastSequence, receiptCount)
	}
}

func TestRenewV2DiscoversNewReadyRouteFromOlderKnownGeneration(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	firstProxyID := insertControlProxy(t, pool, "renew-discovery-first.example:8080", 10)
	secondProxyID := insertControlProxy(t, pool, "renew-discovery-second.example:8080", 20)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignment: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-renew-discovery", "worker-renew-discovery", "instance-renew-discovery",
	))
	if err != nil {
		t.Fatalf("claim proxy: %v", err)
	}
	if claim.ProxyID == nil || *claim.ProxyID != firstProxyID {
		t.Fatalf("initial claim = %+v", claim)
	}

	swapped, err := manager.Swap(ctx, SwapRequest{
		WorkerID:          claim.WorkerID,
		LeaseID:           claim.LeaseID,
		AssignmentVersion: claim.AssignmentVersion,
		FailedProxyID:     firstProxyID,
	})
	if err != nil {
		t.Fatalf("activate replacement route: %v", err)
	}
	if !swapped.Ready || swapped.ProxyID == nil || *swapped.ProxyID != secondProxyID {
		t.Fatalf("replacement assignment = %+v", swapped)
	}

	discovered, err := manager.Renew(ctx, RenewRequest{
		RenewRequestID:       "renew-discovery-1",
		SlotName:             claim.SlotName,
		WorkerID:             claim.WorkerID,
		WorkerInstanceID:     claim.WorkerInstanceID,
		LeaseID:              claim.LeaseID,
		KnownRouteGeneration: claim.AssignmentVersion,
	})
	if err != nil {
		t.Fatalf("discover replacement route: %v", err)
	}
	if !discovered.Ready || !discovered.RouteChanged ||
		discovered.AssignmentVersion != claim.AssignmentVersion+1 ||
		discovered.ProxyID == nil || *discovered.ProxyID != secondProxyID {
		t.Fatalf("discovered assignment = %+v", discovered)
	}
}
