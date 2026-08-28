package proxycontrol

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLiveLeaseFenceRejectsEitherInvalidLeaseRecordAtEveryActivationBoundary(t *testing.T) {
	testCases := []struct {
		name            string
		invalidateSQL   string
		wantSlotLive    bool
		wantHistoryLive bool
	}{
		{
			name: "Slot Lease expired while Lease history remains live",
			invalidateSQL: `
				UPDATE proxy_running_slots
				SET lease_until=NOW()-interval '1 second'
				WHERE current_lease_id=$1
			`,
			wantSlotLive:    false,
			wantHistoryLive: true,
		},
		{
			name: "Lease history expired while Slot Lease remains live",
			invalidateSQL: `
				UPDATE proxy_control_leases
				SET lease_until=NOW()-interval '1 second'
				WHERE lease_id=$1
			`,
			wantSlotLive:    true,
			wantHistoryLive: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			manager, pool := newProxyControlPostgres(t)
			ctx := context.Background()

			proxyID := insertControlProxy(t, pool, "live-lease-fence.example:8080", 10)
			dataPlane := &claimedRouteDataPlane{}
			if err := manager.SetDataPlaneController(dataPlane); err != nil {
				t.Fatalf("set live Lease Fence data plane: %v", err)
			}
			if _, err := manager.reconcile(ctx); err != nil {
				t.Fatalf("reconcile live Lease Fence Route: %v", err)
			}

			assignment, err := manager.Claim(ctx, testClaimRequest(
				"claim-live-lease-fence",
				"worker-live-lease-fence",
				"instance-live-lease-fence",
			))
			if err != nil {
				t.Fatalf("Claim live Route: %v", err)
			}
			if !assignment.Ready || assignment.ProxyID == nil || *assignment.ProxyID != proxyID {
				t.Fatalf("live Route assignment = %+v, want ready proxy %d", assignment, proxyID)
			}
			if _, err := pool.Exec(ctx, `
				UPDATE proxy_running_slots
				SET ready_after=NULL,control_state='pending_new_route',
				    route_activation_old_username='retired-proxy-user'
				WHERE slot_name=$1 AND current_lease_id=$2
			`, assignment.SlotName, assignment.LeaseID); err != nil {
				t.Fatalf("prepare pending Route activation: %v", err)
			}

			fence := routeActivationFence{
				SlotName:        assignment.SlotName,
				LeaseID:         assignment.LeaseID,
				ProxyID:         proxyID,
				RouteGeneration: assignment.AssignmentVersion,
			}
			claim, found, err := manager.loadOrClaimPendingRouteActivation(ctx, fence)
			if err != nil || !found {
				t.Fatalf("establish pending Route activation Claim: claim=%+v found=%v err=%v", claim, found, err)
			}

			if _, err := pool.Exec(ctx, testCase.invalidateSQL, assignment.LeaseID); err != nil {
				t.Fatalf("invalidate one side of live Lease Fence: %v", err)
			}
			var slotLive, historyLive bool
			if err := pool.QueryRow(ctx, `
				SELECT slot.lease_until > NOW(),
				       lease.status='active' AND lease.lease_until > NOW()
				FROM proxy_running_slots slot
				JOIN proxy_control_leases lease ON lease.lease_id=$1
				WHERE slot.slot_name=$2
			`, assignment.LeaseID, assignment.SlotName).Scan(&slotLive, &historyLive); err != nil {
				t.Fatalf("load mismatched live Lease Fence: %v", err)
			}
			if slotLive != testCase.wantSlotLive || historyLive != testCase.wantHistoryLive {
				t.Fatalf(
					"fixture live Fence = slot:%v history:%v, want slot:%v history:%v",
					slotLive,
					historyLive,
					testCase.wantSlotLive,
					testCase.wantHistoryLive,
				)
			}

			if loaded, loadedFound, loadErr := manager.loadOrClaimPendingRouteActivation(ctx, fence); loadErr != nil {
				t.Errorf("load invalid pending Route activation Claim: %v", loadErr)
			} else if loadedFound {
				t.Errorf("invalid live Lease Fence returned activation Claim %+v", loaded)
			}
			if manager.renewRouteActivationClaim(ctx, claim) {
				t.Error("invalid live Lease Fence renewed its activation Claim")
			}
			if finalized, finalizeErr := manager.finalizeRouteActivationClaim(ctx, claim); finalizeErr != nil {
				t.Errorf("Finalize invalid pending Route activation: %v", finalizeErr)
			} else if finalized {
				t.Error("invalid live Lease Fence finalized its pending Route")
			}

			if _, err := pool.Exec(ctx, `
				UPDATE proxy_running_slots
				SET ready_after=NOW(),control_state='leased_idle'
				WHERE slot_name=$1
			`, assignment.SlotName); err != nil {
				t.Fatalf("prepare ready Route recovery fixture: %v", err)
			}
			registry, err := manager.loadRouteActivationRegistry(ctx)
			if err != nil {
				t.Fatalf("load Route activation Registry: %v", err)
			}
			if len(registry) != 1 || !registry[0].Blocked || registry[0].Phase != "" {
				t.Errorf("invalid live Lease Fence rebuilt Registry entry as usable: %+v", registry)
			}

			current, err := manager.claimedRouteFenceCurrent(ctx, assignment)
			if err != nil {
				t.Fatalf("check claimed Route Fence: %v", err)
			}
			if current {
				t.Error("invalid live Lease Fence was accepted as current")
			}
			beginCalls := dataPlane.beginCalls
			if err := manager.publishClaimedRoute(ctx, assignment, ""); !errors.Is(err, ErrLeaseGone) {
				t.Errorf("publish invalid claimed Route error = %v, want ErrLeaseGone", err)
			}
			if dataPlane.beginCalls != beginCalls {
				t.Errorf(
					"invalid live Lease Fence reached data-plane publication: begin calls %d -> %d",
					beginCalls,
					dataPlane.beginCalls,
				)
			}
		})
	}
}

func TestRouteActivationFinalizeBudgetIsShorterThanMinimumLease(t *testing.T) {
	if routeActivationFinalizeTimeout >= routeActivationFinalizeMargin {
		t.Fatalf(
			"Finalize timeout %s must be shorter than margin %s",
			routeActivationFinalizeTimeout,
			routeActivationFinalizeMargin,
		)
	}
	if routeActivationFinalizeMargin >= 15*time.Second {
		t.Fatalf("Finalize margin %s must be shorter than the 15s minimum Lease", routeActivationFinalizeMargin)
	}
}

func TestLiveLeaseFenceNullSlotLeaseUntilProjectsFalseWithoutScanError(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	proxyID := insertControlProxy(t, pool, "null-slot-lease.example:8080", 10)
	manager.SetDataPlaneController(&idleRouteDataPlaneStub{})
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile null Slot Lease Route: %v", err)
	}
	assignment, err := manager.Claim(ctx, testClaimRequest(
		"claim-null-slot-lease",
		"worker-null-slot-lease",
		"instance-null-slot-lease",
	))
	if err != nil {
		t.Fatalf("Claim live Route: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots
		SET ready_after=NULL,control_state='pending_new_route',
		    route_activation_old_username='retired-proxy-user'
		WHERE slot_name=$1 AND current_lease_id=$2
	`, assignment.SlotName, assignment.LeaseID); err != nil {
		t.Fatalf("prepare pending Route: %v", err)
	}
	fence := routeActivationFence{
		SlotName:        assignment.SlotName,
		LeaseID:         assignment.LeaseID,
		ProxyID:         proxyID,
		RouteGeneration: assignment.AssignmentVersion,
	}
	claim, found, err := manager.loadOrClaimPendingRouteActivation(ctx, fence)
	if err != nil || !found {
		t.Fatalf("claim pending Route activation: claim=%+v found=%v err=%v", claim, found, err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots
		SET lease_until=NULL
		WHERE slot_name=$1 AND current_lease_id=$2
	`, assignment.SlotName, assignment.LeaseID); err != nil {
		t.Fatalf("clear Slot lease_until: %v", err)
	}

	dataPlane := &idleRouteDataPlaneStub{}
	manager.SetDataPlaneController(dataPlane)
	if committed := manager.resolveUncertainRouteActivation(ctx, claim); committed {
		t.Fatal("NULL Slot lease_until was accepted as a live committed Route")
	}

	registry, err := manager.loadRouteActivationRegistry(ctx)
	if err != nil {
		t.Fatalf("load Route activation Registry with NULL Slot lease_until: %v", err)
	}
	if len(registry) != 1 || !registry[0].Blocked || registry[0].Phase != "" {
		t.Fatalf("NULL Slot lease_until Registry entry = %+v, want blocked with empty phase", registry)
	}
}
