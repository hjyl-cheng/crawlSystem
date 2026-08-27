package proxycontrol

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/proxylifecycle"
	"github.com/alpkeskin/rota/core/internal/repository"
	"github.com/jackc/pgx/v5/pgxpool"
)

type idleRouteDataPlaneStub struct {
	oldUsername     string
	newUsername     string
	expectedProxyID int
	activationCalls int
	retiredUsers    []string
}

type expiringRouteActivationDataPlane struct {
	pool              *pgxpool.Pool
	slotName          string
	leaseID           string
	activatedUsername string
	retiredUsers      []string
}

func (*expiringRouteActivationDataPlane) RefreshProxyUser(string) {}

func (s *expiringRouteActivationDataPlane) RetireProxyUser(
	_ context.Context,
	username string,
) error {
	s.retiredUsers = append(s.retiredUsers, username)
	return nil
}

func (s *expiringRouteActivationDataPlane) ActivateProxyUser(
	_ context.Context,
	_ string,
	newUsername string,
	_ int,
) error {
	s.activatedUsername = newUsername
	if _, err := s.pool.Exec(context.Background(), `
		UPDATE proxy_running_slots
		SET lease_until=NOW()-interval '1 second'
		WHERE slot_name=$1 AND current_lease_id=$2
	`, s.slotName, s.leaseID); err != nil {
		return err
	}
	if _, err := s.pool.Exec(context.Background(), `
		UPDATE proxy_control_leases
		SET lease_until=NOW()-interval '1 second'
		WHERE lease_id=$1
	`, s.leaseID); err != nil {
		return err
	}
	return nil
}

func TestFailedProxyOnExecutionLockedSlotFinishesBeforeIdleTransition(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	failedProxyID := insertControlProxy(t, pool, "active-route-failed.example:8080", 10)
	reserveProxyID := insertControlProxy(t, pool, "active-route-reserve.example:8080", 20)
	dataPlane := &idleRouteDataPlaneStub{}
	manager.SetDataPlaneController(dataPlane)
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignment: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-active-health", "worker-active-health", "instance-active-health",
	))
	if err != nil {
		t.Fatalf("claim initial route: %v", err)
	}
	task, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration:  claim.AssignmentVersion,
		AttemptRequestID: "attempt-active-health",
		BusinessRunID:    "business-active-health",
		JobExecutionID:   "youtube-channel-crawl:active-health:1:1",
		TaskKind:         TaskKindChannelFull,
	})
	if err != nil {
		t.Fatalf("begin active task: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE proxies
		SET status='failed',base_health_status='failed',youtube_health_status='not_run',
		    health_generation=health_generation+1,updated_at=NOW()
		WHERE id=$1
	`, failedProxyID); err != nil {
		t.Fatalf("apply failed health state: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile execution-locked route: %v", err)
	}
	select {
	case <-manager.reconcileRequests:
	default:
	}

	renewed, err := manager.Renew(ctx, RenewRequest{
		RenewRequestID:       "renew-active-health",
		SlotName:             claim.SlotName,
		WorkerID:             claim.WorkerID,
		WorkerInstanceID:     claim.WorkerInstanceID,
		LeaseID:              claim.LeaseID,
		KnownRouteGeneration: claim.AssignmentVersion,
	})
	if err != nil {
		t.Fatalf("renew execution-locked route: %v", err)
	}
	if !renewed.Ready || renewed.RouteChanged || renewed.ProxyID == nil ||
		*renewed.ProxyID != failedProxyID ||
		renewed.AssignmentVersion != claim.AssignmentVersion ||
		renewed.CredentialGeneration != claim.CredentialGeneration ||
		dataPlane.activationCalls != 0 {
		t.Fatalf("execution-locked assignment = %+v", renewed)
	}

	completion, err := manager.CompleteTask(ctx, CompleteTaskRequest{
		CompletionRequestID: "complete-active-health",
		SlotName:            claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration: claim.AssignmentVersion,
		TaskID:          task.TaskID, BusinessRunID: task.BusinessRunID,
		Outcome: TaskOutcomeSuccess, DurationMS: int64(time.Second / time.Millisecond),
		BusinessComplete: true, ObservationIDs: []string{},
		AttemptQuiesced: true, ActiveManagedRequests: 0,
	})
	if err != nil {
		t.Fatalf("complete active task after health incident: %v", err)
	}
	if completion.ControlState != CompletionReadyKeepRoute || !completion.Ready {
		t.Fatalf("active task completion = %+v", completion)
	}
	select {
	case <-manager.reconcileRequests:
	default:
		t.Fatal("completion on an ineligible active Route did not request idle reconciliation")
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile newly idle route: %v", err)
	}

	replaced, err := manager.Renew(ctx, RenewRequest{
		RenewRequestID:       "renew-after-active-health",
		SlotName:             claim.SlotName,
		WorkerID:             claim.WorkerID,
		WorkerInstanceID:     claim.WorkerInstanceID,
		LeaseID:              claim.LeaseID,
		KnownRouteGeneration: claim.AssignmentVersion,
	})
	if err != nil {
		t.Fatalf("renew post-completion route: %v", err)
	}
	if !replaced.Ready || !replaced.RouteChanged || replaced.ProxyID == nil ||
		*replaced.ProxyID != reserveProxyID ||
		replaced.AssignmentVersion != claim.AssignmentVersion+1 ||
		dataPlane.activationCalls != 1 {
		t.Fatalf("post-completion replacement = %+v", replaced)
	}
}

func (*idleRouteDataPlaneStub) RefreshProxyUser(string) {}

func (s *idleRouteDataPlaneStub) RetireProxyUser(_ context.Context, username string) error {
	s.retiredUsers = append(s.retiredUsers, username)
	return nil
}

func (s *idleRouteDataPlaneStub) ActivateProxyUser(
	_ context.Context,
	oldUsername string,
	newUsername string,
	expectedProxyID int,
) error {
	s.activationCalls++
	s.oldUsername = oldUsername
	s.newUsername = newUsername
	s.expectedProxyID = expectedProxyID
	return nil
}

func TestExpiredLeaseCannotActivatePendingRoute(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	proxyID := insertControlProxy(t, pool, "expired-activation.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignment: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-expired-activation", "worker-expired-activation", "instance-expired-activation",
	))
	if err != nil {
		t.Fatalf("claim initial Route: %v", err)
	}
	if claim.ProxyID == nil || *claim.ProxyID != proxyID {
		t.Fatalf("initial assignment = %+v, want proxy %d", claim, proxyID)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots
		SET ready_after=NULL,control_state='pending_new_route',
		    route_activation_old_username='retired-proxy-user',
		    lease_until=NOW()-interval '1 second'
		WHERE slot_name=$1 AND current_lease_id=$2
	`, claim.SlotName, claim.LeaseID); err != nil {
		t.Fatalf("expire pending Route Lease: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_control_leases
		SET lease_until=NOW()-interval '1 second'
		WHERE lease_id=$1
	`, claim.LeaseID); err != nil {
		t.Fatalf("expire Lease history: %v", err)
	}

	dataPlane := &idleRouteDataPlaneStub{}
	manager.SetDataPlaneController(dataPlane)
	if activated := manager.activatePendingRoute(ctx, routeActivationFence{
		SlotName:        claim.SlotName,
		LeaseID:         claim.LeaseID,
		ProxyID:         proxyID,
		RouteGeneration: claim.AssignmentVersion,
	}); activated {
		t.Fatal("expired Lease activated its pending Route")
	}
	if dataPlane.activationCalls != 0 {
		t.Fatalf("expired Lease made %d data-plane activation calls, want 0", dataPlane.activationCalls)
	}
	var controlState string
	var ready bool
	if err := pool.QueryRow(ctx, `
		SELECT control_state,ready_after IS NOT NULL
		FROM proxy_running_slots WHERE slot_name=$1
	`, claim.SlotName).Scan(&controlState, &ready); err != nil {
		t.Fatalf("load expired pending Route: %v", err)
	}
	if controlState != "pending_new_route" || ready {
		t.Fatalf("expired pending Route state=%q ready=%v", controlState, ready)
	}
}

func TestExpiredLeaseCannotReleasePendingRouteActivationClaim(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	proxyID := insertControlProxy(t, pool, "expired-activation-release.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignment: %v", err)
	}
	lease, err := manager.Claim(ctx, testClaimRequest(
		"claim-expired-activation-release", "worker-expired-activation-release",
		"instance-expired-activation-release",
	))
	if err != nil {
		t.Fatalf("claim initial Route: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots
		SET ready_after=NULL,control_state='pending_new_route',
		    route_activation_old_username='retired-proxy-user'
		WHERE slot_name=$1 AND current_lease_id=$2
	`, lease.SlotName, lease.LeaseID); err != nil {
		t.Fatalf("prepare pending Route: %v", err)
	}
	fence := routeActivationFence{
		SlotName:        lease.SlotName,
		LeaseID:         lease.LeaseID,
		ProxyID:         proxyID,
		RouteGeneration: lease.AssignmentVersion,
	}
	activation, found, err := manager.claimPendingRouteActivation(ctx, fence)
	if err != nil {
		t.Fatalf("claim pending Route activation: %v", err)
	}
	if !found {
		t.Fatal("live Lease did not claim its pending Route activation")
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots
		SET lease_until=NOW()-interval '1 second'
		WHERE slot_name=$1 AND current_lease_id=$2
	`, lease.SlotName, lease.LeaseID); err != nil {
		t.Fatalf("expire claimed Route Slot Lease: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_control_leases
		SET lease_until=NOW()-interval '1 second'
		WHERE lease_id=$1
	`, lease.LeaseID); err != nil {
		t.Fatalf("expire claimed Route Lease history: %v", err)
	}

	manager.releaseRouteActivationClaim(ctx, activation)
	var persistedClaimID string
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(route_activation_claim_id,'')
		FROM proxy_running_slots WHERE slot_name=$1
	`, lease.SlotName).Scan(&persistedClaimID); err != nil {
		t.Fatalf("load pending Route activation claim: %v", err)
	}
	if persistedClaimID != activation.ClaimID {
		t.Fatalf(
			"expired Lease released activation claim %q, want %q retained",
			persistedClaimID,
			activation.ClaimID,
		)
	}
}

func TestLeaseLostDuringRouteActivationRetiresActivatedDataPlaneUser(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	proxyID := insertControlProxy(t, pool, "activation-lease-race.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignment: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-activation-lease-race", "worker-activation-lease-race",
		"instance-activation-lease-race",
	))
	if err != nil {
		t.Fatalf("claim initial Route: %v", err)
	}
	if claim.ProxyID == nil || *claim.ProxyID != proxyID {
		t.Fatalf("initial assignment = %+v, want proxy %d", claim, proxyID)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots
		SET ready_after=NULL,control_state='pending_new_route',
		    route_activation_old_username='retired-proxy-user'
		WHERE slot_name=$1 AND current_lease_id=$2
	`, claim.SlotName, claim.LeaseID); err != nil {
		t.Fatalf("prepare pending Route: %v", err)
	}

	dataPlane := &expiringRouteActivationDataPlane{
		pool: pool, slotName: claim.SlotName, leaseID: claim.LeaseID,
	}
	manager.SetDataPlaneController(dataPlane)
	if activated := manager.activatePendingRoute(ctx, routeActivationFence{
		SlotName:        claim.SlotName,
		LeaseID:         claim.LeaseID,
		ProxyID:         proxyID,
		RouteGeneration: claim.AssignmentVersion,
	}); activated {
		t.Fatal("Route activation survived loss of its Lease Fence")
	}
	if dataPlane.activatedUsername == "" {
		t.Fatal("test did not reach data-plane activation")
	}
	if len(dataPlane.retiredUsers) != 1 ||
		dataPlane.retiredUsers[0] != dataPlane.activatedUsername {
		t.Fatalf(
			"compensating retired users = %v, want %q",
			dataPlane.retiredUsers,
			dataPlane.activatedUsername,
		)
	}
}

func TestCommittedRouteFinalizeResponseLossKeepsActivatedDataPlaneUser(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	proxyID := insertControlProxy(t, pool, "activation-finalize-lost.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignment: %v", err)
	}
	lease, err := manager.Claim(ctx, testClaimRequest(
		"claim-activation-finalize-lost", "worker-activation-finalize-lost",
		"instance-activation-finalize-lost",
	))
	if err != nil {
		t.Fatalf("claim initial Route: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots
		SET ready_after=NULL,control_state='pending_new_route',
		    route_activation_old_username='retired-proxy-user'
		WHERE slot_name=$1 AND current_lease_id=$2
	`, lease.SlotName, lease.LeaseID); err != nil {
		t.Fatalf("prepare pending Route: %v", err)
	}

	fence := routeActivationFence{
		SlotName: lease.SlotName, LeaseID: lease.LeaseID, ProxyID: proxyID,
		RouteGeneration: lease.AssignmentVersion,
	}
	claim, found, err := manager.claimPendingRouteActivation(ctx, fence)
	if err != nil {
		t.Fatalf("claim pending Route activation: %v", err)
	}
	if !found {
		t.Fatal("pending Route activation was not claimed")
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots
		SET ready_after=NOW(),control_state='leased_idle',
		    route_activation_old_username=NULL,route_activation_claim_id=NULL,
		    route_activation_claim_until=NULL,rotation_deadline_at=NULL,updated_at=NOW()
		WHERE slot_name=$1 AND current_lease_id=$2 AND proxy_id=$3
		  AND assignment_version=$4 AND route_activation_claim_id=$5
	`, fence.SlotName, fence.LeaseID, fence.ProxyID, fence.RouteGeneration, claim.ClaimID); err != nil {
		t.Fatalf("commit Route Finalize before simulated response loss: %v", err)
	}

	dataPlane := &idleRouteDataPlaneStub{}
	manager.SetDataPlaneController(dataPlane)
	if committed := manager.compensateLostRouteActivation(ctx, claim); !committed {
		t.Fatal("authoritative read did not resolve the lost Finalize response as committed")
	}
	if len(dataPlane.retiredUsers) != 0 {
		t.Fatalf("committed Route data plane was retired after response loss: %v", dataPlane.retiredUsers)
	}
}

func TestFailedProxyOnLeaseOwnedIdleSlotRotatesToHealthyReserve(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	failedProxyID := insertControlProxy(t, pool, "idle-route-failed.example:8080", 10)
	reserveProxyID := insertControlProxy(t, pool, "idle-route-reserve.example:8080", 20)
	dataPlane := &idleRouteDataPlaneStub{}
	manager.SetDataPlaneController(dataPlane)
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignment: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-idle-route", "worker-idle-route", "instance-idle-route",
	))
	if err != nil {
		t.Fatalf("claim initial route: %v", err)
	}
	if claim.ProxyID == nil || *claim.ProxyID != failedProxyID || !claim.Ready {
		t.Fatalf("initial assignment = %+v", claim)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE proxies
		SET status='failed',base_health_status='failed',youtube_health_status='not_run',
		    health_generation=health_generation+1,updated_at=NOW()
		WHERE id=$1
	`, failedProxyID); err != nil {
		t.Fatalf("apply failed health state: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile failed idle route: %v", err)
	}

	renewed, err := manager.Renew(ctx, RenewRequest{
		RenewRequestID:       "renew-idle-route-after-health",
		SlotName:             claim.SlotName,
		WorkerID:             claim.WorkerID,
		WorkerInstanceID:     claim.WorkerInstanceID,
		LeaseID:              claim.LeaseID,
		KnownRouteGeneration: claim.AssignmentVersion,
	})
	if err != nil {
		t.Fatalf("renew rotated idle route: %v", err)
	}
	if !renewed.Ready || !renewed.RouteChanged || renewed.LeaseID != claim.LeaseID ||
		renewed.ProxyID == nil || *renewed.ProxyID != reserveProxyID ||
		renewed.AssignmentVersion != claim.AssignmentVersion+1 ||
		renewed.CredentialGeneration != claim.CredentialGeneration+1 ||
		renewed.ProxyUser == claim.ProxyUser || renewed.ControlState != "leased_idle" {
		t.Fatalf("rotated idle assignment = %+v, initial = %+v", renewed, claim)
	}
	if dataPlane.activationCalls != 1 || dataPlane.oldUsername != claim.ProxyUser ||
		dataPlane.newUsername != renewed.ProxyUser || dataPlane.expectedProxyID != reserveProxyID {
		t.Fatalf("data-plane activation = %+v", dataPlane)
	}

	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("repeat idle reconciliation: %v", err)
	}
	stable, err := manager.Renew(ctx, RenewRequest{
		RenewRequestID:       "renew-idle-route-stable",
		SlotName:             renewed.SlotName,
		WorkerID:             renewed.WorkerID,
		WorkerInstanceID:     renewed.WorkerInstanceID,
		LeaseID:              renewed.LeaseID,
		KnownRouteGeneration: renewed.AssignmentVersion,
	})
	if err != nil {
		t.Fatalf("renew stable idle route: %v", err)
	}
	if !stable.Ready || stable.RouteChanged ||
		stable.AssignmentVersion != renewed.AssignmentVersion ||
		stable.CredentialGeneration != renewed.CredentialGeneration ||
		dataPlane.activationCalls != 1 {
		t.Fatalf("stable assignment = %+v, activation calls = %d", stable, dataPlane.activationCalls)
	}
}

func TestFailedProxyOnLeaseOwnedIdleSlotPausesAndRecoversWhenReserveArrives(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	failedProxyID := insertControlProxy(t, pool, "idle-no-reserve-failed.example:8080", 10)
	dataPlane := &idleRouteDataPlaneStub{}
	manager.SetDataPlaneController(dataPlane)
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignment: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-idle-no-reserve", "worker-idle-no-reserve", "instance-idle-no-reserve",
	))
	if err != nil {
		t.Fatalf("claim initial route: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE proxies
		SET status='failed',base_health_status='failed',youtube_health_status='not_run',
		    health_generation=health_generation+1,updated_at=NOW()
		WHERE id=$1
	`, failedProxyID); err != nil {
		t.Fatalf("apply failed health state: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile idle route without reserve: %v", err)
	}

	paused, err := manager.Renew(ctx, RenewRequest{
		RenewRequestID:       "renew-idle-no-reserve-paused",
		SlotName:             claim.SlotName,
		WorkerID:             claim.WorkerID,
		WorkerInstanceID:     claim.WorkerInstanceID,
		LeaseID:              claim.LeaseID,
		KnownRouteGeneration: claim.AssignmentVersion,
	})
	if err != nil {
		t.Fatalf("renew paused idle route: %v", err)
	}
	if paused.Ready || !paused.RouteChanged || paused.LeaseID != claim.LeaseID ||
		paused.ProxyID != nil || paused.ControlState != "paused_no_reserve" ||
		paused.AssignmentVersion != claim.AssignmentVersion+1 ||
		paused.CredentialGeneration != claim.CredentialGeneration+1 ||
		paused.ReasonCode != "NO_POLICY_ELIGIBLE_RESERVE" || paused.RetryAfterMS <= 0 {
		t.Fatalf("paused idle assignment = %+v", paused)
	}
	if len(dataPlane.retiredUsers) != 1 || dataPlane.retiredUsers[0] != claim.ProxyUser {
		t.Fatalf("retired proxy users = %v, want %q", dataPlane.retiredUsers, claim.ProxyUser)
	}

	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("repeat paused reconciliation: %v", err)
	}
	stillPaused, err := manager.Renew(ctx, RenewRequest{
		RenewRequestID:       "renew-idle-no-reserve-stable",
		SlotName:             paused.SlotName,
		WorkerID:             paused.WorkerID,
		WorkerInstanceID:     paused.WorkerInstanceID,
		LeaseID:              paused.LeaseID,
		KnownRouteGeneration: paused.AssignmentVersion,
	})
	if err != nil {
		t.Fatalf("renew stable paused route: %v", err)
	}
	if stillPaused.Ready || stillPaused.RouteChanged ||
		stillPaused.AssignmentVersion != paused.AssignmentVersion ||
		stillPaused.CredentialGeneration != paused.CredentialGeneration ||
		len(dataPlane.retiredUsers) != 1 {
		t.Fatalf("stable paused assignment = %+v, retired users = %v", stillPaused, dataPlane.retiredUsers)
	}

	reserveProxyID := insertControlProxy(t, pool, "idle-no-reserve-recovered.example:8080", 20)
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile newly available reserve: %v", err)
	}
	recovered, err := manager.Renew(ctx, RenewRequest{
		RenewRequestID:       "renew-idle-no-reserve-recovered",
		SlotName:             paused.SlotName,
		WorkerID:             paused.WorkerID,
		WorkerInstanceID:     paused.WorkerInstanceID,
		LeaseID:              paused.LeaseID,
		KnownRouteGeneration: paused.AssignmentVersion,
	})
	if err != nil {
		t.Fatalf("renew recovered idle route: %v", err)
	}
	if !recovered.Ready || !recovered.RouteChanged || recovered.LeaseID != claim.LeaseID ||
		recovered.ProxyID == nil || *recovered.ProxyID != reserveProxyID ||
		recovered.ControlState != "leased_idle" ||
		recovered.AssignmentVersion != paused.AssignmentVersion+1 ||
		recovered.CredentialGeneration != paused.CredentialGeneration+1 ||
		dataPlane.activationCalls != 1 || dataPlane.oldUsername != paused.ProxyUser ||
		dataPlane.newUsername != recovered.ProxyUser {
		t.Fatalf("recovered idle assignment = %+v, data plane = %+v", recovered, dataPlane)
	}
}

type concurrentIdleRouteDataPlane struct {
	mu              sync.Mutex
	activationCalls int
}

func (*concurrentIdleRouteDataPlane) RefreshProxyUser(string) {}

func (*concurrentIdleRouteDataPlane) RetireProxyUser(context.Context, string) error {
	return nil
}

func (s *concurrentIdleRouteDataPlane) ActivateProxyUser(context.Context, string, string, int) error {
	s.mu.Lock()
	s.activationCalls++
	s.mu.Unlock()
	return nil
}

func (s *concurrentIdleRouteDataPlane) activations() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activationCalls
}

type blockingIdleRouteDataPlane struct {
	mu                     sync.Mutex
	activationCalls        int
	firstActivationStarted chan struct{}
	releaseFirstActivation chan struct{}
	startedOnce            sync.Once
}

func (*blockingIdleRouteDataPlane) RefreshProxyUser(string) {}

func (*blockingIdleRouteDataPlane) RetireProxyUser(context.Context, string) error {
	return nil
}

func (s *blockingIdleRouteDataPlane) ActivateProxyUser(
	ctx context.Context,
	_ string,
	_ string,
	_ int,
) error {
	s.mu.Lock()
	s.activationCalls++
	call := s.activationCalls
	s.mu.Unlock()
	if call != 1 {
		return nil
	}
	s.startedOnce.Do(func() { close(s.firstActivationStarted) })
	select {
	case <-s.releaseFirstActivation:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *blockingIdleRouteDataPlane) activations() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activationCalls
}

func TestPendingIdleRouteActivationIsClaimedOnceAcrossManagers(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	failedProxyID := insertControlProxy(t, pool, "activation-claim-failed.example:8080", 10)
	reserveProxyID := insertControlProxy(t, pool, "activation-claim-reserve.example:8080", 20)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignment: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-activation-fence", "worker-activation-fence", "instance-activation-fence",
	))
	if err != nil {
		t.Fatalf("claim initial Route: %v", err)
	}
	if claim.ProxyID == nil || *claim.ProxyID != failedProxyID {
		t.Fatalf("initial assignment = %+v, want proxy %d", claim, failedProxyID)
	}

	dataPlane := &blockingIdleRouteDataPlane{
		firstActivationStarted: make(chan struct{}),
		releaseFirstActivation: make(chan struct{}),
	}
	manager.SetDataPlaneController(dataPlane)
	otherManager := New(
		&database.DB{Pool: pool}, nil, nil, manager.options, nil,
	)
	otherManager.SetDataPlaneController(dataPlane)

	if _, err := pool.Exec(ctx, `
		UPDATE proxies
		SET status='failed',base_health_status='failed',youtube_health_status='not_run',
		    health_generation=health_generation+1,updated_at=NOW()
		WHERE id=$1
	`, failedProxyID); err != nil {
		t.Fatalf("apply failed health state: %v", err)
	}

	firstResult := make(chan error, 1)
	go func() {
		_, reconcileErr := manager.reconcile(ctx)
		firstResult <- reconcileErr
	}()
	select {
	case <-dataPlane.firstActivationStarted:
	case <-ctx.Done():
		t.Fatalf("first data-plane activation did not start: %v", ctx.Err())
	}

	if _, err := otherManager.reconcile(ctx); err != nil {
		t.Fatalf("concurrent reconciliation: %v", err)
	}
	if calls := dataPlane.activations(); calls != 1 {
		t.Fatalf("pending Route generation activated %d times while its owner was active, want 1", calls)
	}

	close(dataPlane.releaseFirstActivation)
	if err := <-firstResult; err != nil {
		t.Fatalf("finish claimed reconciliation: %v", err)
	}
	renewed, err := manager.Renew(ctx, RenewRequest{
		RenewRequestID:       "renew-activation-fence",
		SlotName:             claim.SlotName,
		WorkerID:             claim.WorkerID,
		WorkerInstanceID:     claim.WorkerInstanceID,
		LeaseID:              claim.LeaseID,
		KnownRouteGeneration: claim.AssignmentVersion,
	})
	if err != nil {
		t.Fatalf("renew activated Route: %v", err)
	}
	if !renewed.Ready || renewed.ProxyID == nil || *renewed.ProxyID != reserveProxyID ||
		renewed.AssignmentVersion != claim.AssignmentVersion+1 || dataPlane.activations() != 1 {
		t.Fatalf("activated assignment = %+v, activations = %d", renewed, dataPlane.activations())
	}
}

func TestAppliedHealthVerdictRacesRenewReconcileAndBeginTaskWithoutReusingFailedRoute(t *testing.T) {
	manager, pool := newProxyControlPostgresWithOptions(t, func(options *Options) {
		options.ChannelSlots = 2
	})
	ctx := context.Background()

	failedProxyID := insertControlProxy(t, pool, "race-health-failed.example:8080", 10)
	otherSlotProxyID := insertControlProxy(t, pool, "race-health-other-slot.example:8080", 20)
	reserveProxyID := insertControlProxy(t, pool, "race-health-reserve.example:8080", 30)
	dataPlane := &concurrentIdleRouteDataPlane{}
	manager.SetDataPlaneController(dataPlane)
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-health-race", "worker-health-race", "instance-health-race",
	))
	if err != nil {
		t.Fatalf("claim initial route: %v", err)
	}
	if claim.ProxyID == nil || *claim.ProxyID != failedProxyID {
		t.Fatalf("initial assignment = %+v, want proxy %d", claim, failedProxyID)
	}

	const healthVerdictGate int64 = controlAdvisoryLock + 1
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION gate_health_verdict_race() RETURNS trigger
		LANGUAGE plpgsql AS $function$
		BEGIN
		  PERFORM pg_advisory_xact_lock(82642118);
		  RETURN NEW;
		END
		$function$;
		CREATE TRIGGER gate_health_verdict_race
		AFTER UPDATE OF health_generation ON proxies
		FOR EACH ROW EXECUTE FUNCTION gate_health_verdict_race();
	`); err != nil {
		t.Fatalf("install health Verdict race gate: %v", err)
	}
	gate, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin concurrency gate: %v", err)
	}
	defer func() { _ = gate.Rollback(ctx) }()
	if _, err := gate.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, controlAdvisoryLock); err != nil {
		t.Fatalf("lock control concurrency gate: %v", err)
	}
	if _, err := gate.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, healthVerdictGate); err != nil {
		t.Fatalf("lock health concurrency gate: %v", err)
	}

	healthPoolConfig := pool.Config()
	healthApplicationName := "health-verdict-race-" + claim.LeaseID
	healthPoolConfig.ConnConfig.RuntimeParams["application_name"] = healthApplicationName
	healthPool, err := pgxpool.NewWithConfig(ctx, healthPoolConfig)
	if err != nil {
		t.Fatalf("open health Verdict connection: %v", err)
	}
	defer healthPool.Close()
	proxyRepository := repository.NewProxyRepository(&database.DB{Pool: healthPool})
	now := time.Now().UTC()
	healthEvidence := proxylifecycle.HealthEvidence{
		StartedAt: now,
		CheckedAt: now,
		Base: proxylifecycle.ProbeEvidence{
			Status: proxylifecycle.ProbeFailed,
			Error:  "proxy route unavailable",
		},
		YouTube: proxylifecycle.ProbeEvidence{Status: proxylifecycle.ProbeNotRun},
		Verdict: proxylifecycle.Verdict{
			Kind:               proxylifecycle.FailureSoftUnreachable,
			Conclusive:         true,
			ControlPathHealthy: true,
		},
		Error: "proxy route unavailable",
	}
	type healthResult struct {
		decision proxylifecycle.Decision
		applied  bool
		err      error
	}

	start := make(chan struct{})
	operationErrors := make(chan error, 4)
	beginResult := make(chan error, 1)
	healthResults := make(chan healthResult, 1)
	var operations sync.WaitGroup
	operations.Add(1)
	go func() {
		defer operations.Done()
		<-start
		decision, applied, healthErr := proxyRepository.ApplyHealthVerdict(
			ctx, failedProxyID, healthEvidence, proxylifecycle.DefaultPolicy(),
		)
		if healthErr == nil && applied {
			manager.NotifyHealthVerdictApplied(failedProxyID)
			manager.NotifyHealthVerdictApplied(failedProxyID)
		}
		healthResults <- healthResult{decision: decision, applied: applied, err: healthErr}
	}()
	for range 2 {
		operations.Add(1)
		go func() {
			defer operations.Done()
			<-start
			_, reconcileErr := manager.reconcile(ctx)
			operationErrors <- reconcileErr
		}()
	}
	for _, requestID := range []string{"renew-health-race-1", "renew-health-race-2"} {
		operations.Add(1)
		go func() {
			defer operations.Done()
			<-start
			_, renewErr := manager.Renew(ctx, RenewRequest{
				RenewRequestID:       requestID,
				SlotName:             claim.SlotName,
				WorkerID:             claim.WorkerID,
				WorkerInstanceID:     claim.WorkerInstanceID,
				LeaseID:              claim.LeaseID,
				KnownRouteGeneration: claim.AssignmentVersion,
			})
			operationErrors <- renewErr
		}()
	}
	operations.Add(1)
	go func() {
		defer operations.Done()
		<-start
		_, beginErr := manager.BeginTask(ctx, BeginTaskRequest{
			SlotName: claim.SlotName, WorkerID: claim.WorkerID,
			WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
			RouteGeneration:  claim.AssignmentVersion,
			AttemptRequestID: "attempt-health-race",
			BusinessRunID:    "business-health-race",
			JobExecutionID:   "exec:v1:health-race",
			TaskKind:         TaskKindChannelFull,
		})
		beginResult <- beginErr
	}()
	close(start)

	verdictBlocked := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS (
			  SELECT 1 FROM pg_stat_activity
			  WHERE application_name=$1 AND state='active'
			    AND wait_event_type='Lock' AND wait_event='advisory'
			)
		`, healthApplicationName).Scan(&verdictBlocked); err != nil {
			t.Fatalf("observe blocked health Verdict: %v", err)
		}
		if verdictBlocked {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := gate.Commit(ctx); err != nil {
		t.Fatalf("release concurrency gates: %v", err)
	}
	operations.Wait()
	close(operationErrors)
	if !verdictBlocked {
		t.Fatal("health Verdict did not overlap the control operations")
	}
	health := <-healthResults
	if health.err != nil {
		t.Fatalf("apply failed health Verdict: %v", health.err)
	}
	if !health.applied || health.decision.Status != proxylifecycle.StatusFailed {
		t.Fatalf("health Verdict applied=%v decision=%+v", health.applied, health.decision)
	}

	for operationErr := range operationErrors {
		if operationErr != nil {
			t.Fatalf("concurrent control operation: %v", operationErr)
		}
	}
	beginErr := <-beginResult
	if !errors.Is(beginErr, ErrRouteNotReady) && !errors.Is(beginErr, ErrLeaseConflict) {
		t.Fatalf("concurrent BeginTask error = %v, want route not ready or stale Route fence", beginErr)
	}

	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("stabilize post-Verdict route: %v", err)
	}
	stable, err := manager.Renew(ctx, RenewRequest{
		RenewRequestID:       "renew-health-race-stable",
		SlotName:             claim.SlotName,
		WorkerID:             claim.WorkerID,
		WorkerInstanceID:     claim.WorkerInstanceID,
		LeaseID:              claim.LeaseID,
		KnownRouteGeneration: claim.AssignmentVersion,
	})
	if err != nil {
		t.Fatalf("renew stable replacement: %v", err)
	}
	if !stable.Ready || stable.ProxyID == nil || *stable.ProxyID != reserveProxyID ||
		stable.AssignmentVersion != claim.AssignmentVersion+1 ||
		stable.CredentialGeneration != claim.CredentialGeneration+1 ||
		dataPlane.activations() != 1 {
		t.Fatalf("stable replacement = %+v, activations = %d", stable, dataPlane.activations())
	}

	var assignedCount, distinctProxyCount, failedBindings, taskCount, healthGeneration int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*),COUNT(DISTINCT proxy_id),
		       COUNT(*) FILTER (WHERE proxy_id=$1)
		FROM proxy_running_slots
		WHERE proxy_id IS NOT NULL
	`, failedProxyID).Scan(&assignedCount, &distinctProxyCount, &failedBindings); err != nil {
		t.Fatalf("load final proxy bindings: %v", err)
	}
	if assignedCount != 2 || distinctProxyCount != 2 || failedBindings != 0 {
		t.Fatalf(
			"final bindings assigned=%d distinct=%d failed=%d; other=%d reserve=%d",
			assignedCount, distinctProxyCount, failedBindings, otherSlotProxyID, reserveProxyID,
		)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM proxy_control_tasks
		WHERE workload_scope='qy-test' AND business_run_id='business-health-race'
	`).Scan(&taskCount); err != nil {
		t.Fatalf("count raced tasks: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT health_generation FROM proxies WHERE id=$1`, failedProxyID).
		Scan(&healthGeneration); err != nil {
		t.Fatalf("load health generation: %v", err)
	}
	if taskCount != 0 || healthGeneration != 1 {
		t.Fatalf("raced task count=%d health generation=%d, want 0/1", taskCount, healthGeneration)
	}
}
