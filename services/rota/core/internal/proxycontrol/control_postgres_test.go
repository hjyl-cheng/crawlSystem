package proxycontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type claimedRouteDataPlane struct {
	beginCalls          int
	commitCalls         int
	retireCalls         int
	retireErr           error
	username            string
	claimID             string
	committed           bool
	committedByUsername map[string]string
}

type testRouteActivation struct {
	claimID string
	phase   RouteActivationPhase
}

type committedRouteDataPlane struct {
	mu          sync.Mutex
	manager     *Manager
	activations map[string]testRouteActivation
}

func (s *committedRouteDataPlane) RefreshProxyUser(username string) {
	if s.manager == nil {
		return
	}
	s.manager.invalidateMu.RLock()
	invalidate := s.manager.invalidate
	s.manager.invalidateMu.RUnlock()
	if invalidate != nil {
		invalidate(username)
	}
}

func (s *committedRouteDataPlane) RetireProxyUser(
	_ context.Context,
	username string,
) error {
	s.mu.Lock()
	delete(s.activations, username)
	s.mu.Unlock()
	s.RefreshProxyUser(username)
	return nil
}

func (*committedRouteDataPlane) RequireRouteActivationRegistry() {}

func (s *committedRouteDataPlane) RebuildRouteActivationRegistry(
	_ context.Context,
	entries []RouteActivationRegistryEntry,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activations = make(map[string]testRouteActivation, len(entries))
	for _, entry := range entries {
		s.activations[entry.Username] = testRouteActivation{
			claimID: entry.ClaimID,
			phase:   entry.Phase,
		}
	}
	return nil
}

func (s *committedRouteDataPlane) BeginProxyUserActivation(
	_ context.Context,
	_ string,
	newUsername string,
	_ int,
	previousClaimID string,
	claimID string,
) (RouteActivationBeginResult, error) {
	if !strings.HasPrefix(claimID, "lease:") {
		return RouteActivationBeginResult{}, errors.New("idle Route transition data plane is not configured")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found := s.activations[newUsername]
	if found && current.claimID == claimID {
		return RouteActivationBeginResult{
			AlreadyCommitted: current.phase == RouteActivationCommitted,
		}, nil
	}
	if found && current.claimID == "" && previousClaimID == "" &&
		current.phase == RouteActivationCommitted {
		return RouteActivationBeginResult{AlreadyCommitted: true}, nil
	}
	if found && current.claimID != previousClaimID {
		return RouteActivationBeginResult{}, errors.New("activation claim CAS conflict")
	}
	s.activations[newUsername] = testRouteActivation{
		claimID: claimID,
		phase:   RouteActivationActivating,
	}
	return RouteActivationBeginResult{}, nil
}

func (s *committedRouteDataPlane) CommitProxyUserActivation(
	_ context.Context,
	username string,
	claimID string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found := s.activations[username]
	if !found || current.claimID != claimID {
		return errors.New("activation claim changed before Commit")
	}
	current.phase = RouteActivationCommitted
	s.activations[username] = current
	return nil
}

func (s *committedRouteDataPlane) RetireProxyUserIfClaim(
	_ context.Context,
	username string,
	claimID string,
) (bool, error) {
	s.mu.Lock()
	current, found := s.activations[username]
	if !found || current.claimID != claimID || current.phase != RouteActivationActivating {
		s.mu.Unlock()
		return false, nil
	}
	delete(s.activations, username)
	s.mu.Unlock()
	s.RefreshProxyUser(username)
	return true, nil
}

func (*claimedRouteDataPlane) RefreshProxyUser(string) {}

func (s *claimedRouteDataPlane) RetireProxyUser(_ context.Context, username string) error {
	s.retireCalls++
	if s.retireErr != nil {
		return s.retireErr
	}
	delete(s.committedByUsername, username)
	if s.username == username {
		s.committed = false
	}
	return nil
}

func (*claimedRouteDataPlane) RequireRouteActivationRegistry() {}

func (*claimedRouteDataPlane) RebuildRouteActivationRegistry(
	context.Context,
	[]RouteActivationRegistryEntry,
) error {
	return nil
}

func (s *claimedRouteDataPlane) BeginProxyUserActivation(
	_ context.Context,
	oldUsername string,
	newUsername string,
	_ int,
	_ string,
	claimID string,
) (RouteActivationBeginResult, error) {
	s.beginCalls++
	if s.committedByUsername[newUsername] == claimID {
		return RouteActivationBeginResult{AlreadyCommitted: true}, nil
	}
	delete(s.committedByUsername, oldUsername)
	s.username = newUsername
	s.claimID = claimID
	s.committed = false
	return RouteActivationBeginResult{}, nil
}

func (s *claimedRouteDataPlane) CommitProxyUserActivation(
	_ context.Context,
	username string,
	claimID string,
) error {
	s.commitCalls++
	if s.username != username || s.claimID != claimID {
		return fmt.Errorf("activation identity changed before Commit")
	}
	if s.committedByUsername == nil {
		s.committedByUsername = make(map[string]string)
	}
	s.committedByUsername[username] = claimID
	s.committed = true
	return nil
}

func (*claimedRouteDataPlane) RetireProxyUserIfClaim(
	context.Context,
	string,
	string,
) (bool, error) {
	return false, nil
}

func (s *claimedRouteDataPlane) ready(username, claimID string) bool {
	return s.committedByUsername[username] == claimID
}

func TestClaimPublishesCommittedProxyUserAndReplayConfirmsIt(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "claim-connect.example:8080", 10)
	dataPlane := &claimedRouteDataPlane{}
	if err := manager.SetDataPlaneController(dataPlane); err != nil {
		t.Fatalf("set Claim data plane: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile Claim route: %v", err)
	}

	request := testClaimRequest("claim-connect-request", "claim-connect-worker", "claim-connect-instance")
	first, err := manager.Claim(ctx, request)
	if err != nil {
		t.Fatalf("Claim ready route: %v", err)
	}
	activationClaimID := "lease:" + first.LeaseID
	if !first.Ready || !dataPlane.ready(first.ProxyUser, activationClaimID) {
		t.Fatalf("Claim returned before committed data-plane publication: assignment=%+v data_plane=%+v", first, dataPlane)
	}

	replayed, err := manager.Claim(ctx, request)
	if err != nil {
		t.Fatalf("replay Claim: %v", err)
	}
	if replayed.LeaseID != first.LeaseID || replayed.ProxyUser != first.ProxyUser {
		t.Fatalf("replayed assignment changed: first=%+v replayed=%+v", first, replayed)
	}
	if dataPlane.beginCalls != 2 || dataPlane.commitCalls != 1 ||
		!dataPlane.ready(replayed.ProxyUser, activationClaimID) {
		t.Fatalf("Claim replay did not confirm committed publication: %+v", dataPlane)
	}
}

func TestClaimReplayKeepsCommittedRouteDuringActiveTask(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "claim-active-replay.example:8080", 10)
	dataPlane := &claimedRouteDataPlane{}
	if err := manager.SetDataPlaneController(dataPlane); err != nil {
		t.Fatalf("set Claim data plane: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile Claim route: %v", err)
	}

	request := testClaimRequest(
		"claim-active-replay-request",
		"claim-active-replay-worker",
		"claim-active-replay-instance",
	)
	claimed, err := manager.Claim(ctx, request)
	if err != nil {
		t.Fatalf("Claim ready route: %v", err)
	}
	if _, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName:         claimed.SlotName,
		WorkerID:         claimed.WorkerID,
		WorkerInstanceID: claimed.WorkerInstanceID,
		LeaseID:          claimed.LeaseID,
		RouteGeneration:  claimed.AssignmentVersion,
		AttemptRequestID: "claim-active-replay-attempt",
		BusinessRunID:    "claim-active-replay-run",
		JobExecutionID:   "claim-active-replay-execution",
		TaskKind:         TaskKindChannelFull,
	}); err != nil {
		t.Fatalf("begin active Task: %v", err)
	}

	replayed, err := manager.Claim(ctx, request)
	if err != nil {
		t.Fatalf("replay Claim during active Task: %v", err)
	}
	if replayed.LeaseID != claimed.LeaseID || replayed.ProxyUser != claimed.ProxyUser {
		t.Fatalf("active Claim replay changed Route: first=%+v replayed=%+v", claimed, replayed)
	}
	if dataPlane.retireCalls != 0 || !dataPlane.committed {
		t.Fatalf("active Claim replay retired its committed Route: %+v", dataPlane)
	}
}

func TestClaimAfterLeaseExpiryRetiresTheExpiredWorkersRoute(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "claim-expiry.example:8080", 10)
	dataPlane := &claimedRouteDataPlane{}
	if err := manager.SetDataPlaneController(dataPlane); err != nil {
		t.Fatalf("set Claim data plane: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile Claim route: %v", err)
	}

	first, err := manager.Claim(ctx, testClaimRequest(
		"claim-expiry-first-request",
		"claim-expiry-first-worker",
		"claim-expiry-first-instance",
	))
	if err != nil {
		t.Fatalf("first Claim: %v", err)
	}
	firstClaimID := "lease:" + first.LeaseID
	if !dataPlane.ready(first.ProxyUser, firstClaimID) {
		t.Fatalf("first Claim route was not committed: assignment=%+v data_plane=%+v", first, dataPlane)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_control_leases SET lease_until=NOW()-interval '1 second'
		WHERE lease_id=$1
	`, first.LeaseID); err != nil {
		t.Fatalf("expire first Lease history: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots SET lease_until=NOW()-interval '1 second'
		WHERE current_lease_id=$1
	`, first.LeaseID); err != nil {
		t.Fatalf("expire first Slot Lease: %v", err)
	}

	second, err := manager.Claim(ctx, testClaimRequest(
		"claim-expiry-second-request",
		"claim-expiry-second-worker",
		"claim-expiry-second-instance",
	))
	if err != nil {
		t.Fatalf("second Claim: %v", err)
	}
	if second.CredentialGeneration != first.CredentialGeneration+1 {
		t.Fatalf(
			"replacement Claim credential generation = %d, want %d",
			second.CredentialGeneration,
			first.CredentialGeneration+1,
		)
	}
	if dataPlane.ready(first.ProxyUser, firstClaimID) {
		t.Fatalf("expired Worker route %q remained committed after replacement Claim", first.ProxyUser)
	}
	if !dataPlane.ready(second.ProxyUser, "lease:"+second.LeaseID) {
		t.Fatalf("replacement Claim route was not committed: assignment=%+v data_plane=%+v", second, dataPlane)
	}
}

func TestExpiredClaimReplayCommitsExpiryAndRetiresItsRoute(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "claim-expired-replay.example:8080", 10)
	dataPlane := &claimedRouteDataPlane{}
	if err := manager.SetDataPlaneController(dataPlane); err != nil {
		t.Fatalf("set Claim data plane: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile Claim route: %v", err)
	}

	request := testClaimRequest(
		"claim-expired-replay-request",
		"claim-expired-replay-worker",
		"claim-expired-replay-instance",
	)
	claimed, err := manager.Claim(ctx, request)
	if err != nil {
		t.Fatalf("initial Claim: %v", err)
	}
	activationClaimID := "lease:" + claimed.LeaseID
	if !dataPlane.ready(claimed.ProxyUser, activationClaimID) {
		t.Fatalf("initial Claim route was not committed: assignment=%+v data_plane=%+v", claimed, dataPlane)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_control_leases SET lease_until=NOW()-interval '1 second'
		WHERE lease_id=$1
	`, claimed.LeaseID); err != nil {
		t.Fatalf("expire Claim Lease history: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots SET lease_until=NOW()-interval '1 second'
		WHERE current_lease_id=$1
	`, claimed.LeaseID); err != nil {
		t.Fatalf("expire Claim Slot Lease: %v", err)
	}

	if replayed, err := manager.Claim(ctx, request); !errors.Is(err, ErrLeaseGone) {
		t.Fatalf("expired Claim replay: assignment=%+v err=%v", replayed, err)
	}
	if dataPlane.ready(claimed.ProxyUser, activationClaimID) {
		t.Fatalf("expired Claim replay left Route %q committed", claimed.ProxyUser)
	}

	var leaseStatus string
	if err := pool.QueryRow(ctx, `
		SELECT status FROM proxy_control_leases WHERE lease_id=$1
	`, claimed.LeaseID).Scan(&leaseStatus); err != nil {
		t.Fatalf("load expired Claim Lease: %v", err)
	}
	if leaseStatus != "expired" {
		t.Fatalf("expired Claim replay left Lease status %q", leaseStatus)
	}

	var currentLeaseID *string
	var workerID *string
	var rotatedUsername string
	if err := pool.QueryRow(ctx, `
		SELECT slot.current_lease_id,slot.worker_id,proxy_user.username
		FROM proxy_running_slots AS slot
		JOIN proxy_users AS proxy_user ON proxy_user.id=slot.user_id
		WHERE slot.slot_name=$1
	`, claimed.SlotName).Scan(&currentLeaseID, &workerID, &rotatedUsername); err != nil {
		t.Fatalf("load expired Claim Slot: %v", err)
	}
	if currentLeaseID != nil || workerID != nil {
		t.Fatalf("expired Claim replay left Slot leased: lease=%v worker=%v", currentLeaseID, workerID)
	}
	if rotatedUsername == claimed.ProxyUser {
		t.Fatalf("expired Claim replay did not rotate credential %q", claimed.ProxyUser)
	}
}

func TestConflictingExpiredClaimReplayStillCommitsExpiryAndRetiresItsRoute(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "claim-expired-conflict.example:8080", 10)
	dataPlane := &claimedRouteDataPlane{}
	if err := manager.SetDataPlaneController(dataPlane); err != nil {
		t.Fatalf("set Claim data plane: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile Claim route: %v", err)
	}

	request := testClaimRequest(
		"claim-expired-conflict-request",
		"claim-expired-conflict-worker",
		"claim-expired-conflict-instance",
	)
	claimed, err := manager.Claim(ctx, request)
	if err != nil {
		t.Fatalf("initial Claim: %v", err)
	}
	activationClaimID := "lease:" + claimed.LeaseID
	if !dataPlane.ready(claimed.ProxyUser, activationClaimID) {
		t.Fatalf("initial Claim route was not committed: assignment=%+v data_plane=%+v", claimed, dataPlane)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_control_leases SET lease_until=NOW()-interval '1 second'
		WHERE lease_id=$1
	`, claimed.LeaseID); err != nil {
		t.Fatalf("expire Claim Lease history: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots SET lease_until=NOW()-interval '1 second'
		WHERE current_lease_id=$1
	`, claimed.LeaseID); err != nil {
		t.Fatalf("expire Claim Slot Lease: %v", err)
	}

	conflicting := request
	conflicting.WorkerInstanceID = "claim-expired-conflict-other-instance"
	if replayed, err := manager.Claim(ctx, conflicting); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting expired Claim replay: assignment=%+v err=%v", replayed, err)
	}
	if dataPlane.ready(claimed.ProxyUser, activationClaimID) {
		t.Fatalf("expired Route %q remained committed after conflicting Claim replay", claimed.ProxyUser)
	}

	var leaseStatus string
	if err := pool.QueryRow(ctx, `
		SELECT status FROM proxy_control_leases WHERE lease_id=$1
	`, claimed.LeaseID).Scan(&leaseStatus); err != nil {
		t.Fatalf("load expired conflicting Claim Lease: %v", err)
	}
	if leaseStatus != "expired" {
		t.Fatalf("conflicting replay left expired Lease status %q", leaseStatus)
	}
}

func TestClaimAfterLeaseExpiryFailsClosedUntilTheExpiredRouteIsRetired(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "claim-expiry-retire-failure.example:8080", 10)
	dataPlane := &claimedRouteDataPlane{}
	if err := manager.SetDataPlaneController(dataPlane); err != nil {
		t.Fatalf("set Claim data plane: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile Claim route: %v", err)
	}

	first, err := manager.Claim(ctx, testClaimRequest(
		"claim-expiry-retire-failure-first",
		"claim-expiry-retire-failure-worker-one",
		"claim-expiry-retire-failure-instance-one",
	))
	if err != nil {
		t.Fatalf("first Claim: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_control_leases SET lease_until=NOW()-interval '1 second'
		WHERE lease_id=$1
	`, first.LeaseID); err != nil {
		t.Fatalf("expire first Lease history: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots SET lease_until=NOW()-interval '1 second'
		WHERE current_lease_id=$1
	`, first.LeaseID); err != nil {
		t.Fatalf("expire first Slot Lease: %v", err)
	}

	dataPlane.retireErr = errors.New("data-plane retirement unavailable")
	request := testClaimRequest(
		"claim-expiry-retire-failure-second",
		"claim-expiry-retire-failure-worker-two",
		"claim-expiry-retire-failure-instance-two",
	)
	if assignment, err := manager.Claim(ctx, request); err == nil || assignment.Ready {
		t.Fatalf("replacement Claim crossed a failed retirement: assignment=%+v err=%v", assignment, err)
	}

	var replacementCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM proxy_control_leases
		WHERE workload_scope='qy-test' AND claim_request_id=$1
	`, request.ClaimRequestID).Scan(&replacementCount); err != nil {
		t.Fatalf("count failed replacement Leases: %v", err)
	}
	if replacementCount != 0 {
		t.Fatalf("failed retirement committed %d replacement Leases", replacementCount)
	}

	dataPlane.retireErr = nil
	second, err := manager.Claim(ctx, request)
	if err != nil {
		t.Fatalf("retry replacement Claim after retirement recovery: %v", err)
	}
	if !second.Ready || !dataPlane.ready(second.ProxyUser, "lease:"+second.LeaseID) {
		t.Fatalf("replacement Claim did not recover after retirement: %+v", second)
	}
}

func TestReconcileRetiresAnExpiredWorkersRouteBeforeAnotherClaim(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "reconcile-expiry.example:8080", 10)
	dataPlane := &claimedRouteDataPlane{}
	if err := manager.SetDataPlaneController(dataPlane); err != nil {
		t.Fatalf("set Claim data plane: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile Claim route: %v", err)
	}

	claimed, err := manager.Claim(ctx, testClaimRequest(
		"reconcile-expiry-request",
		"reconcile-expiry-worker",
		"reconcile-expiry-instance",
	))
	if err != nil {
		t.Fatalf("Claim route before expiry: %v", err)
	}
	claimID := "lease:" + claimed.LeaseID
	if !dataPlane.ready(claimed.ProxyUser, claimID) {
		t.Fatalf("Claim route was not committed: assignment=%+v data_plane=%+v", claimed, dataPlane)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_control_leases SET lease_until=NOW()-interval '1 second'
		WHERE lease_id=$1
	`, claimed.LeaseID); err != nil {
		t.Fatalf("expire Lease history: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots SET lease_until=NOW()-interval '1 second'
		WHERE current_lease_id=$1
	`, claimed.LeaseID); err != nil {
		t.Fatalf("expire Slot Lease: %v", err)
	}

	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile expired Lease: %v", err)
	}
	if dataPlane.ready(claimed.ProxyUser, claimID) {
		t.Fatalf("expired Worker route %q remained committed after Reconcile", claimed.ProxyUser)
	}
}

func TestReconcileFailureAfterExpiryStillCommitsCleanupAndRetiresRoute(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "reconcile-expiry-failure.example:8080", 10)
	dataPlane := &claimedRouteDataPlane{}
	if err := manager.SetDataPlaneController(dataPlane); err != nil {
		t.Fatalf("set Claim data plane: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile Claim route: %v", err)
	}
	claimed, err := manager.Claim(ctx, testClaimRequest(
		"reconcile-expiry-failure-request",
		"reconcile-expiry-failure-worker",
		"reconcile-expiry-failure-instance",
	))
	if err != nil {
		t.Fatalf("Claim route before expiry: %v", err)
	}
	activationClaimID := "lease:" + claimed.LeaseID
	if !dataPlane.ready(claimed.ProxyUser, activationClaimID) {
		t.Fatalf("Claim route was not committed: assignment=%+v data_plane=%+v", claimed, dataPlane)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_control_leases SET lease_until=NOW()-interval '1 second'
		WHERE lease_id=$1
	`, claimed.LeaseID); err != nil {
		t.Fatalf("expire Lease history: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots SET lease_until=NOW()-interval '1 second'
		WHERE current_lease_id=$1
	`, claimed.LeaseID); err != nil {
		t.Fatalf("expire Slot Lease: %v", err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE pool_proxies RENAME TO pool_proxies_unavailable`); err != nil {
		t.Fatalf("inject post-expiry reconciliation failure: %v", err)
	}

	if _, err := manager.reconcile(ctx); err == nil {
		t.Fatal("Reconcile unexpectedly succeeded after its post-expiry query was removed")
	}
	if dataPlane.ready(claimed.ProxyUser, activationClaimID) {
		t.Fatalf("expired Route %q remained committed after Reconcile failure", claimed.ProxyUser)
	}

	var leaseStatus string
	var currentLeaseID *string
	if err := pool.QueryRow(ctx, `
		SELECT lease.status,slot.current_lease_id
		FROM proxy_control_leases AS lease
		JOIN proxy_running_slots AS slot ON slot.slot_name=lease.slot_name
		WHERE lease.lease_id=$1
	`, claimed.LeaseID).Scan(&leaseStatus, &currentLeaseID); err != nil {
		t.Fatalf("load independently expired Lease: %v", err)
	}
	if leaseStatus != "expired" || currentLeaseID != nil {
		t.Fatalf("failed Reconcile rolled back expiry: status=%q current_lease_id=%v", leaseStatus, currentLeaseID)
	}
}

func TestClaimFailsClosedWhenDataPlaneIsUnavailable(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "claim-no-data-plane.example:8080", 10)
	if err := manager.SetDataPlaneController(nil); err != nil {
		t.Fatalf("remove Claim data plane: %v", err)
	}
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile Claim route: %v", err)
	}

	request := testClaimRequest(
		"claim-no-data-plane-request",
		"claim-no-data-plane-worker",
		"claim-no-data-plane-instance",
	)
	assignment, err := manager.Claim(ctx, request)
	if !errors.Is(err, ErrDisabled) {
		t.Fatalf("Claim without a data plane error = %v, want ErrDisabled", err)
	}
	if assignment.Ready || assignment.LeaseID != "" || assignment.ProxyUser != "" {
		t.Fatalf("Claim exposed an unpublished Route: %+v", assignment)
	}

	var persistedLeaseID string
	if err := pool.QueryRow(ctx, `
		SELECT lease_id
		FROM proxy_control_leases
		WHERE workload_scope='qy-test' AND claim_request_id=$1
	`, request.ClaimRequestID).Scan(&persistedLeaseID); err != nil {
		t.Fatalf("load unpublished persisted Lease: %v", err)
	}
	dataPlane := &claimedRouteDataPlane{}
	if err := manager.SetDataPlaneController(dataPlane); err != nil {
		t.Fatalf("install recovered Claim data plane: %v", err)
	}
	replayed, err := manager.Claim(ctx, request)
	if err != nil {
		t.Fatalf("replay persisted Claim after data-plane recovery: %v", err)
	}
	if replayed.LeaseID != persistedLeaseID || !replayed.Ready ||
		!dataPlane.ready(replayed.ProxyUser, "lease:"+persistedLeaseID) {
		t.Fatalf(
			"replayed Claim did not recover persisted Lease: persisted=%q assignment=%+v data_plane=%+v",
			persistedLeaseID,
			replayed,
			dataPlane,
		)
	}
}

func TestClaimV2IsIdempotentAndFencesWorkerInstances(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "claim-v2.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}

	request := testClaimRequest("claim-v2-request", "worker-v2", "instance-v2")
	first, err := manager.Claim(ctx, request)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	replayed, err := manager.Claim(ctx, request)
	if err != nil {
		t.Fatalf("replayed claim: %v", err)
	}
	if first.LeaseID == "" || replayed.LeaseID != first.LeaseID || replayed.SlotName != first.SlotName {
		t.Fatalf("first=%+v replayed=%+v", first, replayed)
	}
	if first.WorkerInstanceID != request.WorkerInstanceID ||
		first.IdentityPolicyID != request.IdentityPolicyID ||
		first.IdentityPolicyVersion != request.IdentityPolicyVersion ||
		first.IdentityPolicyHash != "sha256:test-channel-v1" ||
		first.ProtocolVersion != ProtocolVersionV2 || first.WorkloadScope != "qy-test" {
		t.Fatalf("claim assignment identity = %+v", first)
	}

	otherInstance := testClaimRequest("claim-v2-other-instance", "worker-v2", "instance-v2-other")
	if _, err := manager.Claim(ctx, otherInstance); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("other instance claim error = %v, want lease conflict", err)
	}
	reusedID := testClaimRequest("claim-v2-request", "worker-v2-other", "instance-v2-other")
	if _, err := manager.Claim(ctx, reusedID); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("reused request id error = %v, want idempotency conflict", err)
	}

	var leaseCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM proxy_control_leases
		WHERE workload_scope='qy-test' AND claim_request_id='claim-v2-request'
	`).Scan(&leaseCount); err != nil {
		t.Fatalf("count persisted leases: %v", err)
	}
	if leaseCount != 1 {
		t.Fatalf("persisted lease count = %d, want 1", leaseCount)
	}

	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("encode assignment: %v", err)
	}
	if strings.Contains(string(encoded), "proxy_id") || strings.Contains(string(encoded), "proxy_address_hash") {
		t.Fatalf("assignment leaked internal proxy identity: %s", encoded)
	}
}

func TestRouteActivationRegistryOnlyCommitsLiveReadyLeases(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "registry-fence.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignment: %v", err)
	}

	registry, err := manager.loadRouteActivationRegistry(ctx)
	if err != nil {
		t.Fatalf("load unleased registry: %v", err)
	}
	if len(registry) != 1 || !registry[0].Blocked || registry[0].Phase != "" {
		t.Fatalf("unleased registry = %+v, want blocked without a phase", registry)
	}

	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-registry-fence", "worker-registry-fence", "instance-registry-fence",
	))
	if err != nil {
		t.Fatalf("claim ready Route: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots
		SET ready_after=NOW(),control_state='leased_idle'
		WHERE slot_name=$1 AND current_lease_id=$2
	`, claim.SlotName, claim.LeaseID); err != nil {
		t.Fatalf("finalize ready Route fixture: %v", err)
	}
	registry, err = manager.loadRouteActivationRegistry(ctx)
	if err != nil {
		t.Fatalf("load live registry: %v", err)
	}
	if len(registry) != 1 || registry[0].Blocked ||
		registry[0].Phase != RouteActivationCommitted {
		t.Fatalf("live ready registry = %+v, want committed", registry)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots
		SET lease_until=NOW()-interval '1 second'
		WHERE slot_name=$1 AND current_lease_id=$2
	`, claim.SlotName, claim.LeaseID); err != nil {
		t.Fatalf("expire Slot Lease: %v", err)
	}
	registry, err = manager.loadRouteActivationRegistry(ctx)
	if err != nil {
		t.Fatalf("load expired registry: %v", err)
	}
	if len(registry) != 1 || !registry[0].Blocked || registry[0].Phase != "" {
		t.Fatalf("expired registry = %+v, want blocked without a phase", registry)
	}
}

func TestQueryQualitySlotCanBeProvisionedClaimedAndReportedInCapacity(t *testing.T) {
	manager, pool := newProxyControlPostgresWithOptions(t, func(options *Options) {
		options.ChannelSlots = 0
		options.QueryQualitySlots = 1
		options.IdentityPolicies = map[string]IdentityPolicy{
			"qy-test-query-quality-v1": {
				ID: "qy-test-query-quality-v1", Version: 1,
				Hash: "sha256:test-query-quality-v1", Role: RoleQueryQuality,
			},
		}
	})
	ctx := context.Background()

	_ = insertControlProxy(t, pool, "query-quality.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync query quality resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile query quality assignment: %v", err)
	}

	claim, err := manager.Claim(ctx, ClaimRequest{
		ClaimRequestID: "claim-query-quality", ProtocolVersion: ProtocolVersionV2,
		Role: RoleQueryQuality, WorkerID: "worker-query-quality",
		WorkerInstanceID: "instance-query-quality",
		IdentityPolicyID: "qy-test-query-quality-v1", IdentityPolicyVersion: 1,
	})
	if err != nil {
		t.Fatalf("claim query quality slot: %v", err)
	}
	if !claim.Ready || claim.Role != RoleQueryQuality || claim.SlotName != "bullmq-query_quality-01" {
		t.Fatalf("query quality claim = %+v", claim)
	}

	capacity, err := manager.Capacity(ctx)
	if err != nil {
		t.Fatalf("query quality capacity: %v", err)
	}
	role := capacity.Roles[RoleQueryQuality]
	if role.Desired != 1 || role.Provisioned != 1 || role.Assigned != 1 || role.Ready != 1 || role.Claimed != 1 {
		t.Fatalf("query quality role capacity = %+v", role)
	}
}

func TestPolicyPreferencesOrderCandidatesWithoutReducingCapacity(t *testing.T) {
	manager, pool := newProxyControlPostgresWithOptions(t, func(options *Options) {
		options.ChannelSlots = 2
		options.CatalogVersion = 7
		options.CatalogDigest = "sha256:test-catalog-v7"
		options.IdentityPolicies["qy-test-channel-v1"] = IdentityPolicy{
			ID: "qy-test-channel-v1", Version: 1, Hash: "sha256:test-channel-v1",
			Role: RoleChannel, RequiredEgressCountry: "BR",
			AllowedProxyTags:   []string{"role:channel"},
			GeoFreshnessWindow: 2 * time.Hour, AttemptSafetyWindow: 10 * time.Minute,
		}
	})
	ctx := context.Background()
	now := time.Now()

	wrongCountryID := insertControlProxy(t, pool, "capacity-us.example:8080", 10)
	staleGeoID := insertControlProxy(t, pool, "capacity-stale-geo.example:8080", 20)
	rotatingID := insertControlProxy(t, pool, "capacity-rotating.example:8080", 30)
	expiringID := insertControlProxy(t, pool, "capacity-expiring.example:8080", 40)
	eligibleID := insertControlProxy(t, pool, "capacity-eligible.example:8080", 50)
	for _, proxyID := range []int{wrongCountryID, staleGeoID, rotatingID, expiringID, eligibleID} {
		if _, err := pool.Exec(ctx, `
			UPDATE proxies
			SET tags=ARRAY['role:channel'],country_code='BR',country_verified_at=$2,
			    egress_identity_mode='static',identity_valid_until=$3,last_identity_verified_at=$2
			WHERE id=$1
		`, proxyID, now, now.Add(time.Hour)); err != nil {
			t.Fatalf("configure proxy %d identity: %v", proxyID, err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE proxies SET country_code='US' WHERE id=$1`, wrongCountryID); err != nil {
		t.Fatalf("configure wrong-country proxy: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE proxies SET country_verified_at=$2 WHERE id=$1`, staleGeoID, now.Add(-3*time.Hour)); err != nil {
		t.Fatalf("configure stale-geo proxy: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE proxies SET egress_identity_mode='rotating_per_connect' WHERE id=$1`, rotatingID); err != nil {
		t.Fatalf("configure rotating proxy: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE proxies SET identity_valid_until=$2 WHERE id=$1`, expiringID, now.Add(5*time.Minute)); err != nil {
		t.Fatalf("configure expiring proxy: %v", err)
	}

	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile policy assignments: %v", err)
	}

	var assigned []int
	rows, err := pool.Query(ctx, `
		SELECT proxy_id FROM proxy_running_slots
		WHERE role='channel' AND proxy_id IS NOT NULL ORDER BY slot_no
	`)
	if err != nil {
		t.Fatalf("load policy assignments: %v", err)
	}
	for rows.Next() {
		var proxyID int
		if err := rows.Scan(&proxyID); err != nil {
			rows.Close()
			t.Fatalf("scan policy assignment: %v", err)
		}
		assigned = append(assigned, proxyID)
	}
	rows.Close()
	if len(assigned) != 2 || assigned[0] != eligibleID {
		t.Fatalf("assigned proxies = %v, want preferred proxy %d first and a healthy fallback", assigned, eligibleID)
	}

	capacity, err := manager.Capacity(ctx)
	if err != nil {
		t.Fatalf("load policy capacity: %v", err)
	}
	channel := capacity.Roles[RoleChannel]
	if capacity.WorkloadScope != "qy-test" || capacity.CatalogVersion != 7 ||
		capacity.CatalogDigest != "sha256:test-catalog-v7" || capacity.Active != 5 ||
		channel.IdentityPolicyID != "qy-test-channel-v1" || channel.IdentityPolicyVersion != 1 ||
		channel.IdentityPolicyHash != "sha256:test-channel-v1" || channel.Eligible != 5 ||
		channel.Desired != 2 || channel.Provisioned != 2 || channel.Assigned != 2 ||
		channel.Ready != 2 || channel.Claimed != 0 || channel.Reserve != 3 {
		t.Fatalf("policy capacity = %+v, channel = %+v", capacity, channel)
	}

	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-policy-capacity-1", "worker-policy-capacity-1", "instance-policy-capacity-1",
	))
	if err != nil || !claim.Ready || claim.ProxyID == nil || *claim.ProxyID != eligibleID {
		t.Fatalf("eligible claim = %+v, err = %v", claim, err)
	}
	second, err := manager.Claim(ctx, testClaimRequest(
		"claim-policy-capacity-2", "worker-policy-capacity-2", "instance-policy-capacity-2",
	))
	if err != nil || !second.Ready || second.ProxyID == nil || *second.ProxyID == eligibleID {
		t.Fatalf("healthy fallback claim = %+v, err = %v", second, err)
	}
}

func TestPolicyPreferencesNeverExcludeALegacyHealthyProxy(t *testing.T) {
	manager, pool := newProxyControlPostgresWithOptions(t, func(options *Options) {
		options.ChannelSlots = 1
		options.IdentityPolicies["qy-test-channel-v1"] = IdentityPolicy{
			ID: "qy-test-channel-v1", Version: 1, Hash: "sha256:test-channel-v1",
			Role: RoleChannel, RequiredEgressCountry: "BR",
			AllowedProxyTags:   []string{"role:channel"},
			GeoFreshnessWindow: 2 * time.Hour, AttemptSafetyWindow: 10 * time.Minute,
		}
	})
	ctx := context.Background()
	proxyID := insertControlProxy(t, pool, "legacy-healthy.example:8080", 10)

	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile legacy healthy proxy: %v", err)
	}

	capacity, err := manager.Capacity(ctx)
	if err != nil {
		t.Fatalf("load capacity: %v", err)
	}
	channel := capacity.Roles[RoleChannel]
	if capacity.Active != 1 || channel.Eligible != 1 || channel.Assigned != 1 {
		t.Fatalf("legacy healthy proxy was hard-filtered: capacity=%+v channel=%+v", capacity, channel)
	}

	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-legacy-healthy", "worker-legacy-healthy", "instance-legacy-healthy",
	))
	if err != nil || !claim.Ready || claim.ProxyID == nil || *claim.ProxyID != proxyID {
		t.Fatalf("legacy healthy proxy claim = %+v, err = %v", claim, err)
	}
}

func TestPostgresProxyControlFullFlow(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	firstID := insertControlProxy(t, pool, "first.example:8080", 10)
	secondID := insertControlProxy(t, pool, "second.example:8080", 20)

	var (
		invalidatedMu sync.Mutex
		invalidated   []string
	)
	manager.SetCacheInvalidator(func(username string) {
		invalidatedMu.Lock()
		invalidated = append(invalidated, username)
		invalidatedMu.Unlock()
	})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}

	claim, err := manager.Claim(ctx, testClaimRequest("claim-worker-1", "worker-1", "instance-1"))
	if err != nil {
		t.Fatalf("claim proxy: %v", err)
	}
	if !claim.Ready || claim.ProxyID == nil || *claim.ProxyID != firstID || claim.LeaseID == "" {
		t.Fatalf("claim = %+v", claim)
	}
	if claim.ProxyUser == claim.SlotName || !strings.HasPrefix(claim.ProxyUser, claim.SlotName+"-g") {
		t.Fatalf("claim proxy user = %q, slot = %q", claim.ProxyUser, claim.SlotName)
	}

	// Periodic resource convergence must preserve the active lease generation.
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("resync managed resources: %v", err)
	}
	var proxyUserAfterSync string
	if err := pool.QueryRow(ctx, `
		SELECT u.username
		FROM proxy_running_slots s JOIN proxy_users u ON u.id=s.user_id
		WHERE s.slot_name=$1
	`, claim.SlotName).Scan(&proxyUserAfterSync); err != nil {
		t.Fatalf("read proxy user after sync: %v", err)
	}
	if proxyUserAfterSync != claim.ProxyUser {
		t.Fatalf("resource sync changed leased user from %q to %q", claim.ProxyUser, proxyUserAfterSync)
	}

	renewRequest := RenewRequest{
		RenewRequestID:       "renew-worker-1-initial",
		SlotName:             claim.SlotName,
		WorkerID:             claim.WorkerID,
		WorkerInstanceID:     claim.WorkerInstanceID,
		LeaseID:              claim.LeaseID,
		KnownRouteGeneration: claim.AssignmentVersion,
	}
	renewed, err := manager.Renew(ctx, renewRequest)
	if err != nil || renewed.ProxyUser != claim.ProxyUser || !renewed.Ready {
		t.Fatalf("renewed = %+v, err = %v", renewed, err)
	}

	success, err := manager.Report(ctx, ReportRequest{
		Outcome:           "success",
		ProxyID:           claim.ProxyID,
		ProxyUser:         claim.ProxyUser,
		LeaseID:           claim.LeaseID,
		AssignmentVersion: &claim.AssignmentVersion,
		SampleCount:       2,
		DurationMS:        120,
	})
	if err != nil || success.Action != "performance_recorded" {
		t.Fatalf("success report = %+v, err = %v", success, err)
	}

	status := 429
	failureRequest := ReportRequest{
		Outcome:           "failure",
		ProxyID:           claim.ProxyID,
		ProxyUser:         claim.ProxyUser,
		LeaseID:           claim.LeaseID,
		AssignmentVersion: &claim.AssignmentVersion,
		IncidentID:        "channel:42:attempt:1",
		Status:            &status,
		ErrorType:         "ip_blocked_or_rate_limited",
		Sample:            "rate limited",
	}
	failure, err := manager.Report(ctx, failureRequest)
	if err != nil || failure.Action != "cooldown" || !failure.Confirmed {
		t.Fatalf("failure report = %+v, err = %v", failure, err)
	}
	duplicate, err := manager.Report(ctx, failureRequest)
	if err != nil || duplicate.Action != "duplicate_incident" {
		t.Fatalf("duplicate report = %+v, err = %v", duplicate, err)
	}

	dataPlane := &idleRouteDataPlaneStub{}
	if err := manager.SetDataPlaneController(dataPlane); err != nil {
		t.Fatalf("initialize data plane: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile unhealthy idle Route: %v", err)
	}
	swapped, err := manager.loadAssignment(ctx, pool, claim.SlotName)
	if err != nil {
		t.Fatalf("load replaced Route: %v", err)
	}
	if !swapped.Ready || swapped.ProxyID == nil || *swapped.ProxyID != secondID ||
		swapped.AssignmentVersion != claim.AssignmentVersion+1 || swapped.ProxyUser == claim.ProxyUser {
		t.Fatalf("swapped = %+v", swapped)
	}
	renewRequest.RenewRequestID = "renew-worker-1-after-swap"
	routeDiscovery, err := manager.Renew(ctx, renewRequest)
	if err != nil || !routeDiscovery.RouteChanged ||
		routeDiscovery.AssignmentVersion != swapped.AssignmentVersion {
		t.Fatalf("route discovery = %+v, err = %v", routeDiscovery, err)
	}

	capacity, err := manager.Capacity(ctx)
	if err != nil {
		t.Fatalf("load capacity: %v", err)
	}
	if capacity.Running != 1 || capacity.Reserve != 0 ||
		capacity.Roles[RoleChannel].Desired != 1 ||
		capacity.Roles[RoleChannel].Provisioned != 1 ||
		capacity.Roles[RoleChannel].Assigned != 1 ||
		capacity.Roles[RoleChannel].Ready != 1 ||
		capacity.Roles[RoleChannel].Claimed != 1 {
		t.Fatalf("capacity = %+v", capacity)
	}

	released, err := manager.Release(ctx, ReleaseRequest{
		ReleaseRequestID:     "release-worker-1",
		SlotName:             swapped.SlotName,
		WorkerID:             swapped.WorkerID,
		WorkerInstanceID:     swapped.WorkerInstanceID,
		LeaseID:              swapped.LeaseID,
		KnownRouteGeneration: swapped.AssignmentVersion,
		Reason:               "worker_shutdown",
	})
	if err != nil || !released.Released {
		t.Fatalf("release = %+v, err = %v", released, err)
	}
	assertProxyUsernameGone(t, pool, swapped.ProxyUser)

	workerTwo, err := manager.Claim(ctx, testClaimRequest("claim-worker-2", "worker-2", "instance-2"))
	if err != nil || !workerTwo.Ready {
		t.Fatalf("worker two claim = %+v, err = %v", workerTwo, err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxy_running_slots SET lease_until=NOW()-interval '1 second'
		WHERE worker_id=$1
	`, workerTwo.WorkerID); err != nil {
		t.Fatalf("expire worker two lease: %v", err)
	}
	workerThree, err := manager.Claim(ctx, testClaimRequest("claim-worker-3", "worker-3", "instance-3"))
	if err != nil || !workerThree.Ready || workerThree.ProxyUser == workerTwo.ProxyUser {
		t.Fatalf("worker three claim = %+v, err = %v", workerThree, err)
	}
	assertProxyUsernameGone(t, pool, workerTwo.ProxyUser)
	if _, err := manager.Renew(ctx, RenewRequest{
		RenewRequestID:       "renew-expired-worker-2",
		SlotName:             workerTwo.SlotName,
		WorkerID:             workerTwo.WorkerID,
		WorkerInstanceID:     workerTwo.WorkerInstanceID,
		LeaseID:              workerTwo.LeaseID,
		KnownRouteGeneration: workerTwo.AssignmentVersion,
	}); !errors.Is(err, ErrLeaseGone) {
		t.Fatalf("expired worker renew error = %v, want lease gone", err)
	}

	invalidatedMu.Lock()
	defer invalidatedMu.Unlock()
	if len(invalidated) == 0 {
		t.Fatal("no proxy user cache invalidations were emitted")
	}
}

func TestExecutionLockedRouteCannotChangeDuringReconcile(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	firstProxyID := insertControlProxy(t, pool, "legacy-swap-active.example:8080", 10)
	_ = insertControlProxy(t, pool, "legacy-swap-reserve.example:8080", 20)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignment: %v", err)
	}
	lease, err := manager.Claim(ctx, testClaimRequest(
		"claim-legacy-swap-active", "worker-legacy-swap-active", "instance-legacy-swap-active",
	))
	if err != nil {
		t.Fatalf("claim initial Route: %v", err)
	}
	task, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName: lease.SlotName, WorkerID: lease.WorkerID,
		WorkerInstanceID: lease.WorkerInstanceID, LeaseID: lease.LeaseID,
		RouteGeneration:  lease.AssignmentVersion,
		AttemptRequestID: "attempt-legacy-swap-active",
		BusinessRunID:    "business-legacy-swap-active",
		JobExecutionID:   "youtube-channel-crawl:legacy-swap-active:1:1",
		TaskKind:         TaskKindChannelFull,
	})
	if err != nil {
		t.Fatalf("begin active Task: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE proxies
		SET status='failed',base_health_status='failed',youtube_health_status='not_run',
		    health_generation=health_generation+1,updated_at=NOW()
		WHERE id=$1
	`, firstProxyID); err != nil {
		t.Fatalf("mark active Route unhealthy: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile Execution-Locked Slot: %v", err)
	}

	var persistedProxyID int
	var persistedGeneration int64
	var persistedTaskID string
	if err := pool.QueryRow(ctx, `
		SELECT proxy_id,assignment_version,active_task_id
		FROM proxy_running_slots WHERE slot_name=$1
	`, lease.SlotName).Scan(
		&persistedProxyID, &persistedGeneration, &persistedTaskID,
	); err != nil {
		t.Fatalf("load Execution-Locked Slot after legacy Swap: %v", err)
	}
	if persistedProxyID != firstProxyID || persistedGeneration != lease.AssignmentVersion ||
		persistedTaskID != task.TaskID {
		t.Fatalf(
			"reconciliation changed Execution-Locked Slot: proxy=%d generation=%d task=%q",
			persistedProxyID, persistedGeneration, persistedTaskID,
		)
	}
}

func newProxyControlPostgres(t *testing.T) (*Manager, *pgxpool.Pool) {
	return newProxyControlPostgresWithOptions(t, nil)
}

func newProxyControlPostgresWithOptions(
	t *testing.T,
	configure func(*Options),
) (*Manager, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("ROTA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("ROTA_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL admin pool: %v", err)
	}
	schema := fmt.Sprintf("rota_proxy_control_test_%d", time.Now().UnixNano())
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		admin.Close()
		t.Fatalf("create test schema: %v", err)
	}

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		admin.Close()
		t.Fatalf("parse PostgreSQL config: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+quotedSchema+" CASCADE")
		admin.Close()
		t.Fatalf("open schema-scoped pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
		admin.Close()
	})

	if _, err := pool.Exec(ctx, proxyControlTestSchema); err != nil {
		t.Fatalf("create proxy control schema: %v", err)
	}
	options := Options{
		Enabled:              true,
		WorkloadScope:        "qy-test",
		WorkerPassword:       "worker-password-secret",
		ChannelSlots:         1,
		LeaseDuration:        time.Minute,
		ReconcileInterval:    time.Second,
		ResourceSyncInterval: time.Minute,
		MinReservePercent:    25,
		MinReserveCount:      1,
		FailureCooldown:      30 * time.Minute,
		NetworkCooldown:      5 * time.Minute,
		IdentityPolicies: map[string]IdentityPolicy{
			"qy-test-channel-v1": {
				ID: "qy-test-channel-v1", Version: 1, Hash: "sha256:test-channel-v1",
				Role: RoleChannel,
			},
		},
	}
	if configure != nil {
		configure(&options)
	}
	manager := New(
		&database.DB{Pool: pool},
		nil,
		nil,
		options,
		nil,
	)
	dataPlane := &committedRouteDataPlane{
		manager:     manager,
		activations: make(map[string]testRouteActivation),
	}
	if err := manager.SetDataPlaneController(dataPlane); err != nil {
		t.Fatalf("install default test data plane: %v", err)
	}
	return manager, pool
}

func testClaimRequest(requestID, workerID, instanceID string) ClaimRequest {
	return ClaimRequest{
		ClaimRequestID:        requestID,
		ProtocolVersion:       ProtocolVersionV2,
		Role:                  RoleChannel,
		WorkerID:              workerID,
		WorkerInstanceID:      instanceID,
		IdentityPolicyID:      "qy-test-channel-v1",
		IdentityPolicyVersion: 1,
	}
}

func insertControlProxy(t *testing.T, pool *pgxpool.Pool, address string, responseTime int) int {
	t.Helper()
	var id int
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO proxies (
		  address, protocol, status, base_health_status, youtube_health_status,
		  last_youtube_status, last_rota_youtube_status, avg_response_time
		) VALUES ($1,'http','active','passed','passed',200,200,$2)
		RETURNING id
	`, address, responseTime).Scan(&id); err != nil {
		t.Fatalf("insert control proxy: %v", err)
	}
	return id
}

func assertProxyUsernameGone(t *testing.T, pool *pgxpool.Pool, username string) {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(context.Background(), `
		SELECT EXISTS (SELECT 1 FROM proxy_users WHERE username=$1)
	`, username).Scan(&exists); err != nil {
		t.Fatalf("check proxy username %q: %v", username, err)
	}
	if exists {
		t.Fatalf("stale proxy username %q still exists", username)
	}
}

const proxyControlTestSchema = `
CREATE TABLE proxies (
  id SERIAL PRIMARY KEY,
  address TEXT NOT NULL,
  protocol TEXT NOT NULL DEFAULT 'http',
  status TEXT NOT NULL DEFAULT 'idle',
  tags TEXT[] NOT NULL DEFAULT '{}',
  avg_response_time INTEGER NOT NULL DEFAULT 0,
  base_health_status TEXT,
  youtube_health_status TEXT,
  last_youtube_status INTEGER,
  last_youtube_error TEXT,
  last_youtube_check TIMESTAMPTZ,
  last_rota_youtube_status INTEGER,
  last_rota_youtube_error TEXT,
  last_rota_youtube_check TIMESTAMPTZ,
  cooldown_until TIMESTAMPTZ,
  next_health_check_at TIMESTAMPTZ,
	health_check_not_before TIMESTAMPTZ,
	revalidation_required BOOLEAN NOT NULL DEFAULT false,
	health_generation BIGINT NOT NULL DEFAULT 0,
	failed_since TIMESTAMPTZ,
	continuous_failed_since TIMESTAMPTZ,
	failure_episode_kind TEXT,
	last_health_check_at TIMESTAMPTZ,
	last_health_success_at TIMESTAMPTZ,
	last_health_verdict JSONB,
	archived_at TIMESTAMPTZ,
	archive_reason TEXT,
	last_check TIMESTAMPTZ,
	last_error TEXT,
  youtube_successful_requests BIGINT NOT NULL DEFAULT 0,
  youtube_failed_requests BIGINT NOT NULL DEFAULT 0,
  youtube_avg_response_time INTEGER,
  youtube_avg_detail_time INTEGER,
  youtube_failure_score DOUBLE PRECISION NOT NULL DEFAULT 0,
  last_youtube_success TIMESTAMPTZ,
  last_youtube_failure TIMESTAMPTZ,
	country_code TEXT,
	country_verified_at TIMESTAMPTZ,
	egress_identity_mode TEXT NOT NULL DEFAULT 'static',
	sticky_session_key_encrypted BYTEA,
	identity_valid_until TIMESTAMPTZ,
	network_identity_key TEXT NOT NULL DEFAULT ('net-' || gen_random_uuid()::text),
	last_identity_verified_at TIMESTAMPTZ,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE proxy_pools (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  rotation_method TEXT NOT NULL DEFAULT 'roundrobin',
  stick_count INTEGER NOT NULL DEFAULT 1,
  health_check_url TEXT NOT NULL,
  health_check_cron TEXT NOT NULL,
  health_check_enabled BOOLEAN NOT NULL DEFAULT false,
  auto_sync BOOLEAN NOT NULL DEFAULT false,
  sync_mode TEXT NOT NULL DEFAULT 'manual',
  enabled BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE proxy_users (
  id SERIAL PRIMARY KEY,
  username TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT true,
  main_pool_id INTEGER REFERENCES proxy_pools(id) ON DELETE SET NULL,
  fallback_pool_ids INTEGER[] NOT NULL DEFAULT '{}',
  max_retries INTEGER NOT NULL DEFAULT 1,
  requests_per_minute INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE pool_proxies (
  pool_id INTEGER NOT NULL REFERENCES proxy_pools(id) ON DELETE CASCADE,
  proxy_id INTEGER NOT NULL REFERENCES proxies(id) ON DELETE CASCADE,
  added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (pool_id, proxy_id)
);
CREATE TABLE proxy_running_slots (
  slot_name TEXT PRIMARY KEY,
  role TEXT NOT NULL,
  slot_no INTEGER NOT NULL,
  pool_id INTEGER NOT NULL REFERENCES proxy_pools(id) ON DELETE CASCADE,
  user_id INTEGER NOT NULL REFERENCES proxy_users(id) ON DELETE CASCADE,
  proxy_id INTEGER REFERENCES proxies(id) ON DELETE SET NULL,
  assignment_version BIGINT NOT NULL DEFAULT 0,
  credential_generation BIGINT NOT NULL DEFAULT 0,
  assigned_at TIMESTAMPTZ,
  ready_after TIMESTAMPTZ,
  worker_id TEXT,
  lease_id TEXT,
  lease_until TIMESTAMPTZ,
  last_heartbeat_at TIMESTAMPTZ,
	worker_instance_id TEXT,
	current_lease_id TEXT,
	identity_policy_id TEXT,
	identity_policy_version INTEGER,
	identity_policy_hash TEXT,
	required_egress_country TEXT,
	network_identity_key TEXT,
	profile_epoch BIGINT NOT NULL DEFAULT 0,
	active_task_id TEXT,
	active_task_started_at TIMESTAMPTZ,
	pending_action TEXT,
	pending_incident_id TEXT,
	control_state TEXT NOT NULL DEFAULT 'unleased',
	rotation_deadline_at TIMESTAMPTZ,
	route_activation_old_username TEXT,
	route_activation_claim_id TEXT,
	route_activation_claim_until TIMESTAMPTZ,
	route_activation_previous_claim_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (role, slot_no)
);
CREATE UNIQUE INDEX proxy_running_slots_proxy_unique
  ON proxy_running_slots(proxy_id) WHERE proxy_id IS NOT NULL;
CREATE UNIQUE INDEX proxy_running_slots_worker_unique
	ON proxy_running_slots(worker_id) WHERE worker_id IS NOT NULL;
CREATE TABLE proxy_health_checks (
	id BIGSERIAL PRIMARY KEY,
	proxy_id INTEGER NOT NULL REFERENCES proxies(id) ON DELETE CASCADE,
	started_at TIMESTAMPTZ NOT NULL,
	checked_at TIMESTAMPTZ NOT NULL,
	base_result JSONB NOT NULL,
	youtube_result JSONB NOT NULL,
	verdict TEXT NOT NULL,
	conclusive BOOLEAN NOT NULL,
	control_path_healthy BOOLEAN NOT NULL,
	previous_status TEXT NOT NULL,
	resulting_status TEXT NOT NULL,
	applied BOOLEAN NOT NULL DEFAULT true,
	transition_preserved BOOLEAN NOT NULL DEFAULT false,
	error TEXT
);
CREATE TABLE proxy_lifecycle_events (
	id BIGSERIAL PRIMARY KEY,
	proxy_id INTEGER NOT NULL REFERENCES proxies(id) ON DELETE CASCADE,
	health_check_id BIGINT UNIQUE REFERENCES proxy_health_checks(id) ON DELETE SET NULL,
	occurred_at TIMESTAMPTZ NOT NULL,
	event_kind TEXT NOT NULL,
	previous_status TEXT NOT NULL,
	resulting_status TEXT NOT NULL,
	reason TEXT,
	details JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE TABLE proxy_control_reports (
  id BIGSERIAL PRIMARY KEY,
  incident_id TEXT UNIQUE,
  proxy_id INTEGER REFERENCES proxies(id) ON DELETE SET NULL,
  proxy_user TEXT,
  outcome TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  result JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);
CREATE TABLE proxy_control_leases (
  lease_id TEXT PRIMARY KEY,
  workload_scope TEXT NOT NULL,
  slot_name TEXT NOT NULL REFERENCES proxy_running_slots(slot_name),
  role TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  worker_instance_id TEXT NOT NULL,
  identity_policy_id TEXT NOT NULL,
  identity_policy_version INTEGER NOT NULL,
  identity_policy_hash TEXT NOT NULL,
  status TEXT NOT NULL,
  claim_request_id TEXT NOT NULL,
  claim_request_hash TEXT NOT NULL,
  last_renew_sequence BIGINT NOT NULL DEFAULT 0,
  lease_until TIMESTAMPTZ NOT NULL,
  released_at TIMESTAMPTZ,
  release_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (workload_scope, claim_request_id)
);
CREATE UNIQUE INDEX proxy_control_leases_one_active_slot
  ON proxy_control_leases(workload_scope, slot_name) WHERE status='active';
CREATE UNIQUE INDEX proxy_control_leases_one_active_worker_instance
  ON proxy_control_leases(workload_scope, worker_id, worker_instance_id) WHERE status='active';
CREATE TABLE proxy_control_command_receipts (
  workload_scope TEXT NOT NULL,
  command_kind TEXT NOT NULL,
  request_id TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  resource_kind TEXT NOT NULL,
  resource_id TEXT NOT NULL,
  result_kind TEXT NOT NULL,
  sanitized_result JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  retain_until TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (workload_scope, command_kind, request_id)
);
CREATE TABLE proxy_control_business_runs (
  workload_scope TEXT NOT NULL,
  business_run_id TEXT NOT NULL,
  next_attempt_number INTEGER NOT NULL DEFAULT 1,
  retry_policy_id TEXT NOT NULL,
  retry_policy_version INTEGER NOT NULL,
  max_route_switches_per_execution INTEGER NOT NULL,
  max_network_attempts_per_business_run INTEGER NOT NULL,
  budget_exhausted_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (workload_scope, business_run_id)
);
CREATE TABLE proxy_control_tasks (
  task_id TEXT PRIMARY KEY,
  attempt_request_id TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  workload_scope TEXT NOT NULL,
  business_run_id TEXT NOT NULL,
  job_execution_id TEXT NOT NULL,
  attempt_number INTEGER NOT NULL,
  slot_name TEXT NOT NULL REFERENCES proxy_running_slots(slot_name),
  worker_id TEXT NOT NULL,
	worker_instance_id TEXT NOT NULL DEFAULT 'legacy',
  lease_id TEXT NOT NULL,
  route_generation BIGINT NOT NULL,
  task_kind TEXT NOT NULL,
	identity_policy_id TEXT NOT NULL DEFAULT 'legacy',
	identity_policy_version INTEGER NOT NULL DEFAULT 1,
	identity_policy_hash TEXT NOT NULL DEFAULT 'legacy',
  status TEXT NOT NULL,
  outcome TEXT,
	failed_stage TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
	completion_request_id TEXT,
	completion_request_hash TEXT,
	completion_result JSONB,
  UNIQUE (workload_scope, attempt_request_id),
	UNIQUE (workload_scope, business_run_id, attempt_number),
	UNIQUE (workload_scope, completion_request_id)
);
CREATE INDEX proxy_control_tasks_job_execution
  ON proxy_control_tasks(workload_scope, job_execution_id, started_at);
CREATE UNIQUE INDEX proxy_control_tasks_one_active_slot
  ON proxy_control_tasks(slot_name) WHERE status='active';
CREATE UNIQUE INDEX proxy_control_tasks_one_active_business_run
  ON proxy_control_tasks(workload_scope, business_run_id) WHERE status='active';
CREATE TABLE proxy_control_observations (
  observation_id TEXT NOT NULL,
  workload_scope TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  task_id TEXT NOT NULL REFERENCES proxy_control_tasks(task_id),
  slot_name TEXT NOT NULL REFERENCES proxy_running_slots(slot_name),
  worker_id TEXT NOT NULL,
  worker_instance_id TEXT NOT NULL,
  lease_id TEXT NOT NULL,
  route_generation BIGINT NOT NULL,
  business_run_id TEXT NOT NULL,
  network_identity_key TEXT NOT NULL,
  kind TEXT NOT NULL,
  source TEXT NOT NULL,
  http_status INTEGER,
  occurred_at TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  action TEXT NOT NULL DEFAULT 'none',
  incident_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (workload_scope, observation_id),
  UNIQUE (task_id, observation_id)
);
CREATE TABLE proxy_control_incident_observations (
  workload_scope TEXT NOT NULL,
  incident_id TEXT NOT NULL,
  observation_id TEXT NOT NULL,
  PRIMARY KEY (workload_scope,incident_id,observation_id),
  UNIQUE (workload_scope,observation_id)
);
CREATE TABLE proxy_identity_profile_epochs (
  identity_policy_id TEXT NOT NULL,
  network_identity_key TEXT NOT NULL,
  profile_epoch BIGINT NOT NULL,
  status TEXT NOT NULL,
  retired_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (identity_policy_id,network_identity_key)
);
`
