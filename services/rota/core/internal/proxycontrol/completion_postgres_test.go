package proxycontrol

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type completionDataPlaneStub struct {
	oldUsername     string
	newUsername     string
	expectedProxy   int
	activationErr   error
	activationCalls int
}

func (*completionDataPlaneStub) RefreshProxyUser(string) {}

func (*completionDataPlaneStub) RetireProxyUser(context.Context, string) error { return nil }

func (*completionDataPlaneStub) RequireRouteActivationRegistry() {}

func (*completionDataPlaneStub) RebuildRouteActivationRegistry(
	context.Context,
	[]RouteActivationRegistryEntry,
) error {
	return nil
}

func (s *completionDataPlaneStub) BeginProxyUserActivation(
	_ context.Context,
	oldUsername string,
	newUsername string,
	expectedProxyID int,
	_ string,
	_ string,
) (RouteActivationBeginResult, error) {
	s.activationCalls++
	s.oldUsername = oldUsername
	s.newUsername = newUsername
	s.expectedProxy = expectedProxyID
	return RouteActivationBeginResult{}, s.activationErr
}

func (*completionDataPlaneStub) CommitProxyUserActivation(context.Context, string, string) error {
	return nil
}

func (*completionDataPlaneStub) RetireProxyUserIfClaim(
	context.Context,
	string,
	string,
) (bool, error) {
	return false, nil
}

func TestCompleteTaskKeepsHealthyRouteAndIsIdempotent(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	proxyID := insertControlProxy(t, pool, "complete-keep.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-complete-keep", "worker-complete-keep", "instance-complete-keep",
	))
	if err != nil {
		t.Fatalf("claim proxy: %v", err)
	}
	task, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration:  claim.AssignmentVersion,
		AttemptRequestID: "attempt-complete-keep",
		BusinessRunID:    "business-complete-keep",
		JobExecutionID:   "youtube-channel-crawl:complete-keep:1",
		TaskKind:         TaskKindChannelFull,
	})
	if err != nil {
		t.Fatalf("begin task: %v", err)
	}
	observation, err := manager.Observe(ctx, ObserveRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration: claim.AssignmentVersion,
		TaskID:          task.TaskID, BusinessRunID: task.BusinessRunID,
		ObservationID: "observation-complete-keep",
		Kind:          ObservationSuccess, Source: "channel_pipeline",
		OccurredAt: time.Date(2026, 8, 13, 12, 5, 0, 0, time.UTC),
		Payload:    map[string]any{"stage": "complete"},
	})
	if err != nil {
		t.Fatalf("observe success: %v", err)
	}

	request := CompleteTaskRequest{
		CompletionRequestID: "completion-keep-1",
		SlotName:            claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration: claim.AssignmentVersion,
		TaskID:          task.TaskID, BusinessRunID: task.BusinessRunID,
		Outcome: TaskOutcomeSuccess, DurationMS: 1250, BusinessComplete: true,
		ObservationIDs:  []string{observation.ObservationID, observation.ObservationID},
		AttemptQuiesced: true, ActiveManagedRequests: 0,
	}
	notQuiesced := request
	notQuiesced.CompletionRequestID = "completion-not-quiesced"
	notQuiesced.AttemptQuiesced = false
	if _, err := manager.CompleteTask(ctx, notQuiesced); !errors.Is(err, ErrAttemptNotQuiesced) {
		t.Fatalf("not-quiesced completion error = %v, want attempt not quiesced", err)
	}
	stillActive := request
	stillActive.CompletionRequestID = "completion-active-request"
	stillActive.ActiveManagedRequests = 1
	if _, err := manager.CompleteTask(ctx, stillActive); !errors.Is(err, ErrAttemptNotQuiesced) {
		t.Fatalf("active-request completion error = %v, want attempt not quiesced", err)
	}

	first, err := manager.CompleteTask(ctx, request)
	if err != nil {
		t.Fatalf("complete healthy task: %v", err)
	}
	replayed, err := manager.CompleteTask(ctx, request)
	if err != nil {
		t.Fatalf("replay healthy completion: %v", err)
	}
	if first != replayed {
		t.Fatalf("replayed completion = %+v, want %+v", replayed, first)
	}
	if !first.OK || !first.TaskCompleted || first.TaskID != task.TaskID ||
		first.CompletionRequestID != request.CompletionRequestID ||
		first.ControlState != CompletionReadyKeepRoute || !first.Ready ||
		first.CompletedTaskRouteGeneration != claim.AssignmentVersion ||
		first.PendingRouteGeneration != nil || first.PendingIdentityAction != "" {
		t.Fatalf("healthy completion = %+v", first)
	}

	changed := request
	changed.DurationMS++
	if _, err := manager.CompleteTask(ctx, changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed completion error = %v, want idempotency conflict", err)
	}
	differentIntent := request
	differentIntent.CompletionRequestID = "completion-keep-other"
	if _, err := manager.CompleteTask(ctx, differentIntent); !errors.Is(err, ErrCompletionConflict) {
		t.Fatalf("second completion intent error = %v, want completion conflict", err)
	}

	var (
		status              string
		outcome             string
		completionRequestID string
		activeTaskID        string
		controlState        string
		currentProxyID      int
		routeGeneration     int64
		observationCount    int
	)
	if err := pool.QueryRow(ctx, `
		SELECT status,outcome,completion_request_id FROM proxy_control_tasks WHERE task_id=$1
	`, task.TaskID).Scan(&status, &outcome, &completionRequestID); err != nil {
		t.Fatalf("load completed task: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(active_task_id,''),control_state,proxy_id,assignment_version
		FROM proxy_running_slots WHERE slot_name=$1
	`, claim.SlotName).Scan(
		&activeTaskID, &controlState, &currentProxyID, &routeGeneration,
	); err != nil {
		t.Fatalf("load completed slot: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM proxy_control_observations WHERE task_id=$1
	`, task.TaskID).Scan(&observationCount); err != nil {
		t.Fatalf("count completion observations: %v", err)
	}
	if status != "completed" || outcome != TaskOutcomeSuccess ||
		completionRequestID != request.CompletionRequestID || activeTaskID != "" ||
		controlState != "leased_idle" || currentProxyID != proxyID ||
		routeGeneration != claim.AssignmentVersion || observationCount != 1 {
		t.Fatalf(
			"task=%q/%q/%q active=%q state=%q proxy=%d generation=%d observations=%d",
			status, outcome, completionRequestID, activeTaskID, controlState,
			currentProxyID, routeGeneration, observationCount,
		)
	}
}

func TestCompleteTaskReservesOnlyPolicyEligibleWarmStandbyAfterChallenge(t *testing.T) {
	manager, pool := newProxyControlPostgresWithOptions(t, func(options *Options) {
		options.IdentityPolicies["qy-test-channel-v1"] = IdentityPolicy{
			ID: "qy-test-channel-v1", Version: 1, Hash: "sha256:test-channel-v1",
			Role: RoleChannel, RequiredEgressCountry: "BR",
			AllowedProxyTags:   []string{"residential"},
			GeoFreshnessWindow: 2 * time.Hour, AttemptSafetyWindow: 10 * time.Minute,
		}
	})
	ctx := context.Background()

	currentID := insertControlProxy(t, pool, "complete-current-br.example:8080", 10)
	usID := insertControlProxy(t, pool, "complete-us.example:8080", 20)
	staleGeoID := insertControlProxy(t, pool, "complete-stale-geo.example:8080", 30)
	rotatingID := insertControlProxy(t, pool, "complete-rotating.example:8080", 40)
	warmStandbyID := insertControlProxy(t, pool, "complete-warm-br.example:8080", 50)
	now := time.Now()
	if _, err := pool.Exec(ctx, `
		UPDATE proxies
		SET tags=ARRAY['residential'],country_code=$2,country_verified_at=$3,
		    egress_identity_mode=$4,identity_valid_until=$5,last_identity_verified_at=$3
		WHERE id=$1
	`, currentID, "BR", now, "static", now.Add(time.Hour)); err != nil {
		t.Fatalf("configure current proxy identity: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxies
		SET tags=ARRAY['residential'],country_code='US',country_verified_at=$2,
		    egress_identity_mode='static',identity_valid_until=$3,last_identity_verified_at=$2
		WHERE id=$1
	`, usID, now, now.Add(time.Hour)); err != nil {
		t.Fatalf("configure US proxy identity: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxies
		SET tags=ARRAY['residential'],country_code='BR',country_verified_at=$2,
		    egress_identity_mode='static',identity_valid_until=$3,last_identity_verified_at=$2
		WHERE id=$1
	`, staleGeoID, now.Add(-3*time.Hour), now.Add(time.Hour)); err != nil {
		t.Fatalf("configure stale-geo proxy identity: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxies
		SET tags=ARRAY['residential'],country_code='BR',country_verified_at=$2,
		    egress_identity_mode='rotating_per_connect',identity_valid_until=$3,
		    last_identity_verified_at=$2
		WHERE id=$1
	`, rotatingID, now, now.Add(time.Hour)); err != nil {
		t.Fatalf("configure rotating proxy identity: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE proxies
		SET tags=ARRAY['residential'],country_code='BR',country_verified_at=$2,
		    egress_identity_mode='static',identity_valid_until=$3,last_identity_verified_at=$2
		WHERE id=$1
	`, warmStandbyID, now, now.Add(time.Hour)); err != nil {
		t.Fatalf("configure warm standby identity: %v", err)
	}

	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-complete-challenge", "worker-complete-challenge", "instance-complete-challenge",
	))
	if err != nil {
		t.Fatalf("claim proxy: %v", err)
	}
	if claim.ProxyID == nil || *claim.ProxyID != currentID {
		t.Fatalf("claimed proxy = %+v, want current ID %d", claim, currentID)
	}
	task, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration:  claim.AssignmentVersion,
		AttemptRequestID: "attempt-complete-challenge",
		BusinessRunID:    "business-complete-challenge",
		JobExecutionID:   "youtube-channel-crawl:complete-challenge:1",
		TaskKind:         TaskKindChannelFull,
	})
	if err != nil {
		t.Fatalf("begin task: %v", err)
	}
	observation, err := manager.Observe(ctx, ObserveRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration: claim.AssignmentVersion,
		TaskID:          task.TaskID, BusinessRunID: task.BusinessRunID,
		ObservationID: "observation-complete-challenge",
		Kind:          ObservationYouTubeChallenge, Source: "youtubejs_player",
		HTTPStatus: intPointer(200),
		OccurredAt: time.Date(2026, 8, 13, 12, 6, 0, 0, time.UTC),
		Payload:    map[string]any{"challenge": "bot_check"},
	})
	if err != nil {
		t.Fatalf("observe challenge: %v", err)
	}

	request := CompleteTaskRequest{
		CompletionRequestID: "completion-challenge-1",
		SlotName:            claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration: claim.AssignmentVersion,
		TaskID:          task.TaskID, BusinessRunID: task.BusinessRunID,
		Outcome: TaskOutcomeFailed, DurationMS: 2500, BusinessComplete: false,
		AttemptQuiesced: true, ActiveManagedRequests: 0,
	}
	withoutEvidence := request
	withoutEvidence.CompletionRequestID = "completion-challenge-without-evidence"
	if _, err := manager.CompleteTask(ctx, withoutEvidence); !errors.Is(err, ErrObservationReference) {
		t.Fatalf("completion without evidence error = %v, want observation reference", err)
	}
	request.ObservationIDs = []string{observation.ObservationID}
	result, err := manager.CompleteTask(ctx, request)
	if err != nil {
		t.Fatalf("complete challenge task: %v", err)
	}
	if !result.OK || !result.TaskCompleted || result.ControlState != CompletionPendingNewRoute ||
		result.Ready || result.CompletedTaskRouteGeneration != claim.AssignmentVersion ||
		result.PendingRouteGeneration == nil || *result.PendingRouteGeneration != claim.AssignmentVersion+1 ||
		result.PendingIdentityAction != PendingActionRotateProfile {
		t.Fatalf("challenge completion = %+v", result)
	}

	var (
		activeTaskID       string
		controlState       string
		pendingAction      string
		newProxyID         int
		routeGeneration    int64
		readyAfter         *time.Time
		profileEpoch       int64
		poolMemberCount    int
		selectedPoolMember int
	)
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(active_task_id,''),control_state,COALESCE(pending_action,''),
		       proxy_id,assignment_version,ready_after,profile_epoch
		FROM proxy_running_slots WHERE slot_name=$1
	`, claim.SlotName).Scan(
		&activeTaskID, &controlState, &pendingAction, &newProxyID,
		&routeGeneration, &readyAfter, &profileEpoch,
	); err != nil {
		t.Fatalf("load pending replacement slot: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*),MIN(proxy_id)
		FROM pool_proxies
		WHERE pool_id=(SELECT pool_id FROM proxy_running_slots WHERE slot_name=$1)
	`, claim.SlotName).Scan(&poolMemberCount, &selectedPoolMember); err != nil {
		t.Fatalf("load replacement pool membership: %v", err)
	}
	if activeTaskID != "" || controlState != "pending_new_route" ||
		pendingAction != PendingActionRotateProfile || newProxyID != warmStandbyID ||
		routeGeneration != claim.AssignmentVersion+1 || readyAfter != nil ||
		profileEpoch < 1 || poolMemberCount != 1 || selectedPoolMember != warmStandbyID {
		t.Fatalf(
			"active=%q state=%q action=%q proxy=%d generation=%d ready=%v epoch=%d members=%d/%d",
			activeTaskID, controlState, pendingAction, newProxyID, routeGeneration,
			readyAfter, profileEpoch, poolMemberCount, selectedPoolMember,
		)
	}
	assertProxyQuarantinedAfterSlotReplacement(t, pool, currentID, "youtube_unusable")
}

func TestCompleteTaskPausesAndRevokesFailedRouteWhenNoWarmStandbyExists(t *testing.T) {
	manager, pool := newProxyControlPostgresWithOptions(t, func(options *Options) {
		options.IdentityPolicies["qy-test-channel-v1"] = IdentityPolicy{
			ID: "qy-test-channel-v1", Version: 1, Hash: "sha256:test-channel-v1",
			Role: RoleChannel, RequiredEgressCountry: "BR",
			AllowedProxyTags:   []string{"residential"},
			GeoFreshnessWindow: 2 * time.Hour, AttemptSafetyWindow: 10 * time.Minute,
		}
	})
	ctx := context.Background()

	currentID := insertControlProxy(t, pool, "complete-no-reserve.example:8080", 10)
	now := time.Now()
	if _, err := pool.Exec(ctx, `
		UPDATE proxies
		SET tags=ARRAY['residential'],country_code='BR',country_verified_at=$2,
		    egress_identity_mode='static',identity_valid_until=$3,last_identity_verified_at=$2
		WHERE id=$1
	`, currentID, now, now.Add(time.Hour)); err != nil {
		t.Fatalf("configure current proxy identity: %v", err)
	}

	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignment: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-complete-no-reserve", "worker-complete-no-reserve", "instance-complete-no-reserve",
	))
	if err != nil {
		t.Fatalf("claim proxy: %v", err)
	}
	task, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration:  claim.AssignmentVersion,
		AttemptRequestID: "attempt-complete-no-reserve",
		BusinessRunID:    "business-complete-no-reserve",
		JobExecutionID:   "youtube-channel-crawl:complete-no-reserve:1",
		TaskKind:         TaskKindChannelFull,
	})
	if err != nil {
		t.Fatalf("begin task: %v", err)
	}
	observation, err := manager.Observe(ctx, ObserveRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration: claim.AssignmentVersion,
		TaskID:          task.TaskID, BusinessRunID: task.BusinessRunID,
		ObservationID: "observation-complete-no-reserve",
		Kind:          ObservationYouTubeChallenge, Source: "youtubejs_player",
		OccurredAt: time.Date(2026, 8, 13, 12, 7, 0, 0, time.UTC),
		Payload:    map[string]any{"challenge": "bot_check"},
	})
	if err != nil {
		t.Fatalf("observe challenge: %v", err)
	}

	result, err := manager.CompleteTask(ctx, CompleteTaskRequest{
		CompletionRequestID:   "completion-no-reserve-1",
		SlotName:              claim.SlotName,
		WorkerID:              claim.WorkerID,
		WorkerInstanceID:      claim.WorkerInstanceID,
		LeaseID:               claim.LeaseID,
		RouteGeneration:       claim.AssignmentVersion,
		TaskID:                task.TaskID,
		BusinessRunID:         task.BusinessRunID,
		Outcome:               TaskOutcomeFailed,
		DurationMS:            1900,
		BusinessComplete:      false,
		ObservationIDs:        []string{observation.ObservationID},
		AttemptQuiesced:       true,
		ActiveManagedRequests: 0,
	})
	if err != nil {
		t.Fatalf("complete task without reserve: %v", err)
	}
	if !result.OK || !result.TaskCompleted || result.ControlState != CompletionPausedNoReserve ||
		result.Ready || result.CompletedTaskRouteGeneration != claim.AssignmentVersion ||
		result.PendingRouteGeneration == nil || *result.PendingRouteGeneration != claim.AssignmentVersion+1 ||
		result.PendingIdentityAction != PendingActionRotateProfile ||
		result.ReasonCode != "NO_POLICY_ELIGIBLE_RESERVE" {
		t.Fatalf("no-reserve completion = %+v", result)
	}

	var (
		status               string
		activeTaskID         string
		controlState         string
		pendingAction        string
		proxyID              *int
		networkIdentityKey   *string
		routeGeneration      int64
		credentialGeneration int64
		readyAfter           *time.Time
		poolMemberCount      int
		sourceProfileStatus  string
	)
	if err := pool.QueryRow(ctx, `
		SELECT status FROM proxy_control_tasks WHERE task_id=$1
	`, task.TaskID).Scan(&status); err != nil {
		t.Fatalf("load completed task: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(active_task_id,''),control_state,COALESCE(pending_action,''),
		       proxy_id,network_identity_key,assignment_version,credential_generation,ready_after
		FROM proxy_running_slots WHERE slot_name=$1
	`, claim.SlotName).Scan(
		&activeTaskID, &controlState, &pendingAction, &proxyID, &networkIdentityKey,
		&routeGeneration, &credentialGeneration, &readyAfter,
	); err != nil {
		t.Fatalf("load paused slot: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM pool_proxies
		WHERE pool_id=(SELECT pool_id FROM proxy_running_slots WHERE slot_name=$1)
	`, claim.SlotName).Scan(&poolMemberCount); err != nil {
		t.Fatalf("count paused pool members: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT status FROM proxy_identity_profile_epochs
		WHERE identity_policy_id=$1 AND network_identity_key=$2
	`, claim.IdentityPolicyID, claim.NetworkIdentityKey).Scan(&sourceProfileStatus); err != nil {
		t.Fatalf("load retired source profile: %v", err)
	}
	if status != "completed" || activeTaskID != "" || controlState != "paused_no_reserve" ||
		pendingAction != PendingActionRotateProfile || proxyID != nil || networkIdentityKey != nil ||
		routeGeneration != claim.AssignmentVersion+1 ||
		credentialGeneration != claim.CredentialGeneration+1 || readyAfter != nil ||
		poolMemberCount != 0 || sourceProfileStatus != "retired" {
		t.Fatalf(
			"task=%q active=%q state=%q action=%q proxy=%v identity=%v generation=%d credential=%d ready=%v members=%d source_profile=%q",
			status, activeTaskID, controlState, pendingAction, proxyID, networkIdentityKey,
			routeGeneration, credentialGeneration, readyAfter, poolMemberCount, sourceProfileStatus,
		)
	}
	assertProxyQuarantinedAfterSlotReplacement(t, pool, currentID, "youtube_unusable")
}

func TestCompleteTaskRetriesDataPlaneActivationAndDoesNotLeakActionIntoNextTask(t *testing.T) {
	manager, pool := newProxyControlPostgresWithOptions(t, func(options *Options) {
		options.IdentityPolicies["qy-test-channel-v1"] = IdentityPolicy{
			ID: "qy-test-channel-v1", Version: 1, Hash: "sha256:test-channel-v1",
			Role: RoleChannel, RequiredEgressCountry: "BR",
			AllowedProxyTags:   []string{"residential"},
			GeoFreshnessWindow: 2 * time.Hour, AttemptSafetyWindow: 10 * time.Minute,
		}
	})
	ctx := context.Background()
	currentID := insertControlProxy(t, pool, "complete-activate-current.example:8080", 10)
	warmStandbyID := insertControlProxy(t, pool, "complete-activate-warm.example:8080", 20)
	now := time.Now()
	for _, proxyID := range []int{currentID, warmStandbyID} {
		if _, err := pool.Exec(ctx, `
			UPDATE proxies
			SET tags=ARRAY['residential'],country_code='BR',country_verified_at=$2,
			    egress_identity_mode='static',identity_valid_until=$3,last_identity_verified_at=$2
			WHERE id=$1
		`, proxyID, now, now.Add(time.Hour)); err != nil {
			t.Fatalf("configure proxy %d identity: %v", proxyID, err)
		}
	}

	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignment: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-complete-activate", "worker-complete-activate", "instance-complete-activate",
	))
	if err != nil {
		t.Fatalf("claim proxy: %v", err)
	}
	if claim.ProxyID == nil || *claim.ProxyID != currentID {
		t.Fatalf("claimed proxy = %+v, want current ID %d", claim, currentID)
	}
	task, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration:  claim.AssignmentVersion,
		AttemptRequestID: "attempt-complete-activate",
		BusinessRunID:    "business-complete-activate",
		JobExecutionID:   "youtube-channel-crawl:complete-activate:1",
		TaskKind:         TaskKindChannelFull,
	})
	if err != nil {
		t.Fatalf("begin task: %v", err)
	}
	observation, err := manager.Observe(ctx, ObserveRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration: claim.AssignmentVersion,
		TaskID:          task.TaskID, BusinessRunID: task.BusinessRunID,
		ObservationID: "observation-complete-activate",
		Kind:          ObservationYouTubeChallenge, Source: "youtubejs_player",
		OccurredAt: time.Date(2026, 8, 13, 12, 8, 0, 0, time.UTC),
		Payload:    map[string]any{"challenge": "bot_check"},
	})
	if err != nil {
		t.Fatalf("observe challenge: %v", err)
	}

	activationFailure := errors.New("data-plane warmup failed")
	dataPlane := &completionDataPlaneStub{activationErr: activationFailure}
	manager.SetDataPlaneController(dataPlane)
	result, err := manager.CompleteTask(ctx, CompleteTaskRequest{
		CompletionRequestID: "completion-activate-1",
		SlotName:            claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration: claim.AssignmentVersion,
		TaskID:          task.TaskID, BusinessRunID: task.BusinessRunID,
		Outcome: TaskOutcomeFailed, DurationMS: 2100, BusinessComplete: false,
		ObservationIDs:  []string{observation.ObservationID},
		AttemptQuiesced: true, ActiveManagedRequests: 0,
	})
	if err != nil {
		t.Fatalf("complete challenge task: %v", err)
	}
	if result.ControlState != CompletionPendingNewRoute || result.Ready {
		t.Fatalf("completion receipt = %+v", result)
	}

	var (
		controlState  string
		pendingAction string
		proxyID       int
		generation    int64
		readyAfter    *time.Time
		newUsername   string
	)
	if err := pool.QueryRow(ctx, `
		SELECT s.control_state,COALESCE(s.pending_action,''),s.proxy_id,
		       s.assignment_version,s.ready_after,u.username
		FROM proxy_running_slots s JOIN proxy_users u ON u.id=s.user_id
		WHERE s.slot_name=$1
	`, claim.SlotName).Scan(
		&controlState, &pendingAction, &proxyID, &generation, &readyAfter, &newUsername,
	); err != nil {
		t.Fatalf("load activated replacement: %v", err)
	}
	if dataPlane.activationCalls != 1 || dataPlane.oldUsername != claim.ProxyUser ||
		dataPlane.newUsername != newUsername || dataPlane.expectedProxy != warmStandbyID {
		t.Fatalf("data-plane activation = %+v, new username = %q", dataPlane, newUsername)
	}
	if controlState != "pending_new_route" || pendingAction != PendingActionRotateProfile ||
		proxyID != warmStandbyID || generation != claim.AssignmentVersion+1 || readyAfter != nil {
		t.Fatalf(
			"state=%q action=%q proxy=%d generation=%d ready=%v",
			controlState, pendingAction, proxyID, generation, readyAfter,
		)
	}

	dataPlane.activationErr = nil
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile pending replacement: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT s.control_state,COALESCE(s.pending_action,''),s.proxy_id,
		       s.assignment_version,s.ready_after,u.username
		FROM proxy_running_slots s JOIN proxy_users u ON u.id=s.user_id
		WHERE s.slot_name=$1
	`, claim.SlotName).Scan(
		&controlState, &pendingAction, &proxyID, &generation, &readyAfter, &newUsername,
	); err != nil {
		t.Fatalf("load reconciled replacement: %v", err)
	}
	if dataPlane.activationCalls != 2 || dataPlane.oldUsername != claim.ProxyUser ||
		dataPlane.newUsername != newUsername || dataPlane.expectedProxy != warmStandbyID {
		t.Fatalf("retried data-plane activation = %+v, new username = %q", dataPlane, newUsername)
	}
	if controlState != "leased_idle" || pendingAction != PendingActionRotateProfile ||
		proxyID != warmStandbyID || generation != claim.AssignmentVersion+1 || readyAfter == nil {
		t.Fatalf(
			"reconciled state=%q action=%q proxy=%d generation=%d ready=%v",
			controlState, pendingAction, proxyID, generation, readyAfter,
		)
	}

	renewed, err := manager.Renew(ctx, RenewRequest{
		RenewRequestID:       "renew-complete-activate",
		SlotName:             claim.SlotName,
		WorkerID:             claim.WorkerID,
		WorkerInstanceID:     claim.WorkerInstanceID,
		LeaseID:              claim.LeaseID,
		KnownRouteGeneration: claim.AssignmentVersion,
	})
	if err != nil {
		t.Fatalf("renew replacement route: %v", err)
	}
	if !renewed.Ready || !renewed.RouteChanged ||
		renewed.AssignmentVersion != claim.AssignmentVersion+1 ||
		renewed.IdentityAction != PendingActionRotateProfile {
		t.Fatalf("renewed replacement assignment = %+v", renewed)
	}

	secondTask, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName: renewed.SlotName, WorkerID: renewed.WorkerID,
		WorkerInstanceID: renewed.WorkerInstanceID, LeaseID: renewed.LeaseID,
		RouteGeneration:  renewed.AssignmentVersion,
		AttemptRequestID: "attempt-complete-activate-2",
		BusinessRunID:    task.BusinessRunID,
		JobExecutionID:   task.JobExecutionID,
		TaskKind:         TaskKindChannelFull,
	})
	if err != nil {
		t.Fatalf("begin task on replacement route: %v", err)
	}
	secondCompletion, err := manager.CompleteTask(ctx, CompleteTaskRequest{
		CompletionRequestID:   "completion-activate-2",
		SlotName:              renewed.SlotName,
		WorkerID:              renewed.WorkerID,
		WorkerInstanceID:      renewed.WorkerInstanceID,
		LeaseID:               renewed.LeaseID,
		RouteGeneration:       renewed.AssignmentVersion,
		TaskID:                secondTask.TaskID,
		BusinessRunID:         secondTask.BusinessRunID,
		Outcome:               TaskOutcomeSuccess,
		DurationMS:            900,
		BusinessComplete:      true,
		AttemptQuiesced:       true,
		ActiveManagedRequests: 0,
	})
	if err != nil {
		t.Fatalf("complete task on replacement route: %v", err)
	}
	if !secondCompletion.Ready ||
		secondCompletion.ControlState != CompletionReadyKeepRoute {
		t.Fatalf("replacement task completion = %+v", secondCompletion)
	}
	var pendingIncident string
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(pending_action,''),COALESCE(pending_incident_id,'')
		FROM proxy_running_slots WHERE slot_name=$1
	`, claim.SlotName).Scan(&pendingAction, &pendingIncident); err != nil {
		t.Fatalf("load consumed replacement action: %v", err)
	}
	if pendingAction != "" || pendingIncident != "" {
		t.Fatalf("replacement action was not consumed: action=%q incident=%q", pendingAction, pendingIncident)
	}
}

func assertProxyQuarantinedAfterSlotReplacement(
	t *testing.T,
	pool *pgxpool.Pool,
	proxyID int,
	wantKind string,
) {
	t.Helper()
	var (
		status, failureKind                           string
		failedSince, cooldownUntil, nextHealthCheckAt *time.Time
		revalidationRequired                          bool
		eventCount                                    int
	)
	if err := pool.QueryRow(context.Background(), `
		SELECT status,COALESCE(failure_episode_kind,''),failed_since,
		       cooldown_until,next_health_check_at,revalidation_required
		FROM proxies WHERE id=$1
	`, proxyID).Scan(
		&status, &failureKind, &failedSince, &cooldownUntil,
		&nextHealthCheckAt, &revalidationRequired,
	); err != nil {
		t.Fatalf("load replaced proxy lifecycle: %v", err)
	}
	if err := pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM proxy_lifecycle_events
		WHERE proxy_id=$1 AND event_kind='task_observation_quarantine'
	`, proxyID).Scan(&eventCount); err != nil {
		t.Fatalf("count replacement lifecycle events: %v", err)
	}
	if status != "failed" || failureKind != wantKind || failedSince == nil ||
		cooldownUntil == nil || nextHealthCheckAt == nil ||
		!cooldownUntil.Equal(*nextHealthCheckAt) || revalidationRequired || eventCount != 1 {
		t.Fatalf(
			"replaced proxy lifecycle status=%q kind=%q failed=%v cooldown=%v next=%v revalidation=%v events=%d",
			status, failureKind, failedSince, cooldownUntil, nextHealthCheckAt,
			revalidationRequired, eventCount,
		)
	}
}
