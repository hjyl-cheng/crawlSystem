package proxycontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (m *Manager) CompleteTask(ctx context.Context, request CompleteTaskRequest) (CompleteTaskResult, error) {
	if err := m.requireEnabled(); err != nil {
		return CompleteTaskResult{}, err
	}
	request = normalizeCompleteTaskRequest(request)
	if err := validateCompleteTaskRequest(request); err != nil {
		return CompleteTaskResult{}, err
	}
	requestHash, err := completeTaskRequestHash(request)
	if err != nil {
		return CompleteTaskResult{}, err
	}

	tx, err := m.db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return CompleteTaskResult{}, fmt.Errorf("begin proxy task completion: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, controlAdvisoryLock); err != nil {
		return CompleteTaskResult{}, fmt.Errorf("lock proxy task completion: %w", err)
	}

	replayed, found, err := loadCompletionByRequestID(
		ctx, tx, m.options.WorkloadScope, request.CompletionRequestID,
	)
	if err != nil {
		return CompleteTaskResult{}, err
	}
	if found {
		if replayed.requestHash != requestHash {
			return CompleteTaskResult{}, fmt.Errorf(
				"%w: completion_request_id %q", ErrIdempotencyConflict, request.CompletionRequestID,
			)
		}
		if err := tx.Commit(ctx); err != nil {
			return CompleteTaskResult{}, fmt.Errorf("commit replayed proxy task completion: %w", err)
		}
		return replayed.CompleteTaskResult, nil
	}

	state, err := lockCompletionTask(ctx, tx, m.options.WorkloadScope, request.TaskID)
	if err != nil {
		return CompleteTaskResult{}, err
	}
	if state.completionRequestID != "" {
		return CompleteTaskResult{}, ErrCompletionConflict
	}
	if state.status != "active" {
		return CompleteTaskResult{}, ErrTaskCompleted
	}
	if state.slotName != request.SlotName || state.workerID != request.WorkerID ||
		state.workerInstanceID != request.WorkerInstanceID || state.leaseID != request.LeaseID ||
		state.routeGeneration != request.RouteGeneration || state.businessRunID != request.BusinessRunID ||
		state.activeTaskID != request.TaskID {
		return CompleteTaskResult{}, ErrTaskConflict
	}
	if state.currentLeaseID != request.LeaseID || !state.leaseUntil.After(time.Now()) {
		return CompleteTaskResult{}, ErrLeaseGone
	}
	if state.slotRouteGeneration != request.RouteGeneration {
		return CompleteTaskResult{}, ErrLeaseConflict
	}
	if err := validateCompletionObservations(
		ctx, tx, m.options.WorkloadScope, request, state.pendingAction,
	); err != nil {
		return CompleteTaskResult{}, err
	}

	result := CompleteTaskResult{
		OK: true, TaskCompleted: true, CompletionRequestID: request.CompletionRequestID,
		TaskID: request.TaskID, SlotName: request.SlotName, LeaseID: request.LeaseID,
		ControlState: CompletionReadyKeepRoute, Ready: true,
		CompletedTaskRouteGeneration: request.RouteGeneration,
	}
	var credentialRotations []credentialRotation
	var replacementProxyID *int
	if state.pendingAction != "" && state.pendingAction != PendingActionNone {
		policy, err := m.policyForCompletion(state)
		if err != nil {
			return CompleteTaskResult{}, err
		}
		selected, err := selectCompletionWarmStandby(ctx, tx, policy, state)
		if err != nil {
			return CompleteTaskResult{}, err
		}
		if err := retireCompletionSourceProfile(ctx, tx, policy, state); err != nil {
			return CompleteTaskResult{}, err
		}
		newGeneration := request.RouteGeneration + 1
		rotation, err := rotateSlotCredential(ctx, tx, request.SlotName)
		if err != nil {
			return CompleteTaskResult{}, err
		}
		credentialRotations = append(credentialRotations, rotation)
		if selected == nil {
			if err := pauseCompletionWithoutReserve(
				ctx, tx, state, request, newGeneration,
			); err != nil {
				return CompleteTaskResult{}, err
			}
			result.ControlState = CompletionPausedNoReserve
			result.Ready = false
			result.PendingRouteGeneration = &newGeneration
			result.PendingIdentityAction = state.pendingAction
			result.RetryAfterMS = 1000
			result.ReasonCode = "NO_POLICY_ELIGIBLE_RESERVE"
		} else {
			selectedID := selected.ID
			replacementProxyID = &selectedID
			profileEpoch, err := bindCompletionWarmStandby(ctx, tx, policy, state, *selected)
			if err != nil {
				return CompleteTaskResult{}, err
			}
			tag, err := tx.Exec(ctx, `
					UPDATE proxy_running_slots
					SET active_task_id=NULL,active_task_started_at=NULL,
					    proxy_id=$2,assignment_version=$3,assigned_at=NOW(),ready_after=NULL,
					    network_identity_key=$4,profile_epoch=$5,
					    control_state='pending_new_route',rotation_deadline_at=NULL,
					    route_activation_old_username=NULLIF($9,''),
					    route_activation_claim_id=NULL,route_activation_claim_until=NULL,
					    route_activation_previous_claim_id=NULL,
					    updated_at=NOW()
					WHERE slot_name=$1 AND active_task_id=$6 AND current_lease_id=$7
					  AND assignment_version=$8
				`, request.SlotName, selected.ID, newGeneration, selected.NetworkIdentityKey,
				profileEpoch, request.TaskID, request.LeaseID, request.RouteGeneration,
				rotation.OldUsername)
			if err != nil {
				return CompleteTaskResult{}, fmt.Errorf("bind completed task replacement slot: %w", err)
			}
			if tag.RowsAffected() != 1 {
				return CompleteTaskResult{}, ErrTaskConflict
			}
			result.ControlState = CompletionPendingNewRoute
			result.Ready = false
			result.PendingRouteGeneration = &newGeneration
			result.PendingIdentityAction = state.pendingAction
			result.RetryAfterMS = 250
			result.ReasonCode = "WAITING_FOR_ROUTE_REFRESH"
		}
	}
	encodedResult, err := json.Marshal(result)
	if err != nil {
		return CompleteTaskResult{}, fmt.Errorf("encode proxy task completion: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE proxy_control_tasks
		SET status='completed',outcome=$2,completed_at=NOW(),
		    completion_request_id=$3,completion_request_hash=$4,completion_result=$5::jsonb
		WHERE task_id=$1 AND workload_scope=$6 AND status='active' AND completion_request_id IS NULL
	`, request.TaskID, request.Outcome, request.CompletionRequestID, requestHash,
		string(encodedResult), m.options.WorkloadScope)
	if err != nil {
		return CompleteTaskResult{}, fmt.Errorf("complete proxy task: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return CompleteTaskResult{}, ErrCompletionConflict
	}
	if state.pendingAction == "" || state.pendingAction == PendingActionNone {
		tag, err = tx.Exec(ctx, `
			UPDATE proxy_running_slots
			SET active_task_id=NULL,active_task_started_at=NULL,
			    pending_action=NULL,pending_incident_id=NULL,
			    control_state='leased_idle',rotation_deadline_at=NULL,updated_at=NOW()
			WHERE slot_name=$1 AND active_task_id=$2 AND current_lease_id=$3
			  AND assignment_version=$4
		`, request.SlotName, request.TaskID, request.LeaseID, request.RouteGeneration)
		if err != nil {
			return CompleteTaskResult{}, fmt.Errorf("release completed proxy task slot: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return CompleteTaskResult{}, ErrTaskConflict
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return CompleteTaskResult{}, fmt.Errorf("commit proxy task completion: %w", err)
	}
	if result.ControlState == CompletionPendingNewRoute && len(credentialRotations) == 1 &&
		replacementProxyID != nil {
		m.activatePendingRoute(ctx, routeActivationFence{
			SlotName:        request.SlotName,
			LeaseID:         request.LeaseID,
			ProxyID:         *replacementProxyID,
			RouteGeneration: *result.PendingRouteGeneration,
		})
	} else {
		m.invalidateCredentials(credentialRotations)
	}
	if (state.pendingAction == "" || state.pendingAction == PendingActionNone) && !state.routeEligible {
		m.requestReconcile()
	}
	return result, nil
}

type completionTaskState struct {
	status              string
	completionRequestID string
	slotName            string
	workerID            string
	workerInstanceID    string
	leaseID             string
	routeGeneration     int64
	businessRunID       string
	activeTaskID        string
	currentLeaseID      string
	leaseUntil          time.Time
	slotRouteGeneration int64
	pendingAction       string
	role                string
	poolID              int
	proxyID             *int
	routeEligible       bool
	networkIdentityKey  string
	identityPolicyID    string
	identityPolicyVer   int
	identityPolicyHash  string
}

func lockCompletionTask(
	ctx context.Context,
	tx pgx.Tx,
	workloadScope string,
	taskID string,
) (completionTaskState, error) {
	var state completionTaskState
	err := tx.QueryRow(ctx, `
		SELECT t.status,COALESCE(t.completion_request_id,''),t.slot_name,t.worker_id,
		       t.worker_instance_id,t.lease_id,t.route_generation,t.business_run_id,
		       COALESCE(s.active_task_id,''),COALESCE(s.current_lease_id,''),s.lease_until,
		       s.assignment_version,COALESCE(s.pending_action,''),s.role,s.pool_id,s.proxy_id,
		       COALESCE(
		         p.status='active' AND p.revalidation_required=false
		         AND (p.cooldown_until IS NULL OR p.cooldown_until <= NOW())
		         AND (
		           (p.base_health_status='passed' AND p.youtube_health_status='passed')
		           OR (p.last_youtube_status=200 AND p.last_rota_youtube_status=200)
		         ),
		         false
		       ),
		       COALESCE(s.network_identity_key,''),t.identity_policy_id,
		       t.identity_policy_version,t.identity_policy_hash
		FROM proxy_control_tasks t
		JOIN proxy_running_slots s ON s.slot_name=t.slot_name
		LEFT JOIN proxies p ON p.id=s.proxy_id
		WHERE t.workload_scope=$1 AND t.task_id=$2
		FOR UPDATE OF t,s
	`, workloadScope, taskID).Scan(
		&state.status, &state.completionRequestID, &state.slotName, &state.workerID,
		&state.workerInstanceID, &state.leaseID, &state.routeGeneration,
		&state.businessRunID, &state.activeTaskID, &state.currentLeaseID,
		&state.leaseUntil, &state.slotRouteGeneration, &state.pendingAction,
		&state.role, &state.poolID, &state.proxyID, &state.routeEligible, &state.networkIdentityKey,
		&state.identityPolicyID, &state.identityPolicyVer, &state.identityPolicyHash,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return completionTaskState{}, ErrTaskConflict
	}
	if err != nil {
		return completionTaskState{}, fmt.Errorf("lock proxy completion task: %w", err)
	}
	return state, nil
}

type storedCompletion struct {
	CompleteTaskResult
	requestHash string
}

func loadCompletionByRequestID(
	ctx context.Context,
	tx pgx.Tx,
	workloadScope string,
	completionRequestID string,
) (storedCompletion, bool, error) {
	var requestHash string
	var encoded []byte
	err := tx.QueryRow(ctx, `
		SELECT completion_request_hash,completion_result
		FROM proxy_control_tasks
		WHERE workload_scope=$1 AND completion_request_id=$2
		FOR UPDATE
	`, workloadScope, completionRequestID).Scan(&requestHash, &encoded)
	if errors.Is(err, pgx.ErrNoRows) {
		return storedCompletion{}, false, nil
	}
	if err != nil {
		return storedCompletion{}, false, fmt.Errorf("load proxy task completion: %w", err)
	}
	var result CompleteTaskResult
	if err := json.Unmarshal(encoded, &result); err != nil {
		return storedCompletion{}, false, fmt.Errorf("decode proxy task completion: %w", err)
	}
	return storedCompletion{CompleteTaskResult: result, requestHash: requestHash}, true, nil
}

func validateCompletionObservations(
	ctx context.Context,
	tx pgx.Tx,
	workloadScope string,
	request CompleteTaskRequest,
	requiredAction string,
) error {
	if len(request.ObservationIDs) == 0 {
		if requiredAction != "" && requiredAction != PendingActionNone {
			return ErrObservationReference
		}
		return nil
	}
	var count, matchingActionCount int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*),COUNT(*) FILTER (WHERE action=$10)
		FROM proxy_control_observations
		WHERE workload_scope=$1 AND observation_id=ANY($2::text[])
		  AND task_id=$3 AND slot_name=$4 AND worker_id=$5 AND worker_instance_id=$6
		  AND lease_id=$7 AND route_generation=$8 AND business_run_id=$9
	`, workloadScope, request.ObservationIDs, request.TaskID, request.SlotName,
		request.WorkerID, request.WorkerInstanceID, request.LeaseID,
		request.RouteGeneration, request.BusinessRunID, requiredAction).Scan(
		&count, &matchingActionCount,
	); err != nil {
		return fmt.Errorf("validate proxy completion observations: %w", err)
	}
	if count != len(request.ObservationIDs) {
		return ErrObservationReference
	}
	if requiredAction != "" && requiredAction != PendingActionNone && matchingActionCount == 0 {
		return ErrObservationReference
	}
	return nil
}

func (m *Manager) policyForCompletion(state completionTaskState) (IdentityPolicy, error) {
	policy, found := m.options.IdentityPolicies[state.identityPolicyID]
	policy.ID = strings.TrimSpace(policy.ID)
	policy.Role = strings.ToLower(strings.TrimSpace(policy.Role))
	policy.Hash = strings.TrimSpace(policy.Hash)
	policy.RequiredEgressCountry = strings.ToUpper(strings.TrimSpace(policy.RequiredEgressCountry))
	policy.AllowedProxyTags = append([]string(nil), policy.AllowedProxyTags...)
	for index := range policy.AllowedProxyTags {
		policy.AllowedProxyTags[index] = strings.ToLower(strings.TrimSpace(policy.AllowedProxyTags[index]))
	}
	if !found || policy.ID != state.identityPolicyID || policy.Version != state.identityPolicyVer ||
		policy.Hash != state.identityPolicyHash || policy.Role != state.role {
		return IdentityPolicy{}, ErrPolicyRejected
	}
	return policy, nil
}

func selectCompletionWarmStandby(
	ctx context.Context,
	tx pgx.Tx,
	policy IdentityPolicy,
	state completionTaskState,
) (*candidate, error) {
	candidates, err := loadEligibleCandidates(ctx, tx, policy)
	if err != nil {
		return nil, err
	}
	assigned, err := assignedProxyIDs(ctx, tx)
	if err != nil {
		return nil, err
	}
	availableRoles, err := availableSlotRoles(ctx, tx)
	if err != nil {
		return nil, err
	}
	var exactRole, generic *candidate
	for index := range candidates {
		item := &candidates[index]
		if (state.proxyID != nil && item.ID == *state.proxyID) || assigned[item.ID] {
			continue
		}
		role := pinnedRole(*item, availableRoles)
		switch {
		case role == state.role && exactRole == nil:
			exactRole = item
		case role == "" && generic == nil:
			generic = item
		}
	}
	if exactRole != nil {
		return exactRole, nil
	}
	return generic, nil
}

func bindCompletionWarmStandby(
	ctx context.Context,
	tx pgx.Tx,
	policy IdentityPolicy,
	state completionTaskState,
	selected candidate,
) (int64, error) {
	if _, err := tx.Exec(ctx, `DELETE FROM pool_proxies WHERE pool_id=$1`, state.poolID); err != nil {
		return 0, fmt.Errorf("clear completed task proxy pool: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO pool_proxies (pool_id,proxy_id) VALUES ($1,$2)
	`, state.poolID, selected.ID); err != nil {
		return 0, fmt.Errorf("reserve completed task warm standby: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE proxy_pools SET updated_at=NOW() WHERE id=$1`, state.poolID); err != nil {
		return 0, fmt.Errorf("touch completed task proxy pool: %w", err)
	}

	initialEpoch := int64(0)
	if state.pendingAction == PendingActionRotateProfile {
		initialEpoch = 1
	}
	var profileEpoch int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO proxy_identity_profile_epochs (
		  identity_policy_id,network_identity_key,profile_epoch,status
		) VALUES ($1,$2,$3,'active')
		ON CONFLICT (identity_policy_id,network_identity_key) DO UPDATE
		SET profile_epoch=CASE WHEN $4 THEN proxy_identity_profile_epochs.profile_epoch+1
		                       ELSE proxy_identity_profile_epochs.profile_epoch END,
		    status='active',retired_at=NULL,updated_at=NOW()
		RETURNING profile_epoch
	`, policy.ID, selected.NetworkIdentityKey, initialEpoch,
		state.pendingAction == PendingActionRotateProfile).Scan(&profileEpoch); err != nil {
		return 0, fmt.Errorf("prepare replacement identity profile: %w", err)
	}
	return profileEpoch, nil
}

func retireCompletionSourceProfile(
	ctx context.Context,
	tx pgx.Tx,
	policy IdentityPolicy,
	state completionTaskState,
) error {
	if state.pendingAction != PendingActionRotateProfile || state.networkIdentityKey == "" {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE proxy_identity_profile_epochs
		SET status='retired',retired_at=COALESCE(retired_at,NOW()),updated_at=NOW()
		WHERE identity_policy_id=$1 AND network_identity_key=$2
	`, policy.ID, state.networkIdentityKey); err != nil {
		return fmt.Errorf("retire challenged identity profile: %w", err)
	}
	return nil
}

func pauseCompletionWithoutReserve(
	ctx context.Context,
	tx pgx.Tx,
	state completionTaskState,
	request CompleteTaskRequest,
	newGeneration int64,
) error {
	if _, err := tx.Exec(ctx, `DELETE FROM pool_proxies WHERE pool_id=$1`, state.poolID); err != nil {
		return fmt.Errorf("clear paused proxy pool: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE proxy_pools SET updated_at=NOW() WHERE id=$1`, state.poolID); err != nil {
		return fmt.Errorf("touch paused proxy pool: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE proxy_running_slots
		SET active_task_id=NULL,active_task_started_at=NULL,
		    proxy_id=NULL,assignment_version=$2,assigned_at=NULL,ready_after=NULL,
		    network_identity_key=NULL,profile_epoch=0,
		    control_state='paused_no_reserve',rotation_deadline_at=NULL,
		    route_activation_old_username=NULL,route_activation_claim_id=NULL,
		    route_activation_claim_until=NULL,route_activation_previous_claim_id=NULL,
		    updated_at=NOW()
		WHERE slot_name=$1 AND active_task_id=$3 AND current_lease_id=$4
		  AND assignment_version=$5
	`, request.SlotName, newGeneration, request.TaskID, request.LeaseID, request.RouteGeneration)
	if err != nil {
		return fmt.Errorf("pause completed task without reserve: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrTaskConflict
	}
	return nil
}

func normalizeCompleteTaskRequest(request CompleteTaskRequest) CompleteTaskRequest {
	request.CompletionRequestID = strings.TrimSpace(request.CompletionRequestID)
	request.SlotName = strings.TrimSpace(request.SlotName)
	request.WorkerID = strings.TrimSpace(request.WorkerID)
	request.WorkerInstanceID = strings.TrimSpace(request.WorkerInstanceID)
	request.LeaseID = strings.TrimSpace(request.LeaseID)
	request.TaskID = strings.TrimSpace(request.TaskID)
	request.BusinessRunID = strings.TrimSpace(request.BusinessRunID)
	request.Outcome = strings.ToLower(strings.TrimSpace(request.Outcome))
	request.ObservationIDs = append([]string(nil), request.ObservationIDs...)
	for index := range request.ObservationIDs {
		request.ObservationIDs[index] = strings.TrimSpace(request.ObservationIDs[index])
	}
	slices.Sort(request.ObservationIDs)
	request.ObservationIDs = slices.Compact(request.ObservationIDs)
	if request.ObservationIDs == nil {
		request.ObservationIDs = []string{}
	}
	return request
}

func validateCompleteTaskRequest(request CompleteTaskRequest) error {
	for _, value := range []string{
		request.CompletionRequestID, request.SlotName, request.WorkerID,
		request.WorkerInstanceID, request.LeaseID, request.TaskID, request.BusinessRunID,
	} {
		if value == "" || len(value) > 255 {
			return fmt.Errorf("%w: completion identity fields are required and limited to 255 bytes", ErrInvalidInput)
		}
	}
	if request.RouteGeneration < 0 || request.DurationMS < 0 || request.ActiveManagedRequests < 0 {
		return fmt.Errorf("%w: completion counters must be non-negative", ErrInvalidInput)
	}
	if request.DurationMS > int64((7*24*time.Hour)/time.Millisecond) {
		return fmt.Errorf("%w: duration_ms exceeds the completion limit", ErrInvalidInput)
	}
	if request.Outcome != TaskOutcomeSuccess && request.Outcome != TaskOutcomeFailed &&
		request.Outcome != TaskOutcomeCancelled {
		return fmt.Errorf("%w: unsupported task outcome %q", ErrInvalidInput, request.Outcome)
	}
	for _, observationID := range request.ObservationIDs {
		if observationID == "" || len(observationID) > 255 {
			return fmt.Errorf("%w: observation_ids must be 1-255 bytes", ErrInvalidInput)
		}
	}
	if !request.AttemptQuiesced || request.ActiveManagedRequests != 0 {
		return ErrAttemptNotQuiesced
	}
	return nil
}

func completeTaskRequestHash(request CompleteTaskRequest) (string, error) {
	payload, err := json.Marshal(struct {
		SchemaVersion         int      `json:"schema_version"`
		SlotName              string   `json:"slot_name"`
		WorkerID              string   `json:"worker_id"`
		WorkerInstanceID      string   `json:"worker_instance_id"`
		LeaseID               string   `json:"lease_id"`
		RouteGeneration       int64    `json:"route_generation"`
		TaskID                string   `json:"task_id"`
		BusinessRunID         string   `json:"business_run_id"`
		Outcome               string   `json:"outcome"`
		DurationMS            int64    `json:"duration_ms"`
		BusinessComplete      bool     `json:"business_complete"`
		ObservationIDs        []string `json:"observation_ids"`
		AttemptQuiesced       bool     `json:"attempt_quiesced"`
		ActiveManagedRequests int      `json:"active_managed_requests"`
	}{
		SchemaVersion: 1, SlotName: request.SlotName, WorkerID: request.WorkerID,
		WorkerInstanceID: request.WorkerInstanceID, LeaseID: request.LeaseID,
		RouteGeneration: request.RouteGeneration, TaskID: request.TaskID,
		BusinessRunID: request.BusinessRunID, Outcome: request.Outcome,
		DurationMS: request.DurationMS, BusinessComplete: request.BusinessComplete,
		ObservationIDs: request.ObservationIDs, AttemptQuiesced: request.AttemptQuiesced,
		ActiveManagedRequests: request.ActiveManagedRequests,
	})
	if err != nil {
		return "", fmt.Errorf("encode proxy task completion request: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}
