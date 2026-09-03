package proxycontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type scanner interface {
	Scan(...any) error
}

func (m *Manager) Claim(ctx context.Context, request ClaimRequest) (Assignment, error) {
	if err := m.requireEnabled(); err != nil {
		return Assignment{}, err
	}
	request = normalizeClaimRequest(request)
	if err := validateClaimRequest(request); err != nil {
		return Assignment{}, err
	}
	policy, err := m.resolveIdentityPolicy(request)
	if err != nil {
		return Assignment{}, err
	}
	requestHash, err := claimRequestHash(request)
	if err != nil {
		return Assignment{}, err
	}
	expiredCredentialRotations, err := m.cleanupExpiredLeases(ctx)
	if err != nil {
		return Assignment{}, err
	}

	tx, err := m.db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Assignment{}, fmt.Errorf("begin proxy claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, controlAdvisoryLock); err != nil {
		return Assignment{}, fmt.Errorf("lock proxy claim: %w", err)
	}

	replayed, found, err := loadClaimLease(ctx, tx, m.options.WorkloadScope, request.ClaimRequestID)
	if err != nil {
		return Assignment{}, err
	}
	if found {
		if replayed.RequestHash != requestHash {
			return Assignment{}, fmt.Errorf("%w: claim_request_id %q", ErrIdempotencyConflict, request.ClaimRequestID)
		}
		if replayed.Status != "active" || !replayed.LeaseUntil.After(time.Now()) {
			return Assignment{}, ErrLeaseGone
		}
		assignment, err := m.loadAssignment(ctx, tx, replayed.SlotName)
		if err != nil {
			return Assignment{}, err
		}
		if assignment.LeaseID != replayed.LeaseID || assignment.WorkerInstanceID != request.WorkerInstanceID {
			return Assignment{}, ErrLeaseGone
		}
		if err := tx.Commit(ctx); err != nil {
			return Assignment{}, fmt.Errorf("commit replayed proxy claim: %w", err)
		}
		if err := m.publishClaimedRoute(ctx, assignment, ""); err != nil {
			return Assignment{}, err
		}
		return assignment, nil
	}

	var liveInstance string
	err = tx.QueryRow(ctx, `
		SELECT worker_instance_id
		FROM proxy_control_leases
		WHERE workload_scope=$1 AND worker_id=$2 AND status='active' AND lease_until > NOW()
		ORDER BY created_at
		LIMIT 1
		FOR UPDATE
	`, m.options.WorkloadScope, request.WorkerID).Scan(&liveInstance)
	if err == nil {
		return Assignment{}, fmt.Errorf("%w: worker %q is already leased by instance %q", ErrLeaseConflict, request.WorkerID, liveInstance)
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Assignment{}, fmt.Errorf("load existing worker lease: %w", err)
	}

	eligible, err := loadEligibleCandidates(ctx, tx, policy, m.options.WorkloadScope)
	if err != nil {
		return Assignment{}, err
	}
	eligibleIDs := make([]int, 0, len(eligible))
	for _, item := range eligible {
		eligibleIDs = append(eligibleIDs, item.ID)
	}
	var slotName, currentProxyUsername string
	err = tx.QueryRow(ctx, `
		SELECT s.slot_name,proxy_user.username
		FROM proxy_running_slots s
		JOIN proxy_users proxy_user ON proxy_user.id=s.user_id
		WHERE s.role=$1 AND s.worker_id IS NULL AND s.current_lease_id IS NULL
		  AND s.proxy_id=ANY($2::int[]) AND s.ready_after <= NOW()
		ORDER BY s.slot_no
		LIMIT 1
		FOR UPDATE OF s SKIP LOCKED
	`, request.Role, eligibleIDs).Scan(&slotName, &currentProxyUsername)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return Assignment{}, fmt.Errorf("commit empty proxy claim: %w", err)
		}
		return Assignment{
			OK:                    true,
			Ready:                 false,
			Reason:                "waiting_for_healthy_proxy",
			ControlState:          "waiting_capacity",
			WorkloadScope:         m.options.WorkloadScope,
			ProtocolVersion:       ProtocolVersionV2,
			Role:                  request.Role,
			WorkerID:              request.WorkerID,
			WorkerInstanceID:      request.WorkerInstanceID,
			IdentityPolicyID:      policy.ID,
			IdentityPolicyVersion: policy.Version,
			IdentityPolicyHash:    policy.Hash,
		}, nil
	}
	if err != nil {
		return Assignment{}, fmt.Errorf("select proxy slot: %w", err)
	}

	leaseID := uuid.NewString()
	var rotation credentialRotation
	for _, expiredRotation := range expiredCredentialRotations {
		if expiredRotation.SlotName == slotName &&
			expiredRotation.NewUsername == currentProxyUsername {
			rotation = expiredRotation
			break
		}
	}
	claimCredentialRotations := make([]credentialRotation, 0, 1)
	if rotation.SlotName == "" {
		rotation, err = rotateSlotCredential(ctx, tx, slotName)
		if err != nil {
			return Assignment{}, err
		}
		claimCredentialRotations = append(claimCredentialRotations, rotation)
	}

	var networkIdentityKey string
	if err := tx.QueryRow(ctx, `
		SELECT p.network_identity_key
		FROM proxy_running_slots s JOIN proxies p ON p.id=s.proxy_id
		WHERE s.slot_name=$1
	`, slotName).Scan(&networkIdentityKey); err != nil {
		return Assignment{}, fmt.Errorf("load claimed network identity: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO proxy_identity_profile_epochs (
		  identity_policy_id, network_identity_key, profile_epoch, status
		) VALUES ($1,$2,0,'active')
		ON CONFLICT (identity_policy_id,network_identity_key) DO UPDATE
		SET status='active', retired_at=NULL, updated_at=NOW()
	`, policy.ID, networkIdentityKey); err != nil {
		return Assignment{}, fmt.Errorf("prepare identity profile epoch: %w", err)
	}

	var leaseUntil time.Time
	err = tx.QueryRow(ctx, `
		UPDATE proxy_running_slots
		SET worker_id=$2, worker_instance_id=$3, lease_id=$4, current_lease_id=$4,
		    lease_until=NOW()+($5 * interval '1 millisecond'), last_heartbeat_at=NOW(),
		    identity_policy_id=$6, identity_policy_version=$7, identity_policy_hash=$8,
		    required_egress_country=NULLIF($9,''), network_identity_key=$10,
		    profile_epoch=(SELECT profile_epoch FROM proxy_identity_profile_epochs
		                   WHERE identity_policy_id=$6 AND network_identity_key=$10),
		    control_state='leased_idle', updated_at=NOW()
		WHERE slot_name=$1
		RETURNING lease_until
	`, slotName, request.WorkerID, request.WorkerInstanceID, leaseID,
		m.options.LeaseDuration.Milliseconds(), policy.ID, policy.Version, policy.Hash,
		policy.RequiredEgressCountry, networkIdentityKey).Scan(&leaseUntil)
	if err != nil {
		return Assignment{}, fmt.Errorf("claim proxy slot: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO proxy_control_leases (
		  lease_id, workload_scope, slot_name, role, worker_id, worker_instance_id,
		  identity_policy_id, identity_policy_version, identity_policy_hash,
		  status, claim_request_id, claim_request_hash, lease_until
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'active',$10,$11,$12)
	`, leaseID, m.options.WorkloadScope, slotName, request.Role, request.WorkerID,
		request.WorkerInstanceID, policy.ID, policy.Version, policy.Hash,
		request.ClaimRequestID, requestHash, leaseUntil); err != nil {
		return Assignment{}, fmt.Errorf("persist proxy lease: %w", err)
	}
	assignment, err := m.loadAssignment(ctx, tx, slotName)
	if err != nil {
		return Assignment{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Assignment{}, fmt.Errorf("commit proxy claim: %w", err)
	}
	m.invalidateCredentials(claimCredentialRotations)
	if err := m.publishClaimedRoute(ctx, assignment, rotation.OldUsername); err != nil {
		return Assignment{}, err
	}
	return assignment, nil
}

const commandReceiptRetention = 30 * 24 * time.Hour

func (m *Manager) Renew(ctx context.Context, request RenewRequest) (Assignment, error) {
	if err := m.requireEnabled(); err != nil {
		return Assignment{}, err
	}
	request = normalizeRenewRequest(request)
	if err := validateRenewRequest(request); err != nil {
		return Assignment{}, err
	}
	requestHash, err := renewRequestHash(request)
	if err != nil {
		return Assignment{}, err
	}
	tx, err := m.db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Assignment{}, fmt.Errorf("begin proxy lease renewal: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, controlAdvisoryLock); err != nil {
		return Assignment{}, fmt.Errorf("lock proxy lease renewal: %w", err)
	}

	receipt, replayed, err := loadCommandReceipt(
		ctx, tx, m.options.WorkloadScope, "renew", request.RenewRequestID,
	)
	if err != nil {
		return Assignment{}, err
	}
	if replayed && receipt.requestHash != requestHash {
		return Assignment{}, fmt.Errorf(
			"%w: renew_request_id %q", ErrIdempotencyConflict, request.RenewRequestID,
		)
	}
	var receiptResult struct {
		RenewSequence int64 `json:"renew_sequence"`
	}
	if replayed {
		if err := json.Unmarshal(receipt.sanitizedResult, &receiptResult); err != nil {
			return Assignment{}, fmt.Errorf("decode proxy renewal receipt: %w", err)
		}
	}

	state, err := lockRenewLease(ctx, tx, m.options.WorkloadScope, request)
	if err != nil {
		return Assignment{}, err
	}
	if request.KnownRouteGeneration > state.routeGeneration {
		return Assignment{}, ErrLeaseConflict
	}

	if !replayed {
		state.renewSequence++
		err = tx.QueryRow(ctx, `
			UPDATE proxy_control_leases
			SET last_renew_sequence=$3,
			    lease_until=NOW()+($4 * interval '1 millisecond'),updated_at=NOW()
			WHERE workload_scope=$1 AND lease_id=$2 AND status='active'
			  AND worker_id=$5 AND worker_instance_id=$6 AND slot_name=$7
			  AND lease_until > NOW()
			RETURNING lease_until
		`, m.options.WorkloadScope, request.LeaseID, state.renewSequence,
			m.options.LeaseDuration.Milliseconds(), request.WorkerID,
			request.WorkerInstanceID, request.SlotName).Scan(&state.leaseUntil)
		if errors.Is(err, pgx.ErrNoRows) {
			return Assignment{}, ErrLeaseGone
		}
		if err != nil {
			return Assignment{}, fmt.Errorf("renew proxy lease history: %w", err)
		}
		tag, err := tx.Exec(ctx, `
			UPDATE proxy_running_slots
			SET lease_until=$6,last_heartbeat_at=NOW(),updated_at=NOW()
			WHERE slot_name=$1 AND worker_id=$2 AND worker_instance_id=$3
			  AND lease_id=$4 AND current_lease_id=$4 AND assignment_version=$5
			  AND lease_until > NOW()
		`, request.SlotName, request.WorkerID, request.WorkerInstanceID,
			request.LeaseID, state.routeGeneration, state.leaseUntil)
		if err != nil {
			return Assignment{}, fmt.Errorf("renew proxy slot lease: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return Assignment{}, ErrLeaseGone
		}

		sanitizedResult, err := json.Marshal(struct {
			RenewSequence   int64 `json:"renew_sequence"`
			RouteGeneration int64 `json:"route_generation"`
		}{
			RenewSequence:   state.renewSequence,
			RouteGeneration: state.routeGeneration,
		})
		if err != nil {
			return Assignment{}, fmt.Errorf("encode proxy renewal receipt: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO proxy_control_command_receipts (
			  workload_scope,command_kind,request_id,request_hash,
			  resource_kind,resource_id,result_kind,sanitized_result,retain_until
			) VALUES ($1,'renew',$2,$3,'lease',$4,'renewed',$5::jsonb,
			          NOW()+($6 * interval '1 millisecond'))
		`, m.options.WorkloadScope, request.RenewRequestID, requestHash,
			request.LeaseID, string(sanitizedResult), commandReceiptRetention.Milliseconds()); err != nil {
			return Assignment{}, fmt.Errorf("persist proxy renewal receipt: %w", err)
		}
	}

	assignment, err := m.loadAssignment(ctx, tx, request.SlotName)
	if err != nil {
		return Assignment{}, err
	}
	assignment.RenewSequence = state.renewSequence
	if replayed {
		assignment.RenewSequence = receiptResult.RenewSequence
	}
	assignment.RouteChanged = assignment.AssignmentVersion > request.KnownRouteGeneration
	if assignment.Ready && assignment.RouteChanged && state.pendingAction != "" &&
		state.pendingAction != PendingActionNone {
		assignment.IdentityAction = state.pendingAction
	}
	if err := tx.Commit(ctx); err != nil {
		return Assignment{}, fmt.Errorf("commit proxy lease renewal: %w", err)
	}
	return assignment, nil
}

type renewLeaseState struct {
	routeGeneration int64
	renewSequence   int64
	pendingAction   string
	leaseUntil      time.Time
}

func lockRenewLease(
	ctx context.Context,
	tx pgx.Tx,
	workloadScope string,
	request RenewRequest,
) (renewLeaseState, error) {
	var state renewLeaseState
	err := tx.QueryRow(ctx, `
		SELECT s.assignment_version,l.last_renew_sequence,
		       COALESCE(s.pending_action,''),l.lease_until
		FROM proxy_control_leases l
		JOIN proxy_running_slots s ON s.slot_name=l.slot_name
		WHERE l.workload_scope=$1 AND l.slot_name=$2 AND l.worker_id=$3
		  AND l.worker_instance_id=$4 AND l.lease_id=$5 AND l.status='active'
		  AND l.lease_until > NOW()
		  AND s.slot_name=l.slot_name AND s.worker_id=l.worker_id
		  AND s.worker_instance_id=l.worker_instance_id
		  AND s.lease_id=l.lease_id AND s.current_lease_id=l.lease_id
		  AND s.lease_until > NOW()
		FOR UPDATE OF l,s
	`, workloadScope, request.SlotName, request.WorkerID,
		request.WorkerInstanceID, request.LeaseID).Scan(
		&state.routeGeneration, &state.renewSequence, &state.pendingAction, &state.leaseUntil,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return renewLeaseState{}, ErrLeaseGone
	}
	if err != nil {
		return renewLeaseState{}, fmt.Errorf("lock proxy renewal lease: %w", err)
	}
	return state, nil
}

type storedCommandReceipt struct {
	requestHash     string
	resourceKind    string
	resourceID      string
	resultKind      string
	sanitizedResult []byte
}

func loadCommandReceipt(
	ctx context.Context,
	tx pgx.Tx,
	workloadScope string,
	commandKind string,
	requestID string,
) (storedCommandReceipt, bool, error) {
	var receipt storedCommandReceipt
	err := tx.QueryRow(ctx, `
		SELECT request_hash,resource_kind,resource_id,result_kind,sanitized_result
		FROM proxy_control_command_receipts
		WHERE workload_scope=$1 AND command_kind=$2 AND request_id=$3
		FOR UPDATE
	`, workloadScope, commandKind, requestID).Scan(
		&receipt.requestHash, &receipt.resourceKind, &receipt.resourceID,
		&receipt.resultKind, &receipt.sanitizedResult,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return storedCommandReceipt{}, false, nil
	}
	if err != nil {
		return storedCommandReceipt{}, false, fmt.Errorf("load proxy control command receipt: %w", err)
	}
	return receipt, true, nil
}

func (m *Manager) Release(ctx context.Context, request ReleaseRequest) (ReleaseResult, error) {
	if err := m.requireEnabled(); err != nil {
		return ReleaseResult{}, err
	}
	request = normalizeReleaseRequest(request)
	if err := validateReleaseRequest(request); err != nil {
		return ReleaseResult{}, err
	}
	requestHash, err := releaseRequestHash(request)
	if err != nil {
		return ReleaseResult{}, err
	}
	tx, err := m.db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ReleaseResult{}, fmt.Errorf("begin proxy lease release: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, controlAdvisoryLock); err != nil {
		return ReleaseResult{}, fmt.Errorf("lock proxy lease release: %w", err)
	}

	receipt, replayed, err := loadCommandReceipt(
		ctx, tx, m.options.WorkloadScope, "release", request.ReleaseRequestID,
	)
	if err != nil {
		return ReleaseResult{}, err
	}
	if replayed {
		if receipt.requestHash != requestHash {
			return ReleaseResult{}, fmt.Errorf(
				"%w: release_request_id %q", ErrIdempotencyConflict, request.ReleaseRequestID,
			)
		}
		var result ReleaseResult
		if err := json.Unmarshal(receipt.sanitizedResult, &result); err != nil {
			return ReleaseResult{}, fmt.Errorf("decode proxy release receipt: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return ReleaseResult{}, fmt.Errorf("commit replayed proxy lease release: %w", err)
		}
		return result, nil
	}

	lease, err := lockReleaseLease(ctx, tx, m.options.WorkloadScope, request)
	if err != nil {
		return ReleaseResult{}, err
	}
	if lease.slotName != request.SlotName || lease.workerID != request.WorkerID ||
		lease.workerInstanceID != request.WorkerInstanceID {
		return ReleaseResult{}, ErrLeaseGone
	}

	result := ReleaseResult{
		OK: true, ReleaseRequestID: request.ReleaseRequestID,
		LeaseID: request.LeaseID, SlotName: request.SlotName,
		Status: lease.status, Reason: lease.releaseReason,
	}
	if lease.releasedAt != nil {
		result.ReleasedAt = *lease.releasedAt
	}
	if lease.status != "active" {
		result.Released = lease.status == "released"
		if err := persistCommandReceipt(
			ctx, tx, m.options.WorkloadScope, "release", request.ReleaseRequestID,
			requestHash, "lease", request.LeaseID, "terminal", result,
		); err != nil {
			return ReleaseResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ReleaseResult{}, fmt.Errorf("commit terminal proxy lease release: %w", err)
		}
		return result, nil
	}

	var activeTaskID string
	err = tx.QueryRow(ctx, `
		SELECT assignment_version,COALESCE(active_task_id,'')
		FROM proxy_running_slots
		WHERE slot_name=$1 AND worker_id=$2 AND worker_instance_id=$3
		  AND lease_id=$4 AND current_lease_id=$4 AND lease_until > NOW()
		FOR UPDATE
	`, request.SlotName, request.WorkerID, request.WorkerInstanceID,
		request.LeaseID).Scan(&result.RouteGeneration, &activeTaskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReleaseResult{}, ErrLeaseGone
	}
	if err != nil {
		return ReleaseResult{}, fmt.Errorf("lock proxy release slot: %w", err)
	}
	if result.RouteGeneration != request.KnownRouteGeneration {
		return ReleaseResult{}, ErrLeaseConflict
	}
	if activeTaskID != "" {
		return ReleaseResult{}, ErrTaskConflict
	}

	rotation, err := rotateSlotCredential(ctx, tx, request.SlotName)
	if err != nil {
		return ReleaseResult{}, err
	}
	err = tx.QueryRow(ctx, `
		UPDATE proxy_control_leases
		SET status='released',released_at=NOW(),release_reason=$3,updated_at=NOW()
		WHERE workload_scope=$1 AND lease_id=$2 AND status='active'
		  AND slot_name=$4 AND worker_id=$5 AND worker_instance_id=$6
		  AND lease_until > NOW()
		RETURNING released_at
	`, m.options.WorkloadScope, request.LeaseID, request.Reason,
		request.SlotName, request.WorkerID, request.WorkerInstanceID).Scan(&result.ReleasedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReleaseResult{}, ErrLeaseGone
	}
	if err != nil {
		return ReleaseResult{}, fmt.Errorf("record released proxy lease: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE proxy_running_slots
		SET worker_id=NULL,worker_instance_id=NULL,lease_id=NULL,current_lease_id=NULL,
		    lease_until=NULL,last_heartbeat_at=NULL,identity_policy_id=NULL,
		    identity_policy_version=NULL,identity_policy_hash=NULL,
		    required_egress_country=NULL,active_task_id=NULL,
		    active_task_started_at=NULL,pending_action=NULL,pending_incident_id=NULL,
		    control_state='unleased',rotation_deadline_at=NULL,
		    route_activation_old_username=NULL,route_activation_claim_id=NULL,
		    route_activation_claim_until=NULL,route_activation_previous_claim_id=NULL,
		    updated_at=NOW()
		WHERE slot_name=$1 AND worker_id=$2 AND worker_instance_id=$3
		  AND lease_id=$4 AND current_lease_id=$4 AND assignment_version=$5
		  AND active_task_id IS NULL
	`, request.SlotName, request.WorkerID, request.WorkerInstanceID,
		request.LeaseID, request.KnownRouteGeneration)
	if err != nil {
		return ReleaseResult{}, fmt.Errorf("release proxy slot: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ReleaseResult{}, ErrLeaseGone
	}
	result.Released = true
	result.Status = "released"
	result.Reason = request.Reason
	if err := persistCommandReceipt(
		ctx, tx, m.options.WorkloadScope, "release", request.ReleaseRequestID,
		requestHash, "lease", request.LeaseID, "terminal", result,
	); err != nil {
		return ReleaseResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ReleaseResult{}, fmt.Errorf("commit proxy lease release: %w", err)
	}
	m.retireUser(ctx, rotation.OldUsername)
	m.invalidateUser(rotation.NewUsername)
	m.requestReconcile()
	return result, nil
}

type releaseLeaseState struct {
	slotName         string
	workerID         string
	workerInstanceID string
	status           string
	releasedAt       *time.Time
	releaseReason    string
}

func lockReleaseLease(
	ctx context.Context,
	tx pgx.Tx,
	workloadScope string,
	request ReleaseRequest,
) (releaseLeaseState, error) {
	var state releaseLeaseState
	err := tx.QueryRow(ctx, `
		SELECT slot_name,worker_id,worker_instance_id,status,released_at,
		       COALESCE(release_reason,'')
		FROM proxy_control_leases
		WHERE workload_scope=$1 AND lease_id=$2
		FOR UPDATE
	`, workloadScope, request.LeaseID).Scan(
		&state.slotName, &state.workerID, &state.workerInstanceID,
		&state.status, &state.releasedAt, &state.releaseReason,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return releaseLeaseState{}, ErrLeaseGone
	}
	if err != nil {
		return releaseLeaseState{}, fmt.Errorf("lock proxy release lease: %w", err)
	}
	return state, nil
}

func persistCommandReceipt(
	ctx context.Context,
	tx pgx.Tx,
	workloadScope string,
	commandKind string,
	requestID string,
	requestHash string,
	resourceKind string,
	resourceID string,
	resultKind string,
	result any,
) error {
	encoded, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode proxy control command receipt: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO proxy_control_command_receipts (
		  workload_scope,command_kind,request_id,request_hash,
		  resource_kind,resource_id,result_kind,sanitized_result,retain_until
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,
		          NOW()+($9 * interval '1 millisecond'))
	`, workloadScope, commandKind, requestID, requestHash, resourceKind,
		resourceID, resultKind, string(encoded), commandReceiptRetention.Milliseconds()); err != nil {
		return fmt.Errorf("persist proxy control command receipt: %w", err)
	}
	return nil
}

func validateLeaseRequest(request LeaseRequest) error {
	if strings.TrimSpace(request.WorkerID) == "" || strings.TrimSpace(request.LeaseID) == "" ||
		request.AssignmentVersion < 0 {
		return fmt.Errorf("%w: worker_id, lease_id, and assignment_version are required", ErrInvalidInput)
	}
	return nil
}

func normalizeRenewRequest(request RenewRequest) RenewRequest {
	request.RenewRequestID = strings.TrimSpace(request.RenewRequestID)
	request.SlotName = strings.TrimSpace(request.SlotName)
	request.WorkerID = strings.TrimSpace(request.WorkerID)
	request.WorkerInstanceID = strings.TrimSpace(request.WorkerInstanceID)
	request.LeaseID = strings.TrimSpace(request.LeaseID)
	return request
}

func validateRenewRequest(request RenewRequest) error {
	for _, value := range []string{
		request.RenewRequestID,
		request.SlotName,
		request.WorkerID,
		request.WorkerInstanceID,
		request.LeaseID,
	} {
		if value == "" || len(value) > 255 {
			return fmt.Errorf("%w: renew fields are required and limited to 255 bytes", ErrInvalidInput)
		}
	}
	if request.KnownRouteGeneration < 0 {
		return fmt.Errorf("%w: known_route_generation must be non-negative", ErrInvalidInput)
	}
	return nil
}

func normalizeReleaseRequest(request ReleaseRequest) ReleaseRequest {
	request.ReleaseRequestID = strings.TrimSpace(request.ReleaseRequestID)
	request.SlotName = strings.TrimSpace(request.SlotName)
	request.WorkerID = strings.TrimSpace(request.WorkerID)
	request.WorkerInstanceID = strings.TrimSpace(request.WorkerInstanceID)
	request.LeaseID = strings.TrimSpace(request.LeaseID)
	request.Reason = strings.ToLower(strings.TrimSpace(request.Reason))
	return request
}

func validateReleaseRequest(request ReleaseRequest) error {
	for _, value := range []string{
		request.ReleaseRequestID,
		request.SlotName,
		request.WorkerID,
		request.WorkerInstanceID,
		request.LeaseID,
		request.Reason,
	} {
		if value == "" || len(value) > 255 {
			return fmt.Errorf("%w: release fields are required and limited to 255 bytes", ErrInvalidInput)
		}
	}
	if request.KnownRouteGeneration < 0 {
		return fmt.Errorf("%w: known_route_generation must be non-negative", ErrInvalidInput)
	}
	for _, character := range request.Reason {
		if (character < 'a' || character > 'z') &&
			(character < '0' || character > '9') && character != '_' &&
			character != '-' && character != '.' {
			return fmt.Errorf("%w: release reason must be a lowercase identifier", ErrInvalidInput)
		}
	}
	return nil
}

func renewRequestHash(request RenewRequest) (string, error) {
	payload, err := json.Marshal(struct {
		SchemaVersion        int    `json:"schema_version"`
		SlotName             string `json:"slot_name"`
		WorkerID             string `json:"worker_id"`
		WorkerInstanceID     string `json:"worker_instance_id"`
		LeaseID              string `json:"lease_id"`
		KnownRouteGeneration int64  `json:"known_route_generation"`
	}{
		SchemaVersion: 1, SlotName: request.SlotName, WorkerID: request.WorkerID,
		WorkerInstanceID: request.WorkerInstanceID, LeaseID: request.LeaseID,
		KnownRouteGeneration: request.KnownRouteGeneration,
	})
	if err != nil {
		return "", fmt.Errorf("encode proxy renewal request: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func releaseRequestHash(request ReleaseRequest) (string, error) {
	payload, err := json.Marshal(struct {
		SchemaVersion        int    `json:"schema_version"`
		SlotName             string `json:"slot_name"`
		WorkerID             string `json:"worker_id"`
		WorkerInstanceID     string `json:"worker_instance_id"`
		LeaseID              string `json:"lease_id"`
		KnownRouteGeneration int64  `json:"known_route_generation"`
		Reason               string `json:"reason"`
	}{
		SchemaVersion: 1, SlotName: request.SlotName, WorkerID: request.WorkerID,
		WorkerInstanceID: request.WorkerInstanceID, LeaseID: request.LeaseID,
		KnownRouteGeneration: request.KnownRouteGeneration, Reason: request.Reason,
	})
	if err != nil {
		return "", fmt.Errorf("encode proxy release request: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func (m *Manager) loadAssignment(ctx context.Context, query interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, slotName string) (Assignment, error) {
	assignment, err := scanAssignment(query.QueryRow(ctx, assignmentSQL(), slotName))
	if err != nil {
		return Assignment{}, err
	}
	assignment.WorkloadScope = m.options.WorkloadScope
	return assignment, nil
}

func scanAssignment(row scanner) (Assignment, error) {
	var (
		assignment Assignment
		address    *string
		protocol   *string
	)
	err := row.Scan(
		&assignment.SlotName, &assignment.Role, &assignment.WorkerID, &assignment.WorkerInstanceID,
		&assignment.ProxyID, &address, &protocol, &assignment.ReadyAfter,
		&assignment.LeaseID, &assignment.LeaseUntil, &assignment.AssignmentVersion,
		&assignment.CredentialGeneration, &assignment.ProxyUser, &assignment.Ready,
		&assignment.ControlState, &assignment.IdentityPolicyID, &assignment.IdentityPolicyVersion,
		&assignment.IdentityPolicyHash, &assignment.NetworkIdentityKey, &assignment.ProfileEpoch,
		&assignment.EgressCountry, &assignment.ServerTime, &assignment.LeaseRemainingMS,
	)
	if err != nil {
		return Assignment{}, fmt.Errorf("load proxy assignment: %w", err)
	}
	assignment.OK = true
	assignment.ProtocolVersion = ProtocolVersionV2
	assignment.IdentityAction = "keep"
	if assignment.ProxyID != nil && address != nil && protocol != nil {
		assignment.ProxyAddressHash = proxyAddressHash(*assignment.ProxyID, *protocol, *address)
	}
	if !assignment.Ready {
		switch {
		case assignment.ControlState == "paused_no_reserve":
			assignment.Reason = "waiting_for_healthy_proxy"
			assignment.ReasonCode = "NO_POLICY_ELIGIBLE_RESERVE"
			assignment.RetryAfterMS = 1000
		case assignment.ControlState == "pending_new_route":
			assignment.Reason = "waiting_for_rota_refresh"
			assignment.ReasonCode = "WAITING_FOR_ROUTE_REFRESH"
			assignment.RetryAfterMS = 250
		case assignment.ProxyID == nil:
			assignment.Reason = "waiting_for_healthy_proxy"
		case assignment.ReadyAfter == nil || assignment.ReadyAfter.After(time.Now()):
			assignment.Reason = "waiting_for_rota_refresh"
		default:
			assignment.Reason = "proxy_unavailable"
		}
	}
	return assignment, nil
}

func assignmentSQL() string {
	return `
		SELECT s.slot_name, s.role, COALESCE(s.worker_id,''), COALESCE(s.worker_instance_id,''), s.proxy_id,
		       p.address, p.protocol, s.ready_after, COALESCE(s.lease_id,''),
		       s.lease_until, s.assignment_version, s.credential_generation, u.username,
			       (
			         s.proxy_id IS NOT NULL AND s.ready_after IS NOT NULL
			         AND s.current_lease_id IS NOT NULL AND s.lease_until > NOW()
			         AND s.ready_after <= NOW()
			         AND (
			           s.active_task_id IS NOT NULL
			           OR (
			             p.status='active' AND p.revalidation_required=false
			             AND (p.cooldown_until IS NULL OR p.cooldown_until <= NOW())
			           )
			         )
			       ) AS ready,
		       s.control_state, COALESCE(s.identity_policy_id,''),
		       COALESCE(s.identity_policy_version,0), COALESCE(s.identity_policy_hash,''),
		       COALESCE(s.network_identity_key,''), s.profile_epoch,
		       COALESCE(p.country_code,''), NOW() AS server_time,
		       GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (s.lease_until-NOW()))*1000))::bigint
		FROM proxy_running_slots s
		JOIN proxy_users u ON u.id=s.user_id
		LEFT JOIN proxies p ON p.id=s.proxy_id
		WHERE s.slot_name=$1
	`
}

func validRole(role string) bool {
	return role == RoleDiscover || role == RoleChannel || role == RoleQueryQuality || role == RoleDetail
}

type storedClaimLease struct {
	LeaseID     string
	SlotName    string
	Status      string
	RequestHash string
	LeaseUntil  time.Time
}

func loadClaimLease(
	ctx context.Context,
	tx pgx.Tx,
	workloadScope string,
	claimRequestID string,
) (storedClaimLease, bool, error) {
	var stored storedClaimLease
	err := tx.QueryRow(ctx, `
		SELECT lease_id, slot_name, status, claim_request_hash, lease_until
		FROM proxy_control_leases
		WHERE workload_scope=$1 AND claim_request_id=$2
		FOR UPDATE
	`, workloadScope, claimRequestID).Scan(
		&stored.LeaseID, &stored.SlotName, &stored.Status, &stored.RequestHash, &stored.LeaseUntil,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return storedClaimLease{}, false, nil
	}
	if err != nil {
		return storedClaimLease{}, false, fmt.Errorf("load proxy claim receipt: %w", err)
	}
	return stored, true, nil
}

func normalizeClaimRequest(request ClaimRequest) ClaimRequest {
	request.ClaimRequestID = strings.TrimSpace(request.ClaimRequestID)
	request.Role = strings.ToLower(strings.TrimSpace(request.Role))
	request.WorkerID = strings.TrimSpace(request.WorkerID)
	request.WorkerInstanceID = strings.TrimSpace(request.WorkerInstanceID)
	request.IdentityPolicyID = strings.TrimSpace(request.IdentityPolicyID)
	return request
}

func validateClaimRequest(request ClaimRequest) error {
	values := []string{
		request.ClaimRequestID,
		request.Role,
		request.WorkerID,
		request.WorkerInstanceID,
		request.IdentityPolicyID,
	}
	for _, value := range values {
		if value == "" || len(value) > 255 {
			return fmt.Errorf("%w: claim fields are required and limited to 255 bytes", ErrInvalidInput)
		}
	}
	if request.ProtocolVersion != ProtocolVersionV2 {
		return fmt.Errorf("%w: protocol_version must be %d", ErrInvalidInput, ProtocolVersionV2)
	}
	if !validRole(request.Role) || request.IdentityPolicyVersion <= 0 {
		return fmt.Errorf("%w: role or identity policy version is invalid", ErrInvalidInput)
	}
	return nil
}

func (m *Manager) resolveIdentityPolicy(request ClaimRequest) (IdentityPolicy, error) {
	policy, found := m.options.IdentityPolicies[request.IdentityPolicyID]
	policy.AllowedProxyTags = append([]string(nil), policy.AllowedProxyTags...)
	if policy.AllowedProxyTags == nil {
		policy.AllowedProxyTags = []string{}
	}
	policy.ID = strings.TrimSpace(policy.ID)
	policy.Role = strings.ToLower(strings.TrimSpace(policy.Role))
	policy.Hash = strings.TrimSpace(policy.Hash)
	policy.RequiredEgressCountry = strings.ToUpper(strings.TrimSpace(policy.RequiredEgressCountry))
	if !found || policy.ID != request.IdentityPolicyID || policy.Version != request.IdentityPolicyVersion ||
		policy.Role != request.Role || policy.Hash == "" {
		return IdentityPolicy{}, fmt.Errorf("%w: identity policy does not match role or version", ErrPolicyRejected)
	}
	if policy.RequiredEgressCountry != "" && len(policy.RequiredEgressCountry) != 2 {
		return IdentityPolicy{}, fmt.Errorf("%w: required egress country must be ISO alpha-2", ErrPolicyRejected)
	}
	for index := range policy.AllowedProxyTags {
		policy.AllowedProxyTags[index] = strings.ToLower(strings.TrimSpace(policy.AllowedProxyTags[index]))
		if policy.AllowedProxyTags[index] == "" {
			return IdentityPolicy{}, fmt.Errorf("%w: allowed proxy tags cannot be empty", ErrPolicyRejected)
		}
	}
	return policy, nil
}

func claimRequestHash(request ClaimRequest) (string, error) {
	payload, err := json.Marshal(struct {
		SchemaVersion         int    `json:"schema_version"`
		ProtocolVersion       int    `json:"protocol_version"`
		Role                  string `json:"role"`
		WorkerID              string `json:"worker_id"`
		WorkerInstanceID      string `json:"worker_instance_id"`
		IdentityPolicyID      string `json:"identity_policy_id"`
		IdentityPolicyVersion int    `json:"identity_policy_version"`
	}{
		SchemaVersion: 1, ProtocolVersion: request.ProtocolVersion, Role: request.Role,
		WorkerID: request.WorkerID, WorkerInstanceID: request.WorkerInstanceID,
		IdentityPolicyID: request.IdentityPolicyID, IdentityPolicyVersion: request.IdentityPolicyVersion,
	})
	if err != nil {
		return "", fmt.Errorf("encode proxy claim request: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func proxyAddressHash(proxyID int, protocol, address string) string {
	value := fmt.Sprintf(
		"%d|%s|%s",
		proxyID,
		strings.ToLower(strings.TrimSpace(protocol)),
		strings.ToLower(strings.TrimSpace(address)),
	)
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
