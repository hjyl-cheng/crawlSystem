package proxycontrol

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	routeActivationAttemptTimeout  = 30 * time.Second
	routeActivationClaimTTL        = time.Minute
	routeActivationFinalizeTimeout = 5 * time.Second
	routeActivationFinalizeMargin  = 10 * time.Second
)

type routeActivationClaim struct {
	Fence           routeActivationFence
	ClaimID         string
	PreviousClaimID string
	OldUsername     string
	NewUsername     string
}

type routeActivationFence struct {
	SlotName        string
	LeaseID         string
	ProxyID         int
	RouteGeneration int64
}

func (m *Manager) activatePendingRoute(
	ctx context.Context,
	fence routeActivationFence,
) bool {
	claim, found, err := m.loadOrClaimPendingRouteActivation(ctx, fence)
	if err != nil {
		m.logError("load or claim pending Route activation failed", err, "slot", fence.SlotName)
		return false
	}
	if !found {
		return false
	}

	activationCtx, cancel := context.WithTimeout(ctx, routeActivationAttemptTimeout)
	begin, began := m.beginUserActivation(
		activationCtx,
		claim.OldUsername,
		claim.NewUsername,
		claim.Fence.ProxyID,
		claim.PreviousClaimID,
		claim.ClaimID,
	)
	cancel()
	if !began {
		// Uncertain Begin may have left an activating Token. Compensate only
		// within that Claim; never unconditionally retire the username.
		return m.resolveUncertainBegin(ctx, claim)
	}
	if !m.renewRouteActivationClaim(ctx, claim) {
		return m.resolveUncertainRouteActivation(ctx, claim)
	}

	if !begin.AlreadyCommitted {
		commitCtx, commitCancel := context.WithTimeout(ctx, routeActivationAttemptTimeout)
		committed := m.commitUserActivation(commitCtx, claim.NewUsername, claim.ClaimID)
		commitCancel()
		if !committed {
			// Commit errors are uncertain: the Token may already be committed.
			// Only a confirmed !began may use claim-scoped compensation.
			return m.resolveUncertainRouteActivation(ctx, claim)
		}
		if !m.renewRouteActivationClaim(ctx, claim) {
			return m.resolveUncertainRouteActivation(ctx, claim)
		}
	}

	finalizeCtx, finalizeCancel := context.WithTimeout(ctx, routeActivationFinalizeTimeout)
	finalized, err := m.finalizeRouteActivationClaim(finalizeCtx, claim)
	finalizeCancel()
	if err != nil {
		if m.resolveUncertainRouteActivation(ctx, claim) {
			return true
		}
		m.logError("mark pending Route activation ready failed", err, "slot", fence.SlotName)
		return false
	}
	if !finalized {
		if m.resolveUncertainRouteActivation(ctx, claim) {
			return true
		}
		m.logError(
			"pending Route activation lost its live Claim Fence",
			ErrLeaseConflict,
			"slot", fence.SlotName,
			"route_generation", fence.RouteGeneration,
			"claim_id", claim.ClaimID,
		)
		return false
	}
	return true
}

func (m *Manager) finalizeRouteActivationClaim(
	ctx context.Context,
	claim routeActivationClaim,
) (bool, error) {
	tx, err := m.db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin pending Route Finalize: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, controlAdvisoryLock); err != nil {
		return false, fmt.Errorf("lock pending Route Finalize: %w", err)
	}

	var leaseID string
	err = tx.QueryRow(ctx, `
		SELECT lease_id
		FROM proxy_control_leases
		WHERE workload_scope=$1 AND lease_id=$2
		FOR UPDATE
	`, m.options.WorkloadScope, claim.Fence.LeaseID).Scan(&leaseID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock pending Route Finalize Lease history: %w", err)
	}

	var slotName string
	err = tx.QueryRow(ctx, `
		SELECT slot_name
		FROM proxy_running_slots
		WHERE slot_name=$1
		FOR UPDATE
	`, claim.Fence.SlotName).Scan(&slotName)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock pending Route Finalize Slot: %w", err)
	}

	tag, err := tx.Exec(ctx, `
		UPDATE proxy_running_slots AS slot
		SET ready_after=statement_timestamp(),control_state='leased_idle',
		    route_activation_old_username=NULL,route_activation_claim_id=NULL,
		    route_activation_claim_until=NULL,route_activation_previous_claim_id=NULL,
		    rotation_deadline_at=NULL,updated_at=statement_timestamp()
		WHERE slot.slot_name=$1
		  AND slot.current_lease_id=$2
		  AND slot.lease_until > statement_timestamp() + interval '10 seconds'
		  AND EXISTS (
		    SELECT 1
		    FROM proxy_control_leases live_lease
		    WHERE live_lease.workload_scope=$6
		      AND live_lease.lease_id=slot.current_lease_id
		      AND live_lease.slot_name=slot.slot_name
		      AND live_lease.status='active'
		      AND live_lease.lease_until > statement_timestamp() + interval '10 seconds'
		  )
		  AND slot.proxy_id=$3 AND slot.assignment_version=$4
		  AND slot.active_task_id IS NULL AND slot.control_state='pending_new_route'
		  AND slot.route_activation_claim_id=$5
		  AND slot.route_activation_claim_until > statement_timestamp() + interval '10 seconds'
	`, claim.Fence.SlotName, claim.Fence.LeaseID, claim.Fence.ProxyID,
		claim.Fence.RouteGeneration, claim.ClaimID, m.options.WorkloadScope)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() != 1 {
		return false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit pending Route Finalize: %w", err)
	}
	return true, nil
}

func (m *Manager) resolveUncertainBegin(
	ctx context.Context,
	claim routeActivationClaim,
) bool {
	return m.resolveUncertainRouteActivationWithOptions(ctx, claim, false)
}

func (m *Manager) resolveUncertainRouteActivation(
	ctx context.Context,
	claim routeActivationClaim,
) bool {
	return m.resolveUncertainRouteActivationWithOptions(ctx, claim, true)
}

func (m *Manager) resolveUncertainRouteActivationWithOptions(
	ctx context.Context,
	claim routeActivationClaim,
	allowUnconditionalRetire bool,
) bool {
	compensationCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		routeActivationAttemptTimeout,
	)
	defer cancel()
	var controlState string
	var ready, leaseLive bool
	err := m.db.Pool.QueryRow(compensationCtx, `
		SELECT slot.control_state,slot.ready_after IS NOT NULL,
		       `+expectedLiveLeaseFencePredicate(2, 6)+`
		FROM proxy_running_slots slot
		JOIN proxy_users proxy_user ON proxy_user.id=slot.user_id
		WHERE slot.slot_name=$1 AND slot.current_lease_id=$2
		  AND slot.proxy_id=$3 AND slot.assignment_version=$4
		  AND proxy_user.username=$5
	`, claim.Fence.SlotName, claim.Fence.LeaseID, claim.Fence.ProxyID,
		claim.Fence.RouteGeneration, claim.NewUsername, m.options.WorkloadScope).Scan(
		&controlState,
		&ready,
		&leaseLive,
	)
	if err == nil && leaseLive && ready &&
		(controlState == "leased_idle" || controlState == "active_task") {
		return true
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		m.logError(
			"resolve uncertain Route activation failed", err,
			"slot", claim.Fence.SlotName,
			"route_generation", claim.Fence.RouteGeneration,
		)
		return false
	}
	fenceGone := errors.Is(err, pgx.ErrNoRows) || (err == nil && !leaseLive)
	if fenceGone && allowUnconditionalRetire {
		// The authoritative Fence no longer exists or is not live. This
		// username was the activation in progress; retire it even if the
		// Token is already committed. Real query errors must not retire.
		m.retireUser(compensationCtx, claim.NewUsername)
		return false
	}
	// A committed Token makes this a no-op. Only an activation that never
	// reached Commit remains eligible for compensating retirement.
	m.retireUserIfClaim(compensationCtx, claim.NewUsername, claim.ClaimID)
	return false
}

func (m *Manager) loadOrClaimPendingRouteActivation(
	ctx context.Context,
	fence routeActivationFence,
) (routeActivationClaim, bool, error) {
	tx, err := m.db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return routeActivationClaim{}, false, fmt.Errorf("begin pending Route activation claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, controlAdvisoryLock); err != nil {
		return routeActivationClaim{}, false, fmt.Errorf("lock pending Route activation claim: %w", err)
	}
	var leaseID string
	err = tx.QueryRow(ctx, `
		SELECT lease_id
		FROM proxy_control_leases
		WHERE workload_scope=$1 AND lease_id=$2
		FOR UPDATE
	`, m.options.WorkloadScope, fence.LeaseID).Scan(&leaseID)
	if errors.Is(err, pgx.ErrNoRows) {
		return routeActivationClaim{}, false, nil
	}
	if err != nil {
		return routeActivationClaim{}, false, fmt.Errorf("lock pending Route activation Lease history: %w", err)
	}

	// Lock Slot before reading Claim expiry. statement_timestamp() is fixed
	// at statement start, so a combined SELECT ... FOR UPDATE can treat a
	// Claim that expired while waiting as still live.
	var slotName string
	err = tx.QueryRow(ctx, `
		SELECT slot.slot_name
		FROM proxy_running_slots slot
		JOIN proxy_users proxy_user ON proxy_user.id=slot.user_id
		WHERE slot.slot_name=$1
		FOR UPDATE OF slot,proxy_user
	`, fence.SlotName).Scan(&slotName)
	if errors.Is(err, pgx.ErrNoRows) {
		return routeActivationClaim{}, false, nil
	}
	if err != nil {
		return routeActivationClaim{}, false, fmt.Errorf("lock pending Route activation Slot: %w", err)
	}

	claim := routeActivationClaim{Fence: fence}
	var persistedClaimID, persistedPreviousClaimID string
	var persistedClaimUntil *time.Time
	var databaseNow time.Time
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(slot.route_activation_old_username,''),proxy_user.username,
		       COALESCE(slot.route_activation_claim_id,''),
		       COALESCE(slot.route_activation_previous_claim_id,''),
		       slot.route_activation_claim_until,statement_timestamp()
		FROM proxy_running_slots slot
		JOIN proxy_users proxy_user ON proxy_user.id=slot.user_id
		WHERE slot.slot_name=$1
		  AND `+expectedLiveLeaseFencePredicate(2, 5)+`
		  AND slot.proxy_id=$3 AND slot.assignment_version=$4
		  AND slot.ready_after IS NULL AND slot.active_task_id IS NULL
		  AND slot.control_state='pending_new_route'
	`, fence.SlotName, fence.LeaseID, fence.ProxyID, fence.RouteGeneration,
		m.options.WorkloadScope).Scan(
		&claim.OldUsername,
		&claim.NewUsername,
		&persistedClaimID,
		&persistedPreviousClaimID,
		&persistedClaimUntil,
		&databaseNow,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return routeActivationClaim{}, false, nil
	}
	if err != nil {
		return routeActivationClaim{}, false, fmt.Errorf("lock pending Route activation: %w", err)
	}

	if persistedClaimID != "" && persistedClaimUntil != nil && persistedClaimUntil.After(databaseNow) {
		claim.ClaimID = persistedClaimID
		claim.PreviousClaimID = persistedPreviousClaimID
		if err := tx.Commit(ctx); err != nil {
			return routeActivationClaim{}, false, fmt.Errorf("commit loaded Route activation claim: %w", err)
		}
		return claim, true, nil
	}

	claim.PreviousClaimID = persistedClaimID
	claim.ClaimID = uuid.NewString()
	tag, err := tx.Exec(ctx, `
		UPDATE proxy_running_slots AS slot
		SET route_activation_claim_id=$5,
		    route_activation_claim_until=statement_timestamp()+($6::bigint*interval '1 millisecond'),
		    route_activation_previous_claim_id=NULLIF($7,''),
		    updated_at=statement_timestamp()
		WHERE slot.slot_name=$1
		  AND `+expectedLiveLeaseFencePredicate(2, 8)+`
		  AND slot.proxy_id=$3 AND slot.assignment_version=$4
		  AND slot.ready_after IS NULL AND slot.active_task_id IS NULL
		  AND slot.control_state='pending_new_route'
	`, fence.SlotName, fence.LeaseID, fence.ProxyID, fence.RouteGeneration,
		claim.ClaimID, routeActivationClaimTTL.Milliseconds(), claim.PreviousClaimID,
		m.options.WorkloadScope)
	if err != nil {
		return routeActivationClaim{}, false, fmt.Errorf("persist pending Route activation claim: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return routeActivationClaim{}, false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return routeActivationClaim{}, false, fmt.Errorf("commit pending Route activation claim: %w", err)
	}
	return claim, true, nil
}

func (m *Manager) renewRouteActivationClaim(ctx context.Context, claim routeActivationClaim) bool {
	tx, err := m.db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		m.logError(
			"renew pending Route activation claim failed", err,
			"slot", claim.Fence.SlotName,
			"claim_id", claim.ClaimID,
		)
		return false
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, controlAdvisoryLock); err != nil {
		m.logError(
			"renew pending Route activation claim failed", err,
			"slot", claim.Fence.SlotName,
			"claim_id", claim.ClaimID,
		)
		return false
	}
	var leaseID string
	err = tx.QueryRow(ctx, `
		SELECT lease_id
		FROM proxy_control_leases
		WHERE workload_scope=$1 AND lease_id=$2
		FOR UPDATE
	`, m.options.WorkloadScope, claim.Fence.LeaseID).Scan(&leaseID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			m.logError(
				"renew pending Route activation claim failed", err,
				"slot", claim.Fence.SlotName,
				"claim_id", claim.ClaimID,
			)
		}
		return false
	}

	// Lock Slot before the expiry check. statement_timestamp() is fixed at
	// statement start, so a combined UPDATE ... FOR UPDATE can revive a
	// Claim that expired while waiting on the row.
	var slotName string
	err = tx.QueryRow(ctx, `
		SELECT slot_name
		FROM proxy_running_slots
		WHERE slot_name=$1
		FOR UPDATE
	`, claim.Fence.SlotName).Scan(&slotName)
	if errors.Is(err, pgx.ErrNoRows) {
		return false
	}
	if err != nil {
		m.logError(
			"renew pending Route activation claim failed", err,
			"slot", claim.Fence.SlotName,
			"claim_id", claim.ClaimID,
		)
		return false
	}

	tag, err := tx.Exec(ctx, `
		UPDATE proxy_running_slots AS slot
		SET route_activation_claim_until=statement_timestamp()+($6::bigint*interval '1 millisecond'),
		    updated_at=statement_timestamp()
		WHERE slot.slot_name=$1
		  AND `+expectedLiveLeaseFencePredicate(2, 7)+`
		  AND slot.proxy_id=$3 AND slot.assignment_version=$4
		  AND slot.active_task_id IS NULL AND slot.control_state='pending_new_route'
		  AND slot.ready_after IS NULL AND slot.route_activation_claim_id=$5
		  AND slot.route_activation_claim_until > statement_timestamp()
	`, claim.Fence.SlotName, claim.Fence.LeaseID, claim.Fence.ProxyID,
		claim.Fence.RouteGeneration, claim.ClaimID, routeActivationClaimTTL.Milliseconds(),
		m.options.WorkloadScope)
	if err != nil {
		m.logError(
			"renew pending Route activation claim failed", err,
			"slot", claim.Fence.SlotName,
			"claim_id", claim.ClaimID,
		)
		return false
	}
	if tag.RowsAffected() != 1 {
		return false
	}
	if err := tx.Commit(ctx); err != nil {
		m.logError(
			"renew pending Route activation claim failed", err,
			"slot", claim.Fence.SlotName,
			"claim_id", claim.ClaimID,
		)
		return false
	}
	return true
}

func (m *Manager) loadRouteActivationRegistry(
	ctx context.Context,
) ([]RouteActivationRegistryEntry, error) {
	rows, err := m.db.Pool.Query(ctx, `
		SELECT proxy_user.username,slot.control_state,slot.ready_after IS NOT NULL,
		       `+liveLeaseFencePredicate(1)+`,
		       COALESCE(slot.route_activation_claim_id,'')
		FROM proxy_running_slots slot
		JOIN proxy_users proxy_user ON proxy_user.id=slot.user_id
		ORDER BY slot.slot_name
	`, m.options.WorkloadScope)
	if err != nil {
		return nil, fmt.Errorf("load Route activation registry: %w", err)
	}
	defer rows.Close()

	entries := make([]RouteActivationRegistryEntry, 0)
	for rows.Next() {
		var (
			entry        RouteActivationRegistryEntry
			controlState string
			ready        bool
			liveLease    bool
		)
		if err := rows.Scan(
			&entry.Username,
			&controlState,
			&ready,
			&liveLease,
			&entry.ClaimID,
		); err != nil {
			return nil, fmt.Errorf("scan Route activation registry: %w", err)
		}
		entry.Blocked = true
		switch {
		case liveLease && ready &&
			(controlState == "leased_idle" || controlState == "active_task"):
			entry.Phase = RouteActivationCommitted
			entry.Blocked = false
		case liveLease && !ready && controlState == "pending_new_route":
			if entry.ClaimID != "" {
				entry.Phase = RouteActivationActivating
			}
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Route activation registry: %w", err)
	}
	return entries, nil
}
