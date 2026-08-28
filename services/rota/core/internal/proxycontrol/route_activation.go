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
	routeActivationAttemptTimeout = 30 * time.Second
	routeActivationClaimTTL       = time.Minute
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
	if !began || !m.renewRouteActivationClaim(ctx, claim) {
		return false
	}

	if !begin.AlreadyCommitted {
		commitCtx, commitCancel := context.WithTimeout(ctx, routeActivationAttemptTimeout)
		committed := m.commitUserActivation(commitCtx, claim.NewUsername, claim.ClaimID)
		commitCancel()
		if !committed || !m.renewRouteActivationClaim(ctx, claim) {
			return false
		}
	}

	finalized, err := m.finalizeRouteActivationClaim(ctx, claim)
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
	tag, err := m.db.Pool.Exec(ctx, `
		UPDATE proxy_running_slots
		SET ready_after=NOW(),control_state='leased_idle',
		    route_activation_old_username=NULL,route_activation_claim_id=NULL,
		    route_activation_claim_until=NULL,route_activation_previous_claim_id=NULL,
		    rotation_deadline_at=NULL,updated_at=NOW()
		WHERE slot_name=$1 AND current_lease_id=$2 AND lease_until > NOW()
		  AND proxy_id=$3 AND assignment_version=$4
		  AND active_task_id IS NULL AND control_state='pending_new_route'
		  AND route_activation_claim_id=$5 AND route_activation_claim_until > NOW()
	`, claim.Fence.SlotName, claim.Fence.LeaseID, claim.Fence.ProxyID,
		claim.Fence.RouteGeneration, claim.ClaimID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (m *Manager) resolveUncertainRouteActivation(
	ctx context.Context,
	claim routeActivationClaim,
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
		       COALESCE(slot.lease_until > NOW(),false) AND EXISTS (
		         SELECT 1
		         FROM proxy_control_leases lease
		         WHERE lease.workload_scope=$6 AND lease.lease_id=slot.current_lease_id
		           AND lease.slot_name=slot.slot_name AND lease.status='active'
		           AND lease.lease_until > NOW()
		       )
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
	if err == nil && !leaseLive {
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

	claim := routeActivationClaim{Fence: fence}
	var persistedClaimID, persistedPreviousClaimID string
	var persistedClaimUntil *time.Time
	var databaseNow time.Time
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(slot.route_activation_old_username,''),proxy_user.username,
		       COALESCE(slot.route_activation_claim_id,''),
		       COALESCE(slot.route_activation_previous_claim_id,''),
		       slot.route_activation_claim_until,NOW()
		FROM proxy_running_slots slot
		JOIN proxy_users proxy_user ON proxy_user.id=slot.user_id
		WHERE slot.slot_name=$1 AND slot.current_lease_id=$2 AND slot.lease_until > NOW()
		  AND slot.proxy_id=$3 AND slot.assignment_version=$4
		  AND slot.ready_after IS NULL AND slot.active_task_id IS NULL
		  AND slot.control_state='pending_new_route'
		FOR UPDATE OF slot,proxy_user
	`, fence.SlotName, fence.LeaseID, fence.ProxyID, fence.RouteGeneration).Scan(
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
		UPDATE proxy_running_slots
		SET route_activation_claim_id=$5,
		    route_activation_claim_until=NOW()+($6::bigint*interval '1 millisecond'),
		    route_activation_previous_claim_id=NULLIF($7,''),
		    updated_at=NOW()
		WHERE slot_name=$1 AND current_lease_id=$2 AND lease_until > NOW()
		  AND proxy_id=$3 AND assignment_version=$4
		  AND ready_after IS NULL AND active_task_id IS NULL
		  AND control_state='pending_new_route'
	`, fence.SlotName, fence.LeaseID, fence.ProxyID, fence.RouteGeneration,
		claim.ClaimID, routeActivationClaimTTL.Milliseconds(), claim.PreviousClaimID)
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
	tag, err := m.db.Pool.Exec(ctx, `
		UPDATE proxy_running_slots
		SET route_activation_claim_until=NOW()+($6::bigint*interval '1 millisecond'),
		    updated_at=NOW()
		WHERE slot_name=$1 AND current_lease_id=$2 AND lease_until > NOW()
		  AND proxy_id=$3 AND assignment_version=$4
		  AND active_task_id IS NULL AND control_state='pending_new_route'
		  AND ready_after IS NULL AND route_activation_claim_id=$5
		  AND route_activation_claim_until > NOW()
	`, claim.Fence.SlotName, claim.Fence.LeaseID, claim.Fence.ProxyID,
		claim.Fence.RouteGeneration, claim.ClaimID, routeActivationClaimTTL.Milliseconds())
	if err != nil {
		m.logError(
			"renew pending Route activation claim failed", err,
			"slot", claim.Fence.SlotName,
			"claim_id", claim.ClaimID,
		)
		return false
	}
	return tag.RowsAffected() == 1
}

func (m *Manager) loadRouteActivationRegistry(
	ctx context.Context,
) ([]RouteActivationRegistryEntry, error) {
	rows, err := m.db.Pool.Query(ctx, `
		SELECT proxy_user.username,slot.control_state,slot.ready_after IS NOT NULL,
		       slot.current_lease_id IS NOT NULL AND slot.lease_until > NOW(),
		       COALESCE(slot.route_activation_claim_id,'')
		FROM proxy_running_slots slot
		JOIN proxy_users proxy_user ON proxy_user.id=slot.user_id
		ORDER BY slot.slot_name
	`)
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
