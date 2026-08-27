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
	Fence       routeActivationFence
	ClaimID     string
	OldUsername string
	NewUsername string
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
	claim, found, err := m.claimPendingRouteActivation(ctx, fence)
	if err != nil {
		m.logError("claim pending Route activation failed", err, "slot", fence.SlotName)
		return false
	}
	if !found {
		return false
	}

	activationCtx, cancel := context.WithTimeout(ctx, routeActivationAttemptTimeout)
	activated := m.activateUser(
		activationCtx, claim.OldUsername, claim.NewUsername, claim.Fence.ProxyID,
	)
	cancel()
	if !activated {
		m.releaseRouteActivationClaim(ctx, claim)
		return false
	}

	tag, err := m.db.Pool.Exec(ctx, `
		UPDATE proxy_running_slots
		SET ready_after=NOW(),control_state='leased_idle',
		    route_activation_old_username=NULL,route_activation_claim_id=NULL,
		    route_activation_claim_until=NULL,rotation_deadline_at=NULL,updated_at=NOW()
		WHERE slot_name=$1 AND current_lease_id=$2 AND lease_until > NOW()
		  AND proxy_id=$3 AND assignment_version=$4
		  AND active_task_id IS NULL AND control_state='pending_new_route'
		  AND route_activation_claim_id=$5
	`, claim.Fence.SlotName, claim.Fence.LeaseID, claim.Fence.ProxyID,
		claim.Fence.RouteGeneration, claim.ClaimID)
	if err != nil {
		m.compensateLostRouteActivation(ctx, claim)
		m.logError("mark pending proxy binding ready failed", err, "slot", fence.SlotName)
		return false
	}
	if tag.RowsAffected() != 1 {
		m.compensateLostRouteActivation(ctx, claim)
		m.logError(
			"pending Route activation lost its Fence",
			ErrLeaseConflict,
			"slot", fence.SlotName,
			"route_generation", fence.RouteGeneration,
		)
		return false
	}
	return true
}

func (m *Manager) compensateLostRouteActivation(
	ctx context.Context,
	claim routeActivationClaim,
) {
	compensationCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		routeActivationAttemptTimeout,
	)
	defer cancel()
	m.retireUser(compensationCtx, claim.NewUsername)
	m.releaseRouteActivationClaim(compensationCtx, claim)
}

func (m *Manager) claimPendingRouteActivation(
	ctx context.Context,
	fence routeActivationFence,
) (routeActivationClaim, bool, error) {
	claim := routeActivationClaim{
		Fence:   fence,
		ClaimID: uuid.NewString(),
	}
	err := m.db.Pool.QueryRow(ctx, `
		UPDATE proxy_running_slots slot
		SET route_activation_claim_id=$5,
		    route_activation_claim_until=NOW()+($6::bigint*interval '1 millisecond'),
		    updated_at=NOW()
		FROM proxy_users proxy_user
		WHERE slot.slot_name=$1 AND slot.current_lease_id=$2 AND slot.lease_until > NOW()
		  AND slot.proxy_id=$3 AND slot.assignment_version=$4
		  AND slot.user_id=proxy_user.id AND slot.ready_after IS NULL
		  AND slot.active_task_id IS NULL AND slot.control_state='pending_new_route'
		  AND (
		    slot.route_activation_claim_id IS NULL
		    OR slot.route_activation_claim_until IS NULL
		    OR slot.route_activation_claim_until <= NOW()
		  )
		RETURNING COALESCE(slot.route_activation_old_username,''),proxy_user.username
	`, fence.SlotName, fence.LeaseID, fence.ProxyID, fence.RouteGeneration, claim.ClaimID,
		routeActivationClaimTTL.Milliseconds()).Scan(
		&claim.OldUsername, &claim.NewUsername,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return routeActivationClaim{}, false, nil
	}
	if err != nil {
		return routeActivationClaim{}, false, fmt.Errorf("claim pending Route activation: %w", err)
	}
	return claim, true, nil
}

func (m *Manager) releaseRouteActivationClaim(ctx context.Context, claim routeActivationClaim) {
	if _, err := m.db.Pool.Exec(ctx, `
		UPDATE proxy_running_slots
		SET route_activation_claim_id=NULL,route_activation_claim_until=NULL,updated_at=NOW()
		WHERE slot_name=$1 AND current_lease_id=$2 AND lease_until > NOW()
		  AND proxy_id=$3 AND assignment_version=$4
		  AND active_task_id IS NULL AND control_state='pending_new_route'
		  AND route_activation_claim_id=$5
	`, claim.Fence.SlotName, claim.Fence.LeaseID, claim.Fence.ProxyID,
		claim.Fence.RouteGeneration, claim.ClaimID); err != nil {
		m.logError("release pending Route activation claim failed", err, "slot", claim.Fence.SlotName)
	}
}
