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
	ClaimID           string
	SlotName          string
	OldUsername       string
	NewUsername       string
	ProxyID           int
	AssignmentVersion int64
}

func (m *Manager) activatePendingRoute(
	ctx context.Context,
	slotName string,
	proxyID int,
	assignmentVersion int64,
) bool {
	claim, found, err := m.claimPendingRouteActivation(
		ctx, slotName, proxyID, assignmentVersion,
	)
	if err != nil {
		m.logError("claim pending Route activation failed", err, "slot", slotName)
		return false
	}
	if !found {
		return false
	}

	activationCtx, cancel := context.WithTimeout(ctx, routeActivationAttemptTimeout)
	activated := m.activateUser(
		activationCtx, claim.OldUsername, claim.NewUsername, claim.ProxyID,
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
		WHERE slot_name=$1 AND proxy_id=$2 AND assignment_version=$3
		  AND active_task_id IS NULL AND control_state='pending_new_route'
		  AND route_activation_claim_id=$4
	`, claim.SlotName, claim.ProxyID, claim.AssignmentVersion, claim.ClaimID)
	if err != nil {
		m.logError("mark pending proxy binding ready failed", err, "slot", slotName)
		return false
	}
	if tag.RowsAffected() != 1 {
		m.logError(
			"pending Route activation lost its Fence",
			ErrLeaseConflict,
			"slot", slotName,
			"route_generation", assignmentVersion,
		)
		return false
	}
	return true
}

func (m *Manager) claimPendingRouteActivation(
	ctx context.Context,
	slotName string,
	proxyID int,
	assignmentVersion int64,
) (routeActivationClaim, bool, error) {
	claim := routeActivationClaim{
		ClaimID:           uuid.NewString(),
		SlotName:          slotName,
		ProxyID:           proxyID,
		AssignmentVersion: assignmentVersion,
	}
	err := m.db.Pool.QueryRow(ctx, `
		UPDATE proxy_running_slots slot
		SET route_activation_claim_id=$4,
		    route_activation_claim_until=NOW()+($5::bigint*interval '1 millisecond'),
		    updated_at=NOW()
		FROM proxy_users proxy_user
		WHERE slot.slot_name=$1 AND slot.proxy_id=$2 AND slot.assignment_version=$3
		  AND slot.user_id=proxy_user.id AND slot.ready_after IS NULL
		  AND slot.active_task_id IS NULL AND slot.control_state='pending_new_route'
		  AND (
		    slot.route_activation_claim_id IS NULL
		    OR slot.route_activation_claim_until IS NULL
		    OR slot.route_activation_claim_until <= NOW()
		  )
		RETURNING COALESCE(slot.route_activation_old_username,''),proxy_user.username
	`, slotName, proxyID, assignmentVersion, claim.ClaimID,
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
		WHERE slot_name=$1 AND proxy_id=$2 AND assignment_version=$3
		  AND active_task_id IS NULL AND control_state='pending_new_route'
		  AND route_activation_claim_id=$4
	`, claim.SlotName, claim.ProxyID, claim.AssignmentVersion, claim.ClaimID); err != nil {
		m.logError("release pending Route activation claim failed", err, "slot", claim.SlotName)
	}
}
