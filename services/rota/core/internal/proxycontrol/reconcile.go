package proxycontrol

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type runningSlot struct {
	Name              string
	Role              string
	Number            int
	PoolID            int
	UserID            int
	ProxyUser         string
	ProxyID           *int
	AssignmentVersion int64
	ReadyAfter        *time.Time
	WorkerID          *string
	LeaseUntil        *time.Time
	CurrentLeaseID    *string
	ActiveTaskID      *string
	ControlState      string
	IdentityPolicyID  string
}

type routeAssignmentRefresh struct {
	SlotName          string
	OldProxyUser      string
	ProxyUser         string
	ProxyID           *int
	AssignmentVersion int64
	ControlState      string
	ActivationFence   *routeActivationFence
}

type reconcileSummary struct {
	Eligible int
	Running  int
	Reserve  int
	Changes  int
}

func (m *Manager) reconcile(ctx context.Context) (reconcileSummary, error) {
	if err := m.requireEnabled(); err != nil {
		return reconcileSummary{}, err
	}
	m.reconcileMu.Lock()
	defer m.reconcileMu.Unlock()
	if _, err := m.cleanupExpiredLeases(ctx); err != nil {
		return reconcileSummary{}, err
	}
	tx, err := m.db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return reconcileSummary{}, fmt.Errorf("begin proxy reconciliation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, controlAdvisoryLock); err != nil {
		return reconcileSummary{}, fmt.Errorf("lock proxy reconciliation: %w", err)
	}

	slots, err := loadRunningSlots(ctx, tx)
	if err != nil {
		return reconcileSummary{}, err
	}
	candidatesByRole, err := m.loadEligibleCandidatesByRole(ctx, tx)
	if err != nil {
		return reconcileSummary{}, err
	}
	members, err := loadPoolMembers(ctx, tx, slots)
	if err != nil {
		return reconcileSummary{}, err
	}

	states := make([]slotState, 0, len(slots))
	for _, slot := range slots {
		leaseOwned := slotLeaseOwned(slot)
		executionLocked := leaseOwned && slot.ActiveTaskID != nil
		idleRouteStable := leaseOwned && slot.ActiveTaskID == nil &&
			proxyEligibleForRole(slot.ProxyID, candidatesByRole[slot.Role])
		states = append(states, slotState{
			Name:    slot.Name,
			Role:    slot.Role,
			Number:  slot.Number,
			ProxyID: slot.ProxyID,
			Locked:  executionLocked || idleRouteStable,
		})
	}
	plan := planPolicyAssignments(states, candidatesByRole)
	refreshes := make([]routeAssignmentRefresh, 0)
	assignmentChanges := make(map[string]bool)
	for _, slot := range slots {
		if slotLeaseOwned(slot) && slot.ActiveTaskID != nil {
			continue
		}
		desired := plan.BySlot[slot.Name]
		membershipMatches := exactPoolMembership(members[slot.PoolID], desired)
		assignmentMatches := equalOptionalInt(slot.ProxyID, desired)
		if !membershipMatches || !assignmentMatches {
			assignmentChanges[slot.Name] = true
		}
	}

	// Release changed assignments first so unique Route assignments cannot collide
	// while two free slots exchange endpoints.
	changedNames := make([]string, 0, len(assignmentChanges))
	for name := range assignmentChanges {
		changedNames = append(changedNames, name)
	}
	if len(changedNames) > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE proxy_running_slots
			SET proxy_id=NULL, ready_after=NULL, updated_at=NOW()
			WHERE slot_name = ANY($1::text[])
		`, changedNames); err != nil {
			return reconcileSummary{}, fmt.Errorf("release changed slot assignments: %w", err)
		}
	}

	for _, slot := range slots {
		desired := plan.BySlot[slot.Name]
		changed := assignmentChanges[slot.Name]
		oldProxyUser := ""
		if changed {
			if _, err := tx.Exec(ctx, `DELETE FROM pool_proxies WHERE pool_id=$1`, slot.PoolID); err != nil {
				return reconcileSummary{}, fmt.Errorf("clear slot pool %s: %w", slot.Name, err)
			}
			if desired != nil {
				if _, err := tx.Exec(ctx, `
					INSERT INTO pool_proxies (pool_id, proxy_id) VALUES ($1,$2)
					ON CONFLICT DO NOTHING
				`, slot.PoolID, *desired); err != nil {
					return reconcileSummary{}, fmt.Errorf("bind slot pool %s: %w", slot.Name, err)
				}
			}
			if _, err := tx.Exec(ctx, `UPDATE proxy_pools SET updated_at=NOW() WHERE id=$1`, slot.PoolID); err != nil {
				return reconcileSummary{}, fmt.Errorf("touch slot pool %s: %w", slot.Name, err)
			}
			if slotLeaseOwned(slot) && slot.ActiveTaskID == nil {
				rotation, controlState, err :=
					transitionLeaseOwnedIdleSlot(ctx, tx, slot, desired)
				if err != nil {
					return reconcileSummary{}, err
				}
				oldProxyUser = rotation.OldUsername
				slot.ProxyUser = rotation.NewUsername
				slot.AssignmentVersion++
				slot.ProxyID = desired
				slot.ReadyAfter = nil
				slot.ControlState = controlState
			} else {
				if _, err := tx.Exec(ctx, `
					UPDATE proxy_running_slots
					SET proxy_id=$2,
					    assignment_version=assignment_version+1,
					    assigned_at=CASE WHEN $2::int IS NULL THEN NULL ELSE NOW() END,
					    ready_after=NULL,
					    updated_at=NOW()
					WHERE slot_name=$1
				`, slot.Name, desired); err != nil {
					return reconcileSummary{}, fmt.Errorf("persist slot assignment %s: %w", slot.Name, err)
				}
				slot.AssignmentVersion++
				slot.ProxyID = desired
				slot.ReadyAfter = nil
			}
		}
		if desired != nil && slot.ReadyAfter == nil {
			var activationFence *routeActivationFence
			if slot.ControlState == "pending_new_route" && slot.CurrentLeaseID != nil {
				activationFence = &routeActivationFence{
					SlotName:        slot.Name,
					LeaseID:         *slot.CurrentLeaseID,
					ProxyID:         *desired,
					RouteGeneration: slot.AssignmentVersion,
				}
			}
			refreshes = append(refreshes, routeAssignmentRefresh{
				SlotName:          slot.Name,
				OldProxyUser:      oldProxyUser,
				ProxyUser:         slot.ProxyUser,
				ProxyID:           desired,
				AssignmentVersion: slot.AssignmentVersion,
				ControlState:      slot.ControlState,
				ActivationFence:   activationFence,
			})
		} else if changed {
			refreshes = append(refreshes, routeAssignmentRefresh{
				SlotName:          slot.Name,
				OldProxyUser:      oldProxyUser,
				ProxyUser:         slot.ProxyUser,
				AssignmentVersion: slot.AssignmentVersion,
				ControlState:      slot.ControlState,
			})
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return reconcileSummary{}, fmt.Errorf("commit proxy reconciliation: %w", err)
	}
	m.finalizeRouteAssignments(ctx, refreshes)
	eligible := uniqueCandidateCount(candidatesByRole)
	return reconcileSummary{
		Eligible: eligible,
		Running:  eligible - len(plan.Reserve),
		Reserve:  len(plan.Reserve),
		Changes:  len(assignmentChanges),
	}, nil
}

func (m *Manager) loadEligibleCandidatesByRole(
	ctx context.Context,
	tx pgx.Tx,
) (map[string][]candidate, error) {
	result := make(map[string][]candidate, 4)
	for _, role := range []string{RoleDiscover, RoleChannel, RoleQueryQuality, RoleDetail} {
		policy, found := m.policyForRole(role)
		if !found {
			result[role] = []candidate{}
			continue
		}
		items, err := loadEligibleCandidates(ctx, tx, policy)
		if err != nil {
			return nil, fmt.Errorf("load %s policy candidates: %w", role, err)
		}
		result[role] = items
	}
	return result, nil
}

func uniqueCandidateCount(candidatesByRole map[string][]candidate) int {
	seen := make(map[int]bool)
	for _, items := range candidatesByRole {
		for _, item := range items {
			seen[item.ID] = true
		}
	}
	return len(seen)
}

func assignedProxyIDs(ctx context.Context, tx pgx.Tx) (map[int]bool, error) {
	rows, err := tx.Query(ctx, `SELECT proxy_id FROM proxy_running_slots WHERE proxy_id IS NOT NULL`)
	if err != nil {
		return nil, fmt.Errorf("load assigned proxy ids: %w", err)
	}
	defer rows.Close()
	assigned := make(map[int]bool)
	for rows.Next() {
		var proxyID int
		if err := rows.Scan(&proxyID); err != nil {
			return nil, fmt.Errorf("scan assigned proxy id: %w", err)
		}
		assigned[proxyID] = true
	}
	return assigned, rows.Err()
}

func availableSlotRoles(ctx context.Context, tx pgx.Tx) (map[string]bool, error) {
	rows, err := tx.Query(ctx, `SELECT DISTINCT role FROM proxy_running_slots`)
	if err != nil {
		return nil, fmt.Errorf("load available proxy roles: %w", err)
	}
	defer rows.Close()
	roles := make(map[string]bool, 4)
	for rows.Next() {
		var role string
		if err := rows.Scan(&role); err != nil {
			return nil, fmt.Errorf("scan available proxy role: %w", err)
		}
		roles[strings.TrimSpace(role)] = true
	}
	return roles, rows.Err()
}

func loadRunningSlots(ctx context.Context, tx pgx.Tx) ([]runningSlot, error) {
	rows, err := tx.Query(ctx, `
		SELECT s.slot_name, s.role, s.slot_no, s.pool_id, s.user_id, u.username, s.proxy_id,
		       assignment_version, ready_after, worker_id, lease_until, current_lease_id,
		       active_task_id, s.control_state, COALESCE(s.identity_policy_id,'')
		FROM proxy_running_slots s
		JOIN proxy_users u ON u.id=s.user_id
		ORDER BY CASE role
		           WHEN 'discover' THEN 0
		           WHEN 'channel' THEN 1
		           WHEN 'query_quality' THEN 2
		           ELSE 3
		         END,
		         slot_no, slot_name
		FOR UPDATE OF s,u
	`)
	if err != nil {
		return nil, fmt.Errorf("load running slots: %w", err)
	}
	defer rows.Close()
	slots := make([]runningSlot, 0)
	for rows.Next() {
		var slot runningSlot
		if err := rows.Scan(
			&slot.Name, &slot.Role, &slot.Number, &slot.PoolID, &slot.UserID,
			&slot.ProxyUser, &slot.ProxyID,
			&slot.AssignmentVersion, &slot.ReadyAfter, &slot.WorkerID, &slot.LeaseUntil,
			&slot.CurrentLeaseID, &slot.ActiveTaskID, &slot.ControlState, &slot.IdentityPolicyID,
		); err != nil {
			return nil, fmt.Errorf("scan running slot: %w", err)
		}
		slots = append(slots, slot)
	}
	return slots, rows.Err()
}

func slotLeaseOwned(slot runningSlot) bool {
	return slot.WorkerID != nil && slot.CurrentLeaseID != nil && slot.LeaseUntil != nil &&
		slot.LeaseUntil.After(time.Now())
}

func proxyEligibleForRole(proxyID *int, candidates []candidate) bool {
	if proxyID == nil {
		return false
	}
	for _, item := range candidates {
		if item.ID == *proxyID {
			return true
		}
	}
	return false
}

func transitionLeaseOwnedIdleSlot(
	ctx context.Context,
	tx pgx.Tx,
	slot runningSlot,
	desired *int,
) (credentialRotation, string, error) {
	rotation, err := rotateSlotCredential(ctx, tx, slot.Name)
	if err != nil {
		return credentialRotation{}, "", err
	}

	var networkIdentityKey *string
	profileEpoch := int64(0)
	controlState := "paused_no_reserve"
	if desired != nil {
		var key string
		if err := tx.QueryRow(ctx, `
			SELECT network_identity_key FROM proxies WHERE id=$1
		`, *desired).Scan(&key); err != nil {
			return credentialRotation{}, "", fmt.Errorf(
				"load idle replacement network identity for slot %s: %w", slot.Name, err,
			)
		}
		networkIdentityKey = &key
		if err := tx.QueryRow(ctx, `
			INSERT INTO proxy_identity_profile_epochs (
			  identity_policy_id,network_identity_key,profile_epoch,status
			) VALUES ($1,$2,0,'active')
			ON CONFLICT (identity_policy_id,network_identity_key) DO UPDATE
			SET status='active',retired_at=NULL,updated_at=NOW()
			RETURNING profile_epoch
		`, slot.IdentityPolicyID, key).Scan(&profileEpoch); err != nil {
			return credentialRotation{}, "", fmt.Errorf(
				"prepare idle replacement identity for slot %s: %w", slot.Name, err,
			)
		}
		controlState = "pending_new_route"
	}

	tag, err := tx.Exec(ctx, `
		UPDATE proxy_running_slots
		SET proxy_id=$2,assignment_version=assignment_version+1,
		    assigned_at=CASE WHEN $2::int IS NULL THEN NULL ELSE NOW() END,
		    ready_after=NULL,network_identity_key=$3,profile_epoch=$4,
		    pending_action=NULL,pending_incident_id=NULL,control_state=$5,
		    rotation_deadline_at=NULL,
		    route_activation_old_username=CASE
		      WHEN $2::int IS NULL THEN NULL ELSE NULLIF($8,'')
			    END,
			    route_activation_claim_id=NULL,route_activation_claim_until=NULL,
			    route_activation_previous_claim_id=NULL,
			    updated_at=NOW()
		WHERE slot_name=$1 AND current_lease_id=$6 AND active_task_id IS NULL
		  AND assignment_version=$7
	`, slot.Name, desired, networkIdentityKey, profileEpoch, controlState,
		*slot.CurrentLeaseID, slot.AssignmentVersion, rotation.OldUsername)
	if err != nil {
		return credentialRotation{}, "", fmt.Errorf(
			"transition lease-owned idle slot %s: %w", slot.Name, err,
		)
	}
	if tag.RowsAffected() != 1 {
		return credentialRotation{}, "", ErrLeaseConflict
	}
	return rotation, controlState, nil
}

func loadEligibleCandidates(
	ctx context.Context,
	tx pgx.Tx,
	policy IdentityPolicy,
) ([]candidate, error) {
	return queryEligibleCandidates(ctx, tx, policy, true)
}

type candidateQueryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func queryEligibleCandidates(
	ctx context.Context,
	query candidateQueryer,
	policy IdentityPolicy,
	lock bool,
) ([]candidate, error) {
	allowedTags := append([]string(nil), policy.AllowedProxyTags...)
	if allowedTags == nil {
		allowedTags = []string{}
	}
	statement := `
		SELECT id, protocol, COALESCE(tags,'{}'), COALESCE(avg_response_time,0),
		       youtube_avg_response_time, COALESCE(youtube_failure_score,0),
		       COALESCE(network_identity_key,''),
		       CASE
		         WHEN $1='' THEN 0
		         WHEN UPPER(COALESCE(country_code,''))=$1 AND (
		           $3::bigint <= 0
		           OR country_verified_at >= NOW()-($3::bigint * interval '1 millisecond')
		         ) THEN 0
		         WHEN UPPER(COALESCE(country_code,''))=$1 THEN 1
		         WHEN COALESCE(country_code,'')='' THEN 2
		         ELSE 3
		       END::bigint AS country_preference,
		       CASE
		         WHEN $2::text[]='{}'::text[] OR COALESCE(tags,'{}'::text[]) @> $2::text[] THEN 0
		         WHEN COALESCE(tags,'{}'::text[]) && $2::text[] THEN 1
		         WHEN COALESCE(tags,'{}'::text[])='{}'::text[] THEN 2
		         ELSE 3
		       END::bigint AS tag_preference,
		       CASE
		         WHEN egress_identity_mode='static' THEN 0
		         WHEN egress_identity_mode='provider_sticky_session'
		              AND sticky_session_key_encrypted IS NOT NULL THEN 0
		         WHEN COALESCE(egress_identity_mode,'')='' THEN 1
		         ELSE 2
		       END::bigint AS identity_preference,
		       CASE
		         WHEN $4::bigint <= 0 OR identity_valid_until >= NOW()+($4::bigint * interval '1 millisecond') THEN 0
		         WHEN identity_valid_until IS NULL THEN 1
		         ELSE 2
		       END::bigint AS validity_preference
		FROM proxies
		WHERE status='active'
		  AND revalidation_required=false
		  AND (cooldown_until IS NULL OR cooldown_until <= NOW())
		  AND (
		    (base_health_status='passed' AND youtube_health_status='passed')
		    OR (last_youtube_status=200 AND last_rota_youtube_status=200)
		  )
		ORDER BY id
	`
	if lock {
		statement += " FOR UPDATE"
	}
	rows, err := query.Query(ctx, statement, policy.RequiredEgressCountry, allowedTags,
		policy.GeoFreshnessWindow.Milliseconds(), policy.AttemptSafetyWindow.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("load eligible proxy candidates: %w", err)
	}
	defer rows.Close()
	items := make([]candidate, 0)
	for rows.Next() {
		var item candidate
		if err := rows.Scan(
			&item.ID, &item.Protocol, &item.Tags, &item.AverageResponseTime,
			&item.YouTubeResponseTime, &item.YouTubeFailureScore, &item.NetworkIdentityKey,
			&item.CountryPreference, &item.TagPreference, &item.IdentityPreference,
			&item.ValidityPreference,
		); err != nil {
			return nil, fmt.Errorf("scan eligible proxy candidate: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	slices.SortFunc(items, compareCandidates)
	return items, nil
}

func loadPoolMembers(ctx context.Context, tx pgx.Tx, slots []runningSlot) (map[int][]int, error) {
	poolIDs := make([]int, 0, len(slots))
	for _, slot := range slots {
		poolIDs = append(poolIDs, slot.PoolID)
	}
	rows, err := tx.Query(ctx, `
		SELECT pool_id, proxy_id FROM pool_proxies
		WHERE pool_id = ANY($1::int[])
		ORDER BY pool_id, proxy_id
	`, poolIDs)
	if err != nil {
		return nil, fmt.Errorf("load slot pool memberships: %w", err)
	}
	defer rows.Close()
	members := make(map[int][]int, len(slots))
	for rows.Next() {
		var poolID, proxyID int
		if err := rows.Scan(&poolID, &proxyID); err != nil {
			return nil, fmt.Errorf("scan slot pool membership: %w", err)
		}
		members[poolID] = append(members[poolID], proxyID)
	}
	return members, rows.Err()
}

func exactPoolMembership(actual []int, desired *int) bool {
	if desired == nil {
		return len(actual) == 0
	}
	return len(actual) == 1 && actual[0] == *desired
}

func equalOptionalInt(left, right *int) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

func (m *Manager) finalizeRouteAssignments(
	ctx context.Context,
	assignments []routeAssignmentRefresh,
) {
	if len(assignments) == 0 {
		return
	}
	slices.SortFunc(assignments, func(left, right routeAssignmentRefresh) int {
		if left.ProxyUser < right.ProxyUser {
			return -1
		}
		if left.ProxyUser > right.ProxyUser {
			return 1
		}
		return 0
	})
	assignments = slices.CompactFunc(assignments, func(left, right routeAssignmentRefresh) bool {
		return left.SlotName == right.SlotName &&
			left.OldProxyUser == right.OldProxyUser &&
			left.ProxyUser == right.ProxyUser &&
			left.AssignmentVersion == right.AssignmentVersion &&
			equalOptionalInt(left.ProxyID, right.ProxyID) &&
			left.ControlState == right.ControlState &&
			equalRouteActivationFence(left.ActivationFence, right.ActivationFence)
	})
	for _, assignment := range assignments {
		if assignment.ProxyID == nil {
			if assignment.OldProxyUser != "" {
				m.retireUser(ctx, assignment.OldProxyUser)
			}
			m.invalidateUser(assignment.ProxyUser)
			continue
		}
		if assignment.ControlState == "pending_new_route" {
			if assignment.ActivationFence == nil {
				m.logError(
					"pending Route activation is missing its Fence",
					ErrLeaseConflict,
					"slot", assignment.SlotName,
				)
				continue
			}
			m.activatePendingRoute(ctx, *assignment.ActivationFence)
			continue
		}
		if !m.invalidateUser(assignment.ProxyUser) {
			continue
		}
		if _, err := m.db.Pool.Exec(ctx, `
			UPDATE proxy_running_slots
			SET ready_after=NOW(), updated_at=NOW()
			WHERE user_id=(SELECT id FROM proxy_users WHERE username=$1)
			  AND proxy_id=$2 AND assignment_version=$3
		`, assignment.ProxyUser, *assignment.ProxyID, assignment.AssignmentVersion); err != nil {
			m.logError("mark Route assignment ready failed", err, "proxy_user", assignment.ProxyUser)
		}
	}
}

func equalRouteActivationFence(left, right *routeActivationFence) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
