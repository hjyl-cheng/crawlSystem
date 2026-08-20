package proxycontrol

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (m *Manager) Swap(ctx context.Context, request SwapRequest) (Assignment, error) {
	if err := m.requireEnabled(); err != nil {
		return Assignment{}, err
	}
	leaseRequest := LeaseRequest{
		WorkerID:          request.WorkerID,
		LeaseID:           request.LeaseID,
		AssignmentVersion: request.AssignmentVersion,
	}
	if err := validateLeaseRequest(leaseRequest); err != nil || request.FailedProxyID <= 0 {
		return Assignment{}, fmt.Errorf("%w: valid lease and failed_proxy_id are required", ErrInvalidInput)
	}

	tx, err := m.db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Assignment{}, fmt.Errorf("begin proxy swap: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, controlAdvisoryLock); err != nil {
		return Assignment{}, fmt.Errorf("lock proxy swap reconciliation: %w", err)
	}
	var slot runningSlot
	err = tx.QueryRow(ctx, `
		SELECT s.slot_name, s.role, s.slot_no, s.pool_id, s.user_id, u.username, s.proxy_id,
		       assignment_version, ready_after, worker_id, lease_until
		FROM proxy_running_slots s
		JOIN proxy_users u ON u.id=s.user_id
		WHERE s.worker_id=$1 AND s.lease_id=$2 AND s.assignment_version=$3
		  AND s.lease_until > NOW()
		FOR UPDATE OF s,u
	`, request.WorkerID, request.LeaseID, request.AssignmentVersion).Scan(
		&slot.Name, &slot.Role, &slot.Number, &slot.PoolID, &slot.UserID,
		&slot.ProxyUser, &slot.ProxyID,
		&slot.AssignmentVersion, &slot.ReadyAfter, &slot.WorkerID, &slot.LeaseUntil,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Assignment{}, ErrLeaseConflict
	}
	if err != nil {
		return Assignment{}, fmt.Errorf("lock proxy swap slot: %w", err)
	}
	if slot.ProxyID == nil || *slot.ProxyID != request.FailedProxyID {
		return Assignment{}, ErrLeaseConflict
	}

	candidates, err := loadEligibleCandidates(ctx, tx, IdentityPolicy{})
	if err != nil {
		return Assignment{}, err
	}
	assigned, err := assignedProxyIDs(ctx, tx)
	if err != nil {
		return Assignment{}, err
	}
	availableRoles, err := availableSlotRoles(ctx, tx)
	if err != nil {
		return Assignment{}, err
	}
	var exactRole, generic *candidate
	for index := range candidates {
		item := &candidates[index]
		if item.ID == request.FailedProxyID || assigned[item.ID] {
			continue
		}
		role := pinnedRole(*item, availableRoles)
		switch {
		case role == slot.Role && exactRole == nil:
			exactRole = item
		case role == "" && generic == nil:
			generic = item
		}
	}
	selected := exactRole
	if selected == nil {
		selected = generic
	}
	if selected == nil {
		leaseUntil := time.Now().Add(m.options.LeaseDuration)
		if _, err := tx.Exec(ctx, `
			UPDATE proxy_running_slots
			SET lease_until=$2, last_heartbeat_at=NOW(), updated_at=NOW()
			WHERE slot_name=$1
		`, slot.Name, leaseUntil); err != nil {
			return Assignment{}, fmt.Errorf("renew proxy lease without reserve: %w", err)
		}
		assignment, err := m.loadAssignment(ctx, tx, slot.Name)
		if err != nil {
			return Assignment{}, err
		}
		assignment.Ready = false
		assignment.Reason = "no_ready_reserve"
		assignment.Replacement = &Replacement{
			FailedProxyID:      request.FailedProxyID,
			FromProxyID:        slot.ProxyID,
			ReplacementProxyID: slot.ProxyID,
		}
		if err := tx.Commit(ctx); err != nil {
			return Assignment{}, fmt.Errorf("commit proxy swap without reserve: %w", err)
		}
		return assignment, nil
	}

	if _, err := tx.Exec(ctx, `DELETE FROM pool_proxies WHERE pool_id=$1`, slot.PoolID); err != nil {
		return Assignment{}, fmt.Errorf("clear swapped proxy pool: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO pool_proxies (pool_id,proxy_id) VALUES ($1,$2)
		ON CONFLICT DO NOTHING
	`, slot.PoolID, selected.ID); err != nil {
		return Assignment{}, fmt.Errorf("bind replacement proxy pool: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE proxy_pools SET updated_at=NOW() WHERE id=$1`, slot.PoolID); err != nil {
		return Assignment{}, fmt.Errorf("touch swapped proxy pool: %w", err)
	}
	newVersion := slot.AssignmentVersion + 1
	credentialRotation, err := rotateSlotCredential(ctx, tx, slot.Name)
	if err != nil {
		return Assignment{}, err
	}
	leaseUntil := time.Now().Add(m.options.LeaseDuration)
	if _, err := tx.Exec(ctx, `
		UPDATE proxy_running_slots
		SET proxy_id=$2, assignment_version=$3, assigned_at=NOW(),
		    ready_after=NULL, lease_until=$4, last_heartbeat_at=NOW(), updated_at=NOW()
		WHERE slot_name=$1
	`, slot.Name, selected.ID, newVersion, leaseUntil); err != nil {
		return Assignment{}, fmt.Errorf("persist proxy swap: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Assignment{}, fmt.Errorf("commit proxy swap: %w", err)
	}

	m.invalidateUser(credentialRotation.OldUsername)
	cacheRefreshed := m.invalidateUser(credentialRotation.NewUsername)
	if cacheRefreshed {
		if _, err := m.db.Pool.Exec(ctx, `
			UPDATE proxy_running_slots
			SET ready_after=NOW(), updated_at=NOW()
			WHERE slot_name=$1 AND proxy_id=$2 AND assignment_version=$3
		`, slot.Name, selected.ID, newVersion); err != nil {
			m.logError("mark swapped proxy ready failed", err, "slot", slot.Name)
			cacheRefreshed = false
		}
	}
	assignment, err := m.loadAssignment(ctx, m.db.Pool, slot.Name)
	if err != nil {
		return Assignment{}, err
	}
	fromProxyID := request.FailedProxyID
	replacementProxyID := selected.ID
	assignment.Replacement = &Replacement{
		Swapped:            true,
		FailedProxyID:      request.FailedProxyID,
		FromProxyID:        &fromProxyID,
		ReplacementProxyID: &replacementProxyID,
		CacheRefreshed:     cacheRefreshed,
	}
	if !cacheRefreshed {
		assignment.Ready = false
		assignment.Reason = "waiting_for_rota_refresh"
	}
	return assignment, nil
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
