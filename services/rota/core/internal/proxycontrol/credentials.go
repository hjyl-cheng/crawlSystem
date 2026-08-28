package proxycontrol

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type credentialRotation struct {
	SlotName    string
	OldUsername string
	NewUsername string
}

func rotateSlotCredential(
	ctx context.Context,
	tx pgx.Tx,
	slotName string,
) (credentialRotation, error) {
	var (
		userID     int
		generation int64
		oldName    string
	)
	err := tx.QueryRow(ctx, `
		SELECT s.user_id, s.credential_generation, u.username
		FROM proxy_running_slots s
		JOIN proxy_users u ON u.id=s.user_id
		WHERE s.slot_name=$1
		FOR UPDATE OF s,u
	`, slotName).Scan(&userID, &generation, &oldName)
	if err != nil {
		return credentialRotation{}, fmt.Errorf("load slot credential %s: %w", slotName, err)
	}

	generation++
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	newName := fmt.Sprintf("%s-g%d-%s", slotName, generation, suffix)
	if _, err := tx.Exec(ctx, `
		UPDATE proxy_users
		SET username=$2, updated_at=NOW()
		WHERE id=$1
	`, userID, newName); err != nil {
		return credentialRotation{}, fmt.Errorf("rotate slot proxy user %s: %w", slotName, err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE proxy_running_slots
		SET credential_generation=$2, updated_at=NOW()
		WHERE slot_name=$1
	`, slotName, generation); err != nil {
		return credentialRotation{}, fmt.Errorf("persist slot credential generation %s: %w", slotName, err)
	}
	return credentialRotation{
		SlotName:    slotName,
		OldUsername: oldName,
		NewUsername: newName,
	}, nil
}

func (m *Manager) invalidateCredentials(rotations []credentialRotation) {
	seen := make(map[string]struct{}, len(rotations)*2)
	for _, rotation := range rotations {
		for _, username := range []string{rotation.OldUsername, rotation.NewUsername} {
			username = strings.TrimSpace(username)
			if username == "" {
				continue
			}
			if _, found := seen[username]; found {
				continue
			}
			seen[username] = struct{}{}
			m.invalidateUser(username)
		}
	}
}

func (m *Manager) retireExpiredCredentialUsers(
	ctx context.Context,
	rotations []credentialRotation,
) error {
	retireCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		routeActivationAttemptTimeout,
	)
	defer cancel()
	seen := make(map[string]struct{}, len(rotations))
	for _, rotation := range rotations {
		username := strings.TrimSpace(rotation.OldUsername)
		if username == "" {
			continue
		}
		if _, found := seen[username]; found {
			continue
		}
		seen[username] = struct{}{}
		if !m.retireUser(retireCtx, username) {
			return fmt.Errorf("retire expired proxy user %s", username)
		}
	}
	return nil
}

func expireLeases(
	ctx context.Context,
	tx pgx.Tx,
) ([]credentialRotation, error) {
	if err := abandonInvalidActiveTasks(ctx, tx, ""); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT slot_name
		FROM proxy_running_slots
		WHERE worker_id IS NOT NULL AND lease_until <= NOW()
		ORDER BY slot_name
		FOR UPDATE
	`)
	if err != nil {
		return nil, fmt.Errorf("list expired proxy leases: %w", err)
	}
	var slotNames []string
	for rows.Next() {
		var slotName string
		if err := rows.Scan(&slotName); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan expired proxy lease: %w", err)
		}
		slotNames = append(slotNames, slotName)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate expired proxy leases: %w", err)
	}
	rows.Close()

	rotations := make([]credentialRotation, 0, len(slotNames))
	for _, slotName := range slotNames {
		rotation, err := rotateSlotCredential(ctx, tx, slotName)
		if err != nil {
			return nil, err
		}
		rotations = append(rotations, rotation)
	}
	if len(slotNames) > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE proxy_control_leases leases
			SET status='expired', released_at=COALESCE(released_at,NOW()),
			    release_reason=COALESCE(release_reason,'lease_expired'), updated_at=NOW()
			FROM proxy_running_slots slots
			WHERE slots.slot_name = ANY($1::text[])
			  AND leases.lease_id=slots.current_lease_id AND leases.status='active'
		`, slotNames); err != nil {
			return nil, fmt.Errorf("record expired proxy leases: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE proxy_running_slots
			SET worker_id=NULL, worker_instance_id=NULL, lease_id=NULL, current_lease_id=NULL,
			    lease_until=NULL, last_heartbeat_at=NULL, identity_policy_id=NULL,
			    identity_policy_version=NULL, identity_policy_hash=NULL,
			    required_egress_country=NULL, active_task_id=NULL,
			    active_task_started_at=NULL, pending_action=NULL, pending_incident_id=NULL,
			    control_state='unleased', rotation_deadline_at=NULL,
			    route_activation_old_username=NULL,route_activation_claim_id=NULL,
			    route_activation_claim_until=NULL,route_activation_previous_claim_id=NULL,
			    updated_at=NOW()
			WHERE slot_name = ANY($1::text[])
		`, slotNames); err != nil {
			return nil, fmt.Errorf("expire proxy leases: %w", err)
		}
	}
	return rotations, nil
}

func abandonInvalidActiveTasks(ctx context.Context, tx pgx.Tx, slotName string) error {
	if _, err := tx.Exec(ctx, `
		UPDATE proxy_control_tasks tasks
		SET status='abandoned',outcome='cancelled',completed_at=COALESCE(completed_at,NOW()),
		    failed_stage=CASE
		      WHEN slots.lease_until IS NULL OR slots.lease_until <= NOW() THEN 'lease_expired'
		      ELSE 'lease_replaced'
		    END
		FROM proxy_running_slots slots
		WHERE tasks.slot_name=slots.slot_name
		  AND tasks.status='active'
		  AND ($1='' OR tasks.slot_name=$1)
		  AND (
		    slots.lease_until IS NULL OR slots.lease_until <= NOW()
		    OR slots.current_lease_id IS DISTINCT FROM tasks.lease_id
		    OR slots.worker_id IS DISTINCT FROM tasks.worker_id
		    OR slots.worker_instance_id IS DISTINCT FROM tasks.worker_instance_id
		    OR slots.active_task_id IS DISTINCT FROM tasks.task_id
		  )
	`, slotName); err != nil {
		return fmt.Errorf("abandon invalid active proxy tasks: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE proxy_running_slots slots
		SET active_task_id=NULL,active_task_started_at=NULL,
		    control_state=CASE
		      WHEN slots.worker_id IS NOT NULL AND slots.lease_until > NOW() THEN 'leased_idle'
		      ELSE slots.control_state
		    END,
		    updated_at=NOW()
		FROM proxy_control_tasks tasks
		WHERE slots.active_task_id=tasks.task_id
		  AND ($1='' OR slots.slot_name=$1)
		  AND tasks.status='abandoned'
		  AND tasks.failed_stage IN ('lease_expired','lease_replaced')
	`, slotName); err != nil {
		return fmt.Errorf("clear abandoned proxy task slots: %w", err)
	}
	return nil
}
