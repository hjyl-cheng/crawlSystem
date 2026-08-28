package proxycontrol

import (
	"context"
	"fmt"
)

func (m *Manager) publishClaimedRoute(
	ctx context.Context,
	assignment Assignment,
	oldUsername string,
) error {
	if !assignment.Ready {
		return nil
	}

	m.invalidateMu.RLock()
	dataPlane := m.dataPlane
	m.invalidateMu.RUnlock()
	if dataPlane == nil {
		return fmt.Errorf("%w: managed proxy data plane is unavailable", ErrDisabled)
	}
	if assignment.ProxyID == nil || assignment.LeaseID == "" || assignment.ProxyUser == "" {
		return fmt.Errorf("claimed ready Route is missing its publication Fence")
	}

	current, err := m.claimedRouteFenceCurrent(ctx, assignment)
	if err != nil {
		return fmt.Errorf("verify claimed Route before publication: %w", err)
	}
	if !current {
		return fmt.Errorf("%w: claimed Route changed before publication", ErrLeaseGone)
	}

	claimID := "lease:" + assignment.LeaseID
	begin, err := dataPlane.BeginProxyUserActivation(
		ctx,
		oldUsername,
		assignment.ProxyUser,
		*assignment.ProxyID,
		"",
		claimID,
	)
	if err != nil {
		return fmt.Errorf("begin claimed Route publication: %w", err)
	}
	if !begin.AlreadyCommitted {
		if err := dataPlane.CommitProxyUserActivation(ctx, assignment.ProxyUser, claimID); err != nil {
			return fmt.Errorf("commit claimed Route publication: %w", err)
		}
	}

	current, err = m.claimedRouteFenceCurrent(ctx, assignment)
	if err == nil && current {
		return nil
	}
	retireErr := dataPlane.RetireProxyUser(context.WithoutCancel(ctx), assignment.ProxyUser)
	if err != nil {
		if retireErr != nil {
			return fmt.Errorf(
				"verify claimed Route after publication: %w (retire failed: %v)",
				err,
				retireErr,
			)
		}
		return fmt.Errorf("verify claimed Route after publication: %w", err)
	}
	if retireErr != nil {
		return fmt.Errorf("%w: claimed Route changed during publication (retire failed: %v)", ErrLeaseGone, retireErr)
	}
	return fmt.Errorf("%w: claimed Route changed during publication", ErrLeaseGone)
}

func (m *Manager) claimedRouteFenceCurrent(ctx context.Context, assignment Assignment) (bool, error) {
	var current bool
	err := m.db.Pool.QueryRow(ctx, `
		SELECT EXISTS (
		  SELECT 1
		  FROM proxy_running_slots slot
		  JOIN proxy_users proxy_user ON proxy_user.id=slot.user_id
		  WHERE slot.slot_name=$1
		    AND `+expectedLiveLeaseFencePredicate(2, 8)+`
		    AND slot.proxy_id=$3
			    AND slot.assignment_version=$4 AND proxy_user.username=$5
			    AND slot.worker_id=$6 AND slot.worker_instance_id=$7
			    AND (
			      (slot.active_task_id IS NULL AND slot.control_state='leased_idle')
			      OR (slot.active_task_id IS NOT NULL AND slot.control_state='active_task')
			    )
			    AND slot.ready_after IS NOT NULL AND slot.ready_after <= NOW()
		)
	`, assignment.SlotName, assignment.LeaseID, *assignment.ProxyID,
		assignment.AssignmentVersion, assignment.ProxyUser,
		assignment.WorkerID, assignment.WorkerInstanceID,
		m.options.WorkloadScope).Scan(&current)
	return current, err
}
