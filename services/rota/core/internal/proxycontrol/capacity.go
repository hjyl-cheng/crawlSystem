package proxycontrol

import (
	"context"
	"fmt"
)

func (m *Manager) Capacity(ctx context.Context) (Capacity, error) {
	if err := m.requireEnabled(); err != nil {
		return Capacity{}, err
	}
	roles := map[string]RoleCapacity{
		RoleDiscover:     {Desired: m.options.DiscoverSlots},
		RoleChannel:      {Desired: m.options.ChannelSlots},
		RoleQueryQuality: {Desired: m.options.QueryQualitySlots},
		RoleDetail:       {Desired: m.options.DetailSlots},
	}
	eligibleByRole := make(map[string][]candidate, len(roles))
	eligibleGlobal := make(map[int]bool)
	for _, role := range []string{RoleDiscover, RoleChannel, RoleQueryQuality, RoleDetail} {
		value := roles[role]
		policy, found := m.policyForRole(role)
		if !found {
			roles[role] = value
			eligibleByRole[role] = []candidate{}
			continue
		}
		items, err := queryEligibleCandidates(
			ctx, m.db.Pool, policy, m.options.WorkloadScope, false,
		)
		if err != nil {
			return Capacity{}, fmt.Errorf("load %s policy capacity candidates: %w", role, err)
		}
		value.IdentityPolicyID = policy.ID
		value.IdentityPolicyVersion = policy.Version
		value.IdentityPolicyHash = policy.Hash
		value.Eligible = len(items)
		eligibleByRole[role] = items
		for _, item := range items {
			eligibleGlobal[item.ID] = true
		}
		roles[role] = value
	}

	bound, err := m.loadBoundProxyIDs(ctx)
	if err != nil {
		return Capacity{}, err
	}
	for _, role := range []string{RoleDiscover, RoleChannel, RoleQueryQuality, RoleDetail} {
		value := roles[role]
		eligibleIDs := make([]int, 0, len(eligibleByRole[role]))
		for _, item := range eligibleByRole[role] {
			eligibleIDs = append(eligibleIDs, item.ID)
			if !bound[item.ID] {
				value.Reserve++
			}
		}
		if err := m.db.Pool.QueryRow(ctx, `
			SELECT count(*)::int,
			       count(*) FILTER (WHERE s.proxy_id=ANY($2::int[]))::int,
			       count(*) FILTER (WHERE
			         s.proxy_id=ANY($2::int[]) AND s.ready_after <= NOW()
			       )::int,
			       count(*) FILTER (WHERE
			         s.worker_id IS NOT NULL AND s.current_lease_id IS NOT NULL
			         AND s.lease_until > NOW()
			         AND s.identity_policy_id=$3 AND s.identity_policy_version=$4
			         AND s.identity_policy_hash=$5
			       )::int
			FROM proxy_running_slots s
			WHERE s.role=$1
		`, role, eligibleIDs, value.IdentityPolicyID, value.IdentityPolicyVersion,
			value.IdentityPolicyHash).Scan(
			&value.Provisioned, &value.Assigned, &value.Ready, &value.Claimed,
		); err != nil {
			return Capacity{}, fmt.Errorf("load %s role capacity: %w", role, err)
		}
		roles[role] = value
	}

	capacity := Capacity{
		OK:             true,
		WorkloadScope:  m.options.WorkloadScope,
		CatalogVersion: m.options.CatalogVersion,
		CatalogDigest:  m.options.CatalogDigest,
		Active:         len(eligibleGlobal),
		Roles:          roles,
	}
	if err := m.db.Pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE cooldown_until > NOW())::int,
		       count(*) FILTER (WHERE status <> 'archived')::int,
		       count(*) FILTER (WHERE status = 'archived')::int
		FROM proxies
	`).Scan(&capacity.Cooldown, &capacity.Total, &capacity.Archived); err != nil {
		return Capacity{}, fmt.Errorf("load proxy inventory capacity: %w", err)
	}
	for proxyID := range eligibleGlobal {
		if bound[proxyID] {
			capacity.Running++
		} else {
			capacity.Reserve++
		}
	}
	totalDesired := 0
	for _, value := range roles {
		totalDesired += value.Desired
	}
	capacity.MinimumReserve = minimumReserveWatermark(
		totalDesired,
		m.options.MinReservePercent,
		m.options.MinReserveCount,
	)
	capacity.ReserveBelowMinimum = capacity.Reserve < capacity.MinimumReserve
	return capacity, nil
}

func (m *Manager) loadBoundProxyIDs(ctx context.Context) (map[int]bool, error) {
	rows, err := m.db.Pool.Query(ctx, `
		SELECT proxy_id FROM proxy_running_slots WHERE proxy_id IS NOT NULL
	`)
	if err != nil {
		return nil, fmt.Errorf("load bound proxy capacity: %w", err)
	}
	defer rows.Close()
	result := make(map[int]bool)
	for rows.Next() {
		var proxyID int
		if err := rows.Scan(&proxyID); err != nil {
			return nil, fmt.Errorf("scan bound proxy capacity: %w", err)
		}
		result[proxyID] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
