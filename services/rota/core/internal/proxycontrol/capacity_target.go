package proxycontrol

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// EnsureCapacity raises a durable lower bound. It never shrinks the fleet or
// changes a leased Slot's Route. Healthy proxy assignment remains reconciler-owned.
type EnsureCapacityRequest struct {
	Role         string `json:"role"`
	MinimumSlots int    `json:"minimum_slots"`
}

type EnsureCapacityResult struct {
	OK          bool   `json:"ok"`
	Role        string `json:"role"`
	Provisioned int    `json:"provisioned"`
}

func (m *Manager) EnsureCapacity(ctx context.Context, request EnsureCapacityRequest) (EnsureCapacityResult, error) {
	if request.Role != RoleChannel || request.MinimumSlots < 1 || request.MinimumSlots > 500 {
		return EnsureCapacityResult{}, fmt.Errorf("%w: channel capacity must be between 1 and 500", ErrInvalidInput)
	}
	if err := m.requireEnabled(); err != nil {
		return EnsureCapacityResult{}, err
	}
	started := time.Now()
	count, satisfied, err := m.provisionedCapacity(ctx, request.MinimumSlots)
	if err != nil {
		return EnsureCapacityResult{}, err
	}
	if satisfied {
		if m.logger != nil {
			m.logger.Info("proxy control capacity already provisioned", "minimum_slots", request.MinimumSlots, "provisioned", count, "elapsed_ms", time.Since(started).Milliseconds())
		}
		return EnsureCapacityResult{OK: true, Role: RoleChannel, Provisioned: count}, nil
	}
	if err := m.syncResourcesMinimum(ctx, request.MinimumSlots); err != nil {
		return EnsureCapacityResult{}, err
	}
	m.requestReconcile()
	count, satisfied, err = m.provisionedCapacity(ctx, request.MinimumSlots)
	if err != nil {
		return EnsureCapacityResult{}, err
	}
	if !satisfied {
		return EnsureCapacityResult{}, fmt.Errorf("capacity resources are incomplete after synchronization")
	}
	return EnsureCapacityResult{OK: true, Role: RoleChannel, Provisioned: count}, nil
}

// Use one MVCC snapshot: a row count alone cannot prove that the desired
// managed slots exist, or that their lower bound will survive a restart.
// Route readiness and lease ownership are deliberately left to admission.
func (m *Manager) provisionedCapacity(ctx context.Context, minimum int) (int, bool, error) {
	var count int
	var satisfied bool
	err := m.db.Pool.QueryRow(ctx, `
		WITH target AS (
			SELECT COALESCE((SELECT minimum_slots FROM proxy_control_capacity_targets
				WHERE workload_scope=$1 AND role='channel'),0) AS durable
		), wanted AS (
			SELECT role, n, 'bullmq-' || role || '-' ||
				CASE WHEN n < 10 THEN '0' ELSE '' END || n::text AS name
			FROM target CROSS JOIN LATERAL (VALUES
				('channel', GREATEST(durable,$2::int,$3::int)),
				('discover',$4::int),('query_quality',$5::int),('detail',$6::int)
			) roles(role,total) CROSS JOIN LATERAL generate_series(1,total) n
		)
		SELECT (SELECT count(*) FROM proxy_running_slots WHERE role='channel'),
			durable >= $3 AND NOT EXISTS (
				SELECT 1 FROM wanted w
				LEFT JOIN proxy_running_slots s ON s.slot_name=w.name AND s.role=w.role AND s.slot_no=w.n
				LEFT JOIN proxy_pools p ON p.id=s.pool_id AND p.name=s.slot_name
				LEFT JOIN proxy_users u ON u.id=s.user_id AND u.main_pool_id=s.pool_id
				WHERE s.slot_name IS NULL OR p.id IS NULL OR u.id IS NULL
			) FROM target`, m.options.WorkloadScope, m.options.ChannelSlots, minimum,
		m.options.DiscoverSlots, m.options.QueryQualitySlots, m.options.DetailSlots).Scan(&count, &satisfied)
	if err != nil {
		return 0, false, fmt.Errorf("check provisioned capacity: %w", err)
	}
	return count, satisfied, nil
}

func (m *Manager) channelSlotMinimum(ctx context.Context, query interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (int, error) {
	var count int
	err := query.QueryRow(ctx, `SELECT GREATEST($1::int, COALESCE((
		SELECT minimum_slots FROM proxy_control_capacity_targets WHERE workload_scope=$2 AND role='channel'
	),0))`, m.options.ChannelSlots, m.options.WorkloadScope).Scan(&count)
	return count, err
}
