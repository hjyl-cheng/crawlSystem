package proxycontrol

import (
	"context"
	"fmt"

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
	if err := m.syncResourcesMinimum(ctx, request.MinimumSlots); err != nil {
		return EnsureCapacityResult{}, err
	}
	m.requestReconcile()
	var count int
	if err := m.db.Pool.QueryRow(ctx, `SELECT count(*) FROM proxy_running_slots WHERE role=$1`, RoleChannel).Scan(&count); err != nil {
		return EnsureCapacityResult{}, err
	}
	return EnsureCapacityResult{OK: true, Role: RoleChannel, Provisioned: count}, nil
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
