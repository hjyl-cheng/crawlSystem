package proxycontrol

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSatisfiedCapacityDoesNotContendWithResourceSync(t *testing.T) {
	for _, gate := range []string{"database", "process"} {
		t.Run(gate, func(t *testing.T) {
			m, pool := newProxyControlPostgres(t)
			ctx := context.Background()
			if _, err := m.EnsureCapacity(ctx, EnsureCapacityRequest{Role: RoleChannel, MinimumSlots: 3}); err != nil {
				t.Fatal(err)
			}
			var before time.Time
			if err := pool.QueryRow(ctx, `SELECT updated_at FROM proxy_control_capacity_targets`).Scan(&before); err != nil {
				t.Fatal(err)
			}
			bcryptBefore := m.resourceBcryptCount.Load()
			if gate == "database" {
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(ctx)
				if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, controlAdvisoryLock); err != nil {
					t.Fatal(err)
				}
			} else {
				m.resourceSyncMu.Lock()
				defer m.resourceSyncMu.Unlock()
			}
			limited, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			result, err := m.EnsureCapacity(limited, EnsureCapacityRequest{Role: RoleChannel, MinimumSlots: 2})
			if err != nil || !result.OK || result.Provisioned != 3 {
				t.Fatalf("satisfied capacity must succeed while %s lock is held: %+v %v", gate, result, err)
			}
			var after time.Time
			if err = pool.QueryRow(ctx, `SELECT updated_at FROM proxy_control_capacity_targets`).Scan(&after); err != nil {
				t.Fatal(err)
			}
			if !after.Equal(before) || m.resourceBcryptCount.Load() != bcryptBefore {
				t.Fatal("satisfied capacity performed resource preparation or writes")
			}
		})
	}
}

func TestCapacityFastPathRequiresDurableCompleteResources(t *testing.T) {
	for _, damage := range []string{"missing_target", "low_target", "missing_slot", "wrong_name", "missing_pool", "missing_user"} {
		t.Run(damage, func(t *testing.T) {
			m, pool := newProxyControlPostgres(t)
			ctx := context.Background()
			if _, err := m.EnsureCapacity(ctx, EnsureCapacityRequest{Role: RoleChannel, MinimumSlots: 3}); err != nil {
				t.Fatal(err)
			}
			statements := map[string]string{
				"missing_target": `DELETE FROM proxy_control_capacity_targets`,
				"low_target":     `UPDATE proxy_control_capacity_targets SET minimum_slots=1`,
				"missing_slot":   `DELETE FROM proxy_running_slots WHERE slot_no=2`,
				"wrong_name":     `UPDATE proxy_running_slots SET slot_name='unmanaged-02' WHERE slot_no=2`,
				"missing_pool":   `DELETE FROM proxy_pools WHERE id=(SELECT pool_id FROM proxy_running_slots WHERE slot_no=2)`,
				"missing_user":   `DELETE FROM proxy_users WHERE id=(SELECT user_id FROM proxy_running_slots WHERE slot_no=2)`,
			}
			if _, err := pool.Exec(ctx, statements[damage]); err != nil {
				t.Fatal(err)
			}
			m.resourceSyncMu.Lock()
			defer m.resourceSyncMu.Unlock()
			_, err := m.EnsureCapacity(ctx, EnsureCapacityRequest{Role: RoleChannel, MinimumSlots: 3})
			if !errors.Is(err, ErrResourceSyncDeferred) {
				t.Fatalf("incomplete capacity must require sync: %v", err)
			}
		})
	}
}
