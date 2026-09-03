package repository

import (
	"context"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestClaimDueHealthChecksSkipsOnlyLiveLeasedSlots(t *testing.T) {
	_, pool := newSourceInventoryPostgres(t)
	ctx := context.Background()
	extendLiveLeaseFixture(t, pool)
	if _, err := pool.Exec(ctx, `
		ALTER TABLE proxies
		  ADD COLUMN address TEXT NOT NULL DEFAULT 'fixture.example:443',
		  ADD COLUMN protocol TEXT NOT NULL DEFAULT 'vless',
		  ADD COLUMN username TEXT,
		  ADD COLUMN password TEXT,
		  ADD COLUMN requests BIGINT NOT NULL DEFAULT 0,
		  ADD COLUMN successful_requests BIGINT NOT NULL DEFAULT 0,
		  ADD COLUMN failed_requests BIGINT NOT NULL DEFAULT 0,
		  ADD COLUMN avg_response_time INTEGER NOT NULL DEFAULT 0,
		  ADD COLUMN last_check TIMESTAMPTZ,
		  ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
		INSERT INTO proxies (node_identity,status) VALUES ('preassigned-unleased','active');
		UPDATE proxies
		SET status='active',next_health_check_at=NOW()-INTERVAL '1 minute',
		    base_health_status='passed',youtube_health_status='passed'
		WHERE id IN (1,2,3);
		INSERT INTO proxy_running_slots (
		  slot_name,proxy_id,current_lease_id,lease_until
		) VALUES
		  ('leased-slot',2,'live-lease',NOW()+INTERVAL '1 hour'),
		  ('unleased-slot',3,NULL,NULL);
		INSERT INTO proxy_control_leases (lease_id,slot_name,status,lease_until)
		VALUES ('live-lease','leased-slot','active',NOW()+INTERVAL '1 hour');
	`); err != nil {
		t.Fatalf("prepare due health fixture: %v", err)
	}

	repo := NewProxyRepository(&database.DB{Pool: pool})
	claimed, err := repo.ClaimDueHealthChecks(ctx, 10)
	if err != nil {
		t.Fatalf("claim due health checks: %v", err)
	}
	if got := claimedProxyIDs(claimed); len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("claimed proxy IDs = %v, want unbound 1 and unleased-assigned 3", got)
	}

	var unboundStatus, leasedStatus, unleasedStatus string
	var unboundNext time.Time
	if err := pool.QueryRow(ctx, `
		SELECT status,next_health_check_at FROM proxies WHERE id=1
	`).Scan(&unboundStatus, &unboundNext); err != nil {
		t.Fatalf("load unbound lifecycle: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM proxies WHERE id=2`).Scan(&leasedStatus); err != nil {
		t.Fatalf("load leased lifecycle: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM proxies WHERE id=3`).Scan(&unleasedStatus); err != nil {
		t.Fatalf("load unleased-assigned lifecycle: %v", err)
	}
	if unboundStatus != "idle" || !unboundNext.After(time.Now()) {
		t.Fatalf("claimed unbound lifecycle = status %q next %v", unboundStatus, unboundNext)
	}
	if leasedStatus != "active" {
		t.Fatalf("live leased proxy status = %q, want active", leasedStatus)
	}
	if unleasedStatus != "idle" {
		t.Fatalf("unleased-assigned proxy status = %q, want idle", unleasedStatus)
	}
}

func extendLiveLeaseFixture(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		ALTER TABLE proxy_running_slots
		  ADD COLUMN current_lease_id TEXT,
		  ADD COLUMN lease_until TIMESTAMPTZ;
		CREATE TABLE proxy_control_leases (
		  lease_id TEXT PRIMARY KEY,
		  slot_name TEXT NOT NULL,
		  status TEXT NOT NULL,
		  lease_until TIMESTAMPTZ NOT NULL
		);
	`); err != nil {
		t.Fatalf("extend live Lease fixture: %v", err)
	}
}

func claimedProxyIDs(proxies []*models.Proxy) []int {
	ids := make([]int, 0, len(proxies))
	for _, proxy := range proxies {
		ids = append(ids, proxy.ID)
	}
	return ids
}
