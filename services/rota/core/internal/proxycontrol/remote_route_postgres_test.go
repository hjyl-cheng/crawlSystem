package proxycontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func newRemoteRouteFixture(t *testing.T) (*Manager, *pgxpool.Pool, RemoteRouteRequest) {
	t.Helper()
	dsn := os.Getenv("ROTA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("ROTA_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	guard, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	var name string
	err = guard.QueryRow(ctx, "SELECT current_database()").Scan(&name)
	guard.Close()
	if err != nil || name != "remote_node_ingestion_test" {
		t.Fatal("remote route tests require the dedicated remote_node_ingestion_test database")
	}
	m, pool := newProxyControlPostgres(t)
	if _, err := pool.Exec(ctx, `ALTER TABLE proxies ADD COLUMN username TEXT,ADD COLUMN password TEXT`); err != nil {
		t.Fatal(err)
	}
	id := insertControlProxy(t, pool, "assigned.example:8080", 10)
	_ = insertControlProxy(t, pool, "reserve.example:8080", 20)
	if _, err := pool.Exec(ctx, `UPDATE proxies SET username='only-assigned-user',password='assigned-test-secret',country_code='BR' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if err := m.syncResources(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	assignment, err := m.Claim(ctx, testClaimRequest("remote-read-claim", "remote-worker", "remote-instance"))
	if err != nil {
		t.Fatal(err)
	}
	task, err := m.BeginTask(ctx, BeginTaskRequest{SlotName: assignment.SlotName, WorkerID: assignment.WorkerID,
		WorkerInstanceID: assignment.WorkerInstanceID, LeaseID: assignment.LeaseID, RouteGeneration: assignment.AssignmentVersion,
		AttemptRequestID: "remote-attempt", BusinessRunID: "incremental:plan-1", JobExecutionID: "clock:plan-1:1", TaskKind: TaskKindChannelIncremental})
	if err != nil {
		t.Fatal(err)
	}
	return m, pool, RemoteRouteRequest{SlotName: assignment.SlotName, WorkerID: assignment.WorkerID, WorkerInstanceID: assignment.WorkerInstanceID,
		LeaseID: assignment.LeaseID, RouteGeneration: assignment.AssignmentVersion, TaskID: task.TaskID, BusinessRunID: task.BusinessRunID, JobExecutionID: task.JobExecutionID}
}

func TestRemoteRouteReadsOnlyActiveAssignmentWithoutChangingBudget(t *testing.T) {
	m, pool, request := newRemoteRouteFixture(t)
	ctx := context.Background()
	var before string
	const snapshot = `SELECT jsonb_build_object('tasks',(SELECT jsonb_agg(to_jsonb(t)) FROM proxy_control_tasks t),
		'leases',(SELECT jsonb_agg(to_jsonb(l)) FROM proxy_control_leases l),'runs',(SELECT jsonb_agg(to_jsonb(b)) FROM proxy_control_business_runs b),
		'slots',(SELECT jsonb_agg(to_jsonb(s)) FROM proxy_running_slots s),'receipts',(SELECT jsonb_agg(to_jsonb(c)) FROM proxy_control_command_receipts c))::text`
	if err := pool.QueryRow(ctx, snapshot).Scan(&before); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		route, err := m.ReadRemoteRoute(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if !route.OK || route.Upstream.Address != "assigned.example:8080" || route.Upstream.Username != "only-assigned-user" || route.Upstream.Password != "assigned-test-secret" || route.EgressCountry != "BR" {
			t.Fatal("incorrect selected proxy")
		}
		if route.LeaseUntil.Before(route.ServerTime) || route.NetworkIdentityKey == "" {
			t.Fatal("missing route identity/lease")
		}
		data, _ := json.Marshal(route)
		for _, rendered := range []string{string(data), fmt.Sprintf("%+v", route), fmt.Sprintf("%#v", route.Upstream)} {
			if strings.Contains(rendered, "assigned-test-secret") || strings.Contains(rendered, "only-assigned-user") {
				t.Fatal("ordinary serialization/logging leaked credentials")
			}
		}
	}
	var after string
	if err := pool.QueryRow(ctx, snapshot).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("configuration read modified control state or retry budget")
	}
}

func TestRemoteRouteRejectsForeignOwnership(t *testing.T) {
	m, _, request := newRemoteRouteFixture(t)
	for _, mutate := range []func(*RemoteRouteRequest){
		func(r *RemoteRouteRequest) { r.SlotName = "other" }, func(r *RemoteRouteRequest) { r.WorkerID = "other" },
		func(r *RemoteRouteRequest) { r.WorkerInstanceID = "other" }, func(r *RemoteRouteRequest) { r.LeaseID = "other" },
		func(r *RemoteRouteRequest) { r.RouteGeneration++ }, func(r *RemoteRouteRequest) { r.TaskID = "other" },
		func(r *RemoteRouteRequest) { r.BusinessRunID = "other" }, func(r *RemoteRouteRequest) { r.JobExecutionID = "other" },
	} {
		bad := request
		mutate(&bad)
		if _, err := m.ReadRemoteRoute(context.Background(), bad); !errors.Is(err, ErrTaskConflict) {
			t.Fatalf("expected fence rejection, got %v", err)
		}
	}
}

func TestRemoteRouteRejectsExpiredReleasedAndUnboundState(t *testing.T) {
	cases := []struct{ name, sql string }{
		{"lease history expired", `UPDATE proxy_control_leases SET lease_until=NOW()-interval '1 second'`},
		{"slot expired", `UPDATE proxy_running_slots SET lease_until=NOW()-interval '1 second'`},
		{"lease released", `UPDATE proxy_control_leases SET status='released'`},
		{"task completed", `UPDATE proxy_control_tasks SET status='completed'`},
		{"task detached", `UPDATE proxy_running_slots SET active_task_id=NULL`},
		{"route transition", `UPDATE proxy_running_slots SET control_state='pending_new_route'`},
		{"lease rebound", `UPDATE proxy_running_slots SET current_lease_id='other'`},
		{"identity changed", `UPDATE proxies SET network_identity_key='different' WHERE address='assigned.example:8080'`},
		{"identity expired", `UPDATE proxies SET identity_valid_until=NOW()-interval '1 second' WHERE address='assigned.example:8080'`},
		{"policy changed", `UPDATE proxy_control_tasks SET identity_policy_hash='other-policy'`},
		{"wrong workload", `UPDATE proxy_control_leases SET workload_scope='other-workload'`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, pool, request := newRemoteRouteFixture(t)
			if _, err := pool.Exec(context.Background(), tc.sql); err != nil {
				t.Fatal(err)
			}
			if _, err := m.ReadRemoteRoute(context.Background(), request); !errors.Is(err, ErrTaskConflict) {
				t.Fatalf("expected fence rejection, got %v", err)
			}
		})
	}
}

func TestRemoteRouteActiveTaskKeepsAuthorityOverBackgroundHealth(t *testing.T) {
	m, pool, request := newRemoteRouteFixture(t)
	if _, err := pool.Exec(context.Background(), `UPDATE proxies SET status='failed',revalidation_required=true,cooldown_until=NOW()+interval '1 hour' WHERE address='assigned.example:8080'`); err != nil {
		t.Fatal(err)
	}
	// A background health flag alone must not silently rebind a running Task.
	if _, err := m.ReadRemoteRoute(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE proxies SET identity_valid_until=NOW()+interval '20 seconds' WHERE address='assigned.example:8080'`); err != nil {
		t.Fatal(err)
	}
	route, err := m.ReadRemoteRoute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if route.LeaseUntil.Sub(route.ServerTime) > 21*time.Second {
		t.Fatal("route outlives proxy identity")
	}
}
