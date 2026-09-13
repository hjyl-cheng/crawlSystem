package proxycontrol

import (
	"context"
	"errors"
	"github.com/alpkeskin/rota/core/internal/database"
	"sync"
	"testing"
)

func TestOnlineCapacityPreservesActiveRouteAndSurvivesRestart(t *testing.T) {
	m, pool := newProxyControlPostgres(t)
	ctx := context.Background()
	for _, address := range []string{"capacity-a:8080", "capacity-b:8080", "capacity-c:8080", "capacity-d:8080", "capacity-e:8080"} {
		insertControlProxy(t, pool, address, 10)
	}
	if _, err := m.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	lease, err := m.Claim(ctx, testClaimRequest("capacity-claim", "capacity-worker", "capacity-instance"))
	if err != nil || !lease.Ready {
		t.Fatalf("claim: %+v %v", lease, err)
	}
	task, err := m.BeginTask(ctx, BeginTaskRequest{SlotName: lease.SlotName, WorkerID: lease.WorkerID, WorkerInstanceID: lease.WorkerInstanceID, LeaseID: lease.LeaseID, RouteGeneration: lease.AssignmentVersion, AttemptRequestID: "capacity-attempt", BusinessRunID: "capacity-run", JobExecutionID: "youtube-channel-crawl:capacity:1", TaskKind: TaskKindChannelFull})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := func() string {
		var s string
		err := pool.QueryRow(ctx, `SELECT jsonb_build_object('slot',to_jsonb(s)-'updated_at','user',to_jsonb(u)-'updated_at','members',(SELECT jsonb_agg(proxy_id ORDER BY proxy_id) FROM pool_proxies WHERE pool_id=s.pool_id))::text FROM proxy_running_slots s JOIN proxy_users u ON u.id=s.user_id WHERE s.slot_name=$1`, lease.SlotName).Scan(&s)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	before := snapshot()
	result, err := m.EnsureCapacity(ctx, EnsureCapacityRequest{Role: RoleChannel, MinimumSlots: 3})
	if err != nil || result.Provisioned != 3 {
		t.Fatalf("grow: %+v %v", result, err)
	}
	if snapshot() != before {
		t.Fatal("online growth modified the active slot/user/route")
	}
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for _, n := range []int{3, 5, 4} {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, e := m.EnsureCapacity(ctx, EnsureCapacityRequest{Role: RoleChannel, MinimumSlots: n})
			errs <- e
		}(n)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	if snapshot() != before {
		t.Fatal("concurrent growth modified active execution")
	}
	cap, err := m.Capacity(ctx)
	if err != nil || cap.Roles[RoleChannel].Desired != 5 || cap.Roles[RoleChannel].Provisioned != 5 {
		t.Fatalf("capacity: %+v %v", cap, err)
	}
	restarted := New(&database.DB{Pool: pool}, nil, nil, m.options, nil)
	if err := restarted.syncResources(ctx); err != nil {
		t.Fatal(err)
	}
	cap, err = restarted.Capacity(ctx)
	if err != nil || cap.Roles[RoleChannel].Desired != 5 || cap.Roles[RoleChannel].Provisioned != 5 {
		t.Fatalf("restart lost durable target: %+v %v", cap, err)
	}
	var active string
	var generation int64
	if err := pool.QueryRow(ctx, `SELECT active_task_id,assignment_version FROM proxy_running_slots WHERE slot_name=$1`, lease.SlotName).Scan(&active, &generation); err != nil {
		t.Fatal(err)
	}
	if active != task.TaskID || generation != lease.AssignmentVersion {
		t.Fatal("restart resource sync changed active task or route generation")
	}
	if _, err := m.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := m.Claim(ctx, testClaimRequest("capacity-second", "capacity-worker-2", "capacity-instance-2"))
	if err != nil || !second.Ready || second.SlotName == lease.SlotName {
		t.Fatalf("new worker cannot claim added capacity: %+v %v", second, err)
	}
}

func TestCapacityGrowthRollsBackAndRejectsInvalidBounds(t *testing.T) {
	m, pool := newProxyControlPostgres(t)
	ctx := context.Background()
	for _, r := range []EnsureCapacityRequest{{Role: RoleChannel, MinimumSlots: 0}, {Role: RoleChannel, MinimumSlots: 501}, {Role: RoleDiscover, MinimumSlots: 2}} {
		if _, err := m.EnsureCapacity(ctx, r); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("accepted invalid request: %+v %v", r, err)
		}
	}
	_, err := pool.Exec(ctx, `CREATE FUNCTION fail_capacity_fixture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.slot_no=3 THEN RAISE EXCEPTION 'fixture provisioning failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER capacity_fixture BEFORE INSERT ON proxy_running_slots FOR EACH ROW EXECUTE FUNCTION fail_capacity_fixture();`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.EnsureCapacity(ctx, EnsureCapacityRequest{Role: RoleChannel, MinimumSlots: 4}); err == nil {
		t.Fatal("expected provisioning failure")
	}
	var targets, slots int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM proxy_control_capacity_targets),(SELECT count(*) FROM proxy_running_slots)`).Scan(&targets, &slots); err != nil {
		t.Fatal(err)
	}
	if targets != 0 || slots != 1 {
		t.Fatalf("partial capacity persisted: targets=%d slots=%d", targets, slots)
	}
}
