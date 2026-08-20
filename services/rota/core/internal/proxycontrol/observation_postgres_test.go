package proxycontrol

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestObserveChallengeIsIdempotentAndDefersRouteChange(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	firstProxyID := insertControlProxy(t, pool, "observe-current.example:8080", 10)
	insertControlProxy(t, pool, "observe-reserve.example:8080", 20)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}

	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-observe-challenge", "worker-observe-challenge", "instance-observe-challenge",
	))
	if err != nil {
		t.Fatalf("claim proxy: %v", err)
	}
	task, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration:  claim.AssignmentVersion,
		AttemptRequestID: "attempt-observe-challenge",
		BusinessRunID:    "business-observe-challenge",
		JobExecutionID:   "youtube-channel-crawl:observe-challenge:1",
		TaskKind:         TaskKindChannelFull,
	})
	if err != nil {
		t.Fatalf("begin task: %v", err)
	}

	request := ObserveRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration: claim.AssignmentVersion,
		TaskID:          task.TaskID, BusinessRunID: task.BusinessRunID,
		ObservationID: "observation-challenge-1",
		Kind:          ObservationYouTubeChallenge, Source: "youtubejs_player",
		HTTPStatus: intPointer(200),
		OccurredAt: time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC),
		Payload:    map[string]any{"stage": "watch_player", "challenge": "bot_check"},
	}
	first, err := manager.Observe(ctx, request)
	if err != nil {
		t.Fatalf("observe challenge: %v", err)
	}
	replayed, err := manager.Observe(ctx, request)
	if err != nil {
		t.Fatalf("replay challenge: %v", err)
	}
	if first != replayed {
		t.Fatalf("replayed observation = %+v, want %+v", replayed, first)
	}
	if !first.OK || first.ObservationID != request.ObservationID ||
		first.TaskID != task.TaskID || first.Action != PendingActionRotateProfile ||
		first.IncidentID == "" {
		t.Fatalf("observation result = %+v", first)
	}

	var (
		observationCount int
		incidentCount    int
		mappingCount     int
		activeTaskID     string
		pendingAction    string
		pendingIncident  string
		controlState     string
		proxyID          int
		routeGeneration  int64
		proxyCooldown    *time.Time
		failureCount     int64
	)
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM proxy_control_observations
		WHERE workload_scope='qy-test' AND observation_id=$1
	`, request.ObservationID).Scan(&observationCount); err != nil {
		t.Fatalf("count observations: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM proxy_control_reports WHERE incident_id=$1
	`, first.IncidentID).Scan(&incidentCount); err != nil {
		t.Fatalf("count incidents: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM proxy_control_incident_observations
		WHERE workload_scope='qy-test' AND observation_id=$1 AND incident_id=$2
	`, request.ObservationID, first.IncidentID).Scan(&mappingCount); err != nil {
		t.Fatalf("count incident mappings: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT active_task_id,COALESCE(pending_action,''),COALESCE(pending_incident_id,''),
		       control_state,proxy_id,assignment_version
		FROM proxy_running_slots WHERE slot_name=$1
	`, claim.SlotName).Scan(
		&activeTaskID, &pendingAction, &pendingIncident, &controlState,
		&proxyID, &routeGeneration,
	); err != nil {
		t.Fatalf("load observed slot: %v", err)
	}
	if observationCount != 1 || incidentCount != 1 || mappingCount != 1 {
		t.Fatalf("observations=%d incidents=%d mappings=%d", observationCount, incidentCount, mappingCount)
	}
	if activeTaskID != task.TaskID || pendingAction != PendingActionRotateProfile ||
		pendingIncident != first.IncidentID || controlState != "active_task" ||
		proxyID != firstProxyID || routeGeneration != claim.AssignmentVersion {
		t.Fatalf(
			"active_task=%q pending=%q/%q state=%q proxy=%d generation=%d",
			activeTaskID, pendingAction, pendingIncident, controlState, proxyID, routeGeneration,
		)
	}
	if err := pool.QueryRow(ctx, `
		SELECT cooldown_until,youtube_failed_requests FROM proxies WHERE id=$1
	`, firstProxyID).Scan(&proxyCooldown, &failureCount); err != nil {
		t.Fatalf("load canonical proxy health: %v", err)
	}
	if proxyCooldown != nil || failureCount != 0 {
		t.Fatalf("worker observation changed canonical proxy health: cooldown=%v failures=%d", proxyCooldown, failureCount)
	}
}

func TestObserveContentTerminalKeepsRouteAndCreatesNoIncident(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	proxyID := insertControlProxy(t, pool, "observe-content-terminal.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-observe-content-terminal", "worker-observe-content-terminal", "instance-observe-content-terminal",
	))
	if err != nil {
		t.Fatalf("claim proxy: %v", err)
	}
	task, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration:  claim.AssignmentVersion,
		AttemptRequestID: "attempt-observe-content-terminal",
		BusinessRunID:    "business-observe-content-terminal",
		JobExecutionID:   "youtube-channel-crawl:observe-content-terminal:1",
		TaskKind:         TaskKindChannelFull,
	})
	if err != nil {
		t.Fatalf("begin task: %v", err)
	}

	result, err := manager.Observe(ctx, ObserveRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration: claim.AssignmentVersion,
		TaskID:          task.TaskID, BusinessRunID: task.BusinessRunID,
		ObservationID: "observation-content-terminal-1",
		Kind:          ObservationContentTerminal, Source: "youtubejs_player",
		HTTPStatus: intPointer(200),
		OccurredAt: time.Date(2026, 8, 13, 12, 1, 0, 0, time.UTC),
		Payload:    map[string]any{"access_status": "private"},
	})
	if err != nil {
		t.Fatalf("observe content terminal: %v", err)
	}
	if !result.OK || result.Action != PendingActionNone || result.IncidentID != "" {
		t.Fatalf("content terminal result = %+v", result)
	}

	var (
		activeTaskID    string
		pendingAction   string
		pendingIncident string
		currentProxyID  int
		routeGeneration int64
		incidentCount   int
	)
	if err := pool.QueryRow(ctx, `
		SELECT active_task_id,COALESCE(pending_action,''),COALESCE(pending_incident_id,''),
		       proxy_id,assignment_version
		FROM proxy_running_slots WHERE slot_name=$1
	`, claim.SlotName).Scan(
		&activeTaskID, &pendingAction, &pendingIncident, &currentProxyID, &routeGeneration,
	); err != nil {
		t.Fatalf("load content-terminal slot: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM proxy_control_reports`).Scan(&incidentCount); err != nil {
		t.Fatalf("count content-terminal incidents: %v", err)
	}
	if activeTaskID != task.TaskID || pendingAction != "" || pendingIncident != "" ||
		currentProxyID != proxyID || routeGeneration != claim.AssignmentVersion || incidentCount != 0 {
		t.Fatalf(
			"active=%q pending=%q/%q proxy=%d generation=%d incidents=%d",
			activeTaskID, pendingAction, pendingIncident, currentProxyID, routeGeneration, incidentCount,
		)
	}
}

func TestObserveReplaysAfterCompletionButRejectsChangedOrLateObservations(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	insertControlProxy(t, pool, "observe-replay.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-observe-replay", "worker-observe-replay", "instance-observe-replay",
	))
	if err != nil {
		t.Fatalf("claim proxy: %v", err)
	}
	task, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration:  claim.AssignmentVersion,
		AttemptRequestID: "attempt-observe-replay",
		BusinessRunID:    "business-observe-replay",
		JobExecutionID:   "youtube-channel-crawl:observe-replay:1",
		TaskKind:         TaskKindChannelFull,
	})
	if err != nil {
		t.Fatalf("begin task: %v", err)
	}
	request := ObserveRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration: claim.AssignmentVersion,
		TaskID:          task.TaskID, BusinessRunID: task.BusinessRunID,
		ObservationID: "observation-replay-1",
		Kind:          ObservationYouTubeRateLimited, Source: "innertube_comments",
		HTTPStatus: intPointer(429),
		OccurredAt: time.Date(2026, 8, 13, 12, 2, 0, 0, time.UTC),
		Payload:    map[string]any{"stage": "comments"},
	}
	first, err := manager.Observe(ctx, request)
	if err != nil {
		t.Fatalf("observe rate limit: %v", err)
	}

	changed := request
	changed.Payload = map[string]any{"stage": "watch"}
	if _, err := manager.Observe(ctx, changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed observation error = %v, want idempotency conflict", err)
	}

	if _, err := pool.Exec(ctx, `
		WITH completed AS (
		  UPDATE proxy_control_tasks SET status='completed',outcome='failed',completed_at=NOW()
		  WHERE task_id=$1 RETURNING slot_name
		)
		UPDATE proxy_running_slots
		SET active_task_id=NULL,active_task_started_at=NULL,control_state='leased_idle'
		WHERE slot_name=(SELECT slot_name FROM completed)
	`, task.TaskID); err != nil {
		t.Fatalf("complete observation task fixture: %v", err)
	}

	replayed, err := manager.Observe(ctx, request)
	if err != nil {
		t.Fatalf("replay completed observation: %v", err)
	}
	if replayed != first {
		t.Fatalf("completed replay = %+v, want %+v", replayed, first)
	}
	late := request
	late.ObservationID = "observation-replay-late"
	if _, err := manager.Observe(ctx, late); !errors.Is(err, ErrTaskCompleted) {
		t.Fatalf("late observation error = %v, want task completed", err)
	}

	var observationCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM proxy_control_observations WHERE task_id=$1
	`, task.TaskID).Scan(&observationCount); err != nil {
		t.Fatalf("count replay observations: %v", err)
	}
	if observationCount != 1 {
		t.Fatalf("observation count = %d, want 1", observationCount)
	}
}

func TestObserveRejectsStaleTaskFencesWithoutWriting(t *testing.T) {
	manager, pool := newProxyControlPostgres(t)
	ctx := context.Background()

	insertControlProxy(t, pool, "observe-fence.example:8080", 10)
	manager.SetCacheInvalidator(func(string) {})
	if err := manager.syncResources(ctx); err != nil {
		t.Fatalf("sync managed resources: %v", err)
	}
	if _, err := manager.reconcile(ctx); err != nil {
		t.Fatalf("reconcile initial assignments: %v", err)
	}
	claim, err := manager.Claim(ctx, testClaimRequest(
		"claim-observe-fence", "worker-observe-fence", "instance-observe-fence",
	))
	if err != nil {
		t.Fatalf("claim proxy: %v", err)
	}
	task, err := manager.BeginTask(ctx, BeginTaskRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration:  claim.AssignmentVersion,
		AttemptRequestID: "attempt-observe-fence",
		BusinessRunID:    "business-observe-fence",
		JobExecutionID:   "youtube-channel-crawl:observe-fence:1",
		TaskKind:         TaskKindChannelFull,
	})
	if err != nil {
		t.Fatalf("begin task: %v", err)
	}
	base := ObserveRequest{
		SlotName: claim.SlotName, WorkerID: claim.WorkerID,
		WorkerInstanceID: claim.WorkerInstanceID, LeaseID: claim.LeaseID,
		RouteGeneration: claim.AssignmentVersion,
		TaskID:          task.TaskID, BusinessRunID: task.BusinessRunID,
		Kind: ObservationSuccess, Source: "youtubejs_player",
		HTTPStatus: intPointer(200),
		OccurredAt: time.Date(2026, 8, 13, 12, 3, 0, 0, time.UTC),
	}

	tests := []struct {
		name   string
		mutate func(*ObserveRequest)
	}{
		{name: "worker instance", mutate: func(request *ObserveRequest) { request.WorkerInstanceID = "stale-instance" }},
		{name: "lease", mutate: func(request *ObserveRequest) { request.LeaseID = "stale-lease" }},
		{name: "route", mutate: func(request *ObserveRequest) { request.RouteGeneration++ }},
		{name: "business run", mutate: func(request *ObserveRequest) { request.BusinessRunID = "other-business-run" }},
		{name: "task", mutate: func(request *ObserveRequest) { request.TaskID = "other-task" }},
	}
	for index, test := range tests {
		request := base
		request.ObservationID = "observation-fence-" + test.name
		test.mutate(&request)
		if _, err := manager.Observe(ctx, request); !errors.Is(err, ErrTaskConflict) {
			t.Fatalf("%s fence error = %v, want task conflict", test.name, err)
		}
		var count int
		if err := pool.QueryRow(ctx, `
			SELECT COUNT(*) FROM proxy_control_observations WHERE observation_id=$1
		`, request.ObservationID).Scan(&count); err != nil {
			t.Fatalf("count %s fence observations: %v", test.name, err)
		}
		if count != 0 {
			t.Fatalf("%s fence wrote %d observations at index %d", test.name, count, index)
		}
	}
}

func TestObserveRejectsUnboundedOrSensitivePayload(t *testing.T) {
	base := ObserveRequest{
		SlotName: "slot", WorkerID: "worker", WorkerInstanceID: "instance",
		LeaseID: "lease", RouteGeneration: 1, TaskID: "task",
		BusinessRunID: "run", ObservationID: "observation",
		Kind: ObservationSuccess, Source: "youtubejs_player",
		OccurredAt: time.Date(2026, 8, 13, 12, 4, 0, 0, time.UTC),
	}
	for _, test := range []struct {
		name    string
		payload map[string]any
	}{
		{name: "cookie", payload: map[string]any{"cookie": "SID=secret"}},
		{name: "html", payload: map[string]any{"html": "<html>full response</html>"}},
		{name: "oversized sample", payload: map[string]any{"sample": string(make([]byte, maxObservationStringBytes+1))}},
	} {
		request := base
		request.Payload = test.payload
		if _, err := validateObserveRequest(request); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("%s payload error = %v, want invalid input", test.name, err)
		}
	}
}
