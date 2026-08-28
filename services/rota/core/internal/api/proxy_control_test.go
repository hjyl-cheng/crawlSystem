package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alpkeskin/rota/core/internal/proxycontrol"
	"github.com/go-chi/chi/v5"
)

type proxyControlStub struct {
	claim       proxycontrol.ClaimRequest
	renew       proxycontrol.RenewRequest
	beginTask   proxycontrol.BeginTaskRequest
	observation proxycontrol.ObserveRequest
	completion  proxycontrol.CompleteTaskRequest
	release     proxycontrol.ReleaseRequest
	completeErr error
	capacity    proxycontrol.Capacity
	budget      proxycontrol.BusinessRunBudget
}

func (*proxyControlStub) Run(context.Context) {}

func (s *proxyControlStub) Claim(_ context.Context, request proxycontrol.ClaimRequest) (proxycontrol.Assignment, error) {
	s.claim = request
	return proxycontrol.Assignment{OK: true, Role: request.Role, WorkerID: request.WorkerID}, nil
}

func (s *proxyControlStub) Renew(_ context.Context, request proxycontrol.RenewRequest) (proxycontrol.Assignment, error) {
	s.renew = request
	return proxycontrol.Assignment{}, nil
}

func (s *proxyControlStub) BeginTask(_ context.Context, request proxycontrol.BeginTaskRequest) (proxycontrol.Task, error) {
	s.beginTask = request
	return proxycontrol.Task{
		OK:               true,
		TaskID:           "task-1",
		AttemptRequestID: request.AttemptRequestID,
		BusinessRunID:    request.BusinessRunID,
		JobExecutionID:   request.JobExecutionID,
		AttemptNumber:    1,
		SlotName:         request.SlotName,
		RouteGeneration:  request.RouteGeneration,
		TaskKind:         request.TaskKind,
	}, nil
}

func (s *proxyControlStub) Observe(_ context.Context, request proxycontrol.ObserveRequest) (proxycontrol.ObservationResult, error) {
	s.observation = request
	return proxycontrol.ObservationResult{
		OK: true, ObservationID: request.ObservationID, TaskID: request.TaskID,
		Action: proxycontrol.PendingActionNone,
	}, nil
}

func (s *proxyControlStub) CompleteTask(_ context.Context, request proxycontrol.CompleteTaskRequest) (proxycontrol.CompleteTaskResult, error) {
	s.completion = request
	if s.completeErr != nil {
		return proxycontrol.CompleteTaskResult{}, s.completeErr
	}
	return proxycontrol.CompleteTaskResult{
		OK: true, TaskCompleted: true,
		CompletionRequestID:          request.CompletionRequestID,
		TaskID:                       request.TaskID,
		SlotName:                     request.SlotName,
		LeaseID:                      request.LeaseID,
		ControlState:                 proxycontrol.CompletionReadyKeepRoute,
		Ready:                        true,
		CompletedTaskRouteGeneration: request.RouteGeneration,
	}, nil
}

func (*proxyControlStub) Report(context.Context, proxycontrol.ReportRequest) (proxycontrol.ReportResult, error) {
	return proxycontrol.ReportResult{}, nil
}

func (s *proxyControlStub) Release(_ context.Context, request proxycontrol.ReleaseRequest) (proxycontrol.ReleaseResult, error) {
	s.release = request
	return proxycontrol.ReleaseResult{}, nil
}

func (s *proxyControlStub) Capacity(context.Context) (proxycontrol.Capacity, error) {
	if s.capacity.OK {
		return s.capacity, nil
	}
	return proxycontrol.Capacity{OK: true}, nil
}

func (s *proxyControlStub) BusinessRunBudget(_ context.Context, businessRunID string) (proxycontrol.BusinessRunBudget, error) {
	if s.budget.BusinessRunID != "" {
		return s.budget, nil
	}
	return proxycontrol.BusinessRunBudget{
		OK: true, BusinessRunID: businessRunID,
	}, nil
}

func TestProxyControlTokenMiddlewareRequiresExactBearerToken(t *testing.T) {
	handler := ProxyControlTokenMiddleware("control-token-secret")(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}),
	)
	for _, test := range []struct {
		name   string
		token  string
		status int
	}{
		{name: "missing", status: http.StatusUnauthorized},
		{name: "wrong", token: "control-token-secrex", status: http.StatusUnauthorized},
		{name: "valid", token: "control-token-secret", status: http.StatusNoContent},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/api/v1/proxy-control/capacity", nil)
			if test.token != "" {
				request.Header.Set("Authorization", "Bearer "+test.token)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d", response.Code, test.status)
			}
		})
	}
}

func TestProxyControlCapacityPreservesPolicyEligibilityContract(t *testing.T) {
	want := proxycontrol.Capacity{
		OK: true, WorkloadScope: "qy-production", CatalogVersion: 7,
		CatalogDigest: "sha256:catalog-v7", Active: 8, Running: 3, Reserve: 5,
		Roles: map[string]proxycontrol.RoleCapacity{
			proxycontrol.RoleChannel: {
				IdentityPolicyID: "qy-br-channel-anonymous-v1", IdentityPolicyVersion: 1,
				IdentityPolicyHash: "sha256:channel-v1", Desired: 20,
				Provisioned: 20, Eligible: 8, Assigned: 3, Ready: 3, Claimed: 2, Reserve: 5,
			},
		},
	}
	handler := NewProxyControlHandler(&proxyControlStub{capacity: want})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/proxy-control/capacity", nil)
	response := httptest.NewRecorder()
	handler.Capacity(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var got proxycontrol.Capacity
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode capacity response: %v", err)
	}
	role := got.Roles[proxycontrol.RoleChannel]
	if got.WorkloadScope != want.WorkloadScope || got.CatalogVersion != want.CatalogVersion ||
		got.CatalogDigest != want.CatalogDigest || role.IdentityPolicyID != "qy-br-channel-anonymous-v1" ||
		role.IdentityPolicyVersion != 1 || role.IdentityPolicyHash != "sha256:channel-v1" ||
		role.Eligible != 8 || role.Reserve != 5 {
		t.Fatalf("capacity response lost scheduling identity: %+v", got)
	}
}

func TestProxyControlHandlerDecodesClaim(t *testing.T) {
	control := &proxyControlStub{}
	handler := NewProxyControlHandler(control)
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/proxy-control/claim",
		strings.NewReader(`{"role":"channel","worker_id":"worker-1"}`),
	)
	response := httptest.NewRecorder()
	handler.Claim(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if control.claim.Role != "channel" || control.claim.WorkerID != "worker-1" {
		t.Fatalf("claim = %#v", control.claim)
	}
}

func TestProxyControlAssignmentSerializesInitialProfileEpochZero(t *testing.T) {
	response := httptest.NewRecorder()
	writeControlJSON(response, http.StatusOK, proxycontrol.Assignment{
		OK:           true,
		Ready:        true,
		ProfileEpoch: 0,
	})

	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	value, present := payload["profile_epoch"]
	if !present {
		t.Fatalf("profile_epoch missing from initial Assignment JSON: %s", response.Body.String())
	}
	if value != float64(0) {
		t.Fatalf("profile_epoch = %#v, want 0", value)
	}
}

func TestProxyControlHandlerMapsLeaseConflict(t *testing.T) {
	response := httptest.NewRecorder()
	writeControlError(response, proxycontrol.ErrLeaseConflict)
	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusConflict)
	}
}

func TestProxyControlHandlerMapsRouteNotReady(t *testing.T) {
	response := httptest.NewRecorder()
	writeControlError(response, proxycontrol.ErrRouteNotReady)
	if response.Code != http.StatusConflict ||
		!strings.Contains(response.Body.String(), "ROUTE_NOT_READY") {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestProxyControlHandlerMapsMissingBusinessRun(t *testing.T) {
	response := httptest.NewRecorder()
	writeControlError(response, proxycontrol.ErrBusinessRunNotFound)
	if response.Code != http.StatusNotFound ||
		!strings.Contains(response.Body.String(), "BUSINESS_RUN_NOT_FOUND") {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestProxyControlHandlerReturnsAuthoritativeBusinessRunBudget(t *testing.T) {
	want := proxycontrol.BusinessRunBudget{
		OK:                  true,
		WorkloadScope:       "qy-test",
		BusinessRunID:       "run-budget-1",
		BusinessTasksUsed:   9,
		BusinessTasksLimit:  9,
		CurrentExecutionID:  "exec:v1:test",
		ExecutionTasksUsed:  3,
		ExecutionTasksLimit: 3,
	}
	handler := NewProxyControlHandler(&proxyControlStub{budget: want})
	router := chi.NewRouter()
	router.Get("/business-runs/{businessRunID}/budget", handler.BusinessRunBudget)
	request := httptest.NewRequest(
		http.MethodGet,
		"/business-runs/run-budget-1/budget",
		nil,
	)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var got proxycontrol.BusinessRunBudget
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode Business Run budget: %v", err)
	}
	if got != want {
		t.Fatalf("Business Run budget = %+v, want %+v", got, want)
	}
}

func TestProxyControlHandlerDecodesFencedRenew(t *testing.T) {
	control := &proxyControlStub{}
	handler := NewProxyControlHandler(control)
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/proxy-control/renew",
		strings.NewReader(`{
			"renew_request_id":"renew-1",
			"slot_name":"bullmq-channel-01",
			"worker_id":"worker-1",
			"worker_instance_id":"instance-1",
			"lease_id":"lease-1",
			"known_route_generation":8
		}`),
	)
	response := httptest.NewRecorder()
	handler.Renew(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if control.renew.RenewRequestID != "renew-1" ||
		control.renew.SlotName != "bullmq-channel-01" ||
		control.renew.WorkerInstanceID != "instance-1" ||
		control.renew.KnownRouteGeneration != 8 {
		t.Fatalf("renew = %#v", control.renew)
	}
}

func TestProxyControlHandlerRejectsLegacyRenewShape(t *testing.T) {
	control := &proxyControlStub{}
	handler := NewProxyControlHandler(control)
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/proxy-control/renew",
		strings.NewReader(`{
			"worker_id":"worker-1",
			"lease_id":"lease-1",
			"assignment_version":8
		}`),
	)
	response := httptest.NewRecorder()
	handler.Renew(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestProxyControlHandlerDecodesFencedRelease(t *testing.T) {
	control := &proxyControlStub{}
	handler := NewProxyControlHandler(control)
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/proxy-control/release",
		strings.NewReader(`{
			"release_request_id":"release-1",
			"slot_name":"bullmq-channel-01",
			"worker_id":"worker-1",
			"worker_instance_id":"instance-1",
			"lease_id":"lease-1",
			"known_route_generation":8,
			"reason":"worker_shutdown"
		}`),
	)
	response := httptest.NewRecorder()
	handler.Release(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if control.release.ReleaseRequestID != "release-1" ||
		control.release.SlotName != "bullmq-channel-01" ||
		control.release.WorkerInstanceID != "instance-1" ||
		control.release.KnownRouteGeneration != 8 ||
		control.release.Reason != "worker_shutdown" {
		t.Fatalf("release = %#v", control.release)
	}
}

func TestProxyControlHandlerRejectsLegacyReleaseShape(t *testing.T) {
	control := &proxyControlStub{}
	handler := NewProxyControlHandler(control)
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/proxy-control/release",
		strings.NewReader(`{
			"worker_id":"worker-1",
			"lease_id":"lease-1",
			"assignment_version":8
		}`),
	)
	response := httptest.NewRecorder()
	handler.Release(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestProxyControlHandlerDecodesBeginTask(t *testing.T) {
	control := &proxyControlStub{}
	handler := NewProxyControlHandler(control)
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/proxy-control/tasks/begin",
		strings.NewReader(`{
			"slot_name":"bullmq-channel-01",
			"worker_id":"worker-1",
			"worker_instance_id":"instance-1",
			"lease_id":"lease-1",
			"route_generation":8,
			"attempt_request_id":"attempt-1",
			"business_run_id":"channel-run-1",
			"job_execution_id":"youtube-channel-crawl:channel-1:1",
			"task_kind":"channel_full"
		}`),
	)
	response := httptest.NewRecorder()
	handler.BeginTask(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if control.beginTask.JobExecutionID != "youtube-channel-crawl:channel-1:1" ||
		control.beginTask.RouteGeneration != 8 ||
		control.beginTask.WorkerInstanceID != "instance-1" {
		t.Fatalf("begin task = %#v", control.beginTask)
	}
}

func TestProxyControlHandlerMapsExecutionBudgetToConflictCode(t *testing.T) {
	response := httptest.NewRecorder()
	writeControlError(response, proxycontrol.ErrExecutionBudget)
	if response.Code != http.StatusConflict ||
		!strings.Contains(response.Body.String(), "EXECUTION_ROUTE_BUDGET_EXHAUSTED") {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestProxyControlHandlerMapsLeaseGone(t *testing.T) {
	response := httptest.NewRecorder()
	writeControlError(response, proxycontrol.ErrLeaseGone)
	if response.Code != http.StatusGone ||
		!strings.Contains(response.Body.String(), "LEASE_GONE") {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestProxyControlHandlerMapsPolicyRejected(t *testing.T) {
	response := httptest.NewRecorder()
	writeControlError(response, proxycontrol.ErrPolicyRejected)
	if response.Code != http.StatusForbidden ||
		!strings.Contains(response.Body.String(), "POLICY_REJECTED") {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestProxyControlHandlerDecodesObserve(t *testing.T) {
	control := &proxyControlStub{}
	handler := NewProxyControlHandler(control)
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/proxy-control/tasks/observe",
		strings.NewReader(`{
			"slot_name":"bullmq-channel-01",
			"worker_id":"worker-1",
			"worker_instance_id":"instance-1",
			"lease_id":"lease-1",
			"route_generation":8,
			"task_id":"task-1",
			"business_run_id":"channel-run-1",
			"observation_id":"task-1:1",
			"kind":"content_terminal",
			"source":"youtubejs_player",
			"http_status":200,
			"occurred_at":"2026-08-13T12:00:00Z",
			"payload":{"access_status":"private"}
		}`),
	)
	response := httptest.NewRecorder()
	handler.Observe(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if control.observation.WorkerInstanceID != "instance-1" ||
		control.observation.TaskID != "task-1" ||
		control.observation.ObservationID != "task-1:1" ||
		control.observation.Kind != proxycontrol.ObservationContentTerminal ||
		control.observation.Payload["access_status"] != "private" {
		t.Fatalf("observation = %#v", control.observation)
	}
}

func TestProxyControlHandlerDecodesCompleteTask(t *testing.T) {
	control := &proxyControlStub{}
	handler := NewProxyControlHandler(control)
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/proxy-control/tasks/complete",
		strings.NewReader(`{
			"completion_request_id":"completion-1",
			"slot_name":"bullmq-channel-01",
			"worker_id":"worker-1",
			"worker_instance_id":"instance-1",
			"lease_id":"lease-1",
			"route_generation":8,
			"task_id":"task-1",
			"business_run_id":"channel-run-1",
			"outcome":"failed",
			"duration_ms":18231,
			"business_complete":false,
			"observation_ids":["task-1:2","task-1:1"],
			"attempt_quiesced":true,
			"active_managed_requests":0
		}`),
	)
	response := httptest.NewRecorder()
	handler.CompleteTask(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if control.completion.CompletionRequestID != "completion-1" ||
		control.completion.WorkerInstanceID != "instance-1" ||
		control.completion.RouteGeneration != 8 ||
		control.completion.Outcome != proxycontrol.TaskOutcomeFailed ||
		control.completion.DurationMS != 18231 ||
		control.completion.BusinessComplete ||
		!control.completion.AttemptQuiesced ||
		control.completion.ActiveManagedRequests != 0 ||
		len(control.completion.ObservationIDs) != 2 {
		t.Fatalf("complete task = %#v", control.completion)
	}
}

func TestProxyControlHandlerMapsObservationReferenceToUnprocessableEntity(t *testing.T) {
	control := &proxyControlStub{completeErr: proxycontrol.ErrObservationReference}
	handler := NewProxyControlHandler(control)
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/proxy-control/tasks/complete",
		strings.NewReader(`{
			"completion_request_id":"completion-invalid-observation",
			"slot_name":"bullmq-channel-01",
			"worker_id":"worker-1",
			"worker_instance_id":"instance-1",
			"lease_id":"lease-1",
			"route_generation":8,
			"task_id":"task-1",
			"business_run_id":"channel-run-1",
			"outcome":"failed",
			"duration_ms":100,
			"business_complete":false,
			"observation_ids":["missing"],
			"attempt_quiesced":true,
			"active_managed_requests":0
		}`),
	)
	response := httptest.NewRecorder()
	handler.CompleteTask(response, request)
	if response.Code != http.StatusUnprocessableEntity ||
		!strings.Contains(response.Body.String(), "OBSERVATION_REFERENCE_INVALID") {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}
