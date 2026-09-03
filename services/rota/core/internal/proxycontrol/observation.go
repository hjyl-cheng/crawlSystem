package proxycontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	maxObservationPayloadBytes = 4 * 1024
	maxObservationStringBytes  = 512
	maxObservationPayloadDepth = 4
)

var observationSourcePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`)

var forbiddenObservationPayloadKeys = map[string]struct{}{
	"authorization":  {},
	"cookie":         {},
	"cookies":        {},
	"headers":        {},
	"html":           {},
	"proxy_password": {},
	"proxy_url":      {},
	"raw_response":   {},
	"response_body":  {},
	"visitor_data":   {},
}

func (m *Manager) Observe(ctx context.Context, request ObserveRequest) (ObservationResult, error) {
	if err := m.requireEnabled(); err != nil {
		return ObservationResult{}, err
	}
	request = normalizeObserveRequest(request)
	payload, err := validateObserveRequest(request)
	if err != nil {
		return ObservationResult{}, err
	}
	requestHash, err := observeRequestHash(request, payload)
	if err != nil {
		return ObservationResult{}, err
	}

	tx, err := m.db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ObservationResult{}, fmt.Errorf("begin proxy observation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, controlAdvisoryLock); err != nil {
		return ObservationResult{}, fmt.Errorf("lock proxy observation: %w", err)
	}

	replayed, found, err := loadObservation(ctx, tx, m.options.WorkloadScope, request.ObservationID)
	if err != nil {
		return ObservationResult{}, err
	}
	if found {
		if replayed.requestHash != requestHash {
			return ObservationResult{}, fmt.Errorf("%w: observation_id %q", ErrIdempotencyConflict, request.ObservationID)
		}
		if err := tx.Commit(ctx); err != nil {
			return ObservationResult{}, fmt.Errorf("commit replayed proxy observation: %w", err)
		}
		return replayed.ObservationResult, nil
	}

	state, err := lockObservationTask(ctx, tx, m.options.WorkloadScope, request.TaskID)
	if err != nil {
		return ObservationResult{}, err
	}
	if state.status != "active" {
		return ObservationResult{}, ErrTaskCompleted
	}
	if state.slotName != request.SlotName || state.workerID != request.WorkerID ||
		state.workerInstanceID != request.WorkerInstanceID || state.leaseID != request.LeaseID ||
		state.routeGeneration != request.RouteGeneration || state.businessRunID != request.BusinessRunID ||
		state.activeTaskID != request.TaskID {
		return ObservationResult{}, ErrTaskConflict
	}
	if state.currentLeaseID != request.LeaseID || !state.leaseUntil.After(time.Now()) {
		return ObservationResult{}, ErrLeaseGone
	}
	if state.slotRouteGeneration != request.RouteGeneration {
		return ObservationResult{}, ErrLeaseConflict
	}

	action := actionForObservation(request.Kind)
	incidentID := ""
	if action != PendingActionNone {
		incidentID = uuid.NewString()
	}

	var createdAt time.Time
	if err := tx.QueryRow(ctx, `
		INSERT INTO proxy_control_observations (
		  observation_id,workload_scope,request_hash,task_id,slot_name,
		  worker_id,worker_instance_id,lease_id,route_generation,business_run_id,
		  network_identity_key,kind,source,http_status,occurred_at,payload,action,incident_id
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb,$17,NULLIF($18,''))
		RETURNING created_at
	`, request.ObservationID, m.options.WorkloadScope, requestHash, request.TaskID,
		request.SlotName, request.WorkerID, request.WorkerInstanceID, request.LeaseID,
		request.RouteGeneration, request.BusinessRunID, state.networkIdentityKey,
		request.Kind, request.Source, request.HTTPStatus, request.OccurredAt,
		string(payload), action, incidentID).Scan(&createdAt); err != nil {
		return ObservationResult{}, fmt.Errorf("persist proxy observation: %w", err)
	}

	if incidentID != "" {
		if state.proxyID == nil {
			return ObservationResult{}, fmt.Errorf("%w: active task has no proxy route", ErrLeaseConflict)
		}
		incidentPayload, err := json.Marshal(map[string]any{
			"observation_id":  request.ObservationID,
			"task_id":         request.TaskID,
			"business_run_id": request.BusinessRunID,
			"kind":            request.Kind,
			"source":          request.Source,
		})
		if err != nil {
			return ObservationResult{}, fmt.Errorf("encode proxy observation incident: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO proxy_control_reports (
			  incident_id,proxy_id,proxy_user,outcome,payload
			) VALUES ($1,$2,$3,'failure',$4::jsonb)
		`, incidentID, *state.proxyID, state.proxyUser, string(incidentPayload)); err != nil {
			return ObservationResult{}, fmt.Errorf("persist proxy observation incident: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO proxy_control_incident_observations (
			  workload_scope,incident_id,observation_id
			) VALUES ($1,$2,$3)
		`, m.options.WorkloadScope, incidentID, request.ObservationID); err != nil {
			return ObservationResult{}, fmt.Errorf("map proxy observation incident: %w", err)
		}

		incidentResult := ReportResult{
			OK: true, Action: "pending_validation", Confirmed: false,
			ProxyID: state.proxyID, Reason: request.Kind,
		}
		encodedResult, err := json.Marshal(incidentResult)
		if err != nil {
			return ObservationResult{}, fmt.Errorf("encode proxy observation incident result: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE proxy_control_reports SET result=$2::jsonb,completed_at=NOW()
			WHERE incident_id=$1
		`, incidentID, string(encodedResult)); err != nil {
			return ObservationResult{}, fmt.Errorf("complete proxy observation incident: %w", err)
		}

		pendingAction, pendingIncident := mergePendingAction(
			state.pendingAction, state.pendingIncidentID, action, incidentID,
		)
		if _, err := tx.Exec(ctx, `
			UPDATE proxy_running_slots
			SET pending_action=$2,pending_incident_id=$3,updated_at=NOW()
			WHERE slot_name=$1 AND active_task_id=$4 AND assignment_version=$5
		`, request.SlotName, pendingAction, pendingIncident, request.TaskID, request.RouteGeneration); err != nil {
			return ObservationResult{}, fmt.Errorf("defer proxy observation action: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return ObservationResult{}, fmt.Errorf("commit proxy observation: %w", err)
	}
	return ObservationResult{
		OK: true, ObservationID: request.ObservationID, TaskID: request.TaskID,
		Action: action, IncidentID: incidentID, CreatedAt: createdAt,
	}, nil
}

type observationTaskState struct {
	status              string
	slotName            string
	workerID            string
	workerInstanceID    string
	leaseID             string
	routeGeneration     int64
	businessRunID       string
	activeTaskID        string
	currentLeaseID      string
	leaseUntil          time.Time
	slotRouteGeneration int64
	networkIdentityKey  string
	proxyID             *int
	proxyUser           string
	pendingAction       string
	pendingIncidentID   string
}

func lockObservationTask(
	ctx context.Context,
	tx pgx.Tx,
	workloadScope string,
	taskID string,
) (observationTaskState, error) {
	var state observationTaskState
	err := tx.QueryRow(ctx, `
		SELECT t.status,t.slot_name,t.worker_id,t.worker_instance_id,t.lease_id,
		       t.route_generation,t.business_run_id,COALESCE(s.active_task_id,''),
		       COALESCE(s.current_lease_id,''),s.lease_until,s.assignment_version,
		       COALESCE(s.network_identity_key,''),s.proxy_id,u.username,
		       COALESCE(s.pending_action,''),COALESCE(s.pending_incident_id,'')
		FROM proxy_control_tasks t
		JOIN proxy_running_slots s ON s.slot_name=t.slot_name
		JOIN proxy_users u ON u.id=s.user_id
		WHERE t.workload_scope=$1 AND t.task_id=$2
		FOR UPDATE OF t,s,u
	`, workloadScope, taskID).Scan(
		&state.status, &state.slotName, &state.workerID, &state.workerInstanceID,
		&state.leaseID, &state.routeGeneration, &state.businessRunID,
		&state.activeTaskID, &state.currentLeaseID, &state.leaseUntil,
		&state.slotRouteGeneration, &state.networkIdentityKey, &state.proxyID,
		&state.proxyUser, &state.pendingAction, &state.pendingIncidentID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return observationTaskState{}, ErrTaskConflict
	}
	if err != nil {
		return observationTaskState{}, fmt.Errorf("lock proxy observation task: %w", err)
	}
	return state, nil
}

type storedObservation struct {
	ObservationResult
	requestHash string
}

func loadObservation(
	ctx context.Context,
	tx pgx.Tx,
	workloadScope string,
	observationID string,
) (storedObservation, bool, error) {
	var stored storedObservation
	err := tx.QueryRow(ctx, `
		SELECT observation_id,task_id,action,COALESCE(incident_id,''),created_at,request_hash
		FROM proxy_control_observations
		WHERE workload_scope=$1 AND observation_id=$2
		FOR UPDATE
	`, workloadScope, observationID).Scan(
		&stored.ObservationID, &stored.TaskID, &stored.Action, &stored.IncidentID,
		&stored.CreatedAt, &stored.requestHash,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return storedObservation{}, false, nil
	}
	if err != nil {
		return storedObservation{}, false, fmt.Errorf("load proxy observation: %w", err)
	}
	stored.OK = true
	return stored, true, nil
}

func normalizeObserveRequest(request ObserveRequest) ObserveRequest {
	request.SlotName = strings.TrimSpace(request.SlotName)
	request.WorkerID = strings.TrimSpace(request.WorkerID)
	request.WorkerInstanceID = strings.TrimSpace(request.WorkerInstanceID)
	request.LeaseID = strings.TrimSpace(request.LeaseID)
	request.TaskID = strings.TrimSpace(request.TaskID)
	request.BusinessRunID = strings.TrimSpace(request.BusinessRunID)
	request.ObservationID = strings.TrimSpace(request.ObservationID)
	request.Kind = strings.ToLower(strings.TrimSpace(request.Kind))
	request.Source = strings.ToLower(strings.TrimSpace(request.Source))
	if !request.OccurredAt.IsZero() {
		request.OccurredAt = request.OccurredAt.UTC().Truncate(time.Second)
	}
	if request.Payload == nil {
		request.Payload = map[string]any{}
	}
	return request
}

func validateObserveRequest(request ObserveRequest) ([]byte, error) {
	for _, value := range []string{
		request.SlotName, request.WorkerID, request.WorkerInstanceID, request.LeaseID,
		request.TaskID, request.BusinessRunID, request.ObservationID,
	} {
		if value == "" || len(value) > 255 {
			return nil, fmt.Errorf("%w: observation identity fields are required and limited to 255 bytes", ErrInvalidInput)
		}
	}
	if request.RouteGeneration < 0 {
		return nil, fmt.Errorf("%w: route_generation must be non-negative", ErrInvalidInput)
	}
	if !validObservationKind(request.Kind) {
		return nil, fmt.Errorf("%w: unsupported observation kind %q", ErrInvalidInput, request.Kind)
	}
	if !observationSourcePattern.MatchString(request.Source) {
		return nil, fmt.Errorf("%w: observation source must be a bounded lowercase identifier", ErrInvalidInput)
	}
	if request.HTTPStatus != nil && (*request.HTTPStatus < 100 || *request.HTTPStatus > 599) {
		return nil, fmt.Errorf("%w: http_status must be between 100 and 599", ErrInvalidInput)
	}
	if request.OccurredAt.IsZero() {
		return nil, fmt.Errorf("%w: occurred_at is required", ErrInvalidInput)
	}
	if err := validateObservationPayloadValue(request.Payload, 0); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(request.Payload)
	if err != nil {
		return nil, fmt.Errorf("%w: observation payload is not valid JSON: %v", ErrInvalidInput, err)
	}
	if len(payload) > maxObservationPayloadBytes {
		return nil, fmt.Errorf("%w: observation payload exceeds %d bytes", ErrInvalidInput, maxObservationPayloadBytes)
	}
	return payload, nil
}

func validateObservationPayloadValue(value any, depth int) error {
	if depth > maxObservationPayloadDepth {
		return fmt.Errorf("%w: observation payload nesting is too deep", ErrInvalidInput)
	}
	switch typed := value.(type) {
	case nil, bool, float64, float32, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, json.Number:
		return nil
	case string:
		if len(typed) > maxObservationStringBytes {
			return fmt.Errorf("%w: observation payload string exceeds %d bytes", ErrInvalidInput, maxObservationStringBytes)
		}
		return nil
	case map[string]any:
		for key, item := range typed {
			normalizedKey := strings.ToLower(strings.TrimSpace(key))
			if normalizedKey == "" || len(normalizedKey) > 64 {
				return fmt.Errorf("%w: observation payload keys must be 1-64 bytes", ErrInvalidInput)
			}
			if _, forbidden := forbiddenObservationPayloadKeys[normalizedKey]; forbidden {
				return fmt.Errorf("%w: observation payload key %q is not allowed", ErrInvalidInput, key)
			}
			if err := validateObservationPayloadValue(item, depth+1); err != nil {
				return err
			}
		}
		return nil
	case []any:
		for _, item := range typed {
			if err := validateObservationPayloadValue(item, depth+1); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("%w: observation payload contains unsupported value type", ErrInvalidInput)
	}
}

func observeRequestHash(request ObserveRequest, payload []byte) (string, error) {
	encoded, err := json.Marshal(struct {
		SchemaVersion    int             `json:"schema_version"`
		SlotName         string          `json:"slot_name"`
		WorkerID         string          `json:"worker_id"`
		WorkerInstanceID string          `json:"worker_instance_id"`
		LeaseID          string          `json:"lease_id"`
		RouteGeneration  int64           `json:"route_generation"`
		TaskID           string          `json:"task_id"`
		BusinessRunID    string          `json:"business_run_id"`
		Kind             string          `json:"kind"`
		Source           string          `json:"source"`
		HTTPStatus       *int            `json:"http_status,omitempty"`
		OccurredAt       string          `json:"occurred_at"`
		Payload          json.RawMessage `json:"payload"`
	}{
		SchemaVersion: 1, SlotName: request.SlotName, WorkerID: request.WorkerID,
		WorkerInstanceID: request.WorkerInstanceID, LeaseID: request.LeaseID,
		RouteGeneration: request.RouteGeneration, TaskID: request.TaskID,
		BusinessRunID: request.BusinessRunID, Kind: request.Kind, Source: request.Source,
		HTTPStatus: request.HTTPStatus, OccurredAt: request.OccurredAt.Format(time.RFC3339),
		Payload: payload,
	})
	if err != nil {
		return "", fmt.Errorf("encode proxy observation request: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func validObservationKind(kind string) bool {
	switch kind {
	case ObservationSuccess, ObservationProxyTransport, ObservationYouTubeRateLimited,
		ObservationYouTubeChallenge, ObservationTokenOrClient, ObservationUpstreamTransient,
		ObservationContentTerminal, ObservationParserContract, ObservationParserRuntime,
		ObservationDatabaseContract, ObservationDatabaseRuntime, ObservationCancelled,
		ObservationUnknown:
		return true
	default:
		return false
	}
}

func actionForObservation(kind string) string {
	switch kind {
	case ObservationYouTubeChallenge:
		return PendingActionRotateProfile
	case ObservationProxyTransport, ObservationYouTubeRateLimited:
		return PendingActionRotateRoute
	default:
		return PendingActionNone
	}
}

func mergePendingAction(currentAction, currentIncident, proposedAction, proposedIncident string) (string, string) {
	if pendingActionPriority(proposedAction) > pendingActionPriority(currentAction) {
		return proposedAction, proposedIncident
	}
	if pendingActionPriority(currentAction) > 0 {
		return currentAction, currentIncident
	}
	return proposedAction, proposedIncident
}

func pendingActionPriority(action string) int {
	switch action {
	case PendingActionRotateProfile:
		return 2
	case PendingActionRotateRoute:
		return 1
	default:
		return 0
	}
}
