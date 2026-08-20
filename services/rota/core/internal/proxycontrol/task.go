package proxycontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (m *Manager) BeginTask(ctx context.Context, request BeginTaskRequest) (Task, error) {
	if err := m.requireEnabled(); err != nil {
		return Task{}, err
	}
	request = normalizeBeginTaskRequest(request)
	if err := validateBeginTaskRequest(request); err != nil {
		return Task{}, err
	}
	requestHash, err := beginTaskRequestHash(request)
	if err != nil {
		return Task{}, err
	}

	tx, err := m.db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Task{}, fmt.Errorf("begin proxy task: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, controlAdvisoryLock); err != nil {
		return Task{}, fmt.Errorf("lock proxy task: %w", err)
	}

	replayed, found, err := loadTaskByAttemptRequest(ctx, tx, m.options.WorkloadScope, request.AttemptRequestID)
	if err != nil {
		return Task{}, err
	}
	if found {
		if replayed.requestHash != requestHash {
			return Task{}, fmt.Errorf("%w: attempt_request_id %q", ErrIdempotencyConflict, request.AttemptRequestID)
		}
		if err := tx.Commit(ctx); err != nil {
			return Task{}, fmt.Errorf("commit replayed proxy task: %w", err)
		}
		return replayed.Task, nil
	}
	if err := abandonInvalidActiveTasks(ctx, tx, request.SlotName); err != nil {
		return Task{}, err
	}

	var slotName, role, identityPolicyID, identityPolicyHash string
	var identityPolicyVersion int
	err = tx.QueryRow(ctx, `
		SELECT slot_name,role,identity_policy_id,identity_policy_version,identity_policy_hash
		FROM proxy_running_slots
		WHERE slot_name=$1 AND worker_id=$2 AND worker_instance_id=$3 AND lease_id=$4
		  AND assignment_version=$5 AND lease_until > NOW()
		  AND active_task_id IS NULL AND control_state='leased_idle'
		FOR UPDATE
	`, request.SlotName, request.WorkerID, request.WorkerInstanceID,
		request.LeaseID, request.RouteGeneration).Scan(
		&slotName, &role, &identityPolicyID, &identityPolicyVersion, &identityPolicyHash,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Task{}, ErrLeaseConflict
	}
	if err != nil {
		return Task{}, fmt.Errorf("lock proxy task lease: %w", err)
	}
	if !roleAllowsTaskKind(role, request.TaskKind) {
		return Task{}, fmt.Errorf("%w: role %q cannot begin task kind %q", ErrPolicyRejected, role, request.TaskKind)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO proxy_control_business_runs (
		  workload_scope, business_run_id, next_attempt_number,
		  retry_policy_id, retry_policy_version,
		  max_route_switches_per_execution, max_network_attempts_per_business_run
		) VALUES ($1,$2,1,'default',1,$3,$4)
		ON CONFLICT (workload_scope, business_run_id) DO NOTHING
	`, m.options.WorkloadScope, request.BusinessRunID,
		m.options.MaxRouteSwitchesPerExecution, m.options.MaxNetworkAttemptsPerBusinessRun); err != nil {
		return Task{}, fmt.Errorf("prepare proxy business run: %w", err)
	}

	var nextAttempt, maxSwitches, maxAttempts int
	if err := tx.QueryRow(ctx, `
		SELECT next_attempt_number, max_route_switches_per_execution,
		       max_network_attempts_per_business_run
		FROM proxy_control_business_runs
		WHERE workload_scope=$1 AND business_run_id=$2
		FOR UPDATE
	`, m.options.WorkloadScope, request.BusinessRunID).Scan(
		&nextAttempt, &maxSwitches, &maxAttempts,
	); err != nil {
		return Task{}, fmt.Errorf("lock proxy business run: %w", err)
	}

	var boundRunID, boundTaskKind string
	err = tx.QueryRow(ctx, `
		SELECT business_run_id, task_kind
		FROM proxy_control_tasks
		WHERE workload_scope=$1 AND job_execution_id=$2
		ORDER BY started_at, task_id
		LIMIT 1
	`, m.options.WorkloadScope, request.JobExecutionID).Scan(&boundRunID, &boundTaskKind)
	if err == nil && (boundRunID != request.BusinessRunID || boundTaskKind != request.TaskKind) {
		return Task{}, fmt.Errorf("%w: job_execution_id %q", ErrJobExecutionConflict, request.JobExecutionID)
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Task{}, fmt.Errorf("load proxy job execution binding: %w", err)
	}

	var executionTaskCount int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM proxy_control_tasks
		WHERE workload_scope=$1 AND job_execution_id=$2
	`, m.options.WorkloadScope, request.JobExecutionID).Scan(&executionTaskCount); err != nil {
		return Task{}, fmt.Errorf("count proxy execution tasks: %w", err)
	}
	if executionTaskCount >= 1+maxSwitches {
		return Task{}, ErrExecutionBudget
	}
	if nextAttempt > maxAttempts {
		if _, err := tx.Exec(ctx, `
			UPDATE proxy_control_business_runs SET budget_exhausted_at=COALESCE(budget_exhausted_at,NOW()), updated_at=NOW()
			WHERE workload_scope=$1 AND business_run_id=$2
		`, m.options.WorkloadScope, request.BusinessRunID); err != nil {
			return Task{}, fmt.Errorf("mark proxy business run budget exhausted: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return Task{}, fmt.Errorf("commit proxy business run budget exhaustion: %w", err)
		}
		return Task{}, ErrBusinessRunBudget
	}

	taskID := uuid.NewString()
	var startedAt Task
	err = tx.QueryRow(ctx, `
		INSERT INTO proxy_control_tasks (
		  task_id, attempt_request_id, request_hash, workload_scope,
		  business_run_id, job_execution_id, attempt_number,
		  slot_name, worker_id, worker_instance_id, lease_id, route_generation, task_kind,
		  identity_policy_id, identity_policy_version, identity_policy_hash, status
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,'active')
		RETURNING task_id, attempt_request_id, business_run_id, job_execution_id,
		          attempt_number, slot_name, route_generation, task_kind, started_at
	`, taskID, request.AttemptRequestID, requestHash, m.options.WorkloadScope,
		request.BusinessRunID, request.JobExecutionID, nextAttempt,
		request.SlotName, request.WorkerID, request.WorkerInstanceID, request.LeaseID,
		request.RouteGeneration, request.TaskKind, identityPolicyID, identityPolicyVersion,
		identityPolicyHash).Scan(
		&startedAt.TaskID, &startedAt.AttemptRequestID, &startedAt.BusinessRunID,
		&startedAt.JobExecutionID, &startedAt.AttemptNumber, &startedAt.SlotName,
		&startedAt.RouteGeneration, &startedAt.TaskKind, &startedAt.StartedAt,
	)
	if err != nil {
		return Task{}, fmt.Errorf("create proxy task: %w", err)
	}
	startedAt.OK = true
	if _, err := tx.Exec(ctx, `
		UPDATE proxy_running_slots
		SET active_task_id=$2,active_task_started_at=NOW(),
		    pending_action=NULL,pending_incident_id=NULL,
		    control_state='active_task',updated_at=NOW()
		WHERE slot_name=$1 AND active_task_id IS NULL
	`, request.SlotName, taskID); err != nil {
		return Task{}, fmt.Errorf("activate proxy task slot: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE proxy_control_business_runs
		SET next_attempt_number=$3, updated_at=NOW()
		WHERE workload_scope=$1 AND business_run_id=$2
	`, m.options.WorkloadScope, request.BusinessRunID, nextAttempt+1); err != nil {
		return Task{}, fmt.Errorf("advance proxy attempt number: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Task{}, fmt.Errorf("commit proxy task: %w", err)
	}
	return startedAt, nil
}

type storedTask struct {
	Task
	requestHash string
}

func loadTaskByAttemptRequest(
	ctx context.Context,
	tx pgx.Tx,
	workloadScope string,
	attemptRequestID string,
) (storedTask, bool, error) {
	var stored storedTask
	err := tx.QueryRow(ctx, `
		SELECT task_id, attempt_request_id, request_hash, business_run_id,
		       job_execution_id, attempt_number, slot_name, route_generation,
		       task_kind, started_at
		FROM proxy_control_tasks
		WHERE workload_scope=$1 AND attempt_request_id=$2
	`, workloadScope, attemptRequestID).Scan(
		&stored.TaskID, &stored.AttemptRequestID, &stored.requestHash,
		&stored.BusinessRunID, &stored.JobExecutionID, &stored.AttemptNumber,
		&stored.SlotName, &stored.RouteGeneration, &stored.TaskKind, &stored.StartedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return storedTask{}, false, nil
	}
	if err != nil {
		return storedTask{}, false, fmt.Errorf("load proxy task receipt: %w", err)
	}
	stored.OK = true
	return stored, true, nil
}

func normalizeBeginTaskRequest(request BeginTaskRequest) BeginTaskRequest {
	request.SlotName = strings.TrimSpace(request.SlotName)
	request.WorkerID = strings.TrimSpace(request.WorkerID)
	request.WorkerInstanceID = strings.TrimSpace(request.WorkerInstanceID)
	request.LeaseID = strings.TrimSpace(request.LeaseID)
	request.AttemptRequestID = strings.TrimSpace(request.AttemptRequestID)
	request.BusinessRunID = strings.TrimSpace(request.BusinessRunID)
	request.JobExecutionID = strings.TrimSpace(request.JobExecutionID)
	request.TaskKind = strings.ToLower(strings.TrimSpace(request.TaskKind))
	return request
}

func validateBeginTaskRequest(request BeginTaskRequest) error {
	values := []string{
		request.SlotName,
		request.WorkerID,
		request.WorkerInstanceID,
		request.LeaseID,
		request.AttemptRequestID,
		request.BusinessRunID,
		request.JobExecutionID,
		request.TaskKind,
	}
	for _, value := range values {
		if value == "" || len(value) > 255 {
			return fmt.Errorf("%w: begin task fields are required and limited to 255 bytes", ErrInvalidInput)
		}
	}
	if request.RouteGeneration < 0 {
		return fmt.Errorf("%w: route_generation must be non-negative", ErrInvalidInput)
	}
	if !validTaskKind(request.TaskKind) {
		return fmt.Errorf("%w: unsupported task_kind %q", ErrInvalidInput, request.TaskKind)
	}
	return nil
}

func validTaskKind(taskKind string) bool {
	switch taskKind {
	case TaskKindChannelFull, TaskKindChannelIncremental, TaskKindDiscoverPage, TaskKindQueryQualityChunk:
		return true
	default:
		return false
	}
}

func roleAllowsTaskKind(role, taskKind string) bool {
	switch role {
	case RoleChannel:
		return taskKind == TaskKindChannelFull || taskKind == TaskKindChannelIncremental
	case RoleDiscover:
		return taskKind == TaskKindDiscoverPage
	case RoleQueryQuality:
		return taskKind == TaskKindQueryQualityChunk
	default:
		return false
	}
}

func beginTaskRequestHash(request BeginTaskRequest) (string, error) {
	payload, err := json.Marshal(struct {
		SchemaVersion    int    `json:"schema_version"`
		SlotName         string `json:"slot_name"`
		WorkerID         string `json:"worker_id"`
		WorkerInstanceID string `json:"worker_instance_id"`
		LeaseID          string `json:"lease_id"`
		RouteGeneration  int64  `json:"route_generation"`
		BusinessRunID    string `json:"business_run_id"`
		JobExecutionID   string `json:"job_execution_id"`
		TaskKind         string `json:"task_kind"`
	}{
		SchemaVersion: 1, SlotName: request.SlotName, WorkerID: request.WorkerID,
		WorkerInstanceID: request.WorkerInstanceID,
		LeaseID:          request.LeaseID, RouteGeneration: request.RouteGeneration,
		BusinessRunID: request.BusinessRunID, JobExecutionID: request.JobExecutionID,
		TaskKind: request.TaskKind,
	})
	if err != nil {
		return "", fmt.Errorf("encode begin task request: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}
