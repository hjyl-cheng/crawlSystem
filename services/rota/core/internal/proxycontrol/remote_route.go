package proxycontrol

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// RemoteRouteRequest identifies an already running Rota Task. It cannot select
// a Proxy, create a Lease, start a Task, or change the retry/route budget.
type RemoteRouteRequest struct {
	SlotName         string `json:"slot_name"`
	WorkerID         string `json:"worker_id"`
	WorkerInstanceID string `json:"worker_instance_id"`
	LeaseID          string `json:"lease_id"`
	RouteGeneration  int64  `json:"route_generation"`
	TaskID           string `json:"task_id"`
	BusinessRunID    string `json:"business_run_id"`
	JobExecutionID   string `json:"job_execution_id"`
}

// Credentials deliberately do not serialize through ordinary status/Assignment
// responses. Only the separately authenticated internal handler exposes them.
type RemoteUpstream struct {
	Protocol string
	Address  string
	Username string
	Password string
}

func (RemoteUpstream) String() string   { return "[assigned upstream credentials redacted]" }
func (RemoteUpstream) GoString() string { return "[assigned upstream credentials redacted]" }

type RemoteRoute struct {
	RemoteRouteRequest
	OK                    bool           `json:"ok"`
	WorkloadScope         string         `json:"workload_scope"`
	CredentialGeneration  int64          `json:"credential_generation"`
	NetworkIdentityKey    string         `json:"network_identity_key"`
	ProfileEpoch          int64          `json:"profile_epoch"`
	IdentityPolicyID      string         `json:"identity_policy_id"`
	IdentityPolicyVersion int            `json:"identity_policy_version"`
	IdentityPolicyHash    string         `json:"identity_policy_hash"`
	EgressCountry         string         `json:"egress_country"`
	LeaseUntil            time.Time      `json:"lease_until"`
	ServerTime            time.Time      `json:"server_time"`
	Upstream              RemoteUpstream `json:"-"`
}

// ReadRemoteRoute returns only the currently assigned upstream. This single SQL
// statement sees one consistent snapshot of Slot, Lease history, Task and Proxy.
// It takes no global control lock and makes no writes. An active Task owns its
// Route under the existing quiesce/completion rules. The caller must additionally
// fence the remote channel lease before issuing a short-lived node authorization.
func (m *Manager) ReadRemoteRoute(ctx context.Context, request RemoteRouteRequest) (RemoteRoute, error) {
	if err := m.requireEnabled(); err != nil {
		return RemoteRoute{}, err
	}
	for _, value := range []string{request.SlotName, request.WorkerID, request.WorkerInstanceID, request.LeaseID, request.TaskID, request.BusinessRunID, request.JobExecutionID} {
		if value == "" || strings.TrimSpace(value) != value || len(value) > 512 {
			return RemoteRoute{}, ErrInvalidInput
		}
	}
	if request.RouteGeneration < 0 || request.RouteGeneration > 9007199254740991 {
		return RemoteRoute{}, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	result := RemoteRoute{RemoteRouteRequest: request, WorkloadScope: m.options.WorkloadScope}
	err := m.db.Pool.QueryRow(ctx, `
		SELECT s.credential_generation,s.network_identity_key,s.profile_epoch,
		       s.identity_policy_id,s.identity_policy_version,s.identity_policy_hash,
		       COALESCE(p.country_code,''),
		       LEAST(s.lease_until,l.lease_until,p.identity_valid_until),statement_timestamp(),
		       p.protocol,p.address,COALESCE(p.username,''),COALESCE(p.password,'')
		FROM proxy_running_slots s
		JOIN proxy_control_leases l ON l.lease_id=s.current_lease_id AND l.slot_name=s.slot_name
		JOIN proxy_control_tasks t ON t.task_id=s.active_task_id AND t.slot_name=s.slot_name
		JOIN proxies p ON p.id=s.proxy_id
		WHERE s.slot_name=$1 AND s.worker_id=$2 AND s.worker_instance_id=$3
		  AND s.current_lease_id=$4 AND s.lease_id=$4 AND s.assignment_version=$5
		  AND t.task_id=$6 AND t.business_run_id=$7 AND t.job_execution_id=$8
		  AND l.workload_scope=$9 AND t.workload_scope=$9
		  AND l.worker_id=s.worker_id AND l.worker_instance_id=s.worker_instance_id
		  AND l.role=s.role AND l.status='active'
		  AND t.worker_id=s.worker_id AND t.worker_instance_id=s.worker_instance_id
		  AND t.lease_id=s.current_lease_id AND t.route_generation=s.assignment_version
		  AND t.status='active' AND s.control_state='active_task'
		  AND s.ready_after<=statement_timestamp()
		  AND s.lease_until>statement_timestamp()+interval '3 seconds'
		  AND l.lease_until>statement_timestamp()+interval '3 seconds'
		  AND (p.identity_valid_until IS NULL OR p.identity_valid_until>statement_timestamp()+interval '3 seconds')
		  AND p.network_identity_key=s.network_identity_key
		  AND l.identity_policy_id=s.identity_policy_id AND t.identity_policy_id=s.identity_policy_id
		  AND l.identity_policy_version=s.identity_policy_version AND t.identity_policy_version=s.identity_policy_version
		  AND l.identity_policy_hash=s.identity_policy_hash AND t.identity_policy_hash=s.identity_policy_hash
	`, request.SlotName, request.WorkerID, request.WorkerInstanceID, request.LeaseID, request.RouteGeneration,
		request.TaskID, request.BusinessRunID, request.JobExecutionID, m.options.WorkloadScope).Scan(
		&result.CredentialGeneration, &result.NetworkIdentityKey, &result.ProfileEpoch,
		&result.IdentityPolicyID, &result.IdentityPolicyVersion, &result.IdentityPolicyHash,
		&result.EgressCountry, &result.LeaseUntil, &result.ServerTime,
		&result.Upstream.Protocol, &result.Upstream.Address, &result.Upstream.Username, &result.Upstream.Password)
	if errors.Is(err, pgx.ErrNoRows) {
		return RemoteRoute{}, ErrTaskConflict
	}
	if err != nil {
		return RemoteRoute{}, errors.New("remote route configuration read failed")
	}
	result.OK = true
	return result, nil
}
