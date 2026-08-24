package proxycontrol

import (
	"context"
	"errors"
	"time"
)

var (
	ErrDisabled             = errors.New("proxy control is disabled")
	ErrInvalidInput         = errors.New("invalid proxy control request")
	ErrLeaseConflict        = errors.New("proxy control lease conflict")
	ErrLeaseGone            = errors.New("proxy control lease is no longer live")
	ErrPolicyRejected       = errors.New("proxy control identity policy rejected")
	ErrIdempotencyConflict  = errors.New("proxy control idempotency key reused")
	ErrJobExecutionConflict = errors.New("proxy control job execution conflict")
	ErrExecutionBudget      = errors.New("proxy control execution route budget exhausted")
	ErrBusinessRunBudget    = errors.New("proxy control business run budget exhausted")
	ErrTaskConflict         = errors.New("proxy control task conflict")
	ErrTaskCompleted        = errors.New("proxy control task is already completed")
	ErrAttemptNotQuiesced   = errors.New("proxy control attempt is not quiesced")
	ErrCompletionConflict   = errors.New("proxy control task already has a different completion")
	ErrObservationReference = errors.New("proxy control observation reference is invalid")
)

const (
	ProtocolVersionV2 = 2

	RoleDiscover     = "discover"
	RoleChannel      = "channel"
	RoleQueryQuality = "query_quality"
	RoleDetail       = "detail"

	TaskKindChannelFull        = "channel_full"
	TaskKindChannelIncremental = "channel_incremental"
	TaskKindContentEnrich      = "content_enrich"
	TaskKindDiscoverPage       = "discover_page"
	TaskKindQueryQualityChunk  = "query_quality_chunk"

	ObservationSuccess            = "success"
	ObservationProxyTransport     = "proxy_transport"
	ObservationYouTubeRateLimited = "youtube_rate_limited"
	ObservationYouTubeChallenge   = "youtube_challenge"
	ObservationTokenOrClient      = "token_or_client"
	ObservationUpstreamTransient  = "upstream_transient"
	ObservationContentTerminal    = "content_terminal"
	ObservationParserContract     = "parser_contract"
	ObservationParserRuntime      = "parser_runtime"
	ObservationDatabaseContract   = "database_contract"
	ObservationDatabaseRuntime    = "database_runtime"
	ObservationCancelled          = "cancelled"
	ObservationUnknown            = "unknown"

	PendingActionNone          = "none"
	PendingActionRotateRoute   = "rotate_route"
	PendingActionRotateProfile = "rotate_profile"

	TaskOutcomeSuccess   = "success"
	TaskOutcomeFailed    = "failed"
	TaskOutcomeCancelled = "cancelled"

	CompletionReadyKeepRoute  = "READY_KEEP_ROUTE"
	CompletionReadyNewRoute   = "READY_NEW_ROUTE"
	CompletionPendingNewRoute = "PENDING_NEW_ROUTE"
	CompletionPausedNoReserve = "PAUSED_NO_RESERVE"
)

type IdentityPolicy struct {
	ID                     string
	Version                int
	Hash                   string
	Role                   string
	YouTubeLanguage        string
	YouTubeControlLanguage string
	YouTubeCountry         string
	BrowserProfileTimezone string
	RequiredEgressCountry  string
	AllowedProxyTags       []string
	AttemptSafetyWindow    time.Duration
	GeoFreshnessWindow     time.Duration
}

type Options struct {
	Enabled                          bool
	WorkloadScope                    string
	CatalogVersion                   int
	CatalogDigest                    string
	WorkerPassword                   string
	DiscoverSlots                    int
	ChannelSlots                     int
	DetailSlots                      int
	QueryQualitySlots                int
	LeaseDuration                    time.Duration
	ReconcileInterval                time.Duration
	ResourceSyncInterval             time.Duration
	MinReservePercent                int
	MinReserveCount                  int
	FailureCooldown                  time.Duration
	NetworkCooldown                  time.Duration
	MaxRouteSwitchesPerExecution     int
	MaxNetworkAttemptsPerBusinessRun int
	IdentityPolicies                 map[string]IdentityPolicy
}

type BeginTaskRequest struct {
	SlotName         string `json:"slot_name"`
	WorkerID         string `json:"worker_id"`
	WorkerInstanceID string `json:"worker_instance_id"`
	LeaseID          string `json:"lease_id"`
	RouteGeneration  int64  `json:"route_generation"`
	AttemptRequestID string `json:"attempt_request_id"`
	BusinessRunID    string `json:"business_run_id"`
	JobExecutionID   string `json:"job_execution_id"`
	TaskKind         string `json:"task_kind"`
}

type Task struct {
	OK               bool      `json:"ok"`
	TaskID           string    `json:"task_id"`
	AttemptRequestID string    `json:"attempt_request_id"`
	BusinessRunID    string    `json:"business_run_id"`
	JobExecutionID   string    `json:"job_execution_id"`
	AttemptNumber    int       `json:"attempt_number"`
	SlotName         string    `json:"slot_name"`
	RouteGeneration  int64     `json:"route_generation"`
	TaskKind         string    `json:"task_kind"`
	StartedAt        time.Time `json:"started_at"`
}

type ObserveRequest struct {
	SlotName         string         `json:"slot_name"`
	WorkerID         string         `json:"worker_id"`
	WorkerInstanceID string         `json:"worker_instance_id"`
	LeaseID          string         `json:"lease_id"`
	RouteGeneration  int64          `json:"route_generation"`
	TaskID           string         `json:"task_id"`
	BusinessRunID    string         `json:"business_run_id"`
	ObservationID    string         `json:"observation_id"`
	Kind             string         `json:"kind"`
	Source           string         `json:"source"`
	HTTPStatus       *int           `json:"http_status,omitempty"`
	OccurredAt       time.Time      `json:"occurred_at"`
	Payload          map[string]any `json:"payload,omitempty"`
}

type ObservationResult struct {
	OK            bool      `json:"ok"`
	ObservationID string    `json:"observation_id"`
	TaskID        string    `json:"task_id"`
	Action        string    `json:"action"`
	IncidentID    string    `json:"incident_id,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

type CompleteTaskRequest struct {
	CompletionRequestID   string   `json:"completion_request_id"`
	SlotName              string   `json:"slot_name"`
	WorkerID              string   `json:"worker_id"`
	WorkerInstanceID      string   `json:"worker_instance_id"`
	LeaseID               string   `json:"lease_id"`
	RouteGeneration       int64    `json:"route_generation"`
	TaskID                string   `json:"task_id"`
	BusinessRunID         string   `json:"business_run_id"`
	Outcome               string   `json:"outcome"`
	DurationMS            int64    `json:"duration_ms"`
	BusinessComplete      bool     `json:"business_complete"`
	ObservationIDs        []string `json:"observation_ids"`
	AttemptQuiesced       bool     `json:"attempt_quiesced"`
	ActiveManagedRequests int      `json:"active_managed_requests"`
}

type CompleteTaskResult struct {
	OK                           bool   `json:"ok"`
	TaskCompleted                bool   `json:"task_completed"`
	CompletionRequestID          string `json:"completion_request_id"`
	TaskID                       string `json:"task_id"`
	SlotName                     string `json:"slot_name"`
	LeaseID                      string `json:"lease_id"`
	ControlState                 string `json:"control_state"`
	Ready                        bool   `json:"ready"`
	CompletedTaskRouteGeneration int64  `json:"completed_task_route_generation"`
	PendingRouteGeneration       *int64 `json:"pending_route_generation,omitempty"`
	PendingIdentityAction        string `json:"pending_identity_action,omitempty"`
	RetryAfterMS                 int64  `json:"retry_after_ms,omitempty"`
	ReasonCode                   string `json:"reason_code,omitempty"`
}

type ClaimRequest struct {
	ClaimRequestID        string `json:"claim_request_id"`
	ProtocolVersion       int    `json:"protocol_version"`
	Role                  string `json:"role"`
	WorkerID              string `json:"worker_id"`
	WorkerInstanceID      string `json:"worker_instance_id"`
	IdentityPolicyID      string `json:"identity_policy_id"`
	IdentityPolicyVersion int    `json:"identity_policy_version"`
}

type LeaseRequest struct {
	WorkerID          string `json:"worker_id"`
	LeaseID           string `json:"lease_id"`
	AssignmentVersion int64  `json:"assignment_version"`
}

type RenewRequest struct {
	RenewRequestID       string `json:"renew_request_id"`
	SlotName             string `json:"slot_name"`
	WorkerID             string `json:"worker_id"`
	WorkerInstanceID     string `json:"worker_instance_id"`
	LeaseID              string `json:"lease_id"`
	KnownRouteGeneration int64  `json:"known_route_generation"`
}

type ReleaseRequest struct {
	ReleaseRequestID     string `json:"release_request_id"`
	SlotName             string `json:"slot_name"`
	WorkerID             string `json:"worker_id"`
	WorkerInstanceID     string `json:"worker_instance_id"`
	LeaseID              string `json:"lease_id"`
	KnownRouteGeneration int64  `json:"known_route_generation"`
	Reason               string `json:"reason"`
}

type SwapRequest struct {
	WorkerID          string `json:"worker_id"`
	LeaseID           string `json:"lease_id"`
	AssignmentVersion int64  `json:"assignment_version"`
	FailedProxyID     int    `json:"failed_proxy_id"`
}

type Assignment struct {
	OK                    bool         `json:"ok"`
	Ready                 bool         `json:"ready"`
	Reason                string       `json:"reason,omitempty"`
	ControlState          string       `json:"control_state,omitempty"`
	WorkloadScope         string       `json:"workload_scope,omitempty"`
	ProtocolVersion       int          `json:"protocol_version,omitempty"`
	Role                  string       `json:"role"`
	WorkerID              string       `json:"worker_id"`
	WorkerInstanceID      string       `json:"worker_instance_id,omitempty"`
	SlotName              string       `json:"slot_name,omitempty"`
	ProxyUser             string       `json:"proxy_user,omitempty"`
	ProxyID               *int         `json:"-"`
	ProxyAddressHash      string       `json:"-"`
	LeaseID               string       `json:"lease_id,omitempty"`
	LeaseUntil            *time.Time   `json:"lease_until,omitempty"`
	LeaseRemainingMS      int64        `json:"lease_remaining_ms,omitempty"`
	ServerTime            time.Time    `json:"server_time,omitempty"`
	AssignmentVersion     int64        `json:"route_generation"`
	CredentialGeneration  int64        `json:"credential_generation,omitempty"`
	NetworkIdentityKey    string       `json:"network_identity_key,omitempty"`
	ProfileEpoch          int64        `json:"profile_epoch"`
	IdentityPolicyID      string       `json:"identity_policy_id,omitempty"`
	IdentityPolicyVersion int          `json:"identity_policy_version,omitempty"`
	IdentityPolicyHash    string       `json:"identity_policy_hash,omitempty"`
	IdentityAction        string       `json:"identity_action,omitempty"`
	EgressCountry         string       `json:"egress_country,omitempty"`
	RouteChanged          bool         `json:"route_changed,omitempty"`
	RenewSequence         int64        `json:"renew_sequence,omitempty"`
	ReadyAfter            *time.Time   `json:"-"`
	Replacement           *Replacement `json:"-"`
}

type Replacement struct {
	Swapped            bool `json:"swapped"`
	FailedProxyID      int  `json:"failed_proxy_id"`
	FromProxyID        *int `json:"from_proxy_id,omitempty"`
	ReplacementProxyID *int `json:"replacement_proxy_id,omitempty"`
	CacheRefreshed     bool `json:"cache_refreshed"`
}

type ReleaseResult struct {
	OK               bool      `json:"ok"`
	Released         bool      `json:"released"`
	ReleaseRequestID string    `json:"release_request_id"`
	LeaseID          string    `json:"lease_id"`
	SlotName         string    `json:"slot_name"`
	RouteGeneration  int64     `json:"route_generation,omitempty"`
	Status           string    `json:"status"`
	ReleasedAt       time.Time `json:"released_at"`
	Reason           string    `json:"reason"`
}

type ReportRequest struct {
	Service           string `json:"service,omitempty"`
	Outcome           string `json:"outcome"`
	ProxyID           *int   `json:"proxy_id,omitempty"`
	ProxyUser         string `json:"proxy_user,omitempty"`
	IncidentID        string `json:"incident_id,omitempty"`
	LeaseID           string `json:"lease_id,omitempty"`
	AssignmentVersion *int64 `json:"assignment_version,omitempty"`
	Source            string `json:"source,omitempty"`
	Status            *int   `json:"status,omitempty"`
	TargetURL         string `json:"target_url,omitempty"`
	ErrorType         string `json:"error_type,omitempty"`
	FailureKind       string `json:"failure_kind,omitempty"`
	Sample            string `json:"sample,omitempty"`
	SampleCount       int    `json:"sample_count,omitempty"`
	BusinessComplete  bool   `json:"business_complete,omitempty"`
	DurationMS        int    `json:"duration_ms,omitempty"`
	DetailDurationMS  *int   `json:"detail_duration_ms,omitempty"`
	ReportedAt        string `json:"reported_at,omitempty"`
}

type ReportResult struct {
	OK                   bool   `json:"ok"`
	Action               string `json:"action"`
	Confirmed            bool   `json:"confirmed"`
	ProxyID              *int   `json:"proxy_id,omitempty"`
	Reason               string `json:"reason,omitempty"`
	CooldownMinutes      int    `json:"cooldown_minutes,omitempty"`
	HealthCheckRequested bool   `json:"health_check_requested,omitempty"`
}

type RoleCapacity struct {
	IdentityPolicyID      string `json:"identity_policy_id,omitempty"`
	IdentityPolicyVersion int    `json:"identity_policy_version,omitempty"`
	IdentityPolicyHash    string `json:"identity_policy_hash,omitempty"`
	Desired               int    `json:"desired"`
	Provisioned           int    `json:"provisioned"`
	Eligible              int    `json:"eligible"`
	Assigned              int    `json:"assigned"`
	Ready                 int    `json:"ready"`
	Claimed               int    `json:"claimed"`
	Reserve               int    `json:"reserve"`
}

type Capacity struct {
	WorkloadScope       string                  `json:"workload_scope"`
	CatalogVersion      int                     `json:"catalog_version"`
	CatalogDigest       string                  `json:"catalog_digest"`
	OK                  bool                    `json:"ok"`
	Active              int                     `json:"active"`
	Cooldown            int                     `json:"cooldown"`
	Total               int                     `json:"total"`
	Archived            int                     `json:"archived"`
	Running             int                     `json:"running"`
	Reserve             int                     `json:"reserve"`
	MinimumReserve      int                     `json:"minimum_reserve"`
	ReserveBelowMinimum bool                    `json:"reserve_below_minimum"`
	Roles               map[string]RoleCapacity `json:"roles"`
}

// Interface is the complete Worker-facing interface. Resource synchronization
// and assignment reconciliation remain implementation details behind Run.
type Interface interface {
	Claim(context.Context, ClaimRequest) (Assignment, error)
	Renew(context.Context, RenewRequest) (Assignment, error)
	BeginTask(context.Context, BeginTaskRequest) (Task, error)
	Observe(context.Context, ObserveRequest) (ObservationResult, error)
	CompleteTask(context.Context, CompleteTaskRequest) (CompleteTaskResult, error)
	Report(context.Context, ReportRequest) (ReportResult, error)
	Swap(context.Context, SwapRequest) (Assignment, error)
	Release(context.Context, ReleaseRequest) (ReleaseResult, error)
	Capacity(context.Context) (Capacity, error)
}
