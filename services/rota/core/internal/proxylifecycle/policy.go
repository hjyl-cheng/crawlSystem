package proxylifecycle

import (
	"time"
)

type Status string

const (
	StatusIdle     Status = "idle"
	StatusActive   Status = "active"
	StatusFailed   Status = "failed"
	StatusArchived Status = "archived"
)

type FailureKind string

const (
	FailureNone            FailureKind = ""
	FailureHardUnreachable FailureKind = "hard_unreachable"
	FailureSoftUnreachable FailureKind = "soft_unreachable"
	FailureYouTubeUnusable FailureKind = "youtube_unusable"
)

type Snapshot struct {
	Status                Status
	FailedSince           *time.Time
	ContinuousFailedSince *time.Time
	FailureKind           FailureKind
	RevalidationRequired  bool
}

type Verdict struct {
	Healthy    bool        `json:"healthy"`
	Kind       FailureKind `json:"kind,omitempty"`
	Conclusive bool        `json:"conclusive"`
	// Deprecated: retained for stored-evidence and API compatibility only.
	ControlPathHealthy bool `json:"control_path_healthy"`
}

type ProbeStatus string

const (
	ProbeNotRun ProbeStatus = "not_run"
	ProbePassed ProbeStatus = "passed"
	ProbeFailed ProbeStatus = "failed"
)

type ProbeEvidence struct {
	Status         ProbeStatus `json:"status"`
	HTTPStatus     *int        `json:"http_status,omitempty"`
	ResponseTimeMS *int        `json:"response_time_ms,omitempty"`
	Error          string      `json:"error,omitempty"`
}

type HealthEvidence struct {
	StartedAt time.Time     `json:"started_at"`
	CheckedAt time.Time     `json:"checked_at"`
	Base      ProbeEvidence `json:"base"`
	YouTube   ProbeEvidence `json:"youtube"`
	Verdict   Verdict       `json:"verdict"`
	Error     string        `json:"error,omitempty"`
}

func HealthyVerdict() Verdict {
	return Verdict{
		Healthy:    true,
		Conclusive: true,
	}
}

type Policy struct {
	AutoArchiveEnabled     bool
	HardUnreachableWindow  time.Duration
	SoftUnreachableWindow  time.Duration
	YouTubeUnusableWindow  time.Duration
	InconclusiveRetry      time.Duration
	FailureRecheckInterval time.Duration
	ActiveRecheckInterval  time.Duration
}

func DefaultPolicy() Policy {
	return Policy{
		AutoArchiveEnabled:     true,
		HardUnreachableWindow:  6 * time.Hour,
		SoftUnreachableWindow:  24 * time.Hour,
		YouTubeUnusableWindow:  72 * time.Hour,
		InconclusiveRetry:      30 * time.Minute,
		FailureRecheckInterval: 30 * time.Minute,
		ActiveRecheckInterval:  2 * time.Hour,
	}
}

func PolicyFromHours(autoArchive bool, hard, soft, youtube, activeRecheckMinutes int) Policy {
	policy := DefaultPolicy()
	policy.AutoArchiveEnabled = autoArchive
	if hard > 0 {
		policy.HardUnreachableWindow = time.Duration(hard) * time.Hour
	}
	if soft > 0 {
		policy.SoftUnreachableWindow = time.Duration(soft) * time.Hour
	}
	if youtube > 0 {
		policy.YouTubeUnusableWindow = time.Duration(youtube) * time.Hour
	}
	if activeRecheckMinutes > 0 {
		policy.ActiveRecheckInterval = time.Duration(activeRecheckMinutes) * time.Minute
	}
	return policy
}

type Decision struct {
	Status                Status
	FailedSince           *time.Time
	ContinuousFailedSince *time.Time
	FailureKind           FailureKind
	NextHealthCheckAt     *time.Time
	RevalidationRequired  bool
	ArchivedAt            *time.Time
	ArchiveReason         string
}

func (p Policy) Decide(now time.Time, current Snapshot, verdict Verdict) Decision {
	if current.Status == StatusArchived {
		return Decision{Status: StatusArchived}
	}

	if verdict.Healthy && verdict.Conclusive {
		interval := p.ActiveRecheckInterval
		if interval <= 0 {
			interval = DefaultPolicy().ActiveRecheckInterval
		}
		return Decision{
			Status:            StatusActive,
			NextHealthCheckAt: timePtr(now.Add(interval)),
		}
	}

	if !verdict.Conclusive || !validFailureKind(verdict.Kind) {
		retry := p.InconclusiveRetry
		if retry <= 0 {
			retry = DefaultPolicy().InconclusiveRetry
		}
		if current.Status == StatusActive {
			return Decision{
				Status:            StatusIdle,
				NextHealthCheckAt: timePtr(now.Add(retry)),
			}
		}
		decision := Decision{
			Status:                current.Status,
			FailedSince:           cloneTime(current.FailedSince),
			ContinuousFailedSince: cloneTime(current.ContinuousFailedSince),
			FailureKind:           current.FailureKind,
			RevalidationRequired:  current.RevalidationRequired,
		}
		if current.Status == StatusFailed || current.Status == StatusIdle || current.RevalidationRequired {
			decision.NextHealthCheckAt = timePtr(now.Add(retry))
		}
		return decision
	}

	failedSince := current.FailedSince
	if current.Status != StatusFailed || failedSince == nil || current.FailureKind != verdict.Kind {
		failedSince = timePtr(now)
	}
	continuousFailedSince := current.ContinuousFailedSince
	if continuousFailedSince == nil {
		continuousFailedSince = cloneTime(current.FailedSince)
	}
	if current.Status != StatusFailed || continuousFailedSince == nil {
		continuousFailedSince = timePtr(now)
	}

	window := p.windowFor(verdict.Kind)
	if window <= 0 {
		window = DefaultPolicy().windowFor(verdict.Kind)
	}
	elapsed := now.Sub(*failedSince)
	if elapsed < 0 {
		failedSince = timePtr(now)
		elapsed = 0
	}
	continuousElapsed := now.Sub(*continuousFailedSince)
	if continuousElapsed < 0 {
		continuousFailedSince = timePtr(now)
		continuousElapsed = 0
	}

	continuousWindow := p.maximumFailureWindow()
	archiveForContinuousFailure := continuousWindow > 0 && continuousElapsed >= continuousWindow
	if p.AutoArchiveEnabled && (elapsed >= window || archiveForContinuousFailure) {
		reason := string(verdict.Kind)
		if elapsed < window && archiveForContinuousFailure {
			reason = "continuous_unusable"
		}
		return Decision{
			Status:                StatusArchived,
			FailedSince:           cloneTime(failedSince),
			ContinuousFailedSince: cloneTime(continuousFailedSince),
			FailureKind:           verdict.Kind,
			ArchivedAt:            timePtr(now),
			ArchiveReason:         reason,
		}
	}

	recheck := p.FailureRecheckInterval
	if recheck <= 0 {
		recheck = DefaultPolicy().FailureRecheckInterval
	}
	next := now.Add(recheck)
	if p.AutoArchiveEnabled {
		failureDeadline := failedSince.Add(window)
		if failureDeadline.After(now) && failureDeadline.Before(next) {
			next = failureDeadline
		}
	}
	if p.AutoArchiveEnabled && continuousWindow > 0 {
		continuousDeadline := continuousFailedSince.Add(continuousWindow)
		if continuousDeadline.After(now) && continuousDeadline.Before(next) {
			next = continuousDeadline
		}
	}
	return Decision{
		Status:                StatusFailed,
		FailedSince:           cloneTime(failedSince),
		ContinuousFailedSince: cloneTime(continuousFailedSince),
		FailureKind:           verdict.Kind,
		NextHealthCheckAt:     timePtr(next),
	}
}

func (p Policy) maximumFailureWindow() time.Duration {
	maximum := p.HardUnreachableWindow
	if p.SoftUnreachableWindow > maximum {
		maximum = p.SoftUnreachableWindow
	}
	if p.YouTubeUnusableWindow > maximum {
		maximum = p.YouTubeUnusableWindow
	}
	return maximum
}

func (p Policy) windowFor(kind FailureKind) time.Duration {
	switch kind {
	case FailureHardUnreachable:
		return p.HardUnreachableWindow
	case FailureSoftUnreachable:
		return p.SoftUnreachableWindow
	case FailureYouTubeUnusable:
		return p.YouTubeUnusableWindow
	default:
		return 0
	}
}

func validFailureKind(kind FailureKind) bool {
	return kind == FailureHardUnreachable || kind == FailureSoftUnreachable || kind == FailureYouTubeUnusable
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	return timePtr(*value)
}

func timePtr(value time.Time) *time.Time {
	return &value
}
