package proxycontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (m *Manager) Report(ctx context.Context, request ReportRequest) (ReportResult, error) {
	if err := m.requireEnabled(); err != nil {
		return ReportResult{}, err
	}
	request.Outcome = strings.ToLower(strings.TrimSpace(request.Outcome))
	request.ProxyUser = strings.TrimSpace(request.ProxyUser)
	request.IncidentID = strings.TrimSpace(request.IncidentID)
	if request.Outcome != "success" && request.Outcome != "failure" {
		return ReportResult{}, fmt.Errorf("%w: outcome must be success or failure", ErrInvalidInput)
	}
	if request.Outcome == "failure" && request.IncidentID == "" {
		return ReportResult{}, fmt.Errorf("%w: incident_id is required for failures", ErrInvalidInput)
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return ReportResult{}, fmt.Errorf("encode proxy report: %w", err)
	}

	tx, err := m.db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ReportResult{}, fmt.Errorf("begin proxy report: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, controlAdvisoryLock); err != nil {
		return ReportResult{}, fmt.Errorf("lock proxy report: %w", err)
	}
	proxyID, status, cooldownUntil, err := resolveReportedProxy(ctx, tx, request)
	if err != nil {
		return ReportResult{}, err
	}
	if proxyID == nil {
		result := ReportResult{OK: true, Action: "ignored", Reason: "proxy_identity_not_resolved"}
		if err := tx.Commit(ctx); err != nil {
			return ReportResult{}, fmt.Errorf("commit ignored proxy report: %w", err)
		}
		return result, nil
	}
	if status == "archived" {
		result := ReportResult{OK: true, Action: "archived", Confirmed: true, ProxyID: proxyID}
		if err := tx.Commit(ctx); err != nil {
			return ReportResult{}, fmt.Errorf("commit archived proxy report: %w", err)
		}
		return result, nil
	}

	if request.IncidentID != "" {
		var inserted bool
		err := tx.QueryRow(ctx, `
			WITH inserted AS (
			  INSERT INTO proxy_control_reports (
			    incident_id, proxy_id, proxy_user, outcome, payload
			  ) VALUES ($1,$2,NULLIF($3,''),$4,$5)
			  ON CONFLICT (incident_id) DO NOTHING
			  RETURNING true
			)
			SELECT COALESCE((SELECT true FROM inserted), false)
		`, request.IncidentID, *proxyID, request.ProxyUser, request.Outcome, payload).Scan(&inserted)
		if err != nil {
			return ReportResult{}, fmt.Errorf("record proxy incident: %w", err)
		}
		if !inserted {
			result := ReportResult{
				OK:        true,
				Action:    "duplicate_incident",
				Confirmed: true,
				ProxyID:   proxyID,
			}
			if err := tx.Commit(ctx); err != nil {
				return ReportResult{}, fmt.Errorf("commit duplicate proxy report: %w", err)
			}
			return result, nil
		}
	}

	var result ReportResult
	if request.Outcome == "success" {
		result, err = applySuccessReport(ctx, tx, *proxyID, request)
	} else if cooldownUntil != nil && cooldownUntil.After(time.Now()) {
		result = ReportResult{
			OK:        true,
			Action:    "already_cooling_down",
			Confirmed: true,
			ProxyID:   proxyID,
		}
	} else {
		result, err = m.applyFailureReport(ctx, tx, *proxyID, request)
	}
	if err != nil {
		return ReportResult{}, err
	}
	if request.IncidentID != "" {
		encoded, encodeErr := json.Marshal(result)
		if encodeErr != nil {
			return ReportResult{}, fmt.Errorf("encode proxy report result: %w", encodeErr)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE proxy_control_reports
			SET result=$2, completed_at=NOW()
			WHERE incident_id=$1
		`, request.IncidentID, encoded); err != nil {
			return ReportResult{}, fmt.Errorf("complete proxy report: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ReportResult{}, fmt.Errorf("commit proxy report: %w", err)
	}
	if request.Outcome == "failure" && result.Action == "cooldown" {
		// The transaction already persisted the revalidation fence and due time.
		// This in-memory request is only a latency optimization.
		_ = m.requestHealthCheck(*proxyID)
		result.HealthCheckRequested = true
	}
	return result, nil
}

func resolveReportedProxy(
	ctx context.Context,
	tx pgx.Tx,
	request ReportRequest,
) (*int, string, *time.Time, error) {
	var (
		proxyID       int
		status        string
		cooldownUntil *time.Time
	)
	if request.LeaseID != "" || request.AssignmentVersion != nil {
		if request.ProxyUser == "" || request.LeaseID == "" || request.AssignmentVersion == nil {
			return nil, "", nil, fmt.Errorf("%w: proxy_user, lease_id, and assignment_version must be reported together", ErrInvalidInput)
		}
		err := tx.QueryRow(ctx, `
			SELECT p.id, p.status, p.cooldown_until
			FROM proxy_running_slots s
			JOIN proxy_users u ON u.id=s.user_id
			JOIN proxies p ON p.id=s.proxy_id
			WHERE u.username=$1 AND s.lease_id=$2 AND s.assignment_version=$3
			  AND s.lease_until > NOW()
			FOR UPDATE OF s,u,p
		`, request.ProxyUser, request.LeaseID, *request.AssignmentVersion).Scan(
			&proxyID, &status, &cooldownUntil,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, "", nil, ErrLeaseConflict
		}
		if err != nil {
			return nil, "", nil, fmt.Errorf("resolve leased proxy report: %w", err)
		}
		if request.ProxyID != nil && *request.ProxyID != proxyID {
			return nil, "", nil, ErrLeaseConflict
		}
		return &proxyID, status, cooldownUntil, nil
	}

	if request.ProxyID != nil && *request.ProxyID > 0 {
		err := tx.QueryRow(ctx, `
			SELECT id, status, cooldown_until FROM proxies WHERE id=$1 FOR UPDATE
		`, *request.ProxyID).Scan(&proxyID, &status, &cooldownUntil)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, "", nil, nil
		}
		if err != nil {
			return nil, "", nil, fmt.Errorf("resolve reported proxy id: %w", err)
		}
		return &proxyID, status, cooldownUntil, nil
	}
	if request.ProxyUser == "" {
		return nil, "", nil, nil
	}
	rows, err := tx.Query(ctx, `
		SELECT p.id, p.status, p.cooldown_until
		FROM proxy_users u
		JOIN pool_proxies members ON members.pool_id=u.main_pool_id
		JOIN proxies p ON p.id=members.proxy_id
		WHERE u.username=$1 AND u.enabled=true
		ORDER BY p.id
		LIMIT 2
		FOR UPDATE OF p
	`, request.ProxyUser)
	if err != nil {
		return nil, "", nil, fmt.Errorf("resolve proxy user report: %w", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		if err := rows.Scan(&proxyID, &status, &cooldownUntil); err != nil {
			return nil, "", nil, fmt.Errorf("scan reported proxy user: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, "", nil, err
	}
	if count != 1 {
		return nil, "", nil, nil
	}
	return &proxyID, status, cooldownUntil, nil
}

func applySuccessReport(
	ctx context.Context,
	tx pgx.Tx,
	proxyID int,
	request ReportRequest,
) (ReportResult, error) {
	sampleCount := request.SampleCount
	if sampleCount < 1 {
		sampleCount = 1
	} else if sampleCount > 10_000 {
		sampleCount = 10_000
	}
	duration := request.DurationMS
	if duration < 1 {
		duration = 1
	} else if duration > 600_000 {
		duration = 600_000
	}
	var detailDuration *int
	if request.DetailDurationMS != nil {
		value := *request.DetailDurationMS
		if value < 0 {
			value = 0
		} else if value > 600_000 {
			value = 600_000
		}
		detailDuration = &value
	}
	if _, err := tx.Exec(ctx, `
		UPDATE proxies
		SET youtube_successful_requests=youtube_successful_requests+$2,
		    youtube_avg_response_time=CASE
		      WHEN youtube_avg_response_time IS NULL OR youtube_avg_response_time<=0 THEN $3
		      ELSE round(youtube_avg_response_time*0.8+$3::int*0.2)::int
		    END,
		    youtube_avg_detail_time=CASE
		      WHEN $4::int IS NULL THEN youtube_avg_detail_time
		      WHEN youtube_avg_detail_time IS NULL OR youtube_avg_detail_time<=0 THEN $4
		      ELSE round(youtube_avg_detail_time*0.8+$4::int*0.2)::int
		    END,
		    youtube_failure_score=GREATEST(0,youtube_failure_score*0.8),
		    last_youtube_success=NOW(), updated_at=NOW()
		WHERE id=$1
	`, proxyID, sampleCount, duration, detailDuration); err != nil {
		return ReportResult{}, fmt.Errorf("record proxy performance: %w", err)
	}
	return ReportResult{
		OK:        true,
		Action:    "performance_recorded",
		Confirmed: false,
		ProxyID:   &proxyID,
	}, nil
}

func (m *Manager) applyFailureReport(
	ctx context.Context,
	tx pgx.Tx,
	proxyID int,
	request ReportRequest,
) (ReportResult, error) {
	cooldown := m.options.FailureCooldown
	if request.ErrorType == "proxy_unavailable" || request.Status != nil && *request.Status == 502 {
		cooldown = m.options.NetworkCooldown
	}
	status := request.Status
	errorText := truncateReportText(fmt.Sprintf(
		"crawler: %s: %s",
		firstNonEmpty(request.ErrorType, "proxy_failure"),
		firstNonEmpty(request.Sample, "crawler request failed"),
	), 500)
	if _, err := tx.Exec(ctx, `
		UPDATE proxies
		SET youtube_failed_requests=youtube_failed_requests+1,
		    youtube_failure_score=LEAST(1,youtube_failure_score*0.8+0.2),
		    last_youtube_failure=NOW(),
			    cooldown_until=GREATEST(
			      COALESCE(cooldown_until,NOW()),
			      NOW()+$2::double precision*interval '1 second'
			    ),
		    last_youtube_status=$3,
		    last_youtube_error=$4,
		    last_youtube_check=NOW(),
		    revalidation_required=true,
		    next_health_check_at=NOW(),
		    health_check_not_before=NOW(),
		    health_generation=health_generation+1,
		    updated_at=NOW()
		WHERE id=$1
	`, proxyID, cooldown.Seconds(), status, errorText); err != nil {
		return ReportResult{}, fmt.Errorf("cool down reported proxy: %w", err)
	}
	return ReportResult{
		OK:              true,
		Action:          "cooldown",
		Confirmed:       true,
		ProxyID:         &proxyID,
		CooldownMinutes: int(cooldown / time.Minute),
	}, nil
}

func truncateReportText(value string, maximum int) string {
	if len(value) <= maximum {
		return value
	}
	return value[:maximum]
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
