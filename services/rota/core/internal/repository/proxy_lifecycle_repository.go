package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/proxylifecycle"
	"github.com/jackc/pgx/v5"
)

var ErrProxyNotFound = errors.New("proxy not found")

func (r *ProxyRepository) ApplyHealthVerdict(
	ctx context.Context,
	proxyID int,
	evidence proxylifecycle.HealthEvidence,
	policy proxylifecycle.Policy,
) (proxylifecycle.Decision, bool, error) {
	var decision proxylifecycle.Decision
	applied := false

	err := pgx.BeginFunc(ctx, r.db.Pool, func(tx pgx.Tx) error {
		var status string
		var failedSince, continuousFailedSince, lastHealthCheckAt, healthCheckNotBefore *time.Time
		var failureKind string
		var revalidationRequired bool
		err := tx.QueryRow(ctx, `
			SELECT status, failed_since, continuous_failed_since,
			       COALESCE(failure_episode_kind, ''), revalidation_required,
			       last_health_check_at, health_check_not_before
			FROM proxies
			WHERE id = $1
			FOR UPDATE
		`, proxyID).Scan(
			&status, &failedSince, &continuousFailedSince, &failureKind,
			&revalidationRequired, &lastHealthCheckAt, &healthCheckNotBefore,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrProxyNotFound
		}
		if err != nil {
			return fmt.Errorf("lock proxy lifecycle: %w", err)
		}
		var boundToSlot bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
			  SELECT 1
			  FROM proxy_running_slots slot
			  WHERE slot.proxy_id=$1
			    AND slot.current_lease_id IS NOT NULL
			    AND slot.lease_until > statement_timestamp()
			    AND EXISTS (
			      SELECT 1
			      FROM proxy_control_leases live_lease
			      WHERE live_lease.lease_id=slot.current_lease_id
			        AND live_lease.slot_name=slot.slot_name
			        AND live_lease.status='active'
			        AND live_lease.lease_until > statement_timestamp()
			    )
			)
		`, proxyID).Scan(&boundToSlot); err != nil {
			return fmt.Errorf("check proxy slot binding: %w", err)
		}

		current := proxylifecycle.Snapshot{
			Status:                proxylifecycle.Status(status),
			FailedSince:           failedSince,
			ContinuousFailedSince: continuousFailedSince,
			FailureKind:           proxylifecycle.FailureKind(failureKind),
			RevalidationRequired:  revalidationRequired,
		}
		if evidence.Verdict.Healthy &&
			evidence.YouTube.Status != proxylifecycle.ProbePassed {
			evidence.Verdict.Healthy = false
			evidence.Verdict.Conclusive = false
			if evidence.Error == "" {
				evidence.Error = "healthy verdict is missing passed YouTube evidence"
			}
		}
		decision = policy.Decide(evidence.CheckedAt, current, evidence.Verdict)
		verifiedHealthy := decision.Status == proxylifecycle.StatusActive &&
			evidence.Verdict.Healthy && evidence.Verdict.Conclusive &&
			evidence.YouTube.Status == proxylifecycle.ProbePassed

		if current.Status == proxylifecycle.StatusArchived || boundToSlot ||
			healthEvidenceIsStale(evidence, lastHealthCheckAt, healthCheckNotBefore) {
			decision = proxylifecycle.Decision{Status: current.Status}
			_, insertErr := insertHealthEvidence(ctx, tx, proxyID, evidence, current.Status, current.Status, false)
			return insertErr
		}

		encoded, err := json.Marshal(evidence)
		if err != nil {
			return fmt.Errorf("encode health verdict: %w", err)
		}
		lastError := evidenceError(evidence)
		_, err = tx.Exec(ctx, `
			UPDATE proxies
			SET status = $2,
			    cooldown_until = CASE WHEN $9 THEN NULL ELSE cooldown_until END,
			    failed_since = $3,
			    continuous_failed_since = $4,
			    failure_episode_kind = NULLIF($5, ''),
			    next_health_check_at = $6,
			    revalidation_required = $7,
			    last_health_check_at = $8,
			    last_health_success_at = CASE WHEN $9 THEN $8 ELSE last_health_success_at END,
			    base_health_status = $10,
			    youtube_health_status = $11,
			    last_health_verdict = $12,
			    archived_at = $13,
			    archive_reason = NULLIF($14, ''),
			    last_check = $8,
			    last_error = NULLIF($15, ''),
				    last_rota_youtube_status = CASE WHEN $9 THEN 200 ELSE $16 END,
				    last_rota_youtube_error = CASE WHEN $9 THEN NULL ELSE NULLIF($15, '') END,
				    last_rota_youtube_check = $8,
				    health_generation = health_generation + 1,
				    updated_at = NOW()
			WHERE id = $1
		`, proxyID, string(decision.Status), decision.FailedSince, decision.ContinuousFailedSince,
			string(decision.FailureKind), decision.NextHealthCheckAt, decision.RevalidationRequired,
			evidence.CheckedAt, verifiedHealthy,
			string(evidence.Base.Status), string(evidence.YouTube.Status), encoded,
			decision.ArchivedAt, decision.ArchiveReason, lastError, evidence.YouTube.HTTPStatus)
		if err != nil {
			return fmt.Errorf("apply health verdict: %w", err)
		}

		healthCheckID, err := insertHealthEvidence(ctx, tx, proxyID, evidence, current.Status, decision.Status, true)
		if err != nil {
			return err
		}
		if current.Status != decision.Status {
			if err := insertLifecycleEvent(
				ctx, tx, proxyID, &healthCheckID, evidence.CheckedAt, "health_verdict",
				current.Status, decision.Status, firstNonEmptyString(decision.ArchiveReason, verdictName(evidence.Verdict)), nil,
			); err != nil {
				return err
			}
		}
		applied = true
		return nil
	})
	if err != nil {
		return proxylifecycle.Decision{}, false, err
	}
	return decision, applied, nil
}

func healthEvidenceIsStale(evidence proxylifecycle.HealthEvidence, lastHealthCheckAt, healthCheckNotBefore *time.Time) bool {
	startedAt := evidence.StartedAt
	if startedAt.IsZero() {
		startedAt = evidence.CheckedAt
	}
	return (lastHealthCheckAt != nil && !startedAt.After(*lastHealthCheckAt)) ||
		(healthCheckNotBefore != nil && startedAt.Before(*healthCheckNotBefore))
}

func insertHealthEvidence(
	ctx context.Context,
	tx pgx.Tx,
	proxyID int,
	evidence proxylifecycle.HealthEvidence,
	previousStatus, resultingStatus proxylifecycle.Status,
	applied bool,
) (int64, error) {
	baseResult, err := json.Marshal(evidence.Base)
	if err != nil {
		return 0, err
	}
	youtubeResult, err := json.Marshal(evidence.YouTube)
	if err != nil {
		return 0, err
	}
	var healthCheckID int64
	transitionPreserved := !applied || previousStatus == resultingStatus
	err = tx.QueryRow(ctx, `
		INSERT INTO proxy_health_checks (
			proxy_id, started_at, checked_at, base_result, youtube_result, verdict,
			conclusive, control_path_healthy, previous_status, resulting_status,
			applied, transition_preserved, error
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NULLIF($13,''))
		RETURNING id
	`, proxyID, evidence.StartedAt, evidence.CheckedAt, baseResult, youtubeResult, verdictName(evidence.Verdict),
		evidence.Verdict.Conclusive, evidence.Verdict.ControlPathHealthy,
		string(previousStatus), string(resultingStatus), applied, transitionPreserved, evidenceError(evidence)).Scan(&healthCheckID)
	if err != nil {
		return 0, fmt.Errorf("insert health evidence: %w", err)
	}
	return healthCheckID, nil
}

func insertLifecycleEvent(
	ctx context.Context,
	tx pgx.Tx,
	proxyID int,
	healthCheckID *int64,
	occurredAt time.Time,
	eventKind string,
	previousStatus, resultingStatus proxylifecycle.Status,
	reason string,
	details []byte,
) error {
	if len(details) == 0 {
		details = []byte(`{}`)
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO proxy_lifecycle_events (
			proxy_id, health_check_id, occurred_at, event_kind,
			previous_status, resulting_status, reason, details
		) VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,''),$8)
		ON CONFLICT (health_check_id) DO NOTHING
	`, proxyID, healthCheckID, occurredAt, eventKind, string(previousStatus), string(resultingStatus), reason, details)
	if err != nil {
		return fmt.Errorf("insert lifecycle event: %w", err)
	}
	if healthCheckID != nil {
		result, updateErr := tx.Exec(ctx, `
			UPDATE proxy_health_checks
			SET transition_preserved = true
			WHERE id = $1 AND proxy_id = $2
		`, *healthCheckID, proxyID)
		if updateErr != nil {
			return fmt.Errorf("mark lifecycle transition preserved: %w", updateErr)
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("mark lifecycle transition preserved: health check %d not found", *healthCheckID)
		}
	}
	return nil
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func verdictName(verdict proxylifecycle.Verdict) string {
	if verdict.Healthy && verdict.Conclusive {
		return "healthy"
	}
	if !verdict.Conclusive || verdict.Kind == proxylifecycle.FailureNone {
		return "inconclusive"
	}
	return string(verdict.Kind)
}

func evidenceError(evidence proxylifecycle.HealthEvidence) string {
	if evidence.Error != "" {
		return evidence.Error
	}
	if evidence.YouTube.Error != "" {
		return evidence.YouTube.Error
	}
	return evidence.Base.Error
}

func (r *ProxyRepository) ClaimDueHealthChecks(ctx context.Context, limit int) ([]*models.Proxy, error) {
	if limit < 1 {
		limit = 100
	}
	proxies := make([]*models.Proxy, 0)
	err := pgx.BeginFunc(ctx, r.db.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			WITH expired AS (
			  SELECT p.id
			  FROM proxies p
			  WHERE p.status='active'
			    AND p.next_health_check_at IS NOT NULL
			    AND p.next_health_check_at <= NOW()
			    AND NOT EXISTS (
			      SELECT 1
			      FROM proxy_running_slots slot
			      WHERE slot.proxy_id=p.id
			        AND slot.current_lease_id IS NOT NULL
			        AND slot.lease_until > statement_timestamp()
			        AND EXISTS (
			          SELECT 1
			          FROM proxy_control_leases live_lease
			          WHERE live_lease.lease_id=slot.current_lease_id
			            AND live_lease.slot_name=slot.slot_name
			            AND live_lease.status='active'
			            AND live_lease.lease_until > statement_timestamp()
			        )
			    )
			  ORDER BY p.next_health_check_at,p.id
			  FOR UPDATE OF p SKIP LOCKED
			), changed AS (
			  UPDATE proxies p
			  SET status='idle',revalidation_required=false,
			      health_generation=health_generation+1,
			      health_check_not_before=NOW(),updated_at=NOW()
			  FROM expired
			  WHERE p.id=expired.id
			  RETURNING p.id
			)
			INSERT INTO proxy_lifecycle_events (
			  proxy_id,occurred_at,event_kind,previous_status,resulting_status,reason
			)
			SELECT id,NOW(),'scheduled_revalidation','active','idle','active_recheck_due'
			FROM changed
		`); err != nil {
			return fmt.Errorf("move due active proxies to pending validation: %w", err)
		}

		rows, err := tx.Query(ctx, `
			WITH due AS (
				SELECT p.id
				FROM proxies p
				WHERE p.status IN ('idle', 'failed')
				  AND p.next_health_check_at IS NOT NULL
				  AND p.next_health_check_at <= NOW()
				  AND NOT EXISTS (
				    SELECT 1
				    FROM proxy_running_slots slot
				    WHERE slot.proxy_id=p.id
				      AND slot.current_lease_id IS NOT NULL
				      AND slot.lease_until > statement_timestamp()
				      AND EXISTS (
				        SELECT 1
				        FROM proxy_control_leases live_lease
				        WHERE live_lease.lease_id=slot.current_lease_id
				          AND live_lease.slot_name=slot.slot_name
				          AND live_lease.status='active'
				          AND live_lease.lease_until > statement_timestamp()
				      )
				  )
				ORDER BY p.next_health_check_at, p.id
				FOR UPDATE OF p SKIP LOCKED
				LIMIT $1
			)
			UPDATE proxies p
			SET next_health_check_at = NOW() + INTERVAL '5 minutes'
			FROM due
			WHERE p.id = due.id
			RETURNING p.id, p.address, p.protocol, p.username, p.password, p.status,
			          p.requests, p.successful_requests, p.failed_requests,
			          p.avg_response_time, p.last_check, p.last_error,
			          p.created_at, p.updated_at
		`, limit)
		if err != nil {
			return fmt.Errorf("claim due health checks: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var proxy models.Proxy
			if err := rows.Scan(
				&proxy.ID, &proxy.Address, &proxy.Protocol, &proxy.Username, &proxy.Password, &proxy.Status,
				&proxy.Requests, &proxy.SuccessfulRequests, &proxy.FailedRequests,
				&proxy.AvgResponseTime, &proxy.LastCheck, &proxy.LastError,
				&proxy.CreatedAt, &proxy.UpdatedAt,
			); err != nil {
				return fmt.Errorf("scan due health check: %w", err)
			}
			proxies = append(proxies, &proxy)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return proxies, nil
}

func (r *ProxyRepository) Archive(ctx context.Context, ids []int, reason string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "manual"
	}
	changed := 0
	err := pgx.BeginFunc(ctx, r.db.Pool, func(tx pgx.Tx) error {
		now := time.Now()
		rows, err := tx.Query(ctx, `
			WITH candidates AS (
			  SELECT id, status FROM proxies
			  WHERE id = ANY($1) AND status <> 'archived'
			  FOR UPDATE
			), changed AS (
			UPDATE proxies p
			SET status = 'archived', archived_at = NOW(), archive_reason = $2,
			    next_health_check_at = NULL, revalidation_required = false,
			    health_generation = health_generation + 1,
			    health_check_not_before = NOW(), updated_at = NOW()
			FROM candidates c
			WHERE p.id = c.id
			RETURNING p.id, c.status AS previous_status
			)
			SELECT id, previous_status FROM changed
		`, ids, reason)
		if err != nil {
			return fmt.Errorf("archive proxies: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var proxyID int
			var previousStatus string
			if err := rows.Scan(&proxyID, &previousStatus); err != nil {
				return err
			}
			if err := insertLifecycleEvent(ctx, tx, proxyID, nil, now, "manual_archive",
				proxylifecycle.Status(previousStatus), proxylifecycle.StatusArchived, reason, nil); err != nil {
				return err
			}
			changed++
		}
		return rows.Err()
	})
	return changed, err
}

func (r *ProxyRepository) Restore(ctx context.Context, ids []int) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	changed := 0
	err := pgx.BeginFunc(ctx, r.db.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			UPDATE proxies
			SET status = 'idle', failed_since = NULL, continuous_failed_since = NULL,
			    failure_episode_kind = NULL, next_health_check_at = NOW(),
			    revalidation_required = false, health_generation = health_generation + 1,
			    health_check_not_before = NOW(), base_health_status = NULL,
			    youtube_health_status = NULL, archived_at = NULL, archive_reason = NULL,
			    last_error = NULL, updated_at = NOW()
			WHERE id = ANY($1) AND status = 'archived'
			RETURNING id
		`, ids)
		if err != nil {
			return fmt.Errorf("restore proxies: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var proxyID int
			if err := rows.Scan(&proxyID); err != nil {
				return err
			}
			if err := insertLifecycleEvent(ctx, tx, proxyID, nil, time.Now(), "manual_restore",
				proxylifecycle.StatusArchived, proxylifecycle.StatusIdle, "manual_restore", nil); err != nil {
				return err
			}
			changed++
		}
		return rows.Err()
	})
	return changed, err
}
