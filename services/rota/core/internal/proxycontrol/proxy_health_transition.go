package proxycontrol

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/alpkeskin/rota/core/internal/proxylifecycle"
	"github.com/jackc/pgx/v5"
)

func (m *Manager) quarantineProxy(
	ctx context.Context,
	tx pgx.Tx,
	proxyID int,
	kind proxylifecycle.FailureKind,
	reason string,
	eventKind string,
) error {
	cooldown := m.options.FailureCooldown
	if kind == proxylifecycle.FailureHardUnreachable || kind == proxylifecycle.FailureSoftUnreachable {
		cooldown = m.options.NetworkCooldown
	}
	if cooldown <= 0 {
		cooldown = 5 * time.Minute
	}
	nextCheck := time.Now().UTC().Add(cooldown)
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = string(kind)
	}

	var changedID int
	err := tx.QueryRow(ctx, `
		WITH current AS (
		  SELECT id,status,failed_since,continuous_failed_since,failure_episode_kind
		  FROM proxies
		  WHERE id=$1 AND status<>'archived'
		  FOR UPDATE
		), changed AS (
		  UPDATE proxies p
		  SET status='failed',
		      failed_since=CASE
		        WHEN current.status='failed'
		         AND current.failure_episode_kind=$2
		         AND current.failed_since IS NOT NULL
		        THEN current.failed_since
		        ELSE NOW()
		      END,
		      continuous_failed_since=COALESCE(
		        current.continuous_failed_since,current.failed_since,NOW()
		      ),
		      failure_episode_kind=$2,
		      cooldown_until=GREATEST(COALESCE(p.cooldown_until,NOW()),$3),
		      next_health_check_at=$3,
		      revalidation_required=false,
		      health_check_not_before=NOW(),
		      health_generation=p.health_generation+1,
		      base_health_status=CASE
		        WHEN $2 IN ('hard_unreachable','soft_unreachable') THEN 'failed'
		        ELSE p.base_health_status
		      END,
		      youtube_health_status=CASE
		        WHEN $2='youtube_unusable' THEN 'failed'
		        ELSE 'not_run'
		      END,
		      last_error=$4,
		      updated_at=NOW()
		  FROM current
		  WHERE p.id=current.id
		  RETURNING p.id,current.status AS previous_status
		), lifecycle_event AS (
		  INSERT INTO proxy_lifecycle_events (
		    proxy_id,occurred_at,event_kind,previous_status,resulting_status,reason
		  )
		  SELECT id,NOW(),$5,previous_status,'failed',$4 FROM changed
		)
		SELECT id FROM changed
	`, proxyID, string(kind), nextCheck, reason, eventKind).Scan(&changedID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("quarantine proxy %d after %s: %w", proxyID, eventKind, err)
	}
	return nil
}

func reportFailureKind(request ReportRequest) proxylifecycle.FailureKind {
	switch proxylifecycle.FailureKind(strings.ToLower(strings.TrimSpace(request.FailureKind))) {
	case proxylifecycle.FailureHardUnreachable:
		return proxylifecycle.FailureHardUnreachable
	case proxylifecycle.FailureSoftUnreachable:
		return proxylifecycle.FailureSoftUnreachable
	case proxylifecycle.FailureYouTubeUnusable:
		return proxylifecycle.FailureYouTubeUnusable
	}
	if request.ErrorType == "proxy_unavailable" || request.Status != nil && *request.Status == 502 {
		return proxylifecycle.FailureHardUnreachable
	}
	return proxylifecycle.FailureYouTubeUnusable
}

func completionFailureKind(
	ctx context.Context,
	tx pgx.Tx,
	state completionTaskState,
) (proxylifecycle.FailureKind, error) {
	if state.pendingAction == PendingActionRotateProfile {
		return proxylifecycle.FailureYouTubeUnusable, nil
	}
	var observationKind string
	err := tx.QueryRow(ctx, `
		SELECT kind
		FROM proxy_control_observations
		WHERE workload_scope=$1 AND incident_id=$2
		ORDER BY created_at DESC
		LIMIT 1
	`, state.workloadScope, state.pendingIncidentID).Scan(&observationKind)
	if errors.Is(err, pgx.ErrNoRows) {
		return proxylifecycle.FailureSoftUnreachable, nil
	}
	if err != nil {
		return proxylifecycle.FailureNone, fmt.Errorf("load completion failure observation: %w", err)
	}
	if observationKind == ObservationProxyTransport {
		return proxylifecycle.FailureHardUnreachable, nil
	}
	return proxylifecycle.FailureYouTubeUnusable, nil
}
