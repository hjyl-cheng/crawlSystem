package proxymaintenance

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/pkg/logger"
	"github.com/jackc/pgx/v5"
)

const legacyRepairRunID = "legacy-lifecycle-repair-v1"

type Options struct {
	AuditInterval      time.Duration
	RepairEnabled      bool
	RepairBatchSize    int
	RepairSpread       time.Duration
	ConstraintsEnabled bool
	RetentionEnabled   bool
	Retention          time.Duration
	RetentionBatchSize int
	RetentionInterval  time.Duration
}

type Maintainer struct {
	db                   *database.DB
	log                  *logger.Logger
	option               Options
	now                  func() time.Time
	constraintsInstalled bool
}

type AuditResult struct {
	Violations map[string]int `json:"violations"`
	Total      int            `json:"total"`
}

func New(db *database.DB, log *logger.Logger, options Options) *Maintainer {
	if options.AuditInterval <= 0 {
		options.AuditInterval = 5 * time.Minute
	}
	if options.RepairBatchSize <= 0 {
		options.RepairBatchSize = 25
	}
	if options.RepairSpread <= 0 {
		options.RepairSpread = time.Hour
	}
	if options.Retention <= 0 {
		options.Retention = 14 * 24 * time.Hour
	}
	if options.RetentionBatchSize <= 0 {
		options.RetentionBatchSize = 2000
	}
	if options.RetentionInterval <= 0 {
		options.RetentionInterval = time.Minute
	}
	return &Maintainer{db: db, log: log, option: options, now: time.Now}
}

func (m *Maintainer) Run(ctx context.Context) {
	auditTicker := time.NewTicker(m.option.AuditInterval)
	retentionTicker := time.NewTicker(m.option.RetentionInterval)
	defer auditTicker.Stop()
	defer retentionTicker.Stop()

	m.runAudit(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-auditTicker.C:
			audit := m.runAudit(ctx)
			if m.option.RepairEnabled {
				result, err := m.RepairLegacyBatch(ctx)
				if err != nil {
					m.log.Error("proxy lifecycle repair batch failed", "error", err)
				} else if result > 0 {
					m.log.Info("proxy lifecycle repair batch completed", "repaired", result)
				}
			}
			if m.option.ConstraintsEnabled && !m.constraintsInstalled && audit.Total == 0 {
				if err := m.EnsureLifecycleConstraints(ctx); err != nil {
					m.log.Error("proxy lifecycle constraint activation failed", "error", err)
				} else {
					m.constraintsInstalled = true
					m.log.Info("proxy lifecycle database constraints activated")
				}
			}
		case <-retentionTicker.C:
			if !m.option.RetentionEnabled {
				continue
			}
			deleted, err := m.PruneHealthEvidenceBatch(ctx)
			if err != nil {
				m.log.Error("proxy health evidence retention batch failed", "error", err)
			} else if deleted > 0 {
				m.log.Info("proxy health evidence retention batch completed", "deleted", deleted)
			}
		}
	}
}

func (m *Maintainer) runAudit(ctx context.Context) AuditResult {
	result, err := m.Audit(ctx)
	if err != nil {
		m.log.Error("proxy lifecycle invariant audit failed", "error", err)
		return AuditResult{Total: -1}
	}
	if result.Total > 0 {
		m.log.Warn("proxy lifecycle invariant violations detected",
			"total", result.Total, "violations", result.Violations)
	}
	return result
}

func (m *Maintainer) Audit(ctx context.Context) (AuditResult, error) {
	rows, err := m.db.Pool.Query(ctx, `
		SELECT violation, COUNT(*)
		FROM proxy_lifecycle_invariant_violations
		GROUP BY violation
		ORDER BY violation
	`)
	if err != nil {
		return AuditResult{}, fmt.Errorf("query proxy lifecycle invariants: %w", err)
	}
	defer rows.Close()
	result := AuditResult{Violations: make(map[string]int)}
	for rows.Next() {
		var name string
		var count int
		if err := rows.Scan(&name, &count); err != nil {
			return AuditResult{}, err
		}
		result.Violations[name] = count
		result.Total += count
	}
	return result, rows.Err()
}

type repairCandidate struct {
	ProxyID              int
	Status               string
	UpdatedAt            time.Time
	FailedSince          *time.Time
	FailureKind          *string
	EvidenceHealthID     *int64
	EvidenceCheckedAt    *time.Time
	EvidenceResultStatus *string
	EvidenceVerdict      *string
}

func (m *Maintainer) RepairLegacyBatch(ctx context.Context) (int, error) {
	repaired := 0
	err := pgx.BeginFunc(ctx, m.db.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			WITH candidate_ids AS (
			  SELECT id
			  FROM proxies
			  WHERE status IN ('idle', 'failed') AND next_health_check_at IS NULL
			  ORDER BY id
			  FOR UPDATE SKIP LOCKED
			  LIMIT $1
			)
			SELECT p.id, p.status, p.updated_at, p.failed_since, p.failure_episode_kind,
			       health.id, health.checked_at, health.resulting_status, health.verdict
			FROM candidate_ids ids
			JOIN proxies p ON p.id = ids.id
			LEFT JOIN LATERAL (
			  SELECT checks.id, checks.checked_at, checks.resulting_status, checks.verdict
			  FROM proxy_health_checks checks
			  WHERE checks.proxy_id = p.id AND checks.applied = true
			  ORDER BY checks.checked_at DESC, checks.id DESC
			  LIMIT 1
			) health ON true
			ORDER BY p.id
		`, m.option.RepairBatchSize)
		if err != nil {
			return fmt.Errorf("claim legacy lifecycle repairs: %w", err)
		}
		var candidates []repairCandidate
		for rows.Next() {
			var candidate repairCandidate
			if err := rows.Scan(
				&candidate.ProxyID, &candidate.Status, &candidate.UpdatedAt,
				&candidate.FailedSince, &candidate.FailureKind,
				&candidate.EvidenceHealthID, &candidate.EvidenceCheckedAt,
				&candidate.EvidenceResultStatus, &candidate.EvidenceVerdict,
			); err != nil {
				rows.Close()
				return err
			}
			candidates = append(candidates, candidate)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()

		for _, candidate := range candidates {
			action := recommendedRepairAction(candidate)
			delay := deterministicSpread(candidate.ProxyID, m.option.RepairSpread)
			before, _ := json.Marshal(candidate)
			after, err := m.applyRepair(ctx, tx, candidate, action, delay)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO proxy_lifecycle_repair_actions (
				  run_id, proxy_id, planned_action, evidence_health_check_id,
				  before_state, after_state, applied_at
				) VALUES ($1,$2,$3,$4,$5,$6,NOW())
				ON CONFLICT (run_id, proxy_id) DO NOTHING
			`, legacyRepairRunID, candidate.ProxyID, action, candidate.EvidenceHealthID, before, after); err != nil {
				return fmt.Errorf("record legacy lifecycle repair: %w", err)
			}
			repaired++
		}
		return nil
	})
	return repaired, err
}

func recommendedRepairAction(candidate repairCandidate) string {
	if candidate.EvidenceResultStatus != nil {
		switch *candidate.EvidenceResultStatus {
		case "archived":
			return "restore_archive_projection"
		case "failed":
			return "schedule_failed_recovery"
		}
	}
	return "reset_pending_validation"
}

func deterministicSpread(proxyID int, spread time.Duration) time.Duration {
	seconds := int64(spread / time.Second)
	if seconds < 1 {
		return 0
	}
	value := int64(proxyID)
	if value < 0 {
		value = -value
	}
	return time.Duration(value%seconds) * time.Second
}

func (m *Maintainer) applyRepair(
	ctx context.Context,
	tx pgx.Tx,
	candidate repairCandidate,
	action string,
	delay time.Duration,
) ([]byte, error) {
	now := m.now().UTC()
	previousStatus := candidate.Status
	resultingStatus := "idle"
	reason := action

	switch action {
	case "restore_archive_projection":
		resultingStatus = "archived"
		archivedAt := now
		if candidate.EvidenceCheckedAt != nil {
			archivedAt = *candidate.EvidenceCheckedAt
		}
		archiveReason := "legacy_authoritative_archive"
		if candidate.EvidenceVerdict != nil && *candidate.EvidenceVerdict != "" {
			archiveReason = *candidate.EvidenceVerdict
		}
		if _, err := tx.Exec(ctx, `
			UPDATE proxies
			SET status='archived', archived_at=$2, archive_reason=$3,
			    next_health_check_at=NULL, revalidation_required=false,
			    health_generation=health_generation+1,
			    health_check_not_before=$4, updated_at=NOW()
			WHERE id=$1 AND status=$5 AND next_health_check_at IS NULL
		`, candidate.ProxyID, archivedAt, archiveReason, now, candidate.Status); err != nil {
			return nil, fmt.Errorf("restore authoritative archive projection: %w", err)
		}
		reason = archiveReason
	case "schedule_failed_recovery":
		resultingStatus = "failed"
		failureKind := "soft_unreachable"
		if candidate.FailureKind != nil && validFailureKind(*candidate.FailureKind) {
			failureKind = *candidate.FailureKind
		} else if candidate.EvidenceVerdict != nil && validFailureKind(*candidate.EvidenceVerdict) {
			failureKind = *candidate.EvidenceVerdict
		}
		failedSince := now
		if candidate.FailedSince != nil {
			failedSince = *candidate.FailedSince
		} else if candidate.EvidenceCheckedAt != nil {
			failedSince = *candidate.EvidenceCheckedAt
		}
		if _, err := tx.Exec(ctx, `
			UPDATE proxies
			SET status='failed', failed_since=$2,
			    continuous_failed_since=COALESCE(continuous_failed_since,$2),
			    failure_episode_kind=$3, next_health_check_at=$4,
			    revalidation_required=false, health_generation=health_generation+1,
			    health_check_not_before=$5, updated_at=NOW()
			WHERE id=$1 AND status=$6 AND next_health_check_at IS NULL
		`, candidate.ProxyID, failedSince, failureKind, now.Add(delay), now, candidate.Status); err != nil {
			return nil, fmt.Errorf("schedule failed proxy recovery: %w", err)
		}
	default:
		if _, err := tx.Exec(ctx, `
			UPDATE proxies
			SET status='idle', failed_since=NULL, continuous_failed_since=NULL,
			    failure_episode_kind=NULL, next_health_check_at=$2,
			    revalidation_required=false, health_generation=health_generation+1,
			    health_check_not_before=$3, base_health_status=NULL,
			    youtube_health_status=NULL, archived_at=NULL, archive_reason=NULL,
			    last_error=NULL, updated_at=NOW()
			WHERE id=$1 AND status=$4 AND next_health_check_at IS NULL
		`, candidate.ProxyID, now.Add(delay), now, candidate.Status); err != nil {
			return nil, fmt.Errorf("reset proxy pending validation: %w", err)
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO proxy_lifecycle_events (
		  proxy_id, occurred_at, event_kind, previous_status, resulting_status, reason,
		  details
		) VALUES ($1,$2,'legacy_repair',$3,$4,$5,jsonb_build_object('run_id',$6::text))
	`, candidate.ProxyID, now, previousStatus, resultingStatus, reason, legacyRepairRunID); err != nil {
		return nil, fmt.Errorf("record repair lifecycle event: %w", err)
	}

	return json.Marshal(map[string]any{
		"status":            resultingStatus,
		"due_delay_seconds": int64(delay / time.Second),
		"reason":            reason,
	})
}

func validFailureKind(value string) bool {
	return value == "hard_unreachable" || value == "soft_unreachable" || value == "youtube_unusable"
}

func (m *Maintainer) PruneHealthEvidenceBatch(ctx context.Context) (int, error) {
	cutoff := m.now().UTC().Add(-m.option.Retention)
	result, err := m.db.Pool.Exec(ctx, `
		WITH doomed AS (
		  SELECT checks.id
		  FROM proxy_health_checks checks
		  WHERE checks.checked_at < $1
		    AND checks.transition_preserved = true
		  ORDER BY checks.checked_at, checks.id
		  FOR UPDATE SKIP LOCKED
		  LIMIT $2
		)
		DELETE FROM proxy_health_checks checks
		USING doomed
		WHERE checks.id = doomed.id
	`, cutoff, m.option.RetentionBatchSize)
	if err != nil {
		return 0, fmt.Errorf("prune proxy health evidence: %w", err)
	}
	return int(result.RowsAffected()), nil
}

func (m *Maintainer) EnsureLifecycleConstraints(ctx context.Context) error {
	var projectionViolations int
	if err := m.db.Pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM proxy_lifecycle_invariant_violations
		WHERE violation IN (
		  'scheduled_state_without_due',
		  'incomplete_failure_episode',
		  'invalid_archive_projection'
		)
	`).Scan(&projectionViolations); err != nil {
		return fmt.Errorf("check lifecycle projection before constraints: %w", err)
	}
	if projectionViolations != 0 {
		return fmt.Errorf("cannot activate lifecycle constraints with %d projection violations", projectionViolations)
	}
	if _, err := m.db.Pool.Exec(ctx, `
		DO $$
		BEGIN
		  IF NOT EXISTS (
		    SELECT 1 FROM pg_constraint
		    WHERE conname='proxies_scheduled_status_has_due' AND conrelid='proxies'::regclass
		  ) THEN
		    ALTER TABLE proxies ADD CONSTRAINT proxies_scheduled_status_has_due
		      CHECK (status NOT IN ('idle','failed','active') OR next_health_check_at IS NOT NULL) NOT VALID;
		  END IF;
		  IF NOT EXISTS (
		    SELECT 1 FROM pg_constraint
		    WHERE conname='proxies_failed_episode_complete' AND conrelid='proxies'::regclass
		  ) THEN
		    ALTER TABLE proxies ADD CONSTRAINT proxies_failed_episode_complete
		      CHECK (status <> 'failed' OR (
		        failed_since IS NOT NULL AND continuous_failed_since IS NOT NULL
		        AND failure_episode_kind IS NOT NULL
		      )) NOT VALID;
		  END IF;
		  IF NOT EXISTS (
		    SELECT 1 FROM pg_constraint
		    WHERE conname='proxies_archive_projection_complete' AND conrelid='proxies'::regclass
		  ) THEN
		    ALTER TABLE proxies ADD CONSTRAINT proxies_archive_projection_complete
		      CHECK (status <> 'archived' OR (
		        archived_at IS NOT NULL AND archive_reason IS NOT NULL
		        AND next_health_check_at IS NULL
		      )) NOT VALID;
		  END IF;
		END $$;
		ALTER TABLE proxies VALIDATE CONSTRAINT proxies_scheduled_status_has_due;
		ALTER TABLE proxies VALIDATE CONSTRAINT proxies_failed_episode_complete;
		ALTER TABLE proxies VALIDATE CONSTRAINT proxies_archive_projection_complete;
	`); err != nil {
		return fmt.Errorf("activate lifecycle projection constraints: %w", err)
	}
	return nil
}
