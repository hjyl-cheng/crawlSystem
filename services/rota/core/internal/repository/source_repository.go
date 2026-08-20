package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/sourceinventory"
	"github.com/jackc/pgx/v5"
)

// SourceRepository handles proxy_sources database operations
type SourceRepository struct {
	db *database.DB
}

// NewSourceRepository creates a new SourceRepository
func NewSourceRepository(db *database.DB) *SourceRepository {
	return &SourceRepository{db: db}
}

// columns returned by every SELECT — keep in lock-step with scanSource().
const sourceColumns = `
	id, name, url, protocol, enabled, interval_minutes,
	last_fetched_at, last_count, last_total, last_supported, last_skipped, last_error,
	successful_refresh_generation, last_complete_refresh_at,
	cleanup_enabled, cleanup_days, default_tags,
	created_at, updated_at
`

// scanSource scans a single row into ProxySource. row can be pgx.Row or pgx.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

func sourceScanDestinations(s *models.ProxySource) []any {
	return []any{
		&s.ID, &s.Name, &s.URL, &s.Protocol, &s.Enabled,
		&s.IntervalMinutes, &s.LastFetchedAt, &s.LastCount, &s.LastTotal,
		&s.LastSupported, &s.LastSkipped, &s.LastError,
		&s.SuccessfulRefreshGeneration, &s.LastCompleteRefreshAt,
		&s.CleanupEnabled, &s.CleanupDays, &s.DefaultTags,
		&s.CreatedAt, &s.UpdatedAt,
	}
}

func scanSource(r rowScanner, s *models.ProxySource) error {
	return r.Scan(sourceScanDestinations(s)...)
}

// List returns all proxy sources
func (r *SourceRepository) List(ctx context.Context) ([]models.ProxySource, error) {
	query := `
		SELECT ` + sourceColumns + `,
		       (
		         SELECT COUNT(*)
		         FROM proxy_source_memberships AS membership
		         JOIN proxies AS proxy ON proxy.id = membership.proxy_id
		         WHERE membership.source_id = proxy_sources.id
		           AND proxy.status = 'active'
		       ) AS active_count
		FROM proxy_sources
		ORDER BY created_at DESC
	`
	rows, err := r.db.Pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list sources: %w", err)
	}
	defer rows.Close()

	var sources []models.ProxySource
	for rows.Next() {
		var s models.ProxySource
		destinations := append(sourceScanDestinations(&s), &s.ActiveCount)
		if err := rows.Scan(destinations...); err != nil {
			return nil, fmt.Errorf("failed to scan source: %w", err)
		}
		sources = append(sources, s)
	}
	if sources == nil {
		sources = []models.ProxySource{}
	}
	return sources, nil
}

// GetByID returns a source by ID
func (r *SourceRepository) GetByID(ctx context.Context, id int) (*models.ProxySource, error) {
	query := `SELECT ` + sourceColumns + ` FROM proxy_sources WHERE id = $1`
	var s models.ProxySource
	err := scanSource(r.db.Pool.QueryRow(ctx, query, id), &s)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get source: %w", err)
	}
	return &s, nil
}

// Create inserts a new source
func (r *SourceRepository) Create(ctx context.Context, req models.CreateProxySourceRequest) (*models.ProxySource, error) {
	query := `
		INSERT INTO proxy_sources (
			name, url, protocol, enabled, interval_minutes, default_tags
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING ` + sourceColumns
	var s models.ProxySource
	err := scanSource(r.db.Pool.QueryRow(ctx, query,
		req.Name, req.URL, req.Protocol, req.Enabled, req.IntervalMinutes,
		normalizeProxyTags(req.DefaultTags),
	), &s)
	if err != nil {
		return nil, fmt.Errorf("failed to create source: %w", err)
	}
	return &s, nil
}

// Update modifies an existing source. Empty/zero fields are left unchanged.
func (r *SourceRepository) Update(ctx context.Context, id int, req models.UpdateProxySourceRequest) (*models.ProxySource, error) {
	var defaultTags []string
	if req.DefaultTags != nil {
		defaultTags = normalizeProxyTags(*req.DefaultTags)
	}
	query := `
		UPDATE proxy_sources SET
			name             = CASE WHEN $1 <> '' THEN $1 ELSE name END,
			url              = CASE WHEN $2 <> '' THEN $2 ELSE url END,
			protocol         = CASE WHEN $3 <> '' THEN $3 ELSE protocol END,
			enabled          = COALESCE($4, enabled),
			interval_minutes = CASE WHEN $5 > 0 THEN $5 ELSE interval_minutes END,
			default_tags     = CASE WHEN $6::boolean THEN $7::text[] ELSE default_tags END,
			cleanup_enabled  = COALESCE($8, cleanup_enabled),
			cleanup_days     = CASE WHEN $9::int > 0 THEN $9 ELSE cleanup_days END,
			updated_at       = NOW()
		WHERE id = $10
		RETURNING ` + sourceColumns
	var s models.ProxySource
	err := scanSource(r.db.Pool.QueryRow(ctx, query,
		req.Name, req.URL, req.Protocol, req.Enabled, req.IntervalMinutes,
		req.DefaultTags != nil, defaultTags, req.CleanupEnabled, req.CleanupDays, id,
	), &s)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to update source: %w", err)
	}
	return &s, nil
}

// ReconcileCompleteRefresh advances source membership state only after the
// caller has completed transport, parsing, and every supported proxy upsert.
// Shadow mode records the exact candidates but never retires or archives.
func (r *SourceRepository) ReconcileCompleteRefresh(
	ctx context.Context,
	refresh sourceinventory.CompleteRefresh,
	policy sourceinventory.Policy,
) (sourceinventory.Result, error) {
	result := sourceinventory.Result{ObservedCount: len(refresh.NodeIdentities)}
	if policy.Mode == sourceinventory.ModeOff {
		return result, r.MarkSeen(ctx, refresh.SourceID, refresh.NodeIdentities)
	}
	if refresh.CompletedAt.IsZero() {
		refresh.CompletedAt = time.Now().UTC()
	}

	err := pgx.BeginFunc(ctx, r.db.Pool, func(tx pgx.Tx) error {
		var cleanupEnabled bool
		var cleanupDays int
		if err := tx.QueryRow(ctx, `
			UPDATE proxy_sources
			SET successful_refresh_generation = successful_refresh_generation + 1,
			    last_complete_refresh_at = $2,
			    updated_at = NOW()
			WHERE id = $1
			RETURNING successful_refresh_generation, cleanup_enabled, cleanup_days
		`, refresh.SourceID, refresh.CompletedAt).Scan(
			&result.Generation, &cleanupEnabled, &cleanupDays,
		); err != nil {
			return fmt.Errorf("advance source refresh generation: %w", err)
		}

		if len(refresh.NodeIdentities) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE proxy_source_memberships AS membership
				SET last_seen_at = $3,
				    last_seen_generation = $4,
				    consecutive_absences = 0,
				    missing_since = NULL,
				    retired_at = NULL,
				    retirement_reason = NULL
				FROM proxies
				WHERE membership.source_id = $1
				  AND proxies.id = membership.proxy_id
				  AND proxies.node_identity = ANY($2::text[])
			`, refresh.SourceID, refresh.NodeIdentities, refresh.CompletedAt, result.Generation); err != nil {
				return fmt.Errorf("mark source generation members: %w", err)
			}
		}

		if err := tx.QueryRow(ctx, `
			WITH missing AS (
			  UPDATE proxy_source_memberships
			  SET consecutive_absences = consecutive_absences + 1,
			      missing_since = COALESCE(missing_since, $3)
			  WHERE source_id = $1
			    AND retired_at IS NULL
			    AND last_seen_generation < $2
			  RETURNING consecutive_absences
			)
			SELECT COUNT(*) FILTER (WHERE consecutive_absences = 1)
			FROM missing
		`, refresh.SourceID, result.Generation, refresh.CompletedAt).Scan(&result.NewlyMissingCount); err != nil {
			return fmt.Errorf("advance source member absences: %w", err)
		}

		minimumAbsence := time.Duration(cleanupDays) * 24 * time.Hour
		if cleanupDays < 1 {
			return fmt.Errorf("source %d cleanup_days must be positive", refresh.SourceID)
		}
		minimumAbsence = time.Duration(cleanupDays) * 24 * time.Hour
		eligibleBefore := refresh.CompletedAt.Add(-minimumAbsence)
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM proxy_source_memberships
			WHERE source_id = $1
			  AND retired_at IS NULL
			  AND consecutive_absences >= $2
			  AND missing_since <= $3
		`, refresh.SourceID, policy.MinimumMisses, eligibleBefore).Scan(&result.EligibleCount); err != nil {
			return fmt.Errorf("count source retirement candidates: %w", err)
		}

		enforce := policy.Mode == sourceinventory.ModeEnforce && cleanupEnabled
		if enforce && result.EligibleCount > 0 {
			tag, err := tx.Exec(ctx, `
				UPDATE proxy_source_memberships
				SET retired_at = $4, retirement_reason = 'source_absent'
				WHERE source_id = $1
				  AND retired_at IS NULL
				  AND consecutive_absences >= $2
				  AND missing_since <= $3
			`, refresh.SourceID, policy.MinimumMisses, eligibleBefore, refresh.CompletedAt)
			if err != nil {
				return fmt.Errorf("retire absent source memberships: %w", err)
			}
			result.RetiredMembershipCount = int(tag.RowsAffected())
		}

		if enforce {
			reactivated, err := reactivateReappearedSourceProxies(ctx, tx, refresh, result.Generation)
			if err != nil {
				return err
			}
			result.ReactivatedProxyCount = reactivated

			archived, err := archiveOrphanedSourceProxies(ctx, tx, refresh.SourceID, refresh.CompletedAt)
			if err != nil {
				return err
			}
			result.ArchivedProxyCount = archived
		}

		_, err := tx.Exec(ctx, `
			INSERT INTO proxy_inventory_reconciliation_runs (
			  source_id, refresh_generation, mode, observed_count,
			  newly_missing_count, eligible_count, retired_membership_count,
			  archived_proxy_count, reactivated_proxy_count, completed_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		`, refresh.SourceID, result.Generation, string(policy.Mode), result.ObservedCount,
			result.NewlyMissingCount, result.EligibleCount, result.RetiredMembershipCount,
			result.ArchivedProxyCount, result.ReactivatedProxyCount, refresh.CompletedAt)
		if err != nil {
			return fmt.Errorf("record source inventory reconciliation: %w", err)
		}
		return nil
	})
	return result, err
}

func reactivateReappearedSourceProxies(
	ctx context.Context,
	tx pgx.Tx,
	refresh sourceinventory.CompleteRefresh,
	generation int64,
) (int, error) {
	if len(refresh.NodeIdentities) == 0 {
		return 0, nil
	}
	rows, err := tx.Query(ctx, `
		WITH candidates AS (
		  SELECT p.id
		  FROM proxies p
		  JOIN proxy_source_memberships m ON m.proxy_id = p.id
		  WHERE m.source_id = $1
		    AND m.last_seen_generation = $2
		    AND p.status = 'archived'
		    AND p.archive_reason = 'source_retired'
		  FOR UPDATE OF p
		), changed AS (
		  UPDATE proxies p
		  SET status = 'idle', failed_since = NULL, continuous_failed_since = NULL,
		      failure_episode_kind = NULL, next_health_check_at = $3,
		      revalidation_required = false, health_generation = health_generation + 1,
		      health_check_not_before = $3, base_health_status = NULL,
		      youtube_health_status = NULL, archived_at = NULL, archive_reason = NULL,
		      last_error = NULL, updated_at = NOW()
		  FROM candidates c
		  WHERE p.id = c.id
		  RETURNING p.id
		)
		INSERT INTO proxy_lifecycle_events (
		  proxy_id, occurred_at, event_kind, previous_status, resulting_status, reason
		)
		SELECT id, $3, 'source_reappeared', 'archived', 'idle', 'source_reappeared'
		FROM changed
		RETURNING proxy_id
	`, refresh.SourceID, generation, refresh.CompletedAt)
	if err != nil {
		return 0, fmt.Errorf("reactivate reappeared source proxies: %w", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	return count, rows.Err()
}

func archiveOrphanedSourceProxies(ctx context.Context, tx pgx.Tx, sourceID int, now time.Time) (int, error) {
	rows, err := tx.Query(ctx, `
		WITH candidates AS (
		  SELECT p.id, p.status
		  FROM proxies p
		  JOIN proxy_source_memberships trigger_membership
		    ON trigger_membership.proxy_id = p.id
		   AND trigger_membership.source_id = $2
		   AND trigger_membership.retired_at = $1
		  WHERE p.status <> 'archived'
		    AND NOT ('origin:paid' = ANY(COALESCE(p.tags, '{}'::text[])))
		    AND EXISTS (SELECT 1 FROM proxy_source_memberships m WHERE m.proxy_id = p.id)
		    AND NOT EXISTS (
		      SELECT 1 FROM proxy_source_memberships m
		      WHERE m.proxy_id = p.id AND m.retired_at IS NULL
		    )
		    AND NOT EXISTS (
		      SELECT 1 FROM proxy_running_slots s WHERE s.proxy_id = p.id
		    )
		    AND (
		      p.last_youtube_success IS NULL OR p.last_youtube_success < (
		        SELECT MIN(m.missing_since)
		        FROM proxy_source_memberships m WHERE m.proxy_id = p.id
		      )
		    )
		  FOR UPDATE OF p SKIP LOCKED
		), changed AS (
		  UPDATE proxies p
		  SET status = 'archived', archived_at = $1, archive_reason = 'source_retired',
		      next_health_check_at = NULL, revalidation_required = false,
		      health_generation = health_generation + 1,
		      health_check_not_before = $1, updated_at = NOW()
		  FROM candidates c
		  WHERE p.id = c.id
		  RETURNING p.id, c.status AS previous_status
		)
		INSERT INTO proxy_lifecycle_events (
		  proxy_id, occurred_at, event_kind, previous_status, resulting_status, reason
		)
		SELECT id, $1, 'source_retired', previous_status, 'archived', 'source_retired'
		FROM changed
		RETURNING proxy_id
	`, now, sourceID)
	if err != nil {
		return 0, fmt.Errorf("archive orphaned source proxies: %w", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	return count, rows.Err()
}

// Delete removes a source
func (r *SourceRepository) Delete(ctx context.Context, id int) error {
	_, err := r.db.Pool.Exec(ctx, `DELETE FROM proxy_sources WHERE id = $1`, id)
	return err
}

// UpdateFetchResult records the outcome of a fetch run.
// imported = newly created on this fetch; total = total parseable lines returned.
func (r *SourceRepository) UpdateFetchResult(
	ctx context.Context,
	id int,
	imported int,
	total int,
	supported int,
	skipped int,
	fetchErr error,
) error {
	var errMsg *string
	if fetchErr != nil {
		s := fetchErr.Error()
		errMsg = &s
	}
	now := time.Now()
	_, err := r.db.Pool.Exec(ctx, `
		UPDATE proxy_sources
		SET last_fetched_at = $1,
		    last_count      = CASE WHEN $6::text IS NULL THEN $2 ELSE last_count END,
		    last_total      = CASE WHEN $6::text IS NULL THEN $3 ELSE last_total END,
		    last_supported  = CASE WHEN $6::text IS NULL THEN $4 ELSE last_supported END,
		    last_skipped    = CASE WHEN $6::text IS NULL THEN $5 ELSE last_skipped END,
		    last_error      = $6,
		    updated_at      = NOW()
		WHERE id = $7
	`, now, imported, total, supported, skipped, errMsg, id)
	return err
}

// MarkSeen updates source membership timestamps for canonical node identities.
func (r *SourceRepository) MarkSeen(ctx context.Context, sourceID int, identities []string) error {
	if len(identities) == 0 {
		return nil
	}
	_, err := r.db.Pool.Exec(ctx, `
		UPDATE proxy_source_memberships AS membership
		SET last_seen_at = NOW()
		FROM proxies
		WHERE membership.source_id = $1
		  AND proxies.id = membership.proxy_id
		  AND proxies.node_identity = ANY($2::text[])
	`, sourceID, identities)
	return err
}

// GetDueForFetch returns sources that are enabled and overdue for refresh
func (r *SourceRepository) GetDueForFetch(ctx context.Context) ([]models.ProxySource, error) {
	query := `
		SELECT ` + sourceColumns + `
		FROM proxy_sources
		WHERE enabled = true
		  AND (last_fetched_at IS NULL
		       OR last_fetched_at + (interval_minutes * interval '1 minute') <= NOW())
		ORDER BY last_fetched_at ASC NULLS FIRST
	`
	rows, err := r.db.Pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query due sources: %w", err)
	}
	defer rows.Close()

	var sources []models.ProxySource
	for rows.Next() {
		var s models.ProxySource
		if err := scanSource(rows, &s); err != nil {
			return nil, err
		}
		sources = append(sources, s)
	}
	return sources, nil
}
