package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/proxyidentity"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ProxyRepository handles proxy database operations
type ProxyRepository struct {
	db *database.DB
}

// NewProxyRepository creates a new ProxyRepository
func NewProxyRepository(db *database.DB) *ProxyRepository {
	return &ProxyRepository{db: db}
}

// GetDB returns the database instance
func (r *ProxyRepository) GetDB() *database.DB {
	return r.db
}

// List retrieves proxies with pagination and filters
func (r *ProxyRepository) List(ctx context.Context, page, limit int, search, status, protocol, tag, sortField, sortOrder string) ([]models.ProxyWithStats, int, error) {
	// Build WHERE clause
	whereClauses := []string{}
	args := []interface{}{}
	argPos := 1

	if search != "" {
		// Use both ILIKE for simple search and to_tsvector for full-text search
		whereClauses = append(whereClauses, fmt.Sprintf("(address ILIKE $%d OR to_tsvector('simple', address) @@ plainto_tsquery('simple', $%d))", argPos, argPos))
		args = append(args, "%"+search+"%")
		argPos++
	}

	if status != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("status = $%d", argPos))
		args = append(args, status)
		argPos++
	}

	if protocol != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("protocol = $%d", argPos))
		args = append(args, protocol)
		argPos++
	}

	if tag != "" {
		whereClauses = append(whereClauses, fmt.Sprintf(`(
			$%d = ANY(COALESCE(tags, '{}'))
			OR ($%d = 'origin:unknown' AND NOT EXISTS (
				SELECT 1 FROM unnest(COALESCE(tags, '{}')) AS existing_tag(value)
				WHERE existing_tag.value LIKE 'origin:%%'
			))
		)`, argPos, argPos))
		args = append(args, tag)
		argPos++
	}

	whereClause := ""
	if len(whereClauses) > 0 {
		whereClause = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	// Validate and set sort field
	validSortFields := map[string]bool{
		"address":           true,
		"status":            true,
		"requests":          true,
		"avg_response_time": true,
		"created_at":        true,
	}

	if !validSortFields[sortField] {
		sortField = "created_at"
	}

	if sortOrder != "asc" && sortOrder != "desc" {
		sortOrder = "desc"
	}

	// Count total
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM proxies %s", whereClause)
	var total int
	if err := r.db.Pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("failed to count proxies: %w", err)
	}

	// Get proxies
	offset := (page - 1) * limit
	query := fmt.Sprintf(`
		SELECT
			id, address, protocol, username, status,
			requests, successful_requests, failed_requests,
			avg_response_time, last_check,
			failed_since, failure_episode_kind, next_health_check_at,
			last_health_check_at, last_health_success_at,
			base_health_status, youtube_health_status, archived_at, archive_reason,
			country_code, country_name, region_name, city_name, isp, geo_updated_at,
			COALESCE(tags, '{}') AS tags,
			created_at, updated_at
		FROM proxies
		%s
		ORDER BY %s %s
		LIMIT $%d OFFSET $%d
	`, whereClause, sortField, sortOrder, argPos, argPos+1)

	args = append(args, limit, offset)

	rows, err := r.db.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list proxies: %w", err)
	}
	defer rows.Close()

	proxies := []models.ProxyWithStats{}
	for rows.Next() {
		var p models.Proxy
		err := rows.Scan(
			&p.ID, &p.Address, &p.Protocol, &p.Username, &p.Status,
			&p.Requests, &p.SuccessfulRequests, &p.FailedRequests,
			&p.AvgResponseTime, &p.LastCheck,
			&p.FailedSince, &p.FailureEpisodeKind, &p.NextHealthCheckAt,
			&p.LastHealthCheckAt, &p.LastHealthSuccessAt,
			&p.BaseHealthStatus, &p.YouTubeHealthStatus, &p.ArchivedAt, &p.ArchiveReason,
			&p.CountryCode, &p.CountryName, &p.RegionName, &p.CityName, &p.ISP, &p.GeoUpdatedAt,
			&p.Tags,
			&p.CreatedAt, &p.UpdatedAt,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan proxy: %w", err)
		}

		// Calculate success rate
		successRate := 0.0
		if p.Requests > 0 {
			successRate = (float64(p.SuccessfulRequests) / float64(p.Requests)) * 100
		}

		tags := p.Tags
		if tags == nil {
			tags = []string{}
		}

		proxies = append(proxies, models.ProxyWithStats{
			ID:                  p.ID,
			Address:             p.Address,
			Protocol:            p.Protocol,
			Username:            p.Username,
			Status:              p.Status,
			Requests:            p.Requests,
			SuccessRate:         successRate,
			AvgResponseTime:     p.AvgResponseTime,
			LastCheck:           p.LastCheck,
			FailedSince:         p.FailedSince,
			FailureEpisodeKind:  p.FailureEpisodeKind,
			NextHealthCheckAt:   p.NextHealthCheckAt,
			LastHealthCheckAt:   p.LastHealthCheckAt,
			LastHealthSuccessAt: p.LastHealthSuccessAt,
			BaseHealthStatus:    p.BaseHealthStatus,
			YouTubeHealthStatus: p.YouTubeHealthStatus,
			ArchivedAt:          p.ArchivedAt,
			ArchiveReason:       p.ArchiveReason,
			CountryCode:         p.CountryCode,
			CountryName:         p.CountryName,
			RegionName:          p.RegionName,
			CityName:            p.CityName,
			ISP:                 p.ISP,
			GeoUpdatedAt:        p.GeoUpdatedAt,
			Tags:                tags,
			CreatedAt:           p.CreatedAt,
			UpdatedAt:           p.UpdatedAt,
		})
	}

	return proxies, total, nil
}

// GetByID retrieves a proxy by ID
func (r *ProxyRepository) GetByID(ctx context.Context, id int) (*models.Proxy, error) {
	query := `
		SELECT
			id, address, protocol, username, password, status,
			requests, successful_requests, failed_requests,
			avg_response_time, last_check, last_error,
			failed_since, failure_episode_kind, next_health_check_at,
			last_health_check_at, last_health_success_at,
			base_health_status, youtube_health_status, archived_at, archive_reason,
			country_code, country_name, region_name, city_name, isp, geo_updated_at,
			COALESCE(tags, '{}') AS tags,
			created_at, updated_at
		FROM proxies
		WHERE id = $1
	`

	var p models.Proxy
	err := r.db.Pool.QueryRow(ctx, query, id).Scan(
		&p.ID, &p.Address, &p.Protocol, &p.Username, &p.Password, &p.Status,
		&p.Requests, &p.SuccessfulRequests, &p.FailedRequests,
		&p.AvgResponseTime, &p.LastCheck, &p.LastError,
		&p.FailedSince, &p.FailureEpisodeKind, &p.NextHealthCheckAt,
		&p.LastHealthCheckAt, &p.LastHealthSuccessAt,
		&p.BaseHealthStatus, &p.YouTubeHealthStatus, &p.ArchivedAt, &p.ArchiveReason,
		&p.CountryCode, &p.CountryName, &p.RegionName, &p.CityName, &p.ISP, &p.GeoUpdatedAt,
		&p.Tags,
		&p.CreatedAt, &p.UpdatedAt,
	)

	if err == pgx.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get proxy: %w", err)
	}
	if p.Tags == nil {
		p.Tags = []string{}
	}
	return &p, nil
}

// Create creates a new proxy
func (r *ProxyRepository) Create(ctx context.Context, req models.CreateProxyRequest) (*models.Proxy, error) {
	tags := normalizeProxyTags(req.Tags)
	identity := requestIdentity(req)
	query := `
		INSERT INTO proxies (address, protocol, username, password, tags, source_id, node_identity)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, address, protocol, username, status, tags, created_at, updated_at
	`

	var p models.Proxy
	err := r.db.Pool.QueryRow(ctx, query,
		req.Address, req.Protocol, req.Username, req.Password, tags, req.SourceID, identity,
	).Scan(
		&p.ID, &p.Address, &p.Protocol, &p.Username, &p.Status, &p.Tags, &p.CreatedAt, &p.UpdatedAt,
	)

	if err != nil {
		// Check if it's a unique constraint violation
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, fmt.Errorf("proxy with address %s and protocol %s already exists", req.Address, req.Protocol)
		}
		return nil, fmt.Errorf("failed to create proxy: %w", err)
	}
	if p.Tags == nil {
		p.Tags = []string{}
	}
	if err := r.upsertSourceMembership(ctx, req.SourceID, p.ID); err != nil {
		return nil, err
	}
	return &p, nil
}

// Upsert creates or updates a proxy, returning the result status
func (r *ProxyRepository) Upsert(ctx context.Context, req models.CreateProxyRequest) (id int, status string, err error) {
	tags := normalizeProxyTags(req.Tags)
	identity := requestIdentity(req)
	// Check if proxy exists
	var existingID int
	var existingTags []string
	checkErr := r.db.Pool.QueryRow(ctx,
		`SELECT id, COALESCE(tags, '{}') FROM proxies WHERE node_identity=$1`, identity,
	).Scan(&existingID, &existingTags)

	if checkErr == pgx.ErrNoRows {
		// Insert new
		insErr := r.db.Pool.QueryRow(ctx,
			`INSERT INTO proxies (address, protocol, username, password, tags, source_id, node_identity)
			 VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
			req.Address, req.Protocol, req.Username, req.Password, tags, req.SourceID, identity,
		).Scan(&id)
		if insErr != nil {
			return 0, "failed", insErr
		}
		if membershipErr := r.upsertSourceMembership(ctx, req.SourceID, id); membershipErr != nil {
			return id, "failed", membershipErr
		}
		return id, "created", nil
	}
	if checkErr != nil {
		return 0, "failed", checkErr
	}
	tags = mergeProxyTags(existingTags, tags)

	// Preserve the first legacy source_id while recording every source in the
	// membership table below.
	_, updErr := r.db.Pool.Exec(ctx,
		`UPDATE proxies SET
			username   = COALESCE($1, username),
			password   = COALESCE($2, password),
			tags       = CASE WHEN array_length($3::text[], 1) > 0 THEN $3::text[] ELSE tags END,
			source_id  = COALESCE(source_id, $4),
			status = CASE
				WHEN status <> 'archived' AND (
					($1 IS NOT NULL AND username IS DISTINCT FROM $1) OR
					($2 IS NOT NULL AND password IS DISTINCT FROM $2)
				) THEN 'idle'
				ELSE status
			END,
			failed_since = CASE
				WHEN status <> 'archived' AND (
					($1 IS NOT NULL AND username IS DISTINCT FROM $1) OR
					($2 IS NOT NULL AND password IS DISTINCT FROM $2)
				) THEN NULL
				ELSE failed_since
			END,
			continuous_failed_since = CASE
				WHEN status <> 'archived' AND (
					($1 IS NOT NULL AND username IS DISTINCT FROM $1) OR
					($2 IS NOT NULL AND password IS DISTINCT FROM $2)
				) THEN NULL
				ELSE continuous_failed_since
			END,
			failure_episode_kind = CASE
				WHEN status <> 'archived' AND (
					($1 IS NOT NULL AND username IS DISTINCT FROM $1) OR
					($2 IS NOT NULL AND password IS DISTINCT FROM $2)
				) THEN NULL
				ELSE failure_episode_kind
			END,
			next_health_check_at = CASE
				WHEN status <> 'archived' AND (
					($1 IS NOT NULL AND username IS DISTINCT FROM $1) OR
					($2 IS NOT NULL AND password IS DISTINCT FROM $2)
				) THEN NOW()
				ELSE next_health_check_at
			END,
			revalidation_required = CASE
				WHEN status <> 'archived' AND (
					($1 IS NOT NULL AND username IS DISTINCT FROM $1) OR
					($2 IS NOT NULL AND password IS DISTINCT FROM $2)
				) THEN false
				ELSE revalidation_required
			END,
			health_generation = CASE
				WHEN status <> 'archived' AND (
					($1 IS NOT NULL AND username IS DISTINCT FROM $1) OR
					($2 IS NOT NULL AND password IS DISTINCT FROM $2)
				) THEN health_generation + 1
				ELSE health_generation
			END,
			health_check_not_before = CASE
				WHEN status <> 'archived' AND (
					($1 IS NOT NULL AND username IS DISTINCT FROM $1) OR
					($2 IS NOT NULL AND password IS DISTINCT FROM $2)
				) THEN NOW()
				ELSE health_check_not_before
			END,
			last_health_check_at = CASE
				WHEN status <> 'archived' AND (
					($1 IS NOT NULL AND username IS DISTINCT FROM $1) OR
					($2 IS NOT NULL AND password IS DISTINCT FROM $2)
				) THEN NULL
				ELSE last_health_check_at
			END,
			last_health_success_at = CASE
				WHEN status <> 'archived' AND (
					($1 IS NOT NULL AND username IS DISTINCT FROM $1) OR
					($2 IS NOT NULL AND password IS DISTINCT FROM $2)
				) THEN NULL
				ELSE last_health_success_at
			END,
			base_health_status = CASE
				WHEN status <> 'archived' AND (
					($1 IS NOT NULL AND username IS DISTINCT FROM $1) OR
					($2 IS NOT NULL AND password IS DISTINCT FROM $2)
				) THEN NULL
				ELSE base_health_status
			END,
			youtube_health_status = CASE
				WHEN status <> 'archived' AND (
					($1 IS NOT NULL AND username IS DISTINCT FROM $1) OR
					($2 IS NOT NULL AND password IS DISTINCT FROM $2)
				) THEN NULL
				ELSE youtube_health_status
			END,
			last_health_verdict = CASE
				WHEN status <> 'archived' AND (
					($1 IS NOT NULL AND username IS DISTINCT FROM $1) OR
					($2 IS NOT NULL AND password IS DISTINCT FROM $2)
				) THEN NULL
				ELSE last_health_verdict
			END,
			last_error = CASE
				WHEN status <> 'archived' AND (
					($1 IS NOT NULL AND username IS DISTINCT FROM $1) OR
					($2 IS NOT NULL AND password IS DISTINCT FROM $2)
				) THEN NULL
				ELSE last_error
			END,
			updated_at = NOW()
		WHERE id = $5`,
		req.Username, req.Password, tags, req.SourceID, existingID,
	)
	if updErr != nil {
		return existingID, "failed", updErr
	}
	if membershipErr := r.upsertSourceMembership(ctx, req.SourceID, existingID); membershipErr != nil {
		return existingID, "failed", membershipErr
	}
	return existingID, "updated", nil
}

func requestIdentity(req models.CreateProxyRequest) string {
	if strings.TrimSpace(req.NodeIdentity) != "" {
		return req.NodeIdentity
	}
	return proxyidentity.ForProxy(req.Protocol, req.Address, req.Password)
}

func (r *ProxyRepository) upsertSourceMembership(ctx context.Context, sourceID *int, proxyID int) error {
	if sourceID == nil {
		return nil
	}
	_, err := r.db.Pool.Exec(ctx, `
		INSERT INTO proxy_source_memberships (source_id, proxy_id, last_seen_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (source_id, proxy_id) DO UPDATE
		SET last_seen_at = EXCLUDED.last_seen_at
	`, *sourceID, proxyID)
	if err != nil {
		return fmt.Errorf("update proxy source membership: %w", err)
	}
	return nil
}

func mergeProxyTags(existing, incoming []string) []string {
	existing = normalizeProxyTags(existing)
	incoming = normalizeProxyTags(incoming)
	incomingOrigin := false
	for _, tag := range incoming {
		if strings.HasPrefix(tag, "origin:") {
			incomingOrigin = true
			break
		}
	}

	merged := make([]string, 0, len(existing)+len(incoming))
	for _, tag := range existing {
		if incomingOrigin && strings.HasPrefix(tag, "origin:") {
			continue
		}
		merged = append(merged, tag)
	}
	return normalizeProxyTags(append(merged, incoming...))
}

func normalizeTagList(tags []string) []string {
	normalized := make([]string, 0, len(tags))
	seen := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		if _, found := seen[tag]; found {
			continue
		}
		seen[tag] = struct{}{}
		normalized = append(normalized, tag)
	}
	return normalized
}

// normalizeProxyTags keeps tags stable while enforcing the single-origin
// classification invariant. When callers provide multiple origin tags, the
// last explicit value wins.
func normalizeProxyTags(tags []string) []string {
	lastOrigin := ""
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if strings.HasPrefix(tag, "origin:") {
			lastOrigin = tag
		}
	}

	normalized := normalizeTagList(tags)
	result := make([]string, 0, len(normalized))
	for _, tag := range normalized {
		if strings.HasPrefix(tag, "origin:") && tag != lastOrigin {
			continue
		}
		result = append(result, tag)
	}
	return result
}

// DeleteAll removes all proxies from the database. Returns count deleted.
func (r *ProxyRepository) DeleteAll(ctx context.Context) (int, error) {
	tag, err := r.db.Pool.Exec(ctx, `DELETE FROM proxies`)
	if err != nil {
		return 0, fmt.Errorf("failed to delete all proxies: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// Update updates a proxy
func (r *ProxyRepository) Update(ctx context.Context, id int, req models.UpdateProxyRequest) (*models.Proxy, error) {
	var tags []string
	if req.Tags != nil {
		tags = normalizeProxyTags(req.Tags)
	}
	query := `
		UPDATE proxies
		SET address    = COALESCE(NULLIF($1, ''), address),
		    protocol   = COALESCE(NULLIF($2, ''), protocol),
		    username   = COALESCE($3, username),
		    password   = COALESCE($4, password),
		    tags       = COALESCE($5::text[], tags),
		    node_identity = CASE WHEN $7 <> '' THEN $7 ELSE node_identity END,
		    country_code = CASE WHEN NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '') THEN NULL ELSE country_code END,
		    country_name = CASE WHEN NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '') THEN NULL ELSE country_name END,
		    region_name = CASE WHEN NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '') THEN NULL ELSE region_name END,
		    city_name = CASE WHEN NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '') THEN NULL ELSE city_name END,
		    latitude = CASE WHEN NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '') THEN NULL ELSE latitude END,
		    longitude = CASE WHEN NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '') THEN NULL ELSE longitude END,
		    isp = CASE WHEN NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '') THEN NULL ELSE isp END,
		    geo_updated_at = CASE WHEN NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '') THEN NULL ELSE geo_updated_at END,
		    status = CASE
		      WHEN status <> 'archived' AND (
		        (NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '')) OR
		        (NULLIF($2, '') IS NOT NULL AND protocol IS DISTINCT FROM NULLIF($2, '')) OR
		        ($3 IS NOT NULL AND username IS DISTINCT FROM $3) OR
		        ($4 IS NOT NULL AND password IS DISTINCT FROM $4)
		      ) THEN 'idle'
		      ELSE status
		    END,
		    failed_since = CASE WHEN status <> 'archived' AND (
		      (NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '')) OR
		      (NULLIF($2, '') IS NOT NULL AND protocol IS DISTINCT FROM NULLIF($2, '')) OR
		      ($3 IS NOT NULL AND username IS DISTINCT FROM $3) OR
		      ($4 IS NOT NULL AND password IS DISTINCT FROM $4)
		    ) THEN NULL ELSE failed_since END,
		    continuous_failed_since = CASE WHEN status <> 'archived' AND (
		      (NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '')) OR
		      (NULLIF($2, '') IS NOT NULL AND protocol IS DISTINCT FROM NULLIF($2, '')) OR
		      ($3 IS NOT NULL AND username IS DISTINCT FROM $3) OR
		      ($4 IS NOT NULL AND password IS DISTINCT FROM $4)
		    ) THEN NULL ELSE continuous_failed_since END,
		    failure_episode_kind = CASE WHEN status <> 'archived' AND (
		      (NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '')) OR
		      (NULLIF($2, '') IS NOT NULL AND protocol IS DISTINCT FROM NULLIF($2, '')) OR
		      ($3 IS NOT NULL AND username IS DISTINCT FROM $3) OR
		      ($4 IS NOT NULL AND password IS DISTINCT FROM $4)
		    ) THEN NULL ELSE failure_episode_kind END,
		    next_health_check_at = CASE WHEN status <> 'archived' AND (
		      (NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '')) OR
		      (NULLIF($2, '') IS NOT NULL AND protocol IS DISTINCT FROM NULLIF($2, '')) OR
		      ($3 IS NOT NULL AND username IS DISTINCT FROM $3) OR
		      ($4 IS NOT NULL AND password IS DISTINCT FROM $4)
		    ) THEN NOW() ELSE next_health_check_at END,
		    revalidation_required = CASE WHEN status <> 'archived' AND (
		      (NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '')) OR
		      (NULLIF($2, '') IS NOT NULL AND protocol IS DISTINCT FROM NULLIF($2, '')) OR
		      ($3 IS NOT NULL AND username IS DISTINCT FROM $3) OR
		      ($4 IS NOT NULL AND password IS DISTINCT FROM $4)
		    ) THEN false ELSE revalidation_required END,
		    health_generation = CASE WHEN status <> 'archived' AND (
		      (NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '')) OR
		      (NULLIF($2, '') IS NOT NULL AND protocol IS DISTINCT FROM NULLIF($2, '')) OR
		      ($3 IS NOT NULL AND username IS DISTINCT FROM $3) OR
		      ($4 IS NOT NULL AND password IS DISTINCT FROM $4)
		    ) THEN health_generation + 1 ELSE health_generation END,
		    health_check_not_before = CASE WHEN status <> 'archived' AND (
		      (NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '')) OR
		      (NULLIF($2, '') IS NOT NULL AND protocol IS DISTINCT FROM NULLIF($2, '')) OR
		      ($3 IS NOT NULL AND username IS DISTINCT FROM $3) OR
		      ($4 IS NOT NULL AND password IS DISTINCT FROM $4)
		    ) THEN NOW() ELSE health_check_not_before END,
		    last_health_check_at = CASE WHEN status <> 'archived' AND (
		      (NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '')) OR
		      (NULLIF($2, '') IS NOT NULL AND protocol IS DISTINCT FROM NULLIF($2, '')) OR
		      ($3 IS NOT NULL AND username IS DISTINCT FROM $3) OR
		      ($4 IS NOT NULL AND password IS DISTINCT FROM $4)
		    ) THEN NULL ELSE last_health_check_at END,
		    last_health_success_at = CASE WHEN status <> 'archived' AND (
		      (NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '')) OR
		      (NULLIF($2, '') IS NOT NULL AND protocol IS DISTINCT FROM NULLIF($2, '')) OR
		      ($3 IS NOT NULL AND username IS DISTINCT FROM $3) OR
		      ($4 IS NOT NULL AND password IS DISTINCT FROM $4)
		    ) THEN NULL ELSE last_health_success_at END,
		    base_health_status = CASE WHEN status <> 'archived' AND (
		      (NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '')) OR
		      (NULLIF($2, '') IS NOT NULL AND protocol IS DISTINCT FROM NULLIF($2, '')) OR
		      ($3 IS NOT NULL AND username IS DISTINCT FROM $3) OR
		      ($4 IS NOT NULL AND password IS DISTINCT FROM $4)
		    ) THEN NULL ELSE base_health_status END,
		    youtube_health_status = CASE WHEN status <> 'archived' AND (
		      (NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '')) OR
		      (NULLIF($2, '') IS NOT NULL AND protocol IS DISTINCT FROM NULLIF($2, '')) OR
		      ($3 IS NOT NULL AND username IS DISTINCT FROM $3) OR
		      ($4 IS NOT NULL AND password IS DISTINCT FROM $4)
		    ) THEN NULL ELSE youtube_health_status END,
		    last_health_verdict = CASE WHEN status <> 'archived' AND (
		      (NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '')) OR
		      (NULLIF($2, '') IS NOT NULL AND protocol IS DISTINCT FROM NULLIF($2, '')) OR
		      ($3 IS NOT NULL AND username IS DISTINCT FROM $3) OR
		      ($4 IS NOT NULL AND password IS DISTINCT FROM $4)
		    ) THEN NULL ELSE last_health_verdict END,
		    last_error = CASE WHEN status <> 'archived' AND (
		      (NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '')) OR
		      (NULLIF($2, '') IS NOT NULL AND protocol IS DISTINCT FROM NULLIF($2, '')) OR
		      ($3 IS NOT NULL AND username IS DISTINCT FROM $3) OR
		      ($4 IS NOT NULL AND password IS DISTINCT FROM $4)
		    ) THEN NULL ELSE last_error END,
		    archived_at = CASE WHEN status <> 'archived' AND (
		      (NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '')) OR
		      (NULLIF($2, '') IS NOT NULL AND protocol IS DISTINCT FROM NULLIF($2, '')) OR
		      ($3 IS NOT NULL AND username IS DISTINCT FROM $3) OR
		      ($4 IS NOT NULL AND password IS DISTINCT FROM $4)
		    ) THEN NULL ELSE archived_at END,
		    archive_reason = CASE WHEN status <> 'archived' AND (
		      (NULLIF($1, '') IS NOT NULL AND address IS DISTINCT FROM NULLIF($1, '')) OR
		      (NULLIF($2, '') IS NOT NULL AND protocol IS DISTINCT FROM NULLIF($2, '')) OR
		      ($3 IS NOT NULL AND username IS DISTINCT FROM $3) OR
		      ($4 IS NOT NULL AND password IS DISTINCT FROM $4)
		    ) THEN NULL ELSE archive_reason END,
		    updated_at = NOW()
		WHERE id = $6
		RETURNING id, address, protocol, status, COALESCE(tags,'{}'), updated_at
	`

	var p models.Proxy
	err := r.db.Pool.QueryRow(ctx, query,
		req.Address, req.Protocol, req.Username, req.Password, tags, id, req.NodeIdentity,
	).Scan(
		&p.ID, &p.Address, &p.Protocol, &p.Status, &p.Tags, &p.UpdatedAt,
	)

	if err == pgx.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("failed to update proxy: %w", err)
	}

	return &p, nil
}

// BulkUpdateTags adds and removes normalized tags in one statement. Removal
// wins when a tag appears in both lists, and adding an origin tag replaces the
// proxy's previous origin classification. No lifecycle or credential column is
// part of this update.
func (r *ProxyRepository) BulkUpdateTags(ctx context.Context, ids []int, add, remove []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	remove = normalizeTagList(remove)
	removeSet := make(map[string]struct{}, len(remove))
	for _, tag := range remove {
		removeSet[tag] = struct{}{}
	}

	add = normalizeProxyTags(add)
	effectiveAdd := make([]string, 0, len(add))
	for _, tag := range add {
		if _, removed := removeSet[tag]; !removed {
			effectiveAdd = append(effectiveAdd, tag)
		}
	}
	if len(effectiveAdd) == 0 && len(remove) == 0 {
		return 0, nil
	}

	originToAdd := ""
	for _, tag := range effectiveAdd {
		if strings.HasPrefix(tag, "origin:") {
			originToAdd = tag
			break
		}
	}

	query := `
		UPDATE proxies AS proxy
		SET tags = (
			WITH normalized AS (
				SELECT DISTINCT btrim(raw_tag) AS tag
				FROM unnest(COALESCE(proxy.tags, '{}'::text[]) || $2::text[]) AS raw_tag
				WHERE btrim(raw_tag) <> ''
				  AND NOT (btrim(raw_tag) = ANY($3::text[]))
				  AND NOT ($4::boolean
				           AND btrim(raw_tag) LIKE 'origin:%'
				           AND btrim(raw_tag) <> $5::text)
			), canonical AS (
				SELECT DISTINCT ON (
					CASE WHEN tag LIKE 'origin:%' THEN 'origin:' ELSE tag END
				) tag
				FROM normalized
				ORDER BY
					CASE WHEN tag LIKE 'origin:%' THEN 'origin:' ELSE tag END,
					CASE WHEN tag = $5::text THEN 0 ELSE 1 END,
					tag
			)
			SELECT COALESCE(array_agg(tag ORDER BY tag), '{}'::text[])
			FROM canonical
		),
		updated_at = NOW()
		WHERE id = ANY($1::int[])
	`
	result, err := r.db.Pool.Exec(ctx, query, ids, effectiveAdd, remove, originToAdd != "", originToAdd)
	if err != nil {
		return 0, fmt.Errorf("failed to bulk update tags: %w", err)
	}
	return int(result.RowsAffected()), nil
}

// Delete deletes a proxy by ID
func (r *ProxyRepository) Delete(ctx context.Context, id int) error {
	query := `DELETE FROM proxies WHERE id = $1`
	_, err := r.db.Pool.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete proxy: %w", err)
	}
	return nil
}

// BulkDelete deletes multiple proxies
func (r *ProxyRepository) BulkDelete(ctx context.Context, ids []int) (int, error) {
	query := `DELETE FROM proxies WHERE id = ANY($1)`
	result, err := r.db.Pool.Exec(ctx, query, ids)
	if err != nil {
		return 0, fmt.Errorf("failed to bulk delete proxies: %w", err)
	}
	return int(result.RowsAffected()), nil
}

// GetStats retrieves overall proxy statistics
func (r *ProxyRepository) GetStats(ctx context.Context) (map[string]interface{}, error) {
	query := `
		SELECT
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE status = 'active') as active,
			COUNT(*) FILTER (WHERE status = 'failed') as failed,
			COUNT(*) FILTER (WHERE status = 'idle') as idle,
			COUNT(*) FILTER (WHERE status = 'archived') as archived,
			COALESCE(SUM(requests), 0) as total_requests,
			COALESCE(AVG(avg_response_time), 0) as avg_response_time
		FROM proxies
	`

	var stats struct {
		Total           int
		Active          int
		Failed          int
		Idle            int
		Archived        int
		TotalRequests   int64
		AvgResponseTime float64
	}

	err := r.db.Pool.QueryRow(ctx, query).Scan(
		&stats.Total, &stats.Active, &stats.Failed, &stats.Idle, &stats.Archived,
		&stats.TotalRequests, &stats.AvgResponseTime,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get stats: %w", err)
	}

	return map[string]interface{}{
		"total":             stats.Total,
		"active":            stats.Active,
		"failed":            stats.Failed,
		"idle":              stats.Idle,
		"archived":          stats.Archived,
		"total_requests":    stats.TotalRequests,
		"avg_response_time": int(stats.AvgResponseTime),
	}, nil
}

// GetAllActive retrieves all active proxies
func (r *ProxyRepository) GetAllActive(ctx context.Context) ([]models.ProxyStatusSimple, error) {
	query := `
		SELECT
			id, address, status, requests,
			successful_requests, failed_requests
		FROM proxies
		WHERE status = 'active'
		  AND revalidation_required = false
		  AND (cooldown_until IS NULL OR cooldown_until <= NOW())
		  AND last_health_success_at IS NOT NULL
		  AND youtube_health_status = 'passed'
		  AND next_health_check_at > NOW()
		ORDER BY address
	`

	rows, err := r.db.Pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get active proxies: %w", err)
	}
	defer rows.Close()

	proxies := []models.ProxyStatusSimple{}
	for rows.Next() {
		var p struct {
			ID                 int
			Address            string
			Status             string
			Requests           int64
			SuccessfulRequests int64
			FailedRequests     int64
		}

		err := rows.Scan(&p.ID, &p.Address, &p.Status, &p.Requests, &p.SuccessfulRequests, &p.FailedRequests)
		if err != nil {
			return nil, fmt.Errorf("failed to scan proxy: %w", err)
		}

		successRate := 0.0
		if p.Requests > 0 {
			successRate = (float64(p.SuccessfulRequests) / float64(p.Requests)) * 100
		}

		proxies = append(proxies, models.ProxyStatusSimple{
			ID:          fmt.Sprintf("%d", p.ID),
			Address:     p.Address,
			Status:      p.Status,
			Requests:    p.Requests,
			SuccessRate: successRate,
		})
	}

	return proxies, nil
}
