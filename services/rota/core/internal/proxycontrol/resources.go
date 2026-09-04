package proxycontrol

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
)

type slotSpec struct {
	Name   string
	Role   string
	Number int
}

func (m *Manager) slotSpecs() []slotSpec {
	counts := []struct {
		role  string
		count int
	}{
		{RoleDiscover, m.options.DiscoverSlots},
		{RoleChannel, m.options.ChannelSlots},
		{RoleQueryQuality, m.options.QueryQualitySlots},
		{RoleDetail, m.options.DetailSlots},
	}
	specs := make([]slotSpec, 0,
		m.options.DiscoverSlots+m.options.ChannelSlots+m.options.QueryQualitySlots+m.options.DetailSlots)
	for _, item := range counts {
		for number := 1; number <= item.count; number++ {
			specs = append(specs, slotSpec{
				Name:   fmt.Sprintf("bullmq-%s-%02d", item.role, number),
				Role:   item.role,
				Number: number,
			})
		}
	}
	return specs
}

func (m *Manager) syncResources(ctx context.Context) error {
	if err := m.requireEnabled(); err != nil {
		return err
	}
	tx, err := m.db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin resource sync: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, controlAdvisoryLock); err != nil {
		return fmt.Errorf("lock resource sync: %w", err)
	}

	specs := m.slotSpecs()
	desiredNames := make([]string, 0, len(specs))
	changedUsers := make([]string, 0)
	for _, spec := range specs {
		desiredNames = append(desiredNames, spec.Name)
		poolID, err := ensureManagedPool(ctx, tx, spec)
		if err != nil {
			return err
		}
		userID, proxyUser, changed, err := ensureManagedUser(ctx, tx, spec, poolID, m.options.WorkerPassword)
		if err != nil {
			return err
		}
		if changed {
			changedUsers = append(changedUsers, proxyUser)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO proxy_running_slots (slot_name, role, slot_no, pool_id, user_id)
			VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT (slot_name) DO UPDATE
			SET role=EXCLUDED.role, slot_no=EXCLUDED.slot_no,
			    pool_id=EXCLUDED.pool_id, user_id=EXCLUDED.user_id, updated_at=NOW()
		`, spec.Name, spec.Role, spec.Number, poolID, userID); err != nil {
			return fmt.Errorf("upsert running slot %s: %w", spec.Name, err)
		}
	}

	rows, err := tx.Query(ctx, `
		SELECT s.slot_name, s.pool_id, s.user_id, u.username
		FROM proxy_running_slots s
		JOIN proxy_users u ON u.id=s.user_id
		WHERE s.slot_name ~ '^bullmq-(discover|channel|query_quality|detail)-[0-9]+$'
		  AND NOT (s.slot_name = ANY($1::text[]))
		  AND NOT (s.worker_id IS NOT NULL AND s.lease_until > NOW())
		FOR UPDATE OF s,u
	`, desiredNames)
	if err != nil {
		return fmt.Errorf("list obsolete running slots: %w", err)
	}
	type obsoleteSlot struct {
		name      string
		poolID    int
		userID    int
		proxyUser string
	}
	obsolete := make([]obsoleteSlot, 0)
	for rows.Next() {
		var item obsoleteSlot
		if err := rows.Scan(&item.name, &item.poolID, &item.userID, &item.proxyUser); err != nil {
			rows.Close()
			return fmt.Errorf("scan obsolete running slot: %w", err)
		}
		obsolete = append(obsolete, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate obsolete running slots: %w", err)
	}
	rows.Close()
	for _, item := range obsolete {
		if _, err := tx.Exec(ctx, `DELETE FROM pool_proxies WHERE pool_id=$1`, item.poolID); err != nil {
			return fmt.Errorf("clear obsolete slot pool %s: %w", item.name, err)
		}
		if _, err := tx.Exec(ctx, `UPDATE proxy_users SET enabled=false, updated_at=NOW() WHERE id=$1`, item.userID); err != nil {
			return fmt.Errorf("disable obsolete slot user %s: %w", item.name, err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM proxy_running_slots WHERE slot_name=$1`, item.name); err != nil {
			return fmt.Errorf("remove obsolete slot %s: %w", item.name, err)
		}
		changedUsers = append(changedUsers, item.proxyUser)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit resource sync: %w", err)
	}
	slices.Sort(changedUsers)
	changedUsers = slices.Compact(changedUsers)
	for _, username := range changedUsers {
		m.invalidateUser(username)
	}
	return nil
}

func ensureManagedPool(ctx context.Context, tx pgx.Tx, spec slotSpec) (int, error) {
	var poolID int
	err := tx.QueryRow(ctx, `
		SELECT id FROM proxy_pools WHERE name=$1 ORDER BY id LIMIT 1 FOR UPDATE
	`, spec.Name).Scan(&poolID)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `
			INSERT INTO proxy_pools (
			  name, description, rotation_method, stick_count,
			  health_check_url, health_check_cron, health_check_enabled,
			  auto_sync, sync_mode, enabled
			) VALUES ($1,$2,'roundrobin',1,$3,'*/30 * * * *',false,false,'manual',true)
			RETURNING id
		`, spec.Name, managedPoolDescription(spec), models.YouTubeSearchURLPrefix).Scan(&poolID)
	}
	if err != nil {
		return 0, fmt.Errorf("ensure managed pool %s: %w", spec.Name, err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE proxy_pools
		SET description=$2, rotation_method='roundrobin', stick_count=1,
		    health_check_url=$3, health_check_cron='*/30 * * * *',
		    health_check_enabled=false, auto_sync=false, sync_mode='manual',
		    enabled=true, updated_at=NOW()
		WHERE id=$1
	`, poolID, managedPoolDescription(spec), models.YouTubeSearchURLPrefix); err != nil {
		return 0, fmt.Errorf("configure managed pool %s: %w", spec.Name, err)
	}
	return poolID, nil
}

func ensureManagedUser(
	ctx context.Context,
	tx pgx.Tx,
	spec slotSpec,
	poolID int,
	password string,
) (int, string, bool, error) {
	var (
		userID       int
		username     string
		passwordHash string
		enabled      bool
		mainPoolID   *int
		fallbackIDs  []int
		maxRetries   int
		rateLimit    int
	)
	err := tx.QueryRow(ctx, `
		SELECT u.id, u.username, u.password_hash, u.enabled, u.main_pool_id, u.fallback_pool_ids,
		       u.max_retries, COALESCE(u.requests_per_minute,0)
		FROM proxy_running_slots s
		JOIN proxy_users u ON u.id=s.user_id
		WHERE s.slot_name=$1
		FOR UPDATE OF s,u
	`, spec.Name).Scan(
		&userID, &username, &passwordHash, &enabled, &mainPoolID, &fallbackIDs, &maxRetries, &rateLimit,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `
			SELECT id, username, password_hash, enabled, main_pool_id, fallback_pool_ids,
		       max_retries, COALESCE(requests_per_minute,0)
		FROM proxy_users WHERE username=$1 FOR UPDATE
		`, spec.Name).Scan(
			&userID, &username, &passwordHash, &enabled, &mainPoolID, &fallbackIDs, &maxRetries, &rateLimit,
		)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		hash, hashErr := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if hashErr != nil {
			return 0, "", false, fmt.Errorf("hash managed user password: %w", hashErr)
		}
		err = tx.QueryRow(ctx, `
			INSERT INTO proxy_users (
			  username, password_hash, enabled, main_pool_id,
			  fallback_pool_ids, max_retries, requests_per_minute
			) VALUES ($1,$2,true,$3,'{}',1,0)
			RETURNING id
			`, spec.Name, string(hash), poolID).Scan(&userID)
		if err != nil {
			return 0, "", false, fmt.Errorf("create managed user %s: %w", spec.Name, err)
		}
		return userID, spec.Name, true, nil
	}
	if err != nil {
		return 0, "", false, fmt.Errorf("load managed user %s: %w", spec.Name, err)
	}

	passwordMatches := bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(password)) == nil
	configurationMatches := enabled && mainPoolID != nil && *mainPoolID == poolID &&
		len(fallbackIDs) == 0 && maxRetries == 1 && rateLimit == 0
	if passwordMatches && configurationMatches {
		return userID, username, false, nil
	}
	if !passwordMatches {
		hash, hashErr := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if hashErr != nil {
			return 0, "", false, fmt.Errorf("rehash managed user password: %w", hashErr)
		}
		passwordHash = string(hash)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE proxy_users
		SET password_hash=$2, enabled=true, main_pool_id=$3,
		    fallback_pool_ids='{}', max_retries=1, requests_per_minute=0,
		    updated_at=NOW()
		WHERE id=$1
	`, userID, passwordHash, poolID); err != nil {
		return 0, "", false, fmt.Errorf("configure managed user %s: %w", spec.Name, err)
	}
	return userID, username, true, nil
}

func managedPoolDescription(spec slotSpec) string {
	return fmt.Sprintf("Managed BullMQ %s worker slot %d", spec.Role, spec.Number)
}
