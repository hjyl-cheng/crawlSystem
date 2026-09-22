package proxycontrol

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
)

var (
	errResourceSyncRetry = errors.New("proxy control resource sync preflight is stale")
)

type managedCredentialCacheEntry struct {
	observedHash string
	desiredHash  string
}

type managedUserPreflight struct {
	username      string
	observedHash  string
	desiredHash   string
	passwordMatch bool
}

type slotSpec struct {
	Name   string
	Role   string
	Number int
}

func (m *Manager) slotSpecs(channelSlots int) []slotSpec {
	counts := []struct {
		role  string
		count int
	}{
		{RoleDiscover, m.options.DiscoverSlots},
		{RoleChannel, channelSlots},
		{RoleQueryQuality, m.options.QueryQualitySlots},
		{RoleDetail, m.options.DetailSlots},
	}
	specs := make([]slotSpec, 0,
		m.options.DiscoverSlots+channelSlots+m.options.QueryQualitySlots+m.options.DetailSlots)
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
	return m.syncResourcesMinimum(ctx, 0)
}

func (m *Manager) syncResourcesMinimum(ctx context.Context, minimum int) error {
	if err := m.requireEnabled(); err != nil {
		return err
	}
	if !m.resourceSyncMu.TryLock() {
		m.resourceSyncDeferred.Add(1)
		return ErrResourceSyncDeferred
	}
	defer m.resourceSyncMu.Unlock()
	preflightStarted := time.Now()
	bcryptBefore := m.resourceBcryptCount.Load()
	bcryptNanosBefore := m.resourceBcryptNanos.Load()

	// A concurrent password/user update can invalidate the lock-free bcrypt
	// preflight. Retry once after re-reading it; the retry still does all bcrypt
	// work before entering the advisory-lock transaction.
	for attempt := 0; attempt < 2; attempt++ {
		available, err := m.probeResourceSyncLock(ctx)
		if err != nil {
			return fmt.Errorf("probe resource sync lock: %w", err)
		}
		if !available {
			m.resourceSyncDeferred.Add(1)
			return ErrResourceSyncDeferred
		}
		channelSlots, err := m.channelSlotMinimum(ctx, m.db.Pool)
		if err != nil {
			return fmt.Errorf("load channel capacity: %w", err)
		}
		if minimum > channelSlots {
			channelSlots = minimum
		}
		specs := m.slotSpecs(channelSlots)
		preflight, err := m.preflightManagedUsers(ctx, specs)
		if err != nil {
			return err
		}

		tx, err := m.db.Pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return fmt.Errorf("begin resource sync: %w", err)
		}
		rollback := true
		defer func() {
			if rollback {
				_ = tx.Rollback(ctx)
			}
		}()

		// Resource reconciliation is best-effort. Never queue behind Claim,
		// Renew, BeginTask or CompleteTask on the shared advisory lock.
		var acquired bool
		if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, controlAdvisoryLock).Scan(&acquired); err != nil {
			return fmt.Errorf("try resource sync lock: %w", err)
		}
		if !acquired {
			_ = tx.Rollback(ctx)
			m.resourceSyncDeferred.Add(1)
			return ErrResourceSyncDeferred
		}
		lockStarted := time.Now()
		if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '250ms'`); err != nil {
			return fmt.Errorf("set resource sync lock timeout: %w", err)
		}

		if minimum > 0 {
			if _, err := tx.Exec(ctx, `INSERT INTO proxy_control_capacity_targets(workload_scope,role,minimum_slots)
				VALUES($1,'channel',$2) ON CONFLICT(workload_scope,role) DO UPDATE
				SET minimum_slots=GREATEST(proxy_control_capacity_targets.minimum_slots,EXCLUDED.minimum_slots),updated_at=NOW()`,
				m.options.WorkloadScope, minimum); err != nil {
				return fmt.Errorf("persist channel capacity: %w", err)
			}
			// Capacity targets may have changed while preflight was running.
			channelSlots, err = m.channelSlotMinimum(ctx, tx)
			if err != nil {
				return fmt.Errorf("reload channel capacity: %w", err)
			}
			if len(m.slotSpecs(channelSlots)) != len(specs) {
				_ = tx.Rollback(ctx)
				if attempt == 0 {
					continue
				}
				return errResourceSyncRetry
			}
		}

		existing := make(map[string]bool)
		if minimum > 0 {
			rows, err := tx.Query(ctx, `SELECT slot_name FROM proxy_running_slots`)
			if err != nil {
				return err
			}
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err != nil {
					rows.Close()
					return err
				}
				existing[name] = true
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return err
			}
			rows.Close()
		}
		desiredNames := make([]string, 0, len(specs))
		changedUsers := make([]string, 0)
		for _, spec := range specs {
			desiredNames = append(desiredNames, spec.Name)
			// Online capacity growth only creates missing resources. Existing users,
			// credentials, pools, active tasks and route generations are untouched.
			if minimum > 0 && existing[spec.Name] {
				continue
			}
			poolID, err := ensureManagedPool(ctx, tx, spec)
			if err != nil {
				return err
			}
			userID, proxyUser, changed, err := ensureManagedUser(ctx, tx, spec, poolID, preflight[spec.Name])
			if errors.Is(err, errResourceSyncRetry) {
				_ = tx.Rollback(ctx)
				rollback = false
				break
			}
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
		if rollback == false {
			if attempt == 0 {
				continue
			}
			return errResourceSyncRetry
		}
		if minimum > 0 {
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit capacity growth: %w", err)
			}
			lockHeld := time.Since(lockStarted)
			rollback = false
			for _, username := range changedUsers {
				m.invalidateUser(username)
			}
			m.logResourceSyncCompleted(preflightStarted, bcryptBefore, bcryptNanosBefore, lockHeld)
			return nil
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
		lockHeld := time.Since(lockStarted)
		rollback = false
		slices.Sort(changedUsers)
		changedUsers = slices.Compact(changedUsers)
		for _, username := range changedUsers {
			m.invalidateUser(username)
		}
		m.logResourceSyncCompleted(preflightStarted, bcryptBefore, bcryptNanosBefore, lockHeld)
		return nil
	}
	return errResourceSyncRetry
}

func (m *Manager) logResourceSyncCompleted(
	preflightStarted time.Time,
	bcryptBefore uint64,
	bcryptNanosBefore int64,
	lockHeld time.Duration,
) {
	m.logInfo("proxy control resource sync completed",
		"preflight_ms", time.Since(preflightStarted).Milliseconds(),
		"lock_held_ms", lockHeld.Milliseconds(),
		"bcrypt_count", m.resourceBcryptCount.Load()-bcryptBefore,
		"bcrypt_ms", (m.resourceBcryptNanos.Load()-bcryptNanosBefore)/int64(time.Millisecond),
		"deferred_total", m.resourceSyncDeferred.Load(),
	)
}

func (m *Manager) probeResourceSyncLock(ctx context.Context) (bool, error) {
	tx, err := m.db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var acquired bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, controlAdvisoryLock).Scan(&acquired); err != nil {
		return false, err
	}
	return acquired, nil
}

// preflightManagedUsers reads managed users without taking the control
// advisory lock and performs the expensive bcrypt work there. The observed
// hash is carried into the lock transaction; ensureManagedUser verifies it is
// still the same row before applying the prepared result.
func (m *Manager) preflightManagedUsers(ctx context.Context, specs []slotSpec) (map[string]managedUserPreflight, error) {
	prepared := make(map[string]managedUserPreflight, len(specs))
	for _, spec := range specs {
		var item managedUserPreflight
		err := m.db.Pool.QueryRow(ctx, `
			SELECT u.username, u.password_hash
			FROM proxy_running_slots s
			JOIN proxy_users u ON u.id=s.user_id
			WHERE s.slot_name=$1
		`, spec.Name).Scan(&item.username, &item.observedHash)
		if errors.Is(err, pgx.ErrNoRows) {
			err = m.db.Pool.QueryRow(ctx, `
				SELECT username, password_hash FROM proxy_users WHERE username=$1
			`, spec.Name).Scan(&item.username, &item.observedHash)
		}
		if errors.Is(err, pgx.ErrNoRows) {
			item.username = spec.Name
			item.observedHash = ""
		} else if err != nil {
			return nil, fmt.Errorf("preflight managed user %s: %w", spec.Name, err)
		}

		var passwordErr error
		item.desiredHash, item.passwordMatch, passwordErr = m.prepareManagedPassword(item.username, item.observedHash)
		if passwordErr != nil {
			return nil, passwordErr
		}
		prepared[spec.Name] = item
	}
	return prepared, nil
}

func (m *Manager) prepareManagedPassword(username, observedHash string) (string, bool, error) {
	m.managedCredentialMu.Lock()
	cached, found := m.managedCredentialCache[username]
	m.managedCredentialMu.Unlock()
	if found && cached.observedHash == observedHash && cached.desiredHash != "" {
		return cached.desiredHash, cached.desiredHash == observedHash, nil
	}

	if observedHash == "" {
		started := time.Now()
		hash, err := bcrypt.GenerateFromPassword([]byte(m.options.WorkerPassword), bcrypt.DefaultCost)
		m.recordResourceBcrypt(started)
		if err != nil {
			// bcrypt currently only fails for invalid cost/input; preserve the
			// error through the caller by returning an empty prepared hash.
			return "", false, fmt.Errorf("hash managed user password: %w", err)
		}
		desired := string(hash)
		m.managedCredentialMu.Lock()
		m.managedCredentialCache[username] = managedCredentialCacheEntry{desiredHash: desired}
		m.managedCredentialMu.Unlock()
		return desired, false, nil
	}

	started := time.Now()
	compareErr := bcrypt.CompareHashAndPassword([]byte(observedHash), []byte(m.options.WorkerPassword))
	m.recordResourceBcrypt(started)
	if compareErr == nil {
		m.managedCredentialMu.Lock()
		m.managedCredentialCache[username] = managedCredentialCacheEntry{observedHash: observedHash, desiredHash: observedHash}
		m.managedCredentialMu.Unlock()
		return observedHash, true, nil
	}
	started = time.Now()
	hash, err := bcrypt.GenerateFromPassword([]byte(m.options.WorkerPassword), bcrypt.DefaultCost)
	m.recordResourceBcrypt(started)
	if err != nil {
		return "", false, fmt.Errorf("rehash managed user password: %w", err)
	}
	desired := string(hash)
	m.managedCredentialMu.Lock()
	m.managedCredentialCache[username] = managedCredentialCacheEntry{observedHash: observedHash, desiredHash: desired}
	m.managedCredentialMu.Unlock()
	return desired, false, nil
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
	preflight managedUserPreflight,
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
		if preflight.observedHash != "" || preflight.desiredHash == "" {
			return 0, "", false, errResourceSyncRetry
		}
		err = tx.QueryRow(ctx, `
			INSERT INTO proxy_users (
			  username, password_hash, enabled, main_pool_id,
			  fallback_pool_ids, max_retries, requests_per_minute
			) VALUES ($1,$2,true,$3,'{}',1,0)
			RETURNING id
			`, spec.Name, preflight.desiredHash, poolID).Scan(&userID)
		if err != nil {
			return 0, "", false, fmt.Errorf("create managed user %s: %w", spec.Name, err)
		}
		return userID, spec.Name, true, nil
	}
	if err != nil {
		return 0, "", false, fmt.Errorf("load managed user %s: %w", spec.Name, err)
	}
	if username != preflight.username || passwordHash != preflight.observedHash || preflight.desiredHash == "" {
		return 0, "", false, errResourceSyncRetry
	}

	configurationMatches := enabled && mainPoolID != nil && *mainPoolID == poolID &&
		len(fallbackIDs) == 0 && maxRetries == 1 && rateLimit == 0
	if preflight.passwordMatch && configurationMatches {
		return userID, username, false, nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE proxy_users
		SET password_hash=$2, enabled=true, main_pool_id=$3,
		    fallback_pool_ids='{}', max_retries=1, requests_per_minute=0,
		    updated_at=NOW()
		WHERE id=$1
	`, userID, preflight.desiredHash, poolID); err != nil {
		return 0, "", false, fmt.Errorf("configure managed user %s: %w", spec.Name, err)
	}
	return userID, username, true, nil
}

func managedPoolDescription(spec slotSpec) string {
	return fmt.Sprintf("Managed BullMQ %s worker slot %d", spec.Role, spec.Number)
}
