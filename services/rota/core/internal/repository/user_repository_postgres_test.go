package repository

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/models"
)

func TestUserRepositoryHasAnyCountsDisabledUsersPostgres(t *testing.T) {
	_, pool := newPoolRepositoryPostgres(t)
	if _, err := pool.Exec(context.Background(), `
		CREATE TABLE proxy_users (
			id SERIAL PRIMARY KEY,
			username TEXT NOT NULL,
			enabled BOOLEAN NOT NULL DEFAULT true
		)`); err != nil {
		t.Fatalf("create proxy_users table: %v", err)
	}
	repo := NewUserRepository(&database.DB{Pool: pool})

	hasUsers, err := repo.HasAny(context.Background())
	if err != nil {
		t.Fatalf("check empty proxy_users: %v", err)
	}
	if hasUsers {
		t.Fatal("empty proxy_users table reported configured authentication")
	}

	if _, err := pool.Exec(context.Background(), `
		INSERT INTO proxy_users (username, enabled) VALUES ('disabled-user', false)
	`); err != nil {
		t.Fatalf("insert disabled proxy user: %v", err)
	}
	hasUsers, err = repo.HasAny(context.Background())
	if err != nil {
		t.Fatalf("check configured proxy_users: %v", err)
	}
	if !hasUsers {
		t.Fatal("disabled Proxy User incorrectly reopened the forward proxy")
	}
}

func TestUserRepositoryUpdateMergesPartialFieldsPostgres(t *testing.T) {
	_, pool := newPoolRepositoryPostgres(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE proxy_users (
			id SERIAL PRIMARY KEY,
			username VARCHAR(255) NOT NULL UNIQUE,
			password_hash TEXT NOT NULL,
			enabled BOOLEAN NOT NULL DEFAULT true,
			main_pool_id INTEGER REFERENCES proxy_pools(id) ON DELETE SET NULL,
			fallback_pool_ids INTEGER[] NOT NULL DEFAULT '{}',
			max_retries INTEGER NOT NULL DEFAULT 5,
			requests_per_minute INTEGER NOT NULL DEFAULT 0,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP NOT NULL DEFAULT NOW()
		)`); err != nil {
		t.Fatalf("create proxy_users table: %v", err)
	}

	mainPoolID := insertTestPool(t, pool, "main")
	fallbackOneID := insertTestPool(t, pool, "fallback-one")
	fallbackTwoID := insertTestPool(t, pool, "fallback-two")
	initialFallbacks := []int{fallbackOneID, fallbackTwoID}
	var userID int
	if err := pool.QueryRow(ctx, `
		INSERT INTO proxy_users (
			username, password_hash, enabled, main_pool_id,
			fallback_pool_ids, max_retries, requests_per_minute
		) VALUES ('alice', 'original-hash', true, $1, $2, 7, 120)
		RETURNING id
	`, mainPoolID, initialFallbacks).Scan(&userID); err != nil {
		t.Fatalf("insert proxy user: %v", err)
	}
	repo := NewUserRepository(&database.DB{Pool: pool})

	disabled := false
	updated, err := repo.Update(ctx, userID, models.UpdateProxyUserRequest{Enabled: &disabled})
	if err != nil {
		t.Fatalf("partial enabled update: %v", err)
	}
	if updated.Enabled || updated.MainPoolID == nil || *updated.MainPoolID != mainPoolID ||
		!reflect.DeepEqual(updated.FallbackPoolIDs, initialFallbacks) || updated.MaxRetries != 7 ||
		updated.RequestsPerMinute != 120 {
		t.Fatalf("omitted fields changed: %#v", updated)
	}
	var passwordHash string
	if err := pool.QueryRow(ctx, `SELECT password_hash FROM proxy_users WHERE id = $1`, userID).Scan(&passwordHash); err != nil {
		t.Fatalf("read password hash: %v", err)
	}
	if passwordHash != "original-hash" {
		t.Fatalf("password hash = %q, want original-hash", passwordHash)
	}

	updated, err = repo.Update(ctx, userID, models.UpdateProxyUserRequest{FallbackPoolIDs: []int{}})
	if err != nil {
		t.Fatalf("clear fallback pools: %v", err)
	}
	if updated.FallbackPoolIDs == nil || len(updated.FallbackPoolIDs) != 0 {
		t.Fatalf("fallback pools = %#v, want explicit empty list", updated.FallbackPoolIDs)
	}
	if updated.MainPoolID == nil || *updated.MainPoolID != mainPoolID {
		t.Fatalf("main Pool changed while clearing fallbacks: %#v", updated.MainPoolID)
	}

	zeroRate := 0
	replacementMainID := fallbackOneID
	replacementFallbacks := []int{mainPoolID}
	updated, err = repo.Update(ctx, userID, models.UpdateProxyUserRequest{
		MainPoolID:        &replacementMainID,
		FallbackPoolIDs:   replacementFallbacks,
		MaxRetries:        11,
		RequestsPerMinute: &zeroRate,
	})
	if err != nil {
		t.Fatalf("replace Pool fields: %v", err)
	}
	if updated.MainPoolID == nil || *updated.MainPoolID != replacementMainID ||
		!reflect.DeepEqual(updated.FallbackPoolIDs, replacementFallbacks) ||
		updated.MaxRetries != 11 || updated.RequestsPerMinute != 0 {
		t.Fatalf("replacement update = %#v", updated)
	}

	var clearMain models.UpdateProxyUserRequest
	if err := json.Unmarshal([]byte(`{"main_pool_id":null}`), &clearMain); err != nil {
		t.Fatalf("decode clear-main request: %v", err)
	}
	updated, err = repo.Update(ctx, userID, clearMain)
	if err != nil {
		t.Fatalf("clear main Pool: %v", err)
	}
	if updated.MainPoolID != nil {
		t.Fatalf("main Pool = %d, want nil", *updated.MainPoolID)
	}
	if !reflect.DeepEqual(updated.FallbackPoolIDs, replacementFallbacks) {
		t.Fatalf("clearing main Pool changed fallbacks: %#v", updated.FallbackPoolIDs)
	}
}
