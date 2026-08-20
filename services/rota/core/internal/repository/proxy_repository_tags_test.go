package repository

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/database"
)

func TestMergeProxyTagsPreservesOriginDuringSourceRefresh(t *testing.T) {
	got := mergeProxyTags(
		[]string{"origin:paid", "provider:static"},
		[]string{"subscription-node-name"},
	)
	want := []string{"origin:paid", "provider:static", "subscription-node-name"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mergeProxyTags() = %#v, want %#v", got, want)
	}
}

func TestMergeProxyTagsReplacesOnlyOriginWhenExplicitlyProvided(t *testing.T) {
	got := mergeProxyTags(
		[]string{"origin:unknown", "provider:static"},
		[]string{"origin:free", "provider:static"},
	)
	want := []string{"provider:static", "origin:free"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mergeProxyTags() = %#v, want %#v", got, want)
	}
}

func TestNormalizeProxyTagsTrimsDeduplicatesAndKeepsLastOrigin(t *testing.T) {
	got := normalizeProxyTags([]string{
		" provider:static ",
		"origin:paid",
		"provider:static",
		"",
		" origin:free ",
		"origin:paid",
		"origin:free",
	})
	want := []string{"provider:static", "origin:free"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalizeProxyTags() = %#v, want %#v", got, want)
	}
}

func TestBulkUpdateTagsPreservesCredentialsAndLifecyclePostgres(t *testing.T) {
	_, pool := newPoolRepositoryPostgres(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		ALTER TABLE proxies
			ADD COLUMN username VARCHAR(255),
			ADD COLUMN password TEXT,
			ADD COLUMN failed_since TIMESTAMPTZ,
			ADD COLUMN failure_episode_kind TEXT,
			ADD COLUMN next_health_check_at TIMESTAMPTZ,
			ADD COLUMN health_check_not_before TIMESTAMPTZ,
			ADD COLUMN last_health_check_at TIMESTAMPTZ,
			ADD COLUMN last_health_success_at TIMESTAMPTZ,
			ADD COLUMN base_health_status TEXT,
			ADD COLUMN youtube_health_status TEXT,
			ADD COLUMN last_health_verdict TEXT,
			ADD COLUMN last_error TEXT,
			ADD COLUMN archived_at TIMESTAMPTZ,
			ADD COLUMN archive_reason TEXT,
			ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	`); err != nil {
		t.Fatalf("add credential and lifecycle columns: %v", err)
	}

	var selectedID int
	if err := pool.QueryRow(ctx, `
		INSERT INTO proxies (
			address, protocol, username, password, status, tags,
			failed_since, failure_episode_kind, next_health_check_at,
			health_check_not_before, last_health_check_at,
			last_health_success_at, base_health_status,
			youtube_health_status, last_health_verdict, last_error,
			archived_at, archive_reason, updated_at
		) VALUES (
			'vless.example:443', 'vless', NULL, 'vless://secret-credential',
			'archived', $1,
			'2026-08-01 01:00:00+00', 'youtube_unusable',
			'2026-08-12 02:00:00+00', '2026-08-12 01:30:00+00',
			'2026-08-11 03:00:00+00', '2026-07-31 04:00:00+00',
			'reachable', 'unusable', 'failed', 'youtube blocked',
			'2026-08-10 05:00:00+00', 'youtube_unusable',
			'2001-01-01 00:00:00+00'
		) RETURNING id
	`, []string{" provider:keep ", "origin:paid", "duplicate", "duplicate", "remove-me", "conflict", ""}).Scan(&selectedID); err != nil {
		t.Fatalf("insert selected proxy: %v", err)
	}

	var untouchedID int
	if err := pool.QueryRow(ctx, `
		INSERT INTO proxies (address, protocol, password, status, tags, updated_at)
		VALUES ('untouched.example:443', 'vless', 'vless://untouched', 'failed', $1,
		        '2002-01-01 00:00:00+00')
		RETURNING id
	`, []string{"origin:paid", "untouched"}).Scan(&untouchedID); err != nil {
		t.Fatalf("insert untouched proxy: %v", err)
	}

	rowJSON := func(id int, omitMutable bool) string {
		t.Helper()
		expression := "to_jsonb(proxy)"
		if omitMutable {
			expression += " - 'tags' - 'updated_at'"
		}
		var value string
		query := "SELECT (" + expression + ")::text FROM proxies AS proxy WHERE id=$1"
		if err := pool.QueryRow(ctx, query, id).Scan(&value); err != nil {
			t.Fatalf("snapshot proxy %d: %v", id, err)
		}
		return value
	}

	selectedBefore := rowJSON(selectedID, true)
	untouchedBefore := rowJSON(untouchedID, false)
	repo := NewProxyRepository(&database.DB{Pool: pool})
	updated, err := repo.BulkUpdateTags(
		ctx,
		[]int{selectedID, selectedID + 1000000},
		[]string{" provider:new ", "conflict", "origin:unknown", "origin:free", "provider:new"},
		[]string{" conflict ", " remove-me ", "missing", "conflict"},
	)
	if err != nil {
		t.Fatalf("bulk update tags: %v", err)
	}
	if updated != 1 {
		t.Fatalf("updated rows = %d, want 1", updated)
	}

	var tags []string
	var updatedAt time.Time
	if err := pool.QueryRow(ctx,
		`SELECT tags, updated_at FROM proxies WHERE id=$1`, selectedID,
	).Scan(&tags, &updatedAt); err != nil {
		t.Fatalf("read updated tags: %v", err)
	}
	wantTags := []string{"duplicate", "origin:free", "provider:keep", "provider:new"}
	if !reflect.DeepEqual(tags, wantTags) {
		t.Fatalf("tags = %#v, want %#v", tags, wantTags)
	}
	if !updatedAt.After(time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("updated_at was not advanced: %s", updatedAt)
	}
	if selectedAfter := rowJSON(selectedID, true); selectedAfter != selectedBefore {
		t.Fatalf("bulk tag update changed credentials or lifecycle\nbefore: %s\nafter:  %s", selectedBefore, selectedAfter)
	}
	if untouchedAfter := rowJSON(untouchedID, false); untouchedAfter != untouchedBefore {
		t.Fatalf("unselected proxy changed\nbefore: %s\nafter:  %s", untouchedBefore, untouchedAfter)
	}
}
