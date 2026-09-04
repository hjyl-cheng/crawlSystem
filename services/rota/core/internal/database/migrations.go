package database

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// Migration represents a database migration
type Migration struct {
	Version     int
	Description string
	Up          string
	Down        string
	// NoTransaction is reserved for one-statement online DDL such as
	// CREATE INDEX CONCURRENTLY. The statement must be idempotent because the
	// migration record is written separately after the DDL succeeds.
	NoTransaction       bool
	ConcurrentIndexName string
}

// migrations holds all database migrations
var migrations = []Migration{
	{
		Version:     1,
		Description: "Create initial schema",
		Up: `
			CREATE TABLE IF NOT EXISTS schema_migrations (
				version INT PRIMARY KEY,
				description TEXT NOT NULL,
				applied_at TIMESTAMP NOT NULL DEFAULT NOW()
			);
		`,
		Down: `
			DROP TABLE IF EXISTS schema_migrations;
		`,
	},
	{
		Version:     2,
		Description: "Enable TimescaleDB extension",
		Up: `
			CREATE EXTENSION IF NOT EXISTS timescaledb;
		`,
		Down: `
			DROP EXTENSION IF EXISTS timescaledb;
		`,
	},
	{
		Version:     3,
		Description: "Create proxies table",
		Up: `
			CREATE TABLE IF NOT EXISTS proxies (
				id SERIAL PRIMARY KEY,
				address VARCHAR(255) NOT NULL,
				protocol VARCHAR(20) NOT NULL DEFAULT 'http',
				username VARCHAR(255),
				password TEXT,
				status VARCHAR(20) NOT NULL DEFAULT 'idle',
				requests BIGINT NOT NULL DEFAULT 0,
				successful_requests BIGINT NOT NULL DEFAULT 0,
				failed_requests BIGINT NOT NULL DEFAULT 0,
				avg_response_time INTEGER DEFAULT 0,
				last_check TIMESTAMP,
				last_error TEXT,
				created_at TIMESTAMP NOT NULL DEFAULT NOW(),
				updated_at TIMESTAMP NOT NULL DEFAULT NOW()
			);

			CREATE INDEX idx_proxies_address ON proxies(address);
			CREATE INDEX idx_proxies_status ON proxies(status);
			CREATE INDEX idx_proxies_protocol ON proxies(protocol);
		`,
		Down: `
			DROP INDEX IF EXISTS idx_proxies_protocol;
			DROP INDEX IF EXISTS idx_proxies_status;
			DROP INDEX IF EXISTS idx_proxies_address;
			DROP TABLE IF EXISTS proxies;
		`,
	},
	{
		Version:     4,
		Description: "Create settings table",
		Up: `
			CREATE TABLE IF NOT EXISTS settings (
				key VARCHAR(255) PRIMARY KEY,
				value JSONB NOT NULL,
				updated_at TIMESTAMP NOT NULL DEFAULT NOW()
			);

			-- Insert default settings
			-- Note: authentication settings are for PROXY server (port 8000), not dashboard/API
			INSERT INTO settings (key, value) VALUES
			('authentication', '{"enabled": false, "username": "", "password": ""}'::jsonb),
			('rotation', '{"method": "random", "time_based": {"interval": 120}, "remove_unhealthy": true, "fallback": true, "fallback_max_retries": 10, "follow_redirect": false, "timeout": 90, "retries": 3}'::jsonb),
			('rate_limit', '{"enabled": false, "interval": 1, "max_requests": 100}'::jsonb),
			('healthcheck', '{"timeout": 60, "workers": 20, "url": "https://api.ipify.org", "status": 200, "headers": ["User-Agent: Rota-HealthCheck/1.0"]}'::jsonb),
			('log_retention', '{"enabled": true, "retention_days": 30, "compression_after_days": 7, "cleanup_interval_hours": 24}'::jsonb)
			ON CONFLICT (key) DO NOTHING;
		`,
		Down: `
			DROP TABLE IF EXISTS settings;
		`,
	},
	{
		Version:     5,
		Description: "Create logs table as hypertable",
		Up: `
			CREATE TABLE IF NOT EXISTS logs (
				id BIGSERIAL,
				timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
				level VARCHAR(20) NOT NULL,
				message TEXT NOT NULL,
				details TEXT,
				metadata JSONB
			);

			-- Create hypertable
			SELECT create_hypertable('logs', 'timestamp', if_not_exists => TRUE);

			-- Create indexes
			CREATE INDEX idx_logs_level ON logs(level, timestamp DESC);
			CREATE INDEX idx_logs_timestamp ON logs(timestamp DESC);

			-- Add retention policy (keep logs for 30 days)
			SELECT add_retention_policy('logs', INTERVAL '30 days', if_not_exists => TRUE);

			-- Add compression policy (compress data older than 7 days)
			ALTER TABLE logs SET (
				timescaledb.compress,
				timescaledb.compress_segmentby = 'level'
			);
			SELECT add_compression_policy('logs', INTERVAL '7 days', if_not_exists => TRUE);
		`,
		Down: `
			DROP TABLE IF EXISTS logs;
		`,
	},
	{
		Version:     6,
		Description: "Create proxy_requests table as hypertable",
		Up: `
			CREATE TABLE IF NOT EXISTS proxy_requests (
				id BIGSERIAL,
				timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
				proxy_id INTEGER REFERENCES proxies(id) ON DELETE CASCADE,
				proxy_address VARCHAR(255) NOT NULL,
				method VARCHAR(10) NOT NULL,
				url TEXT,
				status_code INTEGER,
				response_time INTEGER,
				success BOOLEAN NOT NULL,
				error TEXT
			);

			-- Create hypertable
			SELECT create_hypertable('proxy_requests', 'timestamp', if_not_exists => TRUE);

			-- Create indexes
			CREATE INDEX idx_proxy_requests_proxy_id ON proxy_requests(proxy_id, timestamp DESC);
			CREATE INDEX idx_proxy_requests_success ON proxy_requests(success, timestamp DESC);
			CREATE INDEX idx_proxy_requests_timestamp ON proxy_requests(timestamp DESC);

			-- Add retention policy (keep request logs for 90 days)
			SELECT add_retention_policy('proxy_requests', INTERVAL '90 days', if_not_exists => TRUE);

			-- Add compression policy (compress data older than 14 days)
			ALTER TABLE proxy_requests SET (
				timescaledb.compress,
				timescaledb.compress_segmentby = 'proxy_id'
			);
			SELECT add_compression_policy('proxy_requests', INTERVAL '14 days', if_not_exists => TRUE);
		`,
		Down: `
			DROP TABLE IF EXISTS proxy_requests;
		`,
	},
	{
		Version:     7,
		Description: "Add log retention settings",
		Up: `
			INSERT INTO settings (key, value) VALUES
			('log_retention', '{"enabled": true, "retention_days": 30, "compression_after_days": 7, "cleanup_interval_hours": 24}'::jsonb)
			ON CONFLICT (key) DO NOTHING;
		`,
		Down: `
			DELETE FROM settings WHERE key = 'log_retention';
		`,
	},
	{
		Version:     8,
		Description: "Add metadata source index for proxy logs filtering",
		Up: `
			CREATE INDEX IF NOT EXISTS idx_logs_metadata_source ON logs((metadata->>'source'));
		`,
		Down: `
			DROP INDEX IF EXISTS idx_logs_metadata_source;
		`,
	},
	{
		Version:     9,
		Description: "Add unique constraint to proxy address",
		Up: `
			-- First, remove any duplicate proxies (keep the oldest one)
			DELETE FROM proxies
			WHERE id NOT IN (
				SELECT MIN(id)
				FROM proxies
				GROUP BY address, protocol
			);

			-- Now add the unique constraint
			ALTER TABLE proxies ADD CONSTRAINT unique_proxy_address_protocol UNIQUE (address, protocol);
		`,
		Down: `
			ALTER TABLE proxies DROP CONSTRAINT IF EXISTS unique_proxy_address_protocol;
		`,
	},
	{
		Version:     11,
		Description: "Add GeoIP fields to proxies table",
		Up: `
			ALTER TABLE proxies
				ADD COLUMN IF NOT EXISTS country_code  VARCHAR(3),
				ADD COLUMN IF NOT EXISTS country_name  VARCHAR(100),
				ADD COLUMN IF NOT EXISTS region_name   VARCHAR(100),
				ADD COLUMN IF NOT EXISTS city_name     VARCHAR(100),
				ADD COLUMN IF NOT EXISTS latitude      DOUBLE PRECISION,
				ADD COLUMN IF NOT EXISTS longitude     DOUBLE PRECISION,
				ADD COLUMN IF NOT EXISTS isp           VARCHAR(255),
				ADD COLUMN IF NOT EXISTS geo_updated_at TIMESTAMP;

			CREATE INDEX IF NOT EXISTS idx_proxies_country_code ON proxies(country_code);
			CREATE INDEX IF NOT EXISTS idx_proxies_region_name  ON proxies(region_name);
		`,
		Down: `
			ALTER TABLE proxies
				DROP COLUMN IF EXISTS country_code,
				DROP COLUMN IF EXISTS country_name,
				DROP COLUMN IF EXISTS region_name,
				DROP COLUMN IF EXISTS city_name,
				DROP COLUMN IF EXISTS latitude,
				DROP COLUMN IF EXISTS longitude,
				DROP COLUMN IF EXISTS isp,
				DROP COLUMN IF EXISTS geo_updated_at;
			DROP INDEX IF EXISTS idx_proxies_country_code;
			DROP INDEX IF EXISTS idx_proxies_region_name;
		`,
	},
	{
		Version:     12,
		Description: "Create proxy_sources table",
		Up: `
			CREATE TABLE IF NOT EXISTS proxy_sources (
				id          SERIAL PRIMARY KEY,
				name        VARCHAR(255) NOT NULL,
				url         TEXT NOT NULL,
				protocol    VARCHAR(20) NOT NULL DEFAULT 'http',
				enabled     BOOLEAN NOT NULL DEFAULT true,
				interval_minutes INTEGER NOT NULL DEFAULT 60,
				last_fetched_at  TIMESTAMP,
				last_count       INTEGER NOT NULL DEFAULT 0,
				last_error       TEXT,
				created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
				updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
			);
			CREATE INDEX IF NOT EXISTS idx_proxy_sources_enabled ON proxy_sources(enabled);
		`,
		Down: `
			DROP TABLE IF EXISTS proxy_sources;
		`,
	},
	{
		Version:     13,
		Description: "Create proxy pools and pool_proxies tables",
		Up: `
			CREATE TABLE IF NOT EXISTS proxy_pools (
				id               SERIAL PRIMARY KEY,
				name             VARCHAR(255) NOT NULL,
				description      TEXT,
				country_code     VARCHAR(3),
				region_name      VARCHAR(100),
				city_name        VARCHAR(100),
				rotation_method  VARCHAR(30) NOT NULL DEFAULT 'roundrobin',
				stick_count      INTEGER NOT NULL DEFAULT 10,
				health_check_url TEXT NOT NULL DEFAULT 'https://api.ipify.org',
				health_check_cron VARCHAR(100) NOT NULL DEFAULT '*/30 * * * *',
				health_check_enabled BOOLEAN NOT NULL DEFAULT true,
				auto_sync        BOOLEAN NOT NULL DEFAULT true,
				enabled          BOOLEAN NOT NULL DEFAULT true,
				created_at       TIMESTAMP NOT NULL DEFAULT NOW(),
				updated_at       TIMESTAMP NOT NULL DEFAULT NOW()
			);

			CREATE TABLE IF NOT EXISTS pool_proxies (
				pool_id   INTEGER NOT NULL REFERENCES proxy_pools(id) ON DELETE CASCADE,
				proxy_id  INTEGER NOT NULL REFERENCES proxies(id) ON DELETE CASCADE,
				added_at  TIMESTAMP NOT NULL DEFAULT NOW(),
				PRIMARY KEY (pool_id, proxy_id)
			);

			CREATE INDEX IF NOT EXISTS idx_pool_proxies_pool_id  ON pool_proxies(pool_id);
			CREATE INDEX IF NOT EXISTS idx_pool_proxies_proxy_id ON pool_proxies(proxy_id);
		`,
		Down: `
			DROP TABLE IF EXISTS pool_proxies;
			DROP TABLE IF EXISTS proxy_pools;
		`,
	},
	{
		Version:     16,
		Description: "Create admin_credentials table for dashboard authentication",
		Up: `
			CREATE TABLE IF NOT EXISTS admin_credentials (
				id            SERIAL PRIMARY KEY,
				username      VARCHAR(255) NOT NULL UNIQUE,
				password_hash TEXT NOT NULL,
				created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
				updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
			);
		`,
		Down: `DROP TABLE IF EXISTS admin_credentials;`,
	},
	{
		Version:     15,
		Description: "Add pool_geo_filters table for multi-location pool membership",
		Up: `
			CREATE TABLE IF NOT EXISTS pool_geo_filters (
				id           SERIAL PRIMARY KEY,
				pool_id      INTEGER NOT NULL REFERENCES proxy_pools(id) ON DELETE CASCADE,
				country_code VARCHAR(3),
				city_name    VARCHAR(100),
				UNIQUE (pool_id, country_code, city_name)
			);
			CREATE INDEX IF NOT EXISTS idx_pool_geo_filters_pool_id ON pool_geo_filters(pool_id);

			-- Migrate existing single country/city filters into the new table
			INSERT INTO pool_geo_filters (pool_id, country_code, city_name)
			SELECT id, country_code, city_name
			FROM proxy_pools
			WHERE country_code IS NOT NULL
			ON CONFLICT DO NOTHING;
		`,
		Down: `DROP TABLE IF EXISTS pool_geo_filters;`,
	},
	{
		Version:     14,
		Description: "Create proxy_users table for per-user pool authentication",
		Up: `
			CREATE TABLE IF NOT EXISTS proxy_users (
				id               SERIAL PRIMARY KEY,
				username         VARCHAR(255) NOT NULL UNIQUE,
				password_hash    TEXT NOT NULL,
				enabled          BOOLEAN NOT NULL DEFAULT true,
				main_pool_id     INTEGER REFERENCES proxy_pools(id) ON DELETE SET NULL,
				fallback_pool_ids INTEGER[] NOT NULL DEFAULT '{}',
				max_retries      INTEGER NOT NULL DEFAULT 5,
				created_at       TIMESTAMP NOT NULL DEFAULT NOW(),
				updated_at       TIMESTAMP NOT NULL DEFAULT NOW()
			);
			CREATE INDEX IF NOT EXISTS idx_proxy_users_username ON proxy_users(username);
			CREATE INDEX IF NOT EXISTS idx_proxy_users_enabled  ON proxy_users(enabled);
		`,
		Down: `
			DROP TABLE IF EXISTS proxy_users;
		`,
	},
	{
		Version:     17,
		Description: "Add tags to proxies table",
		Up: `
			ALTER TABLE proxies ADD COLUMN IF NOT EXISTS tags TEXT[] NOT NULL DEFAULT '{}';
			CREATE INDEX IF NOT EXISTS idx_proxies_tags ON proxies USING gin(tags);
		`,
		Down: `
			DROP INDEX IF EXISTS idx_proxies_tags;
			ALTER TABLE proxies DROP COLUMN IF EXISTS tags;
		`,
	},
	{
		Version:     18,
		Description: "Add sync_mode to proxy_pools, ISP filter table, pool_alerts table",
		Up: `
			-- sync_mode: 'auto' | 'manual'
			ALTER TABLE proxy_pools ADD COLUMN IF NOT EXISTS sync_mode VARCHAR(10) NOT NULL DEFAULT 'auto';

			-- ISP filters for pools
			CREATE TABLE IF NOT EXISTS pool_isp_filters (
				id       SERIAL PRIMARY KEY,
				pool_id  INTEGER NOT NULL REFERENCES proxy_pools(id) ON DELETE CASCADE,
				isp      TEXT NOT NULL,
				UNIQUE (pool_id, isp)
			);
			CREATE INDEX IF NOT EXISTS idx_pool_isp_filters_pool_id ON pool_isp_filters(pool_id);

			-- Tag filters for pools
			CREATE TABLE IF NOT EXISTS pool_tag_filters (
				id      SERIAL PRIMARY KEY,
				pool_id INTEGER NOT NULL REFERENCES proxy_pools(id) ON DELETE CASCADE,
				tag     TEXT NOT NULL,
				UNIQUE (pool_id, tag)
			);
			CREATE INDEX IF NOT EXISTS idx_pool_tag_filters_pool_id ON pool_tag_filters(pool_id);

			-- Alert rules per pool: fire webhook when active proxy count drops below threshold
			CREATE TABLE IF NOT EXISTS pool_alert_rules (
				id                  SERIAL PRIMARY KEY,
				pool_id             INTEGER NOT NULL REFERENCES proxy_pools(id) ON DELETE CASCADE,
				enabled             BOOLEAN NOT NULL DEFAULT true,
				min_active_proxies  INTEGER NOT NULL DEFAULT 5,
				webhook_url         TEXT NOT NULL,
				webhook_method      VARCHAR(10) NOT NULL DEFAULT 'POST',
				last_fired_at       TIMESTAMP,
				cooldown_minutes    INTEGER NOT NULL DEFAULT 30,
				created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
				updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
			);
			CREATE INDEX IF NOT EXISTS idx_pool_alert_rules_pool_id ON pool_alert_rules(pool_id);
		`,
		Down: `
			DROP TABLE IF EXISTS pool_alert_rules;
			DROP TABLE IF EXISTS pool_tag_filters;
			DROP TABLE IF EXISTS pool_isp_filters;
			ALTER TABLE proxy_pools DROP COLUMN IF EXISTS sync_mode;
		`,
	},
	{
		Version:     20,
		Description: "Add source_id to proxies for cascade delete on source removal",
		Up: `
			ALTER TABLE proxies ADD COLUMN IF NOT EXISTS source_id INTEGER REFERENCES proxy_sources(id) ON DELETE CASCADE;
			CREATE INDEX IF NOT EXISTS idx_proxies_source_id ON proxies(source_id);
		`,
		Down: `
			DROP INDEX IF EXISTS idx_proxies_source_id;
			ALTER TABLE proxies DROP COLUMN IF EXISTS source_id;
		`,
	},
	{
		Version:     19,
		Description: "Add proxy cleanup settings and rate limit to proxy_users",
		Up: `
			-- Dead proxy cleanup settings
			INSERT INTO settings (key, value) VALUES
			('proxy_cleanup', '{"enabled": false, "max_failed_days": 7, "min_success_rate": 0, "cleanup_interval_hours": 24}'::jsonb)
			ON CONFLICT (key) DO NOTHING;

			-- Per-user rate limiting
			ALTER TABLE proxy_users ADD COLUMN IF NOT EXISTS requests_per_minute INTEGER NOT NULL DEFAULT 0;
		`,
		Down: `
			DELETE FROM settings WHERE key = 'proxy_cleanup';
			ALTER TABLE proxy_users DROP COLUMN IF EXISTS requests_per_minute;
		`,
	},
	{
		Version:     10,
		Description: "Update default timeout and retry settings for better proxy compatibility",
		Up: `
			-- Update rotation settings: increase timeout from 30s to 90s, retries from 2 to 3
			UPDATE settings
			SET value = jsonb_set(
				jsonb_set(value, '{timeout}', '90'),
				'{retries}', '3'
			)
			WHERE key = 'rotation';

			-- Update healthcheck settings: increase timeout from 30s to 60s
			UPDATE settings
			SET value = jsonb_set(value, '{timeout}', '60')
			WHERE key = 'healthcheck';
		`,
		Down: `
			-- Revert rotation settings to original values
			UPDATE settings
			SET value = jsonb_set(
				jsonb_set(value, '{timeout}', '30'),
				'{retries}', '2'
			)
			WHERE key = 'rotation';

			-- Revert healthcheck settings to original values
			UPDATE settings
			SET value = jsonb_set(value, '{timeout}', '30')
			WHERE key = 'healthcheck';
		`,
	},
	{
		Version:     21,
		Description: "Source: last_total column + per-source soft cleanup settings + proxies.last_seen_at",
		Up: `
			-- proxy_sources: total lines in last fetch + opt-in cleanup config
			ALTER TABLE proxy_sources
			  ADD COLUMN IF NOT EXISTS last_total       INTEGER NOT NULL DEFAULT 0,
			  ADD COLUMN IF NOT EXISTS cleanup_enabled  BOOLEAN NOT NULL DEFAULT false,
			  ADD COLUMN IF NOT EXISTS cleanup_days     INTEGER NOT NULL DEFAULT 7;

			-- proxies: last time a proxy was seen in its source's fetch response
			ALTER TABLE proxies
			  ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP;

			CREATE INDEX IF NOT EXISTS idx_proxies_source_last_seen
			  ON proxies(source_id, last_seen_at)
			  WHERE source_id IS NOT NULL;
		`,
		Down: `
			DROP INDEX IF EXISTS idx_proxies_source_last_seen;
			ALTER TABLE proxies       DROP COLUMN IF EXISTS last_seen_at;
			ALTER TABLE proxy_sources DROP COLUMN IF EXISTS cleanup_days;
			ALTER TABLE proxy_sources DROP COLUMN IF EXISTS cleanup_enabled;
			ALTER TABLE proxy_sources DROP COLUMN IF EXISTS last_total;
		`,
	},
	{
		Version:     22,
		Description: "Add managed GeoIP settings",
		Up: `
			INSERT INTO settings (key, value) VALUES
			('geoip', '{"provider":"local","maxmind_license_key":"","maxmind_db_path":"/app/geoip/managed/GeoLite2-City.mmdb","maxmind_url":"","auto_update":false,"update_interval_hours":168}'::jsonb)
			ON CONFLICT (key) DO NOTHING;
		`,
		Down: `DELETE FROM settings WHERE key = 'geoip';`,
	},
	{
		Version:     24,
		Description: "Add strict TLS health-check setting",
		Up: `
			UPDATE settings
			SET value = jsonb_set(value, '{strict_tls}', 'false'::jsonb, true)
			WHERE key = 'healthcheck';
		`,
		Down: `
			UPDATE settings
			SET value = value - 'strict_tls'
			WHERE key = 'healthcheck';
		`,
	},
	{
		Version:     25,
		Description: "Add default tags to proxy sources",
		Up: `
			ALTER TABLE proxy_sources
			ADD COLUMN IF NOT EXISTS default_tags TEXT[] NOT NULL DEFAULT '{}';
		`,
		Down: `
			ALTER TABLE proxy_sources DROP COLUMN IF EXISTS default_tags;
		`,
	},
	{
		Version:     1001,
		Description: "Add proxy lifecycle observation and archival",
		Up: `
			ALTER TABLE proxies DROP CONSTRAINT IF EXISTS proxies_source_id_fkey;
			ALTER TABLE proxies ADD CONSTRAINT proxies_source_id_fkey
			  FOREIGN KEY (source_id) REFERENCES proxy_sources(id) ON DELETE SET NULL;

			ALTER TABLE proxies
			  ADD COLUMN IF NOT EXISTS failed_since TIMESTAMPTZ,
			  ADD COLUMN IF NOT EXISTS failure_episode_kind VARCHAR(32),
			  ADD COLUMN IF NOT EXISTS next_health_check_at TIMESTAMPTZ DEFAULT NOW(),
			  ADD COLUMN IF NOT EXISTS health_check_not_before TIMESTAMPTZ,
			  ADD COLUMN IF NOT EXISTS last_health_check_at TIMESTAMPTZ,
			  ADD COLUMN IF NOT EXISTS last_health_success_at TIMESTAMPTZ,
			  ADD COLUMN IF NOT EXISTS base_health_status VARCHAR(16),
			  ADD COLUMN IF NOT EXISTS youtube_health_status VARCHAR(16),
			  ADD COLUMN IF NOT EXISTS last_health_verdict JSONB,
			  ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ,
			  ADD COLUMN IF NOT EXISTS archive_reason TEXT;

			UPDATE proxies
			SET next_health_check_at = NULL
			WHERE status = 'active';

			UPDATE proxies
			SET failed_since = COALESCE(last_check, updated_at, created_at),
			    failure_episode_kind = 'soft_unreachable',
			    next_health_check_at = NOW()
			WHERE status = 'failed';

			ALTER TABLE proxies DROP CONSTRAINT IF EXISTS proxies_status_check;
			ALTER TABLE proxies ADD CONSTRAINT proxies_status_check
			  CHECK (status IN ('idle', 'active', 'failed', 'archived'));

			DO $$
			BEGIN
			  IF NOT EXISTS (
			    SELECT 1 FROM pg_constraint WHERE conname = 'proxies_failure_episode_kind_check'
			  ) THEN
			    ALTER TABLE proxies ADD CONSTRAINT proxies_failure_episode_kind_check
			      CHECK (failure_episode_kind IS NULL OR failure_episode_kind IN (
			        'hard_unreachable', 'soft_unreachable', 'youtube_unusable'
			      ));
			  END IF;
			END $$;

			CREATE INDEX IF NOT EXISTS idx_proxies_health_check_due
			  ON proxies(next_health_check_at)
			  WHERE status IN ('idle', 'failed') AND next_health_check_at IS NOT NULL;

			CREATE TABLE IF NOT EXISTS proxy_health_checks (
			  id BIGSERIAL PRIMARY KEY,
			  proxy_id INTEGER NOT NULL REFERENCES proxies(id) ON DELETE CASCADE,
			  started_at TIMESTAMPTZ NOT NULL,
			  checked_at TIMESTAMPTZ NOT NULL,
			  base_result JSONB NOT NULL,
			  youtube_result JSONB NOT NULL,
			  verdict VARCHAR(32) NOT NULL,
			  conclusive BOOLEAN NOT NULL,
			  control_path_healthy BOOLEAN NOT NULL,
			  previous_status VARCHAR(20) NOT NULL,
			  resulting_status VARCHAR(20) NOT NULL,
			  applied BOOLEAN NOT NULL DEFAULT true,
			  error TEXT,
			  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
			);
			CREATE INDEX IF NOT EXISTS idx_proxy_health_checks_proxy_checked
			  ON proxy_health_checks(proxy_id, checked_at DESC);

			INSERT INTO settings (key, value) VALUES
			('proxy_lifecycle', '{"auto_archive_enabled": true, "hard_unreachable_after_hours": 6, "soft_unreachable_after_hours": 24, "youtube_unusable_after_hours": 72}'::jsonb)
			ON CONFLICT (key) DO NOTHING;

			UPDATE settings
			SET value = jsonb_set(
			  jsonb_set(value, '{base_url}', '"https://www.google.com/generate_204"'::jsonb, true),
			  '{base_status}', '204'::jsonb, true
			)
			WHERE key = 'healthcheck';

			UPDATE settings
			SET value = jsonb_set(value, '{url}', '"https://www.youtube.com/watch?v=_xXsXvsYAhA"'::jsonb, true)
			WHERE key = 'healthcheck'
			  AND COALESCE(value ->> 'url', '') IN ('', 'https://api.ipify.org');

			UPDATE settings
			SET value = jsonb_set(value, '{enabled}', 'false'::jsonb, true)
			WHERE key = 'proxy_cleanup';

			UPDATE proxy_sources
			SET cleanup_enabled = false
			WHERE cleanup_enabled = true;

			UPDATE proxy_pools
			SET health_check_url = 'https://www.youtube.com/watch?v=_xXsXvsYAhA'
			WHERE health_check_url = 'https://api.ipify.org';

			ALTER TABLE proxy_pools
			ALTER COLUMN health_check_url SET DEFAULT 'https://www.youtube.com/watch?v=_xXsXvsYAhA';
		`,
		Down: `
			ALTER TABLE proxies DROP CONSTRAINT IF EXISTS proxies_source_id_fkey;
			ALTER TABLE proxies ADD CONSTRAINT proxies_source_id_fkey
			  FOREIGN KEY (source_id) REFERENCES proxy_sources(id) ON DELETE CASCADE;
			DELETE FROM settings WHERE key = 'proxy_lifecycle';
			UPDATE settings
			SET value = value - 'base_url' - 'base_status'
			WHERE key = 'healthcheck';
			DROP TABLE IF EXISTS proxy_health_checks;
			DROP INDEX IF EXISTS idx_proxies_health_check_due;
			ALTER TABLE proxies DROP CONSTRAINT IF EXISTS proxies_failure_episode_kind_check;
			ALTER TABLE proxies DROP CONSTRAINT IF EXISTS proxies_status_check;
			ALTER TABLE proxy_pools
			ALTER COLUMN health_check_url SET DEFAULT 'https://api.ipify.org';
			ALTER TABLE proxies
			  DROP COLUMN IF EXISTS archive_reason,
			  DROP COLUMN IF EXISTS archived_at,
			  DROP COLUMN IF EXISTS last_health_verdict,
			  DROP COLUMN IF EXISTS youtube_health_status,
			  DROP COLUMN IF EXISTS base_health_status,
			  DROP COLUMN IF EXISTS last_health_success_at,
			  DROP COLUMN IF EXISTS last_health_check_at,
			  DROP COLUMN IF EXISTS next_health_check_at,
			  DROP COLUMN IF EXISTS health_check_not_before,
			  DROP COLUMN IF EXISTS failure_episode_kind,
			  DROP COLUMN IF EXISTS failed_since;
		`,
	},
	{
		Version:     1002,
		Description: "Add mixed Xray nodes, stable identities, and source memberships",
		Up: `
			CREATE EXTENSION IF NOT EXISTS pgcrypto;

			ALTER TABLE proxy_sources
			  ADD COLUMN IF NOT EXISTS last_supported INTEGER NOT NULL DEFAULT 0,
			  ADD COLUMN IF NOT EXISTS last_skipped INTEGER NOT NULL DEFAULT 0;

			ALTER TABLE proxies
			  ADD COLUMN IF NOT EXISTS node_identity VARCHAR(64);

			UPDATE proxies
			SET node_identity = encode(digest(
			  CASE
			    WHEN protocol IN ('vless', 'vmess', 'trojan', 'shadowsocks')
			         AND NULLIF(btrim(password), '') IS NOT NULL
			      THEN btrim(password)
			    ELSE 'endpoint' || chr(31) || lower(btrim(protocol)) || chr(31) || btrim(address)
			  END,
			  'sha256'
			), 'hex')
			WHERE node_identity IS NULL OR node_identity = '';

			ALTER TABLE proxies ALTER COLUMN node_identity SET NOT NULL;
			ALTER TABLE proxies DROP CONSTRAINT IF EXISTS unique_proxy_address_protocol;
			ALTER TABLE proxies
			  ADD CONSTRAINT unique_proxy_node_identity UNIQUE (node_identity);
			CREATE INDEX IF NOT EXISTS idx_proxies_endpoint_protocol
			  ON proxies(address, protocol);

			CREATE TABLE IF NOT EXISTS proxy_source_memberships (
			  source_id INTEGER NOT NULL REFERENCES proxy_sources(id) ON DELETE CASCADE,
			  proxy_id INTEGER NOT NULL REFERENCES proxies(id) ON DELETE CASCADE,
			  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			  PRIMARY KEY (source_id, proxy_id)
			);
			CREATE INDEX IF NOT EXISTS idx_proxy_source_memberships_proxy
			  ON proxy_source_memberships(proxy_id, source_id);

			INSERT INTO proxy_source_memberships (source_id, proxy_id, last_seen_at)
			SELECT source_id, id, COALESCE(last_seen_at, created_at, NOW())
			FROM proxies
			WHERE source_id IS NOT NULL
			ON CONFLICT (source_id, proxy_id) DO UPDATE
			SET last_seen_at = EXCLUDED.last_seen_at;

			UPDATE settings
			SET value = jsonb_set(
			  value,
			  '{allowed_protocols}',
			  '["http","https","socks4","socks4a","socks5","vless","vmess","trojan","shadowsocks"]'::jsonb,
			  true
			)
			WHERE key = 'rotation'
			  AND NOT (value ? 'allowed_protocols');
		`,
		Down: `
			DROP TABLE IF EXISTS proxy_source_memberships;
			DROP INDEX IF EXISTS idx_proxies_endpoint_protocol;
			ALTER TABLE proxies DROP CONSTRAINT IF EXISTS unique_proxy_node_identity;
			ALTER TABLE proxies
			  ADD CONSTRAINT unique_proxy_address_protocol UNIQUE (address, protocol);
			ALTER TABLE proxies DROP COLUMN IF EXISTS node_identity;
			ALTER TABLE proxy_sources DROP COLUMN IF EXISTS last_skipped;
			ALTER TABLE proxy_sources DROP COLUMN IF EXISTS last_supported;
		`,
	},
	{
		Version:     1003,
		Description: "Move running slots and crawler reports into Rota",
		Up: `
			ALTER TABLE proxies
			  ADD COLUMN IF NOT EXISTS cooldown_until TIMESTAMPTZ,
			  ADD COLUMN IF NOT EXISTS last_youtube_status INTEGER,
			  ADD COLUMN IF NOT EXISTS last_youtube_error TEXT,
			  ADD COLUMN IF NOT EXISTS last_youtube_check TIMESTAMPTZ,
			  ADD COLUMN IF NOT EXISTS last_rota_youtube_status INTEGER,
			  ADD COLUMN IF NOT EXISTS last_rota_youtube_error TEXT,
			  ADD COLUMN IF NOT EXISTS last_rota_youtube_check TIMESTAMPTZ,
			  ADD COLUMN IF NOT EXISTS youtube_successful_requests BIGINT NOT NULL DEFAULT 0,
			  ADD COLUMN IF NOT EXISTS youtube_failed_requests BIGINT NOT NULL DEFAULT 0,
			  ADD COLUMN IF NOT EXISTS youtube_avg_response_time INTEGER,
			  ADD COLUMN IF NOT EXISTS youtube_avg_detail_time INTEGER,
			  ADD COLUMN IF NOT EXISTS youtube_failure_score DOUBLE PRECISION NOT NULL DEFAULT 0,
			  ADD COLUMN IF NOT EXISTS last_youtube_success TIMESTAMPTZ,
			  ADD COLUMN IF NOT EXISTS last_youtube_failure TIMESTAMPTZ;

			CREATE INDEX IF NOT EXISTS idx_proxies_control_eligibility
			  ON proxies(status, cooldown_until, id);

			CREATE TABLE IF NOT EXISTS proxy_running_slots (
			  slot_name TEXT PRIMARY KEY,
			  role TEXT NOT NULL CHECK (role IN ('discover', 'channel', 'detail')),
			  slot_no INTEGER NOT NULL CHECK (slot_no > 0),
			  pool_id INTEGER NOT NULL REFERENCES proxy_pools(id) ON DELETE CASCADE,
			  user_id INTEGER NOT NULL REFERENCES proxy_users(id) ON DELETE CASCADE,
			  proxy_id INTEGER REFERENCES proxies(id) ON DELETE SET NULL,
			  assignment_version BIGINT NOT NULL DEFAULT 0,
			  credential_generation BIGINT NOT NULL DEFAULT 0,
			  assigned_at TIMESTAMPTZ,
			  ready_after TIMESTAMPTZ,
			  worker_id TEXT,
			  lease_id TEXT,
			  lease_until TIMESTAMPTZ,
			  last_heartbeat_at TIMESTAMPTZ,
			  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			  UNIQUE(role, slot_no)
			);
			ALTER TABLE proxy_running_slots
			  ADD COLUMN IF NOT EXISTS credential_generation BIGINT NOT NULL DEFAULT 0;

			DO $$
			BEGIN
			  IF to_regclass('public.bullmq_proxy_slots') IS NOT NULL THEN
			    -- Prefer live leases and the newest binding when corrupt legacy data
			    -- contains the same Worker or Proxy more than once.
			    INSERT INTO proxy_running_slots (
			      slot_name, role, slot_no, pool_id, user_id, proxy_id,
			      assignment_version, assigned_at, ready_after, worker_id,
			      lease_id, lease_until, last_heartbeat_at, created_at, updated_at
			    )
			    SELECT slot_name, role, slot_no, pool_id, user_id,
			           CASE WHEN proxy_rank = 1 THEN proxy_id ELSE NULL END,
			           CASE WHEN proxy_rank = 1 AND proxy_id IS NOT NULL THEN 1 ELSE 0 END,
			           CASE WHEN proxy_rank = 1 THEN assigned_at ELSE NULL END,
			           CASE WHEN proxy_rank = 1 THEN ready_after ELSE NULL END,
			           CASE WHEN live_lease AND worker_rank = 1 THEN worker_id ELSE NULL END,
			           CASE WHEN live_lease AND worker_rank = 1 THEN encode(gen_random_bytes(16), 'hex') ELSE NULL END,
			           CASE WHEN live_lease AND worker_rank = 1 THEN lease_until ELSE NULL END,
			           CASE WHEN live_lease AND worker_rank = 1 THEN last_heartbeat_at ELSE NULL END,
			           created_at, updated_at
			    FROM (
			      SELECT legacy.*,
			             (worker_id IS NOT NULL AND lease_until > NOW()) AS live_lease,
			             CASE WHEN proxy_id IS NULL THEN 1 ELSE
			               ROW_NUMBER() OVER (
			                 PARTITION BY proxy_id
				                 ORDER BY COALESCE(worker_id IS NOT NULL AND lease_until > NOW(), false) DESC,
			                          updated_at DESC, slot_name
			               )
			             END AS proxy_rank,
			             CASE WHEN worker_id IS NULL THEN 1 ELSE
			               ROW_NUMBER() OVER (
			                 PARTITION BY worker_id
				                 ORDER BY COALESCE(lease_until > NOW(), false) DESC, lease_until DESC NULLS LAST,
			                          updated_at DESC, slot_name
			               )
			             END AS worker_rank
			      FROM bullmq_proxy_slots legacy
			    ) ranked
			    ON CONFLICT (slot_name) DO NOTHING;
			  END IF;
			END $$;

			-- A previous partial/manual installation may already have this table.
			-- Normalize it before installing uniqueness constraints.
			UPDATE proxy_running_slots
			SET worker_id = NULL, lease_id = NULL, lease_until = NULL,
			    last_heartbeat_at = NULL, updated_at = NOW()
			WHERE worker_id IS NOT NULL
			  AND (lease_until IS NULL OR lease_until <= NOW());

			WITH ranked AS (
			  SELECT slot_name,
			         ROW_NUMBER() OVER (PARTITION BY proxy_id ORDER BY updated_at DESC, slot_name) AS position
			  FROM proxy_running_slots
			  WHERE proxy_id IS NOT NULL
			)
			UPDATE proxy_running_slots slots
			SET proxy_id = NULL, assigned_at = NULL, ready_after = NULL,
			    assignment_version = assignment_version + 1, updated_at = NOW()
			FROM ranked
			WHERE slots.slot_name = ranked.slot_name AND ranked.position > 1;

			WITH ranked AS (
			  SELECT slot_name,
			         ROW_NUMBER() OVER (PARTITION BY worker_id ORDER BY lease_until DESC, updated_at DESC, slot_name) AS position
			  FROM proxy_running_slots
			  WHERE worker_id IS NOT NULL
			)
			UPDATE proxy_running_slots slots
			SET worker_id = NULL, lease_id = NULL, lease_until = NULL,
			    last_heartbeat_at = NULL, updated_at = NOW()
			FROM ranked
			WHERE slots.slot_name = ranked.slot_name AND ranked.position > 1;

			UPDATE proxy_running_slots
			SET lease_id = encode(gen_random_bytes(16), 'hex')
			WHERE worker_id IS NOT NULL AND lease_id IS NULL;

			CREATE UNIQUE INDEX IF NOT EXISTS idx_proxy_running_slots_proxy
			  ON proxy_running_slots(proxy_id) WHERE proxy_id IS NOT NULL;
			CREATE UNIQUE INDEX IF NOT EXISTS idx_proxy_running_slots_worker
			  ON proxy_running_slots(worker_id) WHERE worker_id IS NOT NULL;
			CREATE UNIQUE INDEX IF NOT EXISTS idx_proxy_running_slots_role_slot
			  ON proxy_running_slots(role, slot_no);
			CREATE INDEX IF NOT EXISTS idx_proxy_running_slots_lease
			  ON proxy_running_slots(role, lease_until);

			CREATE TABLE IF NOT EXISTS proxy_control_reports (
			  id BIGSERIAL PRIMARY KEY,
			  incident_id TEXT UNIQUE,
			  proxy_id INTEGER REFERENCES proxies(id) ON DELETE SET NULL,
			  proxy_user TEXT,
			  outcome TEXT NOT NULL CHECK (outcome IN ('success', 'failure')),
			  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			  result JSONB,
			  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			  completed_at TIMESTAMPTZ
			);
			CREATE INDEX IF NOT EXISTS idx_proxy_control_reports_proxy_created
			  ON proxy_control_reports(proxy_id, created_at DESC);
		`,
		Down: `
			DROP TABLE IF EXISTS proxy_control_reports;
			DROP TABLE IF EXISTS proxy_running_slots;
			DROP INDEX IF EXISTS idx_proxies_control_eligibility;
		`,
	},
	{
		Version:     1004,
		Description: "Add fenced proxy control tasks, observations, and lease history",
		Up: `
			CREATE EXTENSION IF NOT EXISTS pgcrypto;

			ALTER TABLE proxies
			  ADD COLUMN IF NOT EXISTS country_verified_at TIMESTAMPTZ,
			  ADD COLUMN IF NOT EXISTS egress_identity_mode TEXT NOT NULL DEFAULT 'unknown',
			  ADD COLUMN IF NOT EXISTS sticky_session_key_encrypted BYTEA,
			  ADD COLUMN IF NOT EXISTS identity_valid_until TIMESTAMPTZ,
			  ADD COLUMN IF NOT EXISTS network_identity_key TEXT,
			  ADD COLUMN IF NOT EXISTS last_identity_verified_at TIMESTAMPTZ;

			UPDATE proxies
			SET network_identity_key='net_' || encode(gen_random_bytes(16), 'hex')
			WHERE network_identity_key IS NULL OR btrim(network_identity_key)='';

			ALTER TABLE proxies ALTER COLUMN network_identity_key SET NOT NULL;
			CREATE UNIQUE INDEX IF NOT EXISTS idx_proxies_network_identity_key
			  ON proxies(network_identity_key);

			ALTER TABLE proxy_running_slots
			  ADD COLUMN IF NOT EXISTS worker_instance_id TEXT,
			  ADD COLUMN IF NOT EXISTS current_lease_id TEXT,
			  ADD COLUMN IF NOT EXISTS identity_policy_id TEXT,
			  ADD COLUMN IF NOT EXISTS identity_policy_version INTEGER,
			  ADD COLUMN IF NOT EXISTS identity_policy_hash TEXT,
			  ADD COLUMN IF NOT EXISTS required_egress_country VARCHAR(2),
			  ADD COLUMN IF NOT EXISTS network_identity_key TEXT,
			  ADD COLUMN IF NOT EXISTS profile_epoch BIGINT NOT NULL DEFAULT 0,
			  ADD COLUMN IF NOT EXISTS active_task_id TEXT,
			  ADD COLUMN IF NOT EXISTS active_task_started_at TIMESTAMPTZ,
			  ADD COLUMN IF NOT EXISTS pending_action TEXT,
			  ADD COLUMN IF NOT EXISTS pending_incident_id TEXT,
			  ADD COLUMN IF NOT EXISTS control_state TEXT NOT NULL DEFAULT 'unleased',
			  ADD COLUMN IF NOT EXISTS rotation_deadline_at TIMESTAMPTZ;

			ALTER TABLE proxy_running_slots DROP CONSTRAINT IF EXISTS proxy_running_slots_role_check;
			ALTER TABLE proxy_running_slots
			  ADD CONSTRAINT proxy_running_slots_role_check
			  CHECK (role IN ('discover','channel','query_quality','detail'));

			UPDATE proxy_running_slots slots
			SET network_identity_key=proxies.network_identity_key
			FROM proxies
			WHERE slots.proxy_id=proxies.id
			  AND slots.network_identity_key IS NULL;

			CREATE TABLE IF NOT EXISTS proxy_control_leases (
			  lease_id TEXT PRIMARY KEY,
			  workload_scope TEXT NOT NULL,
			  slot_name TEXT NOT NULL REFERENCES proxy_running_slots(slot_name),
			  role TEXT NOT NULL,
			  worker_id TEXT NOT NULL,
			  worker_instance_id TEXT NOT NULL,
			  identity_policy_id TEXT NOT NULL,
			  identity_policy_version INTEGER NOT NULL,
			  identity_policy_hash TEXT NOT NULL,
			  status TEXT NOT NULL CHECK (status IN ('active','released','expired','fenced')),
			  claim_request_id TEXT NOT NULL,
			  claim_request_hash TEXT NOT NULL,
			  last_renew_sequence BIGINT NOT NULL DEFAULT 0,
			  lease_until TIMESTAMPTZ NOT NULL,
			  released_at TIMESTAMPTZ,
			  release_reason TEXT,
			  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			  UNIQUE (workload_scope, claim_request_id)
			);
			CREATE UNIQUE INDEX IF NOT EXISTS proxy_control_leases_one_active_slot
			  ON proxy_control_leases(workload_scope, slot_name) WHERE status='active';
			CREATE UNIQUE INDEX IF NOT EXISTS proxy_control_leases_one_active_worker_instance
			  ON proxy_control_leases(workload_scope, worker_id, worker_instance_id) WHERE status='active';
			CREATE INDEX IF NOT EXISTS proxy_control_leases_expiry
			  ON proxy_control_leases(status, lease_until);

			CREATE TABLE IF NOT EXISTS proxy_control_business_runs (
			  workload_scope TEXT NOT NULL,
			  business_run_id TEXT NOT NULL,
			  next_attempt_number INTEGER NOT NULL DEFAULT 1 CHECK (next_attempt_number > 0),
			  retry_policy_id TEXT NOT NULL,
			  retry_policy_version INTEGER NOT NULL CHECK (retry_policy_version > 0),
			  max_route_switches_per_execution INTEGER NOT NULL CHECK (max_route_switches_per_execution >= 0),
			  max_network_attempts_per_business_run INTEGER NOT NULL CHECK (max_network_attempts_per_business_run > 0),
			  budget_exhausted_at TIMESTAMPTZ,
			  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			  PRIMARY KEY (workload_scope, business_run_id)
			);

			CREATE TABLE IF NOT EXISTS proxy_control_tasks (
			  task_id TEXT PRIMARY KEY,
			  attempt_request_id TEXT NOT NULL,
			  request_hash TEXT NOT NULL,
			  workload_scope TEXT NOT NULL,
			  business_run_id TEXT NOT NULL,
			  job_execution_id TEXT NOT NULL,
			  attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
			  slot_name TEXT NOT NULL REFERENCES proxy_running_slots(slot_name),
			  worker_id TEXT NOT NULL,
			  worker_instance_id TEXT NOT NULL DEFAULT 'legacy',
			  lease_id TEXT NOT NULL,
			  route_generation BIGINT NOT NULL CHECK (route_generation >= 0),
			  task_kind TEXT NOT NULL,
			  identity_policy_id TEXT NOT NULL DEFAULT 'legacy',
			  identity_policy_version INTEGER NOT NULL DEFAULT 1,
			  identity_policy_hash TEXT NOT NULL DEFAULT 'legacy',
			  status TEXT NOT NULL CHECK (status IN ('active','completed','abandoned')),
			  outcome TEXT,
			  failed_stage TEXT,
			  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			  completed_at TIMESTAMPTZ,
			  completion_request_id TEXT,
			  completion_request_hash TEXT,
			  completion_result JSONB,
			  UNIQUE (workload_scope, attempt_request_id),
			  UNIQUE (workload_scope, business_run_id, attempt_number),
			  UNIQUE (workload_scope, completion_request_id)
			);
			CREATE INDEX IF NOT EXISTS proxy_control_tasks_job_execution
			  ON proxy_control_tasks(workload_scope, job_execution_id, started_at);
			CREATE UNIQUE INDEX IF NOT EXISTS proxy_control_tasks_one_active_slot
			  ON proxy_control_tasks(slot_name) WHERE status='active';
			CREATE UNIQUE INDEX IF NOT EXISTS proxy_control_tasks_one_active_business_run
			  ON proxy_control_tasks(workload_scope, business_run_id) WHERE status='active';

			CREATE TABLE IF NOT EXISTS proxy_control_observations (
			  observation_id TEXT NOT NULL,
			  workload_scope TEXT NOT NULL,
			  request_hash TEXT NOT NULL,
			  task_id TEXT NOT NULL REFERENCES proxy_control_tasks(task_id) ON DELETE CASCADE,
			  slot_name TEXT NOT NULL REFERENCES proxy_running_slots(slot_name),
			  worker_id TEXT NOT NULL,
			  worker_instance_id TEXT NOT NULL,
			  lease_id TEXT NOT NULL,
			  route_generation BIGINT NOT NULL,
			  business_run_id TEXT NOT NULL,
			  network_identity_key TEXT NOT NULL,
			  kind TEXT NOT NULL,
			  source TEXT NOT NULL,
			  http_status INTEGER,
			  occurred_at TIMESTAMPTZ NOT NULL,
			  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			  action TEXT NOT NULL DEFAULT 'none',
			  incident_id TEXT,
			  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			  PRIMARY KEY (workload_scope, observation_id),
			  UNIQUE (task_id, observation_id)
			);
			CREATE INDEX IF NOT EXISTS proxy_control_observations_task_created
			  ON proxy_control_observations(task_id, created_at);

			CREATE TABLE IF NOT EXISTS proxy_control_incident_observations (
			  workload_scope TEXT NOT NULL,
			  incident_id TEXT NOT NULL,
			  observation_id TEXT NOT NULL,
			  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			  PRIMARY KEY (workload_scope, incident_id, observation_id),
			  UNIQUE (workload_scope, observation_id),
			  FOREIGN KEY (workload_scope, observation_id)
			    REFERENCES proxy_control_observations(workload_scope, observation_id) ON DELETE CASCADE
			);

			CREATE TABLE IF NOT EXISTS proxy_control_command_receipts (
			  workload_scope TEXT NOT NULL,
			  command_kind TEXT NOT NULL CHECK (command_kind IN ('claim','renew','begin','observe','complete','release')),
			  request_id TEXT NOT NULL,
			  request_hash TEXT NOT NULL,
			  resource_kind TEXT NOT NULL,
			  resource_id TEXT NOT NULL,
			  result_kind TEXT NOT NULL,
			  sanitized_result JSONB NOT NULL DEFAULT '{}'::jsonb,
			  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			  retain_until TIMESTAMPTZ NOT NULL,
			  PRIMARY KEY (workload_scope, command_kind, request_id)
			);

			CREATE TABLE IF NOT EXISTS proxy_identity_profile_epochs (
			  identity_policy_id TEXT NOT NULL,
			  network_identity_key TEXT NOT NULL,
			  profile_epoch BIGINT NOT NULL CHECK (profile_epoch >= 0),
			  status TEXT NOT NULL CHECK (status IN ('active','retired')),
			  retired_at TIMESTAMPTZ,
			  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			  PRIMARY KEY (identity_policy_id, network_identity_key)
			);
		`,
		Down: `
			DROP TABLE IF EXISTS proxy_control_incident_observations;
			DROP TABLE IF EXISTS proxy_control_observations;
			DROP TABLE IF EXISTS proxy_control_command_receipts;
			DROP TABLE IF EXISTS proxy_identity_profile_epochs;
			DROP TABLE IF EXISTS proxy_control_tasks;
			DROP TABLE IF EXISTS proxy_control_business_runs;
			DROP TABLE IF EXISTS proxy_control_leases;

			ALTER TABLE proxy_running_slots
			  DROP COLUMN IF EXISTS rotation_deadline_at,
			  DROP COLUMN IF EXISTS control_state,
			  DROP COLUMN IF EXISTS pending_incident_id,
			  DROP COLUMN IF EXISTS pending_action,
			  DROP COLUMN IF EXISTS active_task_started_at,
			  DROP COLUMN IF EXISTS active_task_id,
			  DROP COLUMN IF EXISTS profile_epoch,
			  DROP COLUMN IF EXISTS network_identity_key,
			  DROP COLUMN IF EXISTS required_egress_country,
			  DROP COLUMN IF EXISTS identity_policy_hash,
			  DROP COLUMN IF EXISTS identity_policy_version,
			  DROP COLUMN IF EXISTS identity_policy_id,
			  DROP COLUMN IF EXISTS current_lease_id,
			  DROP COLUMN IF EXISTS worker_instance_id;

			DROP INDEX IF EXISTS idx_proxies_network_identity_key;
			ALTER TABLE proxies
			  DROP COLUMN IF EXISTS last_identity_verified_at,
			  DROP COLUMN IF EXISTS network_identity_key,
			  DROP COLUMN IF EXISTS identity_valid_until,
			  DROP COLUMN IF EXISTS sticky_session_key_encrypted,
			  DROP COLUMN IF EXISTS egress_identity_mode,
			  DROP COLUMN IF EXISTS country_verified_at;
		`,
	},
	{
		Version:     1005,
		Description: "Add bounded proxy lifecycle maintenance and inventory evidence",
		Up: `
			ALTER TABLE proxies
			  ADD COLUMN IF NOT EXISTS continuous_failed_since TIMESTAMPTZ,
			  ADD COLUMN IF NOT EXISTS revalidation_required BOOLEAN NOT NULL DEFAULT false,
			  ADD COLUMN IF NOT EXISTS health_generation BIGINT NOT NULL DEFAULT 0;

			ALTER TABLE proxy_health_checks
			  ADD COLUMN IF NOT EXISTS transition_preserved BOOLEAN NOT NULL DEFAULT true;

			CREATE OR REPLACE FUNCTION set_proxy_health_transition_preserved()
			RETURNS trigger
			LANGUAGE plpgsql
			AS $$
			BEGIN
			  NEW.transition_preserved := NOT (
			    NEW.applied = true
			    AND NEW.previous_status IS DISTINCT FROM NEW.resulting_status
			  );
			  RETURN NEW;
			END;
			$$;
			DROP TRIGGER IF EXISTS proxy_health_transition_preserved_on_insert
			  ON proxy_health_checks;
			CREATE TRIGGER proxy_health_transition_preserved_on_insert
			BEFORE INSERT ON proxy_health_checks
			FOR EACH ROW EXECUTE FUNCTION set_proxy_health_transition_preserved();

			UPDATE proxies
			SET continuous_failed_since = failed_since
			WHERE status = 'failed'
			  AND continuous_failed_since IS NULL;

			DROP INDEX IF EXISTS idx_proxies_health_check_due;
			CREATE INDEX IF NOT EXISTS idx_proxies_health_check_due
			  ON proxies(next_health_check_at, id)
			  WHERE next_health_check_at IS NOT NULL
			    AND (status IN ('idle', 'failed') OR (status = 'active' AND revalidation_required));
			CREATE INDEX IF NOT EXISTS idx_proxies_operational_status
			  ON proxies(status, id)
			  WHERE status <> 'archived';

			ALTER TABLE proxy_sources
			  ADD COLUMN IF NOT EXISTS successful_refresh_generation BIGINT NOT NULL DEFAULT 0,
			  ADD COLUMN IF NOT EXISTS last_complete_refresh_at TIMESTAMPTZ;

			ALTER TABLE proxy_source_memberships
			  ADD COLUMN IF NOT EXISTS last_seen_generation BIGINT NOT NULL DEFAULT 0,
			  ADD COLUMN IF NOT EXISTS consecutive_absences INTEGER NOT NULL DEFAULT 0,
			  ADD COLUMN IF NOT EXISTS missing_since TIMESTAMPTZ,
			  ADD COLUMN IF NOT EXISTS retired_at TIMESTAMPTZ,
			  ADD COLUMN IF NOT EXISTS retirement_reason TEXT;

			CREATE INDEX IF NOT EXISTS idx_proxy_source_memberships_reconcile
			  ON proxy_source_memberships(source_id, retired_at, missing_since, consecutive_absences);

			CREATE TABLE IF NOT EXISTS proxy_inventory_reconciliation_runs (
			  id BIGSERIAL PRIMARY KEY,
			  source_id INTEGER NOT NULL REFERENCES proxy_sources(id) ON DELETE CASCADE,
			  refresh_generation BIGINT NOT NULL,
			  mode TEXT NOT NULL CHECK (mode IN ('shadow', 'enforce')),
			  observed_count INTEGER NOT NULL,
			  newly_missing_count INTEGER NOT NULL,
			  eligible_count INTEGER NOT NULL,
			  retired_membership_count INTEGER NOT NULL,
			  archived_proxy_count INTEGER NOT NULL,
			  reactivated_proxy_count INTEGER NOT NULL,
			  completed_at TIMESTAMPTZ NOT NULL,
			  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			  UNIQUE (source_id, refresh_generation)
			);

			CREATE TABLE IF NOT EXISTS proxy_lifecycle_events (
			  id BIGSERIAL PRIMARY KEY,
			  proxy_id INTEGER NOT NULL REFERENCES proxies(id) ON DELETE CASCADE,
			  health_check_id BIGINT UNIQUE REFERENCES proxy_health_checks(id) ON DELETE SET NULL,
			  occurred_at TIMESTAMPTZ NOT NULL,
			  event_kind TEXT NOT NULL,
			  previous_status TEXT NOT NULL,
			  resulting_status TEXT NOT NULL,
			  reason TEXT,
			  details JSONB NOT NULL DEFAULT '{}'::jsonb,
			  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
			);
			CREATE INDEX IF NOT EXISTS idx_proxy_lifecycle_events_proxy_occurred
			  ON proxy_lifecycle_events(proxy_id, occurred_at DESC);

			CREATE TABLE IF NOT EXISTS proxy_lifecycle_repair_actions (
			  run_id TEXT NOT NULL,
			  proxy_id INTEGER NOT NULL REFERENCES proxies(id) ON DELETE CASCADE,
			  planned_action TEXT NOT NULL,
			  evidence_health_check_id BIGINT REFERENCES proxy_health_checks(id) ON DELETE SET NULL,
			  before_state JSONB NOT NULL,
			  after_state JSONB NOT NULL,
			  applied_at TIMESTAMPTZ,
			  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			  PRIMARY KEY (run_id, proxy_id)
			);

			CREATE OR REPLACE VIEW proxy_lifecycle_invariant_violations AS
			SELECT id AS proxy_id, status, 'scheduled_state_without_due'::text AS violation
			FROM proxies
			WHERE status IN ('idle', 'failed') AND next_health_check_at IS NULL
			UNION ALL
			SELECT id, status, 'incomplete_failure_episode'
			FROM proxies
			WHERE status = 'failed'
			  AND (failed_since IS NULL OR continuous_failed_since IS NULL OR failure_episode_kind IS NULL)
			UNION ALL
			SELECT id, status, 'invalid_archive_projection'
			FROM proxies
			WHERE status = 'archived'
			  AND (archived_at IS NULL OR archive_reason IS NULL OR next_health_check_at IS NOT NULL)
			UNION ALL
			SELECT p.id, p.status, 'archived_proxy_bound_to_running_slot'
			FROM proxies p
			JOIN proxy_running_slots s ON s.proxy_id = p.id
			WHERE p.status = 'archived'
			UNION ALL
			SELECT checks.proxy_id, proxies.status, 'transition_evidence_not_preserved'
			FROM proxy_health_checks checks
			JOIN proxies ON proxies.id = checks.proxy_id
			WHERE checks.applied = true
			  AND checks.previous_status IS DISTINCT FROM checks.resulting_status
			  AND checks.transition_preserved = false;

			CREATE OR REPLACE VIEW proxy_lifecycle_repair_candidates AS
			SELECT p.id AS proxy_id,
			       p.status,
			       p.updated_at AS proxy_updated_at,
			       p.failed_since,
			       p.failure_episode_kind,
			       health.id AS evidence_health_check_id,
			       health.checked_at AS evidence_checked_at,
			       health.resulting_status AS evidence_resulting_status,
			       health.verdict AS evidence_verdict,
			       CASE
			         WHEN health.resulting_status = 'archived' THEN 'restore_archive_projection'
			         WHEN health.resulting_status = 'failed' THEN 'schedule_failed_recovery'
			         ELSE 'reset_pending_validation'
			       END AS recommended_action
			FROM proxies p
			LEFT JOIN LATERAL (
			  SELECT checks.id, checks.checked_at, checks.resulting_status, checks.verdict
			  FROM proxy_health_checks checks
			  WHERE checks.proxy_id = p.id AND checks.applied = true
			  ORDER BY checks.checked_at DESC, checks.id DESC
			  LIMIT 1
			) health ON true
			WHERE p.status IN ('idle', 'failed')
			  AND p.next_health_check_at IS NULL;
		`,
		Down: `
			DROP VIEW IF EXISTS proxy_lifecycle_repair_candidates;
			DROP VIEW IF EXISTS proxy_lifecycle_invariant_violations;
			DROP TABLE IF EXISTS proxy_lifecycle_repair_actions;
			DROP TABLE IF EXISTS proxy_lifecycle_events;
			DROP TABLE IF EXISTS proxy_inventory_reconciliation_runs;
			DROP INDEX IF EXISTS idx_proxy_source_memberships_reconcile;
			ALTER TABLE proxy_source_memberships
			  DROP COLUMN IF EXISTS retirement_reason,
			  DROP COLUMN IF EXISTS retired_at,
			  DROP COLUMN IF EXISTS missing_since,
			  DROP COLUMN IF EXISTS consecutive_absences,
			  DROP COLUMN IF EXISTS last_seen_generation;
			ALTER TABLE proxy_sources
			  DROP COLUMN IF EXISTS last_complete_refresh_at,
			  DROP COLUMN IF EXISTS successful_refresh_generation;
			DROP INDEX IF EXISTS idx_proxies_health_check_due;
			DROP INDEX IF EXISTS idx_proxies_operational_status;
			CREATE INDEX IF NOT EXISTS idx_proxies_health_check_due
			  ON proxies(next_health_check_at)
			  WHERE status IN ('idle', 'failed') AND next_health_check_at IS NOT NULL;
			ALTER TABLE proxies
			  DROP COLUMN IF EXISTS health_generation,
			  DROP COLUMN IF EXISTS revalidation_required,
			  DROP COLUMN IF EXISTS continuous_failed_since;
			DROP TRIGGER IF EXISTS proxy_health_transition_preserved_on_insert
			  ON proxy_health_checks;
			ALTER TABLE proxy_health_checks
			  DROP COLUMN IF EXISTS transition_preserved;
			DROP FUNCTION IF EXISTS set_proxy_health_transition_preserved();
		`,
	},
	{
		Version:     1006,
		Description: "Backfill proxy lifecycle transition evidence",
		Up: `
			INSERT INTO proxy_lifecycle_events (
			  proxy_id, health_check_id, occurred_at, event_kind,
			  previous_status, resulting_status, reason, details
			)
			SELECT proxy_id, id, checked_at, 'health_verdict',
			       previous_status, resulting_status, verdict,
			       jsonb_build_object(
			         'conclusive', conclusive,
			         'control_path_healthy', control_path_healthy,
			         'backfilled', true
			       )
			FROM proxy_health_checks
			WHERE applied = true
			  AND previous_status IS DISTINCT FROM resulting_status
			ON CONFLICT (health_check_id) DO NOTHING;

			UPDATE proxy_health_checks checks
			SET transition_preserved = EXISTS (
			  SELECT 1
			  FROM proxy_lifecycle_events events
			  WHERE events.health_check_id = checks.id
			)
			WHERE checks.applied = true
			  AND checks.previous_status IS DISTINCT FROM checks.resulting_status;
		`,
		Down: `
			DELETE FROM proxy_lifecycle_events
			WHERE event_kind = 'health_verdict'
			  AND details ->> 'backfilled' = 'true';
			UPDATE proxy_health_checks checks
			SET transition_preserved = EXISTS (
			  SELECT 1
			  FROM proxy_lifecycle_events events
			  WHERE events.health_check_id = checks.id
			)
			WHERE checks.applied = true
			  AND checks.previous_status IS DISTINCT FROM checks.resulting_status;
		`,
	},
	{
		Version:     1007,
		Description: "Add online proxy health evidence retention index",
		Up: `
			CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_proxy_health_checks_retention
			  ON proxy_health_checks(checked_at, id)
			  WHERE transition_preserved = true
		`,
		Down: `
			DROP INDEX CONCURRENTLY IF EXISTS idx_proxy_health_checks_retention
		`,
		NoTransaction:       true,
		ConcurrentIndexName: "idx_proxy_health_checks_retention",
	},
	{
		Version:     1008,
		Description: "Add online sparse proxy transition audit index",
		Up: `
			CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_proxy_health_checks_unpreserved_transitions
			  ON proxy_health_checks(id, proxy_id)
			  WHERE applied = true
			    AND previous_status IS DISTINCT FROM resulting_status
			    AND transition_preserved = false
		`,
		Down: `
			DROP INDEX CONCURRENTLY IF EXISTS idx_proxy_health_checks_unpreserved_transitions
		`,
		NoTransaction:       true,
		ConcurrentIndexName: "idx_proxy_health_checks_unpreserved_transitions",
	},
	{
		Version:     1009,
		Description: "Fence pending Route activation across Rota instances",
		Up: `
			ALTER TABLE proxy_running_slots
			  ADD COLUMN IF NOT EXISTS route_activation_old_username TEXT,
			  ADD COLUMN IF NOT EXISTS route_activation_claim_id TEXT,
			  ADD COLUMN IF NOT EXISTS route_activation_claim_until TIMESTAMPTZ;
			ALTER TABLE proxy_running_slots
			  DROP CONSTRAINT IF EXISTS proxy_running_slots_route_activation_claim_check;
			ALTER TABLE proxy_running_slots
			  ADD CONSTRAINT proxy_running_slots_route_activation_claim_check CHECK (
			    (route_activation_claim_id IS NULL AND route_activation_claim_until IS NULL)
			    OR
			    (route_activation_claim_id IS NOT NULL AND route_activation_claim_until IS NOT NULL)
			  );
		`,
		Down: `
			ALTER TABLE proxy_running_slots
			  DROP CONSTRAINT IF EXISTS proxy_running_slots_route_activation_claim_check;
			ALTER TABLE proxy_running_slots
			  DROP COLUMN IF EXISTS route_activation_claim_until,
			  DROP COLUMN IF EXISTS route_activation_claim_id,
			  DROP COLUMN IF EXISTS route_activation_old_username;
		`,
	},
	{
		Version:     1010,
		Description: "Persist the previous pending Route activation Claim",
		Up: `
			ALTER TABLE proxy_running_slots
			  ADD COLUMN IF NOT EXISTS route_activation_previous_claim_id TEXT;
			ALTER TABLE proxy_running_slots
			  DROP CONSTRAINT IF EXISTS proxy_running_slots_route_activation_claim_check;
			ALTER TABLE proxy_running_slots
			  ADD CONSTRAINT proxy_running_slots_route_activation_claim_check CHECK (
			    (
			      route_activation_claim_id IS NULL
			      AND route_activation_claim_until IS NULL
			      AND route_activation_previous_claim_id IS NULL
			    )
			    OR
			    (
			      route_activation_claim_id IS NOT NULL
			      AND route_activation_claim_until IS NOT NULL
			    )
			  );
		`,
		Down: `
			ALTER TABLE proxy_running_slots
			  DROP CONSTRAINT IF EXISTS proxy_running_slots_route_activation_claim_check;
			ALTER TABLE proxy_running_slots
			  DROP COLUMN IF EXISTS route_activation_previous_claim_id;
			ALTER TABLE proxy_running_slots
			  ADD CONSTRAINT proxy_running_slots_route_activation_claim_check CHECK (
			    (route_activation_claim_id IS NULL AND route_activation_claim_until IS NULL)
			    OR
			    (route_activation_claim_id IS NOT NULL AND route_activation_claim_until IS NOT NULL)
			  );
		`,
	},
	{
		Version:     1011,
		Description: "Generate network identity keys for new proxies",
		Up: `
			ALTER TABLE proxies
			  ALTER COLUMN network_identity_key
			  SET DEFAULT ('net_' || replace(gen_random_uuid()::text, '-', ''));
		`,
		Down: `
			ALTER TABLE proxies
			  ALTER COLUMN network_identity_key DROP DEFAULT;
		`,
	},
	{
		Version:     1012,
		Description: "Make active proxies continuously health verified",
		Up: `
			INSERT INTO settings (key,value,updated_at)
			VALUES (
			  'proxy_lifecycle',
			  '{"auto_archive_enabled":true,"active_recheck_minutes":120,"hard_unreachable_after_hours":6,"soft_unreachable_after_hours":24,"youtube_unusable_after_hours":72}'::jsonb,
			  NOW()
			)
			ON CONFLICT (key) DO UPDATE
			SET value=jsonb_set(settings.value,'{active_recheck_minutes}','120'::jsonb,true),
			    updated_at=NOW();

			UPDATE proxies
			SET next_health_check_at=NOW(),updated_at=NOW()
			WHERE status IN ('idle','failed') AND next_health_check_at IS NULL;

			WITH changed AS (
			  UPDATE proxies p
			  SET status='idle',failed_since=NULL,continuous_failed_since=NULL,
			      failure_episode_kind=NULL,next_health_check_at=NOW(),
			      revalidation_required=false,health_generation=health_generation+1,
			      health_check_not_before=NOW(),base_health_status=NULL,
			      youtube_health_status=NULL,last_error=NULL,updated_at=NOW()
			  WHERE p.status='active'
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
			    AND (
			      p.revalidation_required
			      OR p.last_health_success_at IS NULL
			      OR p.base_health_status IS DISTINCT FROM 'passed'
			      OR p.youtube_health_status IS DISTINCT FROM 'passed'
			      OR p.last_health_success_at <= NOW()-INTERVAL '120 minutes'
			      OR p.cooldown_until > NOW()
			    )
			  RETURNING p.id
			)
			INSERT INTO proxy_lifecycle_events (
			  proxy_id,occurred_at,event_kind,previous_status,resulting_status,reason
			)
			SELECT id,NOW(),'active_evidence_migration','active','idle',
			       'active_proxy_requires_structured_revalidation'
			FROM changed;

			UPDATE proxies
			SET next_health_check_at=COALESCE(
			      last_health_success_at+INTERVAL '120 minutes',
			      NOW()+INTERVAL '120 minutes'
			    ),
			    revalidation_required=false,updated_at=NOW()
			WHERE status='active';

			DROP INDEX IF EXISTS idx_proxies_health_check_due;
			CREATE INDEX idx_proxies_health_check_due
			  ON proxies(next_health_check_at,id)
			  WHERE next_health_check_at IS NOT NULL
			    AND status IN ('idle','failed','active');

			ALTER TABLE proxies DROP CONSTRAINT IF EXISTS proxies_scheduled_status_has_due;
			ALTER TABLE proxies ADD CONSTRAINT proxies_scheduled_status_has_due
			  CHECK (status NOT IN ('idle','failed','active') OR next_health_check_at IS NOT NULL)
			  NOT VALID;
			ALTER TABLE proxies VALIDATE CONSTRAINT proxies_scheduled_status_has_due;

			CREATE OR REPLACE VIEW proxy_lifecycle_invariant_violations AS
			SELECT id AS proxy_id,status,'scheduled_state_without_due'::text AS violation
			FROM proxies
			WHERE status IN ('idle','failed','active') AND next_health_check_at IS NULL
			UNION ALL
			SELECT id,status,'incomplete_failure_episode'
			FROM proxies
			WHERE status='failed'
			  AND (failed_since IS NULL OR continuous_failed_since IS NULL OR failure_episode_kind IS NULL)
			UNION ALL
			SELECT id,status,'invalid_archive_projection'
			FROM proxies
			WHERE status='archived'
			  AND (archived_at IS NULL OR archive_reason IS NULL OR next_health_check_at IS NOT NULL)
			UNION ALL
			SELECT p.id,p.status,'archived_proxy_bound_to_running_slot'
			FROM proxies p
			JOIN proxy_running_slots slot ON slot.proxy_id=p.id
			WHERE p.status='archived'
			UNION ALL
			SELECT checks.proxy_id,proxies.status,'transition_evidence_not_preserved'
			FROM proxy_health_checks checks
			JOIN proxies ON proxies.id=checks.proxy_id
			WHERE checks.applied=true
			  AND checks.previous_status IS DISTINCT FROM checks.resulting_status
			  AND checks.transition_preserved=false;

		`,
		Down: `
			CREATE OR REPLACE VIEW proxy_lifecycle_invariant_violations AS
			SELECT id AS proxy_id,status,'scheduled_state_without_due'::text AS violation
			FROM proxies
			WHERE status IN ('idle','failed') AND next_health_check_at IS NULL
			UNION ALL
			SELECT id,status,'incomplete_failure_episode'
			FROM proxies
			WHERE status='failed'
			  AND (failed_since IS NULL OR continuous_failed_since IS NULL OR failure_episode_kind IS NULL)
			UNION ALL
			SELECT id,status,'invalid_archive_projection'
			FROM proxies
			WHERE status='archived'
			  AND (archived_at IS NULL OR archive_reason IS NULL OR next_health_check_at IS NOT NULL)
			UNION ALL
			SELECT p.id,p.status,'archived_proxy_bound_to_running_slot'
			FROM proxies p
			JOIN proxy_running_slots slot ON slot.proxy_id=p.id
			WHERE p.status='archived'
			UNION ALL
			SELECT checks.proxy_id,proxies.status,'transition_evidence_not_preserved'
			FROM proxy_health_checks checks
			JOIN proxies ON proxies.id=checks.proxy_id
			WHERE checks.applied=true
			  AND checks.previous_status IS DISTINCT FROM checks.resulting_status
			  AND checks.transition_preserved=false;

			ALTER TABLE proxies DROP CONSTRAINT IF EXISTS proxies_scheduled_status_has_due;
			ALTER TABLE proxies ADD CONSTRAINT proxies_scheduled_status_has_due
			  CHECK (status NOT IN ('idle','failed') OR next_health_check_at IS NOT NULL)
			  NOT VALID;
			DROP INDEX IF EXISTS idx_proxies_health_check_due;
			CREATE INDEX idx_proxies_health_check_due
			  ON proxies(next_health_check_at,id)
			  WHERE next_health_check_at IS NOT NULL
			    AND (status IN ('idle','failed') OR (status='active' AND revalidation_required));
			UPDATE settings
			SET value=value-'active_recheck_minutes',updated_at=NOW()
			WHERE key='proxy_lifecycle';
		`,
	},
	{
		Version:     1013,
		Description: "Use one bounded random YouTube search health probe",
		Up: `
			INSERT INTO settings (key,value,updated_at)
			VALUES (
			  'healthcheck',
			  '{"timeout":15,"workers":20,"base_url":"https://www.google.com/generate_204","base_status":204,"url":"https://www.youtube.com/results?search_query=","status":200,"headers":["User-Agent: Rota-HealthCheck/1.0"],"strict_tls":false}'::jsonb,
			  NOW()
			)
			ON CONFLICT (key) DO UPDATE
			SET value=jsonb_set(
			      jsonb_set(
			        jsonb_set(settings.value,'{timeout}','15'::jsonb,true),
			        '{url}',
			        '"https://www.youtube.com/results?search_query="'::jsonb,
			        true
			      ),
			      '{status}',
			      '200'::jsonb,
			      true
			    ),
			    updated_at=NOW();

			UPDATE proxy_pools
			SET health_check_url='https://www.youtube.com/results?search_query=',updated_at=NOW()
			WHERE health_check_url IN (
			  'https://api.ipify.org',
			  'https://www.youtube.com/watch?v=_xXsXvsYAhA'
			);
			ALTER TABLE proxy_pools
			ALTER COLUMN health_check_url SET DEFAULT 'https://www.youtube.com/results?search_query=';
		`,
		Down: `
			UPDATE settings
			SET value=jsonb_set(
			      jsonb_set(value,'{timeout}','60'::jsonb,true),
			      '{url}',
			      '"https://www.youtube.com/watch?v=_xXsXvsYAhA"'::jsonb,
			      true
			    ),
			    updated_at=NOW()
			WHERE key='healthcheck';

			UPDATE proxy_pools
			SET health_check_url='https://www.youtube.com/watch?v=_xXsXvsYAhA',updated_at=NOW()
			WHERE health_check_url='https://www.youtube.com/results?search_query=';
			ALTER TABLE proxy_pools
			ALTER COLUMN health_check_url SET DEFAULT 'https://www.youtube.com/watch?v=_xXsXvsYAhA';
		`,
	},
	{
		Version:     1014,
		Description: "Enable Hysteria2 for default protocol rotation",
		Up: `
			UPDATE settings
			SET value=jsonb_set(
			      value,
			      '{allowed_protocols}',
			      (value->'allowed_protocols') || '"hysteria2"'::jsonb,
			      true
			    ),
			    updated_at=NOW()
			WHERE key='rotation'
			  AND jsonb_typeof(value->'allowed_protocols')='array'
			  AND jsonb_array_length(value->'allowed_protocols')=9
			  AND value->'allowed_protocols' @> '["http","https","socks4","socks4a","socks5","vless","vmess","trojan","shadowsocks"]'::jsonb;

			UPDATE settings
			SET value=jsonb_set(
			      value,
			      '{allowed_protocols}',
			      '["http","https","socks4","socks4a","socks5","vless","vmess","trojan","shadowsocks","hysteria2"]'::jsonb,
			      true
			    ),
			    updated_at=NOW()
			WHERE key='rotation'
			  AND NOT (value ? 'allowed_protocols');
		`,
		Down: `
			UPDATE settings
			SET value=jsonb_set(
			      value,
			      '{allowed_protocols}',
			      COALESCE((
			        SELECT jsonb_agg(protocol)
			        FROM jsonb_array_elements(value->'allowed_protocols') AS protocol
			        WHERE protocol <> '"hysteria2"'::jsonb
			      ), '[]'::jsonb),
			      true
			    ),
			    updated_at=NOW()
			WHERE key='rotation'
			  AND jsonb_typeof(value->'allowed_protocols')='array'
			  AND value->'allowed_protocols' ? 'hysteria2';
		`,
	},
}

// Migrate runs all pending migrations
func (db *DB) Migrate(ctx context.Context) error {
	db.logger.Info("starting database migrations")

	// Sort migrations by version
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	// Track every applied version. Local migrations use a high version range so
	// later upstream lower-numbered migrations can still be ported safely.
	applied, err := db.getAppliedVersions(ctx)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}

	db.logger.Info("current database version", "version", maxAppliedVersion(applied))

	// Apply pending migrations
	appliedCount := 0
	for _, migration := range migrations {
		if applied[migration.Version] {
			continue
		}

		db.logger.Info("applying migration",
			"version", migration.Version,
			"description", migration.Description,
		)

		if err := db.applyMigration(ctx, migration); err != nil {
			return fmt.Errorf("failed to apply migration %d: %w", migration.Version, err)
		}

		appliedCount++
	}

	if appliedCount == 0 {
		db.logger.Info("no migrations to apply")
	} else {
		db.logger.Info("migrations completed", "applied", appliedCount)
	}

	return nil
}

// getCurrentVersion returns the current migration version
func (db *DB) getCurrentVersion(ctx context.Context) (int, error) {
	// Check if migrations table exists
	var exists bool
	query := `SELECT to_regclass('schema_migrations') IS NOT NULL;`
	if err := db.Pool.QueryRow(ctx, query).Scan(&exists); err != nil {
		return 0, err
	}

	if !exists {
		return 0, nil
	}

	// Rollback follows application order, which may differ from numeric order
	// when a lower upstream migration is ported after a high local version.
	var version int
	query = `
		SELECT COALESCE((
			SELECT version
			FROM schema_migrations
			ORDER BY applied_at DESC, version DESC
			LIMIT 1
		), 0);
	`
	if err := db.Pool.QueryRow(ctx, query).Scan(&version); err != nil {
		return 0, err
	}

	return version, nil
}

func (db *DB) getAppliedVersions(ctx context.Context) (map[int]bool, error) {
	applied := make(map[int]bool)
	var exists bool
	query := `SELECT to_regclass('schema_migrations') IS NOT NULL;`
	if err := db.Pool.QueryRow(ctx, query).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return applied, nil
	}

	rows, err := db.Pool.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		applied[version] = true
	}
	return applied, rows.Err()
}

func maxAppliedVersion(applied map[int]bool) int {
	max := 0
	for version := range applied {
		if version > max {
			max = version
		}
	}
	return max
}

// applyMigration applies a single migration
func (db *DB) applyMigration(ctx context.Context, migration Migration) error {
	if migration.NoTransaction {
		if migration.ConcurrentIndexName != "" {
			if err := db.recoverInvalidConcurrentIndex(ctx, migration.ConcurrentIndexName); err != nil {
				return err
			}
		}
		if _, err := db.Pool.Exec(ctx, migration.Up); err != nil {
			return fmt.Errorf("failed to execute non-transactional migration: %w", err)
		}
		if migration.ConcurrentIndexName != "" {
			valid, err := db.concurrentIndexValid(ctx, migration.ConcurrentIndexName)
			if err != nil {
				return err
			}
			if !valid {
				return fmt.Errorf("concurrent index %s is not valid after migration", migration.ConcurrentIndexName)
			}
		}
		if _, err := db.Pool.Exec(ctx, `
			INSERT INTO schema_migrations (version, description, applied_at)
			VALUES ($1, $2, $3)
		`, migration.Version, migration.Description, time.Now()); err != nil {
			return fmt.Errorf("failed to record non-transactional migration: %w", err)
		}
		return nil
	}
	return pgx.BeginFunc(ctx, db.Pool, func(tx pgx.Tx) error {
		// Execute migration
		if _, err := tx.Exec(ctx, migration.Up); err != nil {
			return fmt.Errorf("failed to execute migration: %w", err)
		}

		// Record migration
		query := `
			INSERT INTO schema_migrations (version, description, applied_at)
			VALUES ($1, $2, $3)
		`
		if _, err := tx.Exec(ctx, query, migration.Version, migration.Description, time.Now()); err != nil {
			return fmt.Errorf("failed to record migration: %w", err)
		}

		return nil
	})
}

func (db *DB) recoverInvalidConcurrentIndex(ctx context.Context, name string) error {
	exists, valid, err := db.concurrentIndexState(ctx, name)
	if err != nil {
		return err
	}
	if !exists || valid {
		return nil
	}
	identifier := pgx.Identifier{name}.Sanitize()
	if _, err := db.Pool.Exec(ctx, "DROP INDEX CONCURRENTLY IF EXISTS "+identifier); err != nil {
		return fmt.Errorf("drop invalid concurrent index %s: %w", name, err)
	}
	return nil
}

func (db *DB) concurrentIndexValid(ctx context.Context, name string) (bool, error) {
	exists, valid, err := db.concurrentIndexState(ctx, name)
	return exists && valid, err
}

func (db *DB) concurrentIndexState(ctx context.Context, name string) (bool, bool, error) {
	var exists, valid bool
	if err := db.Pool.QueryRow(ctx, `
		SELECT to_regclass($1) IS NOT NULL,
		       COALESCE((
		         SELECT indisvalid
		         FROM pg_index
		         WHERE indexrelid = to_regclass($1)
		       ), false)
	`, name).Scan(&exists, &valid); err != nil {
		return false, false, fmt.Errorf("inspect concurrent index %s: %w", name, err)
	}
	return exists, valid, nil
}

// Rollback rolls back the last migration
func (db *DB) Rollback(ctx context.Context) error {
	db.logger.Info("rolling back last migration")

	// Get current version
	currentVersion, err := db.getCurrentVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current version: %w", err)
	}

	if currentVersion == 0 {
		db.logger.Info("no migrations to rollback")
		return nil
	}

	// Find migration to rollback
	var migrationToRollback *Migration
	for i := range migrations {
		if migrations[i].Version == currentVersion {
			migrationToRollback = &migrations[i]
			break
		}
	}

	if migrationToRollback == nil {
		return fmt.Errorf("migration version %d not found", currentVersion)
	}

	db.logger.Info("rolling back migration",
		"version", migrationToRollback.Version,
		"description", migrationToRollback.Description,
	)
	if migrationToRollback.NoTransaction {
		if _, err := db.Pool.Exec(ctx, migrationToRollback.Down); err != nil {
			return fmt.Errorf("failed to execute non-transactional rollback: %w", err)
		}
		if _, err := db.Pool.Exec(ctx, `
			DELETE FROM schema_migrations WHERE version = $1
		`, migrationToRollback.Version); err != nil {
			return fmt.Errorf("failed to remove non-transactional migration record: %w", err)
		}
		return nil
	}

	return pgx.BeginFunc(ctx, db.Pool, func(tx pgx.Tx) error {
		// Execute rollback
		if _, err := tx.Exec(ctx, migrationToRollback.Down); err != nil {
			return fmt.Errorf("failed to execute rollback: %w", err)
		}

		// Remove migration record
		query := `DELETE FROM schema_migrations WHERE version = $1`
		if _, err := tx.Exec(ctx, query, migrationToRollback.Version); err != nil {
			return fmt.Errorf("failed to remove migration record: %w", err)
		}

		return nil
	})
}

// GetMigrationStatus returns the status of all migrations
func (db *DB) GetMigrationStatus(ctx context.Context) ([]map[string]interface{}, error) {
	applied, err := db.getAppliedVersions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get applied migrations: %w", err)
	}

	var status []map[string]interface{}
	for _, migration := range migrations {
		status = append(status, map[string]interface{}{
			"version":     migration.Version,
			"description": migration.Description,
			"applied":     applied[migration.Version],
		})
	}

	return status, nil
}
