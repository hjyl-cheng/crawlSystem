SET TIME ZONE 'UTC';

CREATE SCHEMA IF NOT EXISTS crawler;

CREATE TABLE IF NOT EXISTS crawler.channels (
  channel_id TEXT PRIMARY KEY,
  channel_url TEXT NOT NULL,
  handle TEXT,
  title TEXT,
  avatar_url TEXT,
  subscriber_count BIGINT,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS avatar_url TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS subscriber_count BIGINT;

CREATE TABLE IF NOT EXISTS crawler.channel_runs (
  run_id TEXT PRIMARY KEY,
  channel_id TEXT NOT NULL REFERENCES crawler.channels(channel_id) ON DELETE CASCADE,
  plan_id UUID,
  status TEXT NOT NULL DEFAULT 'queued',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS crawler.crawl_observations (
  observation_id UUID PRIMARY KEY,
  observed_at TIMESTAMPTZ NOT NULL,
  channel_id TEXT NOT NULL REFERENCES crawler.channels(channel_id) ON DELETE CASCADE,
  observation_kind TEXT NOT NULL
    CHECK (observation_kind IN ('profile', 'about', 'video', 'agent')),
  kind_sequence BIGINT NOT NULL CHECK (kind_sequence > 0),
  plan_id UUID,
  plan_day DATE,
  trigger_reason TEXT NOT NULL,
  scheduled_at TIMESTAMPTZ,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  outcome TEXT NOT NULL CHECK (outcome IN ('complete', 'partial', 'failed')),
  outcome_reason_code TEXT NOT NULL,
  facts_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (channel_id, observation_kind, kind_sequence)
);
