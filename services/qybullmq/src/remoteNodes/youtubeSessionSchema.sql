-- Opt-in only, after routeSchema.sql. Contains encrypted browser state.
CREATE TABLE IF NOT EXISTS remote_ingestion.youtube_sessions (
  binding_id UUID PRIMARY KEY REFERENCES remote_ingestion.network_bindings(binding_id),
  session_hash TEXT NOT NULL,
  session_cipher BYTEA NOT NULL CHECK (octet_length(session_cipher)<=524288),
  checkpoint_hash TEXT,
  checkpoint_cipher BYTEA CHECK (octet_length(checkpoint_cipher)<=524288),
  checkpoint_at TIMESTAMPTZ
);
ALTER TABLE remote_ingestion.youtube_sessions ADD COLUMN IF NOT EXISTS profile_applied_at TIMESTAMPTZ;
ALTER TABLE remote_ingestion.youtube_sessions ADD COLUMN IF NOT EXISTS checkpoint_request JSONB;
