function requiredText(value, name) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new Error(`${name} is required`);
  return normalized;
}

export function assertCrawlerDashboardIdentity(row, {
  expectedDatabase,
  forbiddenDatabase = "bullmq_crawler_migration",
} = {}) {
  const expected = requiredText(expectedDatabase, "EXPECTED_CRAWLER_DATABASE");
  const forbidden = requiredText(forbiddenDatabase, "FORBIDDEN_CRAWLER_DATABASE");
  const database = String(row?.database_name ?? "").trim();
  if (database === forbidden) throw new Error(`refusing forbidden Crawler database ${database}`);
  if (database !== expected) {
    throw new Error(`refusing unexpected Crawler database ${database || "unknown"}; expected ${expected}`);
  }
  if (row?.transaction_read_only !== "off") {
    throw new Error("Dashboard Crawler database must be writable");
  }
  if (row?.identity_kind !== "crawler" || row?.identity_database !== database) {
    throw new Error(`refusing uninitialized or mismatched Crawler database ${database}`);
  }
  return { database, user: requiredText(row?.database_user, "Crawler database user") };
}

export function assertMigrationSourceIdentity(row, {
  expectedDatabase,
  expectedDatabaseOid,
  expectedUser,
  targetDatabase,
} = {}) {
  const expected = requiredText(expectedDatabase, "EXPECTED_MIGRATION_DATABASE");
  const expectedOid = requiredText(expectedDatabaseOid, "EXPECTED_MIGRATION_DATABASE_OID");
  const expectedRole = requiredText(expectedUser, "EXPECTED_MIGRATION_DATABASE_USER");
  const target = requiredText(targetDatabase, "EXPECTED_CRAWLER_DATABASE");
  if (expected === target) throw new Error("Migration Source and Target databases must differ");
  const database = String(row?.database_name ?? "").trim();
  const databaseOid = String(row?.database_oid ?? "").trim();
  const user = String(row?.database_user ?? "").trim();
  if (database !== expected) throw new Error(`unexpected Migration Source database ${database}`);
  if (databaseOid !== expectedOid) throw new Error(`unexpected Migration Source database OID ${databaseOid}`);
  if (user !== expectedRole) throw new Error(`unexpected Migration Source database user ${user}`);
  if (row?.transaction_read_only !== "on") throw new Error("Migration Source transaction is not read-only");
  if (row?.default_transaction_read_only !== "on") {
    throw new Error("Migration Source role must default to read-only");
  }
  if (row?.candidates_ready !== true || row?.channels_ready !== true) {
    throw new Error("Migration Source schema is not ready");
  }
  if (row?.candidate_write !== false || row?.channel_write !== false) {
    throw new Error("Migration Source role must not have write privileges on source tables");
  }
  return { database, databaseOid, user };
}

const FINAL_STATUSES = new Set(["ready_auto", "ready_partial"]);

export function mergeMigrationCandidate(source, target) {
  const targetState = target || {};
  const targetCandidateStatus = String(targetState.target_candidate_status || "").trim();
  const finalStatus = String(targetState.final_status || "pending");
  const promoted = targetState.target_candidate_id != null
    && String(targetState.target_candidate_id)
      === String(targetState.registry_promotion_candidate_id ?? "");
  const migrationIncomplete = targetCandidateStatus === "accepted"
    && targetState.target_channel_status === "active"
    && promoted
    && !FINAL_STATUSES.has(finalStatus);
  const migrationDone = targetCandidateStatus === "accepted"
    && ["active", "dormant"].includes(String(targetState.target_channel_status || ""))
    && promoted
    && FINAL_STATUSES.has(finalStatus);
  const candidateStatus = migrationIncomplete
    ? "finishing"
    : targetCandidateStatus || "discovered";
  return {
    ...source,
    candidate_id: source.candidate_id,
    source_candidate_id: source.candidate_id,
    target_candidate_id: targetState.target_candidate_id ?? null,
    migration_intent_id: targetState.migration_intent_id ?? null,
    channel_url: source.channel_url,
    handle: source.handle || "",
    title: source.title || "",
    avatar_url: source.snapshot_json?.channel_header?.avatar_url || source.avatar_url || null,
    subscriber_count: source.search_subscriber_count ?? null,
    candidate_status: candidateStatus,
    status: migrationIncomplete
      ? "finishing"
      : targetState.target_channel_status || candidateStatus,
    reject_reason: targetState.target_reject_reason ?? null,
    agent_status: targetState.agent_status || "pending",
    latest_run_id: targetState.latest_run_id ?? null,
    final_status: finalStatus,
    quality_json: targetState.quality_json || {},
    run_status: targetState.run_status ?? null,
    run_detail_status: targetState.run_detail_status ?? null,
    is_candidate_only: !targetState.target_channel_status,
    migration_incomplete: migrationIncomplete,
    migration_started: Boolean(targetState.migration_intent_id),
    migration_done: migrationDone,
    content_count: targetState.content_count ?? 0,
    video_count: targetState.video_count ?? 0,
    short_count: targetState.short_count ?? 0,
    live_count: targetState.live_count ?? 0,
    updated_at: targetState.updated_at || source.updated_at,
  };
}

export function mergeMigrationCandidates(sourceRows, targetRows) {
  const bySourceCandidate = new Map();
  const byChannel = new Map();
  for (const target of targetRows) {
    bySourceCandidate.set(String(target.source_candidate_id), target);
    byChannel.set(String(target.channel_id), target);
  }
  return sourceRows.map((source) => mergeMigrationCandidate(
    source,
    bySourceCandidate.get(String(source.candidate_id))
      || byChannel.get(String(source.channel_id))
      || null,
  ));
}

export function filterAndPageMigrationCandidates(rows, {
  channelStatus = "all",
  agentStatus = "",
  finalStatus = "",
  offset = 0,
  limit = 500,
} = {}) {
  const filtered = rows.filter((row) => (
    (channelStatus === "all" || row.candidate_status === channelStatus)
    && (!agentStatus || row.agent_status === agentStatus)
    && (!finalStatus || row.final_status === finalStatus)
  ));
  return {
    rows: filtered.slice(offset, offset + limit),
    total: filtered.length,
  };
}

export function migrationReadModelStats(sourceTotal, rows) {
  const stats = {
    total: Number(sourceTotal || 0),
    discovered: 0,
    queued: 0,
    validating: 0,
    finishing: 0,
    failed: 0,
    migration_done: 0,
    final_done: 0,
  };
  for (const row of rows) {
    if (Object.hasOwn(stats, row.candidate_status)) stats[row.candidate_status] += 1;
    if (row.migration_done) stats.migration_done += 1;
    if (FINAL_STATUSES.has(String(row.final_status || ""))) stats.final_done += 1;
  }
  return stats;
}
