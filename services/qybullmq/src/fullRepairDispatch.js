import { createHash } from "node:crypto";
import { parseArgs } from "node:util";
import { verifyCrawlerWriterDatabase } from "./databaseIdentity.js";
import {
  CHANNEL_QUEUE_PRESSURE_STATES,
  channelDispatchCapacity,
  channelQueuePressure,
} from "./migrationDispatchPolicy.js";
import { safeJobId } from "./queues.js";

export const FULL_REPAIR_MANIFEST_VERSION = "publication-full-repair-manifest-v2";
export const FULL_REPAIR_SCAN_POLICY_VERSION = "publication-video-window-repair-v1";
export const FULL_REPAIR_CONTENT_LIMIT = 100;
export const FULL_REPAIR_CONTENT_MAX_AGE_DAYS = 90;

function compareText(left, right) {
  return Buffer.compare(Buffer.from(left, "utf8"), Buffer.from(right, "utf8"));
}

function requiredText(value, name) {
  const parsed = String(value ?? "").trim();
  if (!parsed) throw new TypeError(`${name} is required`);
  return parsed;
}

function repairBatchId(value) {
  const parsed = requiredText(value, "batchId");
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(parsed)) {
    throw new TypeError("batchId must use 1-128 letters, numbers, dots, underscores, or hyphens");
  }
  return parsed;
}

function positiveInteger(value, name, { min = 1, max = 1_000_000 } = {}) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    throw new TypeError(`${name} must be an integer between ${min} and ${max}`);
  }
  return parsed;
}

export function parseFullRepairArgs(argv = [], environment = process.env) {
  const { values } = parseArgs({
    args: argv,
    options: {
      "channels-file": { type: "string" },
      "batch-id": { type: "string" },
      execute: { type: "boolean", default: false },
      watch: { type: "boolean", default: false },
      "retry-failed": { type: "boolean", default: false },
      confirm: { type: "string" },
      "high-water": { type: "string" },
      refill: { type: "string" },
      "poll-ms": { type: "string" },
    },
    strict: true,
  });
  const options = {
    channelsFile: requiredText(values["channels-file"], "--channels-file"),
    batchId: repairBatchId(values["batch-id"]),
    execute: values.execute,
    watch: values.watch,
    retryFailed: values["retry-failed"],
    confirm: String(values.confirm ?? "").trim() || null,
    highWater: positiveInteger(
      values["high-water"] ?? environment.FULL_REPAIR_QUEUE_HIGH_WATER ?? 5,
      "--high-water",
      { max: 10_000 },
    ),
    refill: positiveInteger(
      values.refill ?? environment.FULL_REPAIR_QUEUE_REFILL ?? 2,
      "--refill",
      { max: 10_000 },
    ),
    pollMs: positiveInteger(
      values["poll-ms"] ?? environment.FULL_REPAIR_POLL_MS ?? 15000,
      "--poll-ms",
      { min: 1000, max: 300_000 },
    ),
  };
  if (options.refill > options.highWater) throw new TypeError("--refill cannot exceed --high-water");
  if (options.watch && !options.execute) throw new TypeError("--watch requires --execute");
  if (options.retryFailed && !options.execute) throw new TypeError("--retry-failed requires --execute");
  return options;
}

export function parseFullRepairChannelFile(raw) {
  const lines = String(raw ?? "").replace(/^\uFEFF/, "").split(/\r?\n/);
  const seen = new Map();
  const channelIds = [];
  for (let index = 0; index < lines.length; index += 1) {
    const channelId = lines[index].trim();
    if (!channelId || channelId.startsWith("#")) continue;
    if (!/^UC[A-Za-z0-9_-]{22}$/.test(channelId)) {
      throw new TypeError(`invalid YouTube Channel ID at line ${index + 1}: ${channelId}`);
    }
    if (seen.has(channelId)) {
      throw new TypeError(
        `duplicate Channel ID at lines ${seen.get(channelId)} and ${index + 1}: ${channelId}`,
      );
    }
    seen.set(channelId, index + 1);
    channelIds.push(channelId);
  }
  if (channelIds.length === 0) throw new TypeError("Full Repair Channel file is empty");
  return channelIds.sort(compareText);
}

export function assertFullRepairExecutionAuthorized(options, manifest) {
  if (options?.execute !== true) return false;
  if (String(options.confirm ?? "") !== manifest?.confirmation) {
    throw new TypeError("Full Repair confirmation token does not match the manifest");
  }
  return true;
}

export async function assertFullRepairDatabaseIdentity(
  dbQuery,
  environment = process.env,
) {
  if (typeof dbQuery !== "function") throw new TypeError("dbQuery is required");
  return verifyCrawlerWriterDatabase(dbQuery, environment);
}

export function fullRepairSchedulerConflict(scheduler = {}, batchId) {
  const normalizedBatchId = repairBatchId(batchId);
  const status = String(scheduler.status ?? "stopped");
  const pipelineCycleId = String(scheduler.pipeline_cycle_id ?? "").trim() || null;
  if (status === "stopped") return null;
  if (status === "repairing" && pipelineCycleId === normalizedBatchId) return null;
  if (status === "paused") {
    return {
      code: "pipeline_paused",
      message: `crawler pipeline ${pipelineCycleId ?? "unknown"} is paused`,
    };
  }
  if (["running", "finishing", "repairing"].includes(status)) {
    return {
      code: "pipeline_busy",
      message: `crawler pipeline ${pipelineCycleId ?? "unknown"} is ${status}`,
    };
  }
  return {
    code: "scheduler_state_invalid",
    message: `query_scheduler has unsupported status: ${status}`,
  };
}

export function fullRepairRunMetadata(data = {}) {
  const rawBatchId = String(data.repair_batch_id ?? "").trim();
  if (!rawBatchId) return {};
  const batchId = repairBatchId(rawBatchId);
  if (data.trigger_reason !== "repair") {
    throw new TypeError("Full Repair jobs must use trigger_reason=repair");
  }
  const preparedAt = isoTimestamp(data.repair_prepared_at, "repair_prepared_at");
  const manifestHash = requiredText(data.repair_manifest_hash, "repair_manifest_hash");
  if (!/^sha256:[a-f0-9]{64}$/.test(manifestHash)) {
    throw new TypeError("repair_manifest_hash must be a SHA-256 digest");
  }
  const scanPolicyVersion = requiredText(
    data.repair_scan_policy_version,
    "repair_scan_policy_version",
  );
  if (scanPolicyVersion !== FULL_REPAIR_SCAN_POLICY_VERSION) {
    throw new TypeError(`unsupported Full Repair scan policy: ${scanPolicyVersion}`);
  }
  const contentLimit = positiveInteger(
    data.repair_content_limit,
    "repair_content_limit",
    { max: FULL_REPAIR_CONTENT_LIMIT },
  );
  if (contentLimit !== FULL_REPAIR_CONTENT_LIMIT) {
    throw new TypeError(`Full Repair content limit must be ${FULL_REPAIR_CONTENT_LIMIT}`);
  }
  const contentMaxAgeDays = positiveInteger(
    data.repair_content_max_age_days,
    "repair_content_max_age_days",
    { max: FULL_REPAIR_CONTENT_MAX_AGE_DAYS },
  );
  if (contentMaxAgeDays !== FULL_REPAIR_CONTENT_MAX_AGE_DAYS) {
    throw new TypeError(
      `Full Repair content max age must be ${FULL_REPAIR_CONTENT_MAX_AGE_DAYS} days`,
    );
  }
  return {
    publication_repair: {
      batch_id: batchId,
      reason: requiredText(data.repair_reason, "repair_reason"),
      prepared_at: preparedAt,
      manifest_hash: manifestHash,
      mode: "full_crawl",
      scan_policy_version: scanPolicyVersion,
      content_limit: contentLimit,
      content_max_age_days: contentMaxAgeDays,
    },
  };
}

export async function loadFullRepairTargets(dbQuery, manifest) {
  if (typeof dbQuery !== "function") throw new TypeError("dbQuery is required");
  const requestedChannelIds = manifest?.target_channel_ids;
  if (!Array.isArray(requestedChannelIds) || requestedChannelIds.length === 0) {
    throw new TypeError("a non-empty Full Repair manifest is required");
  }
  const result = await dbQuery(
    `WITH requested AS (
       SELECT channel_id,ordinality AS ordinal
       FROM unnest($1::text[]) WITH ORDINALITY AS input(channel_id,ordinality)
     )
     SELECT requested.ordinal,requested.channel_id,
            channel.status AS channel_status,channel.channel_url,
            candidate.candidate_id,candidate.status AS candidate_status,
            candidate.channel_url AS candidate_channel_url,candidate.priority,
            candidate.dispatch_batch_id AS source_dispatch_batch_id
     FROM requested
     LEFT JOIN crawler.channels channel ON channel.channel_id=requested.channel_id
     LEFT JOIN LATERAL (
       SELECT candidate_id,status,channel_url,priority,dispatch_batch_id
       FROM crawler.channel_candidates candidate
       WHERE candidate.channel_id=requested.channel_id
         AND candidate.status='accepted'
       ORDER BY candidate.accepted_at DESC NULLS LAST,candidate.candidate_id DESC
       LIMIT 1
     ) candidate ON true
     ORDER BY requested.ordinal`,
    [requestedChannelIds],
  );
  const issues = [];
  const targets = result.rows.map((row, index) => {
    const expectedChannelId = requestedChannelIds[index];
    if (row.channel_id !== expectedChannelId) {
      issues.push({ channel_id: expectedChannelId, code: "manifest_order_mismatch" });
    }
    if (row.channel_status !== "active") {
      issues.push({
        channel_id: expectedChannelId,
        code: row.channel_status == null ? "channel_missing" : "channel_not_active",
        status: row.channel_status ?? null,
      });
    }
    const candidateId = Number(row.candidate_id);
    if (!Number.isSafeInteger(candidateId) || candidateId <= 0 || row.candidate_status !== "accepted") {
      issues.push({ channel_id: expectedChannelId, code: "accepted_candidate_missing" });
    }
    return {
      candidate_id: candidateId,
      channel_id: row.channel_id,
      channel_url: row.candidate_channel_url || row.channel_url,
      priority: Number(row.priority ?? 100),
      status: row.candidate_status,
      source_dispatch_batch_id: row.source_dispatch_batch_id ?? null,
    };
  });
  if (result.rows.length !== requestedChannelIds.length) {
    issues.push({
      code: "target_count_mismatch",
      expected: requestedChannelIds.length,
      actual: result.rows.length,
    });
  }
  if (issues.length > 0) {
    const error = new TypeError(`Full Repair target validation failed for ${issues.length} item(s)`);
    error.code = "full_repair_targets_invalid";
    error.details = issues;
    throw error;
  }
  return targets;
}

function isoTimestamp(value, name) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new TypeError(`${name} must be a valid timestamp`);
  return parsed.toISOString();
}

function fullRepairMetadata(manifest, preparedAt, status = "dispatching") {
  return {
    version: manifest.version,
    batch_id: manifest.batch_id,
    manifest_hash: manifest.manifest_hash,
    target_count: manifest.target_count,
    target_channel_ids: manifest.target_channel_ids,
    scan_policy_version: manifest.scan_policy_version,
    content_limit: manifest.content_limit,
    content_max_age_days: manifest.content_max_age_days,
    status,
    prepared_at: preparedAt,
    dispatched_at: null,
  };
}

function assertMatchingStoredManifest(stored, manifest) {
  const matches = stored
    && stored.version === manifest.version
    && stored.batch_id === manifest.batch_id
    && stored.manifest_hash === manifest.manifest_hash
    && Number(stored.target_count) === manifest.target_count
    && JSON.stringify(stored.target_channel_ids) === JSON.stringify(manifest.target_channel_ids)
    && stored.scan_policy_version === manifest.scan_policy_version
    && Number(stored.content_limit) === manifest.content_limit
    && Number(stored.content_max_age_days) === manifest.content_max_age_days;
  if (!matches) {
    const error = new TypeError(`Full Repair batch ${manifest.batch_id} has a different manifest`);
    error.code = "full_repair_manifest_conflict";
    throw error;
  }
}

export async function prepareFullRepairBatch(client, {
  manifest,
  preparedAt = new Date().toISOString(),
} = {}) {
  if (!client || typeof client.query !== "function") throw new TypeError("database client is required");
  const normalizedPreparedAt = isoTimestamp(preparedAt, "preparedAt");
  await client.query(
    "SELECT pg_advisory_xact_lock(hashtext($1))",
    [`publication-full-repair:${manifest.batch_id}`],
  );
  const schedulerRows = await client.query(
    `SELECT value_json
     FROM crawler.settings
     WHERE setting_key='query_scheduler'
     LIMIT 1
     FOR UPDATE`,
  );
  if (schedulerRows.rows.length !== 1) throw new Error("query_scheduler setting is missing");
  const conflict = fullRepairSchedulerConflict(schedulerRows.rows[0].value_json, manifest.batch_id);
  if (conflict) {
    const error = new Error(conflict.message);
    error.code = conflict.code;
    throw error;
  }
  const batchRows = await client.query(
    `SELECT status,result_json,started_at,finished_at
     FROM crawler.query_dispatch_batches
     WHERE dispatch_batch_id=$1
     LIMIT 1
     FOR UPDATE`,
    [manifest.batch_id],
  );
  const existing = batchRows.rows[0] ?? null;
  const stored = existing?.result_json?.full_repair_dispatch ?? null;
  if (existing) {
    assertMatchingStoredManifest(stored, manifest);
    const stablePreparedAt = isoTimestamp(stored.prepared_at, "stored prepared_at");
    if (existing.status === "completed" || stored.status === "completed") {
      return {
        created: false,
        completed: true,
        prepared_at: stablePreparedAt,
        batch_id: manifest.batch_id,
      };
    }
    const metadata = {
      ...stored,
      ...fullRepairMetadata(manifest, stablePreparedAt, stored.status || "dispatching"),
      dispatch_cursor: Number(stored.dispatch_cursor ?? 0),
      last_dispatch_at: stored.last_dispatch_at ?? null,
      dispatched_at: stored.dispatched_at ?? null,
    };
    await client.query(
      `UPDATE crawler.query_dispatch_batches
       SET status='finishing',finished_at=NULL,
           result_json=jsonb_set(result_json,'{full_repair_dispatch}',$2::jsonb,true),
           updated_at=now()
       WHERE dispatch_batch_id=$1`,
      [manifest.batch_id, JSON.stringify(metadata)],
    );
  } else {
    const metadata = fullRepairMetadata(manifest, normalizedPreparedAt);
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at,
         validation_closed_at,result_json,started_at,updated_at
       ) VALUES ($1,$1,'finishing',$3::timestamptz,$3::timestamptz,$2::jsonb,$3::timestamptz,now())`,
      [manifest.batch_id, JSON.stringify({ full_repair_dispatch: metadata }), normalizedPreparedAt],
    );
  }
  const stablePreparedAt = existing
    ? isoTimestamp(stored.prepared_at, "stored prepared_at")
    : normalizedPreparedAt;
  const schedulerUpdate = await client.query(
    `UPDATE crawler.settings
     SET value_json=value_json || jsonb_build_object(
           'status','repairing',
           'pipeline_cycle_id',$2::text,
           'started_at',$3::text,
           'paused_at',NULL,
           'stopped_at',NULL,
           'completed_at',NULL,
           'stop_reason','publication_readiness_full_repair',
           'repair_started_at',$3::text,
           'repair_manifest_hash',$4::text,
           'repair_target_count',$5::int,
           'repair_scan_policy_version',$6::text,
           'repair_content_limit',$7::int,
           'repair_content_max_age_days',$8::int,
           'updated_at',$3::text,
           'updated_by','fullRepairDispatcher'
         ),updated_at=now()
     WHERE setting_key=$1`,
    [
      "query_scheduler",
      manifest.batch_id,
      stablePreparedAt,
      manifest.manifest_hash,
      manifest.target_count,
      manifest.scan_policy_version,
      manifest.content_limit,
      manifest.content_max_age_days,
    ],
  );
  if (schedulerUpdate.rowCount !== 1) throw new Error("query_scheduler setting is missing");
  return {
    created: !existing,
    completed: false,
    prepared_at: stablePreparedAt,
    batch_id: manifest.batch_id,
  };
}

const REPRESENTED_JOB_STATES = new Set([
  ...CHANNEL_QUEUE_PRESSURE_STATES,
  "completed",
  "failed",
]);

export async function dispatchFullRepairPass({
  dbQuery,
  queue,
  manifest,
  targets,
  preparedAt,
  dispatchCursor = 0,
  highWater = 5,
  refill = 2,
  retryFailed = false,
  now = new Date().toISOString(),
} = {}) {
  if (typeof dbQuery !== "function") throw new TypeError("dbQuery is required");
  if (!queue || typeof queue.add !== "function" || typeof queue.getJob !== "function") {
    throw new TypeError("channel crawl queue is required");
  }
  if (!Array.isArray(targets) || targets.length !== manifest?.target_count) {
    throw new TypeError("targets must exactly match the Full Repair manifest");
  }
  const stablePreparedAt = isoTimestamp(preparedAt, "preparedAt");
  const observedAt = isoTimestamp(now, "now");
  let cursor = Number(dispatchCursor);
  if (!Number.isSafeInteger(cursor) || cursor < 0 || cursor > targets.length) {
    throw new TypeError("dispatchCursor is outside the Full Repair manifest");
  }
  const runIds = targets.slice(cursor).map((target) => (
    `full-repair:${manifest.batch_id}:${target.channel_id}`
  ));
  const [runRows, counts, paused] = await Promise.all([
    runIds.length === 0
      ? Promise.resolve({ rows: [] })
      : dbQuery(
        `SELECT run_id,channel_id,status,detail_status
         FROM crawler.channel_runs
         WHERE run_id=ANY($1::text[])`,
        [runIds],
      ),
    queue.getJobCounts(...CHANNEL_QUEUE_PRESSURE_STATES),
    queue.isPaused(),
  ]);
  const runs = new Map(runRows.rows.map((row) => [String(row.run_id), row]));
  let capacity = channelDispatchCapacity(counts, { highWater, refill, paused });
  let dispatched = 0;
  let represented = 0;
  let failed = 0;

  while (cursor < targets.length) {
    const target = targets[cursor];
    const spec = fullRepairChannelJob({
      batchId: manifest.batch_id,
      manifestHash: manifest.manifest_hash,
      scanPolicyVersion: manifest.scan_policy_version,
      contentLimit: manifest.content_limit,
      contentMaxAgeDays: manifest.content_max_age_days,
      preparedAt: stablePreparedAt,
      candidate: target,
    });
    const run = runs.get(spec.data.run_id) ?? null;
    if (run && !(retryFailed && run.status === "failed")) {
      if (run.status === "failed") failed += 1;
      represented += 1;
      cursor += 1;
      continue;
    }
    const existing = await queue.getJob(spec.options.jobId);
    if (existing) {
      const state = await existing.getState();
      if (
        ["completed", "failed"].includes(state)
        && retryFailed
        && (!run || run.status === "failed")
      ) {
        if (capacity <= 0) break;
        await existing.remove();
        await queue.add(spec.name, spec.data, spec.options);
        capacity -= 1;
        dispatched += 1;
        cursor += 1;
        continue;
      }
      if (!REPRESENTED_JOB_STATES.has(state)) {
        throw new Error(`Full Repair job ${spec.options.jobId} has unsupported state: ${state}`);
      }
      if (state === "failed") failed += 1;
      represented += 1;
      cursor += 1;
      continue;
    }
    if (capacity <= 0) break;
    await queue.add(spec.name, spec.data, spec.options);
    capacity -= 1;
    dispatched += 1;
    cursor += 1;
  }

  const status = cursor === targets.length ? "dispatched" : "dispatching";
  const progress = {
    dispatch_cursor: cursor,
    status,
    last_dispatch_at: observedAt,
    ...(status === "dispatched" ? { dispatched_at: observedAt } : {}),
  };
  const stored = await dbQuery(
    `UPDATE crawler.query_dispatch_batches
     SET result_json=jsonb_set(
           result_json,
           '{full_repair_dispatch}',
           COALESCE(result_json->'full_repair_dispatch','{}'::jsonb) || $2::jsonb,
           true
         ),updated_at=now()
     WHERE dispatch_batch_id=$1
       AND result_json#>>'{full_repair_dispatch,manifest_hash}'=$3
     RETURNING dispatch_batch_id`,
    [manifest.batch_id, JSON.stringify(progress), manifest.manifest_hash],
  );
  if (stored.rowCount !== 1) throw new Error("Full Repair batch progress could not be persisted");
  return {
    batch_id: manifest.batch_id,
    status,
    dispatch_cursor: cursor,
    target_count: targets.length,
    dispatched,
    represented,
    failed,
    pressure: channelQueuePressure(counts),
    paused,
    counts,
  };
}

export async function loadFullRepairCompletionState(dbQuery, batchId) {
  if (typeof dbQuery !== "function") throw new TypeError("dbQuery is required");
  const normalizedBatchId = repairBatchId(batchId);
  const result = await dbQuery(
    `WITH batch AS (
       SELECT status AS batch_status,
              result_json->'full_repair_dispatch' AS repair
       FROM crawler.query_dispatch_batches
       WHERE dispatch_batch_id=$1
         AND result_json ? 'full_repair_dispatch'
     ), targets AS (
       SELECT target.channel_id
       FROM batch
       CROSS JOIN LATERAL jsonb_array_elements_text(
         COALESCE(batch.repair->'target_channel_ids','[]'::jsonb)
       ) AS target(channel_id)
     ), target_state AS (
       SELECT target.channel_id,channel.status AS channel_status,
              run.status AS run_status
       FROM targets target
       LEFT JOIN crawler.channels channel ON channel.channel_id=target.channel_id
       LEFT JOIN crawler.channel_runs run
         ON run.run_id='full-repair:' || $1::text || ':' || target.channel_id
     )
     SELECT batch.batch_status,
            batch.repair->>'status' AS dispatch_status,
            COALESCE((batch.repair->>'target_count')::int,0)::int AS expected_count,
            count(target_state.channel_id)::int AS target_count,
            count(*) FILTER (
              WHERE target_state.run_status IN ('done','skipped')
                 OR target_state.channel_status='removed'
            )::int AS succeeded_count,
            count(*) FILTER (
              WHERE target_state.channel_status IS DISTINCT FROM 'removed'
                AND target_state.run_status IN (
                  'queued','running','waiting_pages','waiting_detail','waiting_agent','finalizing'
                )
            )::int AS open_count,
            count(*) FILTER (
              WHERE target_state.channel_status IS DISTINCT FROM 'removed'
                AND target_state.run_status='failed'
            )::int AS failed_count,
            count(*) FILTER (
              WHERE target_state.channel_status IS DISTINCT FROM 'removed'
                AND target_state.run_status IS NULL
            )::int AS missing_count,
            count(*) FILTER (WHERE target_state.channel_status='removed')::int AS removed_count
     FROM batch
     LEFT JOIN target_state ON true
     GROUP BY batch.batch_status,batch.repair`,
    [normalizedBatchId],
  );
  const row = result.rows[0];
  if (!row) return null;
  const state = {
    batch_id: normalizedBatchId,
    batch_status: row.batch_status,
    dispatch_status: row.dispatch_status,
    expected_count: Number(row.expected_count ?? 0),
    target_count: Number(row.target_count ?? 0),
    succeeded_count: Number(row.succeeded_count ?? 0),
    open_count: Number(row.open_count ?? 0),
    failed_count: Number(row.failed_count ?? 0),
    missing_count: Number(row.missing_count ?? 0),
    removed_count: Number(row.removed_count ?? 0),
  };
  return {
    ...state,
    complete: state.dispatch_status === "dispatched"
      && state.expected_count > 0
      && state.target_count === state.expected_count
      && state.succeeded_count === state.target_count
      && state.open_count === 0
      && state.failed_count === 0
      && state.missing_count === 0,
  };
}

export async function loadFullRepairDispatchState(dbQuery, manifest) {
  if (typeof dbQuery !== "function") throw new TypeError("dbQuery is required");
  const result = await dbQuery(
    `SELECT status,result_json,started_at,finished_at
     FROM crawler.query_dispatch_batches
     WHERE dispatch_batch_id=$1
     LIMIT 1`,
    [manifest.batch_id],
  );
  const batch = result.rows[0];
  if (!batch) {
    return {
      exists: false,
      completed: false,
      batch_status: null,
      dispatch_status: null,
      dispatch_cursor: 0,
      prepared_at: null,
    };
  }
  const stored = batch.result_json?.full_repair_dispatch;
  assertMatchingStoredManifest(stored, manifest);
  const cursor = Number(stored.dispatch_cursor ?? 0);
  if (!Number.isSafeInteger(cursor) || cursor < 0 || cursor > manifest.target_count) {
    throw new TypeError(`Full Repair batch ${manifest.batch_id} has an invalid dispatch cursor`);
  }
  return {
    exists: true,
    completed: batch.status === "completed" || stored.status === "completed",
    batch_status: batch.status,
    dispatch_status: stored.status ?? "dispatching",
    dispatch_cursor: cursor,
    prepared_at: isoTimestamp(stored.prepared_at, "stored prepared_at"),
  };
}

export function buildFullRepairManifest({ batchId, channelIds } = {}) {
  const normalizedBatchId = repairBatchId(batchId);
  if (!Array.isArray(channelIds) || channelIds.length === 0) {
    throw new TypeError("channelIds must contain at least one Channel ID");
  }
  const normalizedChannelIds = channelIds.map((value) => requiredText(value, "channelId"));
  for (const channelId of normalizedChannelIds) {
    if (!/^UC[A-Za-z0-9_-]{22}$/.test(channelId)) {
      throw new TypeError(`invalid YouTube Channel ID: ${channelId}`);
    }
  }
  const uniqueChannelIds = [...new Set(normalizedChannelIds)].sort(compareText);
  if (uniqueChannelIds.length !== normalizedChannelIds.length) {
    throw new TypeError("channelIds must not contain duplicates");
  }
  const canonical = `${[
    FULL_REPAIR_MANIFEST_VERSION,
    normalizedBatchId,
    FULL_REPAIR_SCAN_POLICY_VERSION,
    FULL_REPAIR_CONTENT_LIMIT,
    FULL_REPAIR_CONTENT_MAX_AGE_DAYS,
    ...uniqueChannelIds,
  ].join("\n")}\n`;
  const digest = createHash("sha256").update(canonical).digest("hex");
  return {
    version: FULL_REPAIR_MANIFEST_VERSION,
    batch_id: normalizedBatchId,
    target_count: uniqueChannelIds.length,
    target_channel_ids: uniqueChannelIds,
    scan_policy_version: FULL_REPAIR_SCAN_POLICY_VERSION,
    content_limit: FULL_REPAIR_CONTENT_LIMIT,
    content_max_age_days: FULL_REPAIR_CONTENT_MAX_AGE_DAYS,
    manifest_hash: `sha256:${digest}`,
    confirmation: `confirm-full-repair:${normalizedBatchId}:${uniqueChannelIds.length}:${digest.slice(0, 16)}`,
  };
}

export function fullRepairChannelJob({
  batchId,
  manifestHash,
  scanPolicyVersion,
  contentLimit,
  contentMaxAgeDays,
  preparedAt,
  candidate,
} = {}) {
  const normalizedBatchId = repairBatchId(batchId);
  const normalizedManifestHash = requiredText(manifestHash, "manifestHash");
  const normalizedScanPolicyVersion = requiredText(scanPolicyVersion, "scanPolicyVersion");
  if (normalizedScanPolicyVersion !== FULL_REPAIR_SCAN_POLICY_VERSION) {
    throw new TypeError(`unsupported Full Repair scan policy: ${normalizedScanPolicyVersion}`);
  }
  const normalizedContentLimit = positiveInteger(contentLimit, "contentLimit", {
    max: FULL_REPAIR_CONTENT_LIMIT,
  });
  if (normalizedContentLimit !== FULL_REPAIR_CONTENT_LIMIT) {
    throw new TypeError(`Full Repair content limit must be ${FULL_REPAIR_CONTENT_LIMIT}`);
  }
  const normalizedContentMaxAgeDays = positiveInteger(contentMaxAgeDays, "contentMaxAgeDays", {
    max: FULL_REPAIR_CONTENT_MAX_AGE_DAYS,
  });
  if (normalizedContentMaxAgeDays !== FULL_REPAIR_CONTENT_MAX_AGE_DAYS) {
    throw new TypeError(
      `Full Repair content max age must be ${FULL_REPAIR_CONTENT_MAX_AGE_DAYS} days`,
    );
  }
  const normalizedPreparedAt = requiredText(preparedAt, "preparedAt");
  if (candidate?.status !== "accepted") {
    throw new TypeError("Full Repair requires an accepted Candidate");
  }
  const channelId = requiredText(candidate.channel_id, "candidate.channel_id");
  const channelUrl = requiredText(candidate.channel_url, "candidate.channel_url");
  const candidateId = Number(candidate.candidate_id);
  if (!Number.isSafeInteger(candidateId) || candidateId <= 0) {
    throw new TypeError("candidate.candidate_id must be a positive integer");
  }

  return {
    name: "channel-full-repair",
    data: {
      candidate_id: candidateId,
      dispatch_batch_id: normalizedBatchId,
      pipeline_cycle_id: normalizedBatchId,
      channel_id: channelId,
      channel_url: channelUrl,
      crawl_mode: "full",
      query_id: null,
      query_text: "publication readiness full repair",
      enforce_min_subscribers: false,
      reject_if_no_recent_content: false,
      trigger_reason: "repair",
      repair_batch_id: normalizedBatchId,
      repair_reason: "publication_readiness_full_repair",
      repair_prepared_at: normalizedPreparedAt,
      repair_manifest_hash: normalizedManifestHash,
      repair_scan_policy_version: normalizedScanPolicyVersion,
      repair_content_limit: normalizedContentLimit,
      repair_content_max_age_days: normalizedContentMaxAgeDays,
      run_id: `full-repair:${normalizedBatchId}:${channelId}`,
    },
    options: {
      jobId: safeJobId("channel-full-repair", normalizedBatchId, channelId),
      priority: Number(candidate.priority ?? 100),
    },
  };
}
