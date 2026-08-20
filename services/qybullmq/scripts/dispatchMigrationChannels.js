import { parseArgs } from "node:util";
import { Queue } from "bullmq";
import { closeDb, pool } from "../src/db.js";
import {
  CHANNEL_QUEUE_PRESSURE_STATES,
  channelDispatchCapacity,
  channelQueuePressure,
  channelSnapshotPayload,
  migrationBatchHasOpenWork,
} from "../src/migrationDispatchPolicy.js";
import { defaultJobOptions, queuesByRole, redisOptions, safeJobId } from "../src/queues.js";

const DEFAULTS = {
  sourceBatchId: process.env.MIGRATION_SOURCE_BATCH_ID || "legacy-results-full-v1",
  batchId: process.env.MIGRATION_DISPATCH_BATCH_ID || "legacy-results-pilot-30-v1",
  limit: Number(process.env.MIGRATION_DISPATCH_LIMIT || 30),
  highWater: Number(process.env.MIGRATION_CHANNEL_QUEUE_HIGH_WATER || 5),
  refill: Number(process.env.MIGRATION_CHANNEL_REFILL || 5),
  pollMs: Number(process.env.MIGRATION_DISPATCH_POLL_MS || 15000),
  minSubscriberCount: Number(process.env.MIN_SUBSCRIBER_COUNT || 1000),
};

function positiveInteger(value, name, { min = 1, max = 1_000_000 } = {}) {
  const number = Number(value);
  if (!Number.isInteger(number) || number < min || number > max) {
    throw new Error(`${name} must be an integer between ${min} and ${max}`);
  }
  return number;
}

function optionsFromArgs() {
  const { values } = parseArgs({
    options: {
      execute: { type: "boolean", default: false },
      watch: { type: "boolean", default: false },
      "prepare-only": { type: "boolean", default: false },
      "source-batch-id": { type: "string" },
      "batch-id": { type: "string" },
      limit: { type: "string" },
      "high-water": { type: "string" },
      refill: { type: "string" },
      "poll-ms": { type: "string" },
      help: { type: "boolean", short: "h", default: false },
    },
    strict: true,
  });
  if (values.help) {
    console.log("Usage: node scripts/dispatchMigrationChannels.js [--execute] [--watch] [--prepare-only]");
    console.log("Defaults: 30-channel pilot, queue high-water 5, refill 5 every 15 seconds.");
    process.exit(0);
  }
  const options = {
    execute: values.execute,
    watch: values.watch,
    prepareOnly: values["prepare-only"],
    sourceBatchId: String(values["source-batch-id"] || DEFAULTS.sourceBatchId).trim(),
    batchId: String(values["batch-id"] || DEFAULTS.batchId).trim(),
    limit: positiveInteger(values.limit ?? DEFAULTS.limit, "limit", { max: 100_000 }),
    highWater: positiveInteger(values["high-water"] ?? DEFAULTS.highWater, "high-water", { max: 10_000 }),
    refill: positiveInteger(values.refill ?? DEFAULTS.refill, "refill", { max: 10_000 }),
    pollMs: positiveInteger(values["poll-ms"] ?? DEFAULTS.pollMs, "poll-ms", { min: 1000, max: 300_000 }),
    minSubscriberCount: positiveInteger(DEFAULTS.minSubscriberCount, "MIN_SUBSCRIBER_COUNT", { max: 1_000_000_000 }),
  };
  if (!options.sourceBatchId || !options.batchId) throw new Error("source and target batch IDs are required");
  if (options.sourceBatchId === options.batchId) throw new Error("source and target batch IDs must differ");
  if (options.refill > options.highWater) throw new Error("refill cannot exceed high-water");
  if (options.watch && !options.execute) throw new Error("--watch requires --execute");
  return options;
}

async function loadBatchState(client, batchId) {
  const result = await client.query(
    `SELECT batch.*,
            count(candidate.candidate_id)::int AS candidate_count,
            count(candidate.candidate_id) FILTER (
              WHERE candidate.status IN ('discovered','queued','validating')
            )::int AS open_count,
            count(candidate.candidate_id) FILTER (
              WHERE candidate.status IN ('accepted','rejected','existing','failed')
            )::int AS terminal_count,
            (
              SELECT count(*)::int
              FROM crawler.channel_runs run
              JOIN crawler.channel_candidates active_candidate
                ON active_candidate.candidate_id=run.candidate_id
              JOIN crawler.channels channel
                ON channel.channel_id=active_candidate.channel_id
               AND channel.latest_run_id=run.run_id
              WHERE active_candidate.dispatch_batch_id=batch.dispatch_batch_id
                AND run.status IN (
                  'queued','running','waiting_pages','waiting_detail','waiting_agent','finalizing'
                )
            ) AS open_run_count
     FROM crawler.query_dispatch_batches batch
     LEFT JOIN crawler.channel_candidates candidate
       ON candidate.dispatch_batch_id=batch.dispatch_batch_id
     WHERE batch.dispatch_batch_id=$1
     GROUP BY batch.dispatch_batch_id`,
    [batchId],
  );
  return result.rows[0] ?? null;
}

function verifyExistingPilot(batch, options) {
  const metadata = batch?.result_json?.migration_dispatcher;
  if (!metadata) throw new Error(`batch ${options.batchId} was not created by the migration dispatcher`);
  if (metadata.source_batch_id !== options.sourceBatchId || Number(metadata.limit) !== options.limit) {
    throw new Error(`batch ${options.batchId} does not match requested source/limit`);
  }
  if (Number(batch.candidate_count) !== options.limit) {
    throw new Error(`batch ${options.batchId} contains ${batch.candidate_count} candidates, expected ${options.limit}`);
  }
}

async function candidatePreview(client, options, { lock = false } = {}) {
  const result = await client.query(
    `SELECT candidate_id,channel_id,channel_url,priority
     FROM crawler.channel_candidates
     WHERE dispatch_batch_id=$1
       AND status='discovered'
       AND source_json->>'source'='legacy_results_db'
     ORDER BY candidate_id
     LIMIT $2
     ${lock ? "FOR UPDATE" : ""}`,
    [options.sourceBatchId, options.limit],
  );
  if (result.rows.length !== options.limit) {
    throw new Error(`source batch has ${result.rows.length} eligible candidates, expected ${options.limit}`);
  }
  return result.rows;
}

async function preparePilot(client, options) {
  const existing = await loadBatchState(client, options.batchId);
  if (existing) {
    verifyExistingPilot(existing, options);
    return { created: false, batch: existing };
  }
  if (!options.execute) {
    const candidates = await candidatePreview(client, options);
    return {
      created: false,
      preview: true,
      candidate_count: candidates.length,
      first_candidate_id: candidates[0]?.candidate_id ?? null,
      last_candidate_id: candidates.at(-1)?.candidate_id ?? null,
    };
  }

  await client.query("BEGIN");
  try {
    const candidates = await candidatePreview(client, options, { lock: true });
    const candidateIds = candidates.map((candidate) => candidate.candidate_id);
    const pageId = `${options.batchId}:page:1`;
    const metadata = {
      source: "results.db",
      purpose: "migration_channel_pilot",
      migration_dispatcher: {
        source_batch_id: options.sourceBatchId,
        limit: options.limit,
        high_water: options.highWater,
        refill: options.refill,
        min_subscriber_count: options.minSubscriberCount,
        first_candidate_id: candidateIds[0],
        last_candidate_id: candidateIds.at(-1),
      },
    };
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,discovered_candidate_count,
         discovery_closed_at,result_json,updated_at
       ) VALUES ($1,$1,'discovery_closed',$2,now(),$3::jsonb,now())`,
      [options.batchId, options.limit, JSON.stringify(metadata)],
    );
    await client.query(
      `INSERT INTO crawler.query_pages (
         page_id,query_text,page_no,status,candidate_count,should_continue,
         stop_reason,result_json,dispatch_batch_id,finished_at,updated_at
       ) VALUES ($1,'results.db migration pilot',1,'done',$2,false,
                 'migration_seed_pilot',$3::jsonb,$4,now(),now())`,
      [pageId, options.limit, JSON.stringify(metadata), options.batchId],
    );
    await client.query(
      `UPDATE crawler.channel_candidates
       SET dispatch_batch_id=$1,pipeline_cycle_id=$1,updated_at=now()
       WHERE candidate_id=ANY($2::bigint[])`,
      [options.batchId, candidateIds],
    );
    await client.query(
      `UPDATE crawler.channel_candidate_sources
       SET page_id=$1,query_text='results.db migration pilot',
           source_json=source_json || jsonb_build_object('pilot_batch_id',$2::text)
       WHERE candidate_id=ANY($3::bigint[])`,
      [pageId, options.batchId, candidateIds],
    );
    await client.query(
      `UPDATE crawler.query_dispatch_batches batch
       SET discovered_candidate_count=stats.total,
           accepted_channel_count=stats.accepted,
           rejected_channel_count=stats.rejected,
           updated_at=now()
       FROM (
         SELECT count(*)::int AS total,
                count(*) FILTER (WHERE status='accepted')::int AS accepted,
                count(*) FILTER (WHERE status='rejected')::int AS rejected
         FROM crawler.channel_candidates WHERE dispatch_batch_id=$1
       ) stats
       WHERE batch.dispatch_batch_id=$1`,
      [options.sourceBatchId],
    );
    await client.query(
      `UPDATE crawler.query_pages page
       SET candidate_count=stats.total,updated_at=now()
       FROM (
         SELECT source.page_id,count(*)::int AS total
         FROM crawler.channel_candidate_sources source
         JOIN crawler.channel_candidates candidate ON candidate.candidate_id=source.candidate_id
         WHERE candidate.dispatch_batch_id=$1 AND source.page_id IS NOT NULL
         GROUP BY source.page_id
       ) stats
       WHERE page.page_id=stats.page_id`,
      [options.sourceBatchId],
    );
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
  const batch = await loadBatchState(client, options.batchId);
  verifyExistingPilot(batch, options);
  return { created: true, batch };
}

async function activatePipeline(client, options) {
  const now = new Date().toISOString();
  const result = await client.query(
    `UPDATE crawler.settings
     SET value_json=value_json || jsonb_build_object(
           'status','finishing',
           'pipeline_cycle_id',$2::text,
           'started_at',COALESCE(value_json->'started_at',to_jsonb($3::text)),
           'stopped_at',NULL,
           'completed_at',NULL,
           'stop_reason','migration_seed_dispatch',
           'updated_at',$3::text,
           'updated_by','migrationChannelDispatcher'
         ),updated_at=now()
     WHERE setting_key=$1`,
    ["query_scheduler", options.batchId, now],
  );
  if (result.rowCount !== 1) throw new Error("query_scheduler setting is missing");
}

async function dispatchOnce(client, queue, options) {
  const [counts, paused] = await Promise.all([
    queue.getJobCounts(...CHANNEL_QUEUE_PRESSURE_STATES),
    queue.isPaused(),
  ]);
  const capacity = channelDispatchCapacity(counts, {
    highWater: options.highWater,
    refill: options.refill,
    paused,
  });
  if (capacity === 0) {
    return { dispatched: 0, pressure: channelQueuePressure(counts), capacity, paused, counts };
  }
  const candidates = await client.query(
    `SELECT candidate_id,channel_id,channel_url,priority,status
     FROM crawler.channel_candidates
     WHERE dispatch_batch_id=$1 AND status IN ('discovered','queued')
     ORDER BY candidate_id
     LIMIT $2`,
    [options.batchId, Math.max(capacity * 10, 20)],
  );
  let dispatched = 0;
  let represented = 0;
  const blocked = [];
  for (const candidate of candidates.rows) {
    if (dispatched >= capacity) break;
    const jobId = safeJobId("channel-snapshot", options.batchId, candidate.channel_id);
    const existing = await queue.getJob(jobId);
    if (existing) {
      const state = await existing.getState();
      if (CHANNEL_QUEUE_PRESSURE_STATES.includes(state)) represented += 1;
      else blocked.push({ candidate_id: candidate.candidate_id, job_id: jobId, state });
      continue;
    }
    await client.query(
      `UPDATE crawler.channel_candidates
       SET status='queued',next_retry_at=NULL,validation_finished_at=NULL,updated_at=now()
       WHERE candidate_id=$1 AND status IN ('discovered','queued')`,
      [candidate.candidate_id],
    );
    try {
      await queue.add(
        "channel-snapshot",
        channelSnapshotPayload(candidate, options.batchId, {
          minSubscriberCount: options.minSubscriberCount,
        }),
        { jobId, priority: Number(candidate.priority ?? 100) },
      );
      dispatched += 1;
    } catch (error) {
      await client.query(
        `UPDATE crawler.channel_candidates
         SET status='discovered',error_message=$2,updated_at=now()
         WHERE candidate_id=$1 AND status='queued' AND validation_started_at IS NULL`,
        [candidate.candidate_id, error?.message || String(error)],
      );
      throw error;
    }
  }
  return {
    dispatched,
    represented,
    blocked,
    pressure: channelQueuePressure(counts),
    capacity,
    paused,
    counts,
  };
}

let stopping = false;
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.once(signal, () => { stopping = true; });
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  const options = optionsFromArgs();
  const client = await pool.connect();
  const queue = new Queue(queuesByRole.channelCrawl, {
    connection: redisOptions,
    defaultJobOptions,
  });
  const lockName = `migration-channel-dispatch:${options.batchId}`;
  try {
    const lock = await client.query("SELECT pg_try_advisory_lock(hashtext($1)) AS acquired", [lockName]);
    if (!lock.rows[0]?.acquired) throw new Error(`another dispatcher owns ${options.batchId}`);
    const prepared = await preparePilot(client, options);
    console.log(JSON.stringify({ event: "migration_pilot_prepare", mode: options.execute ? "execute" : "dry_run", options, prepared }));
    if (!options.execute || options.prepareOnly) return;
    await activatePipeline(client, options);
    do {
      const dispatch = await dispatchOnce(client, queue, options);
      const batch = await loadBatchState(client, options.batchId);
      console.log(JSON.stringify({ event: "migration_channel_dispatch", batch_id: options.batchId, dispatch, batch }));
      if (!migrationBatchHasOpenWork(batch)) break;
      if (!options.watch || stopping) break;
      await sleep(options.pollMs);
    } while (!stopping);
  } finally {
    try {
      await client.query("SELECT pg_advisory_unlock(hashtext($1))", [lockName]);
    } finally {
      client.release();
      await Promise.allSettled([queue.close(), closeDb()]);
    }
  }
}

main().catch((error) => {
  console.error(JSON.stringify({ event: "migration_channel_dispatch_failed", error: error?.stack || String(error) }));
  process.exitCode = 1;
});
