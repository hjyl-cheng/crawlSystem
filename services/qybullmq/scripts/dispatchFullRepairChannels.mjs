#!/usr/bin/env node

import { readFile } from "node:fs/promises";
import { Queue } from "bullmq";
import { closeDb, pool } from "../src/db.js";
import {
  assertFullRepairDatabaseIdentity,
  assertFullRepairExecutionAuthorized,
  buildFullRepairManifest,
  dispatchFullRepairPass,
  fullRepairSchedulerConflict,
  loadFullRepairCompletionState,
  loadFullRepairDispatchState,
  loadFullRepairTargets,
  parseFullRepairArgs,
  parseFullRepairChannelFile,
  prepareFullRepairBatch,
} from "../src/fullRepairDispatch.js";
import { defaultJobOptions, queuesByRole, redisOptions } from "../src/queues.js";

function usage() {
  return `Usage: npm run dispatch:full-repair -- --channels-file <path> --batch-id <id> [options]

Default mode is read-only dry-run. Execute requires the confirmation emitted by dry-run.

Options:
  --execute                 Prepare and dispatch the audited Repair batch
  --confirm <token>         Exact confirmation token emitted by dry-run
  --watch                   Continue dispatching and monitoring until closure
  --retry-failed            Explicitly retry terminal failed Repair jobs
  --high-water <count>      Global Channel queue high-water mark (default: 5)
  --refill <count>          Maximum jobs added per pass (default: 2)
  --poll-ms <milliseconds>  Watch interval (default: 15000)
  --help                    Show this help
`;
}

function publicOptions(options) {
  return {
    channels_file: options.channelsFile,
    batch_id: options.batchId,
    execute: options.execute,
    watch: options.watch,
    retry_failed: options.retryFailed,
    high_water: options.highWater,
    refill: options.refill,
    poll_ms: options.pollMs,
  };
}

function publicManifest(manifest) {
  return {
    version: manifest.version,
    batch_id: manifest.batch_id,
    target_count: manifest.target_count,
    manifest_hash: manifest.manifest_hash,
    scan_policy_version: manifest.scan_policy_version,
    content_limit: manifest.content_limit,
    content_max_age_days: manifest.content_max_age_days,
    confirmation: manifest.confirmation,
    first_channel_id: manifest.target_channel_ids[0],
    last_channel_id: manifest.target_channel_ids.at(-1),
  };
}

async function schedulerState(dbQuery) {
  const result = await dbQuery(
    "SELECT value_json FROM crawler.settings WHERE setting_key='query_scheduler' LIMIT 1",
  );
  if (result.rows.length !== 1) throw new Error("query_scheduler setting is missing");
  return result.rows[0].value_json;
}

async function transaction(client, action) {
  await client.query("BEGIN");
  try {
    const result = await action(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

let stopping = false;
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.once(signal, () => { stopping = true; });
}

async function main() {
  const argv = process.argv.slice(2);
  if (argv.includes("--help")) {
    process.stdout.write(usage());
    return;
  }
  const options = parseFullRepairArgs(argv);
  const channelIds = parseFullRepairChannelFile(await readFile(options.channelsFile, "utf8"));
  const manifest = buildFullRepairManifest({ batchId: options.batchId, channelIds });
  const client = await pool.connect();
  let queue = null;
  let lockAcquired = false;
  const lockName = `publication-full-repair-dispatch:${manifest.batch_id}`;
  try {
    const dbQuery = client.query.bind(client);
    const database = await assertFullRepairDatabaseIdentity(
      dbQuery,
      process.env,
    );
    const previewTargets = await loadFullRepairTargets(dbQuery, manifest);
    const scheduler = await schedulerState(dbQuery);
    const existing = await loadFullRepairDispatchState(dbQuery, manifest);
    const conflict = fullRepairSchedulerConflict(scheduler, manifest.batch_id);
    const preview = {
      event: "publication_full_repair_preview",
      mode: options.execute ? "execute" : "dry_run",
      options: publicOptions(options),
      database,
      manifest: publicManifest(manifest),
      target_validation: {
        valid_count: previewTargets.length,
        accepted_candidate_count: previewTargets.filter((target) => target.status === "accepted").length,
        source_batch_count: new Set(previewTargets.map((target) => target.source_dispatch_batch_id)).size,
      },
      scheduler: {
        status: scheduler.status ?? "stopped",
        pipeline_cycle_id: scheduler.pipeline_cycle_id ?? null,
        conflict,
      },
      existing_batch: existing,
    };
    process.stdout.write(`${JSON.stringify(preview)}\n`);
    if (conflict) {
      const error = new Error(conflict.message);
      error.code = conflict.code;
      throw error;
    }
    if (!assertFullRepairExecutionAuthorized(options, manifest)) return;
    if (existing.completed) {
      process.stdout.write(`${JSON.stringify({
        event: "publication_full_repair_already_completed",
        batch_id: manifest.batch_id,
      })}\n`);
      return;
    }

    const lock = await client.query("SELECT pg_try_advisory_lock(hashtext($1)) AS acquired", [lockName]);
    lockAcquired = lock.rows[0]?.acquired === true;
    if (!lockAcquired) throw new Error(`another Full Repair dispatcher owns ${manifest.batch_id}`);
    const targets = await loadFullRepairTargets(dbQuery, manifest);
    const prepared = await transaction(client, (transactionClient) => prepareFullRepairBatch(
      transactionClient,
      { manifest },
    ));
    if (prepared.completed) return;
    queue = new Queue(queuesByRole.channelCrawl, {
      connection: redisOptions,
      defaultJobOptions,
    });

    let retryMode = options.retryFailed;
    let resetRetryCursor = options.retryFailed;
    do {
      const state = await loadFullRepairDispatchState(dbQuery, manifest);
      let dispatch = null;
      if (state.dispatch_status !== "dispatched" || retryMode) {
        dispatch = await dispatchFullRepairPass({
          dbQuery,
          queue,
          manifest,
          targets,
          preparedAt: state.prepared_at,
          dispatchCursor: resetRetryCursor ? 0 : state.dispatch_cursor,
          highWater: options.highWater,
          refill: options.refill,
          retryFailed: retryMode,
        });
        resetRetryCursor = false;
        if (dispatch.status === "dispatched") retryMode = false;
      }
      const completion = await loadFullRepairCompletionState(dbQuery, manifest.batch_id);
      const currentScheduler = await schedulerState(dbQuery);
      process.stdout.write(`${JSON.stringify({
        event: "publication_full_repair_tick",
        batch_id: manifest.batch_id,
        dispatch,
        completion,
        scheduler: {
          status: currentScheduler.status,
          pipeline_cycle_id: currentScheduler.pipeline_cycle_id ?? null,
          stop_reason: currentScheduler.stop_reason ?? null,
        },
      })}\n`);
      if (
        completion?.complete
        && currentScheduler.status === "stopped"
        && currentScheduler.stop_reason === "pipeline_complete"
      ) break;
      if (!options.watch || stopping) break;
      await sleep(options.pollMs);
    } while (!stopping);
  } finally {
    if (lockAcquired) {
      await client.query("SELECT pg_advisory_unlock(hashtext($1))", [lockName]).catch(() => {});
    }
    client.release();
    await Promise.allSettled([queue?.close(), closeDb()]);
  }
}

main().catch((error) => {
  process.stderr.write(`${JSON.stringify({
    event: "publication_full_repair_failed",
    code: error?.code ?? null,
    error: error?.message ?? String(error),
    details: error?.details ?? null,
  })}\n`);
  process.exitCode = 1;
});
