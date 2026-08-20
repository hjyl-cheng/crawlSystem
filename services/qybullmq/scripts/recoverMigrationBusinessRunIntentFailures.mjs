#!/usr/bin/env node

import process from "node:process";
import { Queue } from "bullmq";
import pg from "pg";

import { recoverMigrationBusinessRunIntentFailures } from "../src/migrationBusinessRunIntentRecovery.js";
import {
  MIGRATION_PROXY_CONTROL_PRESSURE_STATES,
  migrationProxyControlDispatchCapacity,
} from "../src/migrationProxyControlRecovery.js";

const { Pool } = pg;

function positiveInteger(value, field, { min = 1, max = 100_000_000 } = {}) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < min || parsed > max) {
    throw new TypeError(`${field} must be an integer between ${min} and ${max}`);
  }
  return parsed;
}

function parseArgs(argv) {
  const options = {
    apply: false,
    watch: false,
    batchId: null,
    candidateId: null,
    expectedCount: null,
    highWater: 20,
    pollMs: 15_000,
  };
  for (const argument of argv) {
    if (argument === "--apply") options.apply = true;
    else if (argument === "--watch") options.watch = true;
    else if (argument.startsWith("--batch-id=")) options.batchId = argument.slice(11);
    else if (argument.startsWith("--candidate-id=")) {
      options.candidateId = positiveInteger(argument.slice(15), "candidate-id");
    } else if (argument.startsWith("--expected-count=")) {
      options.expectedCount = positiveInteger(argument.slice(17), "expected-count");
    } else if (argument.startsWith("--high-water=")) {
      options.highWater = positiveInteger(argument.slice(13), "high-water", { max: 1000 });
    } else if (argument.startsWith("--poll-ms=")) {
      options.pollMs = positiveInteger(argument.slice(10), "poll-ms", {
        min: 1000,
        max: 300_000,
      });
    } else {
      throw new Error(`unknown argument: ${argument}`);
    }
  }
  options.batchId = String(options.batchId ?? "").trim();
  if (!options.batchId) throw new TypeError("--batch-id is required");
  if (options.watch && !options.apply) throw new TypeError("--watch requires --apply");
  if (options.apply && options.expectedCount === null) {
    throw new TypeError("--expected-count is required with --apply");
  }
  return options;
}

function databaseConfig(environment = process.env) {
  return {
    host: environment.POSTGRES_HOST || "127.0.0.1",
    port: Number(environment.POSTGRES_PORT || 5432),
    user: environment.POSTGRES_USER || "bullmq",
    password: environment.POSTGRES_PASSWORD || "bullmq",
    database: environment.POSTGRES_DB || "bullmq_crawler",
    application_name: "bug-048-migration-business-run-intent-compat-recovery-v1",
    max: 2,
  };
}

function redisConfig(environment = process.env) {
  return {
    host: environment.REDIS_HOST || "127.0.0.1",
    port: Number(environment.REDIS_PORT || 6379),
    password: environment.REDIS_PASSWORD || undefined,
    maxRetriesPerRequest: null,
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const pool = new Pool(databaseConfig());
  const queue = new Queue("youtube-channel-crawl", { connection: redisConfig() });
  const query = (sql, params) => pool.query(sql, params);
  const withTransaction = async (action) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await action(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  };

  try {
    do {
      const counts = await queue.getJobCounts(...MIGRATION_PROXY_CONTROL_PRESSURE_STATES);
      const { pressure, dispatchLimit } = migrationProxyControlDispatchCapacity(
        counts,
        options.highWater,
      );
      const summary = await recoverMigrationBusinessRunIntentFailures({
        query,
        withTransaction,
        queue,
        batchId: options.batchId,
        candidateId: options.candidateId,
        expectedCount: options.expectedCount,
        apply: options.apply,
        dispatchLimit,
      });
      console.log(JSON.stringify({
        event: "migration_business_run_intent_recovery",
        pressure,
        dispatch_limit: dispatchLimit,
        ...summary,
      }));
      if (!options.watch) break;
      const pending = Number(summary.action_counts.prepare_and_retry ?? 0)
        + Number(summary.action_counts.retry_prepared ?? 0)
        + Number(summary.action_counts.recovery_in_progress ?? 0);
      if (pending === 0) break;
      await sleep(options.pollMs);
    } while (true);
  } finally {
    await Promise.allSettled([queue.close(), pool.end()]);
  }
}

main().catch((error) => {
  console.error(JSON.stringify({
    event: "migration_business_run_intent_recovery_failed",
    error: error?.stack || String(error),
  }));
  process.exitCode = 1;
});
