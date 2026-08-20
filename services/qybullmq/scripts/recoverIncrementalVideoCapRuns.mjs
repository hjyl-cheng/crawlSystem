#!/usr/bin/env node

import process from "node:process";
import { Queue } from "bullmq";
import pg from "pg";

import {
  BUG035_RECOVERY_OPERATION_ID,
  recoverBug035IncrementalVideoRuns,
} from "../src/incrementalVideoCapRecovery.js";

const { Pool } = pg;

function parsePositiveInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return parsed;
}

function parseArgs(argv) {
  const options = {
    apply: false,
    planDay: null,
    expectedCount: null,
  };
  for (const argument of argv) {
    if (argument === "--apply") options.apply = true;
    else if (argument.startsWith("--plan-day=")) options.planDay = argument.slice(11);
    else if (argument.startsWith("--expected-count=")) {
      options.expectedCount = parsePositiveInteger(argument.slice(17), "expected-count");
    } else {
      throw new Error(`unknown argument: ${argument}`);
    }
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(options.planDay ?? ""))) {
    throw new TypeError("--plan-day=YYYY-MM-DD is required");
  }
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
    application_name: BUG035_RECOVERY_OPERATION_ID,
    max: 2,
  };
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const pool = new Pool(databaseConfig());
  const queue = new Queue("youtube-channel-incremental", {
    connection: {
      host: process.env.REDIS_HOST || "127.0.0.1",
      port: Number(process.env.REDIS_PORT || 6379),
      maxRetriesPerRequest: null,
    },
  });
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
    const summary = await recoverBug035IncrementalVideoRuns({
      query: pool.query.bind(pool),
      withTransaction,
      queue,
      planDay: options.planDay,
      expectedCount: options.expectedCount,
      apply: options.apply,
    });
    console.log(JSON.stringify(summary, null, 2));
    if (summary.dispatch_errors.length > 0) process.exitCode = 2;
  } finally {
    await queue.close();
    await pool.end();
  }
}

main().catch((error) => {
  console.error(JSON.stringify({
    event: "bug035_incremental_video_recovery_failed",
    operation_id: BUG035_RECOVERY_OPERATION_ID,
    error: String(error?.message ?? error),
  }));
  process.exitCode = 1;
});
