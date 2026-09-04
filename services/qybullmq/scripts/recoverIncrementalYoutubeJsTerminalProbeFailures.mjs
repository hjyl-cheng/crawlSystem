#!/usr/bin/env node

import process from "node:process";
import { Queue } from "bullmq";
import pg from "pg";

import {
  INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_OPERATION_ID,
  recoverIncrementalYoutubeJsTerminalProbeFailures,
} from "../src/incrementalYoutubeJsTerminalProbeRecovery.js";

const { Pool } = pg;

function positiveInteger(value, field) {
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
    confirmation: null,
  };
  for (const argument of argv) {
    if (argument === "--apply") options.apply = true;
    else if (argument.startsWith("--plan-day=")) options.planDay = argument.slice(11);
    else if (argument.startsWith("--expected-count=")) {
      options.expectedCount = positiveInteger(argument.slice(17), "expected-count");
    } else if (argument.startsWith("--confirm-operation=")) {
      options.confirmation = argument.slice(20);
    } else {
      throw new Error(`unknown argument: ${argument}`);
    }
  }
  if (!options.planDay) throw new TypeError("--plan-day=YYYY-MM-DD is required");
  if (options.apply && options.expectedCount === null) {
    throw new TypeError("--expected-count is required with --apply");
  }
  if (options.apply && !options.confirmation) {
    throw new TypeError("--confirm-operation is required with --apply");
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
    application_name: INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_OPERATION_ID,
    max: 2,
  };
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const pool = new Pool(databaseConfig());
  const prefix = String(process.env.BULLMQ_PREFIX ?? "").trim() || undefined;
  const queue = new Queue("youtube-channel-incremental", {
    connection: {
      host: process.env.REDIS_HOST || "127.0.0.1",
      port: Number(process.env.REDIS_PORT || 6379),
      password: process.env.REDIS_PASSWORD || undefined,
      maxRetriesPerRequest: null,
    },
    ...(prefix ? { prefix } : {}),
  });
  try {
    const summary = await recoverIncrementalYoutubeJsTerminalProbeFailures({
      query: pool.query.bind(pool),
      queue,
      planDay: options.planDay,
      expectedCount: options.expectedCount,
      confirmation: options.confirmation,
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
    event: "incremental_youtubejs_terminal_probe_recovery_failed",
    operation_id: INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_OPERATION_ID,
    error: String(error?.message ?? error),
  }));
  process.exitCode = 1;
});
