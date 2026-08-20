#!/usr/bin/env node

import process from "node:process";
import { Queue } from "bullmq";
import pg from "pg";

import { recoverStoredDataApiEvidence } from
  "../src/storedDataApiEvidenceRecovery.js";

const { Pool } = pg;

function positiveInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return parsed;
}

function parseArgs(argv) {
  const options = { apply: false, runId: null, expectedCount: null };
  for (const argument of argv) {
    if (argument === "--apply") options.apply = true;
    else if (argument.startsWith("--run-id=")) options.runId = argument.slice(9);
    else if (argument.startsWith("--expected-count=")) {
      options.expectedCount = positiveInteger(argument.slice(17), "expected-count");
    } else {
      throw new Error(`unknown argument: ${argument}`);
    }
  }
  options.runId = String(options.runId ?? "").trim();
  if (!options.runId) throw new TypeError("--run-id is required");
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
    application_name: "stored-data-api-public-access-replay-v1",
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

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const pool = new Pool(databaseConfig());
  const queue = new Queue("youtube-data-api-batch", { connection: redisConfig() });
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
    const result = await recoverStoredDataApiEvidence({
      query,
      withTransaction,
      queue,
      runId: options.runId,
      expectedCount: options.expectedCount,
      apply: options.apply,
    });
    console.log(JSON.stringify({
      event: "stored_data_api_evidence_recovery",
      ...result,
    }));
  } finally {
    await Promise.allSettled([queue.close(), pool.end()]);
  }
}

main().catch((error) => {
  console.error(JSON.stringify({
    event: "stored_data_api_evidence_recovery_failed",
    error: error?.stack || String(error),
  }));
  process.exitCode = 1;
});
