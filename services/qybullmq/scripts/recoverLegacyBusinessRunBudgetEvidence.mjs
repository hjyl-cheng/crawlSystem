#!/usr/bin/env node

import process from "node:process";
import { Queue } from "bullmq";
import pg from "pg";

import { recoverLegacyBusinessRunBudgetEvidence } from
  "../src/legacyBusinessRunBudgetRecovery.js";

const { Pool } = pg;

function parseArgs(argv) {
  const options = { apply: false, runId: null, jobId: null };
  for (const argument of argv) {
    if (argument === "--apply") options.apply = true;
    else if (argument.startsWith("--run-id=")) options.runId = argument.slice(9);
    else if (argument.startsWith("--job-id=")) options.jobId = argument.slice(9);
    else throw new Error(`unknown argument: ${argument}`);
  }
  if (!String(options.runId ?? "").trim()) throw new TypeError("--run-id is required");
  if (!String(options.jobId ?? "").trim()) throw new TypeError("--job-id is required");
  return options;
}

function databaseConfig(environment = process.env) {
  return {
    host: environment.POSTGRES_HOST || "127.0.0.1",
    port: Number(environment.POSTGRES_PORT || 5432),
    user: environment.POSTGRES_USER || "bullmq",
    password: environment.POSTGRES_PASSWORD || "bullmq",
    database: environment.POSTGRES_DB || "bullmq_crawler",
    application_name: "legacy-business-run-budget-evidence-recovery-v1",
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
  const queue = new Queue("youtube-channel-crawl", { connection: redisConfig() });
  try {
    const result = await recoverLegacyBusinessRunBudgetEvidence({
      query: (sql, params) => pool.query(sql, params),
      queue,
      runId: options.runId,
      jobId: options.jobId,
      apply: options.apply,
    });
    console.log(JSON.stringify({
      event: "legacy_business_run_budget_evidence_recovery",
      ...result,
    }));
  } finally {
    await Promise.allSettled([queue.close(), pool.end()]);
  }
}

main().catch((error) => {
  console.error(JSON.stringify({
    event: "legacy_business_run_budget_evidence_recovery_failed",
    error: error?.stack || String(error),
  }));
  process.exitCode = 1;
});
