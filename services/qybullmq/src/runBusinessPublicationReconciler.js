import { hostname } from "node:os";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { PostgresBusinessPublicationAuditor } from "./businessPublicationAuditor.js";
import { PostgresBusinessPublicationReconciler } from "./businessPublicationReconciler.js";
import { environmentValue } from "./runtimeEnvironment.js";

const { Pool } = pg;

function integerSetting(environment, name, fallback, { minimum, maximum }) {
  const raw = String(environment[name] ?? "").trim();
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new TypeError(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

export function businessPublicationReconcilerRuntimeConfig(environment = process.env) {
  const expectedDatabase = String(environment.EXPECTED_BUSINESS_DATABASE || "yewu_business").trim();
  if (!expectedDatabase) throw new TypeError("EXPECTED_BUSINESS_DATABASE is required");
  const concurrency = integerSetting(
    environment,
    "BUSINESS_PUBLICATION_RECONCILE_CONCURRENCY",
    4,
    { minimum: 1, maximum: 64 },
  );
  const workerId = String(
    environment.BUSINESS_PUBLICATION_RECONCILER_ID
    || `business-publication-reconciler:${hostname()}:${process.pid}`,
  ).trim();
  if (!workerId) throw new TypeError("BUSINESS_PUBLICATION_RECONCILER_ID is required");
  const errorRetrySeconds = integerSetting(
    environment,
    "BUSINESS_PUBLICATION_RECONCILE_ERROR_RETRY_SECONDS",
    10,
    { minimum: 1, maximum: 3600 },
  );
  const maximumErrorRetrySeconds = integerSetting(
    environment,
    "BUSINESS_PUBLICATION_RECONCILE_MAX_ERROR_RETRY_SECONDS",
    300,
    { minimum: 1, maximum: 86400 },
  );
  if (maximumErrorRetrySeconds < errorRetrySeconds) {
    throw new TypeError(
      "BUSINESS_PUBLICATION_RECONCILE_MAX_ERROR_RETRY_SECONDS must not be smaller than the base retry",
    );
  }
  const auditErrorRetrySeconds = integerSetting(
    environment,
    "BUSINESS_PUBLICATION_AUDIT_ERROR_RETRY_SECONDS",
    30,
    { minimum: 1, maximum: 3600 },
  );
  const auditMaximumErrorRetrySeconds = integerSetting(
    environment,
    "BUSINESS_PUBLICATION_AUDIT_MAX_ERROR_RETRY_SECONDS",
    300,
    { minimum: 1, maximum: 86400 },
  );
  if (auditMaximumErrorRetrySeconds < auditErrorRetrySeconds) {
    throw new TypeError(
      "BUSINESS_PUBLICATION_AUDIT_MAX_ERROR_RETRY_SECONDS must not be smaller than the base retry",
    );
  }
  return {
    databaseUrl: environmentValue("BUSINESS_DATABASE_URL", { environment }),
    expectedDatabase,
    workerId,
    pollMs: integerSetting(environment, "BUSINESS_PUBLICATION_RECONCILE_POLL_MS", 1000, {
      minimum: 100,
      maximum: 3600000,
    }),
    poolMaximum: integerSetting(
      environment,
      "BUSINESS_RECONCILER_POSTGRES_POOL_MAX",
      Math.max(12, concurrency + 2),
      { minimum: 2, maximum: 100 },
    ),
    auditPoolMaximum: 1,
    batchSize: integerSetting(
      environment,
      "BUSINESS_PUBLICATION_RECONCILE_BATCH_SIZE",
      100,
      { minimum: 1, maximum: 5000 },
    ),
    concurrency,
    leaseSeconds: integerSetting(
      environment,
      "BUSINESS_PUBLICATION_RECONCILE_LEASE_SECONDS",
      120,
      { minimum: 5, maximum: 3600 },
    ),
    blockedRetrySeconds: integerSetting(
      environment,
      "BUSINESS_PUBLICATION_RECONCILE_BLOCKED_RETRY_SECONDS",
      30,
      { minimum: 1, maximum: 86400 },
    ),
    errorRetrySeconds,
    maximumErrorRetrySeconds,
    gapAlertSeconds: integerSetting(
      environment,
      "BUSINESS_PUBLICATION_GAP_ALERT_SECONDS",
      900,
      { minimum: 0, maximum: 2592000 },
    ),
    projectionStuckSeconds: integerSetting(
      environment,
      "BUSINESS_PUBLICATION_PROJECTION_STUCK_SECONDS",
      300,
      { minimum: 0, maximum: 2592000 },
    ),
    auditIntervalSeconds: integerSetting(
      environment,
      "BUSINESS_PUBLICATION_RECONCILE_AUDIT_INTERVAL_SECONDS",
      300,
      { minimum: 0, maximum: 86400 },
    ),
    auditSampleSize: integerSetting(
      environment,
      "BUSINESS_PUBLICATION_RECONCILE_AUDIT_SAMPLE_SIZE",
      10,
      { minimum: 1, maximum: 100 },
    ),
    auditErrorRetrySeconds,
    auditMaximumErrorRetrySeconds,
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function assertBusinessPublicationDatabase(pool, expectedDatabase) {
  const result = await pool.query(
    `SELECT current_database() AS database_name,
            to_regclass('publication.reconciliation_state') IS NOT NULL AS reconciliation_ready,
            to_regclass('publication.consumer_cursor') IS NOT NULL AS activation_ready,
            to_regclass('result.entity_current') IS NOT NULL AS current_ready`,
  );
  const state = result.rows[0] ?? {};
  if (
    state.database_name !== expectedDatabase
    || state.reconciliation_ready !== true
    || state.activation_ready !== true
    || state.current_ready !== true
  ) {
    throw new Error(
      `refusing to reconcile unexpected or unmigrated Business database: ${state.database_name}`,
    );
  }
}

async function main() {
  const config = businessPublicationReconcilerRuntimeConfig();
  const pool = new Pool({
    connectionString: config.databaseUrl,
    max: config.poolMaximum,
    application_name: "business-publication-reconciler",
  });
  const auditPool = new Pool({
    connectionString: config.databaseUrl,
    max: config.auditPoolMaximum,
    application_name: "business-publication-auditor",
    options: "-c max_parallel_workers_per_gather=0 -c debug_parallel_query=off",
  });
  let stopping = false;
  const stop = () => { stopping = true; };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    await assertBusinessPublicationDatabase(pool, config.expectedDatabase);
    const auditor = new PostgresBusinessPublicationAuditor(auditPool, {
      gapAlertSeconds: config.gapAlertSeconds,
      projectionStuckSeconds: config.projectionStuckSeconds,
      auditIntervalSeconds: config.auditIntervalSeconds,
      auditSampleSize: config.auditSampleSize,
      errorRetrySeconds: config.auditErrorRetrySeconds,
      maximumErrorRetrySeconds: config.auditMaximumErrorRetrySeconds,
    });
    const reconciler = new PostgresBusinessPublicationReconciler(pool, {
      auditor,
      workerId: config.workerId,
      batchSize: config.batchSize,
      concurrency: config.concurrency,
      leaseSeconds: config.leaseSeconds,
      blockedRetrySeconds: config.blockedRetrySeconds,
      errorRetrySeconds: config.errorRetrySeconds,
      maximumErrorRetrySeconds: config.maximumErrorRetrySeconds,
    });
    console.log(JSON.stringify({
      event: "business_publication_reconciler_ready",
      worker_id: config.workerId,
      database: config.expectedDatabase,
      audit_pool_maximum: config.auditPoolMaximum,
      audit_parallel_workers_per_gather: 0,
      audit_debug_parallel_query: "off",
    }));
    while (!stopping) {
      try {
        const summary = await reconciler.runOnce();
        if (summary.claimed > 0 || summary.audit.performed) {
          console.log(JSON.stringify({
            event: "business_publication_reconciliation",
            worker_id: config.workerId,
            ...summary,
          }));
        }
      } catch (error) {
        console.error(JSON.stringify({
          event: "business_publication_reconciliation_failed",
          worker_id: config.workerId,
          error: error?.stack || String(error),
        }));
      }
      if (!stopping) await sleep(config.pollMs);
    }
  } finally {
    await Promise.all([
      pool.end().catch(() => {}),
      auditPool.end().catch(() => {}),
    ]);
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(JSON.stringify({
      event: "business_publication_reconciler_fatal",
      error: error?.stack || String(error),
    }));
    process.exitCode = 1;
  });
}
