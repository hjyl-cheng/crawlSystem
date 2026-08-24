import { hostname } from "node:os";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { PostgresBusinessPublicationProjector } from "./businessPublicationProjector.js";
import { verifyBusinessWriterDatabase } from "./databaseIdentity.js";
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

export function businessPublicationProjectorRuntimeConfig(environment = process.env) {
  const expectedDatabase = String(environment.EXPECTED_BUSINESS_DATABASE || "").trim();
  if (!expectedDatabase) throw new TypeError("EXPECTED_BUSINESS_DATABASE is required");
  const retrySeconds = integerSetting(
    environment,
    "BUSINESS_PUBLICATION_PROJECT_RETRY_SECONDS",
    10,
    { minimum: 1, maximum: 3600 },
  );
  const maximumRetrySeconds = integerSetting(
    environment,
    "BUSINESS_PUBLICATION_PROJECT_MAX_RETRY_SECONDS",
    600,
    { minimum: 1, maximum: 86400 },
  );
  if (maximumRetrySeconds < retrySeconds) {
    throw new TypeError(
      "BUSINESS_PUBLICATION_PROJECT_MAX_RETRY_SECONDS must not be smaller than the base retry",
    );
  }
  const workerId = String(
    environment.BUSINESS_PUBLICATION_PROJECTOR_ID
      || `business-publication-projector:${hostname()}:${process.pid}`,
  ).trim();
  if (!workerId) throw new TypeError("BUSINESS_PUBLICATION_PROJECTOR_ID is required");
  return {
    databaseUrl: environmentValue("BUSINESS_DATABASE_URL", { environment }),
    expectedDatabase,
    expectedRole: String(
      environment.EXPECTED_BUSINESS_PROJECTOR_ROLE || "business_publication_projector",
    ).trim(),
    workerId,
    pollMs: integerSetting(environment, "BUSINESS_PUBLICATION_PROJECT_POLL_MS", 1000, {
      minimum: 100,
      maximum: 3600000,
    }),
    poolMaximum: integerSetting(environment, "BUSINESS_PROJECTOR_POSTGRES_POOL_MAX", 4, {
      minimum: 2,
      maximum: 20,
    }),
    batchSize: integerSetting(environment, "BUSINESS_PUBLICATION_PROJECT_BATCH_SIZE", 25, {
      minimum: 1,
      maximum: 250,
    }),
    claimStatementTimeoutMs: integerSetting(
      environment,
      "BUSINESS_PUBLICATION_PROJECT_CLAIM_TIMEOUT_MS",
      15000,
      { minimum: 1000, maximum: 300000 },
    ),
    leaseSeconds: integerSetting(environment, "BUSINESS_PUBLICATION_PROJECT_LEASE_SECONDS", 300, {
      minimum: 30,
      maximum: 3600,
    }),
    maximumAttempts: integerSetting(
      environment,
      "BUSINESS_PUBLICATION_PROJECT_MAX_ATTEMPTS",
      20,
      { minimum: 1, maximum: 100 },
    ),
    retrySeconds,
    maximumRetrySeconds,
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function assertBusinessPublicationDatabase(pool, config) {
  const result = await pool.query(
    `SELECT current_database() AS database_name,current_user AS role_name,
            to_regclass('publication.projection_batch') IS NOT NULL AS projection_schema_ready,
            to_regclass('publication.projection_outbox') IS NOT NULL AS outbox_ready,
            EXISTS (
              SELECT 1
              FROM pg_index index_state
              WHERE index_state.indexrelid=to_regclass(
                'publication.idx_business_publication_projection_predecessor'
              )
                AND index_state.indisvalid
                AND index_state.indisready
                AND index_state.indislive
            ) AS predecessor_index_ready,
            to_regclass('result.entity_current') IS NOT NULL AS current_ready,
            to_regclass('public.creator_search_live') IS NOT NULL AS search_live_ready,
            to_regclass('publication.creator_search_storage_state') IS NOT NULL
              AS search_storage_state_ready,
            to_regprocedure('public.refresh_creator_search_release_v9(text,text[],text[])')
              IS NOT NULL AS search_release_ready,
            to_regprocedure('public.restore_creator_search_live_from_legacy_v1(text)')
              IS NOT NULL AS search_legacy_restore_ready`,
  );
  const state = result.rows[0] ?? {};
  if (
    state.database_name !== config.expectedDatabase
      || state.role_name !== config.expectedRole
      || state.projection_schema_ready !== true
      || state.outbox_ready !== true
      || state.predecessor_index_ready !== true
      || state.current_ready !== true
      || state.search_live_ready !== true
      || state.search_storage_state_ready !== true
      || state.search_release_ready !== true
      || state.search_legacy_restore_ready !== true
  ) {
    throw new Error(
      `refusing to project unexpected, privileged, or unmigrated Business database: ${state.database_name}`,
    );
  }
}

async function main() {
  const config = businessPublicationProjectorRuntimeConfig();
  const pool = new Pool({ connectionString: config.databaseUrl, max: config.poolMaximum });
  let stopping = false;
  const stop = () => { stopping = true; };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    await verifyBusinessWriterDatabase(pool.query.bind(pool));
    await assertBusinessPublicationDatabase(pool, config);
    const projector = new PostgresBusinessPublicationProjector(pool, config);
    console.log(JSON.stringify({
      event: "business_publication_projector_ready",
      worker_id: config.workerId,
      database: config.expectedDatabase,
    }));
    while (!stopping) {
      try {
        const summary = await projector.runOnce();
        if (summary.claimed > 0) {
          const output = summary.outcome === "failed" ? console.error : console.log;
          output(JSON.stringify({
            event: summary.outcome === "failed"
              ? "business_publication_projection_failed"
              : "business_publication_projection",
            worker_id: config.workerId,
            ...summary,
          }));
        }
      } catch (error) {
        console.error(JSON.stringify({
          event: "business_publication_projector_iteration_failed",
          worker_id: config.workerId,
          error: error?.stack || String(error),
        }));
      }
      if (!stopping) await sleep(config.pollMs);
    }
  } finally {
    await pool.end().catch(() => {});
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(JSON.stringify({
      event: "business_publication_projector_fatal",
      error: error?.stack || String(error),
    }));
    process.exitCode = 1;
  });
}
