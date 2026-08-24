import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { environmentValue } from "../src/runtimeEnvironment.js";

const { Client } = pg;

export const FIRST_SEEN_LEDGER_PENDING_INDEX =
  "idx_crawler_content_candidates_first_seen_ledger_pending";

export const CREATE_FIRST_SEEN_LEDGER_PENDING_INDEX_SQL = `
  CREATE INDEX CONCURRENTLY idx_crawler_content_candidates_first_seen_ledger_pending
  ON crawler.content_candidates (channel_id,candidate_id)
  WHERE first_seen_ledger_status='pending'`;

const SHAPE_CONSTRAINT = "content_candidates_first_seen_ledger_shape_check";
const OBSERVATION_FKEY = "content_candidates_first_seen_ledger_observation_id_fkey";
const MIGRATION_LOCK = "first-seen-ledger-online-schema-v1";
const EXPECTED_SHAPE_EXPRESSION = `
  (((first_seen_ledger_status='not_applicable')
      AND (first_seen_ledger_observation_id IS NULL))
    OR ((first_seen_ledger_status='pending')
      AND (first_seen_ledger_observation_id IS NULL))
    OR ((first_seen_ledger_status='consumed')
      AND (first_seen_ledger_observation_id IS NOT NULL)))`;

function directAdminDatabase(environment) {
  const databaseUrl = environmentValue("FIRST_SEEN_LEDGER_ADMIN_DATABASE_URL", { environment });
  let endpoint;
  try {
    endpoint = new URL(databaseUrl);
  } catch (error) {
    throw new TypeError("FIRST_SEEN_LEDGER_ADMIN_DATABASE_URL must be a PostgreSQL URL", {
      cause: error,
    });
  }
  if (!new Set(["postgres:", "postgresql:"]).has(endpoint.protocol)) {
    throw new TypeError(
      "FIRST_SEEN_LEDGER_ADMIN_DATABASE_URL must use postgres:// or postgresql://",
    );
  }
  if (!endpoint.hostname || !endpoint.pathname.slice(1)) {
    throw new TypeError(
      "FIRST_SEEN_LEDGER_ADMIN_DATABASE_URL must include a host and database",
    );
  }
  if (/pgbouncer/i.test(endpoint.hostname)) {
    throw new TypeError(
      "FIRST_SEEN_LEDGER_ADMIN_DATABASE_URL must target a direct PostgreSQL endpoint, not PgBouncer",
    );
  }
  return {
    databaseUrl,
    expectedServerPort: postgresPort(environment, endpoint.port || 5432),
  };
}

function nonnegativeInteger(environment, name) {
  const raw = String(environment[name] ?? "").trim();
  const value = Number(raw);
  assert.ok(
    /^(0|[1-9][0-9]*)$/.test(raw) && Number.isSafeInteger(value),
    `${name} must be an explicit non-negative integer`,
  );
  return value;
}

function postgresPort(environment, fallback) {
  const name = "EXPECTED_FIRST_SEEN_LEDGER_POSTGRES_SERVER_PORT";
  const raw = String(environment[name] ?? fallback).trim();
  const value = Number(raw);
  assert.ok(
    /^(?:[1-9][0-9]*)$/.test(raw)
      && Number.isSafeInteger(value)
      && value <= 65535,
    `${name} must be an explicit PostgreSQL server port`,
  );
  return value;
}

export function firstSeenLedgerOnlineSchemaApplyGuard(
  environment = process.env,
  argv = process.argv,
) {
  if (!argv.includes("--apply")) throw new Error("refusing to apply: pass --apply explicitly");
  const confirmedDatabase = String(
    environment.CONFIRM_FIRST_SEEN_LEDGER_SCHEMA_APPLY ?? "",
  ).trim();
  if (!confirmedDatabase) {
    throw new Error(
      "CONFIRM_FIRST_SEEN_LEDGER_SCHEMA_APPLY must equal the target database name",
    );
  }
  const directAdmin = directAdminDatabase(environment);
  return {
    ...directAdmin,
    confirmedDatabase,
    expectedMinimumCandidateRows: nonnegativeInteger(
      environment,
      "EXPECTED_CRAWLER_CANDIDATE_MIN_ROWS",
    ),
    expectedPendingCount: nonnegativeInteger(
      environment,
      "EXPECTED_FIRST_SEEN_LEDGER_PENDING_COUNT",
    ),
  };
}

function numericCount(row, name) {
  const value = Number(row?.[name]);
  assert.ok(Number.isSafeInteger(value) && value >= 0, `${name} is not a safe count`);
  return value;
}

async function preflight(client) {
  const identity = (await client.query(
    `SELECT current_database() AS database_name,
            to_regclass('crawler.content_candidates') IS NOT NULL AS candidates_ready`,
  )).rows[0] ?? {};
  assert.equal(identity.candidates_ready, true, "Crawler Content Candidates table is missing");
  const constraints = await constraintStates(client);
  assert.equal(
    constraints.length,
    2,
    "First-Seen ledger compatibility constraints are not installed",
  );
  assert.equal(
    constraintsHaveFinalShape(constraints, { requireValidated: false }),
    true,
    "First-Seen ledger compatibility constraints have the wrong definition",
  );
  const state = (await client.query(
    `SELECT count(*)::bigint AS candidate_count,
            count(*) FILTER (
              WHERE first_seen_ledger_status='pending'
            )::bigint AS pending_count,
            count(*) FILTER (
              WHERE NOT (
                (first_seen_ledger_status='not_applicable'
                  AND first_seen_ledger_observation_id IS NULL)
                OR (first_seen_ledger_status='pending'
                  AND first_seen_ledger_observation_id IS NULL)
                OR (first_seen_ledger_status='consumed'
                  AND first_seen_ledger_observation_id IS NOT NULL)
              )
            )::bigint AS invalid_shape_count,
            count(*) FILTER (
              WHERE first_seen_ledger_observation_id IS NOT NULL
                AND NOT EXISTS (
                  SELECT 1
                  FROM crawler.crawl_observations observation
                  WHERE observation.observation_id=
                    content_candidates.first_seen_ledger_observation_id
                )
            )::bigint AS orphan_observation_count
     FROM crawler.content_candidates`,
  )).rows[0] ?? {};
  return {
    databaseName: identity.database_name,
    candidateCount: numericCount(state, "candidate_count"),
    pendingCount: numericCount(state, "pending_count"),
    invalidShapeCount: numericCount(state, "invalid_shape_count"),
    orphanObservationCount: numericCount(state, "orphan_observation_count"),
  };
}

async function indexState(client) {
  return (await client.query(
    `SELECT index_state.indisvalid,index_state.indisready,index_state.indislive,
            index_state.indisunique,
            table_namespace.nspname AS table_schema,
            table_class.relname AS table_name,
            pg_get_expr(index_state.indpred,index_state.indrelid) AS predicate,
            ARRAY(
              SELECT attribute.attname
              FROM unnest(index_state.indkey) WITH ORDINALITY key(attnum,position)
              JOIN pg_attribute attribute
                ON attribute.attrelid=index_state.indrelid
               AND attribute.attnum=key.attnum
              WHERE key.position<=index_state.indnkeyatts
              ORDER BY key.position
            )::text[] AS key_columns
     FROM pg_class index_class
     JOIN pg_index index_state ON index_state.indexrelid=index_class.oid
     JOIN pg_class table_class ON table_class.oid=index_state.indrelid
     JOIN pg_namespace table_namespace ON table_namespace.oid=table_class.relnamespace
     WHERE index_class.oid=to_regclass($1)`,
    [`crawler.${FIRST_SEEN_LEDGER_PENDING_INDEX}`],
  )).rows[0] ?? null;
}

function normalizedSql(value) {
  return String(value ?? "")
    .toLowerCase()
    .replaceAll('"', "")
    .replace(/::(?:pg_catalog\.)?text/g, "")
    .replace(/\s/g, "");
}

function withoutRedundantOuterParentheses(value) {
  let expression = value;
  while (expression.startsWith("(") && expression.endsWith(")")) {
    let depth = 0;
    let wrapsWholeExpression = true;
    for (let index = 0; index < expression.length; index += 1) {
      if (expression[index] === "(") depth += 1;
      if (expression[index] === ")") depth -= 1;
      if (depth === 0 && index < expression.length - 1) {
        wrapsWholeExpression = false;
        break;
      }
    }
    if (!wrapsWholeExpression || depth !== 0) break;
    expression = expression.slice(1, -1);
  }
  return expression;
}

export function indexHasFinalShape(state) {
  return state?.indisvalid === true
    && state?.indisready === true
    && state?.indislive === true
    && state?.indisunique === false
    && state?.table_schema === "crawler"
    && state?.table_name === "content_candidates"
    && JSON.stringify(state?.key_columns) === JSON.stringify(["channel_id", "candidate_id"])
    && withoutRedundantOuterParentheses(normalizedSql(state?.predicate))
      === normalizedSql("first_seen_ledger_status='pending'");
}

function verifyIndex(state) {
  assert.equal(indexHasFinalShape(state), true, "First-Seen pending index has the wrong shape");
}

async function constraintStates(client) {
  return (await client.query(
    `SELECT constraint_state.conname AS constraint_name,
            constraint_state.contype AS constraint_type,
            constraint_state.convalidated AS validated,
            constraint_state.condeferrable AS deferrable,
            constraint_state.condeferred AS initially_deferred,
            constraint_state.confdeltype AS delete_action,
            constraint_state.confupdtype AS update_action,
            constraint_state.confmatchtype AS match_type,
            constraint_state.connoinherit AS no_inherit,
            pg_get_expr(
              constraint_state.conbin,constraint_state.conrelid,false
            ) AS check_expression,
            ARRAY(
              SELECT attribute.attname
              FROM unnest(constraint_state.conkey)
                WITH ORDINALITY key(attnum,position)
              JOIN pg_attribute attribute
                ON attribute.attrelid=constraint_state.conrelid
               AND attribute.attnum=key.attnum
              ORDER BY key.position
            )::text[] AS key_columns,
            referenced_namespace.nspname AS referenced_schema,
            referenced_table.relname AS referenced_table,
            ARRAY(
              SELECT attribute.attname
              FROM unnest(constraint_state.confkey)
                WITH ORDINALITY key(attnum,position)
              JOIN pg_attribute attribute
                ON attribute.attrelid=constraint_state.confrelid
               AND attribute.attnum=key.attnum
              ORDER BY key.position
            )::text[] AS referenced_columns
     FROM pg_constraint constraint_state
     LEFT JOIN pg_class referenced_table
       ON referenced_table.oid=constraint_state.confrelid
     LEFT JOIN pg_namespace referenced_namespace
       ON referenced_namespace.oid=referenced_table.relnamespace
     WHERE constraint_state.conrelid='crawler.content_candidates'::regclass
       AND constraint_state.conname IN ($1,$2)
     ORDER BY constraint_state.conname`,
    [SHAPE_CONSTRAINT, OBSERVATION_FKEY],
  )).rows;
}

export function constraintsHaveFinalShape(states, { requireValidated = true } = {}) {
  if (!Array.isArray(states) || states.length !== 2) return false;
  const byName = new Map(states.map((state) => [state.constraint_name, state]));
  const shape = byName.get(SHAPE_CONSTRAINT);
  const foreignKey = byName.get(OBSERVATION_FKEY);
  const validationMatches = (state) => !requireValidated || state?.validated === true;
  return shape?.constraint_type === "c"
    && validationMatches(shape)
    && shape?.no_inherit === false
    && normalizedSql(shape?.check_expression) === normalizedSql(EXPECTED_SHAPE_EXPRESSION)
    && foreignKey?.constraint_type === "f"
    && validationMatches(foreignKey)
    && JSON.stringify(foreignKey?.key_columns) === JSON.stringify([
      "first_seen_ledger_observation_id",
    ])
    && foreignKey?.referenced_schema === "crawler"
    && foreignKey?.referenced_table === "crawl_observations"
    && JSON.stringify(foreignKey?.referenced_columns) === JSON.stringify(["observation_id"])
    && foreignKey?.delete_action === "r"
    && foreignKey?.update_action === "a"
    && foreignKey?.match_type === "s"
    && foreignKey?.deferrable === true
    && foreignKey?.initially_deferred === true;
}

async function main() {
  const guard = firstSeenLedgerOnlineSchemaApplyGuard();
  const client = new Client({
    connectionString: guard.databaseUrl,
    application_name: MIGRATION_LOCK,
  });
  const startedAt = Date.now();
  let advisoryLock = false;
  try {
    await client.connect();
    const endpoint = (await client.query(
      `SELECT current_database() AS database_name,
              inet_server_port()::int AS server_port`,
    )).rows[0] ?? {};
    assert.equal(endpoint.database_name, guard.confirmedDatabase, "unexpected Crawler database");
    assert.equal(
      Number(endpoint.server_port),
      guard.expectedServerPort,
      "FIRST_SEEN_LEDGER_ADMIN_DATABASE_URL is not a direct PostgreSQL endpoint",
    );
    await client.query("SET TIME ZONE 'UTC'");
    await client.query("SET lock_timeout='5s'");
    await client.query("SET statement_timeout='1800s'");
    advisoryLock = (await client.query(
      "SELECT pg_try_advisory_lock(hashtext($1)) AS acquired",
      [MIGRATION_LOCK],
    )).rows[0]?.acquired === true;
    assert.equal(advisoryLock, true, "another First-Seen ledger migration is running");

    const before = await preflight(client);
    assert.equal(before.databaseName, guard.confirmedDatabase, "unexpected Crawler database");
    assert.ok(
      before.candidateCount >= guard.expectedMinimumCandidateRows,
      "Crawler Candidate count is below the confirmed minimum",
    );
    assert.equal(
      before.pendingCount,
      guard.expectedPendingCount,
      "unexpected pending First-Seen ledger count",
    );
    assert.equal(before.invalidShapeCount, 0, "invalid First-Seen ledger rows remain");
    assert.equal(before.orphanObservationCount, 0, "orphan First-Seen Observations remain");

    const existing = await indexState(client);
    if (existing && !indexHasFinalShape(existing)) {
      assert.equal(existing.table_schema, "crawler", "refusing to replace an index in another schema");
      assert.equal(
        existing.table_name,
        "content_candidates",
        "refusing to replace an index on another table",
      );
      await client.query(
        `DROP INDEX CONCURRENTLY IF EXISTS crawler.${FIRST_SEEN_LEDGER_PENDING_INDEX}`,
      );
    }
    if (!indexHasFinalShape(await indexState(client))) {
      await client.query(CREATE_FIRST_SEEN_LEDGER_PENDING_INDEX_SQL);
    }

    await client.query(
      `ALTER TABLE crawler.content_candidates
       VALIDATE CONSTRAINT content_candidates_first_seen_ledger_shape_check`,
    );
    await client.query(
      `ALTER TABLE crawler.content_candidates
       VALIDATE CONSTRAINT content_candidates_first_seen_ledger_observation_id_fkey`,
    );

    const after = await preflight(client);
    assert.ok(
      after.candidateCount >= guard.expectedMinimumCandidateRows,
      "Crawler Candidate count fell below the confirmed minimum",
    );
    assert.equal(after.pendingCount, guard.expectedPendingCount, "pending ledger rows changed");
    assert.equal(after.invalidShapeCount, 0, "invalid First-Seen ledger rows remain");
    assert.equal(after.orphanObservationCount, 0, "orphan First-Seen Observations remain");
    assert.equal(
      constraintsHaveFinalShape(await constraintStates(client)),
      true,
      "ledger constraints are not validated or have the wrong definition",
    );
    verifyIndex(await indexState(client));

    console.log(JSON.stringify({
      ok: true,
      database: before.databaseName,
      candidate_count_before: before.candidateCount,
      candidate_count_after: after.candidateCount,
      pending_count: before.pendingCount,
      constraints_validated: true,
      index: FIRST_SEEN_LEDGER_PENDING_INDEX,
      duration_ms: Date.now() - startedAt,
      migration: MIGRATION_LOCK,
    }));
  } finally {
    if (advisoryLock) {
      await client.query("SELECT pg_advisory_unlock(hashtext($1))", [MIGRATION_LOCK])
        .catch(() => {});
    }
    await client.end().catch(() => {});
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(error?.stack || String(error));
    process.exitCode = 1;
  });
}
