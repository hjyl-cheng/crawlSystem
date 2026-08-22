import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { databaseUrl } from "../src/databaseConnection.js";

const { Client } = pg;

function nonnegativeInteger(environment, name) {
  const raw = String(environment[name] ?? "").trim();
  const value = Number(raw);
  assert.ok(
    /^(0|[1-9][0-9]*)$/.test(raw) && Number.isSafeInteger(value),
    `${name} must be an explicit non-negative integer`,
  );
  return value;
}

export function videoDispositionSchemaBlock(schema) {
  const startMarker = "-- video-disposition-schema:start";
  const endMarker = "-- video-disposition-schema:end";
  const start = schema.indexOf(startMarker);
  const end = schema.indexOf(endMarker);
  assert.ok(start >= 0 && end > start, "Video disposition schema markers are missing");
  return schema.slice(start + startMarker.length, end);
}

export function videoDispositionSchemaApplyGuard(
  environment = process.env,
  argv = process.argv,
) {
  if (!argv.includes("--apply")) throw new Error("refusing to apply: pass --apply explicitly");
  const confirmedDatabase = String(
    environment.CONFIRM_VIDEO_DISPOSITION_SCHEMA_APPLY ?? "",
  ).trim();
  if (!confirmedDatabase) {
    throw new Error(
      "CONFIRM_VIDEO_DISPOSITION_SCHEMA_APPLY must equal the target database name",
    );
  }
  return {
    databaseUrl: databaseUrl(environment),
    confirmedDatabase,
    expectedCandidateCount: nonnegativeInteger(
      environment,
      "EXPECTED_CRAWLER_CANDIDATE_COUNT",
    ),
    expectedUndisposedCandidateCount: nonnegativeInteger(
      environment,
      "EXPECTED_UNDISPOSED_CANDIDATE_COUNT",
    ),
  };
}

async function preflight(client) {
  const state = (await client.query(
    `SELECT current_database() AS database_name,
            to_regclass('crawler.content_candidates') IS NOT NULL AS candidates_ready,
            EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='crawler' AND table_name='content_candidates'
                AND column_name='disposition' AND data_type='text'
            ) AS disposition_ready,
            EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='crawler' AND table_name='content_candidates'
                AND column_name='next_attempt_at' AND data_type='timestamp with time zone'
            ) AS next_attempt_at_ready`,
  )).rows[0] ?? {};
  assert.equal(state.candidates_ready, true, "Crawler Content Candidates table is missing");
  assert.equal(
    state.disposition_ready,
    state.next_attempt_at_ready,
    "Video disposition schema is only partially installed",
  );
  const candidateCount = Number((await client.query(
    "SELECT count(*)::bigint AS candidate_count FROM crawler.content_candidates",
  )).rows[0]?.candidate_count);
  const undisposedCount = state.disposition_ready
    ? Number((await client.query(
      `SELECT count(*)::bigint AS undisposed_count
       FROM crawler.content_candidates
       WHERE disposition IS NULL`,
    )).rows[0]?.undisposed_count)
    : candidateCount;
  return {
    databaseName: state.database_name,
    candidateCount,
    undisposedCount,
    schemaAlreadyPresent: state.disposition_ready === true,
  };
}

async function postflight(client) {
  return (await client.query(
    `SELECT
       count(*)::bigint AS candidate_count,
       count(*) FILTER (WHERE disposition IS NULL)::bigint AS undisposed_count,
       count(*) FILTER (
         WHERE disposition NOT IN ('stored','deferred','terminal_excluded')
       )::bigint AS invalid_kind_count,
       count(*) FILTER (
         WHERE NOT (
           (disposition='stored' AND next_attempt_at IS NULL)
           OR (
             disposition IN ('deferred','terminal_excluded')
             AND next_attempt_at IS NOT NULL
           )
         )
       )::bigint AS invalid_schedule_count,
       count(*) FILTER (
         WHERE result_json->'disposition' IS NULL
       )::bigint AS missing_evidence_count,
       (
         SELECT count(*)=2 AND bool_and(constraint_state.convalidated)
         FROM pg_constraint constraint_state
         WHERE constraint_state.conrelid='crawler.content_candidates'::regclass
           AND constraint_state.conname IN (
             'content_candidates_disposition_kind_check',
             'content_candidates_disposition_schedule_check'
           )
       ) AS constraints_validated,
       (
         SELECT count(*)=3
           AND bool_and(index_state.indisvalid)
           AND bool_and(index_state.indisready)
           AND bool_and(index_state.indislive)
         FROM pg_class index_class
         JOIN pg_index index_state ON index_state.indexrelid=index_class.oid
         WHERE index_class.oid IN (
           to_regclass('crawler.idx_crawler_content_candidates_disposition_due'),
           to_regclass('crawler.idx_crawler_content_candidates_channel_disposition_due'),
           to_regclass('crawler.idx_crawler_content_candidates_disposition_history')
         )
       ) AS indexes_ready
     FROM crawler.content_candidates`,
  )).rows[0] ?? {};
}

async function main() {
  const guard = videoDispositionSchemaApplyGuard();
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const ddl = videoDispositionSchemaBlock(schema);
  const client = new Client({
    connectionString: guard.databaseUrl,
    application_name: "video-disposition-schema-v1",
  });
  const startedAt = Date.now();
  let began = false;
  try {
    await client.connect();
    await client.query("BEGIN");
    began = true;
    await client.query("SET LOCAL TIME ZONE 'UTC'");
    await client.query("SET LOCAL lock_timeout='10s'");
    await client.query("SET LOCAL statement_timeout='1800s'");
    await client.query("SELECT pg_advisory_xact_lock(hashtext('video-disposition-schema-v1'))");
    await client.query(
      "LOCK TABLE crawler.content_candidates IN SHARE ROW EXCLUSIVE MODE",
    );

    const before = await preflight(client);
    assert.equal(before.databaseName, guard.confirmedDatabase, "unexpected Crawler database");
    assert.equal(
      before.candidateCount,
      guard.expectedCandidateCount,
      "unexpected Crawler Candidate count",
    );
    assert.equal(
      before.undisposedCount,
      guard.expectedUndisposedCandidateCount,
      "unexpected undisposed Candidate count",
    );

    await client.query(ddl);
    const after = await postflight(client);
    assert.equal(
      Number(after.candidate_count),
      before.candidateCount,
      "Candidate rows were added or lost",
    );
    assert.equal(Number(after.undisposed_count), 0, "undisposed Candidates remain");
    assert.equal(Number(after.invalid_kind_count), 0, "invalid Video dispositions remain");
    assert.equal(Number(after.invalid_schedule_count), 0, "invalid disposition schedules remain");
    assert.equal(Number(after.missing_evidence_count), 0, "disposition evidence is missing");
    assert.equal(after.constraints_validated, true, "disposition constraints are not validated");
    assert.equal(after.indexes_ready, true, "disposition indexes are not ready");

    await client.query("COMMIT");
    began = false;
    console.log(JSON.stringify({
      ok: true,
      database: before.databaseName,
      candidate_count: before.candidateCount,
      undisposed_count_before: before.undisposedCount,
      undisposed_count_after: Number(after.undisposed_count),
      schema_already_present: before.schemaAlreadyPresent,
      constraints_validated: after.constraints_validated,
      indexes_ready: after.indexes_ready,
      duration_ms: Date.now() - startedAt,
      migration: "video-disposition-schema-v1",
    }));
  } catch (error) {
    if (began) await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    await client.end().catch(() => {});
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(error?.stack || String(error));
    process.exitCode = 1;
  });
}
