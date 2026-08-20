import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { environmentValue } from "../src/runtimeEnvironment.js";

const { Client } = pg;

export const RETRACTION_PROJECTION_TIME_ERROR =
  "projection_failed: revision.source_json.complete_observation.observed_at must be a timestamp";

function explicitCount(environment, name) {
  const raw = String(environment[name] ?? "").trim();
  const value = Number(raw);
  assert.ok(
    /^(0|[1-9][0-9]*)$/.test(raw) && Number.isSafeInteger(value),
    `${name} must be an explicit non-negative integer`,
  );
  return value;
}

export function businessRetractionProjectionRecoveryGuard(
  environment = process.env,
  argv = process.argv,
) {
  if (!argv.includes("--apply")) throw new Error("refusing to recover: pass --apply explicitly");
  const confirmedDatabase = String(
    environment.CONFIRM_BUSINESS_RETRACTION_PROJECTION_RECOVERY ?? "",
  ).trim();
  if (!confirmedDatabase) {
    throw new Error(
      "CONFIRM_BUSINESS_RETRACTION_PROJECTION_RECOVERY must equal the target database name",
    );
  }
  return {
    confirmedDatabase,
    expectedCount: explicitCount(
      environment,
      "EXPECTED_BUSINESS_RETRACTION_PROJECTION_DEAD_LETTERS",
    ),
  };
}

async function recoveryState(client) {
  return (await client.query(
    `SELECT count(*)::int AS target_count,
            count(*) FILTER (
              WHERE revision.revision_type<>'retraction'
                 OR revision.operation<>'retract_channel'
                 OR revision.channel_id<>outbox.channel_id
                 OR NULLIF(
                      revision.source_json#>>'{terminal_channel,removed_at}',
                      ''
                    )::timestamptz IS NULL
            )::int AS invalid_count
     FROM publication.projection_outbox outbox
     LEFT JOIN publication.revision revision
       ON revision.revision_id=NULLIF(
            outbox.version_vector#>>'{channel,revision_id}',
            ''
          )::uuid
     WHERE outbox.status='dead_letter'
       AND outbox.last_error=$1`,
    [RETRACTION_PROJECTION_TIME_ERROR],
  )).rows[0];
}

async function main() {
  const apply = process.argv.includes("--apply");
  const guard = apply
    ? businessRetractionProjectionRecoveryGuard()
    : null;
  const client = new Client({
    connectionString: environmentValue("BUSINESS_DATABASE_URL"),
    application_name: "business-retraction-projection-recovery-v1",
  });
  try {
    await client.connect();
    await client.query("SET TIME ZONE 'UTC'");
    const database = (await client.query(
      "SELECT current_database() AS database_name",
    )).rows[0].database_name;
    const before = await recoveryState(client);
    if (!apply) {
      console.log(JSON.stringify({
        ok: true,
        mode: "plan",
        database,
        committed_writes: false,
        target_count: Number(before.target_count),
        invalid_count: Number(before.invalid_count),
      }));
      return;
    }

    assert.equal(database, guard.confirmedDatabase, "unexpected Business database");
    assert.equal(Number(before.target_count), guard.expectedCount, "unexpected recovery count");
    assert.equal(Number(before.invalid_count), 0, "recovery set contains a non-Retraction row");

    await client.query("BEGIN");
    try {
      await client.query("SET LOCAL lock_timeout='10s'");
      await client.query("SET LOCAL statement_timeout='120s'");
      await client.query("SELECT pg_advisory_xact_lock(781137236)");
      const recovered = await client.query(
        `WITH target AS MATERIALIZED (
           SELECT outbox.projection_id
           FROM publication.projection_outbox outbox
           JOIN publication.revision revision
             ON revision.revision_id=NULLIF(
                  outbox.version_vector#>>'{channel,revision_id}',
                  ''
                )::uuid
           WHERE outbox.status='dead_letter'
             AND outbox.last_error=$1
             AND revision.revision_type='retraction'
             AND revision.operation='retract_channel'
             AND revision.channel_id=outbox.channel_id
             AND NULLIF(
                   revision.source_json#>>'{terminal_channel,removed_at}',
                   ''
                 )::timestamptz IS NOT NULL
         )
         UPDATE publication.projection_outbox outbox
         SET status='retry_wait',attempts=0,next_attempt_at=now(),
             lease_owner=NULL,lease_expires_at=NULL,updated_at=now()
         FROM target
         WHERE outbox.projection_id=target.projection_id
         RETURNING outbox.projection_id`,
        [RETRACTION_PROJECTION_TIME_ERROR],
      );
      assert.equal(recovered.rowCount, guard.expectedCount, "recovery update count changed");
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    }

    const after = await recoveryState(client);
    assert.equal(Number(after.target_count), 0, "Retraction Projection Dead Letters remain");
    console.log(JSON.stringify({
      ok: true,
      mode: "apply",
      database,
      committed_writes: true,
      recovered_count: guard.expectedCount,
      remaining_count: Number(after.target_count),
    }));
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
