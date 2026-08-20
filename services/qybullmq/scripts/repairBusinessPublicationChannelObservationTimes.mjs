import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { environmentValue } from "../src/runtimeEnvironment.js";

const { Client } = pg;
const REPAIR_LOCK_ID = 781137235;
const REPAIR_VERSION = "channel-observation-time-v2";

function explicitCount(environment, name) {
  const value = String(environment[name] ?? "").trim();
  const parsed = Number(value);
  assert.ok(
    /^(0|[1-9][0-9]*)$/.test(value) && Number.isSafeInteger(parsed),
    `${name} must be an explicit non-negative integer`,
  );
  return parsed;
}

export function businessPublicationChannelObservationTimeRepairGuard(
  environment = process.env,
  argv = process.argv,
) {
  if (!argv.includes("--apply")) throw new Error("refusing to repair: pass --apply explicitly");
  const confirmedDatabase = String(
    environment.CONFIRM_BUSINESS_CHANNEL_TIME_REPAIR ?? "",
  ).trim();
  if (!confirmedDatabase) {
    throw new Error("CONFIRM_BUSINESS_CHANNEL_TIME_REPAIR must equal the target database name");
  }
  const batchSize = Number(String(environment.BUSINESS_CHANNEL_TIME_REPAIR_BATCH_SIZE ?? "2000"));
  assert.ok(
    Number.isSafeInteger(batchSize) && batchSize >= 100 && batchSize <= 5000,
    "BUSINESS_CHANNEL_TIME_REPAIR_BATCH_SIZE must be between 100 and 5000",
  );
  return {
    confirmedDatabase,
    expectedChannelCount: explicitCount(environment, "EXPECTED_BUSINESS_CHANNEL_COUNT"),
    expectedSnapshotCount: explicitCount(environment, "EXPECTED_BUSINESS_SNAPSHOT_COUNT"),
    expectedSearchCount: explicitCount(environment, "EXPECTED_BUSINESS_SEARCH_COUNT"),
    batchSize,
    databaseUrl: environmentValue("BUSINESS_DATABASE_URL", { environment }),
  };
}

async function transaction(client, callback, { statementTimeout = "120s" } = {}) {
  await client.query("BEGIN");
  try {
    await client.query("SET LOCAL TIME ZONE 'UTC'");
    await client.query("SET LOCAL lock_timeout='10s'");
    await client.query(`SET LOCAL statement_timeout='${statementTimeout}'`);
    const result = await callback();
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

async function prepareRepair(client) {
  await transaction(client, async () => {
    await client.query(
      `ALTER TABLE public.channel_snapshots
       ADD COLUMN IF NOT EXISTS channel_observed_at TIMESTAMPTZ,
       ADD COLUMN IF NOT EXISTS subscriber_count_observed_at TIMESTAMPTZ,
       ADD COLUMN IF NOT EXISTS total_view_count_observed_at TIMESTAMPTZ,
       ADD COLUMN IF NOT EXISTS video_count_observed_at TIMESTAMPTZ`,
    );
    await client.query(
      `ALTER TABLE public.creator_search_current
       ADD COLUMN IF NOT EXISTS channel_observed_at TIMESTAMPTZ,
       ADD COLUMN IF NOT EXISTS subscribers_observed_at TIMESTAMPTZ,
       ADD COLUMN IF NOT EXISTS total_views_observed_at TIMESTAMPTZ,
       ADD COLUMN IF NOT EXISTS channel_video_count_observed_at TIMESTAMPTZ`,
    );
    await client.query(
      `CREATE TABLE IF NOT EXISTS publication.projection_snapshot_time_repair (
         snapshot_id TEXT PRIMARY KEY
           REFERENCES public.channel_snapshots(id) ON DELETE RESTRICT,
         channel_id TEXT NOT NULL,
         active_revision_id UUID
           REFERENCES publication.revision(revision_id) ON DELETE RESTRICT,
         snapshot_captured_at TIMESTAMPTZ NOT NULL,
         source_observed_at TIMESTAMPTZ NOT NULL,
         repair_version TEXT NOT NULL,
         repaired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
         CHECK (
           btrim(channel_id)<>''
           AND btrim(repair_version)<>''
           AND source_observed_at<=snapshot_captured_at
         )
       )`,
    );
  });
}

async function repairSnapshots(client) {
  return transaction(client, async () => {
    await client.query("DROP TABLE IF EXISTS pg_temp.business_projection_snapshot_times");
    await client.query(
      `CREATE TEMP TABLE business_projection_snapshot_times ON COMMIT DROP AS
       WITH RECURSIVE snapshot_chain AS (
         SELECT
           snapshot.id AS snapshot_id,
           snapshot.id AS source_snapshot_id,
           snapshot.channel_id,
           batch.source_kind AS snapshot_source_kind,
           batch.source_kind,
           snapshot.raw_channel,
           snapshot.captured_at AS snapshot_captured_at,
           snapshot.captured_at AS source_snapshot_captured_at,
           0 AS depth
         FROM public.channel_snapshots snapshot
         JOIN public.import_batches batch ON batch.id=snapshot.import_batch_id

         UNION ALL

         SELECT
           chain.snapshot_id,
           previous.id,
           chain.channel_id,
           chain.snapshot_source_kind,
           previous_batch.source_kind,
           previous.raw_channel,
           chain.snapshot_captured_at,
           previous.captured_at,
           chain.depth+1
         FROM snapshot_chain chain
         JOIN public.channel_snapshots previous
           ON previous.id=NULLIF(chain.raw_channel->>'carried_forward_from_snapshot_id','')
         JOIN public.import_batches previous_batch ON previous_batch.id=previous.import_batch_id
         WHERE chain.depth<100
           AND NULLIF(chain.raw_channel->>'active_revision_id','') IS NULL
           AND NOT (
             chain.raw_channel->>'adapter_version' IN (
               'business-publication-projection-v2',
               'business-publication-projection-v3'
             )
             AND NULLIF(chain.raw_channel->>'source_observed_at','') IS NOT NULL
           )
       ), resolved AS (
         SELECT DISTINCT ON (chain.snapshot_id)
           chain.snapshot_id,
           chain.channel_id,
           chain.snapshot_source_kind AS source_kind,
           revision.revision_id AS active_revision_id,
           chain.snapshot_captured_at,
           COALESCE(
             NULLIF(revision.source_json #>> '{complete_observation,observed_at}','')::timestamptz,
             CASE
               WHEN chain.raw_channel->>'adapter_version' IN (
                 'business-publication-projection-v2',
                 'business-publication-projection-v3'
               )
                 THEN NULLIF(chain.raw_channel->>'source_observed_at','')::timestamptz
             END,
             CASE
               WHEN chain.source_kind<>'publication_projection'
                 THEN chain.source_snapshot_captured_at
             END
           ) AS source_observed_at
         FROM snapshot_chain chain
         LEFT JOIN publication.revision revision
           ON revision.revision_id=NULLIF(chain.raw_channel->>'active_revision_id','')::uuid
          AND revision.channel_id=chain.channel_id
          AND revision.domain='channel'
         WHERE revision.revision_id IS NOT NULL
            OR (
              chain.raw_channel->>'adapter_version' IN (
                'business-publication-projection-v2',
                'business-publication-projection-v3'
              )
              AND NULLIF(chain.raw_channel->>'source_observed_at','') IS NOT NULL
            )
            OR chain.source_kind<>'publication_projection'
         ORDER BY chain.snapshot_id,chain.depth
       )
       SELECT * FROM resolved`,
    );
    const preflight = (await client.query(
      `SELECT
         (SELECT count(*)::int FROM public.channel_snapshots) AS snapshot_count,
         count(*)::int AS resolved_count,
         count(*) FILTER (
           WHERE source_observed_at IS NULL OR source_observed_at>snapshot_captured_at
         )::int AS invalid_count
       FROM business_projection_snapshot_times`,
    )).rows[0];
    assert.equal(Number(preflight.resolved_count), Number(preflight.snapshot_count),
      "Channel Snapshot observation time resolution is incomplete");
    assert.equal(Number(preflight.invalid_count), 0,
      "Channel Snapshot observation time is missing or later than its composite capture");

    const audit = await client.query(
      `INSERT INTO publication.projection_snapshot_time_repair (
         snapshot_id,channel_id,active_revision_id,snapshot_captured_at,
         source_observed_at,repair_version
       )
       SELECT snapshot_id,channel_id,active_revision_id,snapshot_captured_at,
              source_observed_at,$1
       FROM business_projection_snapshot_times
       WHERE source_kind='publication_projection'
       ON CONFLICT (snapshot_id) DO UPDATE
       SET channel_id=excluded.channel_id,
           active_revision_id=excluded.active_revision_id,
           snapshot_captured_at=excluded.snapshot_captured_at,
           source_observed_at=excluded.source_observed_at,
           repair_version=excluded.repair_version,
           repaired_at=now()
       WHERE ROW(
         publication.projection_snapshot_time_repair.channel_id,
         publication.projection_snapshot_time_repair.active_revision_id,
         publication.projection_snapshot_time_repair.snapshot_captured_at,
         publication.projection_snapshot_time_repair.source_observed_at,
         publication.projection_snapshot_time_repair.repair_version
       ) IS DISTINCT FROM ROW(
         excluded.channel_id,
         excluded.active_revision_id,
         excluded.snapshot_captured_at,
         excluded.source_observed_at,
         excluded.repair_version
       )`,
      [REPAIR_VERSION],
    );
    const snapshots = await client.query(
      `UPDATE public.channel_snapshots snapshot
       SET channel_observed_at=source.source_observed_at,
           subscriber_count_observed_at=CASE
             WHEN snapshot.subscriber_count IS NULL THEN NULL ELSE source.source_observed_at END,
           total_view_count_observed_at=CASE
             WHEN snapshot.total_view_count IS NULL THEN NULL ELSE source.source_observed_at END,
           video_count_observed_at=CASE
             WHEN snapshot.video_count IS NULL THEN NULL ELSE source.source_observed_at END,
           raw_channel=CASE
             WHEN source.source_kind='publication_projection' THEN
               COALESCE(snapshot.raw_channel,'{}'::jsonb)||jsonb_build_object(
                 'source_observed_at',source.source_observed_at,
                 'projected_at',source.snapshot_captured_at,
                 'timestamp_semantics','composite-capture-with-channel-observation-v2'
               )
             ELSE snapshot.raw_channel
           END
       FROM business_projection_snapshot_times source
       WHERE snapshot.id=source.snapshot_id
         AND (
           ROW(
             snapshot.channel_observed_at,
             snapshot.subscriber_count_observed_at,
             snapshot.total_view_count_observed_at,
             snapshot.video_count_observed_at
           ) IS DISTINCT FROM ROW(
             source.source_observed_at,
             CASE WHEN snapshot.subscriber_count IS NULL THEN NULL ELSE source.source_observed_at END,
             CASE WHEN snapshot.total_view_count IS NULL THEN NULL ELSE source.source_observed_at END,
             CASE WHEN snapshot.video_count IS NULL THEN NULL ELSE source.source_observed_at END
           )
           OR (
             source.source_kind='publication_projection'
             AND (
               NULLIF(snapshot.raw_channel->>'source_observed_at','')::timestamptz
                 IS DISTINCT FROM source.source_observed_at
               OR NULLIF(snapshot.raw_channel->>'projected_at','')::timestamptz
                 IS DISTINCT FROM source.snapshot_captured_at
               OR snapshot.raw_channel->>'timestamp_semantics'
                 IS DISTINCT FROM 'composite-capture-with-channel-observation-v2'
             )
           )
         )`,
    );
    return {
      snapshotCount: Number(preflight.snapshot_count),
      snapshotUpdatedCount: snapshots.rowCount,
      auditChangedCount: audit.rowCount,
    };
  });
}

async function repairSearch(client, { batchSize, onProgress }) {
  let lastWatermark = null;
  let lastChannelId = null;
  let scannedCount = 0;
  let updatedCount = 0;
  let batchCount = 0;
  while (true) {
    const state = await transaction(client, async () => (await client.query(
      `WITH selected AS MATERIALIZED (
         SELECT search.watermark,search.channel_id,
                snapshot.channel_observed_at,
                snapshot.subscriber_count_observed_at,
                snapshot.total_view_count_observed_at,
                snapshot.video_count_observed_at
         FROM public.creator_search_current search
         JOIN public.channel_snapshots snapshot ON snapshot.id=search.snapshot_id
         WHERE $1::text IS NULL
            OR ROW(search.watermark,search.channel_id)>ROW($1::text,$2::text)
         ORDER BY search.watermark,search.channel_id
         LIMIT $3
       ), updated AS (
         UPDATE public.creator_search_current search
         SET channel_observed_at=selected.channel_observed_at,
             subscribers_observed_at=selected.subscriber_count_observed_at,
             total_views_observed_at=selected.total_view_count_observed_at,
             channel_video_count_observed_at=selected.video_count_observed_at
         FROM selected
         WHERE search.watermark=selected.watermark
           AND search.channel_id=selected.channel_id
           AND ROW(
             search.channel_observed_at,
             search.subscribers_observed_at,
             search.total_views_observed_at,
             search.channel_video_count_observed_at
           ) IS DISTINCT FROM ROW(
             selected.channel_observed_at,
             selected.subscriber_count_observed_at,
             selected.total_view_count_observed_at,
             selected.video_count_observed_at
           )
         RETURNING 1
       )
       SELECT
         (SELECT count(*)::int FROM selected) AS scanned_count,
         (SELECT count(*)::int FROM updated) AS updated_count,
         (SELECT watermark FROM selected ORDER BY watermark DESC,channel_id DESC LIMIT 1)
           AS last_watermark,
         (SELECT channel_id FROM selected ORDER BY watermark DESC,channel_id DESC LIMIT 1)
           AS last_channel_id`,
      [lastWatermark, lastChannelId, batchSize],
    )).rows[0]);
    const scanned = Number(state.scanned_count);
    if (scanned === 0) break;
    scannedCount += scanned;
    updatedCount += Number(state.updated_count);
    batchCount += 1;
    lastWatermark = state.last_watermark;
    lastChannelId = state.last_channel_id;
    onProgress?.({ batchCount, scannedCount, updatedCount });
  }
  return { searchScannedCount: scannedCount, searchUpdatedCount: updatedCount, batchCount };
}

async function addAndValidateConstraints(client) {
  await transaction(client, async () => {
    await client.query(
      `DO $channel_snapshot_metric_time_constraint$
       BEGIN
         IF NOT EXISTS (
           SELECT 1 FROM pg_constraint
           WHERE conrelid='public.channel_snapshots'::regclass
             AND conname='channel_snapshot_metric_time_shape'
         ) THEN
           ALTER TABLE public.channel_snapshots
           ADD CONSTRAINT channel_snapshot_metric_time_shape CHECK (
             channel_observed_at IS NOT NULL
             AND channel_observed_at<=captured_at
             AND subscriber_count_observed_at IS NOT DISTINCT FROM
               CASE WHEN subscriber_count IS NULL THEN NULL ELSE channel_observed_at END
             AND total_view_count_observed_at IS NOT DISTINCT FROM
               CASE WHEN total_view_count IS NULL THEN NULL ELSE channel_observed_at END
             AND video_count_observed_at IS NOT DISTINCT FROM
               CASE WHEN video_count IS NULL THEN NULL ELSE channel_observed_at END
           ) NOT VALID;
         END IF;
       END
       $channel_snapshot_metric_time_constraint$`,
    );
    await client.query(
      `DO $creator_search_metric_time_constraint$
       BEGIN
         IF NOT EXISTS (
           SELECT 1 FROM pg_constraint
           WHERE conrelid='public.creator_search_current'::regclass
             AND conname='creator_search_metric_time_shape'
         ) THEN
           ALTER TABLE public.creator_search_current
           ADD CONSTRAINT creator_search_metric_time_shape CHECK (
             channel_observed_at IS NOT NULL
             AND channel_observed_at<=captured_at
             AND subscribers_observed_at IS NOT DISTINCT FROM
               CASE WHEN subscribers IS NULL THEN NULL ELSE channel_observed_at END
             AND total_views_observed_at IS NOT DISTINCT FROM
               CASE WHEN total_views IS NULL THEN NULL ELSE channel_observed_at END
             AND channel_video_count_observed_at IS NOT DISTINCT FROM
               CASE WHEN channel_video_count IS NULL THEN NULL ELSE channel_observed_at END
           ) NOT VALID;
         END IF;
       END
       $creator_search_metric_time_constraint$`,
    );
  });
  await transaction(client, () => client.query(
    "ALTER TABLE public.channel_snapshots VALIDATE CONSTRAINT channel_snapshot_metric_time_shape",
  ), { statementTimeout: "300s" });
  await transaction(client, () => client.query(
    "ALTER TABLE public.creator_search_current VALIDATE CONSTRAINT creator_search_metric_time_shape",
  ), { statementTimeout: "300s" });
  await transaction(client, async () => {
    await client.query(
      "ALTER TABLE public.channel_snapshots ALTER COLUMN channel_observed_at SET NOT NULL",
    );
    await client.query(
      "ALTER TABLE public.creator_search_current ALTER COLUMN channel_observed_at SET NOT NULL",
    );
  });
}

async function verifyRepair(client) {
  return (await client.query(
    `SELECT
       (SELECT count(*)::int
        FROM public.channel_snapshots snapshot
        WHERE snapshot.channel_observed_at IS NULL
           OR snapshot.channel_observed_at>snapshot.captured_at
           OR snapshot.subscriber_count_observed_at IS DISTINCT FROM
             CASE WHEN snapshot.subscriber_count IS NULL
               THEN NULL ELSE snapshot.channel_observed_at END
           OR snapshot.total_view_count_observed_at IS DISTINCT FROM
             CASE WHEN snapshot.total_view_count IS NULL
               THEN NULL ELSE snapshot.channel_observed_at END
           OR snapshot.video_count_observed_at IS DISTINCT FROM
             CASE WHEN snapshot.video_count IS NULL
               THEN NULL ELSE snapshot.channel_observed_at END) AS snapshot_error_count,
       (SELECT count(*)::int
        FROM public.creator_search_current search
        WHERE search.channel_observed_at IS NULL
           OR search.channel_observed_at>search.captured_at
           OR search.subscribers_observed_at IS DISTINCT FROM
             CASE WHEN search.subscribers IS NULL THEN NULL ELSE search.channel_observed_at END
           OR search.total_views_observed_at IS DISTINCT FROM
             CASE WHEN search.total_views IS NULL THEN NULL ELSE search.channel_observed_at END
           OR search.channel_video_count_observed_at IS DISTINCT FROM
             CASE WHEN search.channel_video_count IS NULL
               THEN NULL ELSE search.channel_observed_at END) AS search_error_count,
       (SELECT count(*)::int
        FROM publication.projection_snapshot_time_repair
        WHERE repair_version=$1) AS audit_count`,
    [REPAIR_VERSION],
  )).rows[0];
}

export async function repairBusinessPublicationChannelObservationTimes(client, options) {
  const onProgress = options.onProgress;
  await client.query("SET TIME ZONE 'UTC'");
  await prepareRepair(client);
  const snapshots = await repairSnapshots(client);
  const search = await repairSearch(client, { batchSize: options.batchSize, onProgress });
  assert.equal(search.searchScannedCount, options.expectedSearchCount,
    "unexpected Creator Search count during repair");
  await addAndValidateConstraints(client);
  const verified = await verifyRepair(client);
  assert.equal(Number(verified.snapshot_error_count), 0, "Channel Snapshot time invariant failed");
  assert.equal(Number(verified.search_error_count), 0, "Creator Search time invariant failed");
  return {
    ...snapshots,
    ...search,
    auditCount: Number(verified.audit_count),
  };
}

async function main() {
  const guard = businessPublicationChannelObservationTimeRepairGuard();
  const client = new Client({ connectionString: guard.databaseUrl });
  let repairLockHeld = false;
  let publishLockHeld = false;
  try {
    await client.connect();
    await client.query("SET TIME ZONE 'UTC'");
    await client.query("SELECT pg_advisory_lock($1)", [REPAIR_LOCK_ID]);
    repairLockHeld = true;
    await client.query("SELECT pg_advisory_lock(hashtext('kol_demo:creator-search-publish'))");
    publishLockHeld = true;
    const preflight = (await client.query(
      `SELECT current_database() AS database_name,
              current_setting('TimeZone') AS timezone,
              (SELECT count(*)::int FROM public.channels) AS channel_count,
              (SELECT count(*)::int FROM public.channel_snapshots) AS snapshot_count,
              (SELECT count(*)::int FROM public.creator_search_current) AS search_count`,
    )).rows[0];
    assert.equal(preflight.database_name, guard.confirmedDatabase, "unexpected Business database");
    assert.equal(preflight.timezone, "UTC", "repair session must use UTC");
    assert.equal(Number(preflight.channel_count), guard.expectedChannelCount,
      "unexpected Business Channel count");
    assert.equal(Number(preflight.snapshot_count), guard.expectedSnapshotCount,
      "unexpected Business Channel Snapshot count");
    assert.equal(Number(preflight.search_count), guard.expectedSearchCount,
      "unexpected Business Creator Search count");
    const result = await repairBusinessPublicationChannelObservationTimes(client, {
      batchSize: guard.batchSize,
      expectedSearchCount: guard.expectedSearchCount,
      onProgress(progress) {
        if (progress.batchCount % 10 === 0) {
          console.log(JSON.stringify({ phase: "creator-search-backfill", ...progress }));
        }
      },
    });
    console.log(JSON.stringify({
      ok: true,
      database: guard.confirmedDatabase,
      timezone: "UTC",
      repairVersion: REPAIR_VERSION,
      ...result,
    }));
  } finally {
    if (publishLockHeld) {
      await client.query(
        "SELECT pg_advisory_unlock(hashtext('kol_demo:creator-search-publish'))",
      ).catch(() => {});
    }
    if (repairLockHeld) {
      await client.query("SELECT pg_advisory_unlock($1)", [REPAIR_LOCK_ID]).catch(() => {});
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
