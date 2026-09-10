import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  businessPublicationChannelObservationTimeRepairGuard,
} from "../scripts/repairBusinessPublicationChannelObservationTimes.mjs";
import { businessPublicationProjectionSchemaApplyGuard } from "../scripts/applyBusinessPublicationProjectionSchema.mjs";
import {
  businessCreatorSearchIncrementalSchemaApplyGuard,
} from "../scripts/applyBusinessCreatorSearchIncrementalSchema.mjs";
import { businessPublicationCutoverCommand } from "../scripts/manageBusinessPublicationCutover.mjs";
import { businessPublicationCutoverEvidenceFromDocument } from "../src/businessPublicationCutoverAdmin.js";

test("Projection schema adds controlled batches, cutover audit, and Search removal support", async () => {
  const schema = await readFile(
    new URL("../src/businessPublicationProjectionSchema.sql", import.meta.url),
    "utf8",
  );
  assert.match(schema, /'publication_projection'/);
  assert.match(schema, /CREATE TABLE IF NOT EXISTS publication\.projection_batch \(/);
  assert.match(schema, /CREATE TABLE IF NOT EXISTS publication\.projection_batch_item \(/);
  assert.match(schema, /CREATE TABLE IF NOT EXISTS publication\.projection_cutover \(/);
  assert.match(schema, /released_by_cutover_id/);
  assert.match(schema, /last_projection_watermark/);
  assert.match(schema, /rollback_json/);
  assert.match(schema, /refresh_creator_search_release_v8/);
  assert.match(schema, /p_removed_channel_ids TEXT\[\]/);
  assert.match(schema, /DELETE FROM public\.creator_search_current/);
  assert.match(schema, /is_verified_status/);
  assert.match(schema, /verified_status/);
  assert.match(schema, /channel_observed_at/);
  assert.match(schema, /subscriber_count_observed_at/);
  assert.match(schema, /total_view_count_observed_at/);
  assert.match(schema, /video_count_observed_at/);
  assert.match(schema, /subscribers_observed_at/);
  assert.match(schema, /total_views_observed_at/);
  assert.match(schema, /channel_video_count_observed_at/);
  assert.match(schema, /youtube_business_email_available/);
  assert.match(schema, /youtube_business_email_observed_at/);
  assert.match(schema, /channel_snapshots_youtube_business_email_shape/);
  assert.match(schema, /creator_search_youtube_business_email_shape/);
  assert.match(schema, /accepted_contract_versions SET DEFAULT ARRAY\[1,2\]/);
  assert.match(schema, /business-publication-projection-v2/);
  assert.match(schema, /business-publication-projection-v3/);
  assert.match(schema, /business-publication-projection-v4/);
  assert.match(schema, /projection_snapshot_time_repair/);
  assert.match(schema, /channel_snapshot_metric_time_shape/);
  assert.match(schema, /WITH RECURSIVE snapshot_chain/);
  assert.match(schema, /channel-observation-time-v2/);
  assert.match(schema, /IS DISTINCT FROM ROW/);
  assert.match(schema, /VALIDATE CONSTRAINT creator_search_metric_time_shape/);
  assert.match(schema, /normalize_creator_search_channel_observation_times/);
});

test("Runtime and fresh Business schemas allow unlisted Content snapshots", async () => {
  const [runtime, bootstrap] = await Promise.all([
    readFile(
      new URL("../src/businessPublicationProjectionSchema.sql", import.meta.url),
      "utf8",
    ),
    readFile(
      new URL("../../../database/bootstrap/business.sql", import.meta.url),
      "utf8",
    ),
  ]);

  assert.match(
    runtime,
    /access_status IN \('public','unlisted','login_required','members_only','unavailable','unknown'\)/,
  );
  assert.match(
    bootstrap,
    /CONSTRAINT content_snapshots_access_shape CHECK \(\(\(access_status = ANY \(ARRAY\['public'::text, 'unlisted'::text,/,
  );
  // This is the live raw import contract. Its unlisted migration has not been
  // deployed; public Content snapshots already support unlisted as checked above.
  assert.match(
    bootstrap,
    /CONSTRAINT raw_contents_v4_shape CHECK \([^\n]*access_status = ANY \(ARRAY\['public'::text, 'login_required'::text, 'members_only'::text, 'unavailable'::text, 'unknown'::text\]/,
  );
});

test("Channel Observation time repair recognizes both Projection adapter generations", async () => {
  const repair = await readFile(
    new URL("../scripts/repairBusinessPublicationChannelObservationTimes.mjs", import.meta.url),
    "utf8",
  );
  assert.match(repair, /business-publication-projection-v2/);
  assert.match(repair, /business-publication-projection-v3/);
  assert.match(repair, /business-publication-projection-v4/);
});

test("Creator Search incremental schema keeps current state, exact changes, and guarded lifecycle", async () => {
  const schema = await readFile(
    new URL("../src/businessCreatorSearchIncrementalSchema.sql", import.meta.url),
    "utf8",
  );
  assert.match(schema, /CREATE TABLE IF NOT EXISTS public\.creator_search_live/);
  assert.match(schema, /PRIMARY KEY \(channel_id\)/);
  assert.match(schema, /CREATE TABLE IF NOT EXISTS publication\.creator_search_changes/);
  assert.match(schema, /before_document JSONB/);
  assert.match(schema, /after_document JSONB/);
  assert.match(schema, /refresh_creator_search_release_v9/);
  assert.match(schema, /rollback_creator_search_release_v9/);
  assert.match(schema, /replay_creator_search_release_v9/);
  assert.match(schema, /activate_creator_search_incremental_v1/);
  assert.match(schema, /restore_creator_search_live_from_legacy_v1/);
  assert.match(schema, /prune_creator_search_legacy_history_v1/);
  assert.match(schema, /rollback_creator_search_to_watermark_v1/);
  assert.match(schema, /rollback_creator_search_incremental_storage_v1/);
  assert.match(schema, /write_mode IN \('shadow','incremental'\)/);
  assert.match(schema, /CHECK \(write_mode='shadow' OR read_mode='live'\)/);
  assert.match(schema, /idx_creator_search_live_text/);
  assert.match(
    schema,
    /GRANT SELECT,INSERT,UPDATE,DELETE ON public\.creator_search_active\s+TO business_publication_projector/,
  );
  assert.match(schema, /REVOKE ALL ON FUNCTION public\.activate_creator_search_incremental_v1/);
});

test("Projection Cutover command keeps plan, apply, and rollback explicit", () => {
  assert.deepEqual(businessPublicationCutoverCommand([]), {
    help: false,
    apply: false,
    rollback: false,
    output: null,
  });
  assert.deepEqual(
    businessPublicationCutoverCommand(["--rollback", "--apply", "--output", "result.json"]),
    { help: false, apply: true, rollback: true, output: "result.json" },
  );
  assert.throws(
    () => businessPublicationCutoverCommand(["--rollback", "--rollback"]),
    /may only be provided once/,
  );
});

test("Projection Cutover Apply accepts only the immutable evidence emitted by its Plan", () => {
  const evidence = { evidence_format: "business-publication-projection-cutover-evidence-v1" };
  assert.equal(businessPublicationCutoverEvidenceFromDocument(evidence), evidence);
  assert.equal(businessPublicationCutoverEvidenceFromDocument({
    ok: true,
    mode: "plan",
    writes_performed: false,
    dry_run_transaction: "rolled_back",
    evidence,
  }), evidence);
  assert.throws(
    () => businessPublicationCutoverEvidenceFromDocument({
      ok: true,
      mode: "apply",
      writes_performed: true,
      dry_run_transaction: "committed",
      evidence,
    }),
    /not an immutable dry-run/,
  );
});

test("Projection schema apply guard requires explicit target identity and count", () => {
  const environment = {
    BUSINESS_DATABASE_URL: "postgres://business/db",
    CONFIRM_BUSINESS_PUBLICATION_PROJECTION_SCHEMA_APPLY: "business_test",
    EXPECTED_BUSINESS_CHANNEL_COUNT: "19",
  };
  assert.deepEqual(
    businessPublicationProjectionSchemaApplyGuard(environment, ["node", "script", "--apply"]),
    {
      confirmedDatabase: "business_test",
      expectedChannelCount: 19,
      databaseUrl: "postgres://business/db",
    },
  );
  assert.throws(
    () => businessPublicationProjectionSchemaApplyGuard(environment, ["node", "script"]),
    /pass --apply explicitly/,
  );
  assert.throws(
    () => businessPublicationProjectionSchemaApplyGuard({
      ...environment,
      EXPECTED_BUSINESS_CHANNEL_COUNT: "all",
    }, ["node", "script", "--apply"]),
    /explicit non-negative integer/,
  );
});

test("Creator Search incremental apply guard pins target and active row counts", () => {
  const environment = {
    BUSINESS_DATABASE_URL: "postgres://business/db",
    CONFIRM_BUSINESS_CREATOR_SEARCH_INCREMENTAL_SCHEMA_APPLY: "business_test",
    EXPECTED_BUSINESS_CHANNEL_COUNT: "1654",
    EXPECTED_BUSINESS_ACTIVE_SEARCH_COUNT: "1563",
  };
  assert.deepEqual(
    businessCreatorSearchIncrementalSchemaApplyGuard(
      environment,
      ["node", "script", "--apply"],
    ),
    {
      confirmedDatabase: "business_test",
      expectedChannelCount: 1654,
      expectedActiveSearchCount: 1563,
      databaseUrl: "postgres://business/db",
    },
  );
  assert.throws(
    () => businessCreatorSearchIncrementalSchemaApplyGuard(environment, ["node", "script"]),
    /pass --apply explicitly/,
  );
  assert.throws(
    () => businessCreatorSearchIncrementalSchemaApplyGuard({
      ...environment,
      EXPECTED_BUSINESS_ACTIVE_SEARCH_COUNT: "all",
    }, ["node", "script", "--apply"]),
    /EXPECTED_BUSINESS_ACTIVE_SEARCH_COUNT/,
  );
});

test("Channel Observation time repair guard pins the target and every mutable row count", () => {
  const environment = {
    BUSINESS_DATABASE_URL: "postgres://business/db",
    CONFIRM_BUSINESS_CHANNEL_TIME_REPAIR: "business_test",
    EXPECTED_BUSINESS_CHANNEL_COUNT: "19",
    EXPECTED_BUSINESS_SNAPSHOT_COUNT: "81",
    EXPECTED_BUSINESS_SEARCH_COUNT: "143436",
    BUSINESS_CHANNEL_TIME_REPAIR_BATCH_SIZE: "1000",
  };
  assert.deepEqual(
    businessPublicationChannelObservationTimeRepairGuard(
      environment,
      ["node", "script", "--apply"],
    ),
    {
      confirmedDatabase: "business_test",
      expectedChannelCount: 19,
      expectedSnapshotCount: 81,
      expectedSearchCount: 143436,
      batchSize: 1000,
      databaseUrl: "postgres://business/db",
    },
  );
  assert.throws(
    () => businessPublicationChannelObservationTimeRepairGuard(environment, ["node", "script"]),
    /pass --apply explicitly/,
  );
  assert.throws(
    () => businessPublicationChannelObservationTimeRepairGuard({
      ...environment,
      EXPECTED_BUSINESS_SEARCH_COUNT: "all",
    }, ["node", "script", "--apply"]),
    /EXPECTED_BUSINESS_SEARCH_COUNT/,
  );
});
