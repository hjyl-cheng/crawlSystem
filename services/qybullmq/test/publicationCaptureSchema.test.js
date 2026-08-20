import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  publicationCaptureApplyGuard,
  publicationCaptureSchemaBlock,
} from "../scripts/applyPublicationCaptureSchema.mjs";

test("Publication Capture controlled deployment contains only Phase C Revision and Outbox state", async () => {
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const block = publicationCaptureSchemaBlock(schema);

  assert.match(block, /CREATE TABLE IF NOT EXISTS publication\.revision/);
  assert.match(block, /CREATE TABLE IF NOT EXISTS publication\.outbox/);
  assert.match(block, /domain_current_current_revision_id_fkey/);
  assert.match(block, /domain_current_online_payload_check/);
  assert.match(
    block,
    /FOREIGN KEY \(\s*publication_stream_id, channel_id, domain, data_sequence, current_revision_id, result_hash\s*\)/,
  );
  assert.match(block, /guard_revision_immutable/);
  assert.match(block, /guard_outbox_lifecycle/);
  assert.match(block, /BEFORE UPDATE OR DELETE ON publication\.outbox/);
  assert.match(block, /writer_version_satisfies/);
  assert.match(block, /current_setting\('publication\.writer_version', true\)/);
  for (const table of ["channels", "contents", "agent_profiles", "finalized_profiles"]) {
    assert.match(
      block,
      new RegExp(`BEFORE INSERT OR UPDATE OR DELETE ON crawler\\.${table}`),
    );
  }
  assert.doesNotMatch(block, /publication\.baseline_manifest/);
  assert.doesNotMatch(block, /publication\.inbox/);
  assert.doesNotMatch(block, /result\./);
});

test("Publication Capture schema apply requires explicit database and Channel count confirmation", () => {
  const base = {
    CONFIRM_PUBLICATION_CAPTURE_SCHEMA_APPLY: "bullmq_crawler_isolated",
    EXPECTED_CRAWLER_CHANNEL_COUNT: "17",
  };

  assert.throws(() => publicationCaptureApplyGuard(base, ["node", "script"]), /--apply/);
  assert.throws(() => publicationCaptureApplyGuard({
    EXPECTED_CRAWLER_CHANNEL_COUNT: "17",
  }, ["node", "script", "--apply"]), /CONFIRM_PUBLICATION_CAPTURE_SCHEMA_APPLY/);
  for (const invalid of ["17junk", "17.0", "-1", "01", ""]) {
    assert.throws(() => publicationCaptureApplyGuard({
      ...base,
      EXPECTED_CRAWLER_CHANNEL_COUNT: invalid,
    }, ["node", "script", "--apply"]), /EXPECTED_CRAWLER_CHANNEL_COUNT/);
  }
  assert.deepEqual(
    publicationCaptureApplyGuard(base, ["node", "script", "--apply"]),
    { confirmedDatabase: "bullmq_crawler_isolated", expectedChannelCount: 17 },
  );
});

test("Publication Capture deployment uses a lock distinct from Baseline export", async () => {
  const captureScript = await readFile(
    new URL("../scripts/applyPublicationCaptureSchema.mjs", import.meta.url),
    "utf8",
  );
  const baselineScript = await readFile(
    new URL("../scripts/exportV16BaselineBundle.mjs", import.meta.url),
    "utf8",
  );

  assert.match(captureScript, /pg_advisory_xact_lock\(781137220\)/);
  assert.doesNotMatch(baselineScript, /EXPORT_LOCK_ID = 781137220/);
});

test("Publication Writer identity survives transaction-mode PgBouncer", async () => {
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const dbSource = await readFile(new URL("../src/db.js", import.meta.url), "utf8");

  assert.match(dbSource, /application_name: PUBLICATION_WRITER_VERSION/);
  assert.match(dbSource, /set_config\('publication\.writer_version',\$1,true\)/);
  assert.match(schema, /current_setting\('publication\.writer_version', true\)/);
  assert.match(schema, /current_setting\('application_name', true\)/);
});
