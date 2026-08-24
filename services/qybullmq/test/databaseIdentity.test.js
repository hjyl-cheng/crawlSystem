import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  assertBusinessWriterIdentity,
  assertCrawlerWriterIdentity,
  assertMigrationSourceIdentity,
  verifyBusinessWriterDatabase,
  verifyCrawlerWriterDatabase,
  verifyMigrationSourceDatabase,
} from "../src/databaseIdentity.js";

function writerIdentity(overrides = {}) {
  return {
    database_name: "newcrawler_crawler",
    database_user: "bullmq",
    transaction_read_only: "off",
    identity_kind: "crawler",
    identity_database: "newcrawler_crawler",
    schema_ready: true,
    ...overrides,
  };
}

test("Crawler Writer accepts only the expected initialized fresh database", () => {
  assert.deepEqual(
    assertCrawlerWriterIdentity(writerIdentity(), {
      expectedDatabase: "newcrawler_crawler",
      forbiddenDatabase: "bullmq_crawler_migration",
    }),
    { database: "newcrawler_crawler", user: "bullmq" },
  );

  assert.throws(
    () => assertCrawlerWriterIdentity(writerIdentity({
      database_name: "bullmq_crawler_migration",
      identity_database: "bullmq_crawler_migration",
    }), {
      expectedDatabase: "bullmq_crawler_migration",
      forbiddenDatabase: "bullmq_crawler_migration",
    }),
    /forbidden Crawler database bullmq_crawler_migration/,
  );
});

test("Business Writer refuses the legacy Business database even when misconfigured as expected", () => {
  assert.throws(
    () => assertBusinessWriterIdentity(writerIdentity({
      database_name: "yewu_business",
      database_user: "business_publication_ingress",
      identity_kind: "business",
      identity_database: "yewu_business",
    }), {
      expectedDatabase: "yewu_business",
      forbiddenDatabase: "yewu_business",
    }),
    /forbidden Business database yewu_business/,
  );
});

test("Migration Source requires the expected legacy identity in a read-only transaction", () => {
  const source = {
    database_name: "bullmq_crawler_migration",
    database_oid: "16384",
    database_user: "migration_reader",
    default_transaction_read_only: "on",
    transaction_read_only: "on",
    candidates_ready: true,
    channels_ready: true,
    candidate_write: false,
    channel_write: false,
  };
  assert.deepEqual(
    assertMigrationSourceIdentity(source, {
      expectedDatabase: "bullmq_crawler_migration",
      expectedDatabaseOid: "16384",
      expectedUser: "migration_reader",
      targetDatabase: "newcrawler_crawler",
    }),
    {
      database: "bullmq_crawler_migration",
      databaseOid: "16384",
      user: "migration_reader",
    },
  );
  assert.throws(
    () => assertMigrationSourceIdentity({ ...source, transaction_read_only: "off" }, {
      expectedDatabase: "bullmq_crawler_migration",
      expectedDatabaseOid: "16384",
      expectedUser: "migration_reader",
      targetDatabase: "newcrawler_crawler",
    }),
    /read-only transaction/,
  );
  assert.throws(
    () => assertMigrationSourceIdentity({ ...source, default_transaction_read_only: "off" }, {
      expectedDatabase: "bullmq_crawler_migration",
      expectedDatabaseOid: "16384",
      expectedUser: "migration_reader",
      targetDatabase: "newcrawler_crawler",
    }),
    /role must default to read-only/,
  );
  assert.throws(
    () => assertMigrationSourceIdentity({ ...source, candidate_write: true }, {
      expectedDatabase: "bullmq_crawler_migration",
      expectedDatabaseOid: "16384",
      expectedUser: "migration_reader",
      targetDatabase: "newcrawler_crawler",
    }),
    /must not have write privileges/,
  );
  assert.throws(
    () => assertMigrationSourceIdentity(source, {
      expectedDatabase: "bullmq_crawler_migration",
      expectedDatabaseOid: "16384",
      expectedUser: "migration_reader",
      targetDatabase: "bullmq_crawler_migration",
    }),
    /Source and Target databases must differ/,
  );
  assert.throws(
    () => assertMigrationSourceIdentity({ ...source, database_oid: "16385" }, {
      expectedDatabase: "bullmq_crawler_migration",
      expectedDatabaseOid: "16384",
      expectedUser: "migration_reader",
      targetDatabase: "newcrawler_crawler",
    }),
    /OID/,
  );
});

test("writer verification reads database identity before accepting a connection", async () => {
  const crawler = await verifyCrawlerWriterDatabase(async () => ({ rows: [writerIdentity()] }), {
    EXPECTED_CRAWLER_DATABASE: "newcrawler_crawler",
    FORBIDDEN_CRAWLER_DATABASE: "bullmq_crawler_migration",
  });
  assert.deepEqual(crawler, { database: "newcrawler_crawler", user: "bullmq" });

  const businessRow = writerIdentity({
    database_name: "newcrawler_business",
    database_user: "business_publication_ingress",
    identity_kind: "business",
    identity_database: "newcrawler_business",
  });
  const business = await verifyBusinessWriterDatabase(async () => ({ rows: [businessRow] }), {
    EXPECTED_BUSINESS_DATABASE: "newcrawler_business",
    FORBIDDEN_BUSINESS_DATABASE: "yewu_business",
  });
  assert.deepEqual(business, {
    database: "newcrawler_business",
    user: "business_publication_ingress",
  });

  const migration = await verifyMigrationSourceDatabase(async () => ({ rows: [{
    database_name: "bullmq_crawler_migration",
    database_oid: "16384",
    database_user: "migration_reader",
    default_transaction_read_only: "on",
    transaction_read_only: "on",
    candidates_ready: true,
    channels_ready: true,
    candidate_write: false,
    channel_write: false,
  }] }), {
    EXPECTED_MIGRATION_DATABASE: "bullmq_crawler_migration",
    EXPECTED_MIGRATION_DATABASE_OID: "16384",
    EXPECTED_MIGRATION_DATABASE_USER: "migration_reader",
    EXPECTED_CRAWLER_DATABASE: "newcrawler_crawler",
  });
  assert.deepEqual(migration, {
    database: "bullmq_crawler_migration",
    databaseOid: "16384",
    user: "migration_reader",
  });
});

test("fresh bootstrap records immutable Crawler and Business database identities", async () => {
  const [crawler, business, runtime] = await Promise.all([
    readFile(new URL("../../../database/bootstrap/crawler.sql", import.meta.url), "utf8"),
    readFile(new URL("../../../database/bootstrap/business.sql", import.meta.url), "utf8"),
    readFile(new URL("../src/db.js", import.meta.url), "utf8"),
  ]);
  assert.match(crawler, /CREATE TABLE crawler\.database_identity/);
  assert.match(crawler, /'crawler', current_database\(\)/);
  assert.match(business, /CREATE TABLE publication\.database_identity/);
  assert.match(business, /'business', current_database\(\)/);
  assert.match(runtime, /verifyCrawlerWriterDatabase/);
  assert.ok(
    runtime.indexOf("verifyCrawlerWriterDatabase") < runtime.indexOf("runSchemaMigration(schema)"),
    "Crawler identity must be verified before runtime schema migration",
  );
});

test("fresh Business bootstrap seeds an explicit zero-row Creator Search baseline", async () => {
  const [bootstrap, incremental] = await Promise.all([
    readFile(new URL("../../../database/bootstrap/business.sql", import.meta.url), "utf8"),
    readFile(new URL("../src/businessCreatorSearchIncrementalSchema.sql", import.meta.url), "utf8"),
  ]);
  for (const schema of [bootstrap, incremental]) {
    assert.match(schema, /fresh-business-empty-v1/);
    assert.match(schema, /INSERT INTO public\.import_batches/);
    assert.match(schema, /INSERT INTO public\.creator_search_active/);
    assert.match(schema, /INSERT INTO publication\.creator_search_storage_state/);
    assert.match(schema, /initialized_row_count[^;]*0/s);
  }
});

test("fresh Business schemas seed the content taxonomy required by Projection", async () => {
  const [bootstrap, projectionSchema] = await Promise.all([
    readFile(new URL("../../../database/bootstrap/business.sql", import.meta.url), "utf8"),
    readFile(new URL("../src/businessPublicationProjectionSchema.sql", import.meta.url), "utf8"),
  ]);
  for (const schema of [bootstrap, projectionSchema]) {
    assert.match(schema, /INSERT INTO public\.content_type_taxonomy/);
    assert.match(schema, /\('live'\s*,\s*'lives'\s*,\s*1\)/);
    assert.match(schema, /\('short'\s*,\s*'shorts'\s*,\s*2\)/);
    assert.match(schema, /\('video'\s*,\s*'videos'\s*,\s*3\)/);
    assert.match(schema, /ON CONFLICT \(source_content_type,content_kind\) DO UPDATE/);
  }
});

test("fresh Crawler bootstrap owns idempotent immutable Migration intents", async () => {
  const [runtime, bootstrap, dispatch] = await Promise.all([
    readFile(new URL("../src/schema.sql", import.meta.url), "utf8"),
    readFile(new URL("../../../database/bootstrap/crawler.sql", import.meta.url), "utf8"),
    readFile(new URL("../src/manualMigrationDispatch.js", import.meta.url), "utf8"),
  ]);
  for (const schema of [runtime, bootstrap]) {
    assert.match(schema, /CREATE TABLE(?: IF NOT EXISTS)? crawler\.migration_channel_intents/);
    assert.match(schema, /UNIQUE \(source_id, channel_id\)/);
    assert.match(schema, /UNIQUE \(source_id, source_candidate_id\)/);
    assert.match(schema, /prevent_migration_intent_source_update/);
  }
  assert.doesNotMatch(dispatch, /UPDATE crawler\.channel_candidates[\s\S]*source_json->>'source'='legacy_results_db'/);
  assert.match(dispatch, /loadMigrationSourceChannel/);
});

test("every Business writer verifies the immutable Business identity marker", async () => {
  const runtimes = await Promise.all([
    "runBusinessPublicationIngress.js",
    "runBusinessPublicationReconciler.js",
    "runBusinessPublicationProjector.js",
  ].map((name) => readFile(new URL(`../src/${name}`, import.meta.url), "utf8")));
  for (const runtime of runtimes) {
    assert.match(runtime, /verifyBusinessWriterDatabase/);
    assert.ok(
      runtime.indexOf("verifyBusinessWriterDatabase")
        < runtime.lastIndexOf("refusing to"),
      "Business identity marker must be verified before relation-specific preflight",
    );
  }
});
