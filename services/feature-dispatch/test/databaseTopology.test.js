import assert from "node:assert/strict";
import test from "node:test";
import { assertSharedFeatureDatabase } from "../src/databaseTopology.js";

function row(overrides = {}) {
  return {
    database_name: "bullmq_crawler_migration",
    database_user: "feature_user",
    outbox_ready: true,
    crawler_ready: true,
    channels_ready: true,
    crawler_channels_readable: false,
    timezone_utc: true,
    ...overrides,
  };
}

const expected = {
  expectedDatabase: "bullmq_crawler_migration",
  expectedUser: "feature_user",
};

test("shared database topology accepts a schema-only Feature role", () => {
  assert.doesNotThrow(() => assertSharedFeatureDatabase(row(), expected));
});

test("shared database topology rejects the legacy standalone database", () => {
  assert.throws(
    () => assertSharedFeatureDatabase(row({
      database_name: "feature_clock",
      crawler_ready: false,
      channels_ready: false,
    }), expected),
    /shared Crawler\/Feature database/,
  );
});

test("shared database topology rejects Crawler table read access", () => {
  assert.throws(
    () => assertSharedFeatureDatabase(row({ crawler_channels_readable: true }), expected),
    /shared Crawler\/Feature database/,
  );
});
