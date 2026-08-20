import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { databaseUrl } from "../src/db.js";
import { environmentValue } from "../src/runtimeEnvironment.js";

test("runtime environment reads direct and file-backed values", () => {
  assert.equal(environmentValue("SETTING", { environment: { SETTING: "direct" } }), "direct");
  const directory = mkdtempSync(join(tmpdir(), "qy-crawler-env-"));
  const file = join(directory, "setting");
  writeFileSync(file, "from-file\n");
  assert.equal(environmentValue("SETTING", {
    environment: { SETTING_FILE: file },
  }), "from-file");
});

test("runtime environment rejects ambiguous and missing values", () => {
  assert.throws(() => environmentValue("SETTING", {
    environment: { SETTING: "direct", SETTING_FILE: "/tmp/setting" },
  }), /cannot both be set/);
  assert.throws(() => environmentValue("SETTING", { environment: {} }), /is required/);
  assert.equal(environmentValue("SETTING", {
    environment: {},
    required: false,
  }), null);
});

test("Crawler database URL supports a file-backed Compose secret", () => {
  const directory = mkdtempSync(join(tmpdir(), "qy-crawler-db-env-"));
  const file = join(directory, "database-url");
  writeFileSync(file, "postgres://crawler-secret\n");
  assert.equal(databaseUrl({ DATABASE_URL_FILE: file }), "postgres://crawler-secret");
  assert.equal(databaseUrl({
    POSTGRES_USER: "user",
    POSTGRES_PASSWORD: "password",
    POSTGRES_HOST: "postgres",
    POSTGRES_PORT: "5433",
    POSTGRES_DB: "crawler",
  }), "postgres://user:password@postgres:5433/crawler");
});
