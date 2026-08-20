import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { environmentValue } from "../src/runtimeEnvironment.js";

test("runtime environment reads direct and file-backed values", () => {
  assert.equal(environmentValue("SETTING", { environment: { SETTING: "direct" } }), "direct");
  const directory = mkdtempSync(join(tmpdir(), "feature-dispatch-env-"));
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
