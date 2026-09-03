import assert from "node:assert/strict";
import test from "node:test";

import { allowDashboardRequestDuringControlledMigration } from "./controlledWritePolicy.js";

test("controlled migration permits reads and migration controls", () => {
  assert.equal(allowDashboardRequestDuringControlledMigration("GET", "/queries"), true);
  assert.equal(allowDashboardRequestDuringControlledMigration("POST", "/migration-channels/start"), true);
});

test("controlled migration permits YouTube API configuration", () => {
  assert.equal(allowDashboardRequestDuringControlledMigration("POST", "/youtube-api"), true);
});

test("controlled migration still rejects unrelated writes", () => {
  assert.equal(allowDashboardRequestDuringControlledMigration("POST", "/queries"), false);
  assert.equal(allowDashboardRequestDuringControlledMigration("DELETE", "/youtube-api"), false);
});
