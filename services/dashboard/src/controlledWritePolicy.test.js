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

test("Query page scheduler controls reach the migration-fenced route handlers", () => {
  for (const action of ["draft", "start", "resume", "pause", "stop"]) {
    assert.equal(
      allowDashboardRequestDuringControlledMigration("POST", `/queries/scheduler/${action}`),
      true,
      `Query ${action} must reach its handler instead of returning HTTP 423`,
    );
  }
});

test("Query control exception does not permit arbitrary writes", () => {
  for (const path of ["/queries/import", "/queries/seed-due", "/queries/terms/delete", "/queries/scheduler/unknown", "/queries/scheduler/start/extra"]) {
    assert.equal(allowDashboardRequestDuringControlledMigration("POST", path), false);
  }
  assert.equal(allowDashboardRequestDuringControlledMigration("DELETE", "/queries/scheduler/start"), false);
});

test("controlled migration still rejects unrelated writes", () => {
  assert.equal(allowDashboardRequestDuringControlledMigration("POST", "/queries"), false);
  assert.equal(allowDashboardRequestDuringControlledMigration("DELETE", "/youtube-api"), false);
});
