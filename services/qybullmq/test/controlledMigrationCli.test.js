import assert from "node:assert/strict";
import test from "node:test";
import {
  controlledMigrationPlan,
  controlledMigrationUsage,
  parseControlledMigrationCommand,
} from "../scripts/dispatchMigrationChannels.js";

const environment = { MIGRATION_SOURCE_ID: "isolated-source-v1" };

test("controlled Migration CLI exposes only the approved canary sizes", () => {
  const usage = controlledMigrationUsage();
  assert.match(usage, /--selection <100\|200\|500\|1000\|2000>/);
  assert.doesNotMatch(usage, /source-batch-id|limit <count>|pilot-30/);
  assert.throws(
    () => parseControlledMigrationCommand(["--selection", "30"], environment),
    /must be one of: 100, 200, 500, 1000, 2000/,
  );
  assert.equal(parseControlledMigrationCommand(["--selection", "200"], environment).selection, "200");
  assert.equal(parseControlledMigrationCommand(["--selection", "500"], environment).selection, "500");
});

test("controlled Migration CLI plan is read-only and emits the exact confirmation", () => {
  const command = parseControlledMigrationCommand(["--selection", "100"], environment);
  assert.deepEqual(controlledMigrationPlan(command), {
    ok: true,
    mode: "plan",
    writes_performed: false,
    source_id: "isolated-source-v1",
    selection: "100",
    required_confirmation: "confirm-controlled-migration:isolated-source-v1:100",
  });
});

test("controlled Migration CLI rejects execute without the exact confirmation", () => {
  assert.throws(
    () => parseControlledMigrationCommand(["--selection", "100", "--execute"], environment),
    /--confirm must exactly match/,
  );
  const command = parseControlledMigrationCommand([
    "--selection", "100",
    "--execute",
    "--confirm", "confirm-controlled-migration:isolated-source-v1:100",
  ], environment);
  assert.equal(command.execute, true);
});
