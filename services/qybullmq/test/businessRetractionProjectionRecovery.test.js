import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  businessRetractionProjectionRecoveryGuard,
  RETRACTION_PROJECTION_TIME_ERROR,
} from "../scripts/recoverBusinessRetractionProjectionDeadLetters.mjs";

test("Retraction Projection recovery requires an exact database and count confirmation", () => {
  const environment = {
    CONFIRM_BUSINESS_RETRACTION_PROJECTION_RECOVERY: "yewu_business",
    EXPECTED_BUSINESS_RETRACTION_PROJECTION_DEAD_LETTERS: "446",
  };
  assert.throws(
    () => businessRetractionProjectionRecoveryGuard(environment, ["node", "script"]),
    /--apply/,
  );
  assert.deepEqual(
    businessRetractionProjectionRecoveryGuard(
      environment,
      ["node", "script", "--apply"],
    ),
    { confirmedDatabase: "yewu_business", expectedCount: 446 },
  );
});

test("Retraction Projection recovery selects only the exact terminal-time failure", async () => {
  const source = await readFile(
    new URL("../scripts/recoverBusinessRetractionProjectionDeadLetters.mjs", import.meta.url),
    "utf8",
  );
  assert.match(source, /revision\.revision_type='retraction'/);
  assert.match(source, /revision\.operation='retract_channel'/);
  assert.match(source, /terminal_channel,removed_at/);
  assert.match(source, /status='retry_wait',attempts=0/);
  assert.match(source, /pg_advisory_xact_lock/);
  assert.equal(
    RETRACTION_PROJECTION_TIME_ERROR,
    "projection_failed: revision.source_json.complete_observation.observed_at must be a timestamp",
  );
});
