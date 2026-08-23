import assert from "node:assert/strict";
import test from "node:test";
import { contentEnrichModeCommand } from "../scripts/manageContentEnrichMode.mjs";

test("Content Enrich mode administration is dry-run by default and rejects ambiguous commands", () => {
  assert.deepEqual(contentEnrichModeCommand(["status"]), {
    command: "status",
    apply: false,
    help: false,
  });
  assert.deepEqual(contentEnrichModeCommand(["queue"]), {
    command: "queue",
    apply: false,
    help: false,
  });
  assert.deepEqual(contentEnrichModeCommand(["clock", "--apply"]), {
    command: "clock",
    apply: true,
    help: false,
  });
  assert.throws(() => contentEnrichModeCommand(["enable"]), /status, queue, or clock/);
  assert.throws(() => contentEnrichModeCommand(["queue", "--force"]), /unknown option/);
});
