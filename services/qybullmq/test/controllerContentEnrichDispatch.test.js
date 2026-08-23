import assert from "node:assert/strict";
import test from "node:test";
import { dispatchContentEnrichForController } from "../src/controllerContentEnrichDispatch.js";

test("Content Enrich dispatch failure is recorded without blocking later Controller work", async () => {
  const dispatchFailure = new Error("isolated Enrich database outage");
  const actions = [];
  const errors = [];
  let laterWorkRan = false;

  const result = await dispatchContentEnrichForController({
    dispatcher: {
      async dispatchAvailable() {
        throw dispatchFailure;
      },
    },
    actions,
    logger: { error: (entry) => errors.push(JSON.parse(entry)) },
  });
  laterWorkRan = true;

  assert.equal(result.ok, false);
  assert.equal(laterWorkRan, true);
  assert.deepEqual(actions, [{
    action: "dispatch-content-enrich-failed",
    error_message: "isolated Enrich database outage",
  }]);
  assert.deepEqual(errors, [{
    event: "content_enrich_dispatch_failed",
    error: "isolated Enrich database outage",
  }]);
});

test("successful Content Enrich dispatch preserves the Controller action summary", async () => {
  const actions = [];
  const summary = {
    enqueued: 2,
    recovered: 1,
    existing: 0,
    released: 0,
    failed: 0,
  };

  const result = await dispatchContentEnrichForController({
    dispatcher: { dispatchAvailable: async () => summary },
    actions,
  });

  assert.deepEqual(result, { ok: true, summary });
  assert.deepEqual(actions, [{ action: "dispatch-content-enrich", ...summary }]);
});
