import assert from "node:assert/strict";
import test from "node:test";
import { recordCrawlerObservation } from "../src/crawlObservationStore.js";

function clientFixture() {
  const calls = [];
  return {
    calls,
    client: {
      async query(sql, params = []) {
        const statement = String(sql);
        calls.push({ sql: statement, params });
        if (statement.includes("INSERT INTO crawler.crawl_observation_keys")) {
          return { rowCount: 1, rows: [{ observation_id: params[1] }] };
        }
        if (statement.includes("FROM crawler.channel_domain_cursors")
            && statement.includes("FOR UPDATE")) {
          return { rowCount: 1, rows: [{ latest_sequence: 0 }] };
        }
        return { rowCount: 1, rows: [] };
      },
    },
  };
}

test("generic Observation writers lock the Channel FK before the domain cursor", async () => {
  const fixture = clientFixture();
  await recordCrawlerObservation(fixture.client, {
    idempotencyKey: "video:lock-order:test",
    observationKind: "video",
    channelId: "UClockOrder",
    runId: null,
    observedAt: "2026-08-24T00:00:00.000Z",
    triggerReason: "manual",
    prepare: async () => ({
      outcome: "complete",
      outcomeReasonCode: "lock_order_test",
      payload: {},
    }),
  });

  const keyClaim = fixture.calls.findIndex((call) => (
    call.sql.includes("INSERT INTO crawler.crawl_observation_keys")
  ));
  const cursorLock = fixture.calls.findIndex((call) => (
    call.sql.includes("FROM crawler.channel_domain_cursors")
      && call.sql.includes("FOR UPDATE")
  ));
  assert.ok(
    keyClaim >= 0 && cursorLock > keyClaim,
    "Channel FK lock must precede the cursor lock to match Migration writers",
  );
});
