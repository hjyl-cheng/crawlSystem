import assert from "node:assert/strict";
import test from "node:test";

import {
  createCoalescedWakeup,
  DISCOVERY_PAGE_READY_CHANNEL,
  publishReadyDiscoveryPages,
} from "../src/discoveryPageWakeup.js";

test("publishes a controller wakeup only when a discovery page is ready", async () => {
  const published = [];
  const queue = {
    client: Promise.resolve({
      publish: async (channel, payload) => {
        published.push({ channel, payload: JSON.parse(payload) });
        return 1;
      },
    }),
  };
  const queries = [];
  const query = async (sql, params) => {
    queries.push({ sql, params });
    return { rows: [{ page_id: "query:7:run:test:page:3" }] };
  };

  const pageIds = await publishReadyDiscoveryPages({
    query,
    queue,
    candidateId: 42,
  });

  assert.deepEqual(pageIds, ["query:7:run:test:page:3"]);
  assert.deepEqual(queries[0].params, [42, null]);
  assert.match(queries[0].sql, /status IN \('discovered','queued','validating'\)/);
  assert.deepEqual(published, [{
    channel: DISCOVERY_PAGE_READY_CHANNEL,
    payload: {
      type: "discovery_page_qualification_ready",
      page_ids: ["query:7:run:test:page:3"],
    },
  }]);
});

test("does not publish when another snapshot is still pending", async () => {
  let publishCount = 0;
  const pageIds = await publishReadyDiscoveryPages({
    query: async () => ({ rows: [] }),
    queue: {
      client: Promise.resolve({
        publish: async () => {
          publishCount += 1;
        },
      }),
    },
    pageId: "query:7:run:test:page:3",
  });

  assert.deepEqual(pageIds, []);
  assert.equal(publishCount, 0);
});

test("controller wakeups coalesce and rerun when an event arrives during a tick", async () => {
  let calls = 0;
  let releaseFirst;
  let firstStartedResolve;
  const firstStarted = new Promise((resolve) => {
    firstStartedResolve = resolve;
  });
  const firstRelease = new Promise((resolve) => {
    releaseFirst = resolve;
  });
  let secondFinishedResolve;
  const secondFinished = new Promise((resolve) => {
    secondFinishedResolve = resolve;
  });
  const wakeup = createCoalescedWakeup(async () => {
    calls += 1;
    if (calls === 1) {
      firstStartedResolve();
      await firstRelease;
    }
    if (calls === 2) secondFinishedResolve();
  }, { delayMs: 1 });

  wakeup.request();
  wakeup.request();
  await firstStarted;
  assert.equal(calls, 1);

  wakeup.request();
  releaseFirst();
  await secondFinished;

  assert.equal(calls, 2);
  wakeup.close();
});
