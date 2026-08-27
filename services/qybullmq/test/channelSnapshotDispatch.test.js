import assert from "node:assert/strict";
import test from "node:test";

import {
  allocateChannelSnapshotDispatch,
  allocateDiscoveredChannelSnapshotDispatches,
} from "../src/channelSnapshotDispatch.js";

test("a missing first delivery allocates and persists generation one", async () => {
  const calls = [];
  const candidate = await allocateChannelSnapshotDispatch(async (sql, params) => {
    calls.push({ sql, params });
    return {
      rowCount: 1,
      rows: [{ candidate_id: "42", snapshot_dispatch_generation: "1", status: "queued" }],
    };
  }, { candidateId: 42, expectedGeneration: 0 });

  assert.equal(candidate.snapshot_dispatch_generation, 1);
  assert.match(calls[0].sql, /snapshot_dispatch_generation=\$2/);
  assert.deepEqual(calls[0].params, [42, 0]);
});

test("a missing prior delivery advances with a compare-and-set fence", async () => {
  const candidate = await allocateChannelSnapshotDispatch(async (_sql, params) => {
    assert.deepEqual(params, [42, 3]);
    return {
      rowCount: 1,
      rows: [{ candidate_id: "42", snapshot_dispatch_generation: "4", status: "queued" }],
    };
  }, { candidateId: 42, expectedGeneration: 3 });

  assert.equal(candidate.snapshot_dispatch_generation, 4);
});

test("Discover persists first generations before bulk queue delivery", async () => {
  const candidates = await allocateDiscoveredChannelSnapshotDispatches(async (sql, params) => {
    assert.match(sql, /status='discovered'/);
    assert.deepEqual(params, [[41, 42]]);
    return {
      rows: [
        { candidate_id: "41", snapshot_dispatch_generation: "1" },
        { candidate_id: "42", snapshot_dispatch_generation: "1" },
      ],
    };
  }, [41, 42]);

  assert.deepEqual(candidates, [
    { candidate_id: 41, snapshot_dispatch_generation: 1 },
    { candidate_id: 42, snapshot_dispatch_generation: 1 },
  ]);
});
