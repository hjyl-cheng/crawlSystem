import assert from "node:assert/strict";
import test from "node:test";
import { retryFullCrawlSnapshotJob } from "../src/finalRepairJobRecovery.js";
import { YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT } from "../src/fullCrawlFetchContract.js";

function fixture() {
  const retries = [];
  const job = {
    id: "snapshot-g2", name: "channel-snapshot", attemptsStarted: 3, attemptsMade: 3,
    data: { run_id: "run:one", channel_id: "UCone", candidate_id: 12,
      dispatch_generation: 2, dispatch_batch_id: "batch-one",
      fetch_contract: YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT },
    getState: async () => "failed",
    retry: async (...args) => retries.push(args),
  };
  return { job, retries, queue: { getJob: async () => job },
    run: { run_id: "run:one", channel_id: "UCone", candidate_id: 12,
      result_json: { job_id: job.id, dispatch_batch_id: "batch-one",
        fetch_contract: YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT } },
    candidate: { snapshot_dispatch_generation: 2, snapshot_active_job_id: null,
      snapshot_active_job_attempt: null, status: "accepted" } };
}

test("Full recovery retains original g2 Job identity and monotonic started attempts across repair rounds", async () => {
  for (const round of [1, 2, 3]) {
    const f = fixture();
    const result = await retryFullCrawlSnapshotJob(f.queue, { run: f.run, candidate: f.candidate, round });
    assert.equal(result.job.id, "snapshot-g2");
    assert.equal(f.job.data.dispatch_generation, 2);
    assert.equal(f.job.attemptsStarted, 3);
    assert.deepEqual(f.retries, [["failed", { resetAttemptsMade: true }]]);
  }
});

test("Full recovery rejects superseded generations, different contracts and occupied candidate attempts", async () => {
  for (const mutate of [
    f => { f.candidate.snapshot_dispatch_generation = 3; },
    f => { f.job.data.run_id = "run:other"; },
    f => { f.job.data.fetch_contract = null; },
    f => { f.candidate.snapshot_active_job_id = "other"; },
  ]) {
    const f = fixture(); mutate(f);
    await assert.rejects(retryFullCrawlSnapshotJob(f.queue, f), /Snapshot recovery/);
    assert.equal(f.retries.length, 0);
  }
});

test("Full recovery does not requeue a represented original Job", async () => {
  const f = fixture(); f.job.getState = async () => "active";
  const result = await retryFullCrawlSnapshotJob(f.queue, f);
  assert.equal(result.action, "already_represented");
  assert.equal(f.retries.length, 0);
});
