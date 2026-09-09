import assert from "node:assert/strict";
import test from "node:test";
import { DelayedError } from "bullmq";
import { waitForVideoApiDetail } from "../src/videoApiBatchRequests.js";
import { assertVideoApiNetworkAllowed, gateVideoApiJob, runVideoApiResumable,
  videoApiPendingError } from "../src/videoApiContinuation.js";
import { executeManagedWorkerAttempt } from "../src/managedWorkerExecution.js";
import { runChannelCandidateWorkerJobWithDurableSettlement } from "../src/channelCandidateWorkerLifecycle.js";

function jobFixture() {
  return { id: "job", queueName: "youtube-channel-crawl", attemptsMade: 2, attemptsStarted: 3,
    opts: { attempts: 3 }, data: { run_id: "run", candidate_id: 1, dispatch_generation: 1 },
    async updateData(data) { this.data = data; }, async updateProgress(progress) { this.progress = progress; },
    async moveToDelayed(time, token) { this.delayed = { time, token }; } };
}

test("pending API yields immediately even on the last attempt and does not settle a channel as failed or complete", async () => {
  const job = jobFixture();
  const query = async () => assert.fail("no terminal candidate settlement while waiting for API");
  const execute = async () => {
    const result = await executeManagedWorkerAttempt({ job,
      execute: () => waitForVideoApiDetail(async () => ({ rows: [{ status: "pending" }] }), "request"),
      persistRetryableCheckpoint: async () => assert.fail("API waiting must not switch a proxy") });
    assert.equal(result.businessState, "waiting_downstream");
    return result.result;
  };
  await assert.rejects(runChannelCandidateWorkerJobWithDurableSettlement({ query, job,
    execute: () => runVideoApiResumable({ job, token: "lock", execute }) }), DelayedError);
  assert.equal(job.data.video_api_continuation.request_id, "request");
  assert.equal(job.delayed.token, "lock");
  assert.equal(job.progress.stage, "waiting_video_api");
  assert.equal(job.attemptsMade, 2);
});

test("pending gate only reads durable state, while ready and failed results re-enter fenced processing", async () => {
  const job = jobFixture();
  job.data.video_api_continuation = { request_id: "request" };
  for (const status of ["pending", "done", "failed", "unavailable"]) {
    const call = () => gateVideoApiJob({ job, token: "lock",
      query: async () => ({ rows: [{ status, run_id: "run" }] }) });
    if (status === "pending") await assert.rejects(call(), DelayedError);
    else await call();
  }
  await assert.rejects(gateVideoApiJob({ job, query: async () => ({ rows: [{ run_id: "other" }] }) }), /identity conflicts/);
});

test("API completion replays without another network Task even with exhausted network budget", async () => {
  const job = jobFixture();
  job.data.video_api_continuation = { request_id: "request" };
  const value = await runVideoApiResumable({ job,
    execute: async () => assert.fail("no new Rota task needed for stored evidence"),
    executeReplay: () => waitForVideoApiDetail(async () => ({ rows: [{ status: "done", detail_json: { id: "video" } }] }), "request") });
  assert.equal(value.id, "video");
});

test("remaining network work must leave API replay and enter managed execution", async () => {
  const job = jobFixture();
  job.data.video_api_continuation = { request_id: "request" };
  let managed = 0;
  const result = await runVideoApiResumable({ job,
    executeReplay: async () => assertVideoApiNetworkAllowed(),
    execute: async () => { managed++; assertVideoApiNetworkAllowed(); return "managed"; } });
  assert.equal(result, "managed");
  assert.equal(managed, 1);
});

test("another pending video remains resumable; terminal API results and stale ownership still fail", async () => {
  const job = jobFixture();
  job.data.video_api_continuation = { request_id: "request" };
  await assert.rejects(runVideoApiResumable({ job, token: "lock",
    executeReplay: async () => { throw videoApiPendingError("next-request"); } }), DelayedError);
  assert.equal(job.data.video_api_continuation.request_id, "next-request");
  for (const status of ["unavailable", "failed"]) {
    await assert.rejects(runVideoApiResumable({ job,
      executeReplay: () => waitForVideoApiDetail(async () => ({ rows: [{ status }] }), "next-request") }),
    { code: "VIDEO_API_FALLBACK_UNRESOLVED" });
  }
  const stale = Object.assign(new Error("stale"), { code: "CONTENT_DETAIL_EXECUTION_FENCE_STALE" });
  await assert.rejects(runVideoApiResumable({ job, executeReplay: async () => { throw stale; } }), e => e === stale);
});
