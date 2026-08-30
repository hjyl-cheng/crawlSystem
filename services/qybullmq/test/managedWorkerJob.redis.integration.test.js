import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { Queue, QueueEvents, Worker } from "bullmq";

import {
  clearChannelCandidateJobAttempt,
  markChannelCandidateJobAttemptActive,
  recordChannelCandidateJobFailure,
} from "../src/managedWorkerJob.js";

const redisUrl = String(process.env.MANAGED_JOB_TEST_REDIS_URL ?? "").trim();

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function within(promise, label, timeoutMs = 10_000) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function redisConnection(urlText) {
  const url = new URL(urlText);
  return {
    host: url.hostname,
    port: Number(url.port),
    password: url.password || undefined,
    maxRetriesPerRequest: null,
  };
}

function candidateQuery(candidate) {
  return async (sql, params) => {
    if (sql.includes("SET snapshot_active_job_id=$2")) {
      const [candidateId, jobId, activeAttempt, dispatchGeneration] = params;
      const eligible = candidate.candidateId === candidateId
        && candidate.dispatchGeneration === dispatchGeneration
        && (
          candidate.activeJobId === null
          || (candidate.activeJobId === jobId && candidate.activeAttempt <= activeAttempt)
        );
      if (!eligible) return { rowCount: 0, rows: [] };
      candidate.activeJobId = jobId;
      candidate.activeAttempt = activeAttempt;
      return { rowCount: 1, rows: [{ candidate_id: candidateId }] };
    }

    if (sql.includes("snapshot_active_job_id=$3 AND snapshot_active_job_attempt=$4")) {
      const [candidateId, dispatchGeneration, jobId, completedAttempt] = params;
      const eligible = candidate.candidateId === candidateId
        && candidate.dispatchGeneration === dispatchGeneration
        && candidate.activeJobId === jobId
        && candidate.activeAttempt === completedAttempt;
      if (!eligible) return { rowCount: 0, rows: [] };
      candidate.activeJobId = null;
      candidate.activeAttempt = null;
      return { rowCount: 1, rows: [{ candidate_id: candidateId }] };
    }

    if (sql.includes("SET status=$2,error_message=$3")) {
      const [candidateId, disposition, message, , dispatchGeneration, jobId, failedAttempt] = params;
      const eligible = candidate.candidateId === candidateId
        && candidate.dispatchGeneration === dispatchGeneration
        && candidate.activeJobId === jobId
        && candidate.activeAttempt === failedAttempt
        && ["discovered", "queued", "validating"].includes(candidate.status);
      if (!eligible) return { rowCount: 0, rows: [] };
      candidate.status = disposition;
      candidate.errorMessage = message;
      if (disposition !== "failed") {
        candidate.activeJobId = null;
        candidate.activeAttempt = null;
      }
      return { rowCount: 1, rows: [{ candidate_id: candidateId, status: disposition }] };
    }

    throw new Error("unexpected Candidate fence query");
  };
}

test("a late BullMQ failed event cannot overwrite a newer attempt in the same generation", {
  skip: redisUrl ? false : "MANAGED_JOB_TEST_REDIS_URL is not configured",
  timeout: 20_000,
}, async () => {
  const suffix = randomUUID();
  const queueName = `managed-worker-race-${suffix}`;
  const prefix = "rota-fix-review";
  const connection = redisConnection(redisUrl);
  const queue = new Queue(queueName, { connection, prefix });
  const queueEvents = new QueueEvents(queueName, { connection, prefix });
  const candidate = {
    candidateId: 42,
    dispatchGeneration: 7,
    status: "queued",
    errorMessage: null,
    activeJobId: null,
    activeAttempt: null,
  };
  const query = candidateQuery(candidate);
  const firstFailedListenerEntered = deferred();
  const secondAttemptValidating = deferred();
  const releaseFirstFailedListener = deferred();
  const firstFailedListenerFinished = deferred();
  const processorAttemptsMade = [];
  const failedEventAttemptsMade = [];
  const completedEventAttemptsMade = [];
  const workerErrors = [];
  const completedListenerFinished = deferred();
  let lateFailureApplied = null;
  let worker;

  try {
    await Promise.all([queue.waitUntilReady(), queueEvents.waitUntilReady()]);
    worker = new Worker(queueName, async (job) => {
      processorAttemptsMade.push(job.attemptsMade);
      assert.equal(await markChannelCandidateJobAttemptActive(query, job), true);
      if (job.attemptsMade === 0) {
        throw new Error("controlled first-attempt failure");
      }

      candidate.status = "validating";
      secondAttemptValidating.resolve();
      await within(firstFailedListenerFinished.promise, "late failed listener");
      return { accepted: true };
    }, { connection, prefix, concurrency: 1 });
    worker.on("error", (error) => workerErrors.push(error));
    worker.on("failed", async (job, error) => {
      if (!job || error?.message !== "controlled first-attempt failure") return;
      failedEventAttemptsMade.push(job.attemptsMade);
      firstFailedListenerEntered.resolve();
      try {
        await releaseFirstFailedListener.promise;
        lateFailureApplied = await recordChannelCandidateJobFailure(query, job, {
          disposition: "queued",
          message: error.message,
        });
        firstFailedListenerFinished.resolve();
      } catch (listenerError) {
        firstFailedListenerFinished.reject(listenerError);
      }
    });
    worker.on("completed", async (completedJob) => {
      completedEventAttemptsMade.push(completedJob.attemptsMade);
      try {
        assert.equal(await clearChannelCandidateJobAttempt(query, completedJob), true);
        completedListenerFinished.resolve();
      } catch (listenerError) {
        completedListenerFinished.reject(listenerError);
      }
    });

    const job = await queue.add("channel-crawl", {
      candidate_id: candidate.candidateId,
      dispatch_generation: candidate.dispatchGeneration,
    }, {
      jobId: `channel-job-${suffix}`,
      attempts: 2,
      removeOnComplete: false,
      removeOnFail: false,
    });

    await within(firstFailedListenerEntered.promise, "first failed listener");
    await within(secondAttemptValidating.promise, "second active attempt");
    assert.deepEqual(processorAttemptsMade, [0, 1]);
    assert.equal(candidate.activeAttempt, 2);
    assert.equal(candidate.status, "validating");

    releaseFirstFailedListener.resolve();
    await within(job.waitUntilFinished(queueEvents, 10_000), "BullMQ job completion");
    await within(completedListenerFinished.promise, "completed listener");

    assert.deepEqual(failedEventAttemptsMade, [1]);
    assert.deepEqual(completedEventAttemptsMade, [2]);
    assert.equal(lateFailureApplied, false);
    assert.equal(candidate.status, "validating");
    assert.equal(candidate.errorMessage, null);
    assert.equal(candidate.activeJobId, null);
    assert.equal(candidate.activeAttempt, null);
    assert.deepEqual(workerErrors, []);
  } finally {
    releaseFirstFailedListener.resolve();
    firstFailedListenerFinished.resolve();
    await worker?.close().catch(() => {});
    await queue.obliterate({ force: true }).catch(() => {});
    await Promise.all([queue.close(), queueEvents.close()]);
  }
});
