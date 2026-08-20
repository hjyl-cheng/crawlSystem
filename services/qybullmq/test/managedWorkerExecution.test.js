import assert from "node:assert/strict";
import test from "node:test";
import {
  executeManagedWorkerAttempt,
  retryableRotaFailure,
  validateWorkerQueueConfiguration,
} from "../src/managedWorkerExecution.js";

test("managed Worker queue configuration is Role-exclusive", () => {
  assert.deepEqual(
    validateWorkerQueueConfiguration({
      role: "channel",
      enabledQueues: ["youtube-channel-crawl", "youtube-channel-incremental"],
    }).queues,
    ["youtube-channel-crawl", "youtube-channel-incremental"],
  );
  assert.throws(
    () => validateWorkerQueueConfiguration({
      role: "discover",
      enabledQueues: ["youtube-discover-page", "youtube-channel-crawl"],
    }),
    /cannot consume queues/,
  );
  assert.throws(
    () => validateWorkerQueueConfiguration({
      enabledQueues: ["youtube-query-quality"],
    }),
    /require PROXY_SLOT_ROLE/,
  );
  assert.throws(
    () => validateWorkerQueueConfiguration({
      enabledQueues: ["youtube-content-detail"],
    }),
    /standalone youtube-content-detail/,
  );
});

test("only structured proxy transport, rate-limit, and challenge failures switch Route", () => {
  assert.equal(retryableRotaFailure(new Error("ordinary parser failure")), null);
  assert.equal(retryableRotaFailure(Object.assign(new Error("HTTP 503"), {
    youtube_failure_decision: { kind: "upstream_transient" },
  })), null);
  const error = Object.assign(new Error("outer aggregate error"), {
    channel_execution_attempt: {
      failure_decisions: [
        { kind: "proxy_transport", evidence: { source: "youtubejs_fetch" } },
        { kind: "youtube_challenge", evidence: { source: "youtube_player" } },
      ],
    },
  });
  assert.deepEqual(retryableRotaFailure(error), {
    observation: "youtube_challenge",
    source: "youtube_player",
  });
});

test("a managed failure is returned only after its durable checkpoint", async () => {
  const calls = [];
  const job = { queueName: "youtube-channel-crawl" };
  const result = await executeManagedWorkerAttempt({
    job,
    prepared: { initialResumeMode: "initial" },
    attempt: { resumeMode: "network_attempt_resume" },
    execute: async ({ resumeMode }) => {
      calls.push(`execute:${resumeMode}`);
      const error = new Error("HTTP 429");
      error.youtube_failure_decision = { kind: "youtube_rate_limited" };
      throw error;
    },
    persistRetryableCheckpoint: async () => {
      calls.push("checkpoint");
      return true;
    },
  });
  assert.deepEqual(calls, ["execute:network_attempt_resume", "checkpoint"]);
  assert.deepEqual(result, {
    kind: "retryable_network_failure",
    observation: "youtube_rate_limited",
    source: "youtube_managed_request",
    failedStage: "channel_full",
    checkpointPersisted: true,
  });
});

test("Discover search can finish while downstream qualification remains open", async () => {
  const result = await executeManagedWorkerAttempt({
    job: { queueName: "youtube-discover-page" },
    prepared: {},
    attempt: { resumeMode: "initial" },
    execute: async () => ({ ok: true, qualification_pending: true }),
    persistRetryableCheckpoint: async () => true,
  });
  assert.equal(result.kind, "managed_work_complete");
  assert.equal(result.businessState, "waiting_downstream");
});
