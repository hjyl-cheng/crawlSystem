import assert from "node:assert/strict";
import test from "node:test";
import { VideoExecutionRecoveryPendingError } from '../src/videoExecutionRecovery.js';
import { FingerprintGatewayError } from "../src/fingerprintGateway.js";
import {
  executeManagedWorkerAttempt,
  retryableRotaFailure,
  validateWorkerQueueConfiguration,
} from "../src/managedWorkerExecution.js";

test('video recovery deferral does not switch route because of earlier network diagnostics', () => {
  const error = new VideoExecutionRecoveryPendingError('incremental:waiting');
  error.channel_execution_attempt = { failure_decisions: [{ kind: 'proxy_transport', evidence: { source: 'old_request' } }] };
  assert.equal(retryableRotaFailure(error),null);
});

test("stale detail ownership overrides earlier network diagnostics and never switches Route", async () => {
  const error = Object.assign(new Error("Full Crawl Detail execution was superseded"), {
    code: "CONTENT_DETAIL_EXECUTION_FENCE_STALE",
    channel_execution_attempt: { failure_decisions: [{ kind: "proxy_transport" }] },
  });
  assert.equal(retryableRotaFailure(error), null);
  let checkpoints = 0;
  await assert.rejects(executeManagedWorkerAttempt({
    job: { queueName: "youtube-channel-crawl" },
    execute: async () => { throw error; },
    persistRetryableCheckpoint: async () => { checkpoints += 1; return true; },
  }), value => value === error);
  assert.equal(checkpoints, 0);
});

test("managed Worker queue configuration is Role-exclusive", () => {
  assert.deepEqual(
    validateWorkerQueueConfiguration({
      role: "channel",
      enabledQueues: [
        "youtube-channel-crawl",
        "youtube-channel-incremental",
        "youtube-content-enrich",
        "youtube-content-detail",
      ],
    }).queues,
    [
      "youtube-channel-crawl",
      "youtube-channel-incremental",
      "youtube-content-enrich",
      "youtube-content-detail",
    ],
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
  assert.deepEqual(
    validateWorkerQueueConfiguration({
      enabledQueues: ["youtube-content-detail"],
      fixedProxy: true,
    }).queues,
    ["youtube-content-detail"],
  );
  assert.throws(
    () => validateWorkerQueueConfiguration({
      enabledQueues: ["youtube-content-enrich"],
    }),
    /require PROXY_SLOT_ROLE/,
  );
});

test("Content Enrich reports its real managed task stage", async () => {
  const result = await executeManagedWorkerAttempt({
    job: { queueName: "youtube-content-enrich" },
    prepared: {},
    attempt: { resumeMode: "initial" },
    execute: async () => {
      const error = Object.assign(new Error("HTTP 429"), {
        youtube_failure_decision: { kind: "youtube_rate_limited" },
      });
      throw error;
    },
    persistRetryableCheckpoint: async () => true,
  });
  assert.equal(result.failedStage, "content_enrich");
});

test("only structured proxy transport, rate-limit, and challenge failures switch Route", () => {
  assert.equal(retryableRotaFailure(new Error("ordinary parser failure")), null);
  assert.equal(retryableRotaFailure(Object.assign(new Error("HTTP 503"), {
    youtube_failure_decision: { kind: "upstream_transient" },
  })), null);
  const missingHttp = new FingerprintGatewayError({
    gatewayStatus: 502,
    payload: {
      failure_kind: "invalid_target_status",
      error_type: "InvalidTargetHttpStatus",
      target_status_raw: 0,
    },
    targetUrl: "https://www.youtube.com/channel/UC_mQGbdrG8_dOZRmX5PHzNw",
  });
  assert.deepEqual(retryableRotaFailure(missingHttp), {
    observation: "proxy_transport",
    source: "fingerprint_gateway",
  });
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

test("a nested Fingerprint proxy failure becomes a sourced Rota Observation", async () => {
  const calls = [];
  const gatewayError = Object.assign(new Error("gateway request failed"), {
    failureKind: "proxy_transport",
    code: "FINGERPRINT_PROXY_TRANSPORT",
    youtube_failure_evidence: { source: "fingerprint_gateway" },
  });
  const result = await executeManagedWorkerAttempt({
    job: { queueName: "youtube-channel-crawl" },
    prepared: { initialResumeMode: "initial" },
    attempt: { resumeMode: "initial" },
    execute: async () => { throw new Error("snapshot failed", { cause: gatewayError }); },
    persistRetryableCheckpoint: async () => {
      calls.push("checkpoint");
      return true;
    },
  });
  assert.deepEqual(calls, ["checkpoint"]);
  assert.deepEqual(result, {
    kind: "retryable_network_failure",
    observation: "proxy_transport",
    source: "fingerprint_gateway",
    failedStage: "channel_full",
    checkpointPersisted: true,
  });
});

test("a structured Route failure without a local source uses the managed default", () => {
  const gatewayError = Object.assign(new Error("gateway request failed"), {
    failureKind: "proxy_transport",
    code: "FINGERPRINT_PROXY_TRANSPORT",
  });
  const error = Object.assign(new Error("unrelated wrapper"), {
    cause: gatewayError,
    youtube_failure_evidence: { source: "unrelated_wrapper" },
  });

  assert.deepEqual(retryableRotaFailure(error), {
    observation: "proxy_transport",
    source: "youtube_managed_request",
  });
});

test("a nested rate limit Observation does not borrow its wrapper source", () => {
  const rateLimit = Object.assign(new Error("HTTP 429"), {
    youtube_failure_evidence: {
      status: 429,
      source: "youtubejs_player",
    },
  });
  const error = Object.assign(new Error("snapshot failed", { cause: rateLimit }), {
    youtube_failure_evidence: {
      status: null,
      source: "unrelated_wrapper",
    },
  });

  assert.deepEqual(retryableRotaFailure(error), {
    observation: "youtube_rate_limited",
    source: "youtubejs_player",
  });
});

test("a recorded rate-limit Decision does not borrow Evidence from a 404 sibling", () => {
  const missingContent = Object.assign(new Error("HTTP 404 video not found"), {
    youtube_failure_evidence: {
      status: 404,
      source: "content_lookup",
    },
  });
  const error = Object.assign(
    new AggregateError([new Error("HTTP 429"), missingContent], "parallel requests failed"),
    {
      channel_execution_attempt: {
        failure_decisions: [{ kind: "youtube_rate_limited" }],
      },
    },
  );

  assert.deepEqual(retryableRotaFailure(error), {
    observation: "youtube_rate_limited",
    source: "youtube_managed_request",
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

test("country recheck persists a bounded intent without a network failure checkpoint", async () => {
  const { emptyUploadsDecision } = await import("../src/youtubeUploadsCountry.js");
  const job = { data: { channel_id: "UCcountry" }, async updateData(data) { this.data = data; } };
  const execute = () => emptyUploadsDecision("BR");
  const result = await executeManagedWorkerAttempt({ job, prepared: {}, attempt: { egressCountry: "US" }, execute,
    persistRetryableCheckpoint: async () => assert.fail("country restriction is not proxy health failure") });
  assert.deepEqual(result, { kind: "country_recheck", country: "BR" });
  assert.deepEqual(job.data.uploads_country_recheck, { country: "BR", status: "requested" });
  const resumed = await executeManagedWorkerAttempt({ job, prepared: {}, attempt: { egressCountry: "BR" }, execute,
    persistRetryableCheckpoint: async () => assert.fail("no failure checkpoint") });
  assert.equal(resumed.kind, "managed_work_complete");
  assert.equal(resumed.result.reason, "country_checked");
});
