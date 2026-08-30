import assert from "node:assert/strict";
import test from "node:test";

import { dataApiCircuitState } from "../src/dataApiCircuit.js";

function systemFailure(code, category) {
  return {
    channel_execution_attempt: {
      youtube_requests: {
        request_count: 1,
        failure_count: 1,
        failure_evidence: [{
          source: "youtubejs_fetch",
          status: 503,
          target_url: "https://www.youtube.com/youtubei/v1/player",
        }],
      },
    },
    youtube_failure_decision: {
      kind: "retryable_system_failure",
      evidence: {
        failure_type: "retryable_system_failure",
        code,
        category,
      },
    },
  };
}

function requestFailure({ source, targetUrl = null }) {
  return {
    channel_execution_attempt: {
      youtube_requests: {
        request_count: 1,
        failure_count: 1,
        failure_evidence: [{ source, status: 503, target_url: targetUrl }],
      },
    },
    youtube_failure_decision: {
      kind: "upstream_transient",
      evidence: { source, status: 503, target_url: targetUrl },
    },
  };
}

test("Data API circuit counts only failures backed by a real detail request", async () => {
  const payloads = [
    systemFailure("LEASE_CONFLICT", "lease"),
    systemFailure("ROUTE_NOT_READY", "route"),
    systemFailure("CANDIDATE_ATTEMPT_FENCE_STALE", "fence"),
    systemFailure("ABORT_ERR", "cancellation"),
    systemFailure("CHANNEL_SNAPSHOT_DISPATCH_CONFLICT", "outbox"),
    systemFailure("PROXY_IDENTITY_CHANGED", "identity"),
    requestFailure({
      source: "youtubejs_fetch",
      targetUrl: "https://www.youtube.com/youtubei/v1/browse",
    }),
    requestFailure({ source: "yt_dlp_uploads" }),
    { youtube_failure_decision: { kind: "parser_runtime" } },
    {
      channel_execution_attempt: { youtube_requests: { failure_count: 1 } },
      youtube_failure_decision: { kind: "upstream_transient" },
    },
    requestFailure({
      source: "youtubejs_fetch",
      targetUrl: "https://www.youtube.com/youtubei/v1/player",
    }),
    requestFailure({
      source: "youtubejs_fetch",
      targetUrl: "https://www.youtube.com/youtubei/v1/next",
    }),
    requestFailure({
      source: "youtubejs_player",
      targetUrl: "https://www.youtube.com/watch?v=video-1",
    }),
    requestFailure({ source: "yt_dlp_detail" }),
  ];

  const state = await dataApiCircuitState({
    query: async () => ({ rows: payloads.map((payload_json) => ({ payload_json })) }),
    proxyCapacity: { active: 4, roles: { channel: { ready: 2 } } },
    detailExecutionQueue: "youtube-channel-crawl",
    detailExecutionRole: "channel",
  });

  assert.deepEqual(state, {
    open: false,
    reason: null,
    recent_detail_failures: 4,
  });
});
