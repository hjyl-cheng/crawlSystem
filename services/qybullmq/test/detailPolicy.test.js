import assert from "node:assert/strict";
import test from "node:test";
import { ParserContractError } from "../src/localizedParsing.js";
import {
  classifiedOnlyResolutionAction,
  contentDetailFailureError,
  detailResolutionAction,
  hasCompletePublicVideoSurface,
  hasResolvedDuration,
  isUpcomingLiveDetail,
  isLiveInProgress,
  isTerminalYoutubeError,
  isTransientYoutubeError,
  isYoutubeIpBlockedError,
  isYoutubeNetworkRetryableError,
  missingLikeIsZero,
  positiveDurationSeconds,
  unresolvedParserContractError,
  videoAccessStatus,
  youtubeErrorText,
  youtubeJsDetailFallbackReasons,
} from "../src/detailPolicy.js";

test("classified-only unknown access enters the official API fallback immediately", () => {
  assert.deepEqual(classifiedOnlyResolutionAction({
    accessStatus: "unknown",
    missingFields: [],
    attemptNumber: 1,
    maxAttempts: 3,
    apiFallbackMode: "emergency",
  }), {
    action: "api",
    missingFields: ["access_status"],
  });
  assert.deepEqual(classifiedOnlyResolutionAction({
    accessStatus: "login_required",
    missingFields: ["view_count"],
    attemptNumber: 1,
    maxAttempts: 3,
    apiFallbackMode: "emergency",
  }), {
    action: "api",
    missingFields: ["access_status", "view_count"],
  });
  assert.equal(classifiedOnlyResolutionAction({
    accessStatus: "private",
    attemptNumber: 1,
    maxAttempts: 3,
    apiFallbackMode: "emergency",
  }).action, "terminal");
});

test("content detail aggregate errors preserve a retryable YouTube cause", () => {
  const challenge = new Error("YouTube collection failure (bot challenge): Sign in to confirm you're not a bot");
  const aggregate = contentDetailFailureError(
    "2 content candidates failed during inline channel crawl",
    [
      { error: new Error("content type unresolved"), retryable: false },
      { error: challenge, retryable: true },
    ],
  );

  assert.equal(aggregate.cause, challenge);
  assert.equal(isYoutubeIpBlockedError(aggregate), true);

  const terminal = contentDetailFailureError(
    "1 content candidate failed",
    [{ error: new Error("video is private"), retryable: false }],
  );
  assert.equal(terminal.cause, undefined);
  assert.equal(isYoutubeIpBlockedError(terminal), false);
});

test("failure classification reads retryable causes from AggregateError entries", () => {
  const challenge = new Error("YouTube bot challenge HTTP 200 for /youtubei/v1/player");
  const aggregate = new AggregateError(
    [challenge, new Error("yt-dlp detail did not contain usable facts")],
    "incremental Video detail failed",
  );

  assert.equal(isYoutubeIpBlockedError(aggregate), true);
});

test("isUpcomingLiveDetail only identifies scheduled broadcasts", () => {
  assert.equal(isUpcomingLiveDetail({ live_status: "is_upcoming" }), true);
  assert.equal(isUpcomingLiveDetail({ live_status: "upcoming" }), true);
  assert.equal(isUpcomingLiveDetail({ is_upcoming: true }), true);
  assert.equal(isUpcomingLiveDetail({ live_status: "is_live" }), false);
  assert.equal(isUpcomingLiveDetail({ live_status: "was_live", live_scheduled_at: "2099-01-01T00:00:00Z" }), false);
});

test("a currently running live has no applicable final duration", () => {
  assert.equal(isLiveInProgress({ live_status: "is_live" }), true);
  assert.equal(isLiveInProgress({ is_live: true }), true);
  assert.equal(isLiveInProgress({ live_status: "was_live" }), false);
  assert.equal(isLiveInProgress({ live_status: "is_upcoming" }), false);
});

test("isTransientYoutubeError recognizes proxy and rate-limit failures", () => {
  assert.equal(isTransientYoutubeError(new Error("HTTP Error 429: Too Many Requests")), true);
  assert.equal(isTransientYoutubeError(new Error("Tunnel connection failed: 502")), true);
  assert.equal(isTransientYoutubeError(new Error("video is private")), false);
  assert.equal(isTransientYoutubeError(new Error("YouTube bot challenge HTTP 200")), true);
});

test("YouTube retry policy separates blocked, network, and terminal failures", () => {
  assert.equal(isYoutubeIpBlockedError(new Error("HTTP Error 429")), true);
  assert.equal(isYoutubeNetworkRetryableError(new Error("SSL WRONG_VERSION_NUMBER")), true);
  assert.equal(isTerminalYoutubeError(new Error("HTTP Error 404: playlist does not exist")), true);
  assert.equal(isYoutubeNetworkRetryableError(new Error("HTTP Error 404")), false);
  assert.equal(isYoutubeNetworkRetryableError(new Error("Failed to extract any player response")), true);
});

test("network classification includes nested undici and TLS causes", () => {
  const error = new TypeError("fetch failed", {
    cause: Object.assign(new Error("SSL wrong version number"), { code: "ERR_SSL_WRONG_VERSION_NUMBER" }),
  });
  assert.equal(isYoutubeNetworkRetryableError(error), true);
  assert.match(youtubeErrorText(error), /ERR_SSL_WRONG_VERSION_NUMBER/);
  assert.match(youtubeErrorText(error), /wrong version number/i);
});

test("missing public likes are normalized to zero without changing restricted content", () => {
  assert.equal(missingLikeIsZero({ privacy_status: "public" }, "unknown"), true);
  assert.equal(missingLikeIsZero({ view_count_text: "42" }, "unknown"), true);
  assert.equal(missingLikeIsZero({ published_at: "2026-07-01T00:00:00Z" }, "public"), true);
  assert.equal(missingLikeIsZero({ view_count_text: "42" }, "members_only"), false);
  assert.equal(missingLikeIsZero({ like_count: 3 }, "public"), false);
});

test("zero and placeholder durations remain unresolved", () => {
  assert.equal(positiveDurationSeconds(0), null);
  assert.equal(positiveDurationSeconds("211"), 211);
  assert.equal(hasResolvedDuration({ duration_seconds: 0, length_text: "0:00" }), false);
  assert.equal(hasResolvedDuration({ duration_seconds: 63 }), true);
  assert.equal(hasResolvedDuration({ length_text: "1:03" }), true);
});

test("detailResolutionAction retries only real transient extraction errors", () => {
  assert.equal(detailResolutionAction({
    error: new Error("Unable to download API page: HTTP 502"),
    attemptNumber: 1,
    maxAttempts: 3,
    missingFields: ["published_at"],
    apiFallbackMode: "emergency",
  }), "retry");

  assert.equal(detailResolutionAction({
    attemptNumber: 1,
    maxAttempts: 3,
    missingFields: ["comment_count"],
    apiFallbackMode: "emergency",
  }), "api");

  assert.equal(detailResolutionAction({
    error: new Error("Unable to download API page: HTTP 502"),
    attemptNumber: 3,
    maxAttempts: 3,
    missingFields: ["published_at"],
    apiFallbackMode: "emergency",
  }), "api");

  assert.equal(detailResolutionAction({
    attemptNumber: 1,
    maxAttempts: 3,
    missingFields: ["comment_count"],
    apiFallbackMode: "emergency",
    accessStatus: "public",
  }), "api");

  assert.equal(detailResolutionAction({
    attemptNumber: 3,
    maxAttempts: 3,
    missingFields: ["comment_count"],
    apiFallbackMode: "emergency",
    accessStatus: "login_required",
  }), "api");

  assert.equal(detailResolutionAction({
    attemptNumber: 3,
    maxAttempts: 3,
    missingFields: ["comment_count"],
    apiFallbackMode: "emergency",
    accessStatus: "public",
    apiAlreadyAttempted: true,
  }), "partial");
});

test("detailResolutionAction completes full data and terminates inaccessible partial data", () => {
  assert.equal(detailResolutionAction({ missingFields: [] }), "done");
  assert.equal(detailResolutionAction({
    error: new Error("HTTP Error 429"),
    attemptNumber: 1,
    maxAttempts: 3,
    missingFields: [],
  }), "done");
  assert.equal(detailResolutionAction({
    missingFields: ["view_count"],
    apiFallbackMode: "emergency",
    accessStatus: "members_only",
  }), "partial");
});

test("unlisted access stays non-public and does not enter API repair", () => {
  assert.equal(videoAccessStatus({
    availability: "unlisted",
    privacy_status: "public",
    playability_status: "OK",
  }), "unlisted");
  assert.equal(videoAccessStatus({ is_unlisted: true, playability_status: "OK" }), "unlisted");
  assert.equal(detailResolutionAction({
    missingFields: ["view_count"],
    apiFallbackMode: "emergency",
    accessStatus: "unlisted",
  }), "partial");
});

test("YouTube.js fallback is field- and evidence-driven", () => {
  assert.deepEqual(youtubeJsDetailFallbackReasons({
    missingFields: [],
    accessStatus: "public",
    detail: { live_status: "not_live" },
  }), []);
  assert.deepEqual(youtubeJsDetailFallbackReasons({
    missingFields: [],
    accessStatus: "unknown",
    detail: { live_status: "not_live" },
  }), []);
  assert.deepEqual(youtubeJsDetailFallbackReasons({
    missingFields: [],
    accessStatus: "unknown",
    detail: {
      playability_status: "UNPLAYABLE",
      playability_reason: "Video unavailable",
      playability_kind: "inconclusive",
      playability_reason_code: "generic_video_unavailable",
      playability_retry_mode: "alternate_client",
    },
  }), ["playability:alternate_client"]);
  assert.equal(hasCompletePublicVideoSurface({
    title: "Example",
    published_at: "2026-08-09",
    view_count_text: "17782",
    duration_seconds: 14,
  }), true);
  assert.deepEqual(youtubeJsDetailFallbackReasons({
    missingFields: [],
    accessStatus: "public",
    detail: {
      title: "Example",
      published_at: "2026-08-09",
      view_count_text: "17782",
      duration_seconds: 14,
      live_status: "not_live",
      playability_status: "UNPLAYABLE",
      playability_reason: "Video unavailable",
      playability_kind: "inconclusive",
      playability_reason_code: "generic_video_unavailable",
      playability_retry_mode: "alternate_client",
    },
  }), []);
  assert.deepEqual(youtubeJsDetailFallbackReasons({
    missingFields: ["comment_count"],
    accessStatus: "public",
    detail: {
      title: "Example",
      published_at: "2026-08-09",
      view_count_text: "17782",
      duration_seconds: 14,
      live_status: "not_live",
      playability_retry_mode: "alternate_client",
    },
  }), []);
  assert.deepEqual(youtubeJsDetailFallbackReasons({
    missingFields: ["comment_count"],
    accessStatus: "members_only",
    detail: { was_live: true, live_status: "was_live" },
  }), ["access:members_only", "live_state"]);
});

test("an alternate extractor may suppress a parser error only after full resolution", () => {
  const parserError = new ParserContractError({
    field: "published_age",
    value: "new localized format",
    locale: "ja",
    source: "youtube_channel_tab",
    reason: "unsupported_localized_relative_time",
  });
  assert.equal(unresolvedParserContractError([parserError], {
    contentType: "video",
    missingFields: ["published_at"],
    accessStatus: "public",
  }), parserError);
  assert.equal(unresolvedParserContractError([parserError], {
    contentType: "video",
    missingFields: [],
    accessStatus: "public",
  }), null);
  assert.equal(unresolvedParserContractError([new Error("HTTP 429")], {
    contentType: null,
    missingFields: ["published_at"],
    accessStatus: "unknown",
  }), null);
});
