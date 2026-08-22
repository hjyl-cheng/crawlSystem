import assert from "node:assert/strict";
import test from "node:test";
import { resolveVideoDisposition } from "../src/videoDisposition.js";

test("an existing Full Video remains stored when explicit access evidence changes", () => {
  const observedAt = "2026-07-20T00:00:00.000Z";
  assert.deepEqual(resolveVideoDisposition({
    storageAction: {
      kind: "update_access",
      content_key: "UCvideo:video:existing-video",
      content_type: "video",
      type_source: "youtube_watch_canonical",
    },
    classification: null,
    access: {
      access_status: "private",
      access_status_source: "youtubejs_player",
    },
    detail: { id: "existing-video", access_status: "private" },
    observedAt,
  }), {
    version: "video-disposition-v1",
    kind: "stored",
    reason_code: "content_access_updated",
    retry_class: null,
    retryable: false,
    observed_at: observedAt,
    next_attempt_at: null,
  });
});

test("an upcoming Live terminal state takes precedence over otherwise storable evidence", () => {
  const observedAt = "2026-07-20T00:00:00.000Z";
  assert.deepEqual(resolveVideoDisposition({
    storageAction: {
      kind: "upsert",
      content_type: "live",
      type_source: "youtube_watch_live_flag",
    },
    classification: {
      content_type: "live",
      source: "youtube_watch_live_flag",
      authoritative: true,
    },
    access: { access_status: "public", access_status_source: "youtubejs_player" },
    detail: { id: "scheduled-live", is_upcoming: true },
    terminalReason: "upcoming_live",
    observedAt,
  }), {
    version: "video-disposition-v1",
    kind: "terminal_excluded",
    reason_code: "upcoming_live",
    retry_class: "low_frequency_access_recheck",
    retryable: false,
    observed_at: observedAt,
    next_attempt_at: "2026-07-21T00:00:00.000Z",
  });
});

test("an incomplete Uploads discovery defers the observed ID for a scan retry", () => {
  const observedAt = "2026-07-20T00:00:00.000Z";
  assert.deepEqual(resolveVideoDisposition({
    storageAction: { kind: "unresolved" },
    classification: null,
    access: { access_status: "unknown" },
    detail: null,
    deferredReason: "discovery_scan_incomplete",
    observedAt,
  }), {
    version: "video-disposition-v1",
    kind: "deferred",
    reason_code: "discovery_scan_incomplete",
    retry_class: "uploads_scan_retry",
    retryable: true,
    observed_at: observedAt,
    next_attempt_at: "2026-07-20T01:00:00.000Z",
  });
});

test("an inconclusive low-frequency recheck retains its prior terminal conclusion", () => {
  const observedAt = "2026-07-27T00:00:00.000Z";
  assert.deepEqual(resolveVideoDisposition({
    storageAction: { kind: "unresolved" },
    classification: null,
    access: { access_status: "unknown", access_status_source: null },
    detail: { id: "private-video", title: "Partial detail" },
    priorDisposition: {
      kind: "terminal_excluded",
      reason_code: "access_private",
    },
    observedAt,
  }), {
    version: "video-disposition-v1",
    kind: "terminal_excluded",
    reason_code: "access_private",
    retry_class: "low_frequency_access_recheck",
    retryable: false,
    observed_at: observedAt,
    next_attempt_at: "2026-08-03T00:00:00.000Z",
  });
});
