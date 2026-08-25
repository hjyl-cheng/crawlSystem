import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

function runScenario(scenario, { expectedStatus = 0 } = {}) {
  const loader = new URL("./support/pipelineV2DispositionLoader.mjs", import.meta.url);
  const harness = new URL("./support/pipelineV2DispositionHarness.mjs", import.meta.url);
  const env = { ...process.env };
  delete env.NODE_TEST_CONTEXT;
  const directory = mkdtempSync(join(tmpdir(), "qy-pipeline-disposition-"));
  const outputPath = join(directory, "result.json");
  try {
    const child = spawnSync(process.execPath, [
      "--no-warnings",
      "--experimental-loader",
      loader.pathname,
      harness.pathname,
      outputPath,
      scenario,
    ], {
      cwd: new URL("..", import.meta.url),
      encoding: "utf8",
      env,
    });
    assert.equal(child.status, expectedStatus, child.stderr || child.stdout);
    return JSON.parse(readFileSync(outputPath, "utf8"));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

test("the shared Full Crawl detail path defers public content without authoritative type evidence", () => {
  const observed = runScenario("deferred_type");
  assert.equal(observed.error, null);
  assert.equal(observed.candidate.disposition, "deferred");
  assert.equal(observed.candidate.result_json.disposition.kind, "deferred");
  assert.equal(
    observed.candidate.result_json.disposition.reason_code,
    "authoritative_type_unresolved",
  );
  assert.equal(observed.candidate.result_json.disposition.retry_class, "alternate_player");
  assert.equal(observed.candidate.result_json.disposition.retryable, true);
  assert.match(observed.candidate.next_attempt_at, /^\d{4}-\d{2}-\d{2}T/);
  assert.equal(observed.candidate.content_key, null);
  assert.equal(observed.candidate.result_json.access.access_status, "public");
  assert.equal(observed.candidate.result_json.detail.id, "public-without-type");
});

test("the shared Full Crawl detail path terminally excludes explicit private access", () => {
  const observed = runScenario("terminal_private");
  assert.equal(observed.error, null);
  assert.equal(observed.candidate.disposition, "terminal_excluded");
  assert.equal(observed.candidate.result_json.disposition.kind, "terminal_excluded");
  assert.equal(observed.candidate.result_json.disposition.reason_code, "access_private");
  assert.equal(
    observed.candidate.result_json.disposition.retry_class,
    "low_frequency_access_recheck",
  );
  assert.equal(observed.candidate.result_json.disposition.retryable, false);
  assert.match(observed.candidate.next_attempt_at, /^\d{4}-\d{2}-\d{2}T/);
  assert.equal(observed.candidate.content_key, null);
  assert.equal(observed.candidate.result_json.access.access_status, "private");
  assert.equal(observed.candidate.result_json.classification.authoritative, true);
});

test("the shared Full Crawl detail path stores a public authoritative Video", () => {
  const observed = runScenario("stored_public");
  assert.equal(observed.error, null);
  assert.equal(observed.candidate.disposition, "stored");
  assert.equal(observed.candidate.result_json.disposition.kind, "stored");
  assert.equal(observed.candidate.result_json.disposition.reason_code, "content_stored");
  assert.equal(observed.candidate.result_json.disposition.retry_class, null);
  assert.equal(observed.candidate.result_json.disposition.retryable, false);
  assert.equal(observed.candidate.next_attempt_at, null);
  assert.equal(observed.candidate.content_type, "video");
  assert.equal(
    observed.candidate.content_key,
    "UCsharedDisposition:video:public-without-type",
  );
  assert.equal(observed.candidate.result_json.access.access_status, "public");
  assert.equal(observed.candidate.result_json.classification.authoritative, true);
});

test("the shared Full Crawl detail path normalizes disabled comments to zero", () => {
  const observed = runScenario("disabled_comments");
  assert.equal(observed.error, null);
  assert.equal(observed.candidate.disposition, "stored");
  assert.deepEqual(
    (({ comment_count, comment_count_status, comments_disabled }) => ({
      comment_count,
      comment_count_status,
      comments_disabled,
    }))(observed.candidate.result_json.detail),
    {
      comment_count: 0,
      comment_count_status: "disabled",
      comments_disabled: true,
    },
  );
});

test("migration verifies a YouTube.js disabled result and keeps visible yt-dlp comments", () => {
  const observed = runScenario("youtubejs_disabled_ytdlp_visible");
  const detail = observed.candidate.result_json.detail;

  assert.equal(observed.error, null);
  assert.equal(observed.youtubejs_detail_attempts, 1);
  assert.equal(observed.ytdlp_detail_attempts, 1);
  assert.deepEqual(observed.candidate.result_json.youtubejs_fallback_reasons, [
    "comments_disabled_verification",
  ]);
  assert.equal(detail.comments_disabled, false);
  assert.equal(detail.comment_count, 19);
  assert.equal(detail.comment_count_status, "exact");
  assert.equal(detail.comment_count_source, "yt_dlp");
  assert.equal(detail.comments_status_source, "yt_dlp");
  assert.equal(detail.comments_first_page.returned_count, 15);
  assert.equal(detail.comments_first_page_source, "yt_dlp_top_comments");
});

test("migration does not let a yt-dlp disabled result erase visible YouTube.js comments", () => {
  const observed = runScenario("youtubejs_visible_ytdlp_disabled");
  const detail = observed.candidate.result_json.detail;

  assert.equal(observed.error, null);
  assert.equal(observed.youtubejs_detail_attempts, 1);
  assert.equal(observed.ytdlp_detail_attempts, 1);
  assert.equal(detail.comments_disabled, false);
  assert.equal(detail.comment_count, 12);
  assert.equal(detail.comment_count_status, "exact");
  assert.equal(detail.comment_count_source, "youtubejs_comments");
  assert.equal(detail.comments_status_source, "youtubejs_comments");
  assert.equal(detail.comments_first_page.returned_count, 1);
  assert.equal(detail.comments_first_page_source, "youtubejs_comments");
});

test("migration preserves explicit YouTube.js unlisted access while yt-dlp fills missing detail", () => {
  const observed = runScenario("youtubejs_unlisted_ytdlp_public");
  const detail = observed.candidate.result_json.detail;

  assert.equal(observed.error, null);
  assert.equal(observed.youtubejs_detail_attempts, 1);
  assert.equal(observed.ytdlp_detail_attempts, 1);
  assert.deepEqual(observed.candidate.result_json.youtubejs_fallback_reasons, ["description"]);
  assert.equal(detail.description, "Complete enough for storage except type");
  assert.equal(detail.access_status, "unlisted");
  assert.equal(detail.access_status_source, "youtubejs_microformat");
  assert.equal(detail.availability, "unlisted");
  assert.equal(detail.is_unlisted, true);
  assert.equal(observed.candidate.result_json.access.access_status, "unlisted");
  assert.equal(
    observed.candidate.result_json.access.access_status_source,
    "youtubejs_microformat",
  );
  assert.equal(observed.candidate.disposition, "stored");
});

test("the shared Full Crawl detail path preserves an existing Video when access changes", () => {
  const observed = runScenario("existing_private");
  assert.equal(observed.error, null);
  assert.equal(observed.candidate.disposition, "stored");
  assert.equal(observed.candidate.result_json.disposition.kind, "stored");
  assert.equal(
    observed.candidate.result_json.disposition.reason_code,
    "content_access_updated",
  );
  assert.equal(observed.candidate.next_attempt_at, null);
  assert.equal(
    observed.candidate.content_key,
    "UCsharedDisposition:video:public-without-type",
  );
  assert.equal(observed.candidate.content_type, "video");
  assert.equal(observed.candidate.result_json.access.access_status, "private");
  assert.equal(observed.candidate.result_json.preserved_content_type, true);
});

test("the shared Full Crawl detail path records an upcoming Live exclusion", () => {
  const observed = runScenario("upcoming_live");
  assert.equal(observed.error, null);
  assert.equal(observed.candidate.disposition, "terminal_excluded");
  assert.equal(observed.candidate.result_json.disposition.kind, "terminal_excluded");
  assert.equal(observed.candidate.result_json.disposition.reason_code, "upcoming_live");
  assert.equal(
    observed.candidate.result_json.disposition.retry_class,
    "low_frequency_access_recheck",
  );
  assert.equal(observed.candidate.result_json.disposition.retryable, false);
  assert.match(observed.candidate.next_attempt_at, /^\d{4}-\d{2}-\d{2}T/);
  assert.equal(observed.candidate.content_key, null);
  assert.equal(observed.candidate.result_json.scope.reason, "upcoming_live");
  assert.notEqual(observed.candidate.result_json.classification?.authoritative, true);
});

test("the shared Full Crawl detail path excludes a Live in progress after detail detection", () => {
  const observed = runScenario("live_in_progress");
  assert.equal(observed.error, null);
  assert.equal(observed.requests_after_retry, 1);
  assert.equal(observed.candidate.disposition, "terminal_excluded");
  assert.equal(observed.candidate.result_json.disposition.kind, "terminal_excluded");
  assert.equal(observed.candidate.result_json.disposition.reason_code, "live_in_progress");
  assert.equal(observed.candidate.detail_status, "done");
  assert.equal(observed.candidate.api_status, "not_needed");
  assert.deepEqual(observed.candidate.missing_fields, []);
  assert.equal(observed.candidate.content_key, null);
  assert.equal(observed.candidate.result_json.scope.reason, "live_in_progress");
});

test("the shared Full Crawl path accepts live_status as the only current-Live signal", () => {
  const observed = runScenario("live_in_progress_flat");
  assert.equal(observed.error, null);
  assert.equal(observed.requests_after_retry, 0);
  assert.equal(observed.candidate.disposition, "terminal_excluded");
  assert.equal(observed.candidate.result_json.disposition.reason_code, "live_in_progress");
  assert.equal(observed.candidate.api_status, "not_needed");
  assert.equal(observed.candidate.content_key, null);
});

test("the shared Full Crawl detail path stores an ended Live replay", () => {
  const observed = runScenario("live_replay");
  assert.equal(observed.error, null);
  assert.equal(observed.requests_after_retry, 1);
  assert.equal(observed.candidate.disposition, "stored");
  assert.equal(observed.candidate.result_json.disposition.reason_code, "content_stored");
  assert.equal(observed.candidate.content_type, "live");
  assert.equal(
    observed.candidate.content_key,
    "UCsharedDisposition:live:public-without-type",
  );
  assert.equal(observed.candidate.result_json.detail.live_status, "was_live");
});

test("the shared Full Crawl detail path records an age-window exclusion", () => {
  const observed = runScenario("age_excluded");
  assert.equal(observed.error, null);
  assert.equal(observed.candidate.disposition, "terminal_excluded");
  assert.equal(observed.candidate.result_json.disposition.kind, "terminal_excluded");
  assert.equal(
    observed.candidate.result_json.disposition.reason_code,
    "outside_content_window",
  );
  assert.equal(
    observed.candidate.result_json.disposition.retry_class,
    "low_frequency_policy_recheck",
  );
  assert.equal(observed.candidate.result_json.disposition.retryable, false);
  assert.match(observed.candidate.next_attempt_at, /^\d{4}-\d{2}-\d{2}T/);
  assert.equal(observed.candidate.content_key, null);
  assert.equal(observed.candidate.result_json.scope.reason, "older_than_max_age");
});

test("the shared Full Crawl detail path persists a deferred disposition before retrying a failure", () => {
  const observed = runScenario("detail_failure", { expectedStatus: 1 });
  assert.match(observed.error.message, /content candidates failed before API fallback/);
  assert.equal(observed.candidate.disposition, "deferred");
  assert.equal(observed.candidate.result_json.disposition.kind, "deferred");
  assert.equal(
    observed.candidate.result_json.disposition.reason_code,
    "detail_collection_failed",
  );
  assert.equal(observed.candidate.result_json.disposition.retry_class, "player_retry");
  assert.equal(observed.candidate.result_json.disposition.retryable, true);
  assert.match(observed.candidate.next_attempt_at, /^\d{4}-\d{2}-\d{2}T/);
  assert.equal(observed.candidate.detail_status, "failed");
  assert.equal(
    observed.candidate.result_json.errors.some((message) => message.includes("Player timeout")),
    true,
  );
});

test("the shared detail path rejects a terminal candidate without a disposition", () => {
  const observed = runScenario("undisposed_terminal", { expectedStatus: 1 });
  assert.match(observed.error.message, /has no persisted evidence for disposition recovery/);
  assert.equal(observed.candidate.detail_status, "done");
  assert.equal(observed.candidate.disposition, null);
  assert.equal(observed.requests_after_retry, 0);
});

test("retry repairs a terminal candidate disposition without another YouTube request", () => {
  const observed = runScenario("disposition_write_retry");
  assert.match(observed.first_error.message, /injected disposition persistence failure/);
  assert.equal(observed.retry_error, null);
  assert.equal(observed.value.status, "done");
  assert.equal(observed.value.processed, 1);
  assert.equal(observed.candidate.detail_status, "done");
  assert.equal(observed.candidate.disposition, "stored");
  assert.equal(observed.candidate.result_json.disposition.reason_code, "content_stored");
  assert.equal(observed.requests_after_first, 1);
  assert.equal(observed.requests_after_retry, 1);
  assert.equal(observed.candidate.attempts, 1);
  assert.equal(observed.disposition_write_attempts, 2);
});

test("Data API replay resolves a deferred candidate without losing its prior evidence", () => {
  const observed = runScenario("data_api_replay");
  assert.equal(observed.error, null);
  assert.equal(observed.value.request_attempts, 0);
  assert.equal(observed.candidate.disposition, "stored");
  assert.equal(observed.candidate.result_json.disposition.reason_code, "content_stored");
  assert.equal(observed.candidate.result_json.access.access_status, "public");
  assert.equal(observed.candidate.result_json.api_detail.privacy_status, "public");
  assert.equal(
    observed.candidate.result_json.recovery.from_disposition.reason_code,
    "access_unknown",
  );
  assert.equal(
    observed.candidate.result_json.recovery.deferred_error_message,
    "content access unknown before API fallback",
  );
  assert.equal(
    observed.candidate.result_json.recovery.deferred_evidence.detail.access_status,
    "unknown",
  );
});

test("Data API videos.list excludes a running Live instead of repairing comment fields", () => {
  const observed = runScenario("data_api_live");
  assert.equal(observed.error, null);
  assert.equal(observed.value.request_attempts, 1);
  assert.equal(observed.candidate.disposition, "terminal_excluded");
  assert.equal(observed.candidate.result_json.disposition.reason_code, "live_in_progress");
  assert.equal(observed.candidate.detail_status, "done");
  assert.equal(observed.candidate.api_status, "not_needed");
  assert.deepEqual(observed.candidate.missing_fields, []);
  assert.equal(observed.candidate.content_key, null);
});
