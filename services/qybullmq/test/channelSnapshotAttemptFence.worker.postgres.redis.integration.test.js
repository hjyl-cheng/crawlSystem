import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();
const redisUrl = String(process.env.MANAGED_JOB_TEST_REDIS_URL ?? "").trim();

function runStalledSnapshotScenario(scenario = "uploads") {
  const loader = new URL("./support/channelSnapshotAttemptFenceLoader.mjs", import.meta.url);
  const harness = new URL("./support/channelSnapshotAttemptFenceHarness.mjs", import.meta.url);
  const directory = mkdtempSync(join(tmpdir(), "qy-channel-snapshot-fence-"));
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
      env: { ...process.env, NODE_TEST_CONTEXT: "" },
      encoding: "utf8",
      timeout: 60_000,
    });
    assert.equal(child.status, 0, child.stderr || child.stdout);
    return JSON.parse(readFileSync(outputPath, "utf8"));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

test("a real stalled Channel Snapshot cannot persist stale Uploads after attempt takeover", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 70_000,
}, () => {
  const observed = runStalledSnapshotScenario();
  assert.equal(observed.harnessError, undefined);
  assert.deepEqual(observed.staleOutcome, {
    status: "rejected",
    code: "CANDIDATE_ATTEMPT_FENCE_STALE",
    message: observed.staleOutcome.message,
  });
  assert.match(observed.staleOutcome.message, /Candidate attempt Fence is stale/);
  assert.deepEqual(observed.beforeCurrentAttempt, {
    status: "running",
    detail_status: "pending",
    expected_content_count: 0,
    existing_is_recent: true,
    video_ids: null,
    detail_jobs: [],
  });
  assert.deepEqual(observed.afterCurrentAttempt, {
    status: "waiting_detail",
    detail_status: "queued",
    expected_content_count: 1,
    existing_is_recent: false,
    video_ids: ["current-attempt-video"],
    detail_jobs: [observed.afterCurrentAttempt.detail_jobs[0]],
  });
  assert.deepEqual(observed.afterCurrentAttempt.detail_jobs[0], {
    id: observed.afterCurrentAttempt.detail_jobs[0]?.id,
    origin_candidate_id: observed.afterCurrentAttempt.detail_jobs[0]?.origin_candidate_id,
    origin_dispatch_generation: 1,
    origin_snapshot_job_id: observed.afterCurrentAttempt.detail_jobs[0]?.origin_snapshot_job_id,
    origin_snapshot_job_attempt: 2,
  });
  assert.match(observed.afterCurrentAttempt.detail_jobs[0]?.id ?? "", /__g1__a2$/);
  assert.match(observed.afterCurrentAttempt.detail_jobs[0]?.origin_snapshot_job_id ?? "", /__g1$/);
  assert.ok(Number.isSafeInteger(observed.afterCurrentAttempt.detail_jobs[0]?.origin_candidate_id));
});

test("a real stalled Channel Snapshot cannot mark a Channel dormant after attempt takeover", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 70_000,
}, () => {
  const observed = runStalledSnapshotScenario("dormant");
  assert.equal(observed.harnessError, undefined);
  assert.deepEqual(observed.staleOutcome, {
    status: "rejected",
    code: "CANDIDATE_ATTEMPT_FENCE_STALE",
    message: observed.staleOutcome.message,
  });
  assert.match(observed.staleOutcome.message, /Candidate attempt Fence is stale/);
  assert.deepEqual(observed.beforeCurrentAttempt, {
    status: "running",
    detail_status: "pending",
    expected_content_count: 0,
    channel_status: "active",
    dormant_reason: null,
    video_ids: null,
    observation_count: 0,
    outbox_count: 0,
    finalize_job_count: 0,
    finalize_reasons: [],
    detail_jobs: [],
  });
  assert.deepEqual(observed.afterCurrentAttempt, {
    status: "waiting_detail",
    detail_status: "queued",
    expected_content_count: 1,
    channel_status: "active",
    dormant_reason: null,
    video_ids: ["current-attempt-recent-video"],
    observation_count: 0,
    outbox_count: 0,
    finalize_job_count: 1,
    finalize_reasons: ["channel-crawl-complete"],
    detail_jobs: [observed.afterCurrentAttempt.detail_jobs[0]],
  });
  assert.equal(observed.afterCurrentAttempt.detail_jobs[0]?.origin_dispatch_generation, 1);
  assert.equal(observed.afterCurrentAttempt.detail_jobs[0]?.origin_snapshot_job_attempt, 2);
  assert.match(observed.afterCurrentAttempt.detail_jobs[0]?.id ?? "", /__g1__a2$/);
});
