import assert from "node:assert/strict";
import test from "node:test";

import {
  INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_OPERATION_ID,
  INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGETS,
  INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGET_SQL,
  classifyIncrementalYoutubeJsTerminalProbeRecoveryTarget,
  loadIncrementalYoutubeJsTerminalProbeRecoveryTargets,
  recoverIncrementalYoutubeJsTerminalProbeFailures,
} from "../src/incrementalYoutubeJsTerminalProbeRecovery.js";

function row(target) {
  const privateVideo = target.terminal_reason_code === "private";
  return {
    expected_plan_id: target.plan_id,
    expected_channel_id: target.channel_id,
    expected_video_id: target.video_id,
    terminal_reason_code: target.terminal_reason_code,
    attempts_started_before: target.attempts_started_before,
    run_id: `incremental:${target.plan_id}`,
    channel_id: target.channel_id,
    plan_id: target.plan_id,
    plan_day: "2026-09-04",
    run_status: "failed",
    detail_status: "failed",
    publication_finalized_at: null,
    error_message: privateVideo
      ? "[PARSER_CONTRACT_ERROR] unsupported_playability_status LOGIN_REQUIRED"
      : "This video is unavailable",
    video_status: "failed",
    job_id: `job:${target.plan_id}`,
    cycle_key: "base",
    batch_status: "fetching",
    item_phase: "recent",
    item_status: "pending",
    item_ordinal: 1,
    failure_observation_id: `observation:${target.plan_id}`,
    failure_outcome: "failed",
    failure_reason_code: privateVideo ? "video_parser_failure" : "video_crawler_failure",
    outbox_status: "published",
  };
}

function job(targetRow, overrides = {}) {
  const retryStates = [];
  const value = {
    id: targetRow.job_id,
    name: "channel.incremental.plan",
    data: {
      job_id: targetRow.job_id,
      plan_id: targetRow.plan_id,
      channel_id: targetRow.channel_id,
      task_mask: { about: true, video: true, agent: false },
    },
    opts: { attempts: 5 },
    attemptsMade: targetRow.attempts_started_before,
    attemptsStarted: targetRow.attempts_started_before,
    async getState() { return "failed"; },
    async retry(state) { retryStates.push(state); },
    ...overrides,
  };
  return { value, retryStates };
}

function fixture() {
  const rows = INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGETS.map(row);
  const jobs = new Map(rows.map((targetRow) => {
    const current = job(targetRow);
    return [targetRow.job_id, current];
  }));
  return { rows, jobs };
}

test("the recovery is pinned to the ten audited Plan and video identities", () => {
  assert.equal(INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGETS.length, 10);
  assert.equal(new Set(
    INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGETS.map((target) => target.plan_id),
  ).size, 10);
  assert.equal(new Set(
    INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGETS.map((target) => target.video_id),
  ).size, 10);
  assert.match(INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGET_SQL, /jsonb_to_recordset/);
  assert.match(INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGET_SQL, /batch\.cycle_key='base'/);
  assert.match(INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGET_SQL, /observation_kind='video'/);
});

test("the read-only selector rejects a changed checkpoint target", async () => {
  const { rows } = fixture();
  rows[0] = { ...rows[0], item_status: "captured" };
  await assert.rejects(
    loadIncrementalYoutubeJsTerminalProbeRecoveryTargets(
      async () => ({ rows }),
      { planDay: "2026-09-04" },
    ),
    /target Item status changed/,
  );
});

test("an audited failed Job is eligible for one manual retry", async () => {
  const targetRow = row(INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGETS[0]);
  const { value } = job(targetRow);
  const inspection = await classifyIncrementalYoutubeJsTerminalProbeRecoveryTarget(
    targetRow,
    value,
  );
  assert.equal(inspection.action, "retry_failed");

  value.attemptsMade += 1;
  value.attemptsStarted += 1;
  const repeated = await classifyIncrementalYoutubeJsTerminalProbeRecoveryTarget(
    targetRow,
    value,
  );
  assert.equal(repeated.action, "recovery_failed");
});

test("dry-run audits all targets without retrying a Job", async () => {
  const { rows, jobs } = fixture();
  const summary = await recoverIncrementalYoutubeJsTerminalProbeFailures({
    query: async (sql, params) => {
      assert.equal(sql, INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGET_SQL);
      assert.equal(params[1], "2026-09-04");
      return { rows };
    },
    queue: {
      async getJob(id) { return jobs.get(id)?.value ?? null; },
    },
    planDay: "2026-09-04",
  });

  assert.equal(summary.apply, false);
  assert.equal(summary.target_count, 10);
  assert.deepEqual(summary.action_counts, { retry_failed: 10 });
  assert.equal([...jobs.values()].flatMap((entry) => entry.retryStates).length, 0);
});

test("apply requires exact confirmation and retries only the audited failed Jobs", async () => {
  const { rows, jobs } = fixture();
  const input = {
    query: async () => ({ rows }),
    queue: {
      async getJob(id) { return jobs.get(id)?.value ?? null; },
    },
    planDay: "2026-09-04",
    expectedCount: 10,
    apply: true,
  };
  await assert.rejects(
    recoverIncrementalYoutubeJsTerminalProbeFailures(input),
    /exact recovery operation ID/,
  );

  const summary = await recoverIncrementalYoutubeJsTerminalProbeFailures({
    ...input,
    confirmation: INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_OPERATION_ID,
  });
  assert.deepEqual(summary.dispatch_counts, { retried_failed: 10 });
  assert.deepEqual(
    [...jobs.values()].flatMap((entry) => entry.retryStates),
    Array(10).fill("failed"),
  );
});
