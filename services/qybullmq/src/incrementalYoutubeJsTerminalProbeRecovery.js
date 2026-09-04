const PLAN_DAY = "2026-09-04";

export const INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_OPERATION_ID =
  "incremental-youtubejs-terminal-probe-recovery-20260904-v1";

export const INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGETS = Object.freeze([
  {
    plan_id: "aae55e62-190b-5b54-9783-a1afd9ee1066",
    channel_id: "UC-iDqVtLMbtrOt2EbpIkSsQ",
    video_id: "Ue6kayghUeQ",
    terminal_reason_code: "uploader_removed",
    attempts_started_before: 5,
  },
  {
    plan_id: "ed8d706d-1c31-55a6-9c87-a126a31877c5",
    channel_id: "UCBNEFXF9I9fNja_hGWNK3ew",
    video_id: "YagFmyEQdVc",
    terminal_reason_code: "private",
    attempts_started_before: 1,
  },
  {
    plan_id: "d9139c28-fb30-5093-a2a5-4338f6b97b39",
    channel_id: "UCEGECqTTHLgsXaaqkRTsjnA",
    video_id: "hLP05imSPoY",
    terminal_reason_code: "uploader_removed",
    attempts_started_before: 5,
  },
  {
    plan_id: "c27302d2-2bd2-5650-b532-c4b552ce767a",
    channel_id: "UCVZ8m3aKVsuTnXlpY5kf8PQ",
    video_id: "P4DBGunObdY",
    terminal_reason_code: "uploader_removed",
    attempts_started_before: 5,
  },
  {
    plan_id: "b7fd76e4-9910-5c2e-835e-bf348cead817",
    channel_id: "UCVkGS8DupkUjYPJnUKPiN3A",
    video_id: "qe5KSrm8yJ4",
    terminal_reason_code: "uploader_removed",
    attempts_started_before: 5,
  },
  {
    plan_id: "db530826-a18f-55d5-8fb9-566dab50b9e3",
    channel_id: "UCWtkySCzik7aEYphc2ycJMg",
    video_id: "gKCqwoNjZYs",
    terminal_reason_code: "private",
    attempts_started_before: 1,
  },
  {
    plan_id: "ad369ae5-ca52-5c7d-9e74-583e66160cb3",
    channel_id: "UCmRTpHDXWhSMOgGXZ-aZMYw",
    video_id: "ezH70EPhgKM",
    terminal_reason_code: "uploader_removed",
    attempts_started_before: 5,
  },
  {
    plan_id: "9ddcdc90-f600-57cf-9bbf-e2659e3ff674",
    channel_id: "UCoNJf7XvA75_i1gDLlq8qZQ",
    video_id: "ZidzhZ3T7Y8",
    terminal_reason_code: "uploader_removed",
    attempts_started_before: 5,
  },
  {
    plan_id: "98f6c20c-32b0-54ca-83c8-53193bdc12ab",
    channel_id: "UCs9Jl08ZBrFSYGsj4_iy20Q",
    video_id: "H7_YksHRHP8",
    terminal_reason_code: "uploader_removed",
    attempts_started_before: 5,
  },
  {
    plan_id: "603fcd30-978f-57ea-aaa6-d4fe25e35b95",
    channel_id: "UCwroD4k2RJTCx0gCEEoI1xQ",
    video_id: "3rqJlqrTyYs",
    terminal_reason_code: "private",
    attempts_started_before: 1,
  },
].map(Object.freeze));

const REPRESENTED_JOB_STATES = new Set([
  "waiting",
  "active",
  "delayed",
  "prioritized",
  "waiting-children",
]);

export const INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGET_SQL = `
WITH requested AS (
  SELECT *
    FROM jsonb_to_recordset($1::jsonb) AS target(
      plan_id uuid,
      channel_id text,
      video_id text,
      terminal_reason_code text,
      attempts_started_before integer
    )
)
SELECT requested.plan_id::text AS expected_plan_id,
       requested.channel_id AS expected_channel_id,
       requested.video_id AS expected_video_id,
       requested.terminal_reason_code,
       requested.attempts_started_before,
       run.run_id,
       run.channel_id,
       run.plan_id::text AS plan_id,
       run.plan_day::text AS plan_day,
       run.status AS run_status,
       run.detail_status,
       run.publication_finalized_at,
       run.error_message,
       run.result_json #>> '{domains,video,status}' AS video_status,
       run.result_json ->> 'job_id' AS job_id,
       batch.cycle_key,
       batch.status AS batch_status,
       item.phase AS item_phase,
       item.status AS item_status,
       item.ordinal AS item_ordinal,
       failure.observation_id AS failure_observation_id,
       failure.outcome AS failure_outcome,
       failure.outcome_reason_code AS failure_reason_code,
       failure.outbox_status
  FROM requested
  LEFT JOIN crawler.channel_runs AS run
    ON run.plan_id=requested.plan_id
   AND run.channel_id=requested.channel_id
   AND run.crawl_mode='incremental'
  LEFT JOIN crawler.incremental_youtubejs_video_batches AS batch
    ON batch.run_id=run.run_id
   AND batch.cycle_key='base'
  LEFT JOIN crawler.incremental_youtubejs_video_items AS item
    ON item.run_id=batch.run_id
   AND item.cycle_key=batch.cycle_key
   AND item.video_id=requested.video_id
  LEFT JOIN LATERAL (
    SELECT observation.observation_id,
           observation.outcome,
           observation.outcome_reason_code,
           outbox.status AS outbox_status
      FROM crawler.crawl_observations AS observation
      LEFT JOIN crawler.crawler_outbox AS outbox
        ON outbox.observation_id=observation.observation_id
     WHERE observation.run_id=run.run_id
       AND observation.observation_kind='video'
     ORDER BY observation.kind_sequence DESC
     LIMIT 1
  ) AS failure ON true
 WHERE run.plan_day=$2::date OR run.plan_day IS NULL
 ORDER BY requested.channel_id,requested.plan_id`;

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function positiveInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return parsed;
}

function nonNegativeInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new TypeError(`${field} must be a non-negative integer`);
  }
  return parsed;
}

function normalizedPlanDay(value) {
  const output = requiredText(value, "planDay");
  if (output !== PLAN_DAY) {
    throw new Error(`this recovery only supports plan day ${PLAN_DAY}`);
  }
  return output;
}

function assertSame(actual, expected, field) {
  if (String(actual ?? "") !== String(expected ?? "")) {
    throw new Error(`Incremental YouTubeJS terminal recovery ${field} changed`);
  }
}

function expectedFailureReason(terminalReasonCode) {
  return terminalReasonCode === "private"
    ? "video_parser_failure"
    : "video_crawler_failure";
}

function assertOriginalFailure(target) {
  const message = requiredText(target.error_message, "error_message");
  if (target.terminal_reason_code === "uploader_removed") {
    if (!/^this video is unavailable[.!]?$/i.test(message)) {
      throw new Error(`unexpected uploader-removed source failure: ${target.plan_id}`);
    }
    return;
  }
  if (target.terminal_reason_code !== "private"
      || !message.includes("[PARSER_CONTRACT_ERROR]")
      || !message.includes("unsupported_playability_status")
      || !/login_required|please sign in/i.test(message)) {
    throw new Error(`unexpected private-video source failure: ${target.plan_id}`);
  }
}

function normalizedTarget(row) {
  const expectedPlanId = requiredText(row?.expected_plan_id, "expected_plan_id");
  const expected = INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGETS.find(
    (target) => target.plan_id === expectedPlanId,
  );
  if (!expected) throw new Error(`unexpected recovery Plan: ${expectedPlanId}`);
  assertSame(row.expected_channel_id, expected.channel_id, "expected channel_id");
  assertSame(row.expected_video_id, expected.video_id, "expected video_id");
  assertSame(row.terminal_reason_code, expected.terminal_reason_code, "terminal reason");
  assertSame(
    positiveInteger(row.attempts_started_before, "attempts_started_before"),
    expected.attempts_started_before,
    "attempt baseline",
  );
  assertSame(row.plan_id, expected.plan_id, "plan_id");
  assertSame(row.channel_id, expected.channel_id, "channel_id");
  assertSame(row.run_id, `incremental:${expected.plan_id}`, "run_id");
  assertSame(row.plan_day, PLAN_DAY, "plan_day");
  assertSame(row.run_status, "failed", "Run status");
  assertSame(row.detail_status, "failed", "Run detail status");
  assertSame(row.video_status, "failed", "Video Domain status");
  if (row.publication_finalized_at != null) {
    throw new Error(`recovery Run is already publication-finalized: ${expected.plan_id}`);
  }
  assertSame(row.cycle_key, "base", "Batch cycle key");
  assertSame(row.batch_status, "fetching", "Batch status");
  assertSame(row.item_status, "pending", "target Item status");
  if (!["first_seen", "recent"].includes(String(row.item_phase ?? ""))) {
    throw new Error(`invalid target Item phase: ${expected.plan_id}`);
  }
  nonNegativeInteger(row.item_ordinal, "item_ordinal");
  assertSame(row.failure_outcome, "failed", "latest Video Observation outcome");
  assertSame(
    row.failure_reason_code,
    expectedFailureReason(expected.terminal_reason_code),
    "latest Video Observation reason",
  );
  assertSame(row.outbox_status, "published", "failure Outbox status");
  requiredText(row.failure_observation_id, "failure_observation_id");
  requiredText(row.job_id, "job_id");
  const normalized = { ...row, ...expected };
  assertOriginalFailure(normalized);
  return normalized;
}

export async function loadIncrementalYoutubeJsTerminalProbeRecoveryTargets(query, {
  planDay,
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const normalizedDay = normalizedPlanDay(planDay);
  const result = await query(INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGET_SQL, [
    JSON.stringify(INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGETS),
    normalizedDay,
  ]);
  const targets = (result?.rows ?? []).map(normalizedTarget);
  if (targets.length !== INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGETS.length) {
    throw new Error(
      `Incremental YouTubeJS terminal recovery target count changed: `
      + `expected ${INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_TARGETS.length}, got ${targets.length}`,
    );
  }
  const selected = new Set(targets.map((target) => target.plan_id));
  if (selected.size !== targets.length) {
    throw new Error("Incremental YouTubeJS terminal recovery contains duplicate Plans");
  }
  return targets;
}

function assertJobIdentity(target, job) {
  if (!job) throw new Error(`Incremental recovery Job is missing: ${target.job_id}`);
  assertSame(job.id, target.job_id, "Job ID");
  assertSame(job.name, "channel.incremental.plan", "Job name");
  assertSame(job.data?.job_id, target.job_id, "Job payload ID");
  assertSame(job.data?.plan_id, target.plan_id, "Job Plan ID");
  assertSame(job.data?.channel_id, target.channel_id, "Job channel ID");
  if (job.data?.task_mask?.video !== true) {
    throw new Error(`Incremental recovery Job is not a Video Plan: ${target.job_id}`);
  }
  assertSame(positiveInteger(job.opts?.attempts, "job.opts.attempts"), 5, "Job attempts");
}

export async function classifyIncrementalYoutubeJsTerminalProbeRecoveryTarget(target, job) {
  assertJobIdentity(target, job);
  const state = requiredText(await job.getState(), "BullMQ Job state");
  const attemptsMade = positiveInteger(job.attemptsMade, "job.attemptsMade");
  const attemptsStarted = positiveInteger(job.attemptsStarted, "job.attemptsStarted");
  const baseline = positiveInteger(target.attempts_started_before, "attempts_started_before");
  if (attemptsStarted < baseline || attemptsMade < baseline) {
    throw new Error(`Incremental recovery Job attempt history regressed: ${target.job_id}`);
  }
  if (state === "failed" && attemptsStarted === baseline && attemptsMade === baseline) {
    return { ...target, action: "retry_failed", job_state: state, attemptsMade, attemptsStarted };
  }
  if (REPRESENTED_JOB_STATES.has(state) && attemptsStarted >= baseline) {
    return { ...target, action: "recovery_in_progress", job_state: state, attemptsMade, attemptsStarted };
  }
  if (state === "completed" && attemptsStarted > baseline) {
    return { ...target, action: "recovery_completed", job_state: state, attemptsMade, attemptsStarted };
  }
  if (state === "failed" && attemptsStarted > baseline) {
    return { ...target, action: "recovery_failed", job_state: state, attemptsMade, attemptsStarted };
  }
  throw new Error(`unsupported recovery Job state ${state}: ${target.job_id}`);
}

async function inspectTargets(queue, targets) {
  const output = [];
  for (const target of targets) {
    output.push(await classifyIncrementalYoutubeJsTerminalProbeRecoveryTarget(
      target,
      await queue.getJob(target.job_id),
    ));
  }
  return output;
}

function actionCounts(inspections) {
  const counts = new Map();
  for (const inspection of inspections) {
    counts.set(inspection.action, (counts.get(inspection.action) ?? 0) + 1);
  }
  return Object.fromEntries([...counts].sort(([left], [right]) => left.localeCompare(right)));
}

export async function dispatchIncrementalYoutubeJsTerminalProbeRecoveryTarget(queue, target) {
  const job = await queue.getJob(target.job_id);
  const inspection = await classifyIncrementalYoutubeJsTerminalProbeRecoveryTarget(target, job);
  if (inspection.action !== "retry_failed") return inspection;
  await job.retry("failed");
  return { ...inspection, action: "retried_failed" };
}

export async function recoverIncrementalYoutubeJsTerminalProbeFailures({
  query,
  queue,
  planDay,
  expectedCount = null,
  confirmation = null,
  apply = false,
} = {}) {
  if (!queue || typeof queue.getJob !== "function") {
    throw new TypeError("the Incremental BullMQ queue is required");
  }
  const targets = await loadIncrementalYoutubeJsTerminalProbeRecoveryTargets(query, { planDay });
  if (expectedCount !== null) {
    assertSame(
      positiveInteger(expectedCount, "expectedCount"),
      targets.length,
      "expected target count",
    );
  }
  const inspections = await inspectTargets(queue, targets);
  const summary = {
    apply: Boolean(apply),
    operation_id: INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_OPERATION_ID,
    plan_day: PLAN_DAY,
    target_count: targets.length,
    action_counts: actionCounts(inspections),
    targets: inspections.map((inspection) => ({
      plan_id: inspection.plan_id,
      channel_id: inspection.channel_id,
      video_id: inspection.video_id,
      terminal_reason_code: inspection.terminal_reason_code,
      job_id: inspection.job_id,
      job_state: inspection.job_state,
      attempts_made: inspection.attemptsMade,
      attempts_started: inspection.attemptsStarted,
      action: inspection.action,
    })),
    dispatch_counts: {},
    dispatch_errors: [],
  };
  if (!apply) return summary;
  if (expectedCount === null) throw new Error("expectedCount is required when apply=true");
  if (confirmation !== INCREMENTAL_YOUTUBEJS_TERMINAL_PROBE_RECOVERY_OPERATION_ID) {
    throw new Error("the exact recovery operation ID is required when apply=true");
  }

  const dispatched = [];
  for (const target of targets) {
    try {
      dispatched.push(await dispatchIncrementalYoutubeJsTerminalProbeRecoveryTarget(queue, target));
    } catch (error) {
      summary.dispatch_errors.push({
        plan_id: target.plan_id,
        job_id: target.job_id,
        error: String(error?.message ?? error),
      });
    }
  }
  summary.dispatch_counts = actionCounts(dispatched);
  return summary;
}
