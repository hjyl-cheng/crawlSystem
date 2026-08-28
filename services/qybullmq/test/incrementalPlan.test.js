import assert from "node:assert/strict";
import test from "node:test";
import {
  incrementalPlanHash,
  incrementalRunId,
  IncrementalPlanContractError,
  validateIncrementalJob,
  validateIncrementalPlan,
} from "../src/incrementalPlan.js";

function fixture() {
  return {
    schema_version: 5,
    dispatch_generation: 1,
    job_id: "incremental__UCtest__20260720__clock_7__5d62c032cbbc",
    plan_id: "4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d",
    plan_mode: "standard",
    plan_day: "2026-07-20",
    scheduled_at: "2026-07-20T13:25:40.000Z",
    channel_id: "UCtest",
    task_mask: { about: true, video: false, agent: true },
    capacity: { factor: 0.75, player_cap: 20, next_cap: 8, version: "capacity-1" },
    clock_version: 7,
    policy_version: "v16-rule-1",
    planner_config_version: "video-plan-1",
  };
}

test("incremental Plan accepts only the frozen three-domain contract", () => {
  const payload = fixture();
  const plan = validateIncrementalPlan(payload);
  assert.deepEqual(plan.task_mask, {
    about: true,
    video: false,
    agent: true,
  });
  assert.equal(incrementalRunId(plan.plan_id), `incremental:${plan.plan_id}`);
  assert.equal(plan.scheduled_at, "2026-07-20T13:25:40.000Z");
  assert.match(incrementalPlanHash(plan), /^sha256:[0-9a-f]{64}$/);
});

test("schema v5 scheduled_at must stay inside plan_day UTC", () => {
  assert.throws(
    () => validateIncrementalPlan({
      ...fixture(),
      scheduled_at: "2026-07-21T00:00:00.000Z",
    }),
    /inside plan_day UTC/,
  );
});

test("schema v5 accepts only a Video-only dormant probe", () => {
  const payload = {
    ...fixture(),
    plan_mode: "dormant_probe",
    task_mask: { about: false, video: true, agent: false },
  };
  const plan = validateIncrementalPlan(payload);
  assert.equal(plan.plan_mode, "dormant_probe");
  assert.throws(() => validateIncrementalPlan({
    ...payload,
    task_mask: { ...payload.task_mask, about: true },
  }), /only the Video task/);
});

test("legacy schemas and any Profile task key are rejected", () => {
  const payload = fixture();
  assert.throws(() => validateIncrementalPlan({
    ...payload,
    schema_version: 3,
  }), /schema_version must be 5/);
  assert.throws(() => validateIncrementalPlan({
    ...payload,
    task_mask: { profile: true, ...payload.task_mask },
  }), /keys differ from the contract/);
});

test("incremental Plan rejects Video IDs and the removed subtask mask", () => {
  const videoIds = { ...fixture(), video_ids: ["video-1"] };
  assert.throws(() => validateIncrementalPlan(videoIds), IncrementalPlanContractError);

  const oldMask = {
    ...fixture(),
    video_mask: { discovery: true, recent_sampling: true },
  };
  assert.throws(() => validateIncrementalPlan(oldMask), /keys differ/);
});

test("incremental Job cannot arrive on the full-crawl queue", () => {
  const data = fixture();
  assert.throws(() => validateIncrementalJob({
    id: data.job_id,
    name: "channel.incremental.plan",
    queueName: "youtube-channel-crawl",
    data,
  }), /queue must be youtube-channel-incremental/);

  assert.equal(validateIncrementalJob({
    id: data.job_id,
    name: "channel.incremental.plan",
    queueName: "youtube-channel-incremental",
    data,
  }).plan_id, data.plan_id);
});
