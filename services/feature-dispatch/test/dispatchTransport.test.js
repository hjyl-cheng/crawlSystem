import assert from "node:assert/strict";
import test from "node:test";
import {
  canonicalJson,
  DispatchEnvelopeConflict,
  dispatchPayloadHash,
  publishDispatchOutboxRow,
  timestampInUtcClockWindow,
  validateDispatchOutboxRow,
} from "../src/dispatchTransport.js";

function fixture() {
  const payload = {
    schema_version: 5,
    dispatch_generation: 1,
    job_id: "incremental__UCtest__20260720__clock_7__5d62c032cbbc",
    plan_id: "4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d",
    plan_day: "2026-07-20",
    scheduled_at: "2026-07-20T13:25:40.000Z",
    channel_id: "UCtest",
    plan_mode: "standard",
    task_mask: { about: true, video: true, agent: false },
    capacity: { factor: 0.75, player_cap: 20, next_cap: 8, version: "capacity-1" },
    clock_version: 7,
    policy_version: "v16-rule-1",
    planner_config_version: "video-plan-1",
  };
  return {
    dispatch_event_id: "9e043d70-74f0-5e4e-a4c7-a2ac1591f356",
    plan_id: payload.plan_id,
    job_id: payload.job_id,
    queue_name: "youtube-channel-incremental",
    payload_json: payload,
    payload_hash: dispatchPayloadHash(payload),
    plan_scheduled_at: new Date(payload.scheduled_at),
    attempts: 1,
  };
}

test("Dispatch row validates the frozen Plan contract and canonical hash", () => {
  const row = fixture();
  assert.deepEqual(validateDispatchOutboxRow(row), row.payload_json);
  assert.equal(canonicalJson({ b: 2, a: 1 }), '{"a":1,"b":2}');
  assert.equal("video_ids" in row.payload_json, false);
  assert.equal("video_mask" in row.payload_json, false);
});

test("Dispatch hash is stable for the schema v5 generation-fenced contract", () => {
  const payload = fixture().payload_json;

  assert.equal(
    dispatchPayloadHash(payload),
    "sha256:29ca59a90f3955a654388746123077bb1de13d87c7b6d67b4f43f1e7807e100a",
  );
});

test("Dispatch rejects legacy schemas and any Profile task key", () => {
  const legacy = fixture();
  legacy.payload_json.schema_version = 3;
  legacy.payload_hash = dispatchPayloadHash(legacy.payload_json);
  assert.throws(() => validateDispatchOutboxRow(legacy), /schema_version must be 5/);

  const profile = fixture();
  profile.payload_json.task_mask = { profile: false, ...profile.payload_json.task_mask };
  profile.payload_hash = dispatchPayloadHash(profile.payload_json);
  assert.throws(() => validateDispatchOutboxRow(profile), /keys differ from the contract/);
});

test("Dispatch validation rejects hashes, removed Video masks, and extra Video IDs", () => {
  const hash = fixture();
  hash.payload_hash = "sha256:wrong";
  assert.throws(() => validateDispatchOutboxRow(hash), DispatchEnvelopeConflict);

  const videoIds = fixture();
  videoIds.payload_json.video_ids = ["video-1"];
  videoIds.payload_hash = dispatchPayloadHash(videoIds.payload_json);
  assert.throws(() => validateDispatchOutboxRow(videoIds), DispatchEnvelopeConflict);

  const videoMask = fixture();
  videoMask.payload_json.video_mask = { discovery: true, recent_sampling: true };
  videoMask.payload_hash = dispatchPayloadHash(videoMask.payload_json);
  assert.throws(() => validateDispatchOutboxRow(videoMask), DispatchEnvelopeConflict);
});

test("Dispatch validation enforces the UTC Clock window", () => {
  assert.equal(timestampInUtcClockWindow("2026-07-20T00:30:00Z"), true);
  assert.equal(timestampInUtcClockWindow("2026-07-20T21:29:59Z"), true);
  assert.equal(timestampInUtcClockWindow("2026-07-20T00:29:59Z"), false);
  assert.equal(timestampInUtcClockWindow("2026-07-20T21:30:00Z"), false);

  for (const scheduledAt of [
    "2026-07-20T00:29:59Z",
    "2026-07-20T21:30:00Z",
    "2026-07-21T13:25:40Z",
  ]) {
    const invalid = fixture();
    invalid.payload_json.scheduled_at = scheduledAt;
    invalid.plan_scheduled_at = new Date(scheduledAt);
    invalid.payload_hash = dispatchPayloadHash(invalid.payload_json);
    assert.throws(
      () => validateDispatchOutboxRow(invalid),
      DispatchEnvelopeConflict,
    );
  }
});

test("Dispatch Publisher reuses an identical deterministic BullMQ Job", async () => {
  const row = fixture();
  const jobs = new Map();
  const queue = {
    name: row.queue_name,
    async getJob(id) { return jobs.get(id) || null; },
    async add(name, data, options) {
      assert.equal(name, "channel.incremental.plan");
      const job = { id: options.jobId, data };
      jobs.set(job.id, job);
      return job;
    },
  };
  const first = await publishDispatchOutboxRow(queue, row);
  const second = await publishDispatchOutboxRow(queue, row);
  assert.equal(first.job_id, row.job_id);
  assert.equal(second.job_id, row.job_id);
  assert.equal(jobs.size, 1);
});

test("Dispatch Publisher rejects an existing BullMQ Job with a different Plan", async () => {
  const row = fixture();
  const queue = {
    name: row.queue_name,
    async getJob() { return { id: row.job_id, data: { ...row.payload_json, clock_version: 8 } }; },
  };
  await assert.rejects(publishDispatchOutboxRow(queue, row), DispatchEnvelopeConflict);
});
