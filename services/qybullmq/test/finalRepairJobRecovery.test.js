import assert from "node:assert/strict";
import test from "node:test";
import { ensureFinalRepairJob } from "../src/finalRepairJobRecovery.js";
import { finalRepairDispatchDecision } from "../src/repairPolicy.js";

const spec = {
  name: "channel-crawl-repair",
  data: {
    channel_id: "UCrepair",
    repair_parent_run_id: "run:parent",
    repair_round: 3,
  },
  options: { jobId: "final-repair__run_parent__3", attempts: 3 },
};

test("a failed Final Repair is retried in place without resetting its attempt history", async () => {
  const retryStates = [];
  const job = {
    id: spec.options.jobId,
    name: spec.name,
    data: { ...spec.data, run_id: "run:child", business_run_key: "full-repair:auto:parent:3" },
    attemptsMade: 3,
    attemptsStarted: 3,
    async getState() { return "failed"; },
    async updateData(data) { this.data = data; },
    async retry(state) { retryStates.push(state); },
  };
  const queue = {
    async getJob(id) {
      assert.equal(id, spec.options.jobId);
      return job;
    },
    async add() {
      assert.fail("an existing failed Job must not be deleted or recreated");
    },
  };

  const result = await ensureFinalRepairJob(queue, spec);

  assert.deepEqual(retryStates, ["failed"]);
  assert.equal(job.attemptsMade, 3);
  assert.equal(result.action, "retried_failed");
  assert.equal(result.attempts_made, 3);
});

test("Controller recovery adds at most one execution to the same logical Final Repair Job", async () => {
  const retryStates = [];
  const prepared = [];
  const job = {
    id: spec.options.jobId,
    name: spec.name,
    data: { ...spec.data },
    attemptsMade: 3,
    attemptsStarted: 3,
    async getState() { return "failed"; },
    async updateData(data) { this.data = data; },
    async retry(state) { retryStates.push(state); },
  };
  const queue = {
    async getJob() { return job; },
    async add() { assert.fail("an existing failed Job must not be recreated"); },
  };

  const first = await ensureFinalRepairJob(queue, {
    ...spec,
    beforeDispatch: async ({ action }) => prepared.push(action),
  });
  assert.equal(first.action, "retried_failed");
  assert.deepEqual(retryStates, ["failed"]);
  assert.deepEqual(prepared, ["retry_failed"]);

  // The recovered execution was claimed and failed before the Controller could advance the DB round.
  job.attemptsMade += 1;
  job.attemptsStarted += 1;
  const second = await ensureFinalRepairJob(queue, {
    ...spec,
    beforeDispatch: async ({ action }) => prepared.push(action),
  });

  assert.equal(second.action, "recovery_already_consumed");
  assert.deepEqual(retryStates, ["failed"]);
  assert.deepEqual(prepared, ["retry_failed"]);
  assert.equal(job.attemptsMade, 4);
});

test("Controller recovery resumes after a crash between persisting its marker and retrying", async () => {
  const retryStates = [];
  const job = {
    id: spec.options.jobId,
    name: spec.name,
    data: {
      ...spec.data,
      final_repair_controller_retry_from_attempts_started: 3,
    },
    attemptsMade: 3,
    attemptsStarted: 3,
    async getState() { return "failed"; },
    async updateData() { assert.fail("the existing durable marker must be reused"); },
    async retry(state) { retryStates.push(state); },
  };

  const result = await ensureFinalRepairJob({
    async getJob() { return job; },
    async add() { assert.fail("an existing failed Job must not be recreated"); },
  }, spec);

  assert.equal(result.action, "retried_failed");
  assert.deepEqual(retryStates, ["failed"]);
});

test("Controller recovery preserves Detail intent after candidate reset and a retry crash", async () => {
  const candidateState = {
    failedCandidates: 1,
    repairableCandidates: 1,
    typeMissingCandidates: 0,
    preparedDetailCandidates: 0,
  };
  let retryCalls = 0;
  const job = {
    id: spec.options.jobId,
    name: "channel-detail-repair",
    data: {
      channel_id: spec.data.channel_id,
      run_id: "run:parent",
      repair_round: spec.data.repair_round,
    },
    attemptsMade: 3,
    attemptsStarted: 3,
    async getState() { return "failed"; },
    async updateData(data) { this.data = data; },
    async retry() {
      retryCalls += 1;
      if (retryCalls === 1) throw new Error("simulated crash before BullMQ retry completed");
    },
  };
  const queue = {
    async getJob() { return job; },
    async add() { assert.fail("the failed Detail Repair must be recovered in place"); },
  };
  const currentSpec = () => {
    const dispatch = finalRepairDispatchDecision({
      publicationGap: false,
      ...candidateState,
    });
    return {
      name: dispatch.name,
      data: {
        channel_id: spec.data.channel_id,
        ...(dispatch.detailOnly
          ? { run_id: "run:parent" }
          : { repair_parent_run_id: "run:parent" }),
        repair_round: spec.data.repair_round,
      },
      options: spec.options,
      beforeDispatch: async () => {
        candidateState.failedCandidates = 0;
        candidateState.repairableCandidates = 0;
        candidateState.preparedDetailCandidates = 1;
      },
    };
  };

  await assert.rejects(
    ensureFinalRepairJob(queue, currentSpec()),
    /simulated crash/,
  );
  candidateState.typeMissingCandidates = 1;
  const recovered = await ensureFinalRepairJob(queue, currentSpec());

  assert.equal(recovered.action, "retried_failed");
  assert.equal(retryCalls, 2);
});

test("Controller recovery preserves Detail intent after candidate reset and an enqueue crash", async () => {
  const candidateState = {
    failedCandidates: 1,
    repairableCandidates: 1,
    typeMissingCandidates: 0,
    preparedDetailCandidates: 0,
  };
  let addCalls = 0;
  const addedNames = [];
  const queue = {
    async getJob() { return null; },
    async add(name, data, options) {
      addCalls += 1;
      addedNames.push(name);
      if (addCalls === 1) throw new Error("simulated crash before BullMQ enqueue completed");
      return { id: options.jobId, name, data, attemptsMade: 0 };
    },
  };
  const currentSpec = () => {
    const dispatch = finalRepairDispatchDecision({
      publicationGap: false,
      ...candidateState,
    });
    return {
      name: dispatch.name,
      data: {
        channel_id: spec.data.channel_id,
        ...(dispatch.detailOnly
          ? { run_id: "run:parent" }
          : { repair_parent_run_id: "run:parent" }),
        repair_round: spec.data.repair_round,
      },
      options: spec.options,
      beforeDispatch: async () => {
        candidateState.failedCandidates = 0;
        candidateState.repairableCandidates = 0;
        candidateState.preparedDetailCandidates = 1;
      },
    };
  };

  await assert.rejects(
    ensureFinalRepairJob(queue, currentSpec()),
    /simulated crash/,
  );
  const recovered = await ensureFinalRepairJob(queue, currentSpec());

  assert.equal(recovered.action, "enqueued");
  assert.deepEqual(addedNames, ["channel-detail-repair", "channel-detail-repair"]);
});

test("Controller recovery fails closed when its durable attempt marker is ahead of BullMQ", async () => {
  const job = {
    id: spec.options.jobId,
    name: spec.name,
    data: {
      ...spec.data,
      final_repair_controller_retry_from_attempts_started: 4,
    },
    attemptsMade: 3,
    attemptsStarted: 3,
    async getState() { return "failed"; },
    async retry() { assert.fail("regressed attempt history must not be retried"); },
  };

  await assert.rejects(
    ensureFinalRepairJob({
      async getJob() { return job; },
      async add() { assert.fail("a corrupt Job must not be recreated"); },
    }, spec),
    /regressed attempt history/,
  );
});

test("a mismatched failed Job is rejected instead of being retried", async () => {
  const job = {
    id: spec.options.jobId,
    name: spec.name,
    data: { ...spec.data, channel_id: "UCother" },
    attemptsMade: 3,
    async getState() { return "failed"; },
    async retry() { assert.fail("a mismatched Job must not be retried"); },
  };
  await assert.rejects(
    ensureFinalRepairJob({
      async getJob() { return job; },
      async add() { assert.fail("a mismatched Job must not be recreated"); },
    }, spec),
    /conflicting channel_id/,
  );
});

test("a failed Publication Gap Job cannot be retried for a different root Run", async () => {
  const gapSpec = {
    ...spec,
    data: {
      ...spec.data,
      publication_gap_domains: ["channel", "video"],
      publication_gap_root_run_id: "run:parent",
    },
  };
  const job = {
    id: spec.options.jobId,
    name: spec.name,
    data: {
      ...gapSpec.data,
      publication_gap_root_run_id: "run:other",
    },
    attemptsMade: 3,
    async getState() { return "failed"; },
    async retry() { assert.fail("a mismatched Publication Gap Job must not be retried"); },
  };

  await assert.rejects(
    ensureFinalRepairJob({
      async getJob() { return job; },
      async add() { assert.fail("a mismatched Job must not be recreated"); },
    }, gapSpec),
    /conflicting publication_gap_root_run_id/,
  );
});

test("Publication Gap Job identity compares the normalized repair domain set", async () => {
  const retries = [];
  const gapSpec = {
    ...spec,
    data: {
      ...spec.data,
      publication_gap_domains: ["channel", "video"],
      publication_gap_root_run_id: "run:parent",
    },
  };
  const matchingJob = {
    id: spec.options.jobId,
    name: spec.name,
    data: {
      ...gapSpec.data,
      publication_gap_domains: ["video", "channel", "channel"],
    },
    attemptsMade: 3,
    attemptsStarted: 3,
    async getState() { return "failed"; },
    async updateData(data) { this.data = data; },
    async retry(state) { retries.push(state); },
  };
  const queue = {
    job: matchingJob,
    async getJob() { return this.job; },
    async add() { assert.fail("an existing Job must not be recreated"); },
  };

  const matching = await ensureFinalRepairJob(queue, gapSpec);
  assert.equal(matching.action, "retried_failed");
  assert.deepEqual(retries, ["failed"]);

  queue.job = {
    ...matchingJob,
    data: { ...gapSpec.data, publication_gap_domains: ["channel"] },
    async retry() { assert.fail("a different Publication Gap domain set must not be retried"); },
  };
  await assert.rejects(
    ensureFinalRepairJob(queue, gapSpec),
    /conflicting publication_gap_domains/,
  );
});

test("an ordinary Repair cannot reuse an existing Publication Gap Job", async () => {
  const job = {
    id: spec.options.jobId,
    name: spec.name,
    data: {
      ...spec.data,
      publication_gap_domains: ["channel"],
      publication_gap_root_run_id: "run:parent",
    },
    attemptsMade: 3,
    async getState() { return "failed"; },
    async retry() { assert.fail("Publication Gap evidence is not an optional Job identity field"); },
  };

  await assert.rejects(
    ensureFinalRepairJob({
      async getJob() { return job; },
      async add() { assert.fail("an existing Job must not be recreated"); },
    }, spec),
    /conflicting publication_gap_/,
  );
});

test("a missing Final Repair Job is enqueued with its original retry options", async () => {
  const added = [];
  const order = [];
  const result = await ensureFinalRepairJob({
    async getJob() { return null; },
    async add(name, data, options) {
      order.push("add");
      added.push({ name, data, options });
      return { id: options.jobId, attemptsMade: 0 };
    },
  }, {
    ...spec,
    beforeDispatch: async ({ action, job }) => {
      assert.equal(action, "enqueue");
      assert.equal(job, null);
      order.push("prepare");
    },
  });

  assert.deepEqual(added, [spec]);
  assert.deepEqual(order, ["prepare", "add"]);
  assert.equal(result.action, "enqueued");
});
