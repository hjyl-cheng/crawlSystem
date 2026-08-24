import assert from "node:assert/strict";
import test from "node:test";

import {
  assertFullRepairDatabaseIdentity,
  assertFullRepairExecutionAuthorized,
  buildFullRepairManifest,
  dispatchFullRepairPass,
  fullRepairChannelJob,
  fullRepairRunMetadata,
  fullRepairSchedulerConflict,
  loadFullRepairTargets,
  loadFullRepairCompletionState,
  loadFullRepairDispatchState,
  parseFullRepairArgs,
  parseFullRepairChannelFile,
  prepareFullRepairBatch,
} from "../src/fullRepairDispatch.js";

test("a Full Repair manifest has a canonical Channel set and confirmation token", () => {
  const manifest = buildFullRepairManifest({
    batchId: "repair-20260727-v1",
    channelIds: [
      "UCabcdefghijklmnopqrstuv",
      "UC1234567890123456789012",
    ],
  });

  assert.deepEqual(manifest, {
    version: "publication-full-repair-manifest-v2",
    batch_id: "repair-20260727-v1",
    target_count: 2,
    target_channel_ids: [
      "UC1234567890123456789012",
      "UCabcdefghijklmnopqrstuv",
    ],
    scan_policy_version: "publication-video-window-repair-v1",
    content_limit: 100,
    content_max_age_days: 90,
    manifest_hash: "sha256:0062d9a816a15da92fad8c11f11084da82d6893b5b30dd8476bc3bff1087c011",
    confirmation: "confirm-full-repair:repair-20260727-v1:2:0062d9a816a15da9",
  });
});

test("Full Repair defaults to dry-run and execute requires the exact manifest confirmation", () => {
  const options = parseFullRepairArgs([
    "--channels-file", "/tmp/repair-channels.txt",
    "--batch-id", "repair-20260727-v1",
  ], {});
  assert.deepEqual(options, {
    channelsFile: "/tmp/repair-channels.txt",
    batchId: "repair-20260727-v1",
    execute: false,
    watch: false,
    retryFailed: false,
    confirm: null,
    highWater: 5,
    refill: 2,
    pollMs: 15000,
  });

  const manifest = buildFullRepairManifest({
    batchId: options.batchId,
    channelIds: ["UC1234567890123456789012"],
  });
  assert.equal(assertFullRepairExecutionAuthorized(options, manifest), false);
  assert.throws(
    () => assertFullRepairExecutionAuthorized({ ...options, execute: true, confirm: "wrong" }, manifest),
    /confirmation token does not match/,
  );
  assert.equal(assertFullRepairExecutionAuthorized({
    ...options,
    execute: true,
    confirm: manifest.confirmation,
  }, manifest), true);
});

test("Full Repair accepts only the initialized fresh Crawler Writer database", async () => {
  await assert.rejects(
    assertFullRepairDatabaseIdentity(async () => ({
      rows: [{
        database_name: "bullmq_crawler_migration",
        database_user: "bullmq",
        transaction_read_only: "off",
        identity_kind: "crawler",
        identity_database: "bullmq_crawler_migration",
        schema_ready: true,
      }],
    }), {
      EXPECTED_CRAWLER_DATABASE: "bullmq_crawler_migration",
      FORBIDDEN_CRAWLER_DATABASE: "bullmq_crawler_migration",
    }),
    /forbidden Crawler database bullmq_crawler_migration/,
  );
  assert.deepEqual(
    await assertFullRepairDatabaseIdentity(async () => ({
      rows: [{
        database_name: "newcrawler_crawler",
        database_user: "bullmq",
        transaction_read_only: "off",
        identity_kind: "crawler",
        identity_database: "newcrawler_crawler",
        schema_ready: true,
      }],
    }), {
      EXPECTED_CRAWLER_DATABASE: "newcrawler_crawler",
      FORBIDDEN_CRAWLER_DATABASE: "bullmq_crawler_migration",
    }),
    { database: "newcrawler_crawler", user: "bullmq" },
  );
});

test("Full Repair cannot replace another crawler pipeline and only resumes its own repairing batch", () => {
  assert.deepEqual(
    fullRepairSchedulerConflict(
      { status: "finishing", pipeline_cycle_id: "tomorrow-qy-cycle" },
      "repair-20260727-v1",
    ),
    {
      code: "pipeline_busy",
      message: "crawler pipeline tomorrow-qy-cycle is finishing",
    },
  );
  assert.equal(
    fullRepairSchedulerConflict(
      { status: "repairing", pipeline_cycle_id: "repair-20260727-v1" },
      "repair-20260727-v1",
    ),
    null,
  );
  assert.equal(
    fullRepairSchedulerConflict(
      { status: "stopped", pipeline_cycle_id: "completed-old-cycle" },
      "repair-20260727-v1",
    ),
    null,
  );
  assert.equal(
    fullRepairSchedulerConflict(
      { status: "paused", pipeline_cycle_id: "repair-20260727-v1" },
      "repair-20260727-v1",
    ).code,
    "pipeline_paused",
  );
});

test("Full Repair resolves every manifest Channel to an active Channel and accepted Candidate using reads only", async () => {
  const manifest = buildFullRepairManifest({
    batchId: "repair-20260727-v1",
    channelIds: [
      "UC1234567890123456789012",
      "UCabcdefghijklmnopqrstuv",
    ],
  });
  const calls = [];
  const targets = await loadFullRepairTargets(async (sql, params) => {
    calls.push({ sql, params });
    return {
      rows: [
        {
          ordinal: "1",
          channel_id: "UC1234567890123456789012",
          channel_status: "active",
          channel_url: "https://www.youtube.com/channel/UC1234567890123456789012",
          candidate_id: "42",
          candidate_status: "accepted",
          candidate_channel_url: "https://www.youtube.com/channel/UC1234567890123456789012",
          priority: 75,
        },
        {
          ordinal: "2",
          channel_id: "UCabcdefghijklmnopqrstuv",
          channel_status: "active",
          channel_url: "https://www.youtube.com/channel/UCabcdefghijklmnopqrstuv",
          candidate_id: "43",
          candidate_status: "accepted",
          candidate_channel_url: "https://www.youtube.com/channel/UCabcdefghijklmnopqrstuv",
          priority: 80,
        },
      ],
    };
  }, manifest);

  assert.deepEqual(targets.map((target) => ({
    candidate_id: target.candidate_id,
    channel_id: target.channel_id,
    channel_url: target.channel_url,
    priority: target.priority,
    status: target.status,
  })), [
    {
      candidate_id: 42,
      channel_id: "UC1234567890123456789012",
      channel_url: "https://www.youtube.com/channel/UC1234567890123456789012",
      priority: 75,
      status: "accepted",
    },
    {
      candidate_id: 43,
      channel_id: "UCabcdefghijklmnopqrstuv",
      channel_url: "https://www.youtube.com/channel/UCabcdefghijklmnopqrstuv",
      priority: 80,
      status: "accepted",
    },
  ]);
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].params, [manifest.target_channel_ids]);
  assert.match(calls[0].sql, /candidate\.status='accepted'/);
  assert.doesNotMatch(calls[0].sql, /\b(?:INSERT|UPDATE|DELETE)\b/);
});

test("preparing a Full Repair batch atomically records its manifest and activates repairing without mutating Candidates", async () => {
  const manifest = buildFullRepairManifest({
    batchId: "repair-20260727-v1",
    channelIds: ["UC1234567890123456789012"],
  });
  const calls = [];
  const client = {
    async query(sql, params = []) {
      calls.push({ sql, params });
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return { rows: [{ value_json: { status: "stopped", pipeline_cycle_id: "old-cycle" } }] };
      }
      if (sql.includes("FROM crawler.query_dispatch_batches") && sql.includes("FOR UPDATE")) {
        return { rows: [] };
      }
      return { rows: [], rowCount: 1 };
    },
  };

  const prepared = await prepareFullRepairBatch(client, {
    manifest,
    preparedAt: "2026-07-27T14:00:00.000Z",
  });

  assert.deepEqual(prepared, {
    created: true,
    completed: false,
    prepared_at: "2026-07-27T14:00:00.000Z",
    batch_id: "repair-20260727-v1",
  });
  const batchInsert = calls.find(({ sql }) => sql.includes("INSERT INTO crawler.query_dispatch_batches"));
  assert.ok(batchInsert);
  const batchMetadata = JSON.parse(batchInsert.params[1]);
  assert.deepEqual(batchMetadata.full_repair_dispatch.target_channel_ids, manifest.target_channel_ids);
  assert.equal(batchMetadata.full_repair_dispatch.manifest_hash, manifest.manifest_hash);
  assert.equal(batchMetadata.full_repair_dispatch.scan_policy_version, manifest.scan_policy_version);
  assert.equal(batchMetadata.full_repair_dispatch.content_limit, 100);
  assert.equal(batchMetadata.full_repair_dispatch.content_max_age_days, 90);
  assert.equal(batchMetadata.full_repair_dispatch.status, "dispatching");
  const schedulerUpdate = calls.find(({ sql }) => sql.includes("UPDATE crawler.settings"));
  assert.ok(schedulerUpdate);
  assert.match(schedulerUpdate.sql, /'status','repairing'/);
  assert.equal(
    calls.some(({ sql }) => sql.includes("UPDATE crawler.channel_candidates")),
    false,
  );
});

test("re-preparing the same Full Repair batch preserves its persisted dispatch progress", async () => {
  const manifest = buildFullRepairManifest({
    batchId: "repair-20260727-v1",
    channelIds: [
      "UC1234567890123456789012",
      "UCabcdefghijklmnopqrstuv",
    ],
  });
  const storedRepair = {
    version: manifest.version,
    batch_id: manifest.batch_id,
    manifest_hash: manifest.manifest_hash,
    target_count: manifest.target_count,
    target_channel_ids: manifest.target_channel_ids,
    scan_policy_version: manifest.scan_policy_version,
    content_limit: manifest.content_limit,
    content_max_age_days: manifest.content_max_age_days,
    status: "dispatching",
    dispatch_cursor: 1,
    prepared_at: "2026-07-27T14:00:00.000Z",
    last_dispatch_at: "2026-07-27T14:01:00.000Z",
    dispatched_at: null,
  };
  const calls = [];
  const client = {
    async query(sql, params = []) {
      calls.push({ sql, params });
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return {
          rows: [{
            value_json: {
              status: "repairing",
              pipeline_cycle_id: manifest.batch_id,
            },
          }],
        };
      }
      if (sql.includes("FROM crawler.query_dispatch_batches") && sql.includes("FOR UPDATE")) {
        return {
          rows: [{
            status: "finishing",
            result_json: { full_repair_dispatch: storedRepair },
          }],
        };
      }
      return { rows: [], rowCount: 1 };
    },
  };

  const prepared = await prepareFullRepairBatch(client, { manifest });
  const batchUpdate = calls.find(({ sql }) => sql.includes("UPDATE crawler.query_dispatch_batches"));
  const persisted = JSON.parse(batchUpdate.params[1]);

  assert.equal(prepared.created, false);
  assert.equal(persisted.dispatch_cursor, 1);
  assert.equal(persisted.last_dispatch_at, "2026-07-27T14:01:00.000Z");
});

test("a Full Repair pass respects global queue pressure and persists a resumable dispatch cursor", async () => {
  const manifest = buildFullRepairManifest({
    batchId: "repair-20260727-v1",
    channelIds: [
      "UC1234567890123456789012",
      "UCabcdefghijklmnopqrstuv",
    ],
  });
  const targets = manifest.target_channel_ids.map((channelId, index) => ({
    candidate_id: 42 + index,
    channel_id: channelId,
    channel_url: `https://www.youtube.com/channel/${channelId}`,
    priority: 75 + index,
    status: "accepted",
  }));
  const dbCalls = [];
  const added = [];
  const queue = {
    async getJobCounts() {
      return { waiting: 3, active: 1, delayed: 0, prioritized: 0, paused: 0, "waiting-children": 0 };
    },
    async isPaused() { return false; },
    async getJob() { return null; },
    async add(name, data, options) {
      added.push({ name, data, options });
      return { id: options.jobId };
    },
  };
  const dbQuery = async (sql, params = []) => {
    dbCalls.push({ sql, params });
    if (sql.includes("FROM crawler.channel_runs")) return { rows: [] };
    return { rows: [], rowCount: 1 };
  };

  const result = await dispatchFullRepairPass({
    dbQuery,
    queue,
    manifest,
    targets,
    preparedAt: "2026-07-27T14:00:00.000Z",
    dispatchCursor: 0,
    highWater: 5,
    refill: 2,
    now: "2026-07-27T14:01:00.000Z",
  });

  assert.equal(result.dispatched, 1);
  assert.equal(result.dispatch_cursor, 1);
  assert.equal(result.status, "dispatching");
  assert.equal(result.pressure, 4);
  assert.equal(added.length, 1);
  assert.equal(added[0].name, "channel-full-repair");
  assert.equal(added[0].data.enforce_min_subscribers, false);
  assert.equal(added[0].data.run_id, `full-repair:repair-20260727-v1:${manifest.target_channel_ids[0]}`);
  const progress = dbCalls.find(({ sql }) => sql.includes("UPDATE crawler.query_dispatch_batches"));
  assert.ok(progress);
  assert.equal(JSON.parse(progress.params[1]).dispatch_cursor, 1);
});

test("Full Repair only replaces a terminal failed job during an explicit retry pass", async () => {
  const manifest = buildFullRepairManifest({
    batchId: "repair-20260727-v1",
    channelIds: ["UC1234567890123456789012"],
  });
  const target = {
    candidate_id: 42,
    channel_id: manifest.target_channel_ids[0],
    channel_url: `https://www.youtube.com/channel/${manifest.target_channel_ids[0]}`,
    priority: 75,
    status: "accepted",
  };
  let removed = false;
  const added = [];
  const failedJob = {
    async getState() { return "failed"; },
    async remove() { removed = true; },
  };
  const queue = {
    async getJobCounts() { return {}; },
    async isPaused() { return false; },
    async getJob() { return failedJob; },
    async add(name, data, options) {
      added.push({ name, data, options });
      return { id: options.jobId };
    },
  };
  const result = await dispatchFullRepairPass({
    dbQuery: async (sql) => {
      if (sql.includes("FROM crawler.channel_runs")) {
        return {
          rows: [{
            run_id: `full-repair:repair-20260727-v1:${target.channel_id}`,
            channel_id: target.channel_id,
            status: "failed",
            detail_status: "failed",
          }],
        };
      }
      return { rows: [], rowCount: 1 };
    },
    queue,
    manifest,
    targets: [target],
    preparedAt: "2026-07-27T14:00:00.000Z",
    dispatchCursor: 0,
    retryFailed: true,
  });

  assert.equal(removed, true);
  assert.equal(added.length, 1);
  assert.equal(result.dispatched, 1);
  assert.equal(result.dispatch_cursor, 1);
});

test("Full Repair completion remains blocked until every manifest target is terminal", async () => {
  const state = await loadFullRepairCompletionState(async (sql, params) => {
    assert.match(sql, /jsonb_array_elements_text/);
    assert.deepEqual(params, ["repair-20260727-v1"]);
    return {
      rows: [{
        batch_status: "finishing",
        dispatch_status: "dispatched",
        expected_count: "2",
        target_count: "2",
        succeeded_count: "1",
        open_count: "1",
        failed_count: "0",
        missing_count: "0",
        removed_count: "0",
      }],
    };
  }, "repair-20260727-v1");

  assert.deepEqual(state, {
    batch_id: "repair-20260727-v1",
    batch_status: "finishing",
    dispatch_status: "dispatched",
    expected_count: 2,
    target_count: 2,
    succeeded_count: 1,
    open_count: 1,
    failed_count: 0,
    missing_count: 0,
    removed_count: 0,
    complete: false,
  });
});

test("Full Repair Run metadata carries a stable Publication Repair identity into Finalize", () => {
  assert.deepEqual(fullRepairRunMetadata({
    trigger_reason: "repair",
    repair_batch_id: "repair-20260727-v1",
    repair_reason: "publication_readiness_full_repair",
    repair_prepared_at: "2026-07-27T14:00:00.000Z",
    repair_manifest_hash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    repair_scan_policy_version: "publication-video-window-repair-v1",
    repair_content_limit: 100,
    repair_content_max_age_days: 90,
  }), {
    publication_repair: {
      batch_id: "repair-20260727-v1",
      reason: "publication_readiness_full_repair",
      prepared_at: "2026-07-27T14:00:00.000Z",
      manifest_hash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      mode: "full_crawl",
      scan_policy_version: "publication-video-window-repair-v1",
      content_limit: 100,
      content_max_age_days: 90,
    },
  });
});

test("Full Repair refuses an unversioned or reduced Publication scan depth", () => {
  const base = {
    trigger_reason: "repair",
    repair_batch_id: "repair-20260727-v1",
    repair_reason: "publication_readiness_full_repair",
    repair_prepared_at: "2026-07-27T14:00:00.000Z",
    repair_manifest_hash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    repair_scan_policy_version: "publication-video-window-repair-v1",
    repair_content_limit: 100,
    repair_content_max_age_days: 90,
  };
  assert.throws(
    () => fullRepairRunMetadata({ ...base, repair_scan_policy_version: null }),
    /repair_scan_policy_version is required/,
  );
  assert.throws(
    () => fullRepairRunMetadata({ ...base, repair_content_limit: 30 }),
    /content limit must be 100/,
  );
  assert.throws(
    () => fullRepairRunMetadata({ ...base, repair_content_max_age_days: 30 }),
    /content max age must be 90 days/,
  );
});

test("Full Repair Channel files allow comments but reject duplicate targets", () => {
  assert.deepEqual(parseFullRepairChannelFile([
    "\uFEFF# Publication Readiness repair targets",
    "UCabcdefghijklmnopqrstuv",
    "",
    "UC1234567890123456789012",
    "",
  ].join("\n")), [
    "UC1234567890123456789012",
    "UCabcdefghijklmnopqrstuv",
  ]);
  assert.throws(
    () => parseFullRepairChannelFile([
      "UC1234567890123456789012",
      "UC1234567890123456789012",
    ].join("\n")),
    /duplicate Channel ID at lines 1 and 2/,
  );
});

test("Full Repair resumes from the persisted cursor and original preparation time", async () => {
  const manifest = buildFullRepairManifest({
    batchId: "repair-20260727-v1",
    channelIds: [
      "UC1234567890123456789012",
      "UCabcdefghijklmnopqrstuv",
    ],
  });
  const state = await loadFullRepairDispatchState(async () => ({
    rows: [{
      status: "finishing",
      result_json: {
        full_repair_dispatch: {
          ...manifest,
          batch_id: manifest.batch_id,
          target_channel_ids: manifest.target_channel_ids,
          target_count: manifest.target_count,
          manifest_hash: manifest.manifest_hash,
          status: "dispatching",
          dispatch_cursor: 1,
          prepared_at: "2026-07-27T14:00:00.000Z",
        },
      },
    }],
  }), manifest);

  assert.deepEqual(state, {
    exists: true,
    completed: false,
    batch_status: "finishing",
    dispatch_status: "dispatching",
    dispatch_cursor: 1,
    prepared_at: "2026-07-27T14:00:00.000Z",
  });
});

test("a Full Repair job reuses an accepted Candidate without re-running admission gates", () => {
  const job = fullRepairChannelJob({
    batchId: "publication-readiness-repair-20260727-v1",
    manifestHash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    scanPolicyVersion: "publication-video-window-repair-v1",
    contentLimit: 100,
    contentMaxAgeDays: 90,
    preparedAt: "2026-07-27T14:00:00.000Z",
    candidate: {
      candidate_id: "42",
      channel_id: "UC1234567890123456789012",
      channel_url: "https://www.youtube.com/channel/UC1234567890123456789012",
      priority: 75,
      status: "accepted",
    },
  });

  assert.deepEqual(job, {
    name: "channel-full-repair",
    data: {
      candidate_id: 42,
      dispatch_batch_id: "publication-readiness-repair-20260727-v1",
      pipeline_cycle_id: "publication-readiness-repair-20260727-v1",
      channel_id: "UC1234567890123456789012",
      channel_url: "https://www.youtube.com/channel/UC1234567890123456789012",
      crawl_mode: "full",
      query_id: null,
      query_text: "publication readiness full repair",
      enforce_min_subscribers: false,
      reject_if_no_recent_content: false,
      trigger_reason: "repair",
      repair_batch_id: "publication-readiness-repair-20260727-v1",
      repair_reason: "publication_readiness_full_repair",
      repair_prepared_at: "2026-07-27T14:00:00.000Z",
      repair_manifest_hash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      repair_scan_policy_version: "publication-video-window-repair-v1",
      repair_content_limit: 100,
      repair_content_max_age_days: 90,
      run_id: "full-repair:publication-readiness-repair-20260727-v1:UC1234567890123456789012",
    },
    options: {
      jobId: "channel-full-repair__publication-readiness-repair-20260727-v1__UC1234567890123456789012",
      priority: 75,
    },
  });
});
