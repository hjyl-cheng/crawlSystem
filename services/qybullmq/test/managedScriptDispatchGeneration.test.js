import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { buildManagedDiagnosticJob } from "../src/managedDiagnosticJob.js";

const managedScripts = [
  ["youtubejs-canary.mjs", "youtubejs_canary"],
  ["backfillCommentFirstPages.mjs", "comment_backfill"],
  ["probeYoutubeComments.mjs", "comment_probe"],
];

test("the diagnostic Job Builder fixes queue, attempt and generation while keeping IDs unique", () => {
  const ids = [
    "11111111-1111-4111-8111-111111111111",
    "22222222-2222-4222-8222-222222222222",
    "33333333-3333-4333-8333-333333333333",
  ];
  const randomUUID = () => ids.shift();
  const jobs = managedScripts.map(([, kind]) => buildManagedDiagnosticJob({
    kind,
    channelId: `channel-${kind}`,
    runId: kind === "youtubejs_canary" ? null : `run-${kind}`,
    randomUUID,
  }));

  assert.equal(new Set(jobs.map((job) => job.id)).size, 3);
  assert.deepEqual(jobs.map((job) => job.id.split(":")[0]), [
    "youtubejs-canary",
    "comment-backfill",
    "comment-probe",
  ]);
  for (const job of jobs) {
    assert.equal(job.queueName, "youtube-channel-crawl");
    assert.equal(job.attemptsMade, 0);
    assert.equal(job.data.dispatch_generation, 1);
    assert.ok(job.data.channel_id);
  }
  assert.equal(jobs[0].data.run_id, null);
  assert.equal(jobs[1].data.run_id, "run-comment_backfill");
});

test("the diagnostic Job Builder cannot be used as a generic production Job constructor", () => {
  assert.throws(
    () => buildManagedDiagnosticJob({ kind: "channel_snapshot", channelId: "UCproduction" }),
    /unsupported managed diagnostic Job kind/,
  );
  assert.throws(
    () => buildManagedDiagnosticJob({ kind: "comment_probe", channelId: "" }),
    /channelId is required/,
  );
});

test("all standalone managed scripts use the restricted diagnostic Job Builder", async (t) => {
  for (const [script, kind] of managedScripts) {
    await t.test(script, async () => {
      const source = await readFile(new URL(`../scripts/${script}`, import.meta.url), "utf8");
      assert.match(source, /buildManagedDiagnosticJob/);
      assert.match(source, new RegExp(`kind:\\s*"${kind}"`));
      assert.doesNotMatch(source, /attemptsMade:\s*0/);
      assert.doesNotMatch(source, /dispatch_generation:\s*1/);
    });
  }
});
