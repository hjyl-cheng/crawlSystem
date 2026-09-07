import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const sourceRoot = new URL("../src/", import.meta.url);

function source(name) {
  return readFileSync(new URL(name, sourceRoot), "utf8");
}

test("the production Full Crawl entry exposes one executor and lifecycle cleanup without legacy fetch dependencies", () => {
  const entry = source("fullCrawlYoutubeJs.js");
  const implementation = [
    entry,
    source("fullCrawlYoutubeJsFactory.js"),
    source("fullCrawlYoutubeJsStore.js"),
    source("fullCrawlYoutubeJsModel.js"),
  ].join("\n");

  assert.deepEqual(
    [...entry.matchAll(/export\s+(?:async\s+)?function\s+(\w+)/g)].map((match) => match[1]),
    ["closeFullCrawlYoutubeJsQueues", "executeFullCrawlYoutubeJs"],
  );
  assert.doesNotMatch(implementation, /from\s+["']\.\/pipelineV2\.js["']/);
  assert.doesNotMatch(implementation, /from\s+["']\.\/youtube\.js["']/);
  assert.doesNotMatch(implementation, /\bfetchChannelInitial\b|\bfetchChannelUploads\b/);
  assert.doesNotMatch(implementation, /\bfetchChannelYtDlpMetadata\b|\bfetchVideoYtDlpDetail\b/);
  assert.doesNotMatch(implementation, /processChannelCrawlV2/);
  assert.doesNotMatch(implementation, /queuesByRole\.dataApiBatch/);
});

test("Worker closes Full Crawl queues after workers drain and before database shutdown", () => {
  const worker = source("worker.js");
  const drain = worker.indexOf('await shutdownStep("bullmq_workers"');
  const cleanup = worker.indexOf('await shutdownStep("full_crawl_youtubejs_queues", closeFullCrawlYoutubeJsQueues)');
  const database = worker.indexOf('await shutdownStep("database"');
  assert.ok(drain >= 0 && cleanup > drain && database > cleanup);
});

test("Worker selects the immutable Full Crawl executor in exactly one place", () => {
  const worker = source("worker.js");
  const selections = [...worker.matchAll(/isYoutubeJsFullCrawlFetchContract\s*\(/g)];

  assert.equal(selections.length, 1);
  assert.match(worker, /executeFullCrawlYoutubeJs\(job, \{ resumeMode \}\)/);
  assert.match(worker, /processChannelCrawlV2\(job, \{ resumeMode \}\)/);
});
