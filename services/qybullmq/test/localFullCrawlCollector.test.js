import assert from "node:assert/strict";
import test from "node:test";
import { createLocalFullCrawlCollector } from "../src/localFullCrawlCollector.js";

test("Local Full Crawl Collector preserves stage inputs and results", async () => {
  const calls = [];
  const youtube = {
    async fetchChannel(...args) {
      calls.push(["admission", ...args]);
      return { stage: "admission" };
    },
    async fetchUploads(...args) {
      calls.push(["uploads", ...args]);
      return { stage: "uploads" };
    },
    async fetchDetail(...args) {
      calls.push(["detail", ...args]);
      return { stage: "detail" };
    },
  };
  const collector = createLocalFullCrawlCollector({ youtube });
  const admissionOptions = { includeAbout: true, signal: {} };
  const uploadsOptions = { country: "BR", signal: {} };
  const detailOptions = { detailMode: "full", signal: {} };

  assert.deepEqual(await collector.collectAdmission("UC1", admissionOptions), { stage: "admission" });
  assert.deepEqual(await collector.collectUploads("UC1", 20, uploadsOptions), { stage: "uploads" });
  assert.deepEqual(await collector.collectDetail("video-1", detailOptions), { stage: "detail" });
  assert.deepEqual(calls, [
    ["admission", "UC1", admissionOptions],
    ["uploads", "UC1", 20, uploadsOptions],
    ["detail", "video-1", detailOptions],
  ]);
});

test("Local Full Crawl Collector rejects an incomplete YouTubeJS adapter", () => {
  assert.throws(
    () => createLocalFullCrawlCollector({ youtube: { fetchChannel() {} } }),
    /Local Full Crawl YouTubeJS Adapter is required/,
  );
});
