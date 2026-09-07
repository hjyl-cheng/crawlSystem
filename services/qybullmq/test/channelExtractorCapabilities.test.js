import assert from "node:assert/strict";
import test from "node:test";
import { channelExtractorCapabilities, incrementalVideoExecutorMode } from "../src/channelExtractorCapabilities.js";
import { defaultFullCrawlFetchContract, LEGACY_FULL_CRAWL_FETCH_CONTRACT } from "../src/fullCrawlFetchContract.js";

test("new executors need only YouTubeJS while frozen legacy and Content Enrich retain their resources", () => {
  const mode = incrementalVideoExecutorMode({});
  assert.equal(mode, "youtubejs_checkpoint_v1");
  for (const [prepared, executor, ytdlp] of [
    [{ workloadKind: "channel_full", fetchContract: defaultFullCrawlFetchContract({}) }, mode, false],
    [{ workloadKind: "channel_incremental" }, mode, false],
    [{ workloadKind: "channel_full", fetchContract: LEGACY_FULL_CRAWL_FETCH_CONTRACT }, mode, true],
    [{ workloadKind: "channel_full" }, mode, true],
    [{ workloadKind: "channel_incremental" }, "legacy", true],
    [{ workloadKind: "content_enrich" }, mode, true],
  ]) assert.deepEqual(channelExtractorCapabilities(prepared, executor), { youtubejs: true, ytdlp });
  assert.throws(() => incrementalVideoExecutorMode({ INCREMENTAL_VIDEO_EXECUTOR: "typo" }), /unsupported/);
});
