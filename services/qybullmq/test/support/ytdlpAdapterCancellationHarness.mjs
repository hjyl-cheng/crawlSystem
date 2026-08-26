import { writeFile } from "node:fs/promises";
import {
  ChannelExecutionMetrics,
  runWithChannelExecution,
} from "../../src/channelExecutionContext.js";

const outputPath = process.argv[2];
const scenario = process.argv[3];
const controller = new AbortController();
const reason = new Error(`injected ${scenario} cancellation`);
globalThis.__ytdlpAdapterCancellationState = {
  scenario,
  signal: controller.signal,
  cancel: () => controller.abort(reason),
  spawnCount: 0,
  oneShotKilled: false,
};

const {
  fetchChannelUploads,
  fetchChannelYtDlpMetadata,
  fetchVideoYtDlpDetail,
} = await import("../../src/youtube.js");
const metrics = new ChannelExecutionMetrics();
const adapter = scenario.split("_")[0];
const operation = adapter === "uploads"
  ? () => fetchChannelUploads("UCcancel", 30, { signal: controller.signal })
  : adapter === "detail"
    ? () => fetchVideoYtDlpDetail("cancel-video", null, { signal: controller.signal })
    : () => fetchChannelYtDlpMetadata(
      "https://www.youtube.com/channel/UCcancel",
      { signal: controller.signal },
    );
let caught = null;
try {
  await runWithChannelExecution({ metrics }, operation);
} catch (error) {
  caught = error;
}

await writeFile(outputPath, JSON.stringify({
  reason_preserved: caught === reason,
  spawn_count: globalThis.__ytdlpAdapterCancellationState.spawnCount,
  one_shot_killed: globalThis.__ytdlpAdapterCancellationState.oneShotKilled,
  metrics: metrics.snapshot(),
}), "utf8");
