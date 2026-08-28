import { writeFile } from "node:fs/promises";
import {
  ChannelExecutionMetrics,
  runWithChannelExecution,
} from "../../src/channelExecutionContext.js";

const outputPath = process.argv[2];
const scenario = process.argv[3];
const controller = new AbortController();
const cancellationReason = new Error("injected HTTP cancellation");
globalThis.__youtubeHttpCancellationState = {
  scenario,
  cancel: () => controller.abort(cancellationReason),
  transportAborted: false,
  transportSignal: null,
};

const { youtubeFetch } = await import("../../src/youtube.js");
const metrics = new ChannelExecutionMetrics();
let caught = null;
try {
  await runWithChannelExecution({ metrics }, () => youtubeFetch(
    "https://www.youtube.com/test",
    scenario.startsWith("external_cancel")
      ? { signal: controller.signal, timeoutMs: 1000 }
      : { timeoutMs: 5 },
  ));
} catch (error) {
  caught = error;
}

await writeFile(outputPath, JSON.stringify({
  reason_preserved: caught === cancellationReason,
  error_message: caught?.message ?? String(caught),
  transport_aborted: globalThis.__youtubeHttpCancellationState.transportAborted,
  metrics: metrics.snapshot(),
}), "utf8");
