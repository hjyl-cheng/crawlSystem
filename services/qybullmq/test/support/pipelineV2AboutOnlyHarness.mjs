import { writeFileSync } from "node:fs";

const scenario = process.argv[3] ?? "about_only";
const cancellationController = scenario === "youtubejs_channel_cancelled"
  ? new AbortController()
  : null;
const cancellationReason = cancellationController
  ? new Error("injected YouTube.js channel cancellation")
  : null;
globalThis.__pipelineV2AboutOnlyState = {
  scenario,
  finalizeCalls: [],
  legacyHeaderAttempts: 0,
  queries: [],
  rawObjects: [],
  cancellationSignal: cancellationController?.signal ?? null,
  cancelChannel: () => cancellationController?.abort(cancellationReason),
};

const { processChannelCrawlV2 } = await import("../../src/pipelineV2.js");
const { runWithChannelExecution } = await import("../../src/channelExecutionContext.js");

const job = {
  id: "final-repair__run_promotion__1",
  attemptsMade: 0,
  data: {
    channel_id: "UCaboutOnlyRegression",
    channel_url: "https://www.youtube.com/channel/UCaboutOnlyRegression",
    candidate_id: 42,
    dispatch_generation: 1,
    run_id: "run:promotion",
    business_run_key: "full:candidate:42",
    crawl_mode: "full",
    publication_gap_domains: ["channel"],
    publication_gap_root_run_id: "run:promotion",
    publication_gap_scope: "about_only",
    require_complete_about_metrics: true,
  },
  updateData: async (data) => {
    job.data = data;
  },
};

function report(value) {
  const serialized = JSON.stringify(value);
  if (process.argv[2]) writeFileSync(process.argv[2], serialized, "utf8");
  else console.log(serialized);
}

try {
  const operation = () => processChannelCrawlV2(job, { resumeMode: "repair" });
  const value = cancellationController
    ? await runWithChannelExecution({ abort_signal: cancellationController.signal }, operation)
    : await operation();
  report({
    outcome: "resolved",
    value,
    state: globalThis.__pipelineV2AboutOnlyState,
  });
} catch (error) {
  report({
    outcome: "rejected",
    error: error?.message || String(error),
    reason_preserved: error === cancellationReason,
    state: globalThis.__pipelineV2AboutOnlyState,
  });
}
