export * from "../../src/channelCandidateWorkerLifecycle.js?post-commit-original";

import {
  runChannelCandidateWorkerJobWithDurableSettlement as runOriginal,
} from "../../src/channelCandidateWorkerLifecycle.js?post-commit-original";

export async function runChannelCandidateWorkerJobWithDurableSettlement(options) {
  const result = await runOriginal(options);
  if (
    process.env.MIGRATION_RETRY_TEST_EXIT_AFTER_COMMIT === "true"
    && options?.job?.data?.retry_intent_id
  ) {
    process.kill(process.pid, "SIGKILL");
    await new Promise(() => {});
  }
  return result;
}
