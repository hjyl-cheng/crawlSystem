import { lockChannelCandidateAttempt } from "./channelCandidateAttemptMutations.js";
import { publishReadyDiscoveryPages } from "./discoveryPageWakeup.js";
import { reconcileDispatchBatchCandidateState } from "./dispatchBatchCandidateState.js";
import { dispatchFinalizeForRun } from "./finalizeDispatch.js";

// Original candidate settlement and publication handoff, shared by local and remote execution.
export function createFullCrawlHandoff({query,withTransaction,discoveryQueue,finalizeQueue}) {
async function candidateSettled({ candidateId, dispatchBatchId }) {
  if (dispatchBatchId) {
    await reconcileDispatchBatchCandidateState(query, dispatchBatchId);
  }
  try {
    const pageIds = await publishReadyDiscoveryPages({
      query,
      queue: discoveryQueue(),
      candidateId,
    });
    if (pageIds.length > 0) {
      console.log(JSON.stringify({
        event: "discovery_page_qualification_ready",
        candidate_id: candidateId,
        page_ids: pageIds,
      }));
    }
  } catch (error) {
    console.error(JSON.stringify({
      event: "discovery_page_wakeup_failed",
      candidate_id: candidateId,
      error: error?.message || String(error),
    }));
  }
}

async function fetchCompleted({ channelId, runId, reason, candidateAttemptFence }) {
  return withTransaction(async (client) => {
    const transactionQuery = client.query.bind(client);
    await lockChannelCandidateAttempt(transactionQuery, candidateAttemptFence);
    return dispatchFinalizeForRun({
      query: transactionQuery,
      queue: finalizeQueue(),
      channelId,
      runId,
      reason,
    });
  });
}

return {candidateSettled,fetchCompleted};
}
