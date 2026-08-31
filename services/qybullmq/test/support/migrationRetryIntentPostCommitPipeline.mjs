export * from "../../src/pipelineV2.js?post-commit-original";

import { activeChannelCandidateAttemptFence } from "../../src/channelCandidateAttemptFence.js";
import { query } from "../../src/db.js";

export async function processChannelCrawlV2(job) {
  const fence = activeChannelCandidateAttemptFence(job);
  const updated = await query(
    `UPDATE crawler.channel_candidates
     SET status='accepted',accepted_at=COALESCE(accepted_at,now()),
         validation_finished_at=COALESCE(validation_finished_at,now()),
         snapshot_json=jsonb_set(
           COALESCE(snapshot_json,'{}'::jsonb),
           '{post_commit_test_fetch_count}',
           to_jsonb(COALESCE((snapshot_json->>'post_commit_test_fetch_count')::int,0)+1)
         ),
         updated_at=now()
     WHERE candidate_id=$1 AND snapshot_dispatch_generation=$2
       AND snapshot_active_job_id=$3 AND snapshot_active_job_attempt=$4
       AND status IN ('queued','validating')
     RETURNING candidate_id`,
    [fence.candidateId, fence.dispatchGeneration, fence.jobId, fence.bullmqAttempt],
  );
  if (updated.rowCount !== 1) {
    throw new Error(`post-commit test Candidate Fence rejected Job: ${job.id}`);
  }
  return { accepted: true, post_commit_test_fetch: true };
}
