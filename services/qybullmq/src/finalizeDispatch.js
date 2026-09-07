import { finalizeDispatchRevision } from "./finalizePolicy.js";
import { safeJobId } from "./queues.js";

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

export async function dispatchFinalizeForRun({
  query,
  queue,
  channelId,
  runId,
  reason,
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  if (!queue || typeof queue.add !== "function") throw new TypeError("queue is required");
  const normalizedChannelId = requiredText(channelId, "channelId");
  const normalizedRunId = String(runId ?? "").trim() || null;
  const normalizedReason = requiredText(reason, "reason");
  const revisionRows = await query(
    `SELECT
       c.channel_id,c.latest_run_id,c.status AS channel_status,c.agent_status,c.updated_at AS channel_updated_at,
       r.detail_status,r.expected_content_count,
       r.result_json->'final_repair' AS run_final_repair,
       r.result_json->>'pipeline_cycle_id' AS pipeline_cycle_id,
       (SELECT count(*)::int FROM crawler.content_candidates cc WHERE cc.run_id=$2) AS candidate_count,
       (SELECT max(cc.updated_at) FROM crawler.content_candidates cc WHERE cc.run_id=$2) AS candidate_updated_at,
       (SELECT count(*)::int FROM crawler.contents ct WHERE ct.channel_id=$1 AND ct.run_id=$2) AS content_count,
       (SELECT max(COALESCE(ct.last_enriched_at,ct.last_seen_at))
        FROM crawler.contents ct WHERE ct.channel_id=$1 AND ct.run_id=$2) AS content_updated_at,
       (SELECT ap.updated_at FROM crawler.agent_profiles ap
        WHERE ap.channel_id=$1 AND ap.agent_mode='basic' AND ap.status='success' LIMIT 1) AS agent_updated_at
     FROM crawler.channels c
     LEFT JOIN crawler.channel_runs r ON r.run_id=$2
     WHERE c.channel_id=$1
     LIMIT 1`,
    [normalizedChannelId, normalizedRunId],
  );
  const source = revisionRows.rows[0] ?? {
    channel_id: normalizedChannelId,
    run_id: normalizedRunId,
  };
  const sourceRevision = finalizeDispatchRevision(source);
  const jobId = safeJobId(
    "finalize",
    normalizedRunId || normalizedChannelId,
    sourceRevision,
  );
  await queue.add(
    "finalize-channel",
    {
      channel_id: normalizedChannelId,
      run_id: normalizedRunId,
      reason: normalizedReason,
      source_revision: sourceRevision,
      pipeline_cycle_id: source.pipeline_cycle_id ?? null,
    },
    { jobId },
  );
  return Object.freeze({ jobId, sourceRevision });
}
