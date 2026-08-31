import {
  finalizeDispatchRevision,
  finalizeSourceRevision,
} from "./finalizePolicy.js";
import { lockPublicationChannelMutation } from "./publicationChannelMutationLock.js";

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function activeQuery(query) {
  if (typeof query !== "function") throw new TypeError("query is required");
  return query;
}

function latestTimestamp(values) {
  let latest = null;
  for (const value of values) {
    if (value == null) continue;
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) continue;
    if (latest == null || parsed.getTime() > latest.getTime()) latest = parsed;
  }
  return latest;
}

export function finalizeDispatchStateFromSource({
  channel,
  run,
  candidates = [],
  contents = [],
  agent = null,
} = {}) {
  return {
    channel_id: channel?.channel_id ?? null,
    latest_run_id: channel?.latest_run_id ?? null,
    channel_status: channel?.status ?? null,
    agent_status: channel?.agent_status ?? null,
    channel_updated_at: channel?.updated_at ?? null,
    detail_status: run?.detail_status ?? null,
    expected_content_count: Number(run?.expected_content_count ?? 0),
    pipeline_cycle_id: run?.result_json?.pipeline_cycle_id ?? null,
    run_final_repair: run?.result_json?.final_repair ?? null,
    candidate_count: candidates.length,
    candidate_updated_at: latestTimestamp(candidates.map((row) => row.updated_at)),
    content_count: contents.length,
    content_updated_at: latestTimestamp(contents.map(
      (row) => row.last_enriched_at ?? row.last_seen_at,
    )),
    agent_updated_at: agent?.updated_at ?? null,
  };
}

async function loadFinalizeSource(queryValue, { channelId, runId, lock }) {
  const query = activeQuery(queryValue);
  const normalizedChannelId = requiredText(channelId, "channelId");
  const normalizedRunId = requiredText(runId, "runId");
  const suffix = lock ? " FOR UPDATE" : "";
  const runRows = await query(
    `/* finalize-source-fence:run */
     SELECT run.*
     FROM crawler.channel_runs run
     WHERE run.run_id=$1 AND run.channel_id=$2
     ORDER BY run.run_id${suffix}`,
    [normalizedRunId, normalizedChannelId],
  );
  const run = runRows.rows[0] ?? null;
  if (!run) return null;
  const channelRows = await query(
    `/* finalize-source-fence:channel */
     SELECT channel.*
     FROM crawler.channels channel
     WHERE channel.channel_id=$1
     ORDER BY channel.channel_id${suffix}`,
    [normalizedChannelId],
  );
  const channel = channelRows.rows[0] ?? null;
  if (!channel) return null;
  const contents = await query(
    `/* finalize-source-fence:contents */
     SELECT content.*
     FROM crawler.contents content
     WHERE content.channel_id=$1 AND content.run_id=$2
     ORDER BY content.position ASC NULLS LAST,content.content_key${suffix}`,
    [normalizedChannelId, normalizedRunId],
  );
  const candidates = await query(
    `/* finalize-source-fence:candidates */
     SELECT candidate.*
     FROM crawler.content_candidates candidate
     WHERE candidate.run_id=$1 AND candidate.channel_id=$2
     ORDER BY candidate.position,candidate.candidate_id${suffix}`,
    [normalizedRunId, normalizedChannelId],
  );
  const agents = await query(
    `/* finalize-source-fence:agent */
     SELECT agent.*
     FROM crawler.agent_profiles agent
     WHERE agent.channel_id=$1 AND agent.agent_mode='basic' AND agent.status='success'
     ORDER BY agent.channel_id,agent.agent_mode
     LIMIT 1${suffix}`,
    [normalizedChannelId],
  );
  const source = {
    channel,
    run,
    candidates: candidates.rows,
    contents: contents.rows,
    agent: agents.rows[0] ?? null,
  };
  return Object.freeze({
    ...source,
    sourceRevision: finalizeSourceRevision(source),
    dispatchRevision: finalizeDispatchRevision(finalizeDispatchStateFromSource(source)),
  });
}

export async function readFinalizeSource(query, input) {
  return loadFinalizeSource(query, { ...input, lock: false });
}

export async function lockFinalizeCommitSource(client, {
  channelId,
  runId,
  expectedSourceRevision,
  expectedDispatchRevision,
  transactionGuard = null,
} = {}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  if (transactionGuard != null && typeof transactionGuard !== "function") {
    throw new TypeError("transactionGuard must be a function");
  }
  const normalizedChannelId = requiredText(channelId, "channelId");
  const normalizedRunId = requiredText(runId, "runId");
  const buildRevision = requiredText(expectedSourceRevision, "expectedSourceRevision");
  const dispatchRevision = requiredText(
    expectedDispatchRevision,
    "expectedDispatchRevision",
  );

  await lockPublicationChannelMutation(client, normalizedChannelId);
  if (transactionGuard && await transactionGuard(client) !== true) {
    return Object.freeze({ accepted: false, reason: "transaction_guard_rejected", source: null });
  }
  const source = await loadFinalizeSource(client.query.bind(client), {
    channelId: normalizedChannelId,
    runId: normalizedRunId,
    lock: true,
  });
  if (!source) {
    return Object.freeze({ accepted: false, reason: "source_missing", source: null });
  }
  if (source.sourceRevision !== buildRevision) {
    return Object.freeze({ accepted: false, reason: "source_revision_stale", source });
  }
  if (source.dispatchRevision !== dispatchRevision) {
    return Object.freeze({ accepted: false, reason: "dispatch_revision_stale", source });
  }
  return Object.freeze({ accepted: true, reason: null, source });
}
