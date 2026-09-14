import { nanoid } from 'nanoid';
import { agentConcurrencyLimit, buildAgentDispatchPlan } from './agentDispatchPolicy.js';
import { safeJobId, queuesByRole } from './queues.js';

// Tail eligibility is an existence check, not a full-batch recount on every
// refill. The periodic reconciliation still persists validation status/totals.
export async function agentTailReady(query, dispatchBatchId) {
  const result = await query(`SELECT discovery_closed_at IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM crawler.channel_candidates c WHERE c.dispatch_batch_id=b.dispatch_batch_id
      AND c.status IN ('discovered','queued','validating')
  ) AS ready FROM crawler.query_dispatch_batches b
  WHERE b.dispatch_batch_id=$1 AND b.status<>'completed'`, [dispatchBatchId]);
  return result.rows[0]?.ready === true;
}

export function createAgentBatchDispatch({ query, agentQueue, agentBatchSize, agentMaxBatchesPerTick }) {
async function syncAgentGlobalConcurrency(actions, agentConfigs) {
  const registeredWorkers = await agentQueue.getWorkersCount();
  const concurrency = agentConcurrencyLimit(agentConfigs, registeredWorkers);
  if (concurrency > 0) {
    const previous = await agentQueue.getGlobalConcurrency();
    if (previous !== concurrency) {
      await agentQueue.setGlobalConcurrency(concurrency);
      actions.push({
        action: "set-global-concurrency",
        queue: queuesByRole.agentBatch,
        concurrency,
        previous,
        registered_workers: registeredWorkers,
        configured_capacity: agentConfigs.reduce((total, config) => total + Number(config.max_workers ?? 1), 0),
      });
    }
  }
  return { registeredWorkers, concurrency };
}

async function maybeCreateAgentBatch(actions, agentConfigs, agentCapacity, queryScheduler) {
  if (!queryScheduler.pipeline_cycle_id) return;
  const dispatchBatchId = queryScheduler.pipeline_cycle_id;
  const allowPartialFlush = await agentTailReady(query, dispatchBatchId);
  const jobs = (await agentQueue.getJobs(
    ["waiting", "active", "delayed", "prioritized", "paused", "waiting-children"],
    0,
    9999,
    true,
  )).filter(Boolean);
  // Recovery has its own bounded pending window. It must not suppress every
  // ordinary batch until that historical backlog reaches zero. Reserve at most
  // workerCapacity ordinary batches; BullMQ's shared global concurrency still
  // limits running work across BOTH classes, and FIFO retains recovery progress.
  const ordinaryJobs = jobs.filter(job => job.data?.migration_system_retry_id == null);
  const outstandingByConfig = new Map();
  for (const job of ordinaryJobs) {
    const configId = Number(job.data?.agent_config_id);
    if (!Number.isFinite(configId) || configId <= 0) continue;
    outstandingByConfig.set(configId, (outstandingByConfig.get(configId) ?? 0) + 1);
  }
  const dispatchPlan = buildAgentDispatchPlan({
    configs: agentConfigs,
    outstandingByConfig,
    outstandingTotal: ordinaryJobs.length,
    workerCapacity: agentCapacity.concurrency,
    maxBatches: agentMaxBatchesPerTick,
  });
  for (const agentConfig of dispatchPlan) {
    const batchSize = Math.max(1, Math.min(50, Number(agentConfig?.batch_size ?? agentBatchSize)));
    // Sort eligible identities before reading large Run payloads. The ordered
    // subquery and correlated LIMIT 1 keep each payload check inside the walk;
    // the outer LIMIT stops after one batch instead of checking the whole backlog.
    // Recheck ownership/eligibility under the original channel and Run locks.
    const rows = await query(
      `WITH candidates AS MATERIALIZED (
         SELECT locked.channel_id,locked.channel_url
         FROM (
         SELECT c.channel_id,c.latest_run_id,c.priority,c.created_at
         FROM crawler.channels c
         JOIN crawler.channel_runs current_run ON current_run.run_id=c.latest_run_id
         WHERE c.ready_for_agent=true
           AND c.agent_status IN ('pending','failed')
           AND NOT EXISTS (
             SELECT 1 FROM crawler.agent_refresh_requests refresh
             WHERE refresh.channel_id=c.channel_id
               AND (
                 refresh.status IN ('pending','queued','running')
                 OR (refresh.status='failed' AND isfinite(refresh.next_retry_at))
               )
           )
           AND (c.agent_next_retry_at IS NULL OR c.agent_next_retry_at<=now())
           AND c.status='active'
           AND c.subscriber_count IS NOT NULL
           AND NULLIF(btrim(c.title),'') IS NOT NULL
           AND NOT EXISTS (
             SELECT 1
             FROM crawler.migration_system_retry_items retry
             WHERE retry.candidate_id=current_run.candidate_id
               AND retry.status IN ('retrying','pending','dispatched')
           )
         ORDER BY c.priority DESC,c.created_at ASC OFFSET 0
         ) eligible
         CROSS JOIN LATERAL (
         SELECT c.channel_id,c.channel_url
         FROM crawler.channels c
         JOIN crawler.channel_runs current_run ON current_run.run_id=c.latest_run_id
         WHERE c.channel_id=eligible.channel_id
           AND c.latest_run_id=eligible.latest_run_id
           AND c.ready_for_agent=true
           AND c.agent_status IN ('pending','failed')
           AND NOT EXISTS (
             SELECT 1 FROM crawler.agent_refresh_requests refresh
             WHERE refresh.channel_id=c.channel_id
               AND (
                 refresh.status IN ('pending','queued','running')
                 OR (refresh.status='failed' AND isfinite(refresh.next_retry_at))
               )
           )
           AND (c.agent_next_retry_at IS NULL OR c.agent_next_retry_at<=now())
           AND c.status='active'
           AND c.subscriber_count IS NOT NULL
           AND NULLIF(btrim(c.title),'') IS NOT NULL
           AND COALESCE(current_run.result_json->>'dispatch_batch_id',current_run.result_json->>'pipeline_cycle_id')=$3
           AND NOT EXISTS (
             SELECT 1
             FROM crawler.migration_system_retry_items retry
             WHERE retry.candidate_id=current_run.candidate_id
               AND retry.status IN ('retrying','pending','dispatched')
           )
         LIMIT 1
         FOR UPDATE SKIP LOCKED
         ) locked
         LIMIT $1
       ), candidate_count AS (
         SELECT count(*)::int AS count FROM candidates
       ), picked AS (
         SELECT channel_id,channel_url FROM candidates
         WHERE (SELECT count FROM candidate_count)>=$1 OR $2::boolean=true
       ), updated AS (
         UPDATE crawler.channels channel
         SET agent_status='queued',updated_at=now()
         FROM picked
         WHERE channel.channel_id=picked.channel_id
         RETURNING channel.channel_id,channel.channel_url
       )
       SELECT updated.*,(SELECT count FROM candidate_count) AS eligible_count FROM updated`,
      [batchSize, allowPartialFlush, dispatchBatchId],
    );
    if (rows.rows.length === 0) continue;
    const channelIds = rows.rows.map((row) => row.channel_id);
    const batchId = `agent-batch:${dispatchBatchId}:${Date.now()}:${nanoid(8)}`;
    try {
      await agentQueue.add(
        "agent-profile-batch",
        {
          batch_id: batchId,
          channel_ids: channelIds,
          agent_mode: "basic",
          agent_config_id: agentConfig.config_id,
          pipeline_cycle_id: queryScheduler.pipeline_cycle_id,
          dispatch_batch_id: dispatchBatchId,
        },
        { jobId: safeJobId("agent-batch", batchId) },
      );
    } catch (error) {
      await query(
        `UPDATE crawler.channels
         SET agent_status='pending',agent_error_message=$2,updated_at=now()
         WHERE channel_id=ANY($1::text[]) AND agent_status='queued'`,
        [channelIds, error?.message || String(error)],
      );
      throw error;
    }
    actions.push({
      action: "enqueue-agent-batch",
      queue: queuesByRole.agentBatch,
      count: channelIds.length,
      batch_size: batchSize,
      agent_config_id: agentConfig.config_id,
      agent_config_name: agentConfig.name,
      config_max_workers: Number(agentConfig.max_workers ?? 1),
      registered_agent_workers: agentCapacity.registeredWorkers,
      agent_global_concurrency: agentCapacity.concurrency,
      partial_flush: channelIds.length < batchSize,
      batch_id: batchId,
      dispatch_batch_id: dispatchBatchId,
    });
    if (rows.rows.length < batchSize) break;
  }
  if (allowPartialFlush) {
    const remaining = await query(
      `SELECT count(*)::int AS count
       FROM crawler.channels channel
       JOIN crawler.channel_runs run ON run.run_id=channel.latest_run_id
       WHERE channel.ready_for_agent=true
         AND channel.agent_status IN ('pending','failed')
         AND NOT EXISTS (
           SELECT 1 FROM crawler.agent_refresh_requests refresh
           WHERE refresh.channel_id=channel.channel_id
             AND (
               refresh.status IN ('pending','queued','running')
               OR (refresh.status='failed' AND isfinite(refresh.next_retry_at))
             )
         )
         AND channel.subscriber_count IS NOT NULL
         AND NULLIF(btrim(channel.title),'') IS NOT NULL
         AND COALESCE(run.result_json->>'dispatch_batch_id',run.result_json->>'pipeline_cycle_id')=$1
         AND NOT EXISTS (
           SELECT 1
           FROM crawler.migration_system_retry_items retry
           WHERE retry.candidate_id=run.candidate_id
             AND retry.status IN ('retrying','pending','dispatched')
         )`,
      [dispatchBatchId],
    );
    if (Number(remaining.rows[0]?.count ?? 0) === 0) {
      await query(
        `UPDATE crawler.query_dispatch_batches
         SET agent_tail_flushed_at=COALESCE(agent_tail_flushed_at,now()),updated_at=now()
         WHERE dispatch_batch_id=$1`,
        [dispatchBatchId],
      );
    }
  }
}

  return { syncCapacity: syncAgentGlobalConcurrency, dispatch: maybeCreateAgentBatch };
}
