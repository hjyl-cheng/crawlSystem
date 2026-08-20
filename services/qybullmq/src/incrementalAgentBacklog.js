import { nanoid } from "nanoid";
import { safeJobId } from "./queues.js";
import { decideIncrementalAgentBatch } from "./incrementalAgentBatchPolicy.js";

export class AgentRefreshConflict extends Error {
  constructor(planId) {
    super(`Agent refresh plan ${planId} is already bound to another Channel or Run`);
    this.name = "AgentRefreshConflict";
  }
}

function groupRequests(rows) {
  const grouped = new Map();
  for (const row of rows) {
    const current = grouped.get(row.channel_id) ?? {
      channel_id: row.channel_id,
      plan_ids: [],
      run_ids: [],
    };
    current.plan_ids.push(String(row.plan_id));
    if (row.run_id) current.run_ids.push(String(row.run_id));
    grouped.set(row.channel_id, current);
  }
  return [...grouped.values()];
}

export class IncrementalAgentBacklog {
  constructor({ withTransaction }) {
    if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
    this.withTransaction = withTransaction;
  }

  async register({ plan, runId }) {
    return this.withTransaction(async (client) => {
      const inserted = await client.query(
        `INSERT INTO crawler.agent_refresh_requests (
           plan_id,plan_day,scheduled_at,channel_id,run_id,status,clock_version,policy_version
         )
         SELECT $1,$2,$3,$4,$5,'pending',$6,$7
         WHERE EXISTS (
           SELECT 1 FROM crawler.channels channel
           WHERE channel.channel_id=$4 AND channel.status='active'
         )
         ON CONFLICT (plan_id) DO NOTHING
         RETURNING *`,
        [
          plan.plan_id,
          plan.plan_day,
          plan.scheduled_at,
          plan.channel_id,
          runId,
          plan.clock_version,
          plan.policy_version,
        ],
      );
      let request = inserted.rows[0];
      if (!request) {
        const existing = await client.query(
          "SELECT * FROM crawler.agent_refresh_requests WHERE plan_id=$1 FOR UPDATE",
          [plan.plan_id],
        );
        request = existing.rows[0];
        if (!request) {
          return { created: false, skipped: true, reason: "channel_inactive", request: null };
        }
        if (request.channel_id !== plan.channel_id || request.run_id !== runId) {
          throw new AgentRefreshConflict(plan.plan_id);
        }
      }
      return { created: inserted.rowCount === 1, request };
    });
  }

  async claimBatch({ batchId, batchSize, tailQuietMs, now = new Date() }) {
    return this.withTransaction(async (client) => {
      await client.query("SELECT pg_advisory_xact_lock(781137217)");
      const summaryRows = await client.query(
        `SELECT count(DISTINCT request.channel_id)::int AS pending_channel_count,
                min(request.created_at) AS oldest_pending_at,
                max(request.created_at) AS newest_pending_at,
                (
                  SELECT count(*)::int
                  FROM feature_clock.daily_channel_plans plan
                  JOIN crawler.channels plan_channel ON plan_channel.channel_id=plan.channel_id
                  WHERE plan.run_agent=true AND plan_channel.status='active'
                    AND plan.plan_day<=(now() AT TIME ZONE 'UTC')::date
                    AND plan.status IN ('planned','dispatching','dispatched','running')
                    AND NOT EXISTS (
                      SELECT 1
                      FROM crawler.agent_refresh_requests registered
                      WHERE registered.plan_id=plan.plan_id
                    )
                ) AS unregistered_plan_count
         FROM crawler.agent_refresh_requests request
         JOIN crawler.channels channel ON channel.channel_id=request.channel_id
         WHERE request.status IN ('pending','failed')
           AND channel.status='active'
           AND request.next_retry_at<=now()
           AND NOT EXISTS (
             SELECT 1 FROM crawler.agent_refresh_requests active
             WHERE active.channel_id=request.channel_id
               AND active.status IN ('queued','running')
           )`,
      );
      const summary = summaryRows.rows[0] ?? {};
      const decision = decideIncrementalAgentBatch({
        pendingChannelCount: summary.pending_channel_count,
        unregisteredPlanCount: summary.unregistered_plan_count,
        oldestPendingAt: summary.oldest_pending_at,
        newestPendingAt: summary.newest_pending_at,
        now,
        batchSize,
        tailQuietMs,
      });
      if (!decision.dispatch) return { decision, requests: [] };

      const selected = await client.query(
        `WITH first_per_channel AS MATERIALIZED (
           SELECT DISTINCT ON (request.channel_id)
                  request.channel_id,request.created_at
           FROM crawler.agent_refresh_requests request
           JOIN crawler.channels channel ON channel.channel_id=request.channel_id
           WHERE request.status IN ('pending','failed')
             AND channel.status='active'
             AND request.next_retry_at<=now()
             AND NOT EXISTS (
               SELECT 1 FROM crawler.agent_refresh_requests active
               WHERE active.channel_id=request.channel_id
                 AND active.status IN ('queued','running')
             )
           ORDER BY request.channel_id,request.created_at,request.plan_id
         ), picked AS (
           SELECT channel_id
           FROM first_per_channel
           ORDER BY created_at,channel_id
           LIMIT $1
         )
         UPDATE crawler.agent_refresh_requests request
         SET status='queued',batch_id=$2,queued_at=now(),last_error=NULL,updated_at=now()
         FROM picked
         WHERE request.channel_id=picked.channel_id
           AND request.status IN ('pending','failed')
           AND request.next_retry_at<=now()
         RETURNING request.*`,
        [decision.limit, batchId],
      );
      const requests = groupRequests(selected.rows);
      if (requests.length > 0) {
        await client.query(
          `UPDATE crawler.channels
           SET agent_status='queued',agent_error_message=NULL,updated_at=now()
           WHERE channel_id=ANY($1::text[]) AND status='active'`,
          [requests.map((item) => item.channel_id)],
        );
      }
      return { decision: { ...decision, limit: requests.length }, requests };
    });
  }

  async releaseBatch(batchId, error) {
    const message = String(error?.message || error).slice(0, 2000);
    return this.withTransaction(async (client) => {
      const released = await client.query(
        `UPDATE crawler.agent_refresh_requests
         SET status='pending',batch_id=NULL,queued_at=NULL,last_error=$2,
             next_retry_at=now()+interval '30 seconds',updated_at=now()
         WHERE batch_id=$1 AND status='queued'
         RETURNING channel_id`,
        [batchId, message],
      );
      const channelIds = [...new Set(released.rows.map((row) => row.channel_id))];
      if (channelIds.length > 0) {
        await client.query(
          `UPDATE crawler.channels
           SET agent_status='pending',agent_error_message=$2,updated_at=now()
           WHERE channel_id=ANY($1::text[]) AND agent_status='queued'`,
          [channelIds, message],
        );
      }
      return channelIds.length;
    });
  }

  async recoverOrphans({
    activeBatchIds = [],
    queuedStaleSeconds = 300,
    runningStaleSeconds = 3900,
  } = {}) {
    return this.withTransaction(async (client) => {
      const recovered = await client.query(
        `UPDATE crawler.agent_refresh_requests request
         SET status='failed',batch_id=NULL,queued_at=NULL,last_error='recovered orphaned incremental Agent request',
             next_retry_at=now(),updated_at=now()
         WHERE request.status IN ('queued','running')
           AND NOT (COALESCE(request.batch_id,'')=ANY($1::text[]))
           AND (
             (request.status='queued' AND request.updated_at<=now()-($2::int * interval '1 second'))
             OR
             (request.status='running' AND request.updated_at<=now()-($3::int * interval '1 second'))
           )
         RETURNING request.channel_id`,
        [activeBatchIds, queuedStaleSeconds, runningStaleSeconds],
      );
      const channelIds = [...new Set(recovered.rows.map((row) => row.channel_id))];
      if (channelIds.length > 0) {
        await client.query(
          `UPDATE crawler.channels
           SET agent_status='failed',agent_next_retry_at=now(),
               agent_error_message='recovered orphaned incremental Agent request',updated_at=now()
           WHERE channel_id=ANY($1::text[]) AND agent_status IN ('queued','running')`,
          [channelIds],
        );
      }
      return channelIds;
    });
  }
}

export class IncrementalAgentBatcher {
  constructor({
    backlog,
    queue,
    batchSize = 30,
    tailQuietMs = 15 * 60 * 1000,
    agentConfigId = null,
    now = () => new Date(),
  }) {
    if (!backlog || !queue) throw new TypeError("backlog and queue are required");
    this.backlog = backlog;
    this.queue = queue;
    this.batchSize = batchSize;
    this.tailQuietMs = tailQuietMs;
    const parsedConfigId = Number(agentConfigId);
    this.agentConfigId = Number.isSafeInteger(parsedConfigId) && parsedConfigId > 0
      ? parsedConfigId
      : null;
    this.now = now;
  }

  async runOnce() {
    const batchId = `incremental-agent:${this.now().getTime()}:${nanoid(8)}`;
    const claimed = await this.backlog.claimBatch({
      batchId,
      batchSize: this.batchSize,
      tailQuietMs: this.tailQuietMs,
      now: this.now(),
    });
    if (claimed.requests.length === 0) return claimed;
    const channelIds = claimed.requests.map((request) => request.channel_id);
    try {
      await this.queue.add(
        "agent-profile-batch",
        {
          batch_id: batchId,
          channel_ids: channelIds,
          agent_mode: "basic",
          ...(this.agentConfigId == null ? {} : { agent_config_id: this.agentConfigId }),
          force_refresh: true,
          incremental_agent_requests: claimed.requests,
        },
        { jobId: safeJobId("incremental-agent-batch", batchId) },
      );
      return claimed;
    } catch (error) {
      await this.backlog.releaseBatch(batchId, error);
      throw error;
    }
  }
}
