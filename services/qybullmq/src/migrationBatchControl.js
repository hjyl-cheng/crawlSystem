import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  createControlledMigrationScaffold,
  materializeControlledMigrationChannel,
} from "./manualMigrationDispatch.js";
import { loadMigrationSourceChannel } from "./migrationSource.js";
import { channelSnapshotPayload } from "./migrationDispatchPolicy.js";
import { safeJobId, defaultJobOptions } from "./queues.js";

export const MIGRATION_START_JOB = "migration-channel-start";
export const migrationBatchControlEnabled = () =>
  process.env.MIGRATION_BATCH_CONTROL_ENABLED === "true";
const openStatuses = ["preparing", "running", "pausing", "paused", "stopping"];
export function batchSelection(value) {
  const s = String(value ?? "");
  if (s === "all") return { selection: s, limit: null };
  if (!["100", "200", "500", "1000", "2000"].includes(s))
    throw Object.assign(new Error("请选择有效迁移数量"), { statusCode: 400 });
  return { selection: s, limit: Number(s) };
}
export function batchTransition(batch, action) {
  const s = batch.status;
  if (action === "pause" && ["preparing", "running"].includes(s))
    return "pausing";
  if (action === "resume" && ["paused", "pausing"].includes(s))
    return batch.frozen_at ? "running" : "preparing";
  if (
    action === "stop" &&
    ["preparing", "running", "pausing", "paused"].includes(s)
  )
    return "stopping";
  throw Object.assign(new Error("批次状态已变化，请刷新后重试"), {
    statusCode: 409,
  });
}
async function lockBatch(client, batchId) {
  await client.query(
    "SELECT value_json FROM crawler.settings WHERE setting_key='query_scheduler' FOR UPDATE",
  );
  return (
    await client.query(
      "SELECT * FROM crawler.migration_control_batches WHERE batch_id=$1 FOR UPDATE",
      [batchId],
    )
  ).rows[0];
}
export async function createMigrationControlBatch({
  withTransaction,
  selection,
  sourceId = process.env.MIGRATION_SOURCE_ID,
}) {
  const normalized = batchSelection(selection),
    batchId = `migration-${randomUUID()}`;
  await withTransaction(async (c) => {
    const inventory = (
      await c.query(
        "SELECT status FROM crawler.migration_channel_inventory_syncs WHERE source_id=$1",
        [sourceId],
      )
    ).rows[0];
    if (inventory?.status !== "ready")
      throw Object.assign(new Error("待迁移库存尚未准备好"), {
        statusCode: 409,
      });
    await createControlledMigrationScaffold(c, {
      batchId,
      sourceId,
      selection: normalized.selection,
    });
    await c.query(
      `INSERT INTO crawler.migration_control_batches(batch_id,source_id,selection,status) VALUES($1,$2,$3,'preparing')`,
      [batchId, sourceId, normalized.selection],
    );
  });
  return {
    ok: true,
    created: true,
    batch_id: batchId,
    status: "preparing",
    selection: normalized.selection,
  };
}
export async function controlMigrationBatch({
  withTransaction,
  batchId,
  action,
  version,
}) {
  return withTransaction(async (c) => {
    const b = await lockBatch(c, batchId);
    if (!b || String(b.version) !== String(version))
      throw Object.assign(new Error("批次状态已变化，请刷新后重试"), {
        statusCode: 409,
      });
    const next = batchTransition(b, action);
    if (action === "resume")
      await c.query(
        "UPDATE crawler.query_dispatch_batches SET result_json=result_json-'migration_control_error' WHERE dispatch_batch_id=$1",
        [batchId],
      );
    return (
      await c.query(
        `UPDATE crawler.migration_control_batches SET status=$2,version=version+1,
    paused_seconds=paused_seconds+CASE WHEN paused_at IS NOT NULL THEN EXTRACT(EPOCH FROM now()-paused_at) ELSE 0 END,
    paused_at=NULL,updated_at=now() WHERE batch_id=$1 RETURNING *`,
        [batchId, next],
      )
    ).rows[0];
  });
}
export async function prepareMigrationControlList({
  withTransaction,
  batchId,
}) {
  return withTransaction(async (c) => {
    const b = await lockBatch(c, batchId);
    if (!b || b.status !== "preparing") return;
    const { limit } = batchSelection(b.selection);
    // Freeze IDs only, inside one snapshot. New inventory rows do not join this batch.
    const inserted = await c.query(
      `INSERT INTO crawler.migration_control_items(batch_id,channel_id,source_candidate_id,ordinal)
    SELECT $1,i.channel_id,i.source_candidate_id,row_number() OVER(ORDER BY i.priority DESC,i.source_candidate_id)
    FROM crawler.migration_channel_inventory i
    WHERE i.source_id=$2
      AND NOT EXISTS(SELECT 1 FROM crawler.migration_channel_intents m WHERE m.source_id=i.source_id AND m.channel_id=i.channel_id)
      AND NOT EXISTS(SELECT 1 FROM crawler.channels ch WHERE ch.channel_id=i.channel_id)
      AND NOT EXISTS(SELECT 1 FROM crawler.channel_candidates cc WHERE cc.channel_id=i.channel_id)
    ORDER BY i.priority DESC,i.source_candidate_id LIMIT $3`,
      [batchId, b.source_id, limit],
    );
    // All can expand a previously tiny table by hundreds of thousands of rows.
    // Refresh planner statistics before settlement, progress and per-ID hydration;
    // otherwise the pending-state index can be chosen for each individual ID.
    if (inserted.rowCount >= 10000) {
      await c.query("ANALYZE crawler.migration_control_items");
    }
    await c.query(
      `UPDATE crawler.migration_control_batches SET frozen_at=now(),status='running',
    total_count=(SELECT count(*) FROM crawler.migration_control_items WHERE batch_id=$1),version=version+1,updated_at=now() WHERE batch_id=$1`,
      [batchId],
    );
  });
}
export async function startControlledMigrationChannel({
  withTransaction,
  query,
  batchId,
  channelId,
  executionJobId = null,
}) {
  const item = (
    await query(
      `SELECT i.*,b.status AS batch_status FROM crawler.migration_control_items i JOIN crawler.migration_control_batches b USING(batch_id) WHERE i.batch_id=$1 AND i.channel_id=$2`,
      [batchId, channelId],
    )
  ).rows[0];
  if (executionJobId && item?.state === "started") {
    return resumeControlledAdmission({
      query,
      batchId,
      channelId,
      executionJobId,
    });
  }
  if (!item || item.state !== "pending" || item.batch_status !== "running")
    return { started: false };
  const snapshot = item.snapshot_json;
  if (!snapshot) return { started: false, preparing: true };
  return withTransaction(async (c) => {
    const b = await lockBatch(c, batchId);
    if (b?.status !== "running") return { started: false };
    const row = (
      await c.query(
        "SELECT * FROM crawler.migration_control_items WHERE batch_id=$1 AND channel_id=$2 FOR UPDATE",
        [batchId, channelId],
      )
    ).rows[0];
    if (row.state !== "pending") return { started: false };
    assert.equal(snapshot.channel_id, channelId);
    assert.equal(
      String(snapshot.source_candidate_id),
      String(row.source_candidate_id),
    );
    assert.equal(snapshot.source_id, b.source_id);
    // Only legacy launchers retain the old admission cap during a rolling upgrade.
    // Direct snapshot Jobs are constrained by the existing BullMQ worker concurrency.
    if (!executionJobId) {
      const count = Number(
        (
          await c.query(
            "SELECT count(*) FROM crawler.migration_control_items WHERE batch_id=$1 AND state='started'",
            [batchId],
          )
        ).rows[0].count,
      );
      if (count >= b.max_in_flight) return { started: false };
    }
    const candidate = await materializeControlledMigrationChannel(c, {
      snapshot,
      batchId,
    });
    await c.query(
      `UPDATE crawler.migration_control_items SET state=$3,candidate_id=$4,started_at=now(),
    outcome=$5,finished_at=CASE WHEN $3='terminal' THEN now() ELSE NULL END WHERE batch_id=$1 AND channel_id=$2`,
      [
        batchId,
        channelId,
        candidate ? "started" : "terminal",
        candidate?.candidate_id ?? null,
        candidate ? null : "existing",
      ],
    );
    if (candidate && executionJobId) {
      await c.query(
        "UPDATE crawler.channel_candidates SET snapshot_active_job_id=$2,snapshot_active_job_attempt=0 WHERE candidate_id=$1",
        [candidate.candidate_id, executionJobId],
      );
      return resumeControlledAdmission({
        query: c.query.bind(c),
        batchId,
        channelId,
        executionJobId,
      });
    }
    return { started: !!candidate, candidate_id: candidate?.candidate_id };
  });
}
// Replay the DB commit if the worker stopped before persisting its full Redis payload.
async function resumeControlledAdmission({
  query,
  batchId,
  channelId,
  executionJobId,
}) {
  const row = (
    await query(
      `SELECT c.*,m.migration_intent_id FROM crawler.migration_control_items i
    JOIN crawler.channel_candidates c USING(candidate_id)
    JOIN crawler.migration_channel_intents m ON m.target_candidate_id=c.candidate_id
    WHERE i.batch_id=$1 AND i.channel_id=$2 AND i.state='started' AND c.snapshot_active_job_id=$3`,
      [batchId, channelId, executionJobId],
    )
  ).rows[0];
  if (!row) throw new Error("Controlled migration admission owner changed");
  return {
    started: true,
    candidate_id: row.candidate_id,
    payload: channelSnapshotPayload(row, batchId, {
      minSubscriberCount: Number(process.env.MIN_SUBSCRIBER_COUNT || 1000),
    }),
  };
}

export async function prepareControlledMigrationSnapshot({
  query,
  withTransaction,
  job,
}) {
  if (!job.data?.migration_control_start) return true;
  const admitted = await startControlledMigrationChannel({
    query,
    withTransaction,
    batchId: job.data.batch_id,
    channelId: job.data.channel_id,
    executionJobId: job.id,
  });
  if (!admitted.started) {
    // Remove skipped, never-started placeholders on completion so resume can enqueue them again.
    job.opts.removeOnComplete = true;
    return false;
  }
  const { migration_control_start, ...data } = job.data;
  await job.updateData({ ...data, ...admitted.payload });
  return true;
}

export async function loadMigrationControlProgress(query) {
  const rows = await query(`SELECT b.*,
   (SELECT result_json->>'migration_control_error' FROM crawler.query_dispatch_batches WHERE dispatch_batch_id=b.batch_id) AS control_error,
   (SELECT count(*)::int FROM publication.outbox o JOIN publication.revision r USING(revision_id)
     JOIN crawler.migration_control_items i ON i.channel_id=r.channel_id WHERE i.batch_id=b.batch_id
     AND i.started_at IS NOT NULL AND o.created_at>=i.started_at AND o.status NOT IN ('delivered','covered_by_baseline')) AS publishing_count,
   COALESCE(s.counts,'{}'::jsonb) AS counts,
   GREATEST(0,EXTRACT(EPOCH FROM COALESCE(b.finished_at,now())-b.created_at)-b.paused_seconds-
    CASE WHEN b.paused_at IS NOT NULL THEN EXTRACT(EPOCH FROM COALESCE(b.finished_at,now())-b.paused_at) ELSE 0 END)::float AS active_seconds
   FROM crawler.migration_control_batches b LEFT JOIN LATERAL(
    SELECT jsonb_object_agg(label,n) AS counts FROM(
     SELECT CASE WHEN state='terminal' THEN outcome ELSE state END AS label,count(*)::int AS n
     FROM crawler.migration_control_items WHERE batch_id=b.batch_id GROUP BY 1
    ) x
   ) s ON true ORDER BY b.created_at DESC LIMIT 10`);
  return {
    ok: true,
    batches: rows.rows,
    active: rows.rows.find((b) => openStatuses.includes(b.status)) ?? null,
  };
}
export async function managedBatchBlocksLegacyCompletion(query, batchId) {
  if (!migrationBatchControlEnabled()) return false;
  return (
    (
      await query(
        "SELECT 1 FROM crawler.migration_control_batches WHERE batch_id=$1",
        [batchId],
      )
    ).rows.length > 0
  );
}
export async function reconcileMigrationControl({
  query,
  withTransaction,
  queue,
  sourceLoader = loadMigrationSourceChannel,
  maxSnapshotAttempts = 6,
}) {
  const active = (
    await query(
      "SELECT batch_id FROM crawler.migration_control_batches WHERE status NOT IN ('ended','completed') ORDER BY created_at LIMIT 1",
    )
  ).rows[0];
  if (!active) return;
  const id = active.batch_id;
  await prepareMigrationControlList({ withTransaction, batchId: id });
  await withTransaction(async (c) => {
    const b = await lockBatch(c, id);
    if (!b) return;
    // Materialize only started work before joining outcomes or the large All target.
    // Stale statistics after freezing must not turn empty settlement into a huge join.
    // Root ownership and recovery must settle before a channel counts as finished.
    await c.query(
      `WITH started AS MATERIALIZED (
      SELECT channel_id,candidate_id,batch_id FROM crawler.migration_control_items
      WHERE batch_id=$1 AND state='started'
    ), settled AS MATERIALIZED (
    SELECT i.channel_id,CASE
      WHEN cc.status='rejected' THEN 'rejected'
      WHEN cc.status='existing' THEN 'existing'
      WHEN manual.pending AND cc.status IN ('failed','accepted') THEN 'failed'
      WHEN cc.status='failed' AND (cc.snapshot_attempts>=$2 OR cc.snapshot_json ? 'parser_contract_error') THEN 'failed'
      WHEN r.status='failed' AND (r.result_json ? 'content_detail_recovery_terminal' OR r.result_json ? 'parser_contract_error') THEN 'failed'
      WHEN cc.status='accepted' AND r.status='done' AND r.publication_finalized_status='ready_auto' THEN 'success'
      WHEN cc.status='accepted' AND r.status='done' AND r.publication_finalized_status='ready_partial' THEN 'dormant'
    END AS outcome
    FROM started i JOIN crawler.channel_candidates cc ON cc.candidate_id=i.candidate_id
    LEFT JOIN LATERAL(SELECT status,publication_finalized_status,result_json FROM crawler.channel_runs WHERE candidate_id=cc.candidate_id ORDER BY created_at DESC LIMIT 1) r ON true
    LEFT JOIN LATERAL(SELECT EXISTS(
      SELECT 1 FROM crawler.migration_system_retry_items retry WHERE retry.candidate_id=cc.candidate_id
       AND retry.failed_dispatch_batch_id=i.batch_id AND retry.status='pending'
       AND retry.failed_dispatch_generation=cc.snapshot_dispatch_generation
       AND (retry.failed_job_id=cc.snapshot_active_job_id AND retry.failed_job_attempt=cc.snapshot_active_job_attempt
         OR (cc.status='accepted' AND cc.snapshot_active_job_id IS NULL AND cc.snapshot_active_job_attempt IS NULL
           AND cc.dispatch_batch_id=i.batch_id
           AND cc.snapshot_json->>'failure_type'='retryable_system_failure'
           AND cc.snapshot_json->>'failed_dispatch_batch_id'=i.batch_id
           AND cc.snapshot_json#>>'{system_failure,code}'=retry.failure_code
           AND r.result_json->>'job_id'=retry.failed_job_id))
    ) AS pending) manual ON true
    WHERE (cc.snapshot_active_job_id IS NULL OR manual.pending)
      AND NOT EXISTS(SELECT 1 FROM crawler.migration_system_retry_items retry WHERE retry.candidate_id=cc.candidate_id AND retry.status IN ('retrying','dispatched'))
   ) UPDATE crawler.migration_control_items i SET state='terminal',outcome=s.outcome,finished_at=now()
     FROM settled s WHERE i.batch_id=$1 AND i.channel_id=s.channel_id AND s.outcome IS NOT NULL`,
      [id, maxSnapshotAttempts],
    );
    const stats = (
      await c.query(
        `SELECT count(*) FILTER(WHERE state='pending')::int AS pending,count(*) FILTER(WHERE state='started')::int AS started,
    count(*) FILTER(WHERE state='terminal')::int AS terminal,count(*) FILTER(WHERE outcome='failed')::int AS failed
    FROM crawler.migration_control_items WHERE batch_id=$1`,
        [id],
      )
    ).rows[0];
    if (b.status === "stopping")
      await c.query(
        "UPDATE crawler.migration_control_items SET state='released' WHERE batch_id=$1 AND state='pending'",
        [id],
      );
    let next = b.status;
    const publishing = Number(
      (
        await c.query(
          `SELECT count(*) FROM publication.outbox o JOIN publication.revision r USING(revision_id) JOIN crawler.migration_control_items i ON i.channel_id=r.channel_id WHERE i.batch_id=$1 AND i.started_at IS NOT NULL AND o.created_at>=i.started_at AND o.status NOT IN ('delivered','covered_by_baseline')`,
          [id],
        )
      ).rows[0].count,
    );
    if (!stats.started && !publishing) {
      if (b.status === "stopping") next = "ended";
      else if (b.status === "pausing") next = "paused";
      else if (b.status === "running" && !stats.pending) next = "completed";
    }
    if (next !== b.status) {
      await c.query(
        `UPDATE crawler.migration_control_batches SET status=$2,version=version+1,updated_at=now(),
     paused_at=CASE WHEN $2='paused' THEN now() ELSE NULL END,
     finished_at=CASE WHEN $2 IN ('ended','completed') THEN now() ELSE NULL END WHERE batch_id=$1`,
        [id, next],
      );
      if (["ended", "completed"].includes(next)) {
        await c.query(
          `UPDATE crawler.query_dispatch_batches SET status='completed',finished_at=now(),updated_at=now(),
     total_channel_count=$3,failed_channel_count=$4,result_json=result_json||jsonb_build_object('controlled_batch_status',$2::text,'planned_total',$3::int) WHERE dispatch_batch_id=$1`,
          [id, next, b.total_count, stats.failed],
        );
        await c.query(
          `UPDATE crawler.settings SET value_json=value_json||jsonb_build_object('status','stopped','stop_reason',$2::text,'completed_at',now()),updated_at=now()
     WHERE setting_key='query_scheduler' AND value_json->>'pipeline_cycle_id'=$1`,
          [id, next === "ended" ? "user_ended_batch" : "pipeline_complete"],
        );
      }
    }
  });
  const b = (
    await query(
      "SELECT * FROM crawler.migration_control_batches WHERE batch_id=$1",
      [id],
    )
  ).rows[0];
  if (b.status !== "running") return;
  // Keep a bounded buffer of real snapshot jobs. Downstream Agent/publication work
  // must not consume crawl capacity; BullMQ already limits actual workers to 20.
  // Queued-but-not-started IDs remain pending and occupy this same window on
  // every tick. Deterministic job IDs deduplicate them; do not OFFSET this query.
  const slots = 100;
  const pending = (
    await query(
      `SELECT channel_id,source_candidate_id,snapshot_json FROM crawler.migration_control_items WHERE batch_id=$1 AND state='pending' ORDER BY ordinal LIMIT $2`,
      [id, slots],
    )
  ).rows;
  for (const item of pending) {
    if (!item.snapshot_json) {
      try {
        const snapshot = await sourceLoader({
          channelId: item.channel_id,
          candidateId: item.source_candidate_id,
        });
        await query(
          `UPDATE crawler.migration_control_items SET snapshot_json=$3::jsonb WHERE batch_id=$1 AND channel_id=$2 AND state='pending'`,
          [id, item.channel_id, JSON.stringify(snapshot)],
        );
        item.snapshot_json = snapshot;
      } catch (error) {
        await withTransaction(async (c) => {
          const locked = await lockBatch(c, id);
          if (locked?.status !== "running") return;
          // A source/configuration outage says nothing about the channel's crawl result.
          // Stop admission once, preserve every pending ID, and let admitted work drain.
          await c.query(
            `UPDATE crawler.migration_control_items SET start_failures=start_failures+1,error_message=$3
             WHERE batch_id=$1 AND channel_id=$2 AND state='pending'`,
            [id, item.channel_id, String(error.message).slice(0, 1000)],
          );
          await c.query(
            "UPDATE crawler.migration_control_batches SET status='pausing',version=version+1,updated_at=now() WHERE batch_id=$1",
            [id],
          );
          await c.query(
            "UPDATE crawler.query_dispatch_batches SET result_json=result_json||jsonb_build_object('migration_control_error',$2::text),updated_at=now() WHERE dispatch_batch_id=$1",
            [id, String(error.message).slice(0, 1000)],
          );
        });
        return;
      }
    }
    await queue.add(
      "channel-snapshot",
      {
        batch_id: id,
        channel_id: item.channel_id,
        dispatch_batch_id: id,
        pipeline_cycle_id: id,
        migration_control_start: true,
      },
      {
        ...defaultJobOptions,
        jobId: safeJobId("channel-snapshot", id, item.channel_id, "g1"),
        priority: Number(item.snapshot_json?.priority ?? 100),
      },
    );
  }
}
