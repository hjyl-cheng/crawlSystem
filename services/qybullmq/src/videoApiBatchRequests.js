import { randomUUID } from "node:crypto";
import { videoApiPendingError } from "./videoApiContinuation.js";
import { safeJobId } from "./queues.js";

export const VIDEO_API_BATCH_SCOPE = "youtubejs-video-fallback";

function required(value, name) {
  const result = String(value ?? "").trim();
  if (!result) throw new TypeError(`${name} is required`);
  return result;
}

export function videoApiResultError(message) {
  const error = new Error(message);
  error.code = "VIDEO_API_FALLBACK_UNRESOLVED";
  error.youtube_failure_decision = {
    kind: "parser_runtime", retry_mode: "none", proxy_action: "none", client_action: "none",
  };
  return error;
}

export async function requestVideoApiDetail(withTransaction, {
  requestId, runId, videoId, consumer, partialDetail = {}, requireComments = false,
}) {
  requestId = required(requestId, "requestId");
  runId = required(runId, "runId");
  videoId = required(videoId, "videoId");
  if (!["full", "incremental"].includes(consumer)) throw new TypeError("invalid API consumer");
  return withTransaction(async client => {
    const run = (await client.query("SELECT started_at FROM crawler.channel_runs WHERE run_id=$1", [runId])).rows[0];
    if (!run) throw new Error(`Video API Run is missing: ${runId}`);
    await client.query(`INSERT INTO crawler.youtube_api_tasks(source_content_id,status)
      VALUES ($1,'pending') ON CONFLICT(source_content_id) DO NOTHING`, [videoId]);
    const task = (await client.query(`SELECT * FROM crawler.youtube_api_tasks
      WHERE source_content_id=$1 FOR UPDATE`, [videoId])).rows[0];
    const existing = (await client.query(`SELECT * FROM crawler.youtube_api_detail_requests
      WHERE request_id=$1`, [requestId])).rows[0];
    if (existing) {
      if (existing.run_id !== runId || existing.source_content_id !== videoId || existing.consumer !== consumer) {
        throw new Error("Video API request identity conflicts");
      }
      return existing;
    }
    const fresh = ["done", "unavailable"].includes(task.status)
      && task.finished_at && run.started_at
      && new Date(task.finished_at) >= new Date(run.started_at)
      && typeof task.result_json?.api_verification?.videos_list?.returned === "boolean"
      && (!requireComments || task.result_json?.comments_first_page || task.result_json?.comments_disabled === true);
    const previousFailedRun = task.status === "failed" && task.updated_at && run.started_at
      && new Date(task.updated_at) < new Date(run.started_at);
    if (!fresh && (["done", "unavailable"].includes(task.status) || previousFailedRun)) {
      // Never reopen a Task still owned by a Batch, even after its per-item commit.
      const active = (await client.query(`SELECT 1 FROM crawler.youtube_api_batches
        WHERE status IN ('queued','running') AND $1=ANY(task_ids) LIMIT 1`, [task.task_id])).rows.length;
      if (!active) await client.query(`UPDATE crawler.youtube_api_tasks t SET status='pending',attempts=0,
        result_json='{}'::jsonb,error_message=NULL,next_retry_at=NULL,finished_at=NULL,created_at=now(),updated_at=now(),
        candidate_ids=ARRAY(SELECT c.candidate_id FROM crawler.content_candidates c
          WHERE c.candidate_id=ANY(t.candidate_ids) AND c.detail_status='api_pending')
        WHERE task_id=$1 AND NOT EXISTS (SELECT 1 FROM crawler.youtube_api_detail_requests r
          WHERE r.task_id=t.task_id AND r.status='pending')`, [task.task_id]);
    }
    const status = fresh ? task.result_json.api_verification.videos_list.returned ? "done" : "unavailable" : "pending";
    return (await client.query(`INSERT INTO crawler.youtube_api_detail_requests
      (request_id,run_id,source_content_id,consumer,task_id,status,detail_json,partial_detail,require_comments,finished_at)
      VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9,CASE WHEN $6='pending' THEN NULL ELSE now() END)
      RETURNING *`, [requestId, runId, videoId, consumer, task.task_id, status,
      fresh ? JSON.stringify(task.result_json) : null, JSON.stringify(partialDetail), requireComments])).rows[0];
  });
}

export async function completeVideoApiRequests(client, taskId, detail, returned) {
  return client.query(`UPDATE crawler.youtube_api_detail_requests
    SET status=$2,detail_json=$3::jsonb,error_message=NULL,finished_at=now()
    WHERE task_id=$1 AND status='pending'
      AND (NOT require_comments OR NOT $4 OR $5)`,
  [taskId, returned ? "done" : "unavailable", JSON.stringify(detail ?? {}), returned,
    Boolean(detail?.comments_first_page || detail?.comments_disabled === true)]);
}

export async function waitForVideoApiDetail(query, requestId, { signal } = {}) {
  signal?.throwIfAborted();
  const row = (await query(`SELECT status,detail_json,error_message
    FROM crawler.youtube_api_detail_requests WHERE request_id=$1`, [requestId])).rows[0];
  if (!row) throw new Error("Video API request disappeared");
  if (row.status === "done") return row.detail_json;
  if (["unavailable", "failed"].includes(row.status)) {
    throw videoApiResultError(row.error_message ?? "Data API did not return the requested video; access remains unresolved");
  }
  throw videoApiPendingError(requestId);
}

function batchJob(batch) {
  return { batch_id: batch.batch_id, task_ids: batch.task_ids.map(Number),
    video_ids: batch.video_ids, pipeline_cycle_id: VIDEO_API_BATCH_SCOPE };
}

export async function dispatchVideoApiRequests({ query, withTransaction, queue, batchSize = 50, maxBatches = 4 }) {
  const size = Math.max(1, Math.min(50, Math.floor(batchSize) || 50));
  const pending = Number((await query(`SELECT count(*)::int AS count
    FROM crawler.youtube_api_detail_requests WHERE status='pending'`)).rows[0].count);
  if (!pending) return { pending: 0, dispatched: 0 };
  await withTransaction(async client => {
    // Recover queued Tasks without a live Batch and bound API failures.
    await client.query(`UPDATE crawler.youtube_api_tasks t SET status='pending',updated_at=now()
      WHERE t.status='queued' AND EXISTS (SELECT 1 FROM crawler.youtube_api_detail_requests r
        WHERE r.task_id=t.task_id AND r.status='pending') AND NOT EXISTS
        (SELECT 1 FROM crawler.youtube_api_batches b WHERE b.status IN ('queued','running') AND t.task_id=ANY(b.task_ids))`);
    await client.query(`UPDATE crawler.youtube_api_tasks t SET status='pending',updated_at=now(),
      result_json='{}'::jsonb,finished_at=NULL
      WHERE t.status IN ('done','unavailable') AND t.attempts<3
        AND EXISTS (SELECT 1 FROM crawler.youtube_api_detail_requests r WHERE r.task_id=t.task_id AND r.status='pending')
        AND NOT EXISTS (SELECT 1 FROM crawler.youtube_api_batches b WHERE b.status IN ('queued','running') AND t.task_id=ANY(b.task_ids))`);
    await client.query(`UPDATE crawler.youtube_api_detail_requests r SET status='failed',
      error_message=t.error_message,finished_at=now() FROM crawler.youtube_api_tasks t
      WHERE r.task_id=t.task_id AND r.status='pending' AND t.status IN ('failed','done','unavailable') AND t.attempts>=3
        AND NOT EXISTS(SELECT 1 FROM crawler.youtube_api_batches b WHERE b.status IN ('queued','running') AND t.task_id=ANY(b.task_ids))`);
  });
  const queued = (await query(`SELECT * FROM crawler.youtube_api_batches WHERE status='queued'
    AND result_json->>'video_api_scope'=$1 ORDER BY created_at LIMIT $2`, [VIDEO_API_BATCH_SCOPE, maxBatches])).rows;
  let dispatched = 0;
  for (const batch of queued) {
    const id = safeJobId("youtube-data-api", batch.batch_id);
    const existing = await queue.getJob(id);
    if (existing) {
      const state = await existing.getState();
      if (["failed", "completed"].includes(state)) await existing.retry(state);
    } else await queue.add("youtube-data-api-batch", batchJob(batch), { jobId: id });
    dispatched += 1;
  }
  for (let index = queued.length; index < maxBatches; index += 1) {
    const batch = await withTransaction(async client => {
      const rows = (await client.query(`SELECT t.task_id,t.source_content_id FROM crawler.youtube_api_tasks t
        WHERE t.status IN ('pending','failed') AND t.attempts<3
          AND (t.next_retry_at IS NULL OR t.next_retry_at<=now())
          AND EXISTS (SELECT 1 FROM crawler.youtube_api_detail_requests r WHERE r.task_id=t.task_id AND r.status='pending')
          AND NOT EXISTS (SELECT 1 FROM crawler.youtube_api_batches b WHERE b.status IN ('queued','running') AND t.task_id=ANY(b.task_ids))
        ORDER BY t.created_at,t.task_id LIMIT $1 FOR UPDATE OF t SKIP LOCKED`, [size])).rows;
      if (!rows.length) return null;
      const taskIds = rows.map(row => Number(row.task_id));
      await client.query(`UPDATE crawler.youtube_api_tasks SET status='queued',updated_at=now()
        WHERE task_id=ANY($1::bigint[])`, [taskIds]);
      return (await client.query(`INSERT INTO crawler.youtube_api_batches(batch_id,status,task_ids,video_ids,result_json)
        VALUES($1,'queued',$2::bigint[],$3::text[],$4::jsonb) RETURNING *`,
      [`video-api:${randomUUID()}`, taskIds, rows.map(row => row.source_content_id), JSON.stringify({
        video_api_scope: VIDEO_API_BATCH_SCOPE,
        dispatch_intent: { pipeline_cycle_id: VIDEO_API_BATCH_SCOPE, migration_system_retry_ids: [], recovery_run_ids: [] },
      })])).rows[0];
    });
    if (!batch) break;
    await queue.add("youtube-data-api-batch", batchJob(batch), { jobId: safeJobId("youtube-data-api", batch.batch_id) });
    dispatched += 1;
  }
  return { pending, dispatched };
}
