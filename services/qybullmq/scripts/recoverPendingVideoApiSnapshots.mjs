#!/usr/bin/env node
// Recover only original snapshots that were incorrectly failed while awaiting API.
import { parseArgs } from "node:util";
import { Queue } from "bullmq";
import { query, withTransaction, closeDb } from "../src/db.js";
import { redisOptions, queuesByRole, bullmqPrefix } from "../src/queues.js";
import { retryFullCrawlSnapshotJob } from "../src/finalRepairJobRecovery.js";
import { mergeVideoApiEvidence } from "../src/videoDetailApiFallback.js";
import { validateFullCrawlYoutubeJsDetail } from "../src/fullCrawlYoutubeJsModel.js";

const { values } = parseArgs({ options: { "candidate-ids": { type: "string" }, apply: { type: "boolean", default: false } } });
const ids = String(values["candidate-ids"] ?? "").split(",").map(Number);
if (!ids.length || ids.some(id => !Number.isSafeInteger(id) || id <= 0)) throw new Error("Explicit positive --candidate-ids required");
const queue = new Queue(queuesByRole.channelCrawl, { connection: redisOptions, ...(bullmqPrefix ? { prefix: bullmqPrefix } : {}) });
const timeoutMessage = "Data API fallback is still pending; durable request retained for recovery";
try {
  for (const id of [...new Set(ids)]) {
    const candidate = (await query("SELECT * FROM crawler.channel_candidates WHERE candidate_id=$1", [id])).rows[0];
    const run = (await query("SELECT * FROM crawler.channel_runs WHERE candidate_id=$1 ORDER BY started_at DESC,created_at DESC LIMIT 1", [id])).rows[0];
    if (!candidate || !run) throw new Error(`Missing candidate/run: ${id}`);
    const prepared = run.result_json?.video_api_timeout_recovery?.operation === "api-continuation-20260909";
    if (prepared && run.status !== "failed") {
      const existingJob = await queue.getJob(run.result_json.job_id);
      if (!existingJob || await existingJob.getState() !== "failed") {
        console.log(JSON.stringify({ candidate_id: id, run_id: run.run_id, action: "already_recovering", status: run.status }));
        continue;
      }
    }
    if (candidate.status !== "accepted" || candidate.snapshot_active_job_id != null
        || (!(prepared && run.status === "waiting_detail")
          && ![timeoutMessage, `${timeoutMessage}: VIDEO_API_FALLBACK_UNRESOLVED`].includes(run.error_message))
        || run.publication_finalized_at != null) throw new Error(`Target no longer matches API timeout failure: ${id}`);
    const requests = (await query(`SELECT r.* FROM crawler.youtube_api_detail_requests r
      JOIN crawler.content_candidates c ON c.run_id=r.run_id AND c.source_content_id=r.source_content_id
      WHERE r.run_id=$1 AND c.detail_status NOT IN ('done','unavailable') ORDER BY r.created_at`, [run.run_id])).rows;
    if (!requests.length || requests.some(row => row.status !== "done" || row.consumer !== "full")) {
      throw new Error(`API evidence is not ready: ${id}`);
    }
    for (const row of requests) validateFullCrawlYoutubeJsDetail(row.source_content_id,
      mergeVideoApiEvidence(row.source_content_id, row.partial_detail, row.detail_json), { optionalComments: true });
    const job = await queue.getJob(run.result_json.job_id);
    if (!job || await job.getState() !== "failed" || job.data.run_id !== run.run_id
        || Number(job.data.candidate_id) !== id
        || Number(job.data.dispatch_generation) !== Number(candidate.snapshot_dispatch_generation)) {
      throw new Error(`Original failed snapshot identity changed: ${id}`);
    }
    const plan = { candidate_id: id, run_id: run.run_id, job_id: job.id,
      request_id: requests[0].request_id, failure_finished_at: run.finished_at, apply: values.apply };
    console.log(JSON.stringify(plan));
    if (!values.apply) continue;
    await job.updateData({ ...job.data, video_api_continuation: { request_id: plan.request_id } });
    if (!prepared) await withTransaction(async client => {
      const locked = (await client.query("SELECT * FROM crawler.channel_candidates WHERE candidate_id=$1 FOR UPDATE", [id])).rows[0];
      if (locked.status !== "accepted" || locked.snapshot_active_job_id != null
          || Number(locked.snapshot_dispatch_generation) !== Number(job.data.dispatch_generation)) {
        throw new Error(`Candidate ownership changed: ${id}`);
      }
      const updated = await client.query(`UPDATE crawler.channel_runs SET status='waiting_detail',detail_status='queued',
        error_message=NULL,finished_at=NULL,updated_at=now(),
        result_json=result_json || jsonb_build_object('video_api_timeout_recovery',$3::jsonb)
        WHERE run_id=$1 AND status='failed' AND error_message=$2 RETURNING run_id`,
      [run.run_id, run.error_message, JSON.stringify({ operation: "api-continuation-20260909", original_error: run.error_message,
        original_finished_at: run.finished_at, requested_at: new Date().toISOString(), request_id: plan.request_id })]);
      if (updated.rowCount !== 1) throw new Error(`Run changed before recovery: ${id}`);
    });
    const recovered = await retryFullCrawlSnapshotJob(queue, { run, candidate });
    console.log(JSON.stringify({ candidate_id: id, action: recovered.action }));
  }
} finally { await queue.close(); await closeDb(); }
