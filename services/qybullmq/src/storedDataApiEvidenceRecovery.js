import { safeJobId } from "./queues.js";
import { STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID } from
  "./youtubeDataApiEvidence.js";

export const STORED_DATA_API_EVIDENCE_TARGET_SQL = `
SELECT candidate.candidate_id::text,
       candidate.run_id,
       candidate.channel_id,
       candidate.source_content_id,
       candidate.detail_status,
       candidate.api_status,
       candidate.missing_fields,
       candidate.error_message,
       candidate.result_json AS candidate_result_json,
       task.task_id::text,
       task.status AS task_status,
       task.candidate_ids AS task_candidate_ids,
       task.missing_fields AS task_missing_fields,
       task.result_json AS task_result_json
FROM crawler.content_candidates candidate
JOIN crawler.youtube_api_tasks task
  ON candidate.candidate_id=ANY(task.candidate_ids)
WHERE candidate.run_id=$1
  AND task.result_json#>>'{api_verification,videos_list,returned}'='true'
  AND lower(COALESCE(task.result_json->>'privacy_status',''))='public'
  AND (
    (
      candidate.content_key IS NULL
      AND candidate.detail_status='unavailable'
      AND candidate.api_status='unavailable'
      AND candidate.missing_fields @> ARRAY['access_status']::text[]
      AND COALESCE(candidate.result_json#>>'{access,access_status}','unknown')='unknown'
      AND lower(COALESCE(candidate.result_json#>>'{api_detail,privacy_status}',''))='public'
      AND candidate.error_message='content access unknown after detail and api'
    )
    OR (
      candidate.detail_status='api_pending'
      AND candidate.api_status IN ('queued','running')
      AND candidate.result_json#>>'{stored_data_api_evidence_recovery,operation_id}'=$2
    )
  )
ORDER BY task.task_id,candidate.candidate_id`;

function record(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function positiveInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return parsed;
}

function positiveIntegers(value, field) {
  const input = Array.isArray(value) ? value : [];
  const output = [...new Set(input.map((item) => positiveInteger(item, field)))]
    .sort((left, right) => left - right);
  if (output.length !== input.length) {
    throw new TypeError(`${field} must contain unique positive integers`);
  }
  return output;
}

function equalLists(left, right) {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function normalizeTargets(rows, runId) {
  const normalizedRunId = requiredText(runId, "runId");
  const targets = (Array.isArray(rows) ? rows : []).map((row) => {
    const candidateId = positiveInteger(row.candidate_id, "candidate_id");
    const taskId = positiveInteger(row.task_id, "task_id");
    if (requiredText(row.run_id, "target run_id") !== normalizedRunId) {
      throw new Error(`stored Data API recovery target changed Run: ${candidateId}`);
    }
    const taskMissingFields = [...new Set(
      (Array.isArray(row.task_missing_fields) ? row.task_missing_fields : [])
        .map((field) => String(field ?? "").trim())
        .filter(Boolean),
    )];
    if (taskMissingFields.some((field) => field !== "access_status")) {
      throw new Error(`stored Data API recovery task ${taskId} would require another API request`);
    }
    const taskResult = record(row.task_result_json);
    if (taskResult.api_verification?.videos_list?.returned !== true) {
      throw new Error(`stored Data API recovery task ${taskId} has no returned videos.list evidence`);
    }
    if (String(taskResult.privacy_status ?? "").trim().toLowerCase() !== "public") {
      throw new Error(`stored Data API recovery task ${taskId} is not public`);
    }
    return {
      candidate_id: candidateId,
      run_id: normalizedRunId,
      channel_id: requiredText(row.channel_id, "channel_id"),
      source_content_id: requiredText(row.source_content_id, "source_content_id"),
      detail_status: requiredText(row.detail_status, "detail_status"),
      api_status: requiredText(row.api_status, "api_status"),
      candidate_result_json: record(row.candidate_result_json),
      task_id: taskId,
      task_status: requiredText(row.task_status, "task_status"),
      task_candidate_ids: positiveIntegers(row.task_candidate_ids, "task_candidate_ids"),
      task_missing_fields: taskMissingFields,
      task_result_json: taskResult,
    };
  });

  const candidateIds = targets.map((target) => target.candidate_id);
  if (new Set(candidateIds).size !== candidateIds.length) {
    throw new Error("stored Data API recovery selected a Candidate more than once");
  }
  const selectedByTask = new Map();
  for (const target of targets) {
    const selected = selectedByTask.get(target.task_id) ?? [];
    selected.push(target.candidate_id);
    selectedByTask.set(target.task_id, selected);
  }
  for (const target of targets) {
    const selected = [...selectedByTask.get(target.task_id)].sort((left, right) => left - right);
    if (!equalLists(selected, target.task_candidate_ids)) {
      throw new Error(`stored Data API recovery task ${target.task_id} spans unselected Candidates`);
    }
  }
  return targets;
}

function targetIdentity(target) {
  return `${target.candidate_id}:${target.task_id}:${target.source_content_id}`;
}

function assertSameTargets(before, locked) {
  const expected = before.map(targetIdentity).sort();
  const actual = locked.map(targetIdentity).sort();
  if (!equalLists(expected, actual)) {
    throw new Error("stored Data API recovery targets changed before preparation");
  }
}

function taskGroups(targets) {
  const groups = new Map();
  for (const target of targets) {
    const group = groups.get(target.task_id) ?? {
      task_id: target.task_id,
      source_content_id: target.source_content_id,
      candidate_ids: [],
    };
    group.candidate_ids.push(target.candidate_id);
    groups.set(target.task_id, group);
  }
  return [...groups.values()].map((group) => ({
    ...group,
    candidate_ids: group.candidate_ids.sort((left, right) => left - right),
  })).sort((left, right) => left.task_id - right.task_id);
}

function recoveryMarker({ runId, batchId, candidateIds, taskIds, preparedAt }) {
  return {
    operation_id: STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID,
    run_id: runId,
    batch_id: batchId,
    candidate_ids: candidateIds,
    task_ids: taskIds,
    evidence_source: "crawler.youtube_api_tasks.result_json",
    external_request_count: 0,
    prepared_at: preparedAt,
  };
}

async function prepareRecovery(client, targets, { runId, batchId, preparedAt }) {
  await client.query(
    "SELECT pg_advisory_xact_lock(hashtext('stored-data-api-public-access-replay-v1'))",
  );
  const lockedRows = await client.query(
    STORED_DATA_API_EVIDENCE_TARGET_SQL,
    [runId, STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID],
  );
  const locked = normalizeTargets(lockedRows.rows, runId);
  assertSameTargets(targets, locked);

  const groups = taskGroups(locked);
  const candidateIds = locked.map((target) => target.candidate_id).sort((left, right) => left - right);
  const taskIds = groups.map((group) => group.task_id);
  const videoIds = groups.map((group) => group.source_content_id);
  const batchMarker = recoveryMarker({
    runId,
    batchId,
    candidateIds,
    taskIds,
    preparedAt,
  });
  await client.query(
    `INSERT INTO crawler.youtube_api_batches (
       batch_id,status,task_ids,video_ids,result_json,updated_at
     ) VALUES ($1,'queued',$2::bigint[],$3::text[],$4::jsonb,now())
     ON CONFLICT (batch_id) DO UPDATE
     SET status=CASE
           WHEN crawler.youtube_api_batches.status IN ('queued','running') THEN 'queued'
           ELSE crawler.youtube_api_batches.status
         END,
         task_ids=EXCLUDED.task_ids,video_ids=EXCLUDED.video_ids,
         result_json=COALESCE(crawler.youtube_api_batches.result_json,'{}'::jsonb)
           || EXCLUDED.result_json,
         updated_at=now()`,
    [batchId, taskIds, videoIds, JSON.stringify({ stored_evidence_recovery: batchMarker })],
  );

  for (const group of groups) {
    const marker = recoveryMarker({
      runId,
      batchId,
      candidateIds: group.candidate_ids,
      taskIds: [group.task_id],
      preparedAt,
    });
    const updated = await client.query(
      `UPDATE crawler.youtube_api_tasks
       SET status='queued',
           result_json=jsonb_set(
             COALESCE(result_json,'{}'::jsonb),
             '{stored_data_api_evidence_recovery}',
             $3::jsonb,
             true
           ),
           error_message=NULL,next_retry_at=NULL,finished_at=NULL,updated_at=now()
       WHERE task_id=$1
         AND candidate_ids=$2::bigint[]
         AND result_json#>>'{api_verification,videos_list,returned}'='true'
         AND lower(COALESCE(result_json->>'privacy_status',''))='public'
       RETURNING task_id`,
      [group.task_id, group.candidate_ids, JSON.stringify(marker)],
    );
    if (updated.rowCount !== 1) {
      throw new Error(`stored Data API recovery task changed before update: ${group.task_id}`);
    }
  }

  const candidatesUpdated = await client.query(
    `UPDATE crawler.content_candidates
     SET detail_status='api_pending',api_status='queued',
         result_json=jsonb_set(
           COALESCE(result_json,'{}'::jsonb),
           '{stored_data_api_evidence_recovery}',
           $3::jsonb,
           true
         ),
         error_message=NULL,finished_at=NULL,updated_at=now()
     WHERE candidate_id=ANY($1::bigint[])
       AND run_id=$2
       AND content_key IS NULL
       AND COALESCE(result_json#>>'{access,access_status}','unknown')='unknown'
     RETURNING candidate_id`,
    [candidateIds, runId, JSON.stringify(batchMarker)],
  );
  if (candidatesUpdated.rowCount !== candidateIds.length) {
    throw new Error(
      `stored Data API recovery Candidate update changed: expected ${candidateIds.length}, got ${candidatesUpdated.rowCount}`,
    );
  }
  return { candidateIds, taskIds, videoIds, marker: batchMarker };
}

export async function recoverStoredDataApiEvidence({
  query,
  withTransaction,
  queue,
  runId,
  expectedCount = null,
  apply = false,
  now = new Date(),
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
  const normalizedRunId = requiredText(runId, "runId");
  const rows = await query(
    STORED_DATA_API_EVIDENCE_TARGET_SQL,
    [normalizedRunId, STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID],
  );
  const targets = normalizeTargets(rows.rows, normalizedRunId);
  const groups = taskGroups(targets);
  const preview = {
    operation_id: STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID,
    run_id: normalizedRunId,
    candidate_count: targets.length,
    task_count: groups.length,
    candidate_ids: targets.map((target) => target.candidate_id).sort((left, right) => left - right),
    task_ids: groups.map((group) => group.task_id),
    video_ids: groups.map((group) => group.source_content_id),
  };
  if (!apply) return { ...preview, applied: false, action: "preview" };
  const expected = positiveInteger(expectedCount, "expectedCount");
  if (targets.length !== expected) {
    throw new Error(`stored Data API recovery target count changed: expected ${expected}, got ${targets.length}`);
  }
  if (typeof queue?.add !== "function") throw new TypeError("queue is required for apply");
  const timestamp = new Date(now);
  if (Number.isNaN(timestamp.getTime())) throw new TypeError("now must be a valid date");
  const batchId = safeJobId("stored-data-api-replay", normalizedRunId);
  const prepared = await withTransaction((client) => prepareRecovery(client, targets, {
    runId: normalizedRunId,
    batchId,
    preparedAt: timestamp.toISOString(),
  }));
  const jobId = safeJobId("youtube-data-api", batchId);
  const job = await queue.add(
    "youtube-data-api-batch",
    {
      batch_id: batchId,
      task_ids: prepared.taskIds,
      video_ids: prepared.videoIds,
      stored_evidence_replay: {
        operation_id: STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID,
        run_id: normalizedRunId,
        expected_candidate_count: prepared.candidateIds.length,
        task_ids: prepared.taskIds,
      },
    },
    {
      jobId,
      attempts: 3,
      backoff: { type: "exponential", delay: 5000, jitter: 0.5 },
      removeOnComplete: { age: 86400, count: 10000 },
      removeOnFail: { age: 604800, count: 20000 },
    },
  );
  return {
    ...preview,
    applied: true,
    action: "prepared_and_enqueued",
    batch_id: batchId,
    job_id: String(job?.id ?? jobId),
    job_state: typeof job?.getState === "function" ? await job.getState() : null,
  };
}
