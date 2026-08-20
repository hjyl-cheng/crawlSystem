import { materializeBusinessRunBinding } from "./businessRunBindingStore.js";

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function positiveInteger(value, field) {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return number;
}

export async function materializeCheckpointRepairRun(client, {
  repairRunId,
  targetRunId,
  businessRunKey,
  channelId,
  repairRound,
  jobId = null,
} = {}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const repairId = requiredText(repairRunId, "repairRunId");
  const targetId = requiredText(targetRunId, "targetRunId");
  const bindingKey = requiredText(businessRunKey, "businessRunKey");
  const channel = requiredText(channelId, "channelId");
  const round = positiveInteger(repairRound, "repairRound");
  if (repairId === targetId) throw new TypeError("checkpoint repair requires a new Business Run");

  const parentRows = await client.query(
    `SELECT run_id,channel_id,crawl_mode,content_limit
     FROM crawler.channel_runs
     WHERE run_id=$1 AND channel_id=$2
     FOR SHARE`,
    [targetId, channel],
  );
  const parent = parentRows.rows[0];
  if (!parent) throw new Error(`checkpoint target Run does not exist: ${targetId}`);
  const checkpoint = {
    target_run_id: targetId,
    repair_round: round,
    job_id: String(jobId ?? "").trim() || null,
    status: "running",
  };
  await client.query(
    `INSERT INTO crawler.channel_runs (
       run_id,channel_id,candidate_id,status,crawl_mode,content_limit,
       expected_content_count,detail_status,result_json,started_at,updated_at
     ) VALUES (
       $1,$2,NULL,'running',$3,$4,0,'pending',
       jsonb_build_object('checkpoint_repair',$5::jsonb),now(),now()
     )
     ON CONFLICT (run_id) DO NOTHING`,
    [
      repairId,
      channel,
      parent.crawl_mode,
      Number(parent.content_limit ?? 30),
      JSON.stringify(checkpoint),
    ],
  );
  const storedRows = await client.query(
    `SELECT run_id,channel_id,result_json
     FROM crawler.channel_runs
     WHERE run_id=$1
     FOR UPDATE`,
    [repairId],
  );
  const stored = storedRows.rows[0];
  if (!stored
      || stored.channel_id !== channel
      || stored.result_json?.checkpoint_repair?.target_run_id !== targetId
      || Number(stored.result_json?.checkpoint_repair?.repair_round) !== round) {
    throw new Error(`checkpoint repair Run identity conflict: ${repairId}`);
  }
  await materializeBusinessRunBinding(client, {
    businessRunKey: bindingKey,
    businessRunId: repairId,
  });
  return {
    repair_run_id: repairId,
    target_run_id: targetId,
    channel_id: channel,
    repair_round: round,
  };
}

export async function prepareCheckpointRepairCandidates(query, {
  targetRunId,
  repairRunId,
  repairRound,
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const targetId = requiredText(targetRunId, "targetRunId");
  const repairId = requiredText(repairRunId, "repairRunId");
  const round = positiveInteger(repairRound, "repairRound");
  const prepared = await query(
    `UPDATE crawler.content_candidates
     SET detail_status='queued',api_status='not_needed',attempts=0,
         missing_fields='{}'::text[],error_message=NULL,finished_at=NULL,
         result_json=jsonb_set(
           COALESCE(result_json,'{}'::jsonb),
           '{checkpoint_repair}',
           COALESCE(result_json->'checkpoint_repair','{}'::jsonb)
             || jsonb_build_object(
                  'repair_run_id',$2::text,
                  'repair_round',$3::int,
                  'prepared_at',now()
                ),
           true
         ),
         updated_at=now()
     WHERE run_id=$1
       AND detail_status NOT IN ('done','api_pending')
       AND (
         detail_status IN ('queued','failed')
         OR detail_status='running'
         OR (
           detail_status='unavailable'
           AND COALESCE((result_json->>'classified_only')::boolean,false)=true
           AND COALESCE(result_json#>>'{access,access_status}','unknown')
             IN ('unknown','login_required')
         )
       )
       AND COALESCE(result_json#>>'{checkpoint_repair,repair_run_id}','')<>$2::text`,
    [targetId, repairId, round],
  );
  return {
    target_run_id: targetId,
    repair_run_id: repairId,
    repair_round: round,
    prepared_count: prepared.rowCount,
  };
}

export async function finishCheckpointRepairExecution(query, {
  repairRunId,
  targetRunId,
  status,
  summary = {},
  error = null,
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const repairId = requiredText(repairRunId, "repairRunId");
  const targetId = requiredText(targetRunId, "targetRunId");
  const finalStatus = status === "done" ? "done" : "failed";
  const updated = await query(
    `UPDATE crawler.channel_runs
     SET status=$2,
         detail_status=CASE WHEN $2='done' THEN 'done' ELSE 'failed' END,
         result_json=jsonb_set(
           COALESCE(result_json,'{}'::jsonb),
           '{checkpoint_repair}',
           COALESCE(result_json->'checkpoint_repair','{}'::jsonb)
             || jsonb_build_object(
                  'target_run_id',$3::text,
                  'status',$2::text,
                  'summary',$4::jsonb,
                  'finished_at',now()
                ),
           true
         ),
         error_message=$5,finished_at=now(),updated_at=now()
     WHERE run_id=$1
     RETURNING run_id`,
    [repairId, finalStatus, targetId, JSON.stringify(summary ?? {}), error ? String(error) : null],
  );
  if (updated.rowCount !== 1) throw new Error(`checkpoint repair Run does not exist: ${repairId}`);
  return updated.rows[0];
}
