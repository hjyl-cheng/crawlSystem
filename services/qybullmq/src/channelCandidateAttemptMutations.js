import { normalizeChannelCandidateAttemptFence } from "./channelCandidateAttemptFence.js";

export class StaleChannelCandidateAttemptError extends Error {
  constructor(operation, candidateId) {
    super(`Candidate attempt Fence is stale during ${operation}: ${candidateId}`);
    this.name = "StaleChannelCandidateAttemptError";
    this.code = "CANDIDATE_ATTEMPT_FENCE_STALE";
  }
}

function activeQuery(query) {
  if (typeof query !== "function") throw new TypeError("query is required");
  return query;
}

function requireMutation(result, operation, fence) {
  if (result?.rowCount !== 1) {
    throw new StaleChannelCandidateAttemptError(operation, fence.candidateId);
  }
  return result.rows?.[0] ?? null;
}

export async function lockChannelCandidateAttempt(queryValue, fenceValue) {
  const query = activeQuery(queryValue);
  const fence = normalizeChannelCandidateAttemptFence(fenceValue);
  const locked = await query(
    `SELECT candidate_id
     FROM crawler.channel_candidates
     WHERE candidate_id=$1
       AND snapshot_dispatch_generation=$2
       AND snapshot_active_job_id=$3
       AND snapshot_active_job_attempt=$4
     FOR UPDATE`,
    [fence.candidateId, fence.dispatchGeneration, fence.jobId, fence.bullmqAttempt],
  );
  return requireMutation(locked, "lock snapshot persistence", fence);
}

export async function beginChannelCandidateValidation(queryValue, fenceValue) {
  const query = activeQuery(queryValue);
  const fence = normalizeChannelCandidateAttemptFence(fenceValue);
  const updated = await query(
    `UPDATE crawler.channel_candidates
     SET status='validating',snapshot_attempts=snapshot_attempts+1,
         validation_started_at=COALESCE(validation_started_at,now()),
         snapshot_json=snapshot_json-'parser_contract_error',error_message=NULL,updated_at=now()
     WHERE candidate_id=$1
       AND snapshot_dispatch_generation=$2
       AND snapshot_active_job_id=$3
       AND snapshot_active_job_attempt=$4
       AND status IN ('discovered','queued','validating')
     RETURNING candidate_id,status,snapshot_attempts`,
    [fence.candidateId, fence.dispatchGeneration, fence.jobId, fence.bullmqAttempt],
  );
  return requireMutation(updated, "begin validation", fence);
}

export async function markChannelCandidateAlreadyPromoted(queryValue, fenceValue) {
  const query = activeQuery(queryValue);
  const fence = normalizeChannelCandidateAttemptFence(fenceValue);
  const updated = await query(
    `UPDATE crawler.channel_candidates
     SET status='existing',reject_reason='channel_already_promoted',error_message=NULL,
         snapshot_json=snapshot_json-'parser_contract_error',
         validation_finished_at=now(),updated_at=now()
     WHERE candidate_id=$1
       AND snapshot_dispatch_generation=$2
       AND snapshot_active_job_id=$3
       AND snapshot_active_job_attempt=$4
       AND status IN ('discovered','queued','validating')
     RETURNING candidate_id,status`,
    [fence.candidateId, fence.dispatchGeneration, fence.jobId, fence.bullmqAttempt],
  );
  return requireMutation(updated, "mark already promoted", fence);
}

export async function rejectChannelCandidateAdmission(
  queryValue,
  fenceValue,
  { reason, sourceJson } = {},
) {
  const query = activeQuery(queryValue);
  const fence = normalizeChannelCandidateAttemptFence(fenceValue);
  const normalizedReason = String(reason ?? "").trim();
  if (!normalizedReason) throw new TypeError("Candidate admission rejection reason is required");
  if (!sourceJson || typeof sourceJson !== "object" || Array.isArray(sourceJson)) {
    throw new TypeError("Candidate admission sourceJson must be an object");
  }
  const updated = await query(
    `UPDATE crawler.channel_candidates
     SET status='rejected',reject_reason=$2,error_message=NULL,
         snapshot_json=(snapshot_json-'parser_contract_error') || $3::jsonb,
         validation_finished_at=now(),updated_at=now()
     WHERE candidate_id=$1
       AND snapshot_dispatch_generation=$4
       AND snapshot_active_job_id=$5
       AND snapshot_active_job_attempt=$6
       AND status IN ('discovered','queued','validating')
     RETURNING candidate_id,status`,
    [
      fence.candidateId,
      normalizedReason,
      JSON.stringify(sourceJson),
      fence.dispatchGeneration,
      fence.jobId,
      fence.bullmqAttempt,
    ],
  );
  return requireMutation(updated, "reject admission", fence);
}

export async function recordAcceptedChannelCandidateSnapshot(
  queryValue,
  fenceValue,
  { sourceJson } = {},
) {
  const query = activeQuery(queryValue);
  const fence = normalizeChannelCandidateAttemptFence(fenceValue);
  if (!sourceJson || typeof sourceJson !== "object" || Array.isArray(sourceJson)) {
    throw new TypeError("accepted Candidate sourceJson must be an object");
  }
  const updated = await query(
    `UPDATE crawler.channel_candidates
     SET reject_reason=NULL,error_message=NULL,
         snapshot_json=(snapshot_json-'parser_contract_error') || $2::jsonb,
         validation_finished_at=COALESCE(validation_finished_at,now()),
         accepted_at=COALESCE(accepted_at,now()),updated_at=now()
     WHERE candidate_id=$1
       AND snapshot_dispatch_generation=$3
       AND snapshot_active_job_id=$4
       AND snapshot_active_job_attempt=$5
       AND status='accepted'
     RETURNING candidate_id,status`,
    [
      fence.candidateId,
      JSON.stringify(sourceJson),
      fence.dispatchGeneration,
      fence.jobId,
      fence.bullmqAttempt,
    ],
  );
  return requireMutation(updated, "record accepted snapshot", fence);
}

export async function persistChannelCandidateParserContractFailure(
  queryValue,
  fenceValue,
  { message, details } = {},
) {
  const query = activeQuery(queryValue);
  const fence = normalizeChannelCandidateAttemptFence(fenceValue);
  if (!details || typeof details !== "object" || Array.isArray(details)) {
    throw new TypeError("parser contract failure details must be an object");
  }
  const updated = await query(
    `UPDATE crawler.channel_candidates
     SET status=CASE WHEN status='accepted' THEN status ELSE 'failed' END,
         error_message=CASE WHEN status='accepted' THEN error_message ELSE $2 END,
         snapshot_json=COALESCE(snapshot_json,'{}'::jsonb)
           || jsonb_build_object('parser_contract_error',$3::jsonb),
         next_retry_at=NULL,
         validation_finished_at=CASE WHEN status='accepted' THEN validation_finished_at ELSE now() END,
         updated_at=now()
     WHERE candidate_id=$1
       AND snapshot_dispatch_generation=$4
       AND snapshot_active_job_id=$5
       AND snapshot_active_job_attempt=$6
     RETURNING candidate_id,status`,
    [
      fence.candidateId,
      String(message ?? "Parser contract failure"),
      JSON.stringify(details),
      fence.dispatchGeneration,
      fence.jobId,
      fence.bullmqAttempt,
    ],
  );
  return requireMutation(updated, "persist parser contract failure", fence);
}
