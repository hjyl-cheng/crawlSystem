import { normalizeChannelCandidateAttemptFence } from "./channelCandidateAttemptFence.js";

function activeClient(client) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  return client;
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function optionalText(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function positiveInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return parsed;
}

function nullableInteger(value, field) {
  if (value == null || value === "") return null;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new TypeError(`${field} must be a non-negative integer or null`);
  }
  return parsed;
}

export function channelCandidateCanFailAdmission(candidate) {
  return Boolean(candidate) && candidate.status !== "accepted";
}

export function resolveChannelRegistryRunId({
  requestedRunId,
  candidate,
  channel,
} = {}) {
  const requested = optionalText(requestedRunId);
  if (requested) return requested;
  const candidateId = Number(candidate?.candidate_id);
  const promotionCandidateId = Number(channel?.registry_promotion_candidate_id);
  if (
    candidate?.status !== "accepted"
    || !Number.isSafeInteger(candidateId)
    || candidateId <= 0
    || promotionCandidateId !== candidateId
  ) {
    return null;
  }
  return requiredText(channel?.registry_promotion_run_id, "registry_promotion_run_id");
}

function normalizeInput(input) {
  const candidateId = positiveInteger(input?.candidateId, "candidateId");
  return {
    candidateId,
    runId: requiredText(input?.runId, "runId"),
    channelId: requiredText(input?.channelId, "channelId"),
    channelUrl: requiredText(input?.channelUrl, "channelUrl"),
    handle: optionalText(input?.handle),
    title: optionalText(input?.title),
    country: optionalText(input?.country),
    countryCode: optionalText(input?.countryCode),
    countryCanonicalName: optionalText(input?.countryCanonicalName),
    subscriberCount: nullableInteger(input?.subscriberCount, "subscriberCount"),
    subscriberCountText: optionalText(input?.subscriberCountText),
    readyForAgent: input?.readyForAgent === true,
    sourceJson: input?.sourceJson && typeof input.sourceJson === "object"
      ? input.sourceJson
      : {},
    candidateAttemptFence: normalizeChannelCandidateAttemptFence(
      input?.candidateAttemptFence,
      { candidateId },
    ),
  };
}

export async function claimChannelRegistryPromotion(clientValue, inputValue) {
  const client = activeClient(clientValue);
  const input = normalizeInput(inputValue);
  const inserted = await client.query(
    `/* channel-registry-promotion:claim */
     INSERT INTO crawler.channels (
       channel_id,channel_url,handle,title,country,country_source,
       subscriber_count,subscriber_count_text,status,reject_reason,
       ready_for_agent,agent_status,source_json,country_code,country_canonical_name,
       registry_promotion_candidate_id,registry_promotion_run_id,updated_at
     ) VALUES (
       $1,$2,$3,$4,NULLIF(btrim($5::text),''),
       CASE WHEN NULLIF(btrim($5::text),'') IS NULL THEN NULL ELSE 'youtube_about' END,
       $6,$7,'active',NULL,$11::boolean,'pending',$8::jsonb,$9,$10,$12,$13,now()
     )
     ON CONFLICT (channel_id) DO NOTHING
     RETURNING channel_id,registry_promotion_candidate_id,registry_promotion_run_id`,
    [
      input.channelId,
      input.channelUrl,
      input.handle,
      input.title,
      input.country,
      input.subscriberCount,
      input.subscriberCountText,
      JSON.stringify(input.sourceJson),
      input.countryCode,
      input.countryCanonicalName,
      input.readyForAgent,
      input.candidateId,
      input.runId,
    ],
  );
  if (inserted.rowCount === 1) {
    const accepted = await client.query(
      `/* channel-registry-promotion:accept-candidate */
       UPDATE crawler.channel_candidates
       SET status='accepted',reject_reason=NULL,error_message=NULL,
           snapshot_json=(snapshot_json-'parser_contract_error') || $3::jsonb,
           validation_finished_at=now(),accepted_at=COALESCE(accepted_at,now()),updated_at=now()
       WHERE candidate_id=$1 AND channel_id=$2
         AND status IN ('discovered','queued','validating')
         AND snapshot_dispatch_generation=$4
         AND snapshot_active_job_id=$5
         AND snapshot_active_job_attempt=$6
       RETURNING candidate_id,accepted_at`,
      [
        input.candidateId,
        input.channelId,
        JSON.stringify(input.sourceJson),
        input.candidateAttemptFence.dispatchGeneration,
        input.candidateAttemptFence.jobId,
        input.candidateAttemptFence.bullmqAttempt,
      ],
    );
    if (accepted.rowCount !== 1) {
      throw new Error(`Channel Registry promotion Candidate attempt Fence is stale: ${input.candidateId}`);
    }
    return {
      status: "promoted",
      promoted: true,
      channel_id: input.channelId,
      promotion_candidate_id: input.candidateId,
      promotion_run_id: input.runId,
    };
  }

  const existing = await client.query(
    `/* channel-registry-promotion:load-winner */
     SELECT channel_id,registry_promotion_candidate_id,registry_promotion_run_id
     FROM crawler.channels
     WHERE channel_id=$1
     FOR SHARE`,
    [input.channelId],
  );
  if (existing.rowCount !== 1) {
    throw new Error(`Channel Registry conflict winner is missing: ${input.channelId}`);
  }
  const markedExisting = await client.query(
    `/* channel-registry-promotion:mark-existing */
     UPDATE crawler.channel_candidates
     SET status='existing',reject_reason='channel_already_promoted',error_message=NULL,
         snapshot_json=(snapshot_json-'parser_contract_error') || $3::jsonb,
         validation_finished_at=now(),updated_at=now()
     WHERE candidate_id=$1 AND channel_id=$2
       AND status IN ('discovered','queued','validating')
       AND snapshot_dispatch_generation=$4
       AND snapshot_active_job_id=$5
       AND snapshot_active_job_attempt=$6
     RETURNING candidate_id`,
    [
      input.candidateId,
      input.channelId,
      JSON.stringify(input.sourceJson),
      input.candidateAttemptFence.dispatchGeneration,
      input.candidateAttemptFence.jobId,
      input.candidateAttemptFence.bullmqAttempt,
    ],
  );
  if (markedExisting.rowCount !== 1) {
    throw new Error(`Channel Registry promotion Candidate attempt Fence is stale: ${input.candidateId}`);
  }
  const winner = existing.rows[0];
  return {
    status: "existing",
    promoted: false,
    channel_id: input.channelId,
    promotion_candidate_id: winner.registry_promotion_candidate_id == null
      ? null
      : Number(winner.registry_promotion_candidate_id),
    promotion_run_id: winner.registry_promotion_run_id ?? null,
  };
}
