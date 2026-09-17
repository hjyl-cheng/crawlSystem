// A collection policy, not evidence that the video is private or removed.
export function isPleaseSignInResponse(status, reason) {
  return String(status ?? "").trim().toUpperCase() === "LOGIN_REQUIRED"
    && /^please\s+sign\s+in[.!]?$/i.test(String(reason ?? "").trim());
}

export function isLoginRequiredExclusion(detail) {
  return detail?.collection_exclusion?.reason_code === "login_required"
    && detail.collection_exclusion.policy_version === 1;
}

export function loginRequiredDetail(videoId, attempts) {
  return {
    id: videoId,
    access_status: "unknown",
    source: "youtubejs_player",
    playability_status: "LOGIN_REQUIRED",
    playability_reason: "Please sign in",
    collection_exclusion: { reason_code: "login_required", policy_version: 1 },
    youtube_client_attempts: attempts,
  };
}

export function loginRequiredDisposition(observedAt) {
  return {
    version: "video-disposition-v1",
    kind: "terminal_excluded",
    reason_code: "login_required",
    retry_class: null,
    retryable: false,
    observed_at: new Date(observedAt).toISOString(),
    next_attempt_at: "infinity",
  };
}

// Only the latest settled decision applies. A newer manual reclassification can
// supersede an exclusion. Queued candidates from a new full crawl cannot do so.
export async function loadLoginRequiredExclusion(query, channelId, videoId) {
  const result = await query(
    `SELECT disposition,next_attempt_at,result_json
     FROM crawler.content_candidates
     WHERE channel_id=$1 AND source_content_id=$2 AND disposition IS NOT NULL
     ORDER BY candidate_id DESC LIMIT 1`, [channelId, videoId],
  );
  const row = result.rows[0];
  return row?.disposition === "terminal_excluded"
    && String(row.next_attempt_at).toLowerCase() === "infinity"
    && row.result_json?.disposition?.reason_code === "login_required"
    && isLoginRequiredExclusion(row.result_json?.detail)
    ? row.result_json.detail : null;
}

// Used by the scheduler for stored videos; exclusions of new videos are already
// governed by their candidate's next_attempt_at.
export function loginRequiredExclusionSql(contentAlias = "content") {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(contentAlias)) throw new TypeError("invalid content alias");
  return `COALESCE((SELECT candidate.disposition='terminal_excluded'
      AND candidate.next_attempt_at='infinity'::timestamptz
      AND candidate.result_json #>> '{disposition,reason_code}'='login_required'
    FROM crawler.content_candidates candidate
    WHERE candidate.channel_id=${contentAlias}.channel_id
      AND candidate.source_content_id=${contentAlias}.source_content_id
      AND candidate.disposition IS NOT NULL
    ORDER BY candidate.candidate_id DESC LIMIT 1),false)`;
}
