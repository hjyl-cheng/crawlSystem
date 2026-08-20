import { hasResolvedDuration, isLiveInProgress } from "./detailPolicy.js";
import { commentFirstPageNeedsResolution } from "./youtubeCommentPage.js";
import { hasResolvedDescription } from "./videoMetadata.js";

export const STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID =
  "stored-data-api-public-access-replay-v1";

function record(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function positiveIntegers(value, field) {
  const values = Array.isArray(value) ? value : [];
  const output = [...new Set(values.map(Number).filter(
    (item) => Number.isSafeInteger(item) && item > 0,
  ))].sort((left, right) => left - right);
  if (output.length !== values.length) {
    throw new TypeError(`${field} must contain unique positive integers`);
  }
  return output;
}

function equalIntegerLists(left, right) {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

export function missingApiFields(detail, requiredPrecision = "date_only") {
  const missing = [];
  if (!hasResolvedDescription(detail)) missing.push("description");
  if (!detail?.published_at
      || (requiredPrecision === "second" && detail?.published_at_precision !== "second")) {
    missing.push("published_at");
  }
  if (!hasResolvedDuration(detail) && !isLiveInProgress(detail)) missing.push("duration");
  if (!detail?.view_count_text) missing.push("view_count");
  if (detail?.like_count == null) missing.push("like_count");
  if (detail?.comment_count == null && detail?.comments_disabled !== true) {
    missing.push("comment_count");
  }
  if (commentFirstPageNeedsResolution(detail)) missing.push("comments_first_page");
  return missing;
}

export function terminalApiMissingFields(missingFields, access) {
  const fields = [...new Set((Array.isArray(missingFields) ? missingFields : [])
    .map((field) => String(field ?? "").trim())
    .filter(Boolean))];
  const accessStatus = String(access?.access_status ?? access ?? "unknown").trim().toLowerCase();
  if (["unknown", "login_required"].includes(accessStatus) && !fields.includes("access_status")) {
    fields.push("access_status");
  }
  return fields;
}

export function youtubeApiTaskResultEvidence({
  apiDetail = {},
  detailReturned = false,
  commentApiResult = null,
} = {}) {
  const page = commentApiResult?.detail?.comments_first_page ?? null;
  const resolution = page?.resolution ?? {};
  const apiVerification = {
    videos_list: {
      returned: detailReturned === true,
    },
  };
  if (commentApiResult) {
    apiVerification.comment_threads = {
      status: String(commentApiResult.status ?? "unresolved"),
      source: "youtube_data_api_comment_threads",
      returned_count: Number(page?.returned_count ?? 0),
      total_count: page?.total_count ?? null,
      checked_at: resolution.checked_at ?? page?.collected_at ?? null,
      next_retry_at: resolution.next_retry_at ?? null,
    };
  }
  return {
    ...(apiDetail && typeof apiDetail === "object" ? apiDetail : {}),
    api_verification: apiVerification,
  };
}

export function storedDataApiReplayResult({ jobData = {}, tasks = [] } = {}) {
  const request = record(jobData.stored_evidence_replay);
  if (request.operation_id !== STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID) {
    throw new Error("stored Data API replay operation is invalid");
  }
  const runId = requiredText(request.run_id, "stored replay run_id");
  const expectedCandidateCount = Number(request.expected_candidate_count);
  if (!Number.isSafeInteger(expectedCandidateCount) || expectedCandidateCount <= 0) {
    throw new TypeError("stored replay expected_candidate_count must be positive");
  }
  const requestedTaskIds = positiveIntegers(request.task_ids, "stored replay task_ids");
  const taskRows = Array.isArray(tasks) ? tasks : [];
  const actualTaskIds = positiveIntegers(
    taskRows.map((task) => task?.task_id),
    "stored replay task rows",
  );
  if (!equalIntegerLists(requestedTaskIds, actualTaskIds)) {
    throw new Error("stored Data API replay task identity changed");
  }

  const detailsById = new Map();
  const candidateIds = new Set();
  for (const task of taskRows) {
    const taskId = Number(task.task_id);
    const videoId = requiredText(task.source_content_id, `stored replay task ${taskId} video_id`);
    const taskCandidateIds = positiveIntegers(
      task.candidate_ids,
      `stored replay task ${taskId} candidate_ids`,
    );
    const result = record(task.result_json);
    const marker = record(result.stored_data_api_evidence_recovery);
    if (marker.operation_id !== STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID
        || marker.run_id !== runId) {
      throw new Error(`stored Data API replay task ${taskId} has no matching recovery marker`);
    }
    const markerCandidateIds = positiveIntegers(
      marker.candidate_ids,
      `stored replay task ${taskId} marker candidate_ids`,
    );
    if (!equalIntegerLists(taskCandidateIds, markerCandidateIds)) {
      throw new Error(`stored Data API replay task ${taskId} candidate identity changed`);
    }
    taskCandidateIds.forEach((candidateId) => candidateIds.add(candidateId));

    const missingFields = [...new Set((Array.isArray(task.missing_fields) ? task.missing_fields : [])
      .map((field) => String(field ?? "").trim())
      .filter(Boolean))];
    if (missingFields.some((field) => field !== "access_status")) {
      throw new Error(`stored Data API replay task ${taskId} would require another API request`);
    }
    if (result.api_verification?.videos_list?.returned !== true) {
      throw new Error(`stored Data API replay task ${taskId} does not contain returned videos.list evidence`);
    }
    if (String(result.privacy_status ?? "").trim().toLowerCase() !== "public") {
      throw new Error(`stored Data API replay task ${taskId} is not authoritative public evidence`);
    }
    if (detailsById.has(videoId)) {
      throw new Error(`stored Data API replay contains duplicate video_id ${videoId}`);
    }
    const {
      api_verification: _apiVerification,
      stored_data_api_evidence_recovery: _recoveryMarker,
      ...apiDetail
    } = result;
    detailsById.set(videoId, apiDetail);
  }
  if (candidateIds.size !== expectedCandidateCount) {
    throw new Error(
      `stored Data API replay candidate count changed: expected ${expectedCandidateCount}, got ${candidateIds.size}`,
    );
  }
  return {
    detailsById,
    raw: null,
    returnedCount: detailsById.size,
    requestAttempts: 0,
    replay: {
      operation_id: STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID,
      run_id: runId,
      candidate_ids: [...candidateIds].sort((left, right) => left - right),
      task_ids: actualTaskIds,
    },
  };
}
