import assert from "node:assert/strict";
import test from "node:test";
import {
  STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID,
  missingApiFields,
  storedDataApiReplayResult,
  terminalApiMissingFields,
  youtubeApiTaskResultEvidence,
} from "../src/youtubeDataApiEvidence.js";
import { videoAccessStatus } from "../src/detailPolicy.js";
import { confirmedNoVisibleThreadsPage } from "../src/youtubeCommentPage.js";

function completePublicDetail() {
  return {
    description: "Public video",
    description_status: "exact",
    published_at: "2026-08-17T10:00:00.000Z",
    published_at_precision: "second",
    duration_seconds: 120,
    view_count_text: "100",
    like_count: 5,
    comment_count: 23,
    comments_disabled: false,
    access_status: "public",
  };
}

test("official zero-thread evidence resolves a positive surface comment count", () => {
  const commentsPage = confirmedNoVisibleThreadsPage({
    totalCount: 23,
    checkedAt: "2026-08-18T06:00:00.000Z",
    retryAt: "2026-08-25T06:00:00.000Z",
    sources: ["yt_dlp_top_comments", "youtubejs_comments", "youtube_data_api_comment_threads"],
  });
  const detail = {
    ...completePublicDetail(),
    comments_first_page: commentsPage,
  };

  assert.deepEqual(missingApiFields(detail, "second"), []);
});

test("terminal API evidence keeps unresolved access explicit", () => {
  assert.deepEqual(
    terminalApiMissingFields([], { access_status: "unknown" }),
    ["access_status"],
  );
  assert.deepEqual(
    terminalApiMissingFields(["duration"], { access_status: "public" }),
    ["duration"],
  );
});

test("API task audit preserves both videos.list and commentThreads evidence", () => {
  const page = confirmedNoVisibleThreadsPage({
    totalCount: 23,
    checkedAt: "2026-08-18T06:00:00.000Z",
    retryAt: "2026-08-25T06:00:00.000Z",
    sources: ["youtube_data_api_comment_threads"],
  });
  const evidence = youtubeApiTaskResultEvidence({
    apiDetail: completePublicDetail(),
    detailReturned: true,
    commentApiResult: {
      status: "confirmed_no_visible_threads",
      detail: { comments_first_page: page },
    },
  });

  assert.equal(evidence.access_status, "public");
  assert.equal(evidence.api_verification.videos_list.returned, true);
  assert.equal(
    evidence.api_verification.comment_threads.status,
    "confirmed_no_visible_threads",
  );
  assert.equal(evidence.api_verification.comment_threads.returned_count, 0);
  assert.equal(
    evidence.api_verification.comment_threads.next_retry_at,
    "2026-08-25T06:00:00.000Z",
  );
});

test("returned public Data API evidence overrides unresolved scrape access", () => {
  const evidence = youtubeApiTaskResultEvidence({
    apiDetail: {
      privacy_status: "public",
      source: "youtube_data_api_videos_list",
    },
    detailReturned: true,
  });
  const mergedDetail = {
    access_status: "unknown",
    ...evidence,
  };

  assert.equal(evidence.api_verification.videos_list.returned, true);
  assert.equal(videoAccessStatus(mergedDetail), "public");
});

test("stored public Data API evidence can be replayed without another request", () => {
  const task = {
    task_id: 91,
    source_content_id: "video-1",
    candidate_ids: [501],
    missing_fields: ["access_status"],
    result_json: {
      title: "Recovered video",
      privacy_status: "public",
      source: "youtube_data_api_videos_list",
      api_verification: { videos_list: { returned: true } },
      stored_data_api_evidence_recovery: {
        operation_id: STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID,
        run_id: "run:parent",
        candidate_ids: [501],
      },
    },
  };

  const replay = storedDataApiReplayResult({
    jobData: {
      stored_evidence_replay: {
        operation_id: STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID,
        run_id: "run:parent",
        expected_candidate_count: 1,
        task_ids: [91],
      },
    },
    tasks: [task],
  });

  assert.equal(replay.returnedCount, 1);
  assert.equal(replay.requestAttempts, 0);
  assert.equal(replay.detailsById.get("video-1").privacy_status, "public");
  assert.equal(replay.detailsById.get("video-1").api_verification, undefined);
});

test("stored evidence replay fails closed for incomplete or network-requiring tasks", () => {
  const base = {
    task_id: 91,
    source_content_id: "video-1",
    candidate_ids: [501],
    missing_fields: ["access_status"],
    result_json: {
      privacy_status: "public",
      api_verification: { videos_list: { returned: true } },
      stored_data_api_evidence_recovery: {
        operation_id: STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID,
        run_id: "run:parent",
        candidate_ids: [501],
      },
    },
  };
  const jobData = {
    stored_evidence_replay: {
      operation_id: STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID,
      run_id: "run:parent",
      expected_candidate_count: 1,
      task_ids: [91],
    },
  };

  assert.throws(
    () => storedDataApiReplayResult({
      jobData,
      tasks: [{ ...base, missing_fields: ["access_status", "comments_first_page"] }],
    }),
    /would require another API request/,
  );
  assert.throws(
    () => storedDataApiReplayResult({
      jobData,
      tasks: [{
        ...base,
        result_json: {
          ...base.result_json,
          api_verification: { videos_list: { returned: false } },
        },
      }],
    }),
    /does not contain returned videos.list evidence/,
  );
});
