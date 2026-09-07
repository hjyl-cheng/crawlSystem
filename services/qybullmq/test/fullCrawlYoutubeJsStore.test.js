import assert from "node:assert/strict";
import test from "node:test";
import {
  YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
  YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT,
} from "../src/fullCrawlFetchContract.js";
import {
  fullCrawlTargetHash,
  fullCrawlUploadsDocument,
  fullCrawlUploadsHash,
} from "../src/fullCrawlYoutubeJsModel.js";
import {
  FullCrawlYoutubeJsCheckpointError,
  FullCrawlYoutubeJsStore,
} from "../src/fullCrawlYoutubeJsStore.js";

function target(videoId = "video-1") {
  return {
    video_id: videoId,
    position: 1,
    url: `https://www.youtube.com/watch?v=${videoId}`,
    title: "Video 1",
    published_at: "2026-09-03T00:00:00.000Z",
    published_at_status: "exact",
    published_at_precision: "date_only",
    published_at_source: "youtubejs_player_microformat",
  };
}

function job() {
  return {
    id: "channel-snapshot__batch-1__UCstore__g1",
    attemptsStarted: 1,
    data: {
      channel_id: "UCstore",
      candidate_id: 41,
      run_id: "run-store",
      business_run_key: "full:41:g1",
      dispatch_batch_id: "batch-1",
      dispatch_generation: 1,
      fetch_contract: YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
    },
  };
}

function fixture({
  contract = YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
  apiStatus = "not_needed",
  documentTarget = target(),
  candidateTarget = target(),
  fetch = null,
} = {}) {
  const document = fullCrawlUploadsDocument({
    playlist_id: "UUstore",
    entries: [documentTarget],
    activity_evidence_complete: true,
    scan: {
      complete: true,
      stop_reason: "limit",
      terminal_reason: "limit",
      pages: 1,
      inspected_count: 1,
      parse_gap_count: 0,
    },
  });
  const candidateTargets = fullCrawlUploadsDocument({ entries: [candidateTarget] }).entries;
  const receipt = {
    uploads_hash: fullCrawlUploadsHash(document),
    target_hash: fullCrawlTargetHash(candidateTargets),
    selected_count: 1,
    document,
  };
  const fetchReceipt = fetch == null ? null : {
    uploads_hash: receipt.uploads_hash,
    target_hash: receipt.target_hash,
    selected_count: receipt.selected_count,
    ...fetch,
  };
  const binding = {
    business_run_key: "full:41:g1",
    business_run_id: "run-store",
    channel_id: "UCstore",
    candidate_id: 41,
    run_kind: "full",
    status: "materialized",
    intent_json: { intent: { fetch_contract: contract } },
  };
  const channelCandidate = {
    candidate_id: 41,
    channel_id: "UCstore",
    status: "accepted",
  };
  const channel = {
    channel_id: "UCstore",
    status: "active",
    registry_promotion_candidate_id: 41,
    registry_promotion_run_id: "run-store",
  };
  const run = {
    run_id: "run-store",
    channel_id: "UCstore",
    candidate_id: 41,
    crawl_mode: "full",
    detail_status: fetch ? "done" : "queued",
    result_json: {
      fetch_contract: contract,
      full_crawl: { uploads: receipt, ...(fetchReceipt ? { fetch: fetchReceipt } : {}) },
    },
  };
  const contentCandidate = {
    candidate_id: 99,
    run_id: "run-store",
    channel_id: "UCstore",
    source_content_id: candidateTargets[0].video_id,
    position: candidateTargets[0].position,
    source_url: candidateTargets[0].source_url,
    detail_status: fetch ? "done" : "queued",
    api_status: apiStatus,
    disposition: fetch ? "stored" : null,
    result_json: { full_crawl_target: candidateTargets[0] },
  };
  const client = {
    async query(sql) {
      if (sql.includes("FROM crawler.business_run_bindings")) {
        return { rows: [binding], rowCount: 1 };
      }
      if (sql.includes("FROM crawler.channel_candidates")) {
        return { rows: [channelCandidate], rowCount: 1 };
      }
      if (sql.includes("FROM crawler.channels")) {
        return { rows: [channel], rowCount: 1 };
      }
      if (sql.includes("FROM crawler.channel_runs")) {
        return { rows: [run], rowCount: 1 };
      }
      if (sql.includes("FROM crawler.content_candidates")) {
        return { rows: [contentCandidate], rowCount: 1 };
      }
      throw new Error(`Unexpected SQL: ${sql}`);
    },
  };
  return new FullCrawlYoutubeJsStore({
    query: client.query.bind(client),
    withTransaction: (action) => action(client),
  });
}

test("restore derives the Detail phase from a valid Uploads checkpoint", async () => {
  const state = await fixture().restore(job());

  assert.equal(state.phase, "detail");
  assert.equal(state.uploads.targets[0].video_id, "video-1");
});

test("restore preserves a frozen v1 Run and rejects a v2 Job against its binding", async () => {
  const store = fixture({ contract: YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT });
  const oldJob = job();
  oldJob.data.fetch_contract = YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT;
  assert.equal((await store.restore(oldJob)).phase, "detail");
  await assert.rejects(store.restore(job()), error => error.code === "FULL_CRAWL_FETCH_CONTRACT_CONFLICT");
});

test("restore rejects every Data API state in a YouTubeJS Run", async () => {
  for (const apiStatus of ["pending", "queued", "running", "done", "failed", "unavailable"]) {
    await assert.rejects(
      fixture({ apiStatus }).restore(job()),
      (error) => error instanceof FullCrawlYoutubeJsCheckpointError
        && error.phase === "detail",
      apiStatus,
    );
  }
});

test("restore rejects a document whose entries differ from its frozen Candidate set", async () => {
  await assert.rejects(
    fixture({ documentTarget: target("video-other") }).restore(job()),
    (error) => error instanceof FullCrawlYoutubeJsCheckpointError
      && error.phase === "uploads",
  );
});

test("restore rejects a Fetch receipt with a different selected count", async () => {
  await assert.rejects(
    fixture({
      fetch: {
        status: "complete",
        selected_count: 2,
      },
    }).restore(job()),
    (error) => error instanceof FullCrawlYoutubeJsCheckpointError
      && error.phase === "close_fetch",
  );
});
