import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { BusinessRunBindingStore } from "../src/businessRunBindingStore.js";
import { contentDetailExecutionFence } from "../src/contentDetailExecutionFence.js";
import { YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT } from "../src/fullCrawlFetchContract.js";
import { fullCrawlUploadsDocument } from "../src/fullCrawlYoutubeJsModel.js";
import {
  FullCrawlYoutubeJsCheckpointError,
  FullCrawlYoutubeJsStore,
} from "../src/fullCrawlYoutubeJsStore.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const { Pool } = pg;
const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();
const OBSERVED_AT = "2026-09-04T00:00:00.000Z";

function upload(videoId, position, overrides = {}) {
  return {
    video_id: videoId,
    position,
    url: `https://www.youtube.com/watch?v=${videoId}`,
    title: `Video ${position}`,
    published_at: "2026-09-05T00:00:00.000Z",
    published_at_status: "exact",
    published_at_precision: "date_only",
    published_at_source: "youtubejs_player_microformat",
    is_upcoming: true,
    live_status: "is_upcoming",
    ...overrides,
  };
}

function uploadsDocument(entries) {
  return fullCrawlUploadsDocument({
    playlist_id: "UUstore",
    entries,
    activity_evidence_complete: true,
    scan: {
      complete: true,
      stop_reason: "limit",
      terminal_reason: "limit",
      pages: 1,
      inspected_count: entries.length,
      parse_gap_count: 0,
    },
  });
}

test("YouTubeJS Full Crawl checkpoints remain atomic and resume from PostgreSQL", {
  skip: databaseUrl ? false : "MANAGED_JOB_TEST_DATABASE_URL is not configured",
}, async () => {
  const pool = new Pool({
    connectionString: databaseUrl,
    max: 1,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCfullstore${suffix}`;
  const batchId = `full-store-${suffix}`;
  const runId = `run:full-store:${suffix}`;
  const businessRunKey = `full-store:${suffix}`;
  const jobId = `channel-snapshot__${batchId}__${channelId}__g1`;
  try {
    const identity = await client.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i);
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at
       ) VALUES ($1,$1,'validation_closed',now())`,
      [batchId],
    );
    const inserted = await client.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt
       ) VALUES ($1,$1,$2,$3,'queued',1,$4,1)
       RETURNING candidate_id`,
      [batchId, channelId, `https://www.youtube.com/channel/${channelId}`, jobId],
    );
    const candidateId = Number(inserted.rows[0].candidate_id);
    const inTransaction = (action) => action(client);
    const bindingStore = new BusinessRunBindingStore({
      withTransaction: inTransaction,
      randomUUID: () => suffix,
    });
    await bindingStore.resolve({
      businessRunKey,
      explicitBusinessRunId: runId,
      requestedStatus: "reserved",
      runKind: "full",
      channelId,
      candidateId,
      policy: { id: "test-policy", version: 1, hash: "sha256:test-policy" },
      intent: {
        job_name: "channel-snapshot",
        crawl_mode: "full",
        fetch_contract: YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
      },
    });
    const job = {
      id: jobId,
      name: "channel-snapshot",
      attemptsStarted: 1,
      data: {
        channel_id: channelId,
        candidate_id: candidateId,
        run_id: runId,
        business_run_key: businessRunKey,
        dispatch_batch_id: batchId,
        dispatch_generation: 1,
        fetch_contract: YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
      },
    };
    const store = new FullCrawlYoutubeJsStore({
      query: client.query.bind(client),
      withTransaction: inTransaction,
    });

    assert.equal((await store.restore(job)).phase, "admission");
    await store.beginAdmission(job);
    await store.commitAdmission(job, {
      metadata: {
        channel_id: channelId,
        channel_url: `https://www.youtube.com/channel/${channelId}`,
        handle: `@${suffix.slice(0, 12)}`,
        title: "Full Store",
        country: "Brazil",
        country_code: "BR",
        country_canonical_name: "Brazil",
        subscriber_count: 10_000,
        subscriber_count_text: "10,000",
      },
      sourceJson: { channel_extractor: "youtubejs" },
      aboutObservation: null,
      observedAt: OBSERVED_AT,
      settings: { channelContentLimit: 2, contentMaxAgeDays: 90 },
    });
    let state = await store.restore(job);
    assert.equal(state.phase, "uploads");
    assert.equal(state.channel.ready_for_agent, false);
    assert.deepEqual(state.run.result_json.fetch_contract, YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT);

    const target = upload(`upcoming-${suffix}`, 1);
    const document = uploadsDocument([target]);
    await store.commitUploads(job, {
      document,
      targets: document.entries,
      activityEvidence: null,
      observedAt: OBSERVED_AT,
    });
    state = await store.restore(job);
    assert.equal(state.phase, "detail");
    assert.equal(state.candidates.length, 1);

    const changed = uploadsDocument([
      upload(`different-${suffix}`, 1, { title: "Different target" }),
    ]);
    await assert.rejects(
      store.commitUploads(job, {
        document: changed,
        targets: changed.entries,
        activityEvidence: null,
        observedAt: OBSERVED_AT,
      }),
      FullCrawlYoutubeJsCheckpointError,
    );

    await client.query("SAVEPOINT api_pending_invariant");
    await client.query(
      `UPDATE crawler.content_candidates
       SET detail_status='api_pending',api_status='pending'
       WHERE run_id=$1`,
      [runId],
    );
    await assert.rejects(store.restore(job), FullCrawlYoutubeJsCheckpointError);
    await client.query("ROLLBACK TO SAVEPOINT api_pending_invariant");

    const fence = contentDetailExecutionFence(job, {
      executionMode: "channel_inline",
      candidateAttemptFence: state.identity.candidateAttemptFence,
    });
    assert.ok(await store.claimDetailExecution(fence));
    const candidate = await store.claimNextDetail(fence);
    assert.equal(candidate.target.video_id, target.video_id);
    await store.commitDetail(fence, candidate, {
      detail: {
        id: target.video_id,
        title: target.title,
        is_upcoming: true,
        live_status: "is_upcoming",
        access_status: "public",
        access_status_source: "youtubejs_uploads",
        source: "youtubejs_uploads",
      },
      access: {
        access_status: "public",
        access_status_source: "youtubejs_uploads",
      },
      classification: {
        content_type: "live",
        source: "youtubejs_uploads_live_flag",
        authoritative: true,
      },
      terminalReason: "upcoming_live",
      observedAt: OBSERVED_AT,
      locale: "en",
    });
    const closed = await store.closeFetch(fence, { completedAt: OBSERVED_AT });
    assert.equal(closed.receipt.status, "complete");
    assert.equal(closed.excluded, 1);

    state = await store.restore(job);
    assert.equal(state.phase, "handoff");
    assert.equal(state.channel.ready_for_agent, true);
    assert.equal(state.run.detail_status, "done");
    assert.equal(state.fetch.target_hash, state.uploads.receipt.target_hash);
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});
