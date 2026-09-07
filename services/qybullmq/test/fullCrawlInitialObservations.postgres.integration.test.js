import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";
import { BusinessRunBindingStore } from "../src/businessRunBindingStore.js";
import { contentDetailExecutionFence } from "../src/contentDetailExecutionFence.js";
import { YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT } from "../src/fullCrawlFetchContract.js";
import { fullCrawlUploadsDocument } from "../src/fullCrawlYoutubeJsModel.js";
import { FullCrawlYoutubeJsStore } from "../src/fullCrawlYoutubeJsStore.js";
import { recordInitialFullObservations } from "../src/initialFullObservations.js";
import { inspectPublicationInitialPackage, reconcilePublication } from "../src/publicationReconciler.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";
import { publicationCurrentSchemaBlock, publicationCaptureSchemaBlock } from "../src/publicationCurrentSchema.js";

const databaseUrl = process.env.MANAGED_JOB_TEST_DATABASE_URL;
const observedAt = "2026-09-04T00:00:00.000Z";

async function withCompletedCrawl(action, { count = 30, limit = 30, maxAgeDays = 90,
  stopReason = "max_items", parseGapCount = 0, oldLastItem = false, deferredLastItem = false,
  optionalComments = false } = {}) {
  const pool = new pg.Pool({ connectionString: databaseUrl, max: 1,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}` });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCscan${suffix}`;
  const runId = `run:scan:${suffix}`;
  const batchId = `scan-${suffix}`;
  const jobId = `channel-snapshot__${batchId}__${channelId}__g1`;
  const withTransaction = (fn) => fn(client);
  try {
    assert.match((await client.query("SELECT current_database() AS name")).rows[0].name, /_test$/);
    await client.query("BEGIN");
    await client.query(`INSERT INTO crawler.query_dispatch_batches
      (dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at)
      VALUES ($1,$1,'validation_closed',now())`, [batchId]);
    const candidateId = Number((await client.query(`INSERT INTO crawler.channel_candidates
      (dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
       snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt)
      VALUES ($1,$1,$2,$3,'queued',1,$4,1) RETURNING candidate_id`,
    [batchId, channelId, `https://www.youtube.com/channel/${channelId}`, jobId])).rows[0].candidate_id);
    await new BusinessRunBindingStore({ withTransaction }).resolve({
      businessRunKey: runId, explicitBusinessRunId: runId, requestedStatus: "reserved",
      runKind: "full", channelId, candidateId,
      policy: { id: "test-policy", version: 1, hash: "sha256:test-policy" },
      intent: { job_name: "channel-snapshot", crawl_mode: "full",
        fetch_contract: YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT },
    });
    const job = { id: jobId, name: "channel-snapshot", attemptsStarted: 1, data: {
      channel_id: channelId, candidate_id: candidateId, run_id: runId,
      business_run_key: runId, dispatch_batch_id: batchId, dispatch_generation: 1,
      fetch_contract: YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
    } };
    const store = new FullCrawlYoutubeJsStore({ query: client.query.bind(client), withTransaction });
    await store.beginAdmission(job);
    await store.commitAdmission(job, {
      metadata: { channel_id: channelId, channel_url: `https://www.youtube.com/channel/${channelId}`,
        title: "Scan evidence", country: "Brazil", country_code: "BR",
        country_canonical_name: "Brazil", subscriber_count: 10000, subscriber_count_text: "10,000" },
      sourceJson: { channel_extractor: "youtubejs" }, aboutObservation: null, observedAt,
      settings: { channelContentLimit: limit, contentMaxAgeDays: maxAgeDays },
    });
    const document = fullCrawlUploadsDocument({
      playlist_id: `UU${suffix}`, activity_evidence_complete: true,
      entries: Array.from({ length: count }, (_, i) => ({ video_id: `scan-${suffix}-${i}`,
        position: i + 1, title: `Short ${i}`, url: `https://www.youtube.com/shorts/scan-${suffix}-${i}` })),
      scan: { complete: true, stop_reason: stopReason, terminal_reason: stopReason,
        pages: 1, inspected_count: count, parse_gap_count: parseGapCount },
    });
    await store.commitUploads(job, { document, targets: document.entries, activityEvidence: null, observedAt });
    const state = await store.restore(job);
    const fence = contentDetailExecutionFence(job, { executionMode: "channel_inline",
      candidateAttemptFence: state.identity.candidateAttemptFence });
    assert.ok(await store.claimDetailExecution(fence));
    for (let i = 0; i < count; i++) {
      const candidate = await store.claimNextDetail(fence);
      const old = oldLastItem && i === count - 1;
      await store.commitDetail(fence, candidate, {
        detail: { id: candidate.target.video_id, title: candidate.target.title,
          published_at: old ? "2025-01-01T00:00:00.000Z" : "2026-09-03T00:00:00.000Z",
          published_at_status: "exact", published_at_source: "youtubejs_player", published_at_precision: "second",
          duration_seconds: 60, duration_status: "exact", duration_source: "youtubejs_player",
          view_count: 100, view_count_text: "100", view_count_status: "exact", view_count_source: "youtubejs_player",
          like_count: 7, like_count_status: "exact", like_count_source: "youtubejs_next",
          comment_count: 0, comment_count_status: "disabled", comments_disabled: true,
          comment_count_source: i === count - 1 ? null : "youtubejs_next",
          ...(optionalComments ? { comment_count: 17, comment_count_status: "unresolved",
            comments_disabled: false, comment_count_source: "youtubejs_comments", comments_first_page: null,
            youtubejs_comments_error: "optional comments timeout" } : {}),
          description: "Test short", description_status: "exact", description_source: "youtubejs_player",
          access_status: "public", is_live: false, is_upcoming: false },
        access: { access_status: "public", access_status_source: "youtubejs_player", is_members_only: false },
        classification: deferredLastItem && i === count - 1 ? null
          : { content_type: "short", source: "youtube_shorts_canonical", authoritative: true },
        terminalReason: old ? "outside_content_window" : null, observedAt, locale: "en",
      });
    }
    await store.closeFetch(fence, { completedAt: observedAt });
    // Agent output is a fixture; the scan, detail commits and observations use real stores.
    await client.query("UPDATE crawler.channels SET status='active',agent_status='done' WHERE channel_id=$1", [channelId]);
    await client.query(`INSERT INTO crawler.agent_profiles
      (channel_id,agent_mode,input_url,status,metrics_json,prompt_variant,input_content_ids,
       input_content_hash,taxonomy_version,agent_version_hash,attempts)
      VALUES ($1,'basic',$2,'success','{}'::jsonb,'local_offline','{}'::text[],
        'sha256:' || repeat('a',64),'qy-taxonomy-v1','sha256:' || repeat('b',64),1)`,
    [channelId, `https://www.youtube.com/channel/${channelId}`]);
    const observe = (options = {}) => recordInitialFullObservations({
      withTransaction, channelId, runId, observedAt, ...options,
    });
    await action({ client, store, job, runId, channelId, observe });
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
}

test("new Full Crawl checkpoints prove a fully processed 30-candidate scan at Finalize", {
  skip: !databaseUrl,
}, async () => {
  await withCompletedCrawl(async ({ observe }) => {
    const result = await observe();
    assert.equal(result.outcomes.video, "complete");
  });
});

test("new Full Crawl age exclusions prove the window after old details leave Contents", {
  skip: !databaseUrl,
}, async () => {
  await withCompletedCrawl(async ({ observe }) => {
    assert.equal((await observe()).outcomes.video, "complete");
  }, { count: 2, limit: 100, oldLastItem: true });
});

test("new Full Crawl does not claim a processed candidate cap while a detail is deferred", {
  skip: !databaseUrl,
}, async () => {
  await withCompletedCrawl(async ({ observe }) => {
    assert.equal((await observe()).outcomes.video, "partial");
  }, { deferredLastItem: true });
});

for (const [name, options, expected] of [
  ["list end", { count: 2, stopReason: "list_end" }, "complete"],
  ["parse gap", { parseGapCount: 1 }, "partial"],
  ["shorter collection window", { count: 2, stopReason: "list_end", maxAgeDays: 30 }, "partial"],
  ["unproven candidate cap", { count: 2 }, "partial"],
  ["list end with deferred detail", { count: 2, stopReason: "list_end", deferredLastItem: true }, "partial"],
]) {
  test(`new Full Crawl Finalize respects ${name}`, { skip: !databaseUrl }, async () => {
    await withCompletedCrawl(async ({ observe }) => {
      assert.equal((await observe()).outcomes.video, expected);
    }, options);
  });
}

test("new Full Crawl rejects damaged checkpoints rather than trusting a legacy scan", {
  skip: !databaseUrl,
}, async () => {
  await withCompletedCrawl(async ({ client, runId, observe }) => {
    for (const [path, value] of [
      [["full_crawl", "uploads", "uploads_hash"], "sha256:wrong"],
      [["full_crawl", "uploads", "document"], null],
      [["full_crawl", "fetch"], null],
      [["full_crawl", "fetch", "target_hash"], "sha256:wrong"],
      [["full_crawl", "fetch", "selected_count"], 1],
    ]) {
      await client.query("SAVEPOINT damaged_checkpoint");
      await client.query(`UPDATE crawler.channel_runs SET result_json=
        jsonb_set(result_json,$2::text[],$3::jsonb) || jsonb_build_object('upload_scan',
          jsonb_build_object('stop_reason','list_end','content_max_age_days',90,'parse_gap_count',0))
        WHERE run_id=$1`, [runId, path, JSON.stringify(value)]);
      await assert.rejects(observe(), /scan checkpoint is incomplete or conflicting/);
      await client.query("ROLLBACK TO SAVEPOINT damaged_checkpoint");
    }
    await client.query("UPDATE crawler.content_candidates SET source_content_id=source_content_id || '-changed' WHERE run_id=$1", [runId]);
    await assert.rejects(observe(), /scan checkpoint is incomplete or conflicting/);
  }, { count: 2, stopReason: "list_end" });
});

test("a repair reuses the same Run checkpoint while ordinary replay preserves its original partial observation", {
  skip: !databaseUrl,
}, async () => {
  await withCompletedCrawl(async ({ client, runId, observe }) => {
    // Reproduce the old reader's missing scan evidence without replacing the new checkpoint.
    await client.query("UPDATE crawler.channel_runs SET result_json=result_json-'fetch_contract' WHERE run_id=$1", [runId]);
    const original = await observe();
    assert.equal(original.outcomes.video, "partial");
    await client.query(`UPDATE crawler.channel_runs SET result_json=
      jsonb_set(result_json,'{fetch_contract}',$2::jsonb) WHERE run_id=$1`,
    [runId, JSON.stringify(YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT)]);
    assert.equal((await observe()).outcomes.video, "partial");
    const repair = { revisionType: "repair", repairId: `scan-evidence-${runId}` };
    const repaired = await observe(repair);
    assert.equal(repaired.outcomes.video, "complete");
    assert.notEqual(repaired.observations.video.observation_id, original.observations.video.observation_id);
    const replay = await observe(repair);
    assert.equal(replay.outcomes.video, "complete");
    assert.equal(replay.observations.video.observation_id, repaired.observations.video.observation_id);
  });
});

for (const optionalComments of [false, true]) {
test(`a repaired new Full Crawl publishes actual video items with optionalComments=${optionalComments}`, {
  skip: !databaseUrl,
}, async () => {
  await withCompletedCrawl(async ({ client, channelId, runId, observe }) => {
    assert.equal((await observe({ revisionType: "repair", repairId: `publish-${runId}` })).outcomes.video, "complete");
    const publication = await inspectPublicationInitialPackage(client, { channelId, asOf: observedAt });
    const video = publication.domains.find(row => row.domain === "video");
    assert.equal(video.readiness_status, "ready", JSON.stringify(video.readiness_reasons));
    const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
    await client.query(publicationCurrentSchemaBlock(schema));
    await client.query(publicationCaptureSchemaBlock(schema));
    const streamId = randomUUID();
    await client.query(`INSERT INTO publication.stream (
      publication_stream_id,source_deployment_key,source_identity_json,minimum_writer_version,
      capture_enabled_at,created_by,created_reason,status_changed_by,status_reason)
      VALUES ($1,$2,'{"database":"isolated-test"}'::jsonb,'publication-reconciler-v1',now(),
        'integration-test','scan evidence','integration-test','capture enabled')`, [streamId, runId]);
    await client.query(`INSERT INTO publication.channel_stream_state (
      publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason)
      VALUES ($1,$2,'bootstrap','integration-test','scan evidence')`, [streamId, channelId]);
    await client.query(`INSERT INTO publication.channel_delivery_state (
      destination,publication_stream_id,channel_id,state_changed_by,state_reason)
      VALUES ('business',$1,$2,'integration-test','hold test delivery')`, [streamId, channelId]);
    const command = { channelId, domains: ["video"], asOf: observedAt, revisionType: "repair" };
    const published = await reconcilePublication(client, command);
    assert.equal(published.revisions.length, 1);
    assert.equal((await reconcilePublication(client, command)).revisions.length, 0);
    const messages = (await client.query(`SELECT o.status,r.payload_json AS payload FROM publication.outbox o
      JOIN publication.revision r USING(revision_id) WHERE r.channel_id=$1 AND r.domain='video'`, [channelId])).rows;
    assert.equal(messages.length, 1);
    assert.equal(messages[0].status, "held");
    assert.equal(messages[0].payload.items.length, optionalComments ? 30 : 29);
    if (optionalComments) {
      assert.ok(messages[0].payload.items.every(item => item.comment_count === null && item.comment_count_status === "unresolved"));
      const stored = (await client.query("SELECT comment_count,comment_count_status FROM crawler.contents WHERE channel_id=$1", [channelId])).rows;
      assert.equal(stored.length, 30);
      assert.ok(stored.every(row => Number(row.comment_count) === 17 && row.comment_count_status === "unresolved"));
    }
  }, { optionalComments });
});
}

test("video publication rechecks new checkpoints even after a complete observation exists", {
  skip: !databaseUrl,
}, async () => {
  await withCompletedCrawl(async ({ client, channelId, runId, observe }) => {
    await observe({ revisionType: "repair", repairId: `publish-${runId}` });
    for (const [path, value] of [
      [["full_crawl", "uploads", "uploads_hash"], "sha256:wrong"],
      [["full_crawl", "fetch"], null],
      [["full_crawl", "fetch", "selected_count"], 29],
    ]) {
      await client.query("SAVEPOINT invalid_publication_evidence");
      await client.query(`UPDATE crawler.channel_runs SET result_json=
        jsonb_set(result_json,$2::text[],$3::jsonb) WHERE run_id=$1`, [runId, path, JSON.stringify(value)]);
      const publication = await inspectPublicationInitialPackage(client, { channelId, asOf: observedAt });
      const video = publication.domains.find(row => row.domain === "video");
      assert.equal(video.readiness_status, "not_ready");
      assert.ok(video.readiness_reasons.some(row => row.code === "video_window_termination_unproven"));
      await client.query("ROLLBACK TO SAVEPOINT invalid_publication_evidence");
    }
    await client.query("UPDATE crawler.content_candidates SET source_content_id=source_content_id || '-changed' WHERE run_id=$1", [runId]);
    const publication = await inspectPublicationInitialPackage(client, { channelId, asOf: observedAt });
    assert.equal(publication.domains.find(row => row.domain === "video").readiness_status, "not_ready");
  });
});
