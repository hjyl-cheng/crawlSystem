import assert from "node:assert/strict";
import test from "node:test";
import {
  buildChannelReadiness,
  buildVideoReadiness,
} from "../src/publicationReadinessReport.js";
import { normalizePublicationCurrentCandidate } from "../src/publicationCurrentStore.js";
import { buildVideoPublicationItem } from "../src/videoPublicationCurrent.js";
import {
  buildVideoRevisionDelta,
  inspectPublicationInitialPackage,
  reconcilePublication,
  retractPublicationChannel,
} from "../src/publicationReconciler.js";
import { completePublicationOperationalFixture } from "./support/publicationOperationalFixtures.js";

const STREAM_ID = "11111111-1111-4111-8111-111111111111";
const CHANNEL_ID = "UCpublication-reconciler";
const AS_OF = "2026-07-27T02:00:00.000Z";
const OBSERVED_AT = "2026-07-27T01:00:00.000Z";
const REVISION_OCCURRED_AT = "2026-07-27T03:00:00.000Z";
const REVISION_ID = "22222222-2222-4222-8222-222222222222";

function channelRow(marker = "one", overrides = {}) {
  const factsHash = `sha256:${"a".repeat(64)}`;
  return {
    channel_id: CHANNEL_ID,
    channel_url: `https://www.youtube.com/channel/${CHANNEL_ID}`,
    title: `Publication ${marker}`,
    handle: "@publication",
    avatar_url: "https://yt3.example/avatar.jpg",
    keywords: ["publication", "testing"],
    available_tabs: ["videos", "shorts", "live"],
    about_description: "A complete channel description.",
    country: "Brazil",
    country_code: "BR",
    country_canonical_name: "Brazil",
    joined_date_text: "Joined Jan 1, 2020",
    joined_at: "2020-01-01",
    joined_at_precision: "date_only",
    subscriber_count: "1000",
    subscriber_count_text: "1K subscribers",
    subscriber_count_status: "exact",
    subscriber_count_source: "youtube_about",
    total_view_count: "50000",
    total_view_count_text: "50,000 views",
    total_view_count_status: "exact",
    total_view_count_source: "youtube_about",
    total_video_count: "50",
    total_video_count_text: "50 videos",
    total_video_count_status: "exact",
    total_video_count_source: "youtube_about",
    is_verified: true,
    is_verified_status: "verified",
    external_links: [{
      title: "Website",
      display_url: "example.com",
      url: "https://example.com/",
    }],
    status: "active",
    source_json: {
      channel_header: {
        rss_url: `https://www.youtube.com/feeds/videos.xml?channel_id=${CHANNEL_ID}`,
        vanity_channel_url: "https://www.youtube.com/@publication",
        is_family_safe: true,
      },
    },
    about_last_observed_at: OBSERVED_AT,
    about_current_hash: factsHash,
    about_identity_last_observed_at: OBSERVED_AT,
    about_identity_current_hash: `sha256:${"b".repeat(64)}`,
    ...overrides,
  };
}

function aboutSource(row = channelRow()) {
  const observationId = "33333333-3333-4333-8333-333333333333";
  const runId = "publication-reconciler-run";
  return {
    channel_id: CHANNEL_ID,
    observation_kind: "about",
    cursor: {
      channel_id: CHANNEL_ID,
      observation_kind: "about",
      latest_sequence: "1",
      latest_observation_id: observationId,
      latest_observed_at: OBSERVED_AT,
      latest_complete_observation_id: observationId,
      latest_complete_observed_at: OBSERVED_AT,
      current_facts_hash: row.about_current_hash,
      source_cursor: {},
    },
    latest_observation: {
      observation_id: observationId,
      observed_at: OBSERVED_AT,
      channel_id: CHANNEL_ID,
      observation_kind: "about",
      outcome: "complete",
      outcome_reason_code: "about_complete",
      facts_hash: row.about_current_hash,
    },
    complete_observation: {
      observation_id: observationId,
      observed_at: OBSERVED_AT,
      channel_id: CHANNEL_ID,
      run_id: runId,
      observation_kind: "about",
      kind_sequence: "1",
      outcome: "complete",
      outcome_reason_code: "about_complete",
      facts_hash: row.about_current_hash,
      crawler_version: "integration-test",
      extractor_versions: {},
      result_summary_json: {},
    },
    run: {
      run_id: runId,
      channel_id: CHANNEL_ID,
      status: "done",
      crawl_mode: "incremental",
      plan_id: null,
      policy_version: "v16-rule-6",
      crawler_version: "integration-test",
    },
  };
}

function videoRow(id, overrides = {}) {
  return {
    channel_id: CHANNEL_ID,
    content_key: `${CHANNEL_ID}:video:${id}`,
    source_content_id: id,
    content_type: "video",
    title: `Video ${id}`,
    url: `https://www.youtube.com/watch?v=${id}`,
    thumbnail_url: `https://i.ytimg.com/vi/${id}/hqdefault.jpg`,
    published_at: "2026-07-25T08:00:00.000Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "youtube_player",
    duration_seconds: 120,
    duration_status: "exact",
    duration_source: "youtube_player",
    view_count: 1000,
    view_count_status: "exact",
    view_count_source: "youtube_player",
    like_count: 100,
    like_count_status: "exact",
    like_count_source: "youtube_player",
    comment_count: 10,
    comment_count_status: "exact",
    comment_count_source: "youtube_next",
    comments_disabled: false,
    description: "",
    description_status: "empty",
    description_source: "youtube_player",
    hashtags: [],
    keywords: [],
    access_status: "public",
    access_status_source: "youtube_player",
    is_members_only: false,
    extractor_version: "youtubei.js@test",
    run_id: "video-run",
    last_observation_id: "video-observation",
    playlist_last_seen_at: OBSERVED_AT,
    player_last_observed_at: OBSERVED_AT,
    first_seen_at: "2026-07-25T09:00:00.000Z",
    ...overrides,
  };
}

function withVideoItemHash(row) {
  return {
    ...row,
    publication_item_hash: buildVideoPublicationItem(row, {
      channelId: row.channel_id,
    }).item_hash,
  };
}

function storedChannelCurrent(marker, {
  dataSequence = 0,
  revisionId = null,
} = {}) {
  const row = channelRow(marker);
  const raw = buildChannelReadiness({ row, source: aboutSource(row) });
  assert.equal(raw.ready, true);
  return {
    ...normalizePublicationCurrentCandidate(CHANNEL_ID, "channel", raw),
    data_sequence: String(dataSequence),
    current_revision_id: revisionId,
  };
}

function weakChannelRow(marker = "one") {
  return channelRow(marker, {
    handle: null,
    avatar_url: null,
    keywords: [],
    keywords_status: "observed",
    available_tabs: [],
    available_tabs_status: "observed",
    about_description: null,
    summary: null,
    description_status: "empty",
    country: null,
    country_code: null,
    country_canonical_name: null,
    joined_date_text: null,
    joined_at: null,
    joined_at_precision: "unknown",
    subscriber_count: null,
    subscriber_count_text: null,
    subscriber_count_status: "unresolved",
    subscriber_count_source: null,
    total_view_count: null,
    total_view_count_text: null,
    total_view_count_status: "unresolved",
    total_view_count_source: null,
    total_video_count: null,
    total_video_count_text: null,
    total_video_count_status: "unresolved",
    total_video_count_source: null,
    is_verified: null,
    is_verified_status: "unknown",
    external_links: [],
    external_links_status: "observed",
    source_json: { channel_header: {} },
  });
}

function businessChannelBaseline() {
  return {
    status: "available",
    payload: {
      channel_id: CHANNEL_ID,
      handle: "@trusted-business-handle",
      avatar: [{ url: "https://business.example/avatar.jpg", position: 0 }],
      description: "Trusted Business description",
      is_verified: false,
      is_verified_status: "not_verified",
      subscriber_count: 3210,
      subscriber_count_status: "estimated",
      total_video_count: 45,
      total_video_count_status: "exact",
      total_view_count: 654321,
      total_view_count_status: "exact",
      joined_date: "2019-03-02",
      joined_date_status: "exact",
      joined_date_raw: "Joined Mar 2, 2019",
      country_name: "Brazil",
      links: [{
        title: "Official",
        display_url: null,
        target_url: "https://business.example/",
        favicon_url: null,
        position: 0,
        link_type: "website",
        purpose: "public_reference",
      }],
    },
    source: {
      type: "legacy_business_active_snapshot",
      database_name: "yewu_business",
      active_watermark: "business-release-20260725",
      snapshot_id: "crawler_snapshot_trusted",
      captured_at: "2026-07-25T08:00:00.000Z",
    },
  };
}

function parseJson(value) {
  return value == null ? null : JSON.parse(value);
}

function fakeClient({
  onboardingMode = "baseline",
  captureEnabled = true,
  owned = true,
  streamStatus = "active",
  row = channelRow(),
  sources = null,
  contents = [],
  existing = [],
  deliveries = [],
  rejectTransactionGuard = false,
  minimumWriterVersion = "publication-reconciler-v1",
} = {}) {
  const calls = [];
  const currents = new Map(existing.map((current) => [current.domain, structuredClone(current)]));
  const revisions = [];
  const outbox = [];
  const sourceRows = sources ?? [aboutSource(row)];

  return {
    calls,
    currents,
    revisions,
    outbox,
    async query(sql, params = []) {
      calls.push({ sql, params });
      if (sql.includes("publication-reconciler:transaction-guard")) {
        if (rejectTransactionGuard) throw new Error("SAVEPOINT can only be used in transaction blocks");
        return { rows: [] };
      }
      if (sql.startsWith("RELEASE SAVEPOINT")) return { rows: [] };
      if (sql.includes("publication-reconciler:find-owner")) {
        return { rows: owned ? [{ publication_stream_id: STREAM_ID }] : [] };
      }
      if (sql.includes("publication-reconciler:lock-domains")) {
        return { rows: [...currents.values()] };
      }
      if (sql.includes("publication-reconciler:lock-owner")) {
        return { rows: owned ? [{
          channel_status: "owned",
          onboarding_mode: onboardingMode,
          seed_status: "pending",
          stream_status: streamStatus,
          capture_enabled_at: captureEnabled ? OBSERVED_AT : null,
          minimum_writer_version: minimumWriterVersion,
        }] : [] };
      }
      if (sql.includes("publication-reconciler:refresh-domains")) {
        return { rows: [...currents.values()] };
      }
      if (sql.includes("publication-readiness:sources")) return { rows: sourceRows };
      if (sql.includes("publication-readiness:channels")) return { rows: [{ row }] };
      if (sql.includes("publication-readiness:contents")) {
        return { rows: contents.map((content) => ({ row: content })) };
      }
      if (sql.includes("publication-readiness:agents")) return { rows: [] };
      if (sql.includes("publication-reconciler:lock-deliveries")) return { rows: deliveries };
      if (sql.includes("publication-reconciler:insert-revision")) {
        revisions.push({ sql, params });
        return { rows: [{ occurred_at: REVISION_OCCURRED_AT }] };
      }
      if (sql.includes("publication-reconciler:insert-outbox")) {
        outbox.push({ destination: params[0], revision_id: params[1], status: params[2] });
        return { rows: [] };
      }
      if (sql.includes("publication-reconciler:store-current")) {
        const domain = params[2];
        const previous = currents.get(domain);
        const inserting = sql.includes("INSERT INTO publication.domain_current");
        const ready = params[5] === "ready";
        const allowNotReadyPayload = params[15] === true;
        const writeCandidate = inserting || ready || allowNotReadyPayload;
        const trusted = previous?.payload_json != null;
        const current = {
          domain,
          contract_version: writeCandidate || !trusted
            ? params[3]
            : previous.contract_version,
          policy_version: writeCandidate || !trusted
            ? params[4]
            : previous.policy_version,
          readiness_status: params[5],
          readiness_reasons: parseJson(params[6]),
          payload_json: writeCandidate
            ? parseJson(params[7])
            : previous?.payload_json ?? null,
          result_hash: writeCandidate ? params[8] : previous?.result_hash ?? null,
          source_refs: writeCandidate || !trusted
            ? parseJson(params[9])
            : previous.source_refs,
          complete_observed_at: writeCandidate || !trusted
            ? params[10]
            : previous.complete_observed_at,
          data_sequence: String(params[11]),
          current_revision_id: params[12],
        };
        currents.set(domain, current);
        return { rows: [{
          domain,
          readiness_status: current.readiness_status,
          result_hash: current.result_hash,
          data_sequence: current.data_sequence,
          current_revision_id: current.current_revision_id,
        }] };
      }
      if (sql.includes("publication-reconciler:seed-status")) {
        const values = [...currents.values()];
        const complete = values.length === 3
          && values.every((current) => (
            current.readiness_status === "ready"
            && current.payload_json
            && current.result_hash
          ));
        return { rows: [{
          seed_status: complete ? "complete" : "pending",
          seed_completed_at: complete ? REVISION_OCCURRED_AT : null,
        }] };
      }
      throw new Error(`unexpected query: ${sql}`);
    },
  };
}

test("Reconciler stays inert without owned Publication state", async () => {
  const client = fakeClient({ owned: false });
  const result = await reconcilePublication(client, {
    channelId: CHANNEL_ID,
    domains: ["channel"],
    asOf: AS_OF,
  });

  assert.equal(result.status, "not_owned");
  assert.equal(
    client.calls.some((call) => call.sql.includes("publication-readiness:")),
    false,
  );
});

test("Reconciler does not read Operational Current before Capture is enabled", async () => {
  const client = fakeClient({ captureEnabled: false });
  const result = await reconcilePublication(client, {
    channelId: CHANNEL_ID,
    domains: ["channel"],
    asOf: AS_OF,
  });

  assert.equal(result.status, "capture_disabled");
  assert.equal(client.revisions.length, 0);
  assert.equal(
    client.calls.some((call) => call.sql.includes("publication-readiness:")),
    false,
  );
});

test("Reconciler rejects a Stream requiring a newer Writer before reading source Current", async () => {
  const client = fakeClient({ minimumWriterVersion: "publication-reconciler-v2" });
  await assert.rejects(
    reconcilePublication(client, {
      channelId: CHANNEL_ID,
      domains: ["channel"],
      asOf: AS_OF,
    }),
    /writer version does not satisfy the Stream minimum/,
  );
  assert.equal(
    client.calls.some((call) => call.sql.includes("publication-readiness:sources")),
    false,
  );
});

for (const onboardingMode of ["baseline", "cutover"]) {
  test(`${onboardingMode} onboarding seeds Sequence 0 without a Revision`, async () => {
    const client = fakeClient({ onboardingMode });
    const result = await reconcilePublication(client, {
      channelId: CHANNEL_ID,
      domains: ["channel"],
      asOf: AS_OF,
    });

    assert.equal(result.status, "seeded");
    assert.equal(result.domains[0].data_sequence, 0);
    assert.equal(result.domains[0].current_revision_id, null);
    assert.equal(client.revisions.length, 0);
    assert.equal(client.currents.get("channel").data_sequence, "0");
  });
}

test("Bootstrap creates Sequence 1 and only configured non-sealed Outbox rows", async () => {
  const client = fakeClient({
    onboardingMode: "bootstrap",
    deliveries: [
      { destination: "business-hold", mode: "hold" },
      { destination: "business-online", mode: "online" },
      { destination: "business-sealed", mode: "sealed" },
    ],
  });
  const result = await reconcilePublication(client, {
    channelId: CHANNEL_ID,
    domains: ["channel"],
    asOf: AS_OF,
  });

  assert.equal(result.status, "revised");
  assert.equal(result.revisions.length, 1);
  assert.deepEqual(
    {
      type: result.revisions[0].revision_type,
      operation: result.revisions[0].operation,
      sequence: result.revisions[0].data_sequence,
      previousSequence: result.revisions[0].previous_data_sequence,
      previousHash: result.revisions[0].previous_result_hash,
      occurredAt: result.revisions[0].occurred_at,
    },
    {
      type: "bootstrap",
      operation: "replace",
      sequence: 1,
      previousSequence: null,
      previousHash: null,
      occurredAt: REVISION_OCCURRED_AT,
    },
  );
  assert.match(client.revisions[0].sql, /now\(\)/);
  assert.deepEqual(client.outbox.map(({ destination, status }) => ({ destination, status })), [
    { destination: "business-hold", status: "held" },
    { destination: "business-online", status: "pending" },
  ]);
});

test("NoChange refreshes provenance without allocating a Sequence", async () => {
  const previous = storedChannelCurrent("one", {
    dataSequence: 2,
    revisionId: REVISION_ID,
  });
  const client = fakeClient({ existing: [previous] });
  const result = await reconcilePublication(client, {
    channelId: CHANNEL_ID,
    domains: ["channel"],
    asOf: AS_OF,
  });

  assert.equal(result.status, "no_change");
  assert.equal(result.domains[0].data_sequence, 2);
  assert.equal(result.domains[0].current_revision_id, REVISION_ID);
  assert.equal(client.revisions.length, 0);
});

test("NotReady preserves the last trusted Current and Sequence", async () => {
  const previous = storedChannelCurrent("one", {
    dataSequence: 2,
    revisionId: REVISION_ID,
  });
  const client = fakeClient({
    existing: [previous],
    row: channelRow("unpublishable", { status: "removed" }),
  });
  const result = await reconcilePublication(client, {
    channelId: CHANNEL_ID,
    domains: ["channel"],
    asOf: AS_OF,
  });

  const stored = client.currents.get("channel");
  assert.equal(result.status, "not_ready");
  assert.equal(stored.readiness_status, "not_ready");
  assert.equal(stored.result_hash, previous.result_hash);
  assert.deepEqual(stored.payload_json, previous.payload_json);
  assert.equal(stored.data_sequence, "2");
  assert.equal(stored.current_revision_id, REVISION_ID);
  assert.equal(client.revisions.length, 0);
});

test("a changed trusted Current creates the next contiguous Incremental Revision", async () => {
  const previous = storedChannelCurrent("one", {
    dataSequence: 3,
    revisionId: REVISION_ID,
  });
  const client = fakeClient({
    existing: [previous],
    row: channelRow("two"),
    deliveries: [{ destination: "business", mode: "online" }],
  });
  const result = await reconcilePublication(client, {
    channelId: CHANNEL_ID,
    domains: ["channel"],
    asOf: AS_OF,
  });

  const revision = result.revisions[0];
  assert.equal(revision.revision_type, "incremental");
  assert.equal(revision.operation, "replace");
  assert.equal(revision.data_sequence, 4);
  assert.equal(revision.previous_data_sequence, 3);
  assert.equal(revision.previous_result_hash, previous.result_hash);
  assert.equal(client.currents.get("channel").data_sequence, "4");
  assert.equal(client.currents.get("channel").current_revision_id, revision.revision_id);
});

test("a Channel Revision carries prior non-empty fields across a weak refresh", async () => {
  const previous = storedChannelCurrent("one", {
    dataSequence: 3,
    revisionId: REVISION_ID,
  });
  const client = fakeClient({
    existing: [previous],
    row: weakChannelRow("two"),
    deliveries: [{ destination: "business", mode: "online" }],
  });

  const result = await reconcilePublication(client, {
    channelId: CHANNEL_ID,
    domains: ["channel"],
    asOf: AS_OF,
  });

  const revision = result.revisions[0];
  assert.equal(revision.payload.title, "Publication two");
  for (const field of [
    "vanity_channel_url",
    "handle",
    "avatar",
    "keywords",
    "is_family_safe",
    "is_verified",
    "is_verified_status",
    "has_videos",
    "has_shorts",
    "has_live_streams",
    "description",
    "subscriber_count",
    "subscriber_count_status",
    "total_video_count",
    "total_video_count_status",
    "total_view_count",
    "total_view_count_status",
    "joined_date",
    "joined_date_status",
    "joined_date_raw",
    "country_code",
    "country_name",
    "links",
  ]) {
    assert.deepEqual(revision.payload[field], previous.payload_json[field], field);
  }
  assert.equal(
    revision.source.publication_merge.previous_result_hash,
    previous.result_hash,
  );
  assert.equal(revision.source.publication_merge.carried_forward_fields.includes("links"), true);
});

test("a legacy Bootstrap carries trusted Business fields before Source Current exists", async () => {
  const client = fakeClient({
    onboardingMode: "bootstrap",
    row: weakChannelRow("business-takeover"),
    deliveries: [{ destination: "business", mode: "online" }],
  });
  const businessBaseline = businessChannelBaseline();

  const result = await reconcilePublication(client, {
    channelId: CHANNEL_ID,
    domains: ["channel"],
    asOf: AS_OF,
    preservationBaselines: { channel: businessBaseline },
  });

  const revision = result.revisions[0];
  assert.equal(revision.revision_type, "bootstrap");
  for (const field of [
    "handle",
    "avatar",
    "description",
    "is_verified",
    "is_verified_status",
    "subscriber_count",
    "subscriber_count_status",
    "total_video_count",
    "total_video_count_status",
    "total_view_count",
    "total_view_count_status",
    "joined_date",
    "joined_date_status",
    "joined_date_raw",
    "country_name",
    "links",
  ]) {
    assert.deepEqual(revision.payload[field], businessBaseline.payload[field], field);
  }
  assert.deepEqual(
    revision.source.publication_merge.preservation_source,
    businessBaseline.source,
  );
  assert.equal(revision.source.publication_merge.carried_forward_fields.includes("links"), true);
});

test("a legacy Bootstrap audits a Preservation baseline even when no field needs carrying", async () => {
  const client = fakeClient({
    onboardingMode: "bootstrap",
    deliveries: [{ destination: "business", mode: "online" }],
  });
  const baseline = {
    status: "available",
    payload: {
      channel_id: CHANNEL_ID,
      title: "Older Business title",
    },
    source: businessChannelBaseline().source,
  };

  const result = await reconcilePublication(client, {
    channelId: CHANNEL_ID,
    domains: ["channel"],
    asOf: AS_OF,
    preservationBaselines: { channel: baseline },
  });

  assert.equal(result.revisions[0].payload.title, "Publication one");
  assert.deepEqual(
    result.revisions[0].source.publication_merge.preservation_source,
    baseline.source,
  );
  assert.deepEqual(result.revisions[0].source.publication_merge.carried_forward_fields, []);
});

test("a NotReady legacy Channel seeds preservation before its later automatic Bootstrap", async () => {
  const baseline = businessChannelBaseline();
  const pending = fakeClient({
    onboardingMode: "bootstrap",
    row: weakChannelRow("pending-observation"),
    sources: [],
    deliveries: [{ destination: "business", mode: "online" }],
  });

  const seeded = await reconcilePublication(pending, {
    channelId: CHANNEL_ID,
    domains: ["channel"],
    asOf: AS_OF,
    preservationBaselines: { channel: baseline },
  });

  assert.equal(seeded.domains[0].status, "preservation_seeded");
  assert.equal(seeded.domains[0].data_sequence, 0);
  assert.equal(pending.revisions.length, 0);
  assert.equal(pending.outbox.length, 0);
  const preserved = pending.currents.get("channel");
  assert.deepEqual(preserved.payload_json, baseline.payload);
  assert.equal(preserved.readiness_status, "not_ready");
  assert.equal(preserved.current_revision_id, null);
  assert.deepEqual(
    preserved.source_refs.publication_preservation.source,
    baseline.source,
  );
  assert.equal(seeded.seed_status, "pending");

  const replay = fakeClient({
    onboardingMode: "bootstrap",
    existing: [preserved],
    row: weakChannelRow("pending-observation"),
    sources: [],
    deliveries: [{ destination: "business", mode: "online" }],
  });
  const retained = await reconcilePublication(replay, {
    channelId: CHANNEL_ID,
    domains: ["channel"],
    asOf: AS_OF,
    preservationBaselines: { channel: baseline },
  });
  assert.equal(retained.status, "no_change");
  assert.equal(retained.domains[0].status, "preservation_retained");
  assert.equal(retained.domains[0].result_hash, preserved.result_hash);
  assert.equal(replay.revisions.length, 0);
  assert.equal(replay.outbox.length, 0);

  const ready = fakeClient({
    onboardingMode: "bootstrap",
    existing: [preserved],
    row: weakChannelRow("later-observation"),
    deliveries: [{ destination: "business", mode: "online" }],
  });
  const bootstrapped = await reconcilePublication(ready, {
    channelId: CHANNEL_ID,
    domains: ["channel"],
    asOf: AS_OF,
  });

  const revision = bootstrapped.revisions[0];
  assert.equal(revision.revision_type, "bootstrap");
  assert.equal(revision.previous_data_sequence, null);
  assert.equal(revision.previous_result_hash, null);
  assert.deepEqual(revision.payload.links, baseline.payload.links);
  assert.deepEqual(
    revision.source.publication_merge.preservation_source,
    baseline.source,
  );
});

test("a weak Channel refresh with no real change does not create a Revision", async () => {
  const previous = storedChannelCurrent("one", {
    dataSequence: 3,
    revisionId: REVISION_ID,
  });
  const client = fakeClient({
    existing: [previous],
    row: weakChannelRow("one"),
    deliveries: [{ destination: "business", mode: "online" }],
  });

  const result = await reconcilePublication(client, {
    channelId: CHANNEL_ID,
    domains: ["channel"],
    asOf: AS_OF,
  });

  assert.equal(result.status, "no_change");
  assert.equal(client.revisions.length, 0);
  assert.equal(client.outbox.length, 0);
  assert.equal(client.currents.get("channel").data_sequence, "3");
});

test("an explicit Repair creates a contiguous Repair Revision", async () => {
  const previous = storedChannelCurrent("one", {
    dataSequence: 3,
    revisionId: REVISION_ID,
  });
  const client = fakeClient({
    existing: [previous],
    row: channelRow("repaired"),
    deliveries: [{ destination: "business", mode: "online" }],
  });
  const result = await reconcilePublication(client, {
    channelId: CHANNEL_ID,
    domains: ["channel"],
    asOf: AS_OF,
    revisionType: "repair",
  });

  const revision = result.revisions[0];
  assert.equal(revision.revision_type, "repair");
  assert.equal(revision.operation, "replace");
  assert.equal(revision.data_sequence, 4);
  assert.equal(revision.previous_data_sequence, 3);
  assert.equal(revision.previous_result_hash, previous.result_hash);
});

test("Channel Retraction requires a previously published Revision", async () => {
  const client = fakeClient({
    existing: [storedChannelCurrent("seeded")],
  });
  const result = await retractPublicationChannel(client, {
    channelId: CHANNEL_ID,
    reasonCode: "channel_not_found",
    removedAt: AS_OF,
    source: "youtube_alert",
    evidence: "This channel does not exist.",
  });

  assert.equal(result.status, "not_previously_published");
  assert.equal(client.revisions.length, 0);
  assert.equal(client.outbox.length, 0);
});

test("Channel Retraction is contiguous, durable, and idempotent", async () => {
  const previous = storedChannelCurrent("published", {
    dataSequence: 3,
    revisionId: REVISION_ID,
  });
  const client = fakeClient({
    existing: [previous],
    deliveries: [
      { destination: "business-hold", mode: "hold" },
      { destination: "business-online", mode: "online" },
    ],
  });
  const command = {
    channelId: CHANNEL_ID,
    reasonCode: "channel_not_found",
    removedAt: AS_OF,
    source: "youtube_alert",
    evidence: "This channel does not exist.",
  };

  const result = await retractPublicationChannel(client, command);

  const revision = result.revisions[0];
  assert.equal(revision.revision_type, "retraction");
  assert.equal(revision.operation, "retract_channel");
  assert.equal(revision.data_sequence, 4);
  assert.equal(revision.previous_data_sequence, 3);
  assert.equal(revision.previous_result_hash, previous.result_hash);
  assert.deepEqual(revision.payload.retraction, {
    object_id: CHANNEL_ID,
    domain: "channel",
    reason_code: "channel_not_found",
    source: "youtube_alert",
    evidence: {
      type: "terminal_channel_response",
      source: "youtube_alert",
      detail: "This channel does not exist.",
    },
    removed_at: AS_OF,
  });
  assert.deepEqual(client.outbox.map(({ destination, status }) => ({ destination, status })), [
    { destination: "business-hold", status: "held" },
    { destination: "business-online", status: "pending" },
  ]);
  assert.equal(client.currents.get("channel").data_sequence, "4");

  const duplicate = await retractPublicationChannel(client, command);
  assert.equal(duplicate.status, "no_change");
  assert.equal(client.revisions.length, 1);
  assert.equal(client.currents.get("channel").data_sequence, "4");
});

test("multi-Domain reconciliation uses canonical order and keeps partial NotReady diagnostics", async () => {
  const row = channelRow();
  const client = fakeClient({ row, sources: [aboutSource(row)] });
  const result = await reconcilePublication(client, {
    channelId: CHANNEL_ID,
    domains: ["agent", "channel", "video"],
    asOf: AS_OF,
  });

  assert.deepEqual(result.domains.map((domain) => domain.domain), ["channel", "video", "agent"]);
  assert.deepEqual(result.domains.map((domain) => domain.status), ["seeded", "not_ready", "not_ready"]);
  assert.deepEqual(
    client.calls
      .filter((call) => call.sql.includes("publication-reconciler:store-current"))
      .map((call) => call.params[2]),
    ["channel", "video", "agent"],
  );
  assert.equal(client.revisions.length, 0);
});

test("Initial Package inspection requires all three Domains and Video termination proof", async () => {
  const fixture = completePublicationOperationalFixture(CHANNEL_ID, { observedAt: OBSERVED_AT });
  const videoSource = fixture.sources.find((source) => source.observation_kind === "video");
  videoSource.cursor.source_cursor = {};
  videoSource.complete_observation.result_summary_json.discovery.stop_reason = null;
  const client = {
    async query(sql) {
      const statement = String(sql);
      if (statement.includes("publication-readiness:sources")) return { rows: fixture.sources };
      if (statement.includes("publication-readiness:channels")) {
        return { rows: [{ row: fixture.channel }] };
      }
      if (statement.includes("publication-readiness:contents")) return { rows: [] };
      if (statement.includes("publication-readiness:agents")) {
        return { rows: [{ row: fixture.agent, config: fixture.agentConfig }] };
      }
      throw new Error(`unexpected query: ${statement}`);
    },
  };

  const incomplete = await inspectPublicationInitialPackage(client, {
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });
  assert.equal(incomplete.status, "not_ready");
  assert.deepEqual(incomplete.domains.map((domain) => domain.domain), ["channel", "video", "agent"]);
  assert.equal(
    incomplete.domains.find((domain) => domain.domain === "video")
      .readiness_reasons.some((reason) => reason.code === "video_window_termination_unproven"),
    true,
  );

  videoSource.cursor.source_cursor = { terminal_reason: "list_end" };
  videoSource.complete_observation.result_summary_json.discovery.stop_reason = "list_end";
  const complete = await inspectPublicationInitialPackage(client, {
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });
  assert.equal(complete.status, "ready");
  assert.equal(complete.domains.every((domain) => domain.readiness_status === "ready"), true);
});

test("Video reconciliation reuses a trusted window after exact anchor closure", async () => {
  const anchorId = "trusted-anchor";
  const baselineObservedAt = "2026-07-26T10:00:00.000Z";
  const fixture = completePublicationOperationalFixture(CHANNEL_ID, {
    observedAt: baselineObservedAt,
  });
  const baselineSource = fixture.sources.find((item) => item.observation_kind === "video");
  const baselineRow = withVideoItemHash(videoRow(anchorId, {
    last_observation_id: baselineSource.complete_observation.observation_id,
    playlist_last_seen_at: baselineObservedAt,
    player_last_observed_at: baselineObservedAt,
  }));
  const baseline = buildVideoReadiness({
    rows: [baselineRow],
    source: baselineSource,
    channelId: CHANNEL_ID,
    asOf: "2026-07-26T12:00:00.000Z",
  });
  assert.equal(baseline.ready, true);
  const previous = {
    ...normalizePublicationCurrentCandidate(CHANNEL_ID, "video", baseline),
    data_sequence: "1",
    current_revision_id: REVISION_ID,
  };

  const incrementalSource = structuredClone(baselineSource);
  const observationId = "video-incremental-observation";
  incrementalSource.cursor.latest_observation_id = observationId;
  incrementalSource.cursor.latest_observed_at = OBSERVED_AT;
  incrementalSource.cursor.latest_complete_observation_id = observationId;
  incrementalSource.cursor.latest_complete_observed_at = OBSERVED_AT;
  incrementalSource.cursor.source_cursor = {
    terminal_reason: "anchor_matched",
    matched_anchor_id: anchorId,
  };
  incrementalSource.latest_observation = {
    ...incrementalSource.latest_observation,
    observation_id: observationId,
    observed_at: OBSERVED_AT,
  };
  incrementalSource.complete_observation = {
    ...incrementalSource.complete_observation,
    observation_id: observationId,
    observed_at: OBSERVED_AT,
    result_summary_json: {
      discovery: {
        items: 1,
        pages: 1,
        stop_reason: "anchor_matched",
        anchor_matched: true,
        parse_gap_count: 0,
        detail_failure_count: 0,
      },
    },
  };
  const currentRow = withVideoItemHash({
    ...baselineRow,
    last_observation_id: observationId,
    playlist_last_seen_at: OBSERVED_AT,
    player_last_observed_at: OBSERVED_AT,
  });
  const client = fakeClient({
    existing: [previous],
    sources: [incrementalSource],
    contents: [currentRow],
  });

  const result = await reconcilePublication(client, {
    channelId: CHANNEL_ID,
    domains: ["video"],
    asOf: AS_OF,
  });

  assert.equal(result.status, "no_change");
  assert.equal(result.domains[0].status, "no_change");
  assert.equal(client.currents.get("video").readiness_status, "ready");
  assert.equal(
    client.currents.get("video").payload_json.window_proof.terminal_condition,
    "trusted_anchor_continuity",
  );
  assert.equal(result.revisions.length, 0);
});

test("Video Delta records only proven Window Exits", () => {
  const oldHash = `sha256:${"1".repeat(64)}`;
  const nextHash = `sha256:${"2".repeat(64)}`;
  const current = {
    payload_json: {
      items: [
        { content_id: "kept", position: 1, item_hash: oldHash },
        { content_id: "outside", position: 2, item_hash: oldHash },
      ],
    },
  };
  const candidate = {
    result_hash: nextHash,
    payload: {
      channel_id: CHANNEL_ID,
      window_policy: { cutoff_at: "2026-04-28T02:00:00.000Z", max_items: 30 },
      window_proof: { complete: true },
      items: [{ content_id: "kept", position: 1, item_hash: nextHash }],
    },
    exclusions: [{ content_id: "outside", reason_code: "outside_limit" }],
  };

  const delta = buildVideoRevisionDelta(current, candidate);
  assert.equal(delta.ready, true);
  assert.deepEqual(delta.payload.upserts, candidate.payload.items);
  assert.deepEqual(delta.payload.window_exits, [{ content_id: "outside", reason: "outside_limit" }]);
  assert.deepEqual(delta.payload.retractions, []);
});

test("Video Delta explicitly retracts a previously public Item that becomes unlisted", () => {
  const itemHash = `sha256:${"5".repeat(64)}`;
  const current = {
    payload_json: {
      items: [{ content_id: "now-unlisted", position: 1, item_hash: itemHash }],
    },
  };
  const candidate = {
    result_hash: `sha256:${"6".repeat(64)}`,
    payload: {
      channel_id: CHANNEL_ID,
      window_policy: { cutoff_at: "2026-04-28T02:00:00.000Z", max_items: 30 },
      window_proof: { complete: true },
      items: [],
    },
    exclusions: [{ content_id: "now-unlisted", reason_code: "source_unlisted" }],
  };

  const delta = buildVideoRevisionDelta(current, candidate);

  assert.equal(delta.ready, true);
  assert.deepEqual(delta.payload.window_exits, []);
  assert.deepEqual(delta.payload.retractions, [{
    content_id: "now-unlisted",
    reason: "source_unlisted",
  }]);
  assert.deepEqual(delta.issues, []);
});

test("a full Video Window does not by itself prove why old Content disappeared", () => {
  const itemHash = `sha256:${"3".repeat(64)}`;
  const current = {
    payload_json: {
      items: [{
        content_id: "missing-without-proof",
        position: 1,
        item_hash: itemHash,
        published_at: "2026-07-20T00:00:00.000Z",
      }],
    },
  };
  const candidate = {
    result_hash: `sha256:${"4".repeat(64)}`,
    payload: {
      channel_id: CHANNEL_ID,
      window_policy: {
        cutoff_at: "2026-04-28T02:00:00.000Z",
        cutoff_date: "2026-04-28",
        max_items: 30,
      },
      window_proof: { complete: true },
      items: Array.from({ length: 30 }, (_, index) => ({
        content_id: `new-${index}`,
        position: index + 1,
        item_hash: itemHash,
      })),
    },
    exclusions: [],
  };

  const delta = buildVideoRevisionDelta(current, candidate);
  assert.equal(delta.ready, false);
  assert.deepEqual(delta.payload.window_exits, []);
  assert.deepEqual(delta.issues, [{
    domain: "video",
    code: "video_window_exit_reason_unproven",
    content_id: "missing-without-proof",
  }]);
});

test("Reconciler rejects a client without an open transaction", async () => {
  const client = fakeClient({ rejectTransactionGuard: true });
  await assert.rejects(
    reconcilePublication(client, {
      channelId: CHANNEL_ID,
      domains: ["channel"],
      asOf: AS_OF,
    }),
    /SAVEPOINT can only be used in transaction blocks/,
  );
  assert.equal(client.calls.length, 1);
});
