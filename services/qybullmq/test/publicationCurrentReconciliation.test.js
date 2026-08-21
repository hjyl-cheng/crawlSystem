import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { publicationCurrentReconciliationCommand } from "../scripts/reconcilePublicationCurrent.mjs";
import {
  BUSINESS_CHANNEL_PRESERVATION_SQL,
  buildBusinessChannelPreservationBaselines,
} from "../src/publicationBusinessPreservation.js";
import {
  PublicationCurrentReconciliationAdministrator,
  PublicationCurrentReconciliationPartialFailure,
  buildPublicationCurrentReconciliationTarget,
  publicationCurrentReconciliationConfig,
  publicationCurrentReconciliationConfirmation,
  publicationCurrentReconciliationSummary,
  readPublicationCurrentReconciliationEvidence,
} from "../src/publicationCurrentReconciliation.js";

const STREAM_ID = "11111111-1111-4111-8111-111111111111";
const AS_OF = "2026-07-29T02:00:00.000Z";
const CHANNEL_IDS = ["UC-current-a", "UC-current-b"];
const RESULT_HASH = `sha256:${"a".repeat(64)}`;

function environment(overrides = {}) {
  return {
    CRAWLER_DATABASE_URL: "postgres://crawler-admin@crawler/crawler_test",
    EXPECTED_CRAWLER_DATABASE: "crawler_test",
    EXPECTED_CRAWLER_CHANNEL_COUNT: "1561",
    BUSINESS_DATABASE_URL: "postgres://business-reader@business/business_test",
    EXPECTED_BUSINESS_DATABASE: "business_test",
    EXPECTED_BUSINESS_CHANNEL_COUNT: "1643",
    PUBLICATION_STREAM_ID: STREAM_ID,
    PUBLICATION_DESTINATION: "business",
    PUBLICATION_CURRENT_RECONCILE_KEY: "empty-preservation-20260729-v1",
    PUBLICATION_CURRENT_RECONCILE_AS_OF: AS_OF,
    PUBLICATION_CHANNEL_IDS_FILE: "/tmp/publication-current-channels.txt",
    PUBLICATION_RECONCILE_DOMAINS: "channel",
    PUBLICATION_OPERATOR: "publication-test",
    PUBLICATION_ACTION_REASON: "reconcile Current without refetch",
    ...overrides,
  };
}

function fixture() {
  const config = publicationCurrentReconciliationConfig(environment());
  const target = buildPublicationCurrentReconciliationTarget(config, CHANNEL_IDS);
  return { config, target };
}

function result(channelId, {
  status = "revision_created",
  resultHash = RESULT_HASH,
  dataSequence = 2,
  carriedForwardFields = ["links"],
  domain = "channel",
} = {}) {
  return {
    status: status === "revision_created" ? "revised" : status,
    publication_stream_id: STREAM_ID,
    channel_id: channelId,
    seed_status: "complete",
    domains: [{
      domain,
      status,
      result_hash: resultHash,
      data_sequence: dataSequence,
      current_revision_id: status === "revision_created"
        ? "22222222-2222-4222-8222-222222222222"
        : "11111111-1111-4111-8111-111111111112",
      carried_forward_fields: carriedForwardFields,
    }],
    revisions: status === "revision_created" ? [{ domain }] : [],
  };
}

function fakePool(config, target, { unexpectedOpenDeliveryCount = 0 } = {}) {
  const calls = [];
  const clients = [];
  return {
    calls,
    clients,
    async connect() {
      const clientCalls = [];
      const client = {
        calls: clientCalls,
        release() {
          clientCalls.push({ sql: "RELEASE_CLIENT", params: [] });
        },
        async query(sql, params = []) {
          const text = String(sql);
          calls.push({ sql: text, params });
          clientCalls.push({ sql: text, params });
          if (text.includes("current_database() AS database_name")) {
            return {
              rows: [{
                database_name: config.expectedCrawlerDatabase,
                channel_count: config.expectedCrawlerChannelCount,
              }],
            };
          }
          if (text.includes("FROM publication.stream WHERE")) {
            return {
              rows: [{
                status: "active",
                capture_enabled_at: "2026-07-27T09:18:40.222Z",
                minimum_writer_version: "publication-reconciler-v1",
              }],
            };
          }
          if (text.includes("FROM unnest($2::text[])")) {
            return {
              rows: [{
                owned_channel_count: target.channel_count,
                online_delivery_count: target.channel_count,
                unexpected_open_delivery_count: unexpectedOpenDeliveryCount,
                domain_current_count: target.channel_count * target.domains.length,
              }],
            };
          }
          return { rows: [], rowCount: 0 };
        },
      };
      clients.push(client);
      return client;
    },
  };
}

function fakeBusinessPool(config, target, {
  activeWatermark = "business-release-20260725",
  title = "Trusted Business title",
  preservationRows = null,
} = {}) {
  const calls = [];
  return {
    calls,
    async connect() {
      return {
        release() {},
        async query(sql, params = []) {
          const statement = String(sql);
          calls.push({ sql: statement, params });
          if (statement.includes("publication-current-reconciliation:business-identity")) {
            return { rows: [{
              database_name: config.expectedBusinessDatabase,
              channel_count: config.expectedBusinessChannelCount,
              active_watermark: activeWatermark,
            }] };
          }
          if (statement.includes("publication-current-reconciliation:business-preservation")) {
            return { rows: preservationRows ?? target.channel_ids.map((channelId, index) => ({
              target_channel_id: channelId,
              active_watermark: activeWatermark,
              snapshot_id: `business-snapshot-${index}`,
              snapshot_channel_id: channelId,
              snapshot_captured_at: "2026-07-25T08:00:00.000Z",
              title,
              handle: `@business-${index}`,
              avatar_url: null,
              description: "Trusted Business description",
              is_verified: false,
              subscriber_count: "100",
              subscriber_count_status: "exact",
              total_view_count: "200",
              total_view_count_status: "exact",
              video_count: "3",
              video_count_status: "exact",
              joined_date: "2020-01-02",
              joined_date_text: "Joined Jan 2, 2020",
              joined_date_status: "exact",
              country_text: "Brazil",
              raw_country_code: "BR",
              raw_country_canonical_name: "Brazil",
              channel_url: `https://www.youtube.com/channel/${channelId}`,
              link_id: null,
            })) };
          }
          return { rows: [], rowCount: 0 };
        },
      };
    },
  };
}

test("Business Active Snapshots become bounded Channel preservation baselines", () => {
  const rows = [{
    target_channel_id: CHANNEL_IDS[0],
    active_watermark: "business-release-20260725",
    snapshot_id: "snapshot-a",
    snapshot_channel_id: CHANNEL_IDS[0],
    snapshot_captured_at: "2026-07-25T08:00:00.000Z",
    title: "Trusted title",
    handle: "@trusted",
    avatar_url: "https://yt3.googleusercontent.com/avatar.jpg?token=temporary",
    description: "Trusted description",
    is_verified: false,
    subscriber_count: "0",
    subscriber_count_status: "exact",
    total_view_count: "1234",
    total_view_count_status: "exact",
    video_count: "5",
    video_count_status: "approximate",
    joined_date: "2020-01-02",
    joined_date_text: "Joined Jan 2, 2020",
    joined_date_status: "exact",
    country_text: "Brazil",
    raw_country_code: "BR",
    raw_country_canonical_name: "Brazil",
    channel_url: `https://www.youtube.com/channel/${CHANNEL_IDS[0]}`,
    link_id: "link-a",
    link_type: "website",
    link_url: "https://Example.com/?utm_source=youtube",
    link_title: "Official",
    raw_link: { purpose: "public_reference", rawValue: "example.com" },
    link_position: "0",
  }, {
    target_channel_id: CHANNEL_IDS[1],
    active_watermark: "business-release-20260725",
    snapshot_id: null,
    registry_exists: false,
    historical_snapshot_exists: false,
  }];

  const baselines = buildBusinessChannelPreservationBaselines(rows, CHANNEL_IDS, {
    databaseName: "yewu_business",
  });
  const available = baselines.get(CHANNEL_IDS[0]);
  assert.equal(available.status, "available");
  assert.equal(available.payload.subscriber_count, 0);
  assert.equal(available.payload.is_verified, false);
  assert.equal(available.payload.total_video_count_status, "estimated");
  assert.equal(available.payload.avatar[0].url, "https://yt3.googleusercontent.com/avatar.jpg");
  assert.equal(available.payload.links[0].target_url, "https://example.com/");
  assert.match(available.payload_hash, /^sha256:[0-9a-f]{64}$/);
  assert.deepEqual(baselines.get(CHANNEL_IDS[1]), {
    status: "not_found",
    payload: null,
    payload_hash: null,
    source: {
      type: "legacy_business_active_snapshot_lookup",
      database_name: "yewu_business",
      active_watermark: "business-release-20260725",
      absence_checks: {
        channel_registry: false,
        historical_snapshot: false,
      },
    },
  });
});

test("Business preservation follows the active Creator Search storage mode", () => {
  assert.match(BUSINESS_CHANNEL_PRESERVATION_SQL, /creator_search_storage_state/);
  assert.match(BUSINESS_CHANNEL_PRESERVATION_SQL, /public\.creator_search_live/);
  assert.match(BUSINESS_CHANNEL_PRESERVATION_SQL, /public\.creator_search_current/);

  const baselines = buildBusinessChannelPreservationBaselines([{
    target_channel_id: CHANNEL_IDS[0],
    storage_read_mode: "live",
    active_watermark: "publication-projection-live",
    snapshot_id: "snapshot-live",
    snapshot_channel_id: CHANNEL_IDS[0],
    snapshot_captured_at: "2026-08-21T01:00:00.000Z",
    title: "Live Channel",
    channel_url: `https://www.youtube.com/channel/${CHANNEL_IDS[0]}`,
    link_id: null,
  }], [CHANNEL_IDS[0]], { databaseName: "yewu_business" });

  assert.deepEqual(baselines.get(CHANNEL_IDS[0]).source, {
    type: "business_live_snapshot",
    database_name: "yewu_business",
    active_watermark: "publication-projection-live",
    snapshot_id: "snapshot-live",
    captured_at: "2026-08-21T01:00:00.000Z",
  });
});

test("Business presence outside the Active release cannot become an empty baseline", () => {
  for (const presence of [{
    registry_exists: true,
    historical_snapshot_exists: false,
  }, {
    registry_exists: true,
    historical_snapshot_exists: true,
  }]) {
    assert.throws(
      () => buildBusinessChannelPreservationBaselines([{
        target_channel_id: CHANNEL_IDS[0],
        active_watermark: "business-release-20260725",
        snapshot_id: null,
        ...presence,
      }], [CHANNEL_IDS[0]], { databaseName: "yewu_business" }),
      /exists outside the Active release/,
    );
  }
});

test("Current reconciliation config and CLI require an explicit bounded target", () => {
  const config = publicationCurrentReconciliationConfig(environment());
  assert.deepEqual(config.domains, ["channel"]);
  assert.equal(config.concurrency, 4);
  assert.equal(config.evidenceFile, null);
  assert.equal(config.expectedBusinessDatabase, "business_test");
  assert.deepEqual(publicationCurrentReconciliationCommand([]), {
    help: false,
    apply: false,
    output: null,
  });
  assert.deepEqual(
    publicationCurrentReconciliationCommand(["--apply", "--output", "/tmp/result.json"]),
    { help: false, apply: true, output: "/tmp/result.json" },
  );
  assert.throws(
    () => publicationCurrentReconciliationConfig(environment(), { apply: true }),
    /EVIDENCE_FILE/,
  );
  assert.throws(
    () => publicationCurrentReconciliationConfig(environment({
      PUBLICATION_RECONCILE_DOMAINS: "channel,unknown",
    })),
    /unsupported Publication Domain/,
  );
  assert.throws(
    () => buildPublicationCurrentReconciliationTarget(config, [CHANNEL_IDS[0], CHANNEL_IDS[0]]),
    /unique/,
  );
  assert.throws(() => publicationCurrentReconciliationCommand(["--all"]), /unknown option/);
});

test("rollback preview executes the real Reconciler but commits no writes", async () => {
  const { config, target } = fixture();
  const pool = fakePool(config, target);
  const businessPool = fakeBusinessPool(config, target);
  const reconciled = [];
  const administrator = new PublicationCurrentReconciliationAdministrator({
    crawlerPool: pool,
    businessPool,
    config,
    target,
    reconcile: async (_client, input) => {
      reconciled.push(input);
      return result(input.channelId);
    },
    lock: async () => {},
  });

  const evidence = await administrator.inspectRollbackPreview({
    generatedAt: "2026-07-29T02:01:00.000Z",
  });
  const summary = publicationCurrentReconciliationSummary(evidence);

  assert.deepEqual(reconciled.map((item) => item.channelId), CHANNEL_IDS);
  assert.equal(reconciled.every((item) => item.revisionType === "repair"), true);
  assert.equal(reconciled.every((item) => (
    item.preservationBaselines.channel.status === "available"
  )), true);
  assert.equal(pool.calls.filter((call) => call.sql === "ROLLBACK").length, CHANNEL_IDS.length);
  assert.equal(pool.calls.some((call) => call.sql === "COMMIT" && !call.sql.includes("READ")), true);
  assert.equal(summary.domains.channel.outcomes.revision_created, 2);
  assert.equal(summary.domains.channel.carried_forward, 2);
  assert.deepEqual(summary.business_preservation_baselines, { available: 2 });
  assert.equal(summary.crawler_refetch_performed, false);
  assert.match(
    publicationCurrentReconciliationConfirmation(config, target, evidence),
    /^RECONCILE_PUBLICATION_CURRENT:/,
  );
});

test("video-only reconciliation does not require a Business Channel preservation baseline", async () => {
  const config = publicationCurrentReconciliationConfig(environment({
    PUBLICATION_RECONCILE_DOMAINS: "video",
  }));
  const target = buildPublicationCurrentReconciliationTarget(config, CHANNEL_IDS);
  const businessPool = fakeBusinessPool(config, target, {
    preservationRows: target.channel_ids.map((channelId) => ({
      target_channel_id: channelId,
      active_watermark: "business-release-20260725",
      snapshot_id: null,
      registry_exists: true,
      historical_snapshot_exists: true,
    })),
  });
  const reconciled = [];
  const administrator = new PublicationCurrentReconciliationAdministrator({
    crawlerPool: fakePool(config, target),
    businessPool,
    config,
    target,
    reconcile: async (_client, input) => {
      reconciled.push(input);
      return result(input.channelId, { domain: "video", carriedForwardFields: [] });
    },
    lock: async () => {},
  });

  const evidence = await administrator.inspectRollbackPreview({
    generatedAt: "2026-07-29T02:01:00.000Z",
  });

  assert.equal(
    businessPool.calls.some((call) => (
      call.sql.includes("publication-current-reconciliation:business-preservation")
    )),
    false,
  );
  assert.equal(evidence.business_state.target_active_snapshot_count, 0);
  assert.equal(evidence.channels.every((channel) => channel.preservation_baseline === null), true);
  assert.equal(reconciled.every((input) => input.preservationBaselines === undefined), true);
  assert.deepEqual(publicationCurrentReconciliationSummary(evidence).business_preservation_baselines, {});

  const applyBusinessPool = fakeBusinessPool(config, target, {
    preservationRows: target.channel_ids.map((channelId) => ({
      target_channel_id: channelId,
      active_watermark: "business-release-20260725",
      snapshot_id: null,
      registry_exists: true,
      historical_snapshot_exists: true,
    })),
  });
  const apply = new PublicationCurrentReconciliationAdministrator({
    crawlerPool: fakePool(config, target),
    businessPool: applyBusinessPool,
    config,
    target,
    evidence,
    reconcile: async (_client, input) => (
      result(input.channelId, { domain: "video", carriedForwardFields: [] })
    ),
    lock: async () => {},
  });

  const applied = await apply.apply();
  assert.equal(applied.succeeded, CHANNEL_IDS.length);
  assert.equal(
    applyBusinessPool.calls.some((call) => (
      call.sql.includes("publication-current-reconciliation:business-preservation")
    )),
    false,
  );
});

test("rollback preview refuses an unexpected unsealed Destination", async () => {
  const { config, target } = fixture();
  const administrator = new PublicationCurrentReconciliationAdministrator({
    crawlerPool: fakePool(config, target, { unexpectedOpenDeliveryCount: 1 }),
    businessPool: fakeBusinessPool(config, target),
    config,
    target,
    reconcile: async (_client, input) => result(input.channelId),
    lock: async () => {},
  });

  await assert.rejects(
    administrator.inspectRollbackPreview(),
    /unexpected unsealed Publication Destination/,
  );
});

test("evidence is immutable and bound to the exact Channel set", async () => {
  const { config, target } = fixture();
  const pool = fakePool(config, target);
  const businessPool = fakeBusinessPool(config, target);
  const administrator = new PublicationCurrentReconciliationAdministrator({
    crawlerPool: pool,
    businessPool,
    config,
    target,
    reconcile: async (_client, input) => result(input.channelId),
    lock: async () => {},
  });
  const evidence = await administrator.inspectRollbackPreview({
    generatedAt: "2026-07-29T02:01:00.000Z",
  });
  const directory = await mkdtemp(join(tmpdir(), "publication-current-reconcile-"));
  try {
    const valid = join(directory, "valid.json");
    await writeFile(valid, `${JSON.stringify({ evidence })}\n`, "utf8");
    assert.deepEqual(
      await readPublicationCurrentReconciliationEvidence(valid, config, target),
      evidence,
    );

    const tampered = join(directory, "tampered.json");
    const changed = structuredClone(evidence);
    changed.channels[0].result.domains[0].result_hash = `sha256:${"f".repeat(64)}`;
    await writeFile(tampered, `${JSON.stringify({ evidence: changed })}\n`, "utf8");
    await assert.rejects(
      readPublicationCurrentReconciliationEvidence(tampered, config, target),
      /evidence hash mismatch/,
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("apply commits only an outcome matching the approved rollback preview", async () => {
  const { config, target } = fixture();
  const previewPool = fakePool(config, target);
  const previewBusinessPool = fakeBusinessPool(config, target);
  const preview = new PublicationCurrentReconciliationAdministrator({
    crawlerPool: previewPool,
    businessPool: previewBusinessPool,
    config,
    target,
    reconcile: async (_client, input) => result(input.channelId),
    lock: async () => {},
  });
  const evidence = await preview.inspectRollbackPreview({
    generatedAt: "2026-07-29T02:01:00.000Z",
  });

  const applyPool = fakePool(config, target);
  const applyBusinessPool = fakeBusinessPool(config, target);
  const apply = new PublicationCurrentReconciliationAdministrator({
    crawlerPool: applyPool,
    businessPool: applyBusinessPool,
    config,
    target,
    evidence,
    reconcile: async (_client, input) => result(input.channelId),
    lock: async () => {},
  });
  const applied = await apply.apply();

  assert.equal(applied.succeeded, 2);
  assert.equal(applied.committed, 2);
  assert.equal(applied.already_applied, 0);
  assert.equal(applyPool.calls.filter((call) => call.sql === "COMMIT").length, 3);
  assert.equal(applyPool.calls.filter((call) => call.sql === "ROLLBACK").length, 0);
});

test("apply stops before Source writes when a Business baseline changed", async () => {
  const { config, target } = fixture();
  const preview = new PublicationCurrentReconciliationAdministrator({
    crawlerPool: fakePool(config, target),
    businessPool: fakeBusinessPool(config, target),
    config,
    target,
    reconcile: async (_client, input) => result(input.channelId),
    lock: async () => {},
  });
  const evidence = await preview.inspectRollbackPreview({
    generatedAt: "2026-07-29T02:01:00.000Z",
  });
  const applyPool = fakePool(config, target);
  const apply = new PublicationCurrentReconciliationAdministrator({
    crawlerPool: applyPool,
    businessPool: fakeBusinessPool(config, target, { title: "Changed after plan" }),
    config,
    target,
    evidence,
    reconcile: async (_client, input) => result(input.channelId),
    lock: async () => {},
  });

  await assert.rejects(apply.apply(), /Business preservation baseline changed/);
  assert.equal(
    applyPool.calls.some((call) => call.sql.includes("set_config('publication.writer_version'")),
    false,
  );
});

test("apply is resumable and rolls back a Channel whose approved result changed", async () => {
  const { config, target } = fixture();
  const previewPool = fakePool(config, target);
  const previewBusinessPool = fakeBusinessPool(config, target);
  const preview = new PublicationCurrentReconciliationAdministrator({
    crawlerPool: previewPool,
    businessPool: previewBusinessPool,
    config,
    target,
    reconcile: async (_client, input) => result(input.channelId),
    lock: async () => {},
  });
  const evidence = await preview.inspectRollbackPreview({
    generatedAt: "2026-07-29T02:01:00.000Z",
  });

  const applyPool = fakePool(config, target);
  const applyBusinessPool = fakeBusinessPool(config, target);
  const apply = new PublicationCurrentReconciliationAdministrator({
    crawlerPool: applyPool,
    businessPool: applyBusinessPool,
    config,
    target,
    evidence,
    reconcile: async (_client, input) => input.channelId === CHANNEL_IDS[0]
      ? result(input.channelId, { status: "no_change" })
      : result(input.channelId, { resultHash: `sha256:${"b".repeat(64)}` }),
    lock: async () => {},
  });

  await assert.rejects(
    apply.apply(),
    (error) => {
      assert.equal(error instanceof PublicationCurrentReconciliationPartialFailure, true);
      assert.equal(error.details.succeeded, 1);
      assert.equal(error.details.already_applied, 1);
      assert.equal(error.details.failed, 1);
      return true;
    },
  );
  assert.equal(applyPool.calls.filter((call) => call.sql === "ROLLBACK").length, 1);
});

test("apply recognizes an already stored Preservation Current as resumable", async () => {
  const { config, target } = fixture();
  const preview = new PublicationCurrentReconciliationAdministrator({
    crawlerPool: fakePool(config, target),
    businessPool: fakeBusinessPool(config, target),
    config,
    target,
    reconcile: async (_client, input) => result(input.channelId, {
      status: "preservation_seeded",
      dataSequence: 0,
      carriedForwardFields: [],
    }),
    lock: async () => {},
  });
  const evidence = await preview.inspectRollbackPreview({
    generatedAt: "2026-07-29T02:01:00.000Z",
  });
  const apply = new PublicationCurrentReconciliationAdministrator({
    crawlerPool: fakePool(config, target),
    businessPool: fakeBusinessPool(config, target),
    config,
    target,
    evidence,
    reconcile: async (_client, input) => result(input.channelId, {
      status: "preservation_retained",
      dataSequence: 0,
      carriedForwardFields: [],
    }),
    lock: async () => {},
  });

  const applied = await apply.apply();
  assert.equal(applied.succeeded, CHANNEL_IDS.length);
  assert.equal(applied.already_applied, CHANNEL_IDS.length);
  assert.equal(applied.committed, 0);
});
