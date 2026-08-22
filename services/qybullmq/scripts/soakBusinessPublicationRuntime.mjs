import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import pg from "pg";
import { PostgresBusinessPublicationAuditor } from "../src/businessPublicationAuditor.js";
import { PostgresBusinessPublicationStore } from "../src/businessPublicationIngress.js";
import { PostgresBusinessPublicationProjector } from "../src/businessPublicationProjector.js";
import { PostgresBusinessPublicationReconciler } from "../src/businessPublicationReconciler.js";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import {
  PostgresPublicationOutboxStore,
  PublicationPublisher,
} from "../src/publicationPublisher.js";
import { publicationResultHash } from "../src/publicationResultHash.js";
import { publicationEnvelopeFromRow } from "../src/publicationTransport.js";
import {
  ServiceThroughputMetric,
  businessPublicationLoadOptions,
  parseShmMountBytes,
  percentile,
  requiredEnvironment,
} from "./businessPublicationLoadSupport.mjs";

const { Pool } = pg;
const FACT_KEYS = [
  "country",
  "creator_language",
  "creator_gender",
  "creator_age_range",
  "audience_region",
  "audience_language",
  "audience_age_gender",
  "active_subscriber_ratio",
  "channel_categories",
  "channel_tags",
];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function transaction(pool, action) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await action(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function assertTestDatabase(pool, requiredRelations) {
  const identity = await pool.query(
    `SELECT current_database() AS database_name,
            $1::text[] <@ ARRAY(
              SELECT relation_name
              FROM unnest($1::text[]) relation_name
              WHERE to_regclass(relation_name) IS NOT NULL
            ) AS schema_ready`,
    [requiredRelations],
  );
  const row = identity.rows[0];
  if (!/_test$/i.test(row.database_name) || row.schema_ready !== true) {
    throw new Error(`refusing non-test or incomplete database: ${row.database_name}`);
  }
  return row.database_name;
}

async function assertBusinessPostgresRuntime(
  businessPool,
  auditPool,
  { expectedPostgresVersion, expectedShmMib },
) {
  const [coreResult, auditResult] = await Promise.all([
    businessPool.query(
      `SELECT current_setting('server_version') AS server_version,
              current_setting('dynamic_shared_memory_type') AS dsm_type,
              current_setting('max_parallel_workers_per_gather')::int
                AS parallel_workers_per_gather,
              current_setting('debug_parallel_query') AS debug_parallel_query,
              pg_read_file('/proc/mounts') AS mounts`,
    ),
    auditPool.query(
      `SELECT current_setting('max_parallel_workers_per_gather')::int
                AS parallel_workers_per_gather,
              current_setting('debug_parallel_query') AS debug_parallel_query`,
    ),
  ]);
  const core = coreResult.rows[0];
  const audit = auditResult.rows[0];
  const shmBytes = parseShmMountBytes(core.mounts);
  const expectedShmBytes = expectedShmMib * 1024 * 1024;
  if (
    core.server_version !== expectedPostgresVersion
    || core.dsm_type !== "posix"
    || core.parallel_workers_per_gather !== 2
    || core.debug_parallel_query !== "off"
    || audit.parallel_workers_per_gather !== 0
    || audit.debug_parallel_query !== "off"
    || shmBytes !== expectedShmBytes
  ) {
    throw new Error("Business PostgreSQL runtime does not match the INC-009 load baseline");
  }
  return {
    server_version: core.server_version,
    dynamic_shared_memory_type: core.dsm_type,
    core_parallel_workers_per_gather: core.parallel_workers_per_gather,
    audit_parallel_workers_per_gather: audit.parallel_workers_per_gather,
    audit_debug_parallel_query: audit.debug_parallel_query,
    shm_bytes: shmBytes,
  };
}

async function seedBusinessProjectionFixture(pool) {
  const expected = [
    ["video", "videos", 1],
    ["short", "shorts", 1],
    ["live", "lives", 1],
  ];
  const emptySearchWatermark = "inc009-soak-empty-bootstrap";
  await transaction(pool, async (client) => {
    await client.query(
      `INSERT INTO public.content_type_taxonomy (
         source_content_type,content_kind,canonical_priority
       )
       SELECT * FROM unnest($1::text[],$2::text[],$3::smallint[])
       ON CONFLICT (source_content_type) DO NOTHING`,
      [
        expected.map(([sourceContentType]) => sourceContentType),
        expected.map(([, contentKind]) => contentKind),
        expected.map(([, , canonicalPriority]) => canonicalPriority),
      ],
    );

    // Schema-only snapshots omit the runtime rows required by the production Projector path.
    await client.query(
      `INSERT INTO public.import_batches (
         id,source_file,source_sha256,captured_at,raw_payload,source_kind,status
       ) VALUES ($1,$1,$2,'2000-01-01T00:00:00Z','{"inc009_soak":true}'::jsonb,
                 'publication_projection','published')
       ON CONFLICT (id) DO NOTHING`,
      [emptySearchWatermark, "0".repeat(64)],
    );
    await client.query(
      `INSERT INTO public.creator_search_releases (watermark,status,activated_at)
       SELECT $1,'active',clock_timestamp()
       WHERE NOT EXISTS (SELECT 1 FROM public.creator_search_active WHERE singleton=true)
         AND NOT EXISTS (SELECT 1 FROM public.creator_search_releases WHERE status='active')
       ON CONFLICT (watermark) DO NOTHING`,
      [emptySearchWatermark],
    );
    await client.query(
      `INSERT INTO public.creator_search_active (singleton,watermark)
       SELECT true,watermark
       FROM public.creator_search_releases
       WHERE status='active'
       ORDER BY activated_at DESC,watermark
       LIMIT 1
       ON CONFLICT (singleton) DO NOTHING`,
    );
    await client.query(
      `INSERT INTO publication.creator_search_storage_state (
         singleton,write_mode,read_mode,initialized_watermark,initialized_row_count
       )
       SELECT true,'incremental','live',active.watermark,
              (SELECT count(*)::int FROM public.creator_search_current current_row
               WHERE current_row.watermark=active.watermark)
       FROM public.creator_search_active active
       WHERE active.singleton=true
       ON CONFLICT (singleton) DO NOTHING`,
    );

    const [actual, storage] = await Promise.all([
      client.query(
        `SELECT source_content_type,content_kind,canonical_priority::int
         FROM public.content_type_taxonomy
         WHERE source_content_type=ANY($1::text[])
         ORDER BY source_content_type`,
        [expected.map(([sourceContentType]) => sourceContentType)],
      ),
      client.query(
        `SELECT state.write_mode,state.read_mode,
                (SELECT count(*)::int FROM public.creator_search_current current_row
                 WHERE current_row.watermark=state.initialized_watermark)
                  =state.initialized_row_count AS initialized_fallback_ready
         FROM publication.creator_search_storage_state state
         WHERE state.singleton=true`,
      ),
    ]);
    const normalized = actual.rows.map((row) => [
      row.source_content_type,
      row.content_kind,
      Number(row.canonical_priority),
    ]).sort(([left], [right]) => left.localeCompare(right));
    const expectedSorted = expected.slice().sort(([left], [right]) => left.localeCompare(right));
    if (JSON.stringify(normalized) !== JSON.stringify(expectedSorted)) {
      throw new Error("test database content type taxonomy does not match the publication contract");
    }
    const storageState = storage.rows[0];
    if (
      storageState?.write_mode !== "incremental"
      || storageState?.read_mode !== "live"
      || storageState?.initialized_fallback_ready !== true
    ) {
      throw new Error("test database Creator Search storage does not match production mode");
    }
  });
}

function channelPayload(channelId, title) {
  return {
    channel_id: channelId,
    title,
    canonical_url: `https://www.youtube.com/channel/${channelId}`,
    vanity_channel_url: null,
    handle: null,
    avatar: [],
    rss_url: `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`,
    keywords: ["inc009", "soak"],
    is_family_safe: true,
    is_verified: false,
    is_verified_status: "observed_false",
    has_videos: true,
    has_shorts: false,
    has_live_streams: false,
    description: "INC-009 joint load fixture",
    subscriber_count: 1000,
    subscriber_count_status: "exact",
    total_video_count: 1,
    total_video_count_status: "exact",
    total_view_count: 100,
    total_view_count_status: "exact",
    joined_date: "2020-01-01",
    joined_date_status: "exact",
    joined_date_raw: "Joined Jan 1, 2020",
    country_code: "US",
    country_name: "United States",
    links: [],
    lifecycle_status: "active",
  };
}

function videoItem(channelId, contentId, observedAt) {
  const item = {
    content_id: contentId,
    content_key: `${channelId}:${contentId}`,
    kind: "video",
    title: `INC-009 video ${contentId}`,
    url: `https://www.youtube.com/watch?v=${contentId}`,
    thumbnail_url: null,
    published_at: observedAt,
    published_date: observedAt.slice(0, 10),
    published_at_precision: "second",
    published_at_status: "exact",
    published_at_source: "inc009-soak",
    duration_seconds: 60,
    duration_status: "exact",
    duration_source: "inc009-soak",
    view_count: 100,
    view_count_status: "exact",
    view_count_source: "inc009-soak",
    view_count_observed_at: observedAt,
    like_count: 10,
    like_count_status: "exact",
    like_count_source: "inc009-soak",
    like_count_observed_at: observedAt,
    comment_count: 1,
    comment_count_status: "exact",
    comment_count_source: "inc009-soak",
    comment_count_observed_at: observedAt,
    comments_disabled: false,
    description: "INC-009 load video",
    description_status: "exact",
    description_source: "inc009-soak",
    hashtags: [],
    keywords: [],
    access_status: "public",
    access_status_source: "inc009-soak",
    is_members_only: false,
    live_scheduled_at: null,
    live_started_at: null,
    live_ended_at: null,
    extractor_version: "inc009-soak",
  };
  const hashValue = Object.fromEntries(Object.entries(item).filter(([key]) => (
    !key.endsWith("_observed_at")
      && !key.endsWith("_source")
      && key !== "extractor_version"
  )));
  return { position: 1, item_hash: observationFactsHash(hashValue), ...item };
}

function videoPayload(channelId, contentId, observedAt) {
  const item = videoItem(channelId, contentId, observedAt);
  const windowPolicy = {
    policy_version: "video-window-v1",
    as_of: observedAt,
    cutoff_at: new Date(Date.parse(observedAt) - 90 * 86400000).toISOString(),
    cutoff_date: new Date(Date.parse(observedAt) - 90 * 86400000).toISOString().slice(0, 10),
    max_age_days: 90,
    max_items: 30,
  };
  const resultHash = publicationResultHash("video", {
    channel_id: channelId,
    window_policy: windowPolicy,
    items: [item],
  });
  return {
    payload: {
      channel_id: channelId,
      window_policy: windowPolicy,
      window_proof: {
        complete: true,
        terminal_condition: "list_end_confirmed",
        catalog_candidate_count: 1,
        qualified_count: 1,
        selected_count: 1,
        excluded_count: 0,
        latest_scan_items: 1,
        latest_scan_pages: 1,
        latest_scan_stop_reason: "list_end",
        latest_scan_detail_failure_count: 0,
      },
      items: [item],
      result_hash: resultHash,
    },
    resultHash,
  };
}

function agentFactValue(field) {
  return {
    country: "United States",
    creator_language: "English",
    creator_gender: "brand_team",
    creator_age_range: 35,
    audience_region: [
      { region: "United States", percentage: 80 },
      { region: "Canada", percentage: 10 },
      { region: "United Kingdom", percentage: 5 },
      { region: "Australia", percentage: 3 },
      { region: "Other", percentage: 2 },
    ],
    audience_language: [
      { language: "English", percentage: 95 },
      { language: "Other", percentage: 5 },
    ],
    audience_age_gender: [
      { age_range: "18-24", male: 10, female: 10 },
      { age_range: "25-34", male: 15, female: 15 },
      { age_range: "35-44", male: 10, female: 10 },
      { age_range: "45-54", male: 5, female: 5 },
      { age_range: "55-64", male: 3, female: 3 },
      { age_range: "65+", male: 2, female: 2 },
    ],
    active_subscriber_ratio: 0,
    channel_categories: {
      level_1: "Software & Internet",
      level_2: ["Artificial Intelligence"],
    },
    channel_tags: {
      tags: [
        "Artificial Intelligence",
        "Software",
        "Programming",
        "Technology",
        "Tutorials",
        "Machine Learning",
        "Developer Tools",
        "Product Reviews",
        "Industry News",
        "Digital Culture",
      ],
      top_5_distribution: [
        { tag: "Artificial Intelligence", percentage: 25 },
        { tag: "Software", percentage: 22 },
        { tag: "Programming", percentage: 18 },
        { tag: "Technology", percentage: 14 },
        { tag: "Tutorials", percentage: 11 },
        { tag: "Other", percentage: 10 },
      ],
    },
  }[field];
}

function agentPayload(channelId, contentId) {
  const inputIds = [contentId];
  return {
    channel_id: channelId,
    agent_mode: "basic",
    input_url: `https://www.youtube.com/channel/${channelId}`,
    facts: Object.fromEntries(FACT_KEYS.map((key) => [key, {
      value: agentFactValue(key),
      confidence: "high",
      evidence: [`INC-009 joint load evidence for ${key}`],
      source_urls: [`https://www.youtube.com/watch?v=${contentId}`],
      reason: null,
      source: "inc009-soak",
    }])),
    agent_model: "inc009-model",
    agent_config_id: 1,
    prompt_template_id: 1,
    prompt_hash: "a".repeat(64),
    prompt_variant: "with_country",
    agent_version_hash: `sha256:${"b".repeat(64)}`,
    output_hash: `sha256:${"c".repeat(64)}`,
    input_content_ids: inputIds,
    input_content_hash: observationFactsHash(inputIds),
    taxonomy_version: "qy-taxonomy-v1",
  };
}

function envelope({ streamId, channelId, domain, payload, resultHash, observedAt }) {
  return publicationEnvelopeFromRow({
    revision_id: randomUUID(),
    publication_stream_id: streamId,
    revision_type: "bootstrap",
    channel_id: channelId,
    domain,
    data_sequence: 1,
    previous_data_sequence: null,
    operation: domain === "video" ? "replace_window" : "replace",
    contract_version: 1,
    policy_version: domain === "video" ? "video-window-v1" : "publication-policy-v1",
    occurred_at: observedAt,
    source_refs: {
      inc009_soak: true,
      complete_observation: { observed_at: observedAt },
    },
    previous_result_hash: null,
    result_hash: resultHash,
    payload_hash: observationFactsHash(payload),
    payload_json: payload,
  });
}

function channelEnvelopes(streamId, channelId, index) {
  const observedAt = new Date().toISOString();
  const channel = channelPayload(channelId, `INC-009 channel ${index}`);
  const contentId = `inc009-video-${channelId}`;
  const video = videoPayload(channelId, contentId, observedAt);
  const agent = agentPayload(channelId, contentId);
  return [
    envelope({
      streamId,
      channelId,
      domain: "channel",
      payload: channel,
      resultHash: observationFactsHash(channel),
      observedAt,
    }),
    envelope({
      streamId,
      channelId,
      domain: "video",
      payload: video.payload,
      resultHash: video.resultHash,
      observedAt,
    }),
    envelope({
      streamId,
      channelId,
      domain: "agent",
      payload: agent,
      resultHash: observationFactsHash(agent),
      observedAt,
    }),
  ];
}

async function registerStream(crawlerPool, businessPool, streamId, runId) {
  await Promise.all([
    crawlerPool.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         minimum_writer_version,capture_enabled_at,created_by,created_reason,
         status_changed_by,status_reason
       ) VALUES ($1,$2,'{"database":"inc009-crawler-test"}'::jsonb,
                 'publication-reconciler-v1',now(),
                 'inc009-soak','joint load','inc009-soak','active test stream')`,
      [streamId, `inc009-soak-${runId}`],
    ),
    businessPool.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         registered_by,registered_reason,status_changed_by,status_reason
       ) VALUES ($1,$2,'{"database":"inc009-business-test"}'::jsonb,
                 'inc009-soak','joint load','inc009-soak','active test stream')`,
      [streamId, `inc009-soak-${runId}`],
    ),
  ]);
}

async function enqueueChannel(crawlerPool, businessPool, streamId, channelId, index) {
  const envelopes = channelEnvelopes(streamId, channelId, index);
  await businessPool.query(
    `INSERT INTO publication.channel_ownership (
       channel_id,active_publication_stream_id,projection_mode,state_changed_by,state_reason
     ) VALUES ($1,$2,'online','inc009-soak','joint load ownership')`,
    [channelId, streamId],
  );
  await transaction(crawlerPool, async (client) => {
    await client.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,$3,'active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`, `INC-009 channel ${index}`],
    );
    await client.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
       ) VALUES ($1,$2,'bootstrap','inc009-soak','joint load owner')`,
      [streamId, channelId],
    );
    for (const item of envelopes) {
      await client.query(
        `INSERT INTO publication.revision (
           revision_id,publication_stream_id,channel_id,domain,data_sequence,
           previous_data_sequence,revision_type,operation,contract_version,policy_version,
           occurred_at,source_refs,previous_result_hash,result_hash,payload_hash,payload_json
         ) VALUES (
           $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13,$14,$15,$16::jsonb
         )`,
        [
          item.revision_id,
          item.publication_stream_id,
          item.channel_id,
          item.domain,
          item.data_sequence,
          item.previous_data_sequence,
          item.revision_type,
          item.operation,
          item.contract_version,
          item.policy_version,
          item.occurred_at,
          JSON.stringify(item.source),
          item.previous_result_hash,
          item.result_hash,
          item.payload_hash,
          JSON.stringify(item.payload),
        ],
      );
      await client.query(
        `INSERT INTO publication.outbox (destination,revision_id,status,next_attempt_at)
         VALUES ('business',$1,'pending',now())`,
        [item.revision_id],
      );
    }
  });
}

async function pendingCounts(crawlerPool, businessPool, streamId) {
  const [crawler, business] = await Promise.all([
    crawlerPool.query(
      `SELECT count(*)::int AS count
       FROM publication.outbox outbox
       JOIN publication.revision revision USING(revision_id)
       WHERE revision.publication_stream_id=$1 AND outbox.status<>'delivered'`,
      [streamId],
    ),
    businessPool.query(
      `SELECT
         (SELECT count(*)::int FROM publication.revision
          WHERE publication_stream_id=$1 AND activation_status<>'active') AS revisions,
         (SELECT count(*)::int FROM publication.projection_outbox
          WHERE publication_stream_id=$1 AND status<>'delivered') AS projections`,
      [streamId],
    ),
  ]);
  return {
    crawler: Number(crawler.rows[0].count),
    revisions: Number(business.rows[0].revisions),
    projections: Number(business.rows[0].projections),
  };
}

function hasPending(counts) {
  return counts.crawler > 0 || counts.revisions > 0 || counts.projections > 0;
}

async function finalState(crawlerPool, businessPool, streamId) {
  const [crawler, business] = await Promise.all([
    crawlerPool.query(
      `SELECT count(*)::int AS revisions,
              count(*) FILTER (WHERE outbox.status='delivered')::int AS delivered,
              count(*) FILTER (WHERE outbox.status='dead_letter')::int AS dead_letter
       FROM publication.revision revision
       JOIN publication.outbox outbox USING(revision_id)
       WHERE revision.publication_stream_id=$1`,
      [streamId],
    ),
    businessPool.query(
      `SELECT
         (SELECT count(*)::int FROM publication.inbox WHERE publication_stream_id=$1) AS inbox,
         (SELECT count(*)::int FROM publication.revision WHERE publication_stream_id=$1) AS revisions,
         (SELECT count(*)::int FROM publication.consumer_cursor WHERE publication_stream_id=$1)
           AS cursors,
         (SELECT count(*)::int FROM publication.activation WHERE publication_stream_id=$1)
           AS activations,
         (SELECT count(*)::int FROM publication.projection_outbox
          WHERE publication_stream_id=$1 AND status='delivered') AS projections,
         (SELECT count(*)::int FROM publication.projection_outbox
          WHERE publication_stream_id=$1 AND status='dead_letter') AS projection_dead_letters,
         (SELECT count(*)::int FROM public.creator_search_live search
          JOIN publication.channel_ownership ownership USING(channel_id)
          WHERE ownership.active_publication_stream_id=$1) AS search_rows`,
      [streamId],
    ),
  ]);
  return {
    crawler: Object.fromEntries(Object.entries(crawler.rows[0]).map(([key, value]) => (
      [key, Number(value)]
    ))),
    business: Object.fromEntries(Object.entries(business.rows[0]).map(([key, value]) => (
      [key, Number(value)]
    ))),
  };
}

async function main() {
  const crawlerDatabaseUrl = requiredEnvironment(
    process.env,
    "INC009_CRAWLER_TEST_DATABASE_URL",
  );
  const businessDatabaseUrl = requiredEnvironment(
    process.env,
    "INC009_BUSINESS_TEST_DATABASE_URL",
  );
  const options = businessPublicationLoadOptions();
  const {
    auditIntervalSeconds,
    capacityChannels,
    channelsPerMinute,
    drainTimeoutSeconds,
    durationSeconds,
    minimumAudits,
    minimumRates,
    mode,
  } = options;
  const runId = String(process.env.INC009_SOAK_RUN_ID || randomUUID().replaceAll("-", ""));
  const streamId = randomUUID();
  const crawlerPool = new Pool({
    connectionString: crawlerDatabaseUrl,
    max: 4,
    application_name: "inc009-soak-crawler",
  });
  const businessPool = new Pool({
    connectionString: businessDatabaseUrl,
    max: 12,
    application_name: "inc009-soak-business",
  });
  const auditPool = new Pool({
    connectionString: businessDatabaseUrl,
    max: 1,
    application_name: "inc009-soak-auditor",
    options: "-c max_parallel_workers_per_gather=0 -c debug_parallel_query=off",
  });
  let processing = true;
  let fatalError = null;
  let fixtureGenerationMs = 0;
  const serviceMetrics = {
    publisher: new ServiceThroughputMetric(),
    ingress: new ServiceThroughputMetric(),
    reconciler: new ServiceThroughputMetric(),
    projector: new ServiceThroughputMetric(),
  };
  const metrics = {
    generated_channels: 0,
    publisher_delivered: 0,
    publisher_dead_lettered: 0,
    ingress_received: 0,
    ingress_rejected: 0,
    reconciler_processed: 0,
    projector_delivered: 0,
    audit_runs: 0,
    audit_failures: 0,
    shared_memory_errors: 0,
    publisher_duration_ms: [],
    ingress_duration_ms: [],
    reconciler_duration_ms: [],
    projector_duration_ms: [],
    audit_duration_ms: [],
  };

  try {
    const [crawlerDatabase, businessDatabase] = await Promise.all([
      assertTestDatabase(crawlerPool, [
        "crawler.channels",
        "publication.revision",
        "publication.outbox",
      ]),
      assertTestDatabase(businessPool, [
        "publication.inbox",
        "publication.reconciliation_state",
        "publication.projection_outbox",
        "public.content_type_taxonomy",
        "public.creator_search_live",
      ]),
    ]);
    const postgresRuntime = await assertBusinessPostgresRuntime(
      businessPool,
      auditPool,
      options,
    );
    await seedBusinessProjectionFixture(businessPool);
    await registerStream(crawlerPool, businessPool, streamId, runId);
    const ingressStore = new PostgresBusinessPublicationStore(businessPool);
    const ingress = {
      async acceptShard(shard) {
        const startedAt = performance.now();
        let received = 0;
        try {
          const result = await ingressStore.acceptShard(shard);
          received = result.receipts.length;
          metrics.ingress_received += received;
          metrics.ingress_rejected += result.receipts.filter((receipt) => (
            receipt.status === "rejected" || receipt.status === "conflict"
          )).length;
          return result;
        } finally {
          const durationMs = performance.now() - startedAt;
          metrics.ingress_duration_ms.push(Math.round(durationMs));
          serviceMetrics.ingress.observe({
            durationMs,
            units: received,
            productive: shard.items.length > 0,
          });
        }
      },
    };
    const publisher = new PublicationPublisher({
      store: new PostgresPublicationOutboxStore({
        query: crawlerPool.query.bind(crawlerPool),
        withTransaction: (action) => transaction(crawlerPool, action),
      }),
      ingress,
      destination: "business",
      leaseOwner: `inc009-publisher-${runId}`,
      batchSize: 100,
      leaseSeconds: 120,
      maxAttempts: 20,
      retryDelay: () => 100,
      logger: { info() {}, error() {} },
    });
    const auditor = new PostgresBusinessPublicationAuditor(auditPool, {
      auditIntervalSeconds,
      errorRetrySeconds: 30,
      maximumErrorRetrySeconds: 300,
    });
    const reconciler = new PostgresBusinessPublicationReconciler(businessPool, {
      auditor,
      workerId: `inc009-reconciler-${runId}`,
      batchSize: 100,
      concurrency: 4,
    });
    const projector = new PostgresBusinessPublicationProjector(businessPool, {
      workerId: `inc009-projector-${runId}`,
      batchSize: 25,
    });

    const generateChannel = async () => {
      metrics.generated_channels += 1;
      const channelId = `UCinc009${runId.slice(0, 10)}${String(
        metrics.generated_channels,
      ).padStart(8, "0")}`;
      await enqueueChannel(
        crawlerPool,
        businessPool,
        streamId,
        channelId,
        metrics.generated_channels,
      );
    };
    if (mode === "capacity") {
      const generationStartedAt = performance.now();
      for (let index = 0; index < capacityChannels; index += 1) {
        await generateChannel();
      }
      fixtureGenerationMs = Math.round(performance.now() - generationStartedAt);
    }

    const guard = async (name, action) => {
      try {
        await action();
      } catch (error) {
        fatalError = new Error(`${name} failed: ${error?.stack || error}`);
        processing = false;
      }
    };
    const publisherLoop = guard("Publisher", async () => {
      while (processing) {
        const startedAt = performance.now();
        const summary = await publisher.runOnce();
        const durationMs = performance.now() - startedAt;
        metrics.publisher_duration_ms.push(Math.round(durationMs));
        serviceMetrics.publisher.observe({
          durationMs,
          units: summary.delivered,
          productive: summary.claimed > 0,
        });
        metrics.publisher_delivered += summary.delivered;
        metrics.publisher_dead_lettered += summary.dead_lettered;
        if (summary.claimed === 0) await sleep(100);
      }
    });
    const reconcilerLoop = guard("Reconciler", async () => {
      while (processing) {
        const summary = await reconciler.runOnce();
        metrics.reconciler_duration_ms.push(summary.duration_ms);
        serviceMetrics.reconciler.observe({
          durationMs: summary.duration_ms,
          units: summary.processed,
          productive: summary.claimed > 0,
        });
        metrics.reconciler_processed += summary.processed;
        if (summary.audit.performed) {
          metrics.audit_runs += 1;
          if (Number.isFinite(summary.audit.duration_ms)) {
            metrics.audit_duration_ms.push(summary.audit.duration_ms);
          }
          if (summary.audit.status === "failed" || summary.audit.error) {
            metrics.audit_failures += 1;
          }
          if (
            summary.audit.failure_kind === "dynamic_shared_memory_exhausted"
            || /shared memory segment.*No space left on device/i.test(summary.audit.error ?? "")
          ) {
            metrics.shared_memory_errors += 1;
          }
        }
        if (summary.claimed === 0) await sleep(100);
      }
    });
    const projectorLoop = guard("Projector", async () => {
      while (processing) {
        const startedAt = performance.now();
        const summary = await projector.runOnce();
        const durationMs = performance.now() - startedAt;
        metrics.projector_duration_ms.push(Math.round(durationMs));
        serviceMetrics.projector.observe({
          durationMs,
          units: summary.delivered,
          productive: summary.claimed > 0,
        });
        metrics.projector_delivered += summary.delivered;
        if (summary.claimed === 0) await sleep(100);
      }
    });

    const startedAtMs = Date.now();
    if (mode === "steady") {
      const deadlineMs = startedAtMs + durationSeconds * 1000;
      const generateEveryMs = Math.ceil(60000 / channelsPerMinute);
      let nextGeneratedAtMs = startedAtMs;
      let nextProgressAtMs = startedAtMs + 60000;
      while (Date.now() < deadlineMs && processing) {
        const nowMs = Date.now();
        if (nowMs >= nextGeneratedAtMs) {
          await generateChannel();
          nextGeneratedAtMs += generateEveryMs;
        }
        if (nowMs >= nextProgressAtMs) {
          console.log(JSON.stringify({
            event: "inc009_soak_progress",
            elapsed_seconds: Math.floor((nowMs - startedAtMs) / 1000),
            generated_channels: metrics.generated_channels,
            audit_runs: metrics.audit_runs,
            audit_failures: metrics.audit_failures,
            shared_memory_errors: metrics.shared_memory_errors,
          }));
          nextProgressAtMs += 60000;
        }
        await sleep(Math.min(100, Math.max(1, nextGeneratedAtMs - Date.now())));
      }
    }
    if (fatalError) throw fatalError;

    const drainDeadlineMs = Date.now() + drainTimeoutSeconds * 1000;
    let pending = await pendingCounts(crawlerPool, businessPool, streamId);
    while (hasPending(pending) && Date.now() < drainDeadlineMs && processing) {
      await sleep(250);
      pending = await pendingCounts(crawlerPool, businessPool, streamId);
    }
    if (fatalError) throw fatalError;
    if (hasPending(pending)) throw new Error(`pipeline did not drain: ${JSON.stringify(pending)}`);
    processing = false;
    await Promise.all([publisherLoop, reconcilerLoop, projectorLoop]);

    const finalAudit = await new PostgresBusinessPublicationAuditor(auditPool, {
      auditIntervalSeconds: 0,
    }).runIfDue();
    const state = await finalState(crawlerPool, businessPool, streamId);
    const expectedRevisions = metrics.generated_channels * 3;
    const expectedChannels = metrics.generated_channels;
    const elapsedMs = Date.now() - startedAtMs;
    const elapsedSeconds = elapsedMs / 1000;
    const servicePerformance = Object.fromEntries(Object.entries(serviceMetrics).map(
      ([name, metric]) => [name, metric.report(elapsedMs)],
    ));
    const measuredRates = {
      publisher_revisions: servicePerformance.publisher.productive_capacity_per_second,
      ingress_revisions: servicePerformance.ingress.productive_capacity_per_second,
      reconciler_channels: servicePerformance.reconciler.productive_capacity_per_second,
      projector_channels: servicePerformance.projector.productive_capacity_per_second,
    };
    const performanceChecks = Object.fromEntries(Object.entries(minimumRates).map(
      ([name, minimum]) => [name, {
        minimum_per_second: minimum,
        measured_per_second: measuredRates[name],
        passed: minimum === 0 || measuredRates[name] >= minimum,
      }],
    ));
    const correctness = {
      crawler_revisions: state.crawler.revisions === expectedRevisions,
      crawler_delivered: state.crawler.delivered === expectedRevisions,
      crawler_no_dead_letters: state.crawler.dead_letter === 0,
      ingress_receipts: metrics.ingress_received === expectedRevisions,
      ingress_no_rejections: metrics.ingress_rejected === 0,
      business_inbox: state.business.inbox === expectedRevisions,
      business_revisions: state.business.revisions === expectedRevisions,
      business_cursors: state.business.cursors === expectedRevisions,
      business_activations_complete: state.business.activations >= expectedChannels,
      business_projections_complete: (
        state.business.projections === state.business.activations
      ),
      business_no_projection_dead_letters: state.business.projection_dead_letters === 0,
      search_rows: state.business.search_rows === expectedChannels,
      final_audit_clean: finalAudit.status === "succeeded" && finalAudit.issue_count === 0,
      minimum_audits: metrics.audit_runs >= minimumAudits,
      no_audit_failures: metrics.audit_failures === 0,
      no_shared_memory_errors: metrics.shared_memory_errors === 0,
      performance_thresholds: Object.values(performanceChecks).every((check) => check.passed),
    };
    const report = {
      event: "inc009_soak_completed",
      run_id: runId,
      crawler_database: crawlerDatabase,
      business_database: businessDatabase,
      mode,
      configured_duration_seconds: durationSeconds,
      configured_capacity_channels: capacityChannels,
      fixture_generation_ms: fixtureGenerationMs,
      elapsed_seconds: Math.round(elapsedSeconds * 1000) / 1000,
      postgres_runtime: postgresRuntime,
      generated_channels: expectedChannels,
      generated_revisions: expectedRevisions,
      audit_runs: metrics.audit_runs,
      audit_failures: metrics.audit_failures,
      shared_memory_errors: metrics.shared_memory_errors,
      throughput_per_second: {
        publisher_revisions: metrics.publisher_delivered / elapsedSeconds,
        ingress_revisions: metrics.ingress_received / elapsedSeconds,
        reconciler_channels: metrics.reconciler_processed / elapsedSeconds,
        projector_channels: metrics.projector_delivered / elapsedSeconds,
      },
      service_performance: servicePerformance,
      performance_checks: performanceChecks,
      latency_ms: {
        publisher_p50: percentile(metrics.publisher_duration_ms, 0.5),
        publisher_p95: percentile(metrics.publisher_duration_ms, 0.95),
        ingress_p50: percentile(metrics.ingress_duration_ms, 0.5),
        ingress_p95: percentile(metrics.ingress_duration_ms, 0.95),
        reconciler_p50: percentile(metrics.reconciler_duration_ms, 0.5),
        reconciler_p95: percentile(metrics.reconciler_duration_ms, 0.95),
        projector_p50: percentile(metrics.projector_duration_ms, 0.5),
        projector_p95: percentile(metrics.projector_duration_ms, 0.95),
        audit_p50: percentile(metrics.audit_duration_ms, 0.5),
        audit_p95: percentile(metrics.audit_duration_ms, 0.95),
      },
      final_state: state,
      final_audit: {
        status: finalAudit.status,
        issue_count: finalAudit.issue_count,
        duration_ms: finalAudit.duration_ms,
        parallel_workers_per_gather: finalAudit.parallel_workers_per_gather,
        debug_parallel_query: finalAudit.debug_parallel_query,
        attempts_total: finalAudit.attempts_total,
        successes_total: finalAudit.successes_total,
        failures_total: finalAudit.failures_total,
        shared_memory_failures_total: finalAudit.shared_memory_failures_total,
      },
      correctness,
    };
    console.log(JSON.stringify(report));
    if (Object.values(correctness).some((passed) => !passed)) {
      throw new Error(`soak correctness failed: ${JSON.stringify(correctness)}`);
    }
  } finally {
    processing = false;
    await Promise.all([
      crawlerPool.end().catch(() => {}),
      businessPool.end().catch(() => {}),
      auditPool.end().catch(() => {}),
    ]);
  }
}

main().catch((error) => {
  console.error(JSON.stringify({
    event: "inc009_soak_failed",
    error: error?.stack || String(error),
  }));
  process.exitCode = 1;
});
