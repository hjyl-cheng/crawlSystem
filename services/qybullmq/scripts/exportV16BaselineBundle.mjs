import { randomUUID } from "node:crypto";
import {
  mkdir,
  mkdtemp,
  readFile,
  rename,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { basename, dirname, join, parse, resolve } from "node:path";
import pg from "pg";
import { recordAboutObservation } from "../src/aboutObservationStore.js";
import {
  aboutBaseline,
  agentBaseline,
  baselineFileSha256,
  baselineManifest,
  discoveryBaseline,
  recentSamplingBaseline,
  serializeBaselineEvents,
} from "../src/baselineBundle.js";
import { recordCrawlerObservation } from "../src/crawlObservationStore.js";

const { Client } = pg;
const EXPORT_LOCK_ID = 781137219;
const MANDATORY_KINDS = [
  "about",
  "video",
];
const ALL_KINDS = [...MANDATORY_KINDS, "agent"];
const EVENTS_FILE = "events.ndjson";
const MANIFEST_FILE = "manifest.json";

function databaseUrl() {
  return process.env.DATABASE_URL || [
    "postgres://",
    encodeURIComponent(process.env.POSTGRES_USER || "bullmq"),
    ":",
    encodeURIComponent(process.env.POSTGRES_PASSWORD || "bullmq"),
    "@",
    process.env.POSTGRES_HOST || "127.0.0.1",
    ":",
    process.env.POSTGRES_PORT || "5432",
    "/",
    process.env.POSTGRES_DB || "bullmq_crawler",
  ].join("");
}

function requiredEnvironment(name) {
  const value = String(process.env[name] ?? "").trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function positiveInteger(name) {
  const raw = requiredEnvironment(name);
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`${name} must be an explicit positive integer`);
  }
  const value = Number.parseInt(raw, 10);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be an explicit positive integer`);
  }
  return value;
}

function utcTimestamp(name) {
  const value = requiredEnvironment(name);
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/.test(value)) {
    throw new Error(`${name} must be an explicit UTC timestamp ending in Z`);
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new Error(`${name} is not a valid timestamp`);
  const canonicalInput = value.includes(".")
    ? value.replace(/\.(\d{1,3})Z$/, (_match, fraction) => `.${fraction.padEnd(3, "0")}Z`)
    : value.replace(/Z$/, ".000Z");
  const normalized = parsed.toISOString();
  if (normalized !== canonicalInput) throw new Error(`${name} is not a valid UTC calendar time`);
  return normalized;
}

function configuration() {
  if (!process.argv.includes("--apply")) {
    throw new Error("refusing to export: pass --apply explicitly");
  }
  const expectedDatabase = requiredEnvironment("EXPECTED_CRAWLER_DATABASE");
  const expectedChannelCount = positiveInteger("EXPECTED_CRAWLER_CHANNEL_COUNT");
  const baselineVersion = requiredEnvironment("BASELINE_VERSION");
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(baselineVersion)) {
    throw new Error("BASELINE_VERSION must use 1-128 letters, digits, dots, underscores, or dashes");
  }
  const confirmation = requiredEnvironment("CONFIRM_V16_BASELINE_EXPORT");
  const expectedConfirmation = `${expectedDatabase}/${baselineVersion}/${expectedChannelCount}`;
  if (confirmation !== expectedConfirmation) {
    throw new Error(`CONFIRM_V16_BASELINE_EXPORT must equal ${expectedConfirmation}`);
  }
  const outputDirectory = resolve(requiredEnvironment("BASELINE_OUTPUT_DIRECTORY"));
  if (outputDirectory === parse(outputDirectory).root) {
    throw new Error("BASELINE_OUTPUT_DIRECTORY cannot be a filesystem root");
  }
  return {
    baselineVersion,
    asOfAt: utcTimestamp("BASELINE_AS_OF_UTC"),
    outputDirectory,
    expectedDatabase,
    expectedChannelCount,
    crawlerVersion: String(
      process.env.BASELINE_CRAWLER_VERSION || "qy-v16-migration-baseline",
    ).trim(),
    taxonomyVersion: String(
      process.env.AGENT_TAXONOMY_VERSION || "qy-taxonomy-v1",
    ).trim(),
  };
}

function iso(value) {
  return new Date(value).toISOString();
}

function mapRows(rows, key = "channel_id") {
  return new Map(rows.map((row) => [String(row[key]), row]));
}

function addGroupedRow(grouped, row) {
  const key = String(row.channel_id);
  const values = grouped.get(key) ?? [];
  values.push(row);
  grouped.set(key, values);
}

function genericCommand(config, exportId, channelId, observationKind, prepare) {
  return {
    idempotencyKey: `migration-baseline:${config.baselineVersion}:${channelId}:${observationKind}`,
    observationKind,
    channelId,
    runId: null,
    observedAt: config.asOfAt,
    planId: null,
    planDay: null,
    triggerReason: "migration_baseline",
    scheduledAt: config.asOfAt,
    startedAt: config.asOfAt,
    finishedAt: config.asOfAt,
    crawlerVersion: config.crawlerVersion,
    extractorVersions: { migration_baseline: config.baselineVersion },
    command: {
      baseline_version: config.baselineVersion,
      source_snapshot_id: exportId,
    },
    prepare,
  };
}

async function verifyInstalledSchema(client) {
  const result = await client.query(
    `SELECT current_database() AS database_name,
            current_setting('TimeZone')='UTC' AS timezone_utc,
            to_regclass('crawler.channels') IS NOT NULL AS channels_ready,
            to_regclass('crawler.crawl_observations') IS NOT NULL AS observations_ready,
            to_regclass('crawler.channel_about_metric_snapshots') IS NOT NULL AS snapshots_ready,
            to_regclass('crawler.channel_domain_cursors') IS NOT NULL AS cursors_ready,
            to_regclass('crawler.crawler_outbox') IS NOT NULL AS outbox_ready,
            to_regclass('crawler.baseline_exports') IS NOT NULL AS exports_ready,
            to_regclass('crawler.baseline_export_events') IS NOT NULL AS export_events_ready`,
  );
  const row = result.rows[0];
  if (!row || Object.entries(row).some(([name, value]) => name !== "database_name" && value !== true)) {
    throw new Error("Crawler V16 schema is incomplete or the PostgreSQL session is not UTC");
  }
  return String(row.database_name);
}

async function lockSourceTables(client) {
  await client.query(
    `LOCK TABLE
       crawler.channels,crawler.contents,crawler.content_candidates,
       crawler.agent_profiles,crawler.agent_configs,
       crawler.channel_domain_cursors,crawler.crawl_observation_keys,
       crawler.crawl_observations,crawler.channel_about_metric_snapshots,
       crawler.crawler_outbox,crawler.baseline_exports,crawler.baseline_export_events
     IN SHARE ROW EXCLUSIVE MODE`,
  );
}

async function verifySourceSnapshot(client, config) {
  const counts = await client.query(
    `SELECT count(*)::int AS channel_count,clock_timestamp() AS checked_at
     FROM crawler.channels
     WHERE status='active'`,
  );
  const row = counts.rows[0];
  if (Number(row.channel_count) !== config.expectedChannelCount) {
    throw new Error(
      `Channel count mismatch: expected ${config.expectedChannelCount}, got ${row.channel_count}`,
    );
  }
  const cutoff = await client.query(
    `SELECT $1::timestamptz<=clock_timestamp() AS not_future`,
    [config.asOfAt],
  );
  if (!cutoff.rows[0].not_future) {
    throw new Error("BASELINE_AS_OF_UTC cannot be in the future");
  }
  const latest = await client.query(
    `SELECT max(source_at) AS source_at,
            max(source_at)<=$1::timestamptz AS not_after_cutoff
     FROM (
       SELECT max(channel.updated_at) AS source_at
       FROM crawler.channels channel
       WHERE channel.status='active'
       UNION ALL SELECT max(GREATEST(
         content.first_seen_at,content.last_seen_at,
         COALESCE(content.last_enriched_at,'-infinity'::timestamptz),
         COALESCE(content.player_last_observed_at,'-infinity'::timestamptz),
         COALESCE(content.next_last_observed_at,'-infinity'::timestamptz)
       ))
       FROM crawler.contents content
       JOIN crawler.channels channel ON channel.channel_id=content.channel_id
       WHERE channel.status='active'
       UNION ALL SELECT max(candidate.updated_at)
       FROM crawler.content_candidates candidate
       JOIN crawler.channels channel ON channel.channel_id=candidate.channel_id
       WHERE channel.status='active'
       UNION ALL SELECT max(profile.updated_at)
       FROM crawler.agent_profiles profile
       JOIN crawler.channels channel ON channel.channel_id=profile.channel_id
       WHERE channel.status='active'
       UNION ALL SELECT max(observation.observed_at)
       FROM crawler.crawl_observations observation
       JOIN crawler.channels channel ON channel.channel_id=observation.channel_id
       WHERE channel.status='active'
     ) source_times`,
    [config.asOfAt],
  );
  if (latest.rows[0]?.source_at && !latest.rows[0].not_after_cutoff) {
    throw new Error(
      `BASELINE_AS_OF_UTC is older than the latest source row (${iso(latest.rows[0].source_at)})`,
    );
  }
}

async function loadSourceRows(client, config) {
  const [channels, identityCounts, identityEntries, recentCounts, agents] = await Promise.all([
    client.query(
      `SELECT channel_id,title,handle,avatar_url,keywords,available_tabs,summary,
              about_description,country,joined_date_text,joined_at,joined_at_precision,
              external_links,subscriber_count,subscriber_count_text,subscriber_count_status,
              subscriber_count_source,total_view_count,total_view_count_text,
              total_view_count_status,total_view_count_source,total_video_count,
              total_video_count_text,total_video_count_status,total_video_count_source
       FROM crawler.channels
       WHERE status='active'
       ORDER BY channel_id`,
    ),
    client.query(
      `SELECT content.channel_id,
              count(DISTINCT content.source_content_id)::int AS identity_count
       FROM crawler.contents content
       JOIN crawler.channels channel ON channel.channel_id=content.channel_id
       WHERE channel.status='active'
         AND content.content_type IN ('video','short','live')
         AND NULLIF(btrim(content.source_content_id),'') IS NOT NULL
       GROUP BY content.channel_id`,
    ),
    client.query(
      `WITH current_content AS (
         SELECT DISTINCT ON (content.channel_id,content.source_content_id)
                content.channel_id,content.source_content_id,content.content_type,
                CASE WHEN content.published_at_status='exact' THEN content.published_at END
                  AS reliable_published_at,
                CASE WHEN content.published_at_status='exact' THEN content.published_at_precision
                     ELSE 'unknown' END AS reliable_published_at_precision,
                content.last_seen_at
         FROM crawler.contents content
         JOIN crawler.channels channel ON channel.channel_id=content.channel_id
         WHERE channel.status='active'
           AND content.content_type IN ('video','short','live')
         ORDER BY content.channel_id,content.source_content_id,
                  (content.published_at_status='exact') DESC,content.last_seen_at DESC
       ), ranked AS (
         SELECT current_content.channel_id,current_content.source_content_id AS video_id,
                current_content.content_type,
                current_content.reliable_published_at,
                current_content.reliable_published_at_precision,
                row_number() OVER (
                  PARTITION BY current_content.channel_id
                  ORDER BY current_content.reliable_published_at DESC NULLS LAST,
                           current_content.last_seen_at DESC NULLS LAST,
                           current_content.source_content_id
                ) AS baseline_position
         FROM current_content
       )
       SELECT ranked.channel_id,ranked.video_id,ranked.baseline_position AS position,
              ranked.content_type,
              ranked.reliable_published_at AS first_published_at,
              ranked.reliable_published_at_precision AS first_published_at_precision
       FROM ranked
       WHERE ranked.baseline_position<=30
       ORDER BY ranked.channel_id,ranked.baseline_position,ranked.video_id`,
    ),
    client.query(
      `SELECT content.channel_id,
              count(DISTINCT content.source_content_id)::int AS recent_count,
              count(DISTINCT content.source_content_id) FILTER (
                WHERE content.player_last_observed_at IS NULL
                   OR content.player_last_observed_at <= $1::timestamptz - interval '7 days'
              )::int AS stale_count
       FROM crawler.contents content
       JOIN crawler.channels channel ON channel.channel_id=content.channel_id
       WHERE channel.status='active'
         AND content.published_at >= $1::timestamptz - interval '30 days'
         AND content.content_type IN ('video','short','live')
       GROUP BY content.channel_id`,
      [config.asOfAt],
    ),
    client.query(
      `SELECT profile.channel_id,profile.metrics_json,profile.agent_model,
              profile.agent_config_id,profile.prompt_template_id,profile.prompt_hash,
              profile.prompt_variant,config.provider,config.model AS config_model,
              config.tools_json,$1::text AS taxonomy_version
       FROM crawler.agent_profiles profile
       LEFT JOIN crawler.agent_configs config ON config.config_id=profile.agent_config_id
       JOIN crawler.channels channel ON channel.channel_id=profile.channel_id
       WHERE profile.agent_mode='basic' AND profile.status='success'
         AND channel.status='active'
         AND jsonb_typeof(profile.metrics_json)='object'
         AND profile.metrics_json<>'{}'::jsonb`,
      [config.taxonomyVersion],
    ),
  ]);
  const entries = new Map();
  for (const row of identityEntries.rows) addGroupedRow(entries, row);
  return {
    channels: channels.rows,
    identityCounts: mapRows(identityCounts.rows),
    identityEntries: entries,
    recentCounts: mapRows(recentCounts.rows),
    agents: mapRows(agents.rows),
  };
}

async function ensureCursorsAreConsistent(client) {
  await client.query(
    `INSERT INTO crawler.channel_domain_cursors (channel_id,observation_kind)
     SELECT channel.channel_id,kind.observation_kind
     FROM crawler.channels channel
     CROSS JOIN unnest($1::text[]) AS kind(observation_kind)
     WHERE channel.status='active'
     ON CONFLICT (channel_id,observation_kind) DO NOTHING`,
    [ALL_KINDS],
  );
  const cursorRows = await client.query(
    `SELECT cursor.channel_id,cursor.observation_kind,cursor.latest_sequence
     FROM crawler.channel_domain_cursors cursor
     JOIN crawler.channels channel ON channel.channel_id=cursor.channel_id
     WHERE channel.status='active'
     ORDER BY cursor.channel_id,cursor.observation_kind
     FOR UPDATE OF cursor`,
  );
  const mismatch = await client.query(
    `SELECT cursor.channel_id,cursor.observation_kind,cursor.latest_sequence,
            count(observation.observation_id)::bigint AS observation_count,
            count(outbox.event_id)::bigint AS outbox_count,
            COALESCE(min(observation.kind_sequence),0)::bigint AS minimum_sequence,
            COALESCE(max(observation.kind_sequence),0)::bigint AS maximum_sequence
     FROM crawler.channel_domain_cursors cursor
     JOIN crawler.channels channel
       ON channel.channel_id=cursor.channel_id AND channel.status='active'
     LEFT JOIN crawler.crawl_observations observation
       ON observation.channel_id=cursor.channel_id
      AND observation.observation_kind=cursor.observation_kind
     LEFT JOIN crawler.crawler_outbox outbox
       ON outbox.observation_id=observation.observation_id
     GROUP BY cursor.channel_id,cursor.observation_kind,cursor.latest_sequence
     HAVING count(observation.observation_id)<>cursor.latest_sequence
         OR count(outbox.event_id)<>cursor.latest_sequence
         OR (cursor.latest_sequence>0 AND (
              min(observation.kind_sequence)<>1
              OR max(observation.kind_sequence)<>cursor.latest_sequence
            ))
     ORDER BY cursor.channel_id,cursor.observation_kind
     LIMIT 1`,
  );
  if (mismatch.rowCount > 0) {
    const row = mismatch.rows[0];
    throw new Error(
      `Cursor/Observation/Outbox mismatch for ${row.channel_id}/${row.observation_kind}`,
    );
  }
  return new Map(cursorRows.rows.map((row) => [
    `${row.channel_id}\u0000${row.observation_kind}`,
    Number(row.latest_sequence),
  ]));
}

async function createMissingBaselines(client, config, exportId, source) {
  const cursors = await ensureCursorsAreConsistent(client);
  const created = Object.fromEntries(ALL_KINDS.map((kind) => [kind, 0]));
  for (const row of source.channels) {
    const channelId = String(row.channel_id);
    const sequence = (kind) => cursors.get(`${channelId}\u0000${kind}`) ?? 0;
    if (sequence("about") === 0) {
      const baseline = aboutBaseline(row);
      await recordAboutObservation(client, {
        idempotencyKey: `migration-baseline:${config.baselineVersion}:${channelId}:about`,
        channelId,
        runId: null,
        observedAt: config.asOfAt,
        planId: null,
        planDay: null,
        triggerReason: "migration_baseline",
        scheduledAt: config.asOfAt,
        startedAt: config.asOfAt,
        finishedAt: config.asOfAt,
        crawlerVersion: config.crawlerVersion,
        extractorVersions: { migration_baseline: config.baselineVersion },
        command: {
          baseline_version: config.baselineVersion,
          source_snapshot_id: exportId,
        },
        about: baseline.about,
        current: baseline.current,
      });
      created.about += 1;
    }
    if (sequence("video") === 0) {
      const count = Number(source.identityCounts.get(channelId)?.identity_count ?? 0);
      const entries = source.identityEntries.get(channelId) ?? [];
      const discovery = discoveryBaseline({ identityCount: count, entries });
      const summary = source.recentCounts.get(channelId) ?? {};
      const recentSampling = recentSamplingBaseline({
        recentCount: summary.recent_count,
        staleCount: summary.stale_count,
      });
      const outcome = discovery.outcome === "complete"
        && recentSampling.outcome === "complete" ? "complete" : "partial";
      await recordCrawlerObservation(client, genericCommand(
        config,
        exportId,
        channelId,
        "video",
        async () => ({
          outcome,
          outcomeReasonCode: outcome === "complete"
            ? "migration_baseline_video_complete"
            : "migration_baseline_video_partial",
          resultSummary: {
            known_identity_count: count,
            exported_identity_count: discovery.payload.first_seen_count,
            recent_count: recentSampling.payload.recent_count,
            stale_ratio: recentSampling.payload.stale_ratio,
          },
          payload: {
            discovery: { outcome: discovery.outcome, payload: discovery.payload },
            recent_sampling: {
              outcome: recentSampling.outcome,
              payload: recentSampling.payload,
            },
          },
          anchorVideoIds: entries.slice(0, 20).map((entry) => String(entry.video_id)),
          sourceCursor: {
            baseline_version: config.baselineVersion,
            known_identity_count: count,
          },
        }),
      ));
      created.video += 1;
    }
    const agentRow = source.agents.get(channelId);
    if (sequence("agent") === 0 && agentRow) {
      const baseline = agentBaseline(agentRow);
      await recordCrawlerObservation(client, genericCommand(
        config,
        exportId,
        channelId,
        "agent",
        async ({ client: transactionClient, observationId }) => {
          await transactionClient.query(
            `UPDATE crawler.agent_profiles
             SET last_observation_id=$2,last_observed_at=$3,current_output_hash=$4,updated_at=now()
             WHERE channel_id=$1 AND agent_mode='basic' AND status='success'`,
            [channelId, observationId, config.asOfAt, baseline.current_hash],
          );
          return {
            outcome: baseline.outcome,
            outcomeReasonCode: "migration_baseline_agent_complete",
            resultSummary: { fulfilled_plan_count: 1, baseline: true },
            payload: baseline.payload,
          };
        },
      ));
      created.agent += 1;
    }
  }
  return created;
}

async function freezeExportEvents(client, config, exportId) {
  const missingMandatory = await client.query(
    `SELECT count(*)::int AS missing_count
     FROM crawler.channel_domain_cursors cursor
     JOIN crawler.channels channel ON channel.channel_id=cursor.channel_id
     WHERE channel.status='active'
       AND cursor.observation_kind=ANY($1::text[])
       AND cursor.latest_sequence=0`,
    [MANDATORY_KINDS],
  );
  if (Number(missingMandatory.rows[0].missing_count) !== 0) {
    throw new Error("not every Channel has the mandatory About and Video Baseline domains");
  }
  await client.query(
    `INSERT INTO crawler.baseline_export_events (
       export_id,event_id,channel_id,observation_kind,kind_sequence
     )
     SELECT $1,outbox.event_id,observation.channel_id,observation.observation_kind,
            observation.kind_sequence
     FROM crawler.channel_domain_cursors cursor
     JOIN crawler.channels channel
       ON channel.channel_id=cursor.channel_id AND channel.status='active'
     JOIN crawler.crawl_observations observation
       ON observation.channel_id=cursor.channel_id
      AND observation.observation_kind=cursor.observation_kind
      AND observation.kind_sequence<=cursor.latest_sequence
     JOIN crawler.crawler_outbox outbox ON outbox.observation_id=observation.observation_id
     WHERE cursor.latest_sequence>0
       AND cursor.observation_kind=ANY($2::text[])
     ORDER BY observation.channel_id,observation.observation_kind,observation.kind_sequence`,
    [exportId, ALL_KINDS],
  );
  const audit = await client.query(
    `SELECT count(*)::bigint AS event_count,
            count(DISTINCT member.channel_id)::int AS channel_count,
            count(*) FILTER (
              WHERE observation.observed_at>$2::timestamptz
            )::bigint AS events_after_cutoff,
            count(DISTINCT member.channel_id) FILTER (
              WHERE observation.outcome<>'failed'
            )::int AS channels_with_facts,
            (SELECT COALESCE(sum(latest_sequence),0)::bigint
             FROM crawler.channel_domain_cursors cursor
             JOIN crawler.channels channel ON channel.channel_id=cursor.channel_id
             WHERE channel.status='active') AS expected_event_count
     FROM crawler.baseline_export_events member
     JOIN crawler.crawler_outbox outbox ON outbox.event_id=member.event_id
     JOIN crawler.crawl_observations observation
       ON observation.observation_id=outbox.observation_id
     WHERE member.export_id=$1`,
    [exportId, config.asOfAt],
  );
  const row = audit.rows[0];
  if (String(row.event_count) !== String(row.expected_event_count)) {
    throw new Error("frozen Baseline membership does not match Cursor watermarks");
  }
  if (Number(row.channel_count) !== config.expectedChannelCount
      || Number(row.channels_with_facts) !== config.expectedChannelCount) {
    throw new Error("frozen Baseline membership does not cover every Channel with facts");
  }
  if (Number(row.events_after_cutoff) !== 0) {
    throw new Error("frozen Baseline contains an Observation newer than BASELINE_AS_OF_UTC");
  }
  return { eventCount: Number(row.event_count), channelCount: Number(row.channel_count) };
}

function verifyExistingExport(row, config) {
  if (iso(row.as_of_at) !== config.asOfAt
      || Number(row.expected_channel_count) !== config.expectedChannelCount
      || resolve(String(row.output_directory ?? "")) !== config.outputDirectory) {
    throw new Error("BASELINE_VERSION already exists with different immutable inputs");
  }
  if (row.status === "preparing") {
    throw new Error("Baseline export is stuck in preparing and requires operator inspection");
  }
}

async function prepareExport(client, config) {
  await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ");
  let committed = false;
  try {
    await client.query("SET LOCAL TIME ZONE 'UTC'");
    await client.query("SET LOCAL lock_timeout = '10s'");
    await client.query("SET LOCAL statement_timeout = '30min'");
    const database = await verifyInstalledSchema(client);
    if (database !== config.expectedDatabase) {
      throw new Error(`database confirmation mismatch: expected ${config.expectedDatabase}, got ${database}`);
    }
    const actualCount = await client.query(
      "SELECT count(*)::int AS channel_count FROM crawler.channels WHERE status='active'",
    );
    if (Number(actualCount.rows[0].channel_count) !== config.expectedChannelCount) {
      throw new Error(
        `Channel count mismatch: expected ${config.expectedChannelCount}, got ${actualCount.rows[0].channel_count}`,
      );
    }
    const existingResult = await client.query(
      `SELECT * FROM crawler.baseline_exports WHERE baseline_version=$1 FOR UPDATE`,
      [config.baselineVersion],
    );
    if (existingResult.rowCount > 0) {
      const existing = existingResult.rows[0];
      verifyExistingExport(existing, config);
      await client.query("COMMIT");
      committed = true;
      return {
        exportId: String(existing.export_id),
        sourceDatabase: database,
        eventCount: Number(existing.event_count),
        channelCount: Number(existing.channel_count),
        byteCount: Number(existing.byte_count),
        eventsSha256: existing.events_sha256,
        manifestSha256: existing.manifest_sha256,
        created: null,
        resumedFrom: String(existing.status),
      };
    }

    await lockSourceTables(client);
    await verifySourceSnapshot(client, config);
    const exportId = randomUUID();
    await client.query(
      `INSERT INTO crawler.baseline_exports (
         baseline_version,export_id,as_of_at,expected_channel_count,status,output_directory
       ) VALUES ($1,$2,$3,$4,'preparing',$5)`,
      [
        config.baselineVersion,
        exportId,
        config.asOfAt,
        config.expectedChannelCount,
        config.outputDirectory,
      ],
    );
    const source = await loadSourceRows(client, config);
    const created = await createMissingBaselines(client, config, exportId, source);
    const frozen = await freezeExportEvents(client, config, exportId);
    await client.query(
      `UPDATE crawler.baseline_exports
       SET status='events_committed',event_count=$2,channel_count=$3,last_error=NULL,
           updated_at=now()
       WHERE export_id=$1`,
      [exportId, frozen.eventCount, frozen.channelCount],
    );
    await client.query("COMMIT");
    committed = true;
    return {
      exportId,
      sourceDatabase: database,
      ...frozen,
      created,
      resumedFrom: null,
    };
  } finally {
    if (!committed) await client.query("ROLLBACK").catch(() => {});
  }
}

async function loadFrozenEvents(client, config, prepared) {
  await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
  let committed = false;
  try {
    await client.query("SET LOCAL TIME ZONE 'UTC'");
    const result = await client.query(
      `SELECT outbox.payload_json AS event
       FROM crawler.baseline_export_events member
       JOIN crawler.crawler_outbox outbox ON outbox.event_id=member.event_id
       WHERE member.export_id=$1
       ORDER BY member.channel_id,member.observation_kind,member.kind_sequence`,
      [prepared.exportId],
    );
    const serialized = serializeBaselineEvents(result.rows.map((row) => row.event), {
      expectedChannelCount: config.expectedChannelCount,
      exportedAt: config.asOfAt,
    });
    if (serialized.eventCount !== prepared.eventCount
        || serialized.channelCount !== prepared.channelCount) {
      throw new Error("serialized Baseline counts differ from the frozen export ledger");
    }
    await client.query("COMMIT");
    committed = true;
    return serialized;
  } finally {
    if (!committed) await client.query("ROLLBACK").catch(() => {});
  }
}

async function existingDirectory(path) {
  try {
    const value = await stat(path);
    return value.isDirectory();
  } catch (error) {
    if (error?.code === "ENOENT") return false;
    throw error;
  }
}

async function loadReadyBundle(config, prepared) {
  const [eventsBytes, manifestBytes] = await Promise.all([
    readFile(join(config.outputDirectory, EVENTS_FILE)),
    readFile(join(config.outputDirectory, MANIFEST_FILE)),
  ]);
  if (eventsBytes.length !== prepared.byteCount
      || baselineFileSha256(eventsBytes) !== prepared.eventsSha256
      || baselineFileSha256(manifestBytes) !== prepared.manifestSha256) {
    throw new Error("ready Baseline files disagree with the export ledger");
  }
  let events;
  try {
    events = eventsBytes.toString("utf8").trimEnd().split("\n")
      .map((line) => JSON.parse(line));
  } catch (error) {
    throw new Error(`ready Baseline events are not valid NDJSON: ${error.message}`);
  }
  const serialized = serializeBaselineEvents(events, {
    expectedChannelCount: config.expectedChannelCount,
    exportedAt: config.asOfAt,
  });
  if (!serialized.bytes.equals(eventsBytes)
      || serialized.eventCount !== prepared.eventCount
      || serialized.channelCount !== prepared.channelCount) {
    throw new Error("ready Baseline event content disagrees with the export ledger");
  }
  const expectedManifest = baselineManifest({
    baselineVersion: config.baselineVersion,
    sourceDatabase: prepared.sourceDatabase,
    sourceSnapshotId: prepared.exportId,
    exportedAt: config.asOfAt,
    eventsFile: EVENTS_FILE,
    eventCount: serialized.eventCount,
    channelCount: serialized.channelCount,
    byteCount: serialized.bytes.length,
    eventsSha256: serialized.eventsSha256,
  });
  const expectedManifestBytes = Buffer.from(`${JSON.stringify(expectedManifest)}\n`);
  if (!manifestBytes.equals(expectedManifestBytes)) {
    throw new Error("ready Baseline Manifest disagrees with immutable export inputs");
  }
  return {
    serialized,
    published: {
      manifest: expectedManifest,
      manifestBytes,
      manifestSha256: prepared.manifestSha256,
      reusedOutput: true,
    },
  };
}

async function publishBundle(config, prepared, serialized) {
  const manifest = baselineManifest({
    baselineVersion: config.baselineVersion,
    sourceDatabase: prepared.sourceDatabase,
    sourceSnapshotId: prepared.exportId,
    exportedAt: config.asOfAt,
    eventsFile: EVENTS_FILE,
    eventCount: serialized.eventCount,
    channelCount: serialized.channelCount,
    byteCount: serialized.bytes.length,
    eventsSha256: serialized.eventsSha256,
  });
  const manifestBytes = Buffer.from(`${JSON.stringify(manifest)}\n`);
  const manifestSha256 = baselineFileSha256(manifestBytes);
  if (await existingDirectory(config.outputDirectory)) {
    const [existingEvents, existingManifest] = await Promise.all([
      readFile(join(config.outputDirectory, EVENTS_FILE)),
      readFile(join(config.outputDirectory, MANIFEST_FILE)),
    ]);
    if (!existingEvents.equals(serialized.bytes) || !existingManifest.equals(manifestBytes)) {
      throw new Error("Baseline output directory exists with different content");
    }
    return { manifest, manifestBytes, manifestSha256, reusedOutput: true };
  }
  await mkdir(dirname(config.outputDirectory), { recursive: true });
  const staging = await mkdtemp(join(
    dirname(config.outputDirectory),
    `.${basename(config.outputDirectory)}.tmp-`,
  ));
  let renamed = false;
  try {
    await Promise.all([
      writeFile(join(staging, EVENTS_FILE), serialized.bytes, { flag: "wx", mode: 0o600 }),
      writeFile(join(staging, MANIFEST_FILE), manifestBytes, { flag: "wx", mode: 0o600 }),
    ]);
    await rename(staging, config.outputDirectory);
    renamed = true;
  } finally {
    if (!renamed) await rm(staging, { recursive: true, force: true }).catch(() => {});
  }
  return { manifest, manifestBytes, manifestSha256, reusedOutput: false };
}

async function markReady(client, config, prepared, serialized, published) {
  await client.query("BEGIN");
  let committed = false;
  try {
    await client.query("SET LOCAL TIME ZONE 'UTC'");
    const locked = await client.query(
      `SELECT * FROM crawler.baseline_exports WHERE export_id=$1 FOR UPDATE`,
      [prepared.exportId],
    );
    const row = locked.rows[0];
    if (!row) throw new Error("Baseline export ledger disappeared before finalization");
    verifyExistingExport(row, config);
    if (row.status === "ready" && (
      row.events_sha256 !== serialized.eventsSha256
      || row.manifest_sha256 !== published.manifestSha256
      || Number(row.byte_count) !== serialized.bytes.length
    )) {
      throw new Error("ready Baseline export ledger disagrees with generated files");
    }
    await client.query(
      `UPDATE crawler.baseline_exports
       SET status='ready',event_count=$2,channel_count=$3,byte_count=$4,
           events_sha256=$5,manifest_sha256=$6,last_error=NULL,
           completed_at=COALESCE(completed_at,now()),updated_at=now()
       WHERE export_id=$1`,
      [
        prepared.exportId,
        serialized.eventCount,
        serialized.channelCount,
        serialized.bytes.length,
        serialized.eventsSha256,
        published.manifestSha256,
      ],
    );
    await client.query("COMMIT");
    committed = true;
  } finally {
    if (!committed) await client.query("ROLLBACK").catch(() => {});
  }
}

async function markFailed(client, exportId, error) {
  if (!exportId) return;
  await client.query(
    `UPDATE crawler.baseline_exports
     SET status='failed',last_error=$2,updated_at=now()
     WHERE export_id=$1 AND status<>'ready'`,
    [exportId, String(error?.message || error).slice(0, 2000)],
  ).catch(() => {});
}

const config = configuration();
const client = new Client({ connectionString: databaseUrl(), options: "-c timezone=UTC" });
let locked = false;
let prepared = null;
try {
  await client.connect();
  const acquired = await client.query(
    "SELECT pg_try_advisory_lock($1) AS acquired",
    [EXPORT_LOCK_ID],
  );
  if (!acquired.rows[0]?.acquired) {
    throw new Error("another V16 Baseline export is already running");
  }
  locked = true;
  prepared = await prepareExport(client, config);
  const ready = prepared.resumedFrom === "ready"
    ? await loadReadyBundle(config, prepared)
    : null;
  const serialized = ready?.serialized ?? await loadFrozenEvents(client, config, prepared);
  const published = ready?.published ?? await publishBundle(config, prepared, serialized);
  await markReady(client, config, prepared, serialized, published);
  console.log(JSON.stringify({
    ok: true,
    database: prepared.sourceDatabase,
    baseline_version: config.baselineVersion,
    source_snapshot_id: prepared.exportId,
    exported_at: config.asOfAt,
    channel_count: serialized.channelCount,
    event_count: serialized.eventCount,
    byte_count: serialized.bytes.length,
    events_sha256: serialized.eventsSha256,
    manifest_sha256: published.manifestSha256,
    output_directory: config.outputDirectory,
    created_observations: prepared.created,
    resumed_from: prepared.resumedFrom,
    reused_output: published.reusedOutput,
  }));
} catch (error) {
  await markFailed(client, prepared?.exportId, error);
  throw error;
} finally {
  if (locked) {
    await client.query("SELECT pg_advisory_unlock($1)", [EXPORT_LOCK_ID]).catch(() => {});
  }
  await client.end().catch(() => {});
}
