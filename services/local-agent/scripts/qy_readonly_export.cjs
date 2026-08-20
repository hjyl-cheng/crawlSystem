"use strict";

/*
 * Run this script inside a QY crawler container with:
 *   docker exec -i -w /app <worker> node - --mode agent-reference --limit 200 \
 *     < scripts/qy_readonly_export.cjs
 *
 * It emits JSONL to stdout and never writes to PostgreSQL.
 */

const { Client } = require("pg");

function option(name, fallback = null) {
  const exact = `--${name}`;
  const prefix = `${exact}=`;
  const index = process.argv.findIndex((value) => value === exact || value.startsWith(prefix));
  if (index < 0) return fallback;
  const value = process.argv[index];
  return value.startsWith(prefix) ? value.slice(prefix.length) : process.argv[index + 1];
}

function positiveInteger(value, fallback, maximum) {
  const parsed = Number(value ?? fallback);
  if (!Number.isSafeInteger(parsed) || parsed <= 0 || parsed > maximum) {
    throw new TypeError(`expected integer in [1, ${maximum}], got ${value}`);
  }
  return parsed;
}

function booleanOption(name, fallback = false) {
  const raw = option(name, fallback ? "true" : "false");
  if (raw === true || raw === "true" || raw === "1") return true;
  if (raw === false || raw === "false" || raw === "0") return false;
  throw new TypeError(`--${name} must be true or false`);
}

function timestamp(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new TypeError(`invalid timestamp: ${value}`);
  return parsed.toISOString();
}

function channelPayload(row) {
  return {
    channel_id: row.channel_id,
    channel_url: row.channel_url,
    handle: row.handle,
    title: row.title,
    country: row.country,
    country_source: row.country_source,
    country_code: row.country_code,
    country_canonical_name: row.country_canonical_name,
    avatar_url: row.avatar_url,
    summary: row.summary,
    keywords: row.keywords || [],
    about_description: row.about_description,
    joined_at: row.joined_at,
    external_links: row.external_links || [],
    is_verified: row.is_verified,
    subscriber_count: row.snapshot_subscriber_count ?? row.subscriber_count,
    total_view_count: row.snapshot_total_view_count ?? row.total_view_count,
    total_video_count: row.snapshot_total_video_count ?? row.total_video_count,
    channel_extractor: row.source_json?.channel_extractor ?? null,
  };
}

function contentPayload(row) {
  const payload = {
    source_content_id: row.source_content_id,
    content_type: row.content_type,
    content_type_source: row.content_type_source,
    title: row.title,
    description: row.description,
    description_status: row.description_status,
    description_source: row.description_source,
    thumbnail_url: row.thumbnail_url,
    keywords: row.keywords || [],
    hashtags: row.hashtags || [],
    published_at: row.published_at,
    published_at_status: row.published_at_status,
    published_at_source: row.published_at_source,
    published_at_precision: row.published_at_precision,
    first_seen_at: row.first_seen_at,
    view_count: row.view_count,
    view_count_status: row.view_count_status,
    view_count_source: row.view_count_source,
    like_count: row.like_count,
    like_count_status: row.like_count_status,
    like_count_source: row.like_count_source,
    comment_count: row.comment_count,
    comment_count_status: row.comment_count_status,
    comment_count_source: row.comment_count_source,
    comments_disabled: row.comments_disabled,
    duration_seconds: row.duration_seconds,
    duration_status: row.duration_status,
    duration_source: row.duration_source,
    extractor_version: row.extractor_version,
  };
  if (row.comments_first_page != null) {
    payload.comments_first_page = row.comments_first_page;
  }
  return payload;
}

async function selectChannels(client, { mode, lastChannelId, batchSize, channelId }) {
  const commonColumns = `
    ch.channel_id,ch.channel_url,ch.handle,ch.title,ch.country,ch.country_source,
    ch.country_code,ch.country_canonical_name,ch.avatar_url,ch.summary,ch.keywords,
    ch.about_description,ch.joined_at,ch.external_links,ch.is_verified,
    ch.subscriber_count,ch.total_view_count,ch.total_video_count,ch.source_json`;
  if (mode === "agent-reference") {
    return client.query(
      `SELECT ${commonColumns},
              ap.input_url,ap.metrics_json,ap.agent_model,ap.updated_at AS agent_as_of,
              ap.input_content_ids,ap.input_content_hash,ap.prompt_variant,
              ap.taxonomy_version,ap.agent_version_hash,
              historical.observed_at AS historical_stats_at,
              historical.subscriber_count AS snapshot_subscriber_count,
              historical.total_view_count AS snapshot_total_view_count,
              historical.total_video_count AS snapshot_total_video_count
       FROM crawler.agent_profiles ap
       JOIN crawler.channels ch ON ch.channel_id=ap.channel_id
       LEFT JOIN LATERAL (
         SELECT snap.observed_at,snap.subscriber_count,snap.total_view_count,snap.total_video_count
         FROM crawler.channel_about_metric_snapshots snap
         WHERE snap.channel_id=ap.channel_id AND snap.observed_at<=ap.updated_at
         ORDER BY snap.observed_at DESC,snap.observation_id DESC
         LIMIT 1
       ) historical ON true
       WHERE ap.status='success' AND ap.agent_mode='basic'
         AND ch.channel_id>$1
         AND ($2::text IS NULL OR ch.channel_id=$2)
       ORDER BY ch.channel_id
       LIMIT $3`,
      [lastChannelId, channelId, batchSize],
    );
  }
  if (mode === "current-agent-cohort") {
    return client.query(
      `SELECT ${commonColumns},
              ap.input_url,ap.metrics_json,ap.agent_model,ap.updated_at AS agent_as_of,
              ap.input_content_ids,ap.input_content_hash,ap.prompt_variant,
              ap.taxonomy_version,ap.agent_version_hash,
              NULL::timestamptz AS historical_stats_at,
              NULL::bigint AS snapshot_subscriber_count,
              NULL::bigint AS snapshot_total_view_count,
              NULL::bigint AS snapshot_total_video_count
       FROM crawler.agent_profiles ap
       JOIN crawler.channels ch ON ch.channel_id=ap.channel_id
       WHERE ap.status='success' AND ap.agent_mode='basic'
         AND ch.channel_id>$1
         AND ($2::text IS NULL OR ch.channel_id=$2)
       ORDER BY ch.channel_id
       LIMIT $3`,
      [lastChannelId, channelId, batchSize],
    );
  }
  return client.query(
    `SELECT ${commonColumns},
            NULL::bigint AS snapshot_subscriber_count,
            NULL::bigint AS snapshot_total_view_count,
            NULL::bigint AS snapshot_total_video_count
     FROM crawler.channels ch
     WHERE ch.channel_id>$1
       AND ($2::text IS NULL OR ch.channel_id=$2)
       AND ($2::text IS NOT NULL OR ch.status IN ('active','dormant'))
     ORDER BY ch.channel_id
     LIMIT $3`,
    [lastChannelId, channelId, batchSize],
  );
}

async function selectContents(client, channelRows, mode, contentLimit, currentAsOf, includeComments) {
  const bounds = Object.fromEntries(channelRows.map((row) => [
    row.channel_id,
    mode === "agent-reference" ? timestamp(row.agent_as_of) : currentAsOf,
  ]));
  const result = await client.query(
    `WITH bounds AS (
       SELECT key AS channel_id,value::timestamptz AS as_of,
              COALESCE(($2::jsonb -> key), '[]'::jsonb) AS retained_ids
       FROM jsonb_each_text($1::jsonb)
     ), ranked AS (
       SELECT c.channel_id,c.source_content_id,c.content_type,c.content_type_source,c.title,c.description,
              c.description_status,c.description_source,c.thumbnail_url,c.keywords,c.hashtags,
              c.published_at,c.published_at_status,c.published_at_source,c.published_at_precision,
              c.first_seen_at,c.view_count,c.view_count_status,c.view_count_source,
              c.like_count,c.like_count_status,c.like_count_source,c.comment_count,
              c.comment_count_status,c.comment_count_source,c.comments_disabled,
              c.duration_seconds,c.duration_status,c.duration_source,c.extractor_version,
              CASE
                WHEN $4::boolean
                 AND c.comments_first_page IS NOT NULL
                 AND (c.comments_first_page->>'collected_at') ~
                     '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}'
                 AND (c.comments_first_page->>'collected_at')::timestamptz<=b.as_of
                THEN c.comments_first_page
                ELSE NULL
              END AS comments_first_page,
              row_number() OVER (
                PARTITION BY c.channel_id
                ORDER BY c.published_at DESC NULLS LAST,c.position ASC NULLS LAST,c.source_content_id
              ) AS position_rank
       FROM crawler.contents c
       JOIN bounds b ON b.channel_id=c.channel_id
       WHERE c.content_type IN ('video','short','live')
         AND c.first_seen_at<=b.as_of
         AND (c.published_at IS NULL OR c.published_at<=b.as_of)
         AND (
           jsonb_array_length(b.retained_ids)=0
           OR b.retained_ids ? c.source_content_id
         )
     )
     SELECT * FROM ranked
     WHERE position_rank<=$3
     ORDER BY channel_id,position_rank`,
    [
      JSON.stringify(bounds),
      JSON.stringify(Object.fromEntries(channelRows.map((row) => [
        row.channel_id,
        mode === "agent-reference" && Array.isArray(row.input_content_ids)
          ? row.input_content_ids.filter(Boolean)
          : [],
      ]))),
      contentLimit,
      includeComments,
    ],
  );
  const byChannel = new Map(channelRows.map((row) => [row.channel_id, []]));
  for (const row of result.rows) byChannel.get(row.channel_id).push(contentPayload(row));
  return byChannel;
}

async function main() {
  const mode = String(option("mode", "current"));
  if (!new Set(["current", "current-agent-cohort", "agent-reference"]).has(mode)) {
    throw new TypeError("--mode must be current, current-agent-cohort, or agent-reference");
  }
  const limit = positiveInteger(option("limit", "100"), 100, 100000);
  const batchSize = Math.min(limit, positiveInteger(option("batch-size", "100"), 100, 1000));
  const contentLimit = positiveInteger(option("content-limit", "30"), 30, 100);
  const includeComments = booleanOption("include-comments", false);
  const channelId = option("channel-id");
  if (channelId && !/^UC[-_A-Za-z0-9]{20,}$/.test(channelId)) {
    throw new TypeError("--channel-id is not a valid YouTube channel ID");
  }
  const afterChannelId = String(option("after-channel-id", "") || "").trim();
  if (afterChannelId && !/^UC[-_A-Za-z0-9]{20,}$/.test(afterChannelId)) {
    throw new TypeError("--after-channel-id is not a valid YouTube channel ID");
  }
  if (channelId && afterChannelId) {
    throw new TypeError("--channel-id and --after-channel-id cannot be combined");
  }

  const client = new Client({
    host: process.env.POSTGRES_HOST,
    port: Number(process.env.POSTGRES_PORT || 5432),
    database: process.env.POSTGRES_DB,
    user: process.env.POSTGRES_USER,
    password: process.env.POSTGRES_PASSWORD,
    application_name: "qy_channel_profile_readonly_export",
  });
  await client.connect();
  let exported = 0;
  let lastChannelId = afterChannelId;
  const currentAsOf = new Date().toISOString();
  try {
    await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
    await client.query("SET LOCAL statement_timeout='300s'");
    const readOnly = await client.query("SHOW transaction_read_only");
    if (readOnly.rows[0]?.transaction_read_only !== "on") {
      throw new Error("database transaction is not read-only");
    }
    while (exported < limit) {
      const selected = await selectChannels(client, {
        mode,
        lastChannelId,
        batchSize: Math.min(batchSize, limit - exported),
        channelId,
      });
      if (selected.rows.length === 0) break;
      const contents = await selectContents(
        client,
        selected.rows,
        mode,
        contentLimit,
        currentAsOf,
        includeComments,
      );
      for (const row of selected.rows) {
        const historicalReplay = mode === "agent-reference";
        const hasAgentReference = historicalReplay || mode === "current-agent-cohort";
        const asOf = historicalReplay ? timestamp(row.agent_as_of) : currentAsOf;
        const retainedIds = Array.isArray(row.input_content_ids) ? row.input_content_ids.filter(Boolean) : [];
        const returnedIds = (contents.get(row.channel_id) || []).map((item) => item.source_content_id);
        const exactRetainedSet = retainedIds.length > 0
          && retainedIds.length === returnedIds.length
          && retainedIds.every((value) => returnedIds.includes(value));
        const replayQuality = historicalReplay
          ? (exactRetainedSet ? "exact_content_set_current_metrics" : "approximate_as_of")
          : "current_exact";
        const envelope = {
          input_url: hasAgentReference ? row.input_url : row.channel_url,
          snapshot: {
            channel: channelPayload(row),
            contents: contents.get(row.channel_id) || [],
            as_of: asOf,
            replay_quality: replayQuality,
            provenance: historicalReplay ? {
              data_lineage_version: "snapshot-field-lineage-v1",
              comment_page_source_status: includeComments ? "extractor_not_persisted" : "not_requested",
              historical_agent_as_of: asOf,
              input_content_ids_retained: retainedIds.length,
              input_content_ids_resolved: returnedIds.length,
              historical_input_content_hash: row.input_content_hash,
              content_set_boundary: "first_seen_at_and_published_at_lte_agent_as_of",
              channel_text_temporality: "current_projection",
              content_metric_temporality: "current_projection_not_historical",
              channel_stats_temporality: row.historical_stats_at
                ? "nearest_historical_about_snapshot"
                : "current_projection_fallback",
              historical_stats_at: row.historical_stats_at,
            } : {
              data_lineage_version: "snapshot-field-lineage-v1",
              comment_page_source_status: includeComments ? "extractor_not_persisted" : "not_requested",
              content_set_boundary: "repeatable_read_transaction",
              channel_text_temporality: "current_projection",
              content_metric_temporality: "current_projection",
              channel_stats_temporality: "current_projection",
              comments_included: includeComments,
              comment_boundary: "comments_first_page.collected_at_lte_snapshot_as_of",
            },
          },
        };
        if (hasAgentReference) {
          envelope.agent_reference = {
            metrics_json: row.metrics_json,
            agent_model: row.agent_model,
            updated_at: row.agent_as_of,
            prompt_variant: row.prompt_variant,
            taxonomy_version: row.taxonomy_version,
            agent_version_hash: row.agent_version_hash,
          };
        }
        process.stdout.write(`${JSON.stringify(envelope)}\n`);
        exported += 1;
      }
      lastChannelId = selected.rows[selected.rows.length - 1].channel_id;
      process.stderr.write(`${JSON.stringify({
        event: "qy_readonly_export_progress",
        mode,
        include_comments: includeComments,
        exported,
        last_channel_id: lastChannelId,
      })}\n`);
      if (channelId) break;
    }
    await client.query("ROLLBACK");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    await client.end();
  }
  process.stderr.write(`${JSON.stringify({
    event: "qy_readonly_export_complete",
    mode,
    include_comments: includeComments,
    exported,
  })}\n`);
}

process.stdout.on("error", (error) => {
  if (error.code === "EPIPE") process.exit(0);
  throw error;
});

main().catch((error) => {
  process.stderr.write(`${JSON.stringify({ event: "qy_readonly_export_failed", error: error.message })}\n`);
  process.exitCode = 1;
});
