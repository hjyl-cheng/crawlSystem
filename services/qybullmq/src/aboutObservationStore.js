import { createHash, randomUUID } from "node:crypto";
import { collectObservedQueryTerms } from "./queryCollector.js";
import { normalizeAboutCurrentIdentity } from "./aboutCurrent.js";
import { normalizePublicationLinks } from "./publicationLinks.js";
import { reconcilePublication } from "./publicationReconciler.js";
import { normalizePublicationUrl } from "./publicationUrl.js";
import { normalizeVerifiedCurrent } from "./verifiedCurrent.js";
import { normalizeYoutubeBusinessEmailCurrent } from "./youtubeBusinessEmailAvailability.js";

const RESOLVED_STATUSES = new Set(["exact", "estimated"]);
const METRIC_STATUSES = new Set(["exact", "estimated", "unavailable", "unresolved"]);
const OBSERVATION_STATUSES = new Set(["observed", "unresolved"]);
const DESCRIPTION_STATUSES = new Set(["exact", "empty", "unresolved"]);
const BUSINESS_EMAIL_STATUSES = new Set(["available", "not_available", "unknown"]);

export class IdempotencyConflict extends Error {
  constructor(idempotencyKey) {
    super(`idempotency key was reused with a different command: ${idempotencyKey}`);
    this.name = "IdempotencyConflict";
    this.idempotencyKey = idempotencyKey;
  }
}

export function aboutObservationIdempotencyKey({ runId, executionAttemptId }) {
  return `about:${requiredText(runId, "runId")}:${requiredText(executionAttemptId, "executionAttemptId")}`;
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function optionalText(value) {
  if (value === null || value === undefined) return null;
  return String(value).slice(0, 4000);
}

function enumValue(value, allowed, field, fallback) {
  const output = optionalText(value) ?? fallback;
  if (!allowed.has(output)) throw new TypeError(`invalid ${field}: ${output}`);
  return output;
}

function optionalBoolean(value, field) {
  if (value === null || value === undefined) return null;
  if (typeof value !== "boolean") throw new TypeError(`${field} must be a boolean or null`);
  return value;
}

function timestamp(value, field, { required = false } = {}) {
  if (value === null || value === undefined || value === "") {
    if (required) throw new TypeError(`${field} is required`);
    return null;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new TypeError(`${field} must be a valid timestamp`);
  return parsed.toISOString();
}

function canonicalize(value) {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(canonicalize);
  return Object.fromEntries(Object.keys(value).sort()
    .filter((key) => value[key] !== undefined)
    .map((key) => [key, canonicalize(value[key])]));
}

function canonicalHash(value) {
  const body = JSON.stringify(canonicalize(value));
  return `sha256:${createHash("sha256").update(body).digest("hex")}`;
}

function metricFacts(metrics) {
  return {
    subscriber_count: metrics.subscriber_count.value,
    subscriber_count_status: metrics.subscriber_count.status,
    total_view_count: metrics.total_view_count.value,
    total_view_count_status: metrics.total_view_count.status,
    total_video_count: metrics.total_video_count.value,
    total_video_count_status: metrics.total_video_count.status,
  };
}

function metricFactsHash(metrics) {
  const body = JSON.stringify(metricFacts(metrics));
  return `sha256:${createHash("sha256").update(body).digest("hex")}`;
}

function validateMetric(about, name) {
  const status = requiredText(about?.[`${name}_status`], `${name}_status`);
  if (!METRIC_STATUSES.has(status)) throw new TypeError(`invalid ${name}_status: ${status}`);
  const rawValue = about?.[name];
  const value = rawValue === null || rawValue === undefined ? null : Number(rawValue);
  if (value !== null && (!Number.isSafeInteger(value) || value < 0)) {
    throw new TypeError(`${name} must be a non-negative safe integer or null`);
  }
  if (RESOLVED_STATUSES.has(status) !== (value !== null)) {
    throw new TypeError(`${name} value and status disagree`);
  }
  return {
    value,
    text: optionalText(about?.[`${name}_text`]),
    status,
    source: optionalText(about?.[`${name}_source`]),
  };
}

function normalizeCommand(command) {
  const about = command?.about ?? {};
  const outcome = requiredText(about.outcome, "about.outcome");
  if (!["complete", "partial", "failed"].includes(outcome)) {
    throw new TypeError(`invalid About outcome: ${outcome}`);
  }
  const metrics = {
    subscriber_count: validateMetric(about, "subscriber_count"),
    total_view_count: validateMetric(about, "total_view_count"),
    total_video_count: validateMetric(about, "total_video_count"),
  };
  const resolvedMetricCount = Object.values(metrics)
    .filter((metric) => RESOLVED_STATUSES.has(metric.status)).length;
  if (outcome === "failed" && resolvedMetricCount > 0) {
    throw new TypeError("failed About observations cannot contain resolved metrics");
  }
  if (outcome === "complete" && resolvedMetricCount !== 3) {
    throw new TypeError("complete About observations require all three metrics");
  }
  if (outcome === "partial" && resolvedMetricCount === 3) {
    throw new TypeError("partial About observations cannot contain all three metrics");
  }
  const expectedFactsHash = metricFactsHash(metrics);
  const suppliedFactsHash = requiredText(about.facts_hash, "about.facts_hash");
  if (suppliedFactsHash !== expectedFactsHash) {
    throw new TypeError("about.facts_hash does not match the normalized metrics");
  }

  const current = command.current ?? {};
  const identity = current.identity == null
    ? null
    : normalizeAboutCurrentIdentity({
        title: current.identity.title,
        handle: current.identity.handle,
        avatar_url: current.identity.avatar_url,
        keywords: current.identity.keywords,
        available_tabs: current.identity.available_tabs,
        description: current.identity.summary,
      });
  const descriptionStatus = enumValue(
    current.descriptionStatus,
    DESCRIPTION_STATUSES,
    "current.descriptionStatus",
    current.aboutDescription == null ? "unresolved" : optionalText(current.aboutDescription) ? "exact" : "empty",
  );
  const keywordsStatus = enumValue(
    current.keywordsStatus,
    OBSERVATION_STATUSES,
    "current.keywordsStatus",
    identity == null ? "unresolved" : "observed",
  );
  const availableTabsStatus = enumValue(
    current.availableTabsStatus,
    OBSERVATION_STATUSES,
    "current.availableTabsStatus",
    identity == null ? "unresolved" : "observed",
  );
  const requestedLinksStatus = enumValue(
    current.externalLinksStatus,
    OBSERVATION_STATUSES,
    "current.externalLinksStatus",
    Array.isArray(current.externalLinks) ? "observed" : "unresolved",
  );
  const normalizedLinks = normalizePublicationLinks(current.externalLinks, {
    observed: requestedLinksStatus === "observed",
  });
  const externalLinksStatus = normalizedLinks.ready ? "observed" : "unresolved";
  const isVerified = optionalBoolean(current.isVerified, "current.isVerified");
  const requestedVerifiedStatus = optionalText(current.isVerifiedStatus) ?? "unknown";
  const verified = normalizeVerifiedCurrent(isVerified, requestedVerifiedStatus);
  if (!verified.valid || verified.status !== requestedVerifiedStatus) {
    throw new TypeError("current.isVerified and current.isVerifiedStatus disagree");
  }
  const isVerifiedStatus = verified.status;
  const youtubeBusinessEmailAvailable = optionalBoolean(
    current.youtubeBusinessEmailAvailable,
    "current.youtubeBusinessEmailAvailable",
  );
  const requestedBusinessEmailStatus = enumValue(
    current.youtubeBusinessEmailStatus,
    BUSINESS_EMAIL_STATUSES,
    "current.youtubeBusinessEmailStatus",
    "unknown",
  );
  const businessEmail = normalizeYoutubeBusinessEmailCurrent(
    youtubeBusinessEmailAvailable,
    requestedBusinessEmailStatus,
  );
  if (businessEmail.status !== requestedBusinessEmailStatus) {
    throw new TypeError(
      "current.youtubeBusinessEmailAvailable and current.youtubeBusinessEmailStatus disagree",
    );
  }

  const normalized = {
    idempotencyKey: requiredText(command.idempotencyKey, "idempotencyKey"),
    channelId: requiredText(command.channelId, "channelId"),
    runId: optionalText(command.runId),
    observedAt: timestamp(command.observedAt, "observedAt", { required: true }),
    planId: optionalText(command.planId),
    planDay: optionalText(command.planDay),
    triggerReason: requiredText(command.triggerReason, "triggerReason"),
    scheduledAt: timestamp(command.scheduledAt, "scheduledAt"),
    startedAt: timestamp(command.startedAt, "startedAt"),
    finishedAt: timestamp(command.finishedAt, "finishedAt"),
    crawlerVersion: optionalText(command.crawlerVersion),
    extractorVersions: canonicalize(command.extractorVersions ?? {}),
    errorClass: optionalText(command.errorClass)?.slice(0, 255) ?? null,
    errorMessage: optionalText(command.errorMessage)?.slice(0, 2000) ?? null,
    publicationReconcile: command.publicationReconcile !== false,
    outcome,
    outcomeReasonCode: requiredText(about.outcome_reason_code, "about.outcome_reason_code"),
    factsHash: expectedFactsHash,
    snapshotEligible: outcome !== "failed" && resolvedMetricCount > 0,
    resolvedMetricCount,
    metrics,
    current: {
      aboutDescription: optionalText(current.aboutDescription),
      descriptionStatus,
      country: optionalText(current.country),
      joinedDateText: optionalText(current.joinedDateText),
      joinedAt: current.joinedAt == null ? null : optionalText(current.joinedAt),
      joinedAtPrecision: current.joinedAt == null
        ? "unknown"
        : optionalText(current.joinedAtPrecision) ?? "date_only",
      externalLinks: normalizedLinks.ready ? normalizedLinks.links : null,
      externalLinksStatus,
      rssUrl: normalizePublicationUrl(current.rssUrl),
      vanityChannelUrl: normalizePublicationUrl(current.vanityChannelUrl),
      isFamilySafe: optionalBoolean(current.isFamilySafe, "current.isFamilySafe"),
      isVerified,
      isVerifiedStatus,
      youtubeBusinessEmailAvailable: businessEmail.available,
      youtubeBusinessEmailStatus: businessEmail.status,
      keywordsStatus,
      availableTabsStatus,
      normalization: {
        links_ready: normalizedLinks.ready,
        link_issue_codes: [...new Set(normalizedLinks.issues.map((issue) => issue.code))].sort(),
      },
      ...(identity == null ? {} : { identity }),
    },
  };
  normalized.commandHash = canonicalHash({
    channel_id: normalized.channelId,
    run_id: normalized.runId,
    observed_at: normalized.observedAt,
    plan_id: normalized.planId,
    plan_day: normalized.planDay,
    trigger_reason: normalized.triggerReason,
    scheduled_at: normalized.scheduledAt,
    started_at: normalized.startedAt,
    finished_at: normalized.finishedAt,
    crawler_version: normalized.crawlerVersion,
    extractor_versions: normalized.extractorVersions,
    error_class: normalized.errorClass,
    error_message: normalized.errorMessage,
    outcome: normalized.outcome,
    outcome_reason_code: normalized.outcomeReasonCode,
    facts_hash: normalized.factsHash,
    metrics: normalized.metrics,
    current: normalized.current,
  });
  return normalized;
}

async function duplicateResult(client, keyRow, idempotencyKey) {
  const result = await client.query(
    `SELECT observation.kind_sequence,observation.outcome,outbox.event_id,
            snapshot.observation_id AS snapshot_observation_id
     FROM crawler.crawl_observations observation
     LEFT JOIN crawler.crawler_outbox outbox
       ON outbox.observation_id=observation.observation_id
     LEFT JOIN crawler.channel_about_metric_snapshots snapshot
       ON snapshot.observation_id=observation.observation_id
     WHERE observation.observation_id=$1`,
    [keyRow.observation_id],
  );
  const row = result.rows[0] ?? {};
  return {
    duplicate: true,
    idempotency_key: idempotencyKey,
    observation_id: keyRow.observation_id,
    event_id: row.event_id ?? null,
    kind_sequence: row.kind_sequence == null ? null : Number(row.kind_sequence),
    outcome: row.outcome ?? null,
    snapshot_written: row.outcome == null ? null : row.snapshot_observation_id != null,
  };
}

async function claimObservationKey(client, command, observationId) {
  const claimed = await client.query(
    `INSERT INTO crawler.crawl_observation_keys (
       idempotency_key,observation_id,command_hash,observed_at,channel_id,
       observation_kind,expires_at
     ) VALUES ($1,$2,$3,$4,$5,'about',now() + interval '90 days')
     ON CONFLICT (idempotency_key) DO NOTHING
     RETURNING observation_id,command_hash`,
    [
      command.idempotencyKey,
      observationId,
      command.commandHash,
      command.observedAt,
      command.channelId,
    ],
  );
  if (claimed.rowCount > 0) return null;

  const existing = await client.query(
    `SELECT observation_id,command_hash
     FROM crawler.crawl_observation_keys
     WHERE idempotency_key=$1
     FOR UPDATE`,
    [command.idempotencyKey],
  );
  const keyRow = existing.rows[0];
  if (!keyRow || keyRow.command_hash !== command.commandHash) {
    throw new IdempotencyConflict(command.idempotencyKey);
  }
  return duplicateResult(client, keyRow, command.idempotencyKey);
}

async function allocateSequence(client, channelId) {
  await client.query(
    `INSERT INTO crawler.channel_domain_cursors (channel_id,observation_kind)
     VALUES ($1,'about')
     ON CONFLICT (channel_id,observation_kind) DO NOTHING`,
    [channelId],
  );
  const cursor = await client.query(
    `SELECT latest_sequence
     FROM crawler.channel_domain_cursors
     WHERE channel_id=$1 AND observation_kind='about'
     FOR UPDATE`,
    [channelId],
  );
  return (BigInt(cursor.rows[0]?.latest_sequence ?? 0) + 1n).toString();
}

async function updateCurrentAbout(client, command) {
  if (command.outcome === "failed") return;
  const subscriber = command.metrics.subscriber_count;
  const views = command.metrics.total_view_count;
  const videos = command.metrics.total_video_count;
  await client.query(
    `UPDATE crawler.channels
     SET subscriber_count=CASE
           WHEN $3::bigint IS NOT NULL
             AND (subscriber_count_observed_at IS NULL OR subscriber_count_observed_at <= $2::timestamptz)
           THEN $3::bigint ELSE subscriber_count END,
         subscriber_count_text=CASE
           WHEN $3::bigint IS NOT NULL
             AND (subscriber_count_observed_at IS NULL OR subscriber_count_observed_at <= $2::timestamptz)
           THEN $4 ELSE subscriber_count_text END,
         subscriber_count_status=CASE
           WHEN $3::bigint IS NOT NULL
             AND (subscriber_count_observed_at IS NULL OR subscriber_count_observed_at <= $2::timestamptz)
           THEN $5 ELSE subscriber_count_status END,
         subscriber_count_source=CASE
           WHEN $3::bigint IS NOT NULL
             AND (subscriber_count_observed_at IS NULL OR subscriber_count_observed_at <= $2::timestamptz)
           THEN $6 ELSE subscriber_count_source END,
         subscriber_count_observed_at=CASE
           WHEN $3::bigint IS NOT NULL
             AND (subscriber_count_observed_at IS NULL OR subscriber_count_observed_at <= $2::timestamptz)
           THEN $2::timestamptz ELSE subscriber_count_observed_at END,
         total_view_count=CASE
           WHEN $7::bigint IS NOT NULL
             AND (total_view_count_observed_at IS NULL OR total_view_count_observed_at <= $2::timestamptz)
           THEN $7::bigint ELSE total_view_count END,
         total_view_count_text=CASE
           WHEN $7::bigint IS NOT NULL
             AND (total_view_count_observed_at IS NULL OR total_view_count_observed_at <= $2::timestamptz)
           THEN $8 ELSE total_view_count_text END,
         total_view_count_status=CASE
           WHEN $7::bigint IS NOT NULL
             AND (total_view_count_observed_at IS NULL OR total_view_count_observed_at <= $2::timestamptz)
           THEN $9 ELSE total_view_count_status END,
         total_view_count_source=CASE
           WHEN $7::bigint IS NOT NULL
             AND (total_view_count_observed_at IS NULL OR total_view_count_observed_at <= $2::timestamptz)
           THEN $10 ELSE total_view_count_source END,
         total_view_count_observed_at=CASE
           WHEN $7::bigint IS NOT NULL
             AND (total_view_count_observed_at IS NULL OR total_view_count_observed_at <= $2::timestamptz)
           THEN $2::timestamptz ELSE total_view_count_observed_at END,
         total_video_count=CASE
           WHEN $11::bigint IS NOT NULL
             AND (total_video_count_observed_at IS NULL OR total_video_count_observed_at <= $2::timestamptz)
           THEN $11::bigint ELSE total_video_count END,
         total_video_count_text=CASE
           WHEN $11::bigint IS NOT NULL
             AND (total_video_count_observed_at IS NULL OR total_video_count_observed_at <= $2::timestamptz)
           THEN $12 ELSE total_video_count_text END,
         total_video_count_status=CASE
           WHEN $11::bigint IS NOT NULL
             AND (total_video_count_observed_at IS NULL OR total_video_count_observed_at <= $2::timestamptz)
           THEN $13 ELSE total_video_count_status END,
         total_video_count_source=CASE
           WHEN $11::bigint IS NOT NULL
             AND (total_video_count_observed_at IS NULL OR total_video_count_observed_at <= $2::timestamptz)
           THEN $14 ELSE total_video_count_source END,
         total_video_count_observed_at=CASE
           WHEN $11::bigint IS NOT NULL
             AND (total_video_count_observed_at IS NULL OR total_video_count_observed_at <= $2::timestamptz)
           THEN $2::timestamptz ELSE total_video_count_observed_at END,
         about_description=CASE
           WHEN (about_last_observed_at IS NULL OR about_last_observed_at <= $2::timestamptz)
             AND (
               $16::text='exact'
               OR (
                 $16::text='empty'
                 AND COALESCE(
                   NULLIF(btrim(about_description),''),
                   NULLIF(btrim(summary),'')
                 ) IS NULL
               )
             )
           THEN $15::text ELSE about_description END,
         description_status=CASE
           WHEN (about_last_observed_at IS NULL OR about_last_observed_at <= $2::timestamptz)
             AND (
               $16::text='exact'
               OR (
                 $16::text='empty'
                 AND COALESCE(
                   NULLIF(btrim(about_description),''),
                   NULLIF(btrim(summary),'')
                 ) IS NULL
               )
             )
           THEN $16::text ELSE description_status END,
         joined_date_text=CASE
           WHEN about_last_observed_at IS NULL OR about_last_observed_at <= $2::timestamptz
           THEN COALESCE($17,joined_date_text) ELSE joined_date_text END,
         joined_at=CASE
           WHEN (about_last_observed_at IS NULL OR about_last_observed_at <= $2::timestamptz) AND $18::date IS NOT NULL
           THEN $18::date ELSE joined_at END,
         joined_at_precision=CASE
           WHEN (about_last_observed_at IS NULL OR about_last_observed_at <= $2::timestamptz) AND $18::date IS NOT NULL
           THEN $19::text ELSE joined_at_precision END,
         external_links=CASE
           WHEN (about_last_observed_at IS NULL OR about_last_observed_at <= $2::timestamptz)
             AND $21::text='observed' AND $20::jsonb IS NOT NULL
             AND (
               jsonb_array_length($20::jsonb)>0
               OR CASE
                 WHEN external_links IS NULL THEN true
                 WHEN jsonb_typeof(external_links)='array'
                   THEN jsonb_array_length(external_links)=0
                 ELSE false
               END
             )
           THEN $20::jsonb ELSE external_links END,
         external_links_status=CASE
           WHEN (about_last_observed_at IS NULL OR about_last_observed_at <= $2::timestamptz)
             AND $21::text='observed' AND $20::jsonb IS NOT NULL
             AND (
               jsonb_array_length($20::jsonb)>0
               OR CASE
                 WHEN external_links IS NULL THEN true
                 WHEN jsonb_typeof(external_links)='array'
                   THEN jsonb_array_length(external_links)=0
                 ELSE false
               END
             )
           THEN 'observed' ELSE external_links_status END,
         country=CASE
           WHEN about_last_observed_at IS NULL OR about_last_observed_at <= $2::timestamptz
           THEN COALESCE(NULLIF(btrim($22::text),''),country) ELSE country END,
         country_source=CASE
           WHEN (about_last_observed_at IS NULL OR about_last_observed_at <= $2::timestamptz)
             AND NULLIF(btrim($22::text),'') IS NOT NULL
           THEN 'youtube_about' ELSE country_source END,
         youtube_business_email_available=CASE
           WHEN (youtube_business_email_observed_at IS NULL
                 OR youtube_business_email_observed_at <= $2::timestamptz)
             AND $25::text IN ('available','not_available')
           THEN $24::boolean ELSE youtube_business_email_available END,
         youtube_business_email_observed_at=CASE
           WHEN (youtube_business_email_observed_at IS NULL
                 OR youtube_business_email_observed_at <= $2::timestamptz)
             AND $25::text IN ('available','not_available')
           THEN $2::timestamptz ELSE youtube_business_email_observed_at END,
         about_last_observed_at=CASE
           WHEN about_last_observed_at IS NULL OR about_last_observed_at <= $2::timestamptz
           THEN $2::timestamptz ELSE about_last_observed_at END,
         about_current_hash=CASE
           WHEN about_last_observed_at IS NULL OR about_last_observed_at <= $2::timestamptz
           THEN $23::text ELSE about_current_hash END,
         updated_at=now()
     WHERE channel_id=$1`,
    [
      command.channelId,
      command.observedAt,
      subscriber.value,
      subscriber.text,
      subscriber.status,
      subscriber.source,
      views.value,
      views.text,
      views.status,
      views.source,
      videos.value,
      videos.text,
      videos.status,
      videos.source,
      command.current.aboutDescription,
      command.current.descriptionStatus,
      command.current.joinedDateText,
      command.current.joinedAt,
      command.current.joinedAtPrecision,
      command.current.externalLinks == null ? null : JSON.stringify(command.current.externalLinks),
      command.current.externalLinksStatus,
      command.current.country,
      command.factsHash,
      command.current.youtubeBusinessEmailAvailable,
      command.current.youtubeBusinessEmailStatus,
    ],
  );
}

async function updateCurrentIdentity(client, command) {
  const identity = command.current.identity;
  if (identity == null) return;
  await client.query(
    `UPDATE crawler.channels
     SET title=COALESCE($3,title),handle=COALESCE($4,handle),
         avatar_url=COALESCE($5,avatar_url),
         keywords=CASE
           WHEN $14='observed' AND (
             COALESCE(cardinality($6::text[]),0)>0 OR COALESCE(cardinality(keywords),0)=0
           ) THEN $6::text[] ELSE keywords END,
         keywords_status=CASE
           WHEN $14='observed' AND (
             COALESCE(cardinality($6::text[]),0)>0 OR COALESCE(cardinality(keywords),0)=0
           ) THEN 'observed' ELSE keywords_status END,
         available_tabs=CASE
           WHEN $15='observed' AND (
             COALESCE(cardinality($7::text[]),0)>0 OR COALESCE(cardinality(available_tabs),0)=0
           ) THEN $7::text[] ELSE available_tabs END,
         available_tabs_status=CASE
           WHEN $15='observed' AND (
             COALESCE(cardinality($7::text[]),0)>0 OR COALESCE(cardinality(available_tabs),0)=0
           ) THEN 'observed' ELSE available_tabs_status END,
         summary=CASE
           WHEN $16::text='exact' THEN $8::text
           WHEN $16::text='empty'
             AND COALESCE(
               NULLIF(btrim(about_description),''),
               NULLIF(btrim(summary),'')
             ) IS NULL
             THEN $8::text
           ELSE summary
         END,
         rss_url=COALESCE($9,rss_url),
         vanity_channel_url=COALESCE($10,vanity_channel_url),
         is_family_safe=COALESCE($11::boolean,is_family_safe),
         is_verified=CASE WHEN $13::text IN ('verified','not_verified') THEN $12::boolean ELSE is_verified END,
         is_verified_status=CASE WHEN $13::text IN ('verified','not_verified') THEN $13::text ELSE is_verified_status END,
         about_identity_last_observed_at=$2::timestamptz,
         about_identity_current_hash=$17::text,updated_at=now()
     WHERE channel_id=$1
       AND (about_identity_last_observed_at IS NULL
         OR about_identity_last_observed_at<=$2::timestamptz)`,
    [
      command.channelId,
      command.observedAt,
      identity.title,
      identity.handle,
      identity.avatar_url,
      identity.keywords,
      identity.available_tabs,
      identity.summary,
      command.current.rssUrl,
      command.current.vanityChannelUrl,
      command.current.isFamilySafe,
      command.current.isVerified,
      command.current.isVerifiedStatus,
      command.current.keywordsStatus,
      command.current.availableTabsStatus,
      command.current.descriptionStatus,
      canonicalHash({
        identity,
        description_status: command.current.descriptionStatus,
        keywords_status: command.current.keywordsStatus,
        available_tabs_status: command.current.availableTabsStatus,
        rss_url: command.current.rssUrl,
        vanity_channel_url: command.current.vanityChannelUrl,
        is_family_safe: command.current.isFamilySafe,
        is_verified: command.current.isVerified,
        is_verified_status: command.current.isVerifiedStatus,
      }),
    ],
  );
}

export async function recordAboutObservation(client, input) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const command = normalizeCommand(input);
  await client.query("SAVEPOINT about_observation_writer_guard");
  await client.query("RELEASE SAVEPOINT about_observation_writer_guard");
  const observationId = randomUUID();
  const duplicate = await claimObservationKey(client, command, observationId);
  if (duplicate) {
    if (command.publicationReconcile) {
      await reconcilePublication(client, {
        channelId: command.channelId,
        domains: ["channel"],
        asOf: command.observedAt,
      });
    }
    return duplicate;
  }

  const sequence = await allocateSequence(client, command.channelId);
  await client.query(
    `INSERT INTO crawler.crawl_observations (
       observation_id,observed_at,channel_id,run_id,observation_kind,kind_sequence,
       plan_id,plan_day,trigger_reason,scheduled_at,started_at,finished_at,
       outcome,outcome_reason_code,result_summary_json,facts_hash,crawler_version,
       extractor_versions,error_class,error_message
     ) VALUES (
       $1,$2,$3,$4,'about',$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb,$15,$16,$17::jsonb,$18,$19
     )`,
    [
      observationId,
      command.observedAt,
      command.channelId,
      command.runId,
      sequence,
      command.planId,
      command.planDay,
      command.triggerReason,
      command.scheduledAt,
      command.startedAt,
      command.finishedAt,
      command.outcome,
      command.outcomeReasonCode,
      JSON.stringify({
        resolved_metric_count: command.resolvedMetricCount,
        snapshot_written: command.snapshotEligible,
        channel_current: {
          description_status: command.current.descriptionStatus,
          joined_date_status: command.current.joinedAt ? "exact" : "unresolved",
          external_links_status: command.current.externalLinksStatus,
          keywords_status: command.current.keywordsStatus,
          available_tabs_status: command.current.availableTabsStatus,
          is_verified_status: command.current.isVerifiedStatus,
          youtube_business_email_status: command.current.youtubeBusinessEmailStatus,
          ...command.current.normalization,
        },
      }),
      command.factsHash,
      command.crawlerVersion,
      JSON.stringify(command.extractorVersions),
      command.errorClass,
      command.errorMessage,
    ],
  );

  await updateCurrentAbout(client, command);
  await updateCurrentIdentity(client, command);
  await collectObservedQueryTerms(client, {
    qualityBatchId: `metadata:${observationId}`,
    observationId,
    channelId: command.channelId,
    sources: [{
      kind: "channel_keyword",
      values: command.current.identity?.keywords ?? [],
    }],
  });
  if (command.snapshotEligible) {
    await client.query(
      `INSERT INTO crawler.channel_about_metric_snapshots (
         observation_id,observed_at,channel_id,
         subscriber_count,total_view_count,total_video_count,
         subscriber_count_status,total_view_count_status,total_video_count_status,facts_hash
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [
        observationId,
        command.observedAt,
        command.channelId,
        command.metrics.subscriber_count.value,
        command.metrics.total_view_count.value,
        command.metrics.total_video_count.value,
        command.metrics.subscriber_count.status,
        command.metrics.total_view_count.status,
        command.metrics.total_video_count.status,
        command.factsHash,
      ],
    );
  }

  await client.query(
    `UPDATE crawler.channel_domain_cursors
     SET latest_sequence=$3,
         latest_observation_id=$4,
         latest_observed_at=$2,
         latest_complete_observation_id=CASE WHEN $5='complete' THEN $4 ELSE latest_complete_observation_id END,
         latest_complete_observed_at=CASE WHEN $5='complete' THEN $2 ELSE latest_complete_observed_at END,
         consecutive_failures=CASE WHEN $5='failed' THEN consecutive_failures+1 ELSE 0 END,
         current_facts_hash=CASE WHEN $5='failed' THEN current_facts_hash ELSE $6 END,
         updated_at=now()
     WHERE channel_id=$1 AND observation_kind='about'`,
    [command.channelId, command.observedAt, sequence, observationId, command.outcome, command.factsHash],
  );

  const eventId = randomUUID();
  const event = {
    event_id: eventId,
    event_type: "crawler.observation.recorded",
    event_version: 1,
    observation_id: observationId,
    plan_id: command.planId,
    channel_id: command.channelId,
    observation_kind: "about",
    kind_sequence: Number(sequence),
    observed_at: command.observedAt,
    outcome: command.outcome,
    crawler_version: command.crawlerVersion,
    payload_hash: command.factsHash,
    payload: metricFacts(command.metrics),
  };
  await client.query(
    `INSERT INTO crawler.crawler_outbox (
       event_id,observation_id,observed_at,event_type,event_version,
       aggregate_key,kind_sequence,payload_json,payload_hash
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9)`,
    [
      eventId,
      observationId,
      command.observedAt,
      event.event_type,
      event.event_version,
      `${command.channelId}:about`,
      sequence,
      JSON.stringify(event),
      command.factsHash,
    ],
  );

  if (command.publicationReconcile) {
    await reconcilePublication(client, {
      channelId: command.channelId,
      domains: ["channel"],
      asOf: command.observedAt,
    });
  }

  return {
    duplicate: false,
    idempotency_key: command.idempotencyKey,
    observation_id: observationId,
    event_id: eventId,
    kind_sequence: Number(sequence),
    outcome: command.outcome,
    snapshot_written: command.snapshotEligible,
  };
}
