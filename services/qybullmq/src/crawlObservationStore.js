import { createHash, randomUUID } from "node:crypto";
import {
  collectObservedQueryTerms,
  queryMetadataCollectionEnabled,
} from "./queryCollector.js";

const OBSERVATION_KINDS = new Set([
  "about",
  "video",
  "agent",
]);
const OUTCOMES = new Set(["complete", "partial", "failed"]);

export class CrawlObservationIdempotencyConflict extends Error {
  constructor(idempotencyKey) {
    super(`idempotency key already exists with different command: ${idempotencyKey}`);
    this.name = "CrawlObservationIdempotencyConflict";
    this.idempotencyKey = idempotencyKey;
  }
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function optionalText(value) {
  if (value === null || value === undefined) return null;
  const output = String(value).trim();
  return output || null;
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

export function canonicalizeObservationValue(value) {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(canonicalizeObservationValue);
  return Object.fromEntries(
    Object.keys(value).sort()
      .filter((key) => value[key] !== undefined)
      .map((key) => [key, canonicalizeObservationValue(value[key])]),
  );
}

export function observationFactsHash(value) {
  const body = JSON.stringify(canonicalizeObservationValue(value));
  return `sha256:${createHash("sha256").update(body).digest("hex")}`;
}

function normalizeCommand(input) {
  const observationKind = requiredText(input?.observationKind, "observationKind");
  if (!OBSERVATION_KINDS.has(observationKind)) {
    throw new TypeError(`unsupported generic observation kind: ${observationKind}`);
  }
  if (typeof input?.prepare !== "function") throw new TypeError("prepare is required");
  const command = {
    idempotencyKey: requiredText(input.idempotencyKey, "idempotencyKey"),
    observationKind,
    channelId: requiredText(input.channelId, "channelId"),
    runId: optionalText(input.runId),
    observedAt: timestamp(input.observedAt, "observedAt", { required: true }),
    planId: optionalText(input.planId),
    planDay: optionalText(input.planDay),
    triggerReason: requiredText(input.triggerReason, "triggerReason"),
    scheduledAt: timestamp(input.scheduledAt, "scheduledAt"),
    startedAt: timestamp(input.startedAt, "startedAt"),
    finishedAt: timestamp(input.finishedAt, "finishedAt"),
    crawlerVersion: optionalText(input.crawlerVersion),
    extractorVersions: canonicalizeObservationValue(input.extractorVersions ?? {}),
    command: canonicalizeObservationValue(input.command ?? {}),
    prepare: input.prepare,
  };
  command.commandHash = observationFactsHash({
    observation_kind: command.observationKind,
    channel_id: command.channelId,
    run_id: command.runId,
    observed_at: command.observedAt,
    plan_id: command.planId,
    plan_day: command.planDay,
    trigger_reason: command.triggerReason,
    scheduled_at: command.scheduledAt,
    started_at: command.startedAt,
    finished_at: command.finishedAt,
    crawler_version: command.crawlerVersion,
    extractor_versions: command.extractorVersions,
    command: command.command,
  });
  return command;
}

async function duplicateResult(client, observationId, idempotencyKey) {
  const result = await client.query(
    `SELECT observation.kind_sequence,observation.outcome,outbox.event_id
     FROM crawler.crawl_observations observation
     LEFT JOIN crawler.crawler_outbox outbox
       ON outbox.observation_id=observation.observation_id
     WHERE observation.observation_id=$1`,
    [observationId],
  );
  const row = result.rows[0] ?? {};
  return {
    duplicate: true,
    idempotency_key: idempotencyKey,
    observation_id: observationId,
    event_id: row.event_id ?? null,
    kind_sequence: row.kind_sequence == null ? null : Number(row.kind_sequence),
    outcome: row.outcome ?? null,
  };
}

async function claimKey(client, command, observationId) {
  const claimed = await client.query(
    `INSERT INTO crawler.crawl_observation_keys (
       idempotency_key,observation_id,command_hash,observed_at,channel_id,
       observation_kind,expires_at
     ) VALUES ($1,$2,$3,$4,$5,$6,now() + interval '90 days')
     ON CONFLICT (idempotency_key) DO NOTHING
     RETURNING observation_id`,
    [
      command.idempotencyKey,
      observationId,
      command.commandHash,
      command.observedAt,
      command.channelId,
      command.observationKind,
    ],
  );
  if (claimed.rowCount === 1) return null;
  const existing = await client.query(
    `SELECT observation_id,command_hash
     FROM crawler.crawl_observation_keys
     WHERE idempotency_key=$1
     FOR UPDATE`,
    [command.idempotencyKey],
  );
  const row = existing.rows[0];
  if (!row || row.command_hash !== command.commandHash) {
    throw new CrawlObservationIdempotencyConflict(command.idempotencyKey);
  }
  return duplicateResult(client, row.observation_id, command.idempotencyKey);
}

async function lockCursor(client, command) {
  await client.query(
    `INSERT INTO crawler.channel_domain_cursors (channel_id,observation_kind)
     VALUES ($1,$2)
     ON CONFLICT (channel_id,observation_kind) DO NOTHING`,
    [command.channelId, command.observationKind],
  );
  const cursor = await client.query(
    `SELECT * FROM crawler.channel_domain_cursors
     WHERE channel_id=$1 AND observation_kind=$2
     FOR UPDATE`,
    [command.channelId, command.observationKind],
  );
  return cursor.rows[0];
}

function normalizePrepared(value) {
  const outcome = requiredText(value?.outcome, "prepare.outcome");
  if (!OUTCOMES.has(outcome)) throw new TypeError(`invalid observation outcome: ${outcome}`);
  const payload = canonicalizeObservationValue(value?.payload ?? {});
  const factsHash = observationFactsHash(payload);
  const suppliedHash = optionalText(value?.factsHash);
  if (suppliedHash && suppliedHash !== factsHash) {
    throw new TypeError("prepare.factsHash does not match prepare.payload");
  }
  const anchors = value?.anchorVideoIds == null
    ? null
    : [...new Set(value.anchorVideoIds.map(String).filter(Boolean))].slice(0, 20);
  return {
    outcome,
    outcomeReasonCode: requiredText(value?.outcomeReasonCode, "prepare.outcomeReasonCode"),
    resultSummary: canonicalizeObservationValue(value?.resultSummary ?? {}),
    payload,
    factsHash,
    anchorVideoIds: anchors,
    sourceCursor: value?.sourceCursor == null
      ? null
      : canonicalizeObservationValue(value.sourceCursor),
    errorClass: optionalText(value?.errorClass)?.slice(0, 255) ?? null,
    errorMessage: optionalText(value?.errorMessage)?.slice(0, 2000) ?? null,
    result: value?.result,
  };
}

export async function recordCrawlerObservation(client, input) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const command = normalizeCommand(input);
  const observationId = randomUUID();
  const duplicate = await claimKey(client, command, observationId);
  if (duplicate) return duplicate;
  const cursor = await lockCursor(client, command);
  const sequence = (BigInt(cursor.latest_sequence ?? 0) + 1n).toString();
  const prepared = normalizePrepared(await command.prepare({
    client,
    observationId,
    sequence: Number(sequence),
    cursor,
  }));

  await client.query(
    `INSERT INTO crawler.crawl_observations (
       observation_id,observed_at,channel_id,run_id,observation_kind,kind_sequence,
       plan_id,plan_day,trigger_reason,scheduled_at,started_at,finished_at,
       outcome,outcome_reason_code,result_summary_json,facts_hash,crawler_version,
       extractor_versions,error_class,error_message
     ) VALUES (
       $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16,$17,$18::jsonb,$19,$20
     )`,
    [
      observationId,
      command.observedAt,
      command.channelId,
      command.runId,
      command.observationKind,
      sequence,
      command.planId,
      command.planDay,
      command.triggerReason,
      command.scheduledAt,
      command.startedAt,
      command.finishedAt,
      prepared.outcome,
      prepared.outcomeReasonCode,
      JSON.stringify(prepared.resultSummary),
      prepared.factsHash,
      command.crawlerVersion,
      JSON.stringify(command.extractorVersions),
      prepared.errorClass,
      prepared.errorMessage,
    ],
  );

  if (command.observationKind === "video" && queryMetadataCollectionEnabled()) {
    const contentTerms = await client.query(
      `SELECT keywords,hashtags
       FROM crawler.contents
       WHERE channel_id=$1 AND last_observation_id=$2
       ORDER BY content_key`,
      [command.channelId, observationId],
    );
    await collectObservedQueryTerms(client, {
      qualityBatchId: `metadata:${observationId}`,
      observationId,
      channelId: command.channelId,
      sources: [
        {
          kind: "video_keyword",
          values: contentTerms.rows.flatMap((row) => row.keywords ?? []),
        },
        {
          kind: "video_hashtag",
          values: contentTerms.rows.flatMap((row) => row.hashtags ?? []),
        },
      ],
    });
  }

  await client.query(
    `UPDATE crawler.channel_domain_cursors
     SET latest_sequence=$3,
         latest_observation_id=$4,
         latest_observed_at=$2,
         latest_complete_observation_id=CASE
           WHEN $5='complete' THEN $4 ELSE latest_complete_observation_id END,
         latest_complete_observed_at=CASE
           WHEN $5='complete' THEN $2 ELSE latest_complete_observed_at END,
         consecutive_failures=CASE WHEN $5='failed' THEN consecutive_failures+1 ELSE 0 END,
         anchor_video_ids=CASE WHEN $7::text[] IS NULL THEN anchor_video_ids ELSE $7::text[] END,
         source_cursor=CASE WHEN $8::jsonb IS NULL THEN source_cursor ELSE $8::jsonb END,
         current_facts_hash=CASE WHEN $5='failed' THEN current_facts_hash ELSE $6 END,
         updated_at=now()
     WHERE channel_id=$1 AND observation_kind=$9`,
    [
      command.channelId,
      command.observedAt,
      sequence,
      observationId,
      prepared.outcome,
      prepared.factsHash,
      prepared.anchorVideoIds,
      prepared.sourceCursor == null ? null : JSON.stringify(prepared.sourceCursor),
      command.observationKind,
    ],
  );

  const eventId = randomUUID();
  const event = {
    event_id: eventId,
    event_type: "crawler.observation.recorded",
    event_version: 1,
    observation_id: observationId,
    plan_id: command.planId,
    channel_id: command.channelId,
    observation_kind: command.observationKind,
    kind_sequence: Number(sequence),
    observed_at: command.observedAt,
    outcome: prepared.outcome,
    crawler_version: command.crawlerVersion,
    payload_hash: prepared.factsHash,
    payload: prepared.payload,
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
      `${command.channelId}:${command.observationKind}`,
      sequence,
      JSON.stringify(event),
      prepared.factsHash,
    ],
  );
  return {
    duplicate: false,
    idempotency_key: command.idempotencyKey,
    observation_id: observationId,
    event_id: eventId,
    kind_sequence: Number(sequence),
    outcome: prepared.outcome,
    payload_hash: prepared.factsHash,
    result: prepared.result,
  };
}
