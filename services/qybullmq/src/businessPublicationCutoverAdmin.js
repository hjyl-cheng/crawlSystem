import { readFile } from "node:fs/promises";
import { observationFactsHash } from "./crawlObservationStore.js";
import { projectBusinessPublicationChannels } from "./businessPublicationProjector.js";
import { environmentValue } from "./runtimeEnvironment.js";

export const BUSINESS_PROJECTION_CUTOVER_EVIDENCE_FORMAT =
  "business-publication-projection-cutover-evidence-v1";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function expectedCount(environment, name) {
  const raw = requiredText(environment[name], name);
  if (!/^(0|[1-9][0-9]*)$/.test(raw)) {
    throw new TypeError(`${name} must be an explicit non-negative integer`);
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value)) throw new TypeError(`${name} is too large`);
  return value;
}

function integerSetting(environment, name, fallback, { minimum, maximum }) {
  const raw = String(environment[name] ?? "").trim();
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new TypeError(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

function uniqueSorted(channelIds) {
  if (!Array.isArray(channelIds) || channelIds.length === 0) {
    throw new TypeError("Cutover Channel set must be a non-empty array");
  }
  const values = channelIds.map((value) => requiredText(value, "Channel ID"));
  const unique = [...new Set(values)].sort((left, right) => (
    Buffer.compare(Buffer.from(left, "utf8"), Buffer.from(right, "utf8"))
  ));
  if (unique.length !== values.length) throw new TypeError("Cutover Channel set contains duplicates");
  return unique;
}

function chunks(values, size) {
  const output = [];
  for (let index = 0; index < values.length; index += size) {
    output.push(values.slice(index, index + size));
  }
  return output;
}

function evidenceBody(evidence) {
  const { evidence_hash: ignored, ...body } = evidence;
  return body;
}

export function businessPublicationCutoverConfig(
  environment = process.env,
  { apply = false, rollback = false } = {},
) {
  const streamId = requiredText(environment.PUBLICATION_STREAM_ID, "PUBLICATION_STREAM_ID");
  if (!UUID.test(streamId)) throw new TypeError("PUBLICATION_STREAM_ID must be a UUID");
  return {
    databaseUrl: environmentValue("BUSINESS_DATABASE_URL", { environment }),
    expectedDatabase: requiredText(environment.EXPECTED_BUSINESS_DATABASE, "EXPECTED_BUSINESS_DATABASE"),
    expectedBusinessChannelCount: expectedCount(environment, "EXPECTED_BUSINESS_CHANNEL_COUNT"),
    streamId,
    channelIdsFile: requiredText(environment.PUBLICATION_CHANNEL_IDS_FILE, "PUBLICATION_CHANNEL_IDS_FILE"),
    operator: requiredText(environment.PUBLICATION_OPERATOR, "PUBLICATION_OPERATOR"),
    reason: requiredText(environment.PUBLICATION_ACTION_REASON, "PUBLICATION_ACTION_REASON"),
    batchSize: integerSetting(environment, "BUSINESS_PUBLICATION_CUTOVER_DRY_RUN_BATCH_SIZE", 25, {
      minimum: 1,
      maximum: 100,
    }),
    evidenceFile: apply || rollback
      ? requiredText(
        environment.BUSINESS_PUBLICATION_CUTOVER_EVIDENCE_FILE,
        "BUSINESS_PUBLICATION_CUTOVER_EVIDENCE_FILE",
      )
      : null,
  };
}

function businessPublicationCutoverId(evidence) {
  return `projection_cutover_${evidence.evidence_hash.slice("sha256:".length, "sha256:".length + 32)}`;
}

export function buildBusinessPublicationCutoverTarget(config, channelIdsValue) {
  const channelIds = uniqueSorted(channelIdsValue);
  return {
    publication_stream_id: config.streamId,
    channel_ids: channelIds,
    channel_count: channelIds.length,
    cohort_hash: observationFactsHash(channelIds),
  };
}

async function inspectState(client, config, target, { requireHeld = true } = {}) {
  const identity = (await client.query(
    `SELECT current_database() AS database_name,
            to_regclass('publication.projection_batch') IS NOT NULL AS projection_schema_ready,
            to_regprocedure('public.refresh_creator_search_release_v9(text,text[],text[])')
              IS NOT NULL AS search_release_ready,
            to_regprocedure('public.restore_creator_search_live_from_legacy_v1(text)')
              IS NOT NULL AS search_legacy_restore_ready,
            to_regclass('public.creator_search_live') IS NOT NULL AS search_live_ready,
            (SELECT count(*)::int FROM public.channels) AS public_channel_count,
            (SELECT watermark FROM public.creator_search_active WHERE singleton=true)
              AS active_watermark,
            (SELECT status FROM publication.stream WHERE publication_stream_id=$1::uuid)
              AS stream_status`,
    [target.publication_stream_id],
  )).rows[0] ?? {};
  if (identity.database_name !== config.expectedDatabase) {
    throw new Error(`Business database mismatch: ${identity.database_name}`);
  }
  if (Number(identity.public_channel_count) !== config.expectedBusinessChannelCount) {
    throw new Error(`Business Channel count changed: ${identity.public_channel_count}`);
  }
  if (!identity.projection_schema_ready || !identity.search_release_ready
      || !identity.search_legacy_restore_ready || !identity.search_live_ready) {
    throw new Error("Business Projection schema is not ready");
  }
  if (identity.stream_status !== "active") throw new Error("Publication Stream is not active");

  const owners = (await client.query(
    `SELECT channel_id,active_publication_stream_id,status,projection_mode,
            ownership_reference
     FROM publication.channel_ownership
     WHERE channel_id=ANY($1::text[])
     ORDER BY channel_id`,
    [target.channel_ids],
  )).rows;
  if (owners.length !== target.channel_count) {
    const found = new Set(owners.map((row) => row.channel_id));
    throw new Error(
      `Business ownership missing for Channels: ${target.channel_ids.filter((id) => !found.has(id)).join(",")}`,
    );
  }
  for (const owner of owners) {
    if (String(owner.active_publication_stream_id) !== target.publication_stream_id) {
      throw new Error(`Business ownership Stream differs: ${owner.channel_id}`);
    }
    if (owner.status !== "active") throw new Error(`Business ownership is not active: ${owner.channel_id}`);
    if (requireHeld && owner.projection_mode !== "held_shadow") {
      throw new Error(`Business ownership is not held_shadow: ${owner.channel_id}`);
    }
  }

  const cursors = (await client.query(
    `SELECT channel_id,domain,publication_stream_id,active_sequence,
            active_revision_id,active_result_hash
     FROM publication.consumer_cursor
     WHERE channel_id=ANY($1::text[])
     ORDER BY channel_id,domain`,
    [target.channel_ids],
  )).rows;
  const outbox = (await client.query(
    `SELECT projection_id,activation_id,publication_stream_id,channel_id,
            version_vector,status,attempts
     FROM publication.projection_outbox
     WHERE channel_id=ANY($1::text[])
     ORDER BY channel_id,created_at,projection_id`,
    [target.channel_ids],
  )).rows;
  if (requireHeld) {
    const unexpected = outbox.filter((row) => row.status !== "held_shadow");
    if (unexpected.length > 0) {
      throw new Error(`Projection Outbox is not fully held_shadow: ${JSON.stringify(unexpected.slice(0, 10))}`);
    }
  }

  const coverage = (await client.query(
    `WITH target AS (
       SELECT channel_id FROM unnest($1::text[]) channel_id
     ), public_current AS (
       SELECT search.channel_id,search.snapshot_id
       FROM public.creator_search_live search
       JOIN target USING(channel_id)
     )
     SELECT target.channel_id,
            public_current.snapshot_id,
            entity.active_sequence AS channel_sequence,entity.result_hash AS channel_hash,
            entity.is_retracted AS channel_retracted,
            video.active_sequence AS video_sequence,video.result_hash AS video_hash,
            agent.active_sequence AS agent_sequence,agent.result_hash AS agent_hash,
            agent.is_retracted AS agent_retracted,
            CASE
              WHEN entity.channel_id IS NULL AND public_current.snapshot_id IS NULL THEN false
              WHEN video.channel_id IS NULL AND public_current.snapshot_id IS NULL THEN false
              WHEN agent.channel_id IS NULL AND public_current.snapshot_id IS NULL THEN false
              ELSE true
            END AS projection_ready
     FROM target
     LEFT JOIN public_current USING(channel_id)
     LEFT JOIN result.entity_current entity USING(channel_id)
     LEFT JOIN result.video_current video USING(channel_id)
     LEFT JOIN result.agent_current agent USING(channel_id)
     ORDER BY target.channel_id`,
    [target.channel_ids],
  )).rows;
  const incomplete = coverage.filter((row) => !row.projection_ready);
  if (incomplete.length > 0) {
    throw new Error(
      `Projection cannot build a complete aggregate for Channels: ${incomplete.map((row) => row.channel_id).join(",")}`,
    );
  }

  const issues = (await client.query(
    `SELECT
       (SELECT count(*)::int
        FROM publication.quarantine quarantine
        JOIN publication.inbox inbox USING(revision_id)
        WHERE quarantine.status='open' AND inbox.channel_id=ANY($1::text[])) AS open_quarantine,
       (SELECT count(*)::int
        FROM publication.inbox_conflict conflict
        JOIN publication.inbox inbox USING(revision_id)
        WHERE inbox.channel_id=ANY($1::text[])) AS inbox_conflicts,
       (SELECT count(*)::int
        FROM publication.inbox inbox
        LEFT JOIN publication.revision revision USING(revision_id)
        WHERE inbox.channel_id=ANY($1::text[])
          AND inbox.receive_status='waiting_ownership'
          AND (revision.revision_id IS NULL OR revision.activation_status='waiting_ownership'))
          AS waiting_ownership,
       (SELECT count(*)::int FROM publication.revision
        WHERE channel_id=ANY($1::text[])
          AND activation_status IN ('staged','waiting_gap','waiting_ownership'))
          AS pending_activation`,
    [target.channel_ids],
  )).rows[0];
  const blockingIssues = Object.entries(issues).filter(([, value]) => Number(value) !== 0);
  if (blockingIssues.length > 0) {
    throw new Error(`Business Publication has unresolved issues: ${JSON.stringify(issues)}`);
  }

  const state = {
    database_name: identity.database_name,
    public_channel_count: Number(identity.public_channel_count),
    active_watermark: identity.active_watermark,
    stream_status: identity.stream_status,
    owners,
    cursors,
    projection_outbox: outbox,
    coverage,
    issues: Object.fromEntries(Object.entries(issues).map(([key, value]) => [key, Number(value)])),
  };
  return {
    state,
    state_hash: observationFactsHash(state),
    summary: {
      owner_count: owners.length,
      cursor_count: cursors.length,
      held_projection_count: outbox.filter((row) => row.status === "held_shadow").length,
      public_baseline_count: coverage.filter((row) => row.snapshot_id).length,
      new_channel_count: coverage.filter((row) => !row.snapshot_id).length,
      current_domain_counts: {
        channel: coverage.filter((row) => row.channel_sequence != null).length,
        video: coverage.filter((row) => row.video_sequence != null).length,
        agent: coverage.filter((row) => row.agent_sequence != null).length,
      },
      issues: state.issues,
    },
  };
}

async function dryRunProjection(client, config, target, state) {
  const beforeTargetOnly = Number((await client.query(
    `SELECT count(*)::int AS count
     FROM public.creator_search_live search
     WHERE NOT (search.channel_id=ANY($1::text[]))`,
    [target.channel_ids],
  )).rows[0].count);
  const batches = [];
  for (const channelBatch of chunks(target.channel_ids, config.batchSize)) {
    batches.push(await projectBusinessPublicationChannels(client, channelBatch, {
      projectionStatuses: ["held_shadow"],
      markOutbox: false,
      lock: false,
    }));
  }
  const verification = (await client.query(
    `WITH target AS (SELECT channel_id FROM unnest($1::text[]) channel_id),
     current_release AS (
       SELECT search.channel_id,search.snapshot_id
       FROM public.creator_search_live search
     )
     SELECT
       (SELECT watermark FROM public.creator_search_active WHERE singleton=true)
         AS preview_watermark,
       (SELECT count(*)::int FROM public.channels) AS preview_public_channel_count,
       (SELECT count(*)::int FROM current_release JOIN target USING(channel_id))
         AS visible_target_count,
       (SELECT count(*)::int FROM current_release
        WHERE NOT (channel_id=ANY($1::text[]))) AS target_only_preserved_count,
       (SELECT count(*)::int
        FROM current_release release
        JOIN target ON target.channel_id=release.channel_id
        JOIN public.channel_snapshots snapshot
          ON snapshot.id=release.snapshot_id AND snapshot.channel_id=release.channel_id
        WHERE NULLIF(btrim(snapshot.title),'') IS NULL) AS invalid_title_count`,
    [target.channel_ids],
  )).rows[0];
  const removedCount = batches.reduce(
    (sum, batch) => sum + batch.removed_channel_ids.length,
    0,
  );
  const expectedVisible = target.channel_count - removedCount;
  if (Number(verification.visible_target_count) !== expectedVisible) {
    throw new Error(
      `Projection dry-run visible count differs: ${verification.visible_target_count} != ${expectedVisible}`,
    );
  }
  if (Number(verification.target_only_preserved_count) !== beforeTargetOnly) {
    throw new Error("Projection dry-run did not preserve Target-only Channels");
  }
  if (Number(verification.invalid_title_count) !== 0) {
    throw new Error("Projection dry-run produced invalid Channel titles");
  }
  return {
    transaction_outcome: "rolled_back",
    batch_count: batches.length,
    projected_channel_count: batches.reduce((sum, batch) => sum + batch.projected, 0),
    upsert_channel_count: batches.reduce((sum, batch) => sum + batch.upsert_channel_ids.length, 0),
    removed_channel_count: removedCount,
    target_only_preserved_count: beforeTargetOnly,
    initial_watermark: state.active_watermark,
    final_preview_watermark: verification.preview_watermark,
    final_preview_public_channel_count: Number(verification.preview_public_channel_count),
    row_counts: batches.reduce((totals, batch) => {
      for (const [key, value] of Object.entries(batch.row_counts)) {
        totals[key] = (totals[key] ?? 0) + Number(value);
      }
      return totals;
    }, {}),
    batches: batches.map((batch) => ({
      batch_id: batch.batch_id,
      source_sha256: batch.source_sha256,
      projected: batch.projected,
      upsert_count: batch.upsert_channel_ids.length,
      removed_count: batch.removed_channel_ids.length,
    })),
  };
}

export function businessPublicationCutoverConfirmation(config, target, evidence) {
  return [
    "APPLY_BUSINESS_PUBLICATION_PROJECTION_CUTOVER",
    config.expectedDatabase,
    target.publication_stream_id,
    target.channel_count,
    target.cohort_hash,
    evidence.evidence_hash,
  ].join(":");
}

export function businessPublicationCutoverRollbackConfirmation(config, target, evidence) {
  return [
    "ROLLBACK_BUSINESS_PUBLICATION_PROJECTION_CUTOVER",
    config.expectedDatabase,
    target.publication_stream_id,
    target.channel_count,
    target.cohort_hash,
    businessPublicationCutoverId(evidence),
  ].join(":");
}

async function inspectRollbackState(client, config, target, evidence, { lock = false } = {}) {
  const cutoverId = businessPublicationCutoverId(evidence);
  const identity = (await client.query(
    `SELECT current_database() AS database_name,
            (SELECT count(*)::int FROM public.channels) AS public_channel_count`,
  )).rows[0] ?? {};
  if (identity.database_name !== config.expectedDatabase) {
    throw new Error(`Business database mismatch: ${identity.database_name}`);
  }
  const minimumChannelCount = Number(evidence.state.public_channel_count);
  const maximumChannelCount = Number(evidence.dry_run.final_preview_public_channel_count);
  if (
    Number(identity.public_channel_count) < minimumChannelCount
      || Number(identity.public_channel_count) > maximumChannelCount
  ) {
    throw new Error(`Business Channel count changed: ${identity.public_channel_count}`);
  }
  const cutover = (await client.query(
    `SELECT cutover_id,publication_stream_id,evidence_sha256,cohort_hash,
            channel_ids,owner_count,released_projection_count,previous_watermark,
            first_projection_watermark,last_projection_watermark,status,applied_at,
            rolled_back_at
     FROM publication.projection_cutover WHERE cutover_id=$1`,
    [cutoverId],
  )).rows[0];
  if (!cutover) throw new Error(`Business Projection Cutover does not exist: ${cutoverId}`);
  if (
    String(cutover.publication_stream_id) !== target.publication_stream_id
      || cutover.evidence_sha256 !== evidence.evidence_hash
      || cutover.cohort_hash !== target.cohort_hash
      || Number(cutover.owner_count) !== target.channel_count
      || observationFactsHash(uniqueSorted(cutover.channel_ids))
        !== observationFactsHash(target.channel_ids)
  ) {
    throw new Error("Persisted Business Projection Cutover differs from its approved evidence");
  }

  const owners = (await client.query(
    `SELECT channel_id,status,projection_mode,active_publication_stream_id
     FROM publication.channel_ownership
     WHERE channel_id=ANY($1::text[])
     ORDER BY channel_id ${lock ? "FOR UPDATE" : ""}`,
    [target.channel_ids],
  )).rows;
  if (owners.length !== target.channel_count) {
    throw new Error("Business Projection rollback ownership set is incomplete");
  }
  const expectedMode = cutover.status === "rolled_back" ? "held_shadow" : "online";
  for (const owner of owners) {
    if (
      owner.status !== "active"
        || owner.projection_mode !== expectedMode
        || String(owner.active_publication_stream_id) !== target.publication_stream_id
    ) {
      throw new Error(`Business Projection rollback ownership differs: ${owner.channel_id}`);
    }
  }

  const outboxStatuses = (await client.query(
    `SELECT status,count(*)::int AS count,max(attempts)::int AS maximum_attempts
     FROM publication.projection_outbox
     WHERE released_by_cutover_id=$1
     GROUP BY status ORDER BY status`,
    [cutoverId],
  )).rows.map((row) => ({
    status: row.status,
    count: Number(row.count),
    maximum_attempts: Number(row.maximum_attempts),
  }));
  const releasedCount = outboxStatuses.reduce((sum, row) => sum + row.count, 0);
  if (releasedCount !== Number(cutover.released_projection_count)) {
    throw new Error("Business Projection rollback Outbox set differs from the applied Cutover");
  }
  if (
    cutover.status === "rolled_back"
      && outboxStatuses.some((row) => row.status !== "held_shadow")
  ) {
    throw new Error("Rolled-back Business Projection has non-held Outbox rows");
  }

  const laterState = (await client.query(
    `SELECT
       (SELECT count(*)::int
        FROM publication.projection_outbox projection
        WHERE projection.channel_id=ANY($1::text[])
          AND projection.created_at >= $2
          AND projection.released_by_cutover_id IS DISTINCT FROM $3) AS newer_outbox,
       (SELECT count(*)::int
        FROM publication.projection_cutover later
        WHERE later.cutover_id<>$3 AND later.status='applied'
          AND later.channel_ids && $1::text[]
          AND later.applied_at > (
            SELECT current_cutover.applied_at
            FROM publication.projection_cutover current_cutover
            WHERE current_cutover.cutover_id=$3
          )) AS overlapping_cutover,
       (SELECT watermark FROM public.creator_search_active WHERE singleton=true)
         AS active_watermark,
       (SELECT status FROM public.creator_search_releases WHERE watermark=$4)
         AS previous_release_status`,
    [target.channel_ids, cutover.applied_at, cutoverId, cutover.previous_watermark],
  )).rows[0];
  if (Number(laterState.newer_outbox) !== 0) {
    throw new Error("Business Projection rollback is blocked by newer Channel revisions");
  }
  if (Number(laterState.overlapping_cutover) !== 0) {
    throw new Error("Business Projection rollback is blocked by a later overlapping Cutover");
  }
  const expectedWatermark = cutover.status === "rolled_back"
    ? cutover.previous_watermark
    : cutover.last_projection_watermark ?? cutover.previous_watermark;
  if (laterState.active_watermark !== expectedWatermark) {
    throw new Error(
      `Business Projection rollback Search watermark differs: ${laterState.active_watermark}`,
    );
  }
  if (!laterState.previous_release_status) {
    throw new Error("Business Projection rollback previous Search release is missing");
  }
  const state = {
    cutover_id: cutoverId,
    cutover_status: cutover.status,
    owner_count: owners.length,
    released_projection_count: releasedCount,
    outbox_statuses: outboxStatuses,
    active_watermark: laterState.active_watermark,
    previous_watermark: cutover.previous_watermark,
    first_projection_watermark: cutover.first_projection_watermark,
    last_projection_watermark: cutover.last_projection_watermark,
    newer_outbox_count: Number(laterState.newer_outbox),
    overlapping_cutover_count: Number(laterState.overlapping_cutover),
  };
  return { cutover, state, state_hash: observationFactsHash(state) };
}

export function businessPublicationCutoverEvidenceFromDocument(document) {
  if (!document || typeof document !== "object" || Array.isArray(document)) {
    throw new Error("Business Projection Cutover evidence document must be an object");
  }
  if (!Object.hasOwn(document, "evidence")) return document;
  if (
    document.ok !== true
      || document.mode !== "plan"
      || document.writes_performed !== false
      || document.dry_run_transaction !== "rolled_back"
      || !document.evidence
      || typeof document.evidence !== "object"
      || Array.isArray(document.evidence)
  ) {
    throw new Error("Business Projection Cutover plan document is not an immutable dry-run");
  }
  return document.evidence;
}

export async function readBusinessPublicationCutoverEvidence(path, config, target) {
  const document = JSON.parse(await readFile(path, "utf8"));
  const evidence = businessPublicationCutoverEvidenceFromDocument(document);
  if (evidence.evidence_format !== BUSINESS_PROJECTION_CUTOVER_EVIDENCE_FORMAT) {
    throw new Error("unsupported Business Projection Cutover evidence format");
  }
  if (evidence.database_name !== config.expectedDatabase) {
    throw new Error("Cutover evidence targets a different Business database");
  }
  if (evidence.target?.cohort_hash !== target.cohort_hash
      || evidence.target?.publication_stream_id !== target.publication_stream_id
      || evidence.target?.channel_count !== target.channel_count) {
    throw new Error("Cutover evidence target differs from the requested Cohort");
  }
  const actualHash = observationFactsHash(evidenceBody(evidence));
  if (actualHash !== evidence.evidence_hash) throw new Error("Cutover evidence hash is invalid");
  if (evidence.dry_run?.transaction_outcome !== "rolled_back"
      || evidence.dry_run?.projected_channel_count !== target.channel_count) {
    throw new Error("Cutover evidence does not contain a complete rolled-back dry-run");
  }
  if (
    !Number.isSafeInteger(evidence.state?.public_channel_count)
      || !Number.isSafeInteger(evidence.dry_run?.final_preview_public_channel_count)
      || evidence.dry_run.final_preview_public_channel_count
        < evidence.state.public_channel_count
  ) {
    throw new Error("Cutover evidence does not contain valid Business Channel count bounds");
  }
  return evidence;
}

export class BusinessPublicationCutoverAdministrator {
  constructor({ pool, config, target, evidence = null }) {
    if (!pool?.connect || !pool?.query) throw new TypeError("a PostgreSQL Pool is required");
    this.pool = pool;
    this.config = config;
    this.target = target;
    this.evidence = evidence;
  }

  async inspectReadOnly() {
    const client = await this.pool.connect();
    let began = false;
    try {
      await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ");
      began = true;
      await client.query("SET LOCAL lock_timeout='10s'");
      await client.query("SET LOCAL statement_timeout='900s'");
      const inspected = await inspectState(client, this.config, this.target);
      const dryRun = await dryRunProjection(client, this.config, this.target, inspected.state);
      await client.query("ROLLBACK");
      began = false;
      const body = {
        evidence_format: BUSINESS_PROJECTION_CUTOVER_EVIDENCE_FORMAT,
        database_name: this.config.expectedDatabase,
        target: this.target,
        state_hash: inspected.state_hash,
        state_summary: inspected.summary,
        state: inspected.state,
        dry_run: dryRun,
        operator: this.config.operator,
        reason: this.config.reason,
        planned_at: new Date().toISOString(),
      };
      const evidence = { ...body, evidence_hash: observationFactsHash(body) };
      return { evidence, summary: { ...inspected.summary, dry_run: dryRun } };
    } catch (error) {
      if (began) await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async apply() {
    if (!this.evidence) throw new Error("Cutover apply requires immutable evidence");
    const cutoverId = businessPublicationCutoverId(this.evidence);
    const client = await this.pool.connect();
    let began = false;
    try {
      await client.query("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
      began = true;
      await client.query("SET LOCAL lock_timeout='10s'");
      await client.query("SET LOCAL statement_timeout='180s'");
      await client.query("SELECT pg_advisory_xact_lock(781137233)");
      const existing = (await client.query(
        `SELECT cutover_id,evidence_sha256,status,owner_count,released_projection_count
         FROM publication.projection_cutover WHERE cutover_id=$1`,
        [cutoverId],
      )).rows[0];
      if (existing) {
        if (existing.evidence_sha256 !== this.evidence.evidence_hash) {
          throw new Error("Cutover ID already exists with different evidence");
        }
        if (existing.status !== "applied") {
          throw new Error("Cutover was rolled back; generate new evidence before applying again");
        }
        await client.query("COMMIT");
        began = false;
        return { outcome: "already_applied", ...existing };
      }

      await client.query(
        `SELECT channel_id FROM publication.channel_ownership
         WHERE channel_id=ANY($1::text[]) ORDER BY channel_id FOR UPDATE`,
        [this.target.channel_ids],
      );
      const inspected = await inspectState(client, this.config, this.target);
      if (inspected.state_hash !== this.evidence.state_hash) {
        throw new Error("Cutover live state changed after the approved dry-run; generate new evidence");
      }
      await client.query(
        `INSERT INTO publication.projection_cutover (
           cutover_id,publication_stream_id,evidence_format,evidence_sha256,
           cohort_hash,channel_ids,owner_count,released_projection_count,
           previous_watermark,status,evidence_json,applied_by,applied_reason,applied_at
         ) VALUES (
           $1,$2::uuid,$3,$4,$5,$6::text[],$7,$8,$9,'applied',$10::jsonb,$11,$12,
           clock_timestamp()
         )`,
        [
          cutoverId,
          this.target.publication_stream_id,
          this.evidence.evidence_format,
          this.evidence.evidence_hash,
          this.target.cohort_hash,
          this.target.channel_ids,
          this.target.channel_count,
          this.evidence.state_summary.held_projection_count,
          this.evidence.state.active_watermark,
          JSON.stringify(this.evidence),
          this.config.operator,
          this.config.reason,
        ],
      );
      const ownership = await client.query(
        `UPDATE publication.channel_ownership
         SET projection_mode='online',state_changed_by=$2,state_reason=$3,
             state_changed_at=now(),updated_at=now()
         WHERE channel_id=ANY($1::text[]) AND projection_mode='held_shadow'
         RETURNING channel_id`,
        [this.target.channel_ids, this.config.operator, this.config.reason],
      );
      if (ownership.rows.length !== this.target.channel_count) {
        throw new Error("Cutover did not switch every approved Business ownership");
      }
      const released = await client.query(
        `UPDATE publication.projection_outbox
         SET status='pending',next_attempt_at=now(),released_by_cutover_id=$2,
             updated_at=now()
         WHERE channel_id=ANY($1::text[]) AND status='held_shadow'
         RETURNING projection_id`,
        [this.target.channel_ids, cutoverId],
      );
      if (released.rows.length !== this.evidence.state_summary.held_projection_count) {
        throw new Error("Cutover released a different Projection Outbox set than the evidence");
      }
      await client.query("COMMIT");
      began = false;
      return {
        outcome: "applied",
        cutover_id: cutoverId,
        owner_count: ownership.rows.length,
        released_projection_count: released.rows.length,
        previous_watermark: this.evidence.state.active_watermark,
      };
    } catch (error) {
      if (began) await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async inspectRollbackReadOnly() {
    if (!this.evidence) throw new Error("Cutover rollback requires immutable evidence");
    const client = await this.pool.connect();
    let began = false;
    try {
      await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
      began = true;
      await client.query("SET LOCAL lock_timeout='10s'");
      await client.query("SET LOCAL statement_timeout='180s'");
      const inspected = await inspectRollbackState(
        client,
        this.config,
        this.target,
        this.evidence,
      );
      await client.query("ROLLBACK");
      began = false;
      return inspected;
    } catch (error) {
      if (began) await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async rollback() {
    if (!this.evidence) throw new Error("Cutover rollback requires immutable evidence");
    const client = await this.pool.connect();
    let began = false;
    try {
      await client.query("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
      began = true;
      await client.query("SET LOCAL lock_timeout='10s'");
      await client.query("SET LOCAL statement_timeout='180s'");
      await client.query("SELECT pg_advisory_xact_lock(781137233)");
      await inspectRollbackState(
        client,
        this.config,
        this.target,
        this.evidence,
        { lock: true },
      );
      await client.query("SELECT pg_advisory_xact_lock(hashtext('kol_demo:creator-search-publish'))");
      const inspected = await inspectRollbackState(
        client,
        this.config,
        this.target,
        this.evidence,
      );
      if (inspected.cutover.status === "rolled_back") {
        await client.query("COMMIT");
        began = false;
        return { outcome: "already_rolled_back", ...inspected.state };
      }

      const ownership = await client.query(
        `UPDATE publication.channel_ownership
         SET projection_mode='held_shadow',state_changed_by=$2,state_reason=$3,
             state_changed_at=now(),updated_at=now()
         WHERE channel_id=ANY($1::text[]) AND projection_mode='online'
         RETURNING channel_id`,
        [this.target.channel_ids, this.config.operator, this.config.reason],
      );
      if (ownership.rows.length !== this.target.channel_count) {
        throw new Error("Rollback did not hold every approved Business ownership");
      }
      const held = await client.query(
        `UPDATE publication.projection_outbox
         SET status='held_shadow',attempts=0,next_attempt_at=now(),
             lease_owner=NULL,lease_expires_at=NULL,last_error=NULL,
             delivered_at=NULL,updated_at=now()
         WHERE released_by_cutover_id=$1
         RETURNING projection_id`,
        [inspected.cutover.cutover_id],
      );
      if (held.rows.length !== Number(inspected.cutover.released_projection_count)) {
        throw new Error("Rollback did not hold every Projection released by the Cutover");
      }

      if (inspected.state.active_watermark !== inspected.cutover.previous_watermark) {
        const activeRelease = (await client.query(
          `SELECT storage_mode FROM public.creator_search_releases
           WHERE watermark=$1 AND status='active'`,
          [inspected.state.active_watermark],
        )).rows[0];
        if (activeRelease?.storage_mode) {
          await client.query(
            `SELECT public.rollback_creator_search_to_watermark_v1($1,$2,$3)`,
            [
              inspected.cutover.previous_watermark,
              this.config.operator,
              this.config.reason,
            ],
          );
        } else {
          await client.query(
            `SELECT public.restore_creator_search_live_from_legacy_v1($1)`,
            [inspected.cutover.previous_watermark],
          );
          await client.query(
            `UPDATE public.creator_search_releases
             SET status='retired'
             WHERE status='active' AND watermark<>$1`,
            [inspected.cutover.previous_watermark],
          );
          const restored = await client.query(
            `UPDATE public.creator_search_releases
             SET status='active',activated_at=COALESCE(activated_at,clock_timestamp()),
                 generation=generation+1,rebuilt_at=clock_timestamp()
             WHERE watermark=$1 AND status='retired'
             RETURNING watermark`,
            [inspected.cutover.previous_watermark],
          );
          if (restored.rows.length !== 1) {
            throw new Error("Rollback could not restore the previous Search release");
          }
          await client.query(
            `UPDATE public.creator_search_active SET watermark=$1 WHERE singleton=true`,
            [inspected.cutover.previous_watermark],
          );
        }
      }

      const rollbackAudit = {
        state_hash: inspected.state_hash,
        state: inspected.state,
      };
      const updated = await client.query(
        `UPDATE publication.projection_cutover
         SET status='rolled_back',rolled_back_by=$2,rolled_back_reason=$3,
             rolled_back_at=now(),rollback_json=$4::jsonb
         WHERE cutover_id=$1 AND status='applied'
         RETURNING rolled_back_at`,
        [
          inspected.cutover.cutover_id,
          this.config.operator,
          this.config.reason,
          JSON.stringify(rollbackAudit),
        ],
      );
      if (updated.rows.length !== 1) throw new Error("Business Projection Cutover changed during rollback");
      await client.query("COMMIT");
      began = false;
      return {
        outcome: "rolled_back",
        cutover_id: inspected.cutover.cutover_id,
        owner_count: ownership.rows.length,
        held_projection_count: held.rows.length,
        restored_watermark: inspected.cutover.previous_watermark,
        rolled_back_at: updated.rows[0].rolled_back_at,
      };
    } catch (error) {
      if (began) await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }
}
