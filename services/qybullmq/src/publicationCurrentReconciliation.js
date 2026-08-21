import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { isDeepStrictEqual } from "node:util";
import {
  BUSINESS_CHANNEL_PRESERVATION_SQL,
  buildBusinessChannelPreservationBaselines,
} from "./publicationBusinessPreservation.js";
import { environmentValue } from "./runtimeEnvironment.js";
import { lockPublicationChannelMutation } from "./publicationChannelMutationLock.js";
import { reconcilePublication } from "./publicationReconciler.js";
import { PUBLICATION_WRITER_VERSION } from "./publicationWriterVersion.js";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const SAFE_KEY = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const DOMAINS = Object.freeze(["channel", "video", "agent"]);
const DOMAIN_SET = new Set(DOMAINS);
const EVIDENCE_VERSION = "publication-current-reconciliation-evidence-v2";

export class PublicationCurrentReconciliationConflict extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "PublicationCurrentReconciliationConflict";
    this.details = details;
  }
}

export class PublicationCurrentReconciliationPartialFailure extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "PublicationCurrentReconciliationPartialFailure";
    this.details = details;
  }
}

function fail(message, details = {}) {
  throw new PublicationCurrentReconciliationConflict(message, details);
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function optionalText(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function safeKey(environment, name) {
  const value = requiredText(environment[name], name);
  if (!SAFE_KEY.test(value)) {
    throw new TypeError(`${name} must use only letters, numbers, dot, underscore, or hyphen`);
  }
  return value;
}

function explicitCount(environment, name) {
  const value = requiredText(environment[name], name);
  if (!/^(0|[1-9][0-9]*)$/.test(value)) {
    throw new TypeError(`${name} must be an explicit non-negative integer`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new TypeError(`${name} must be an explicit non-negative integer`);
  }
  return parsed;
}

function positiveInteger(value, fallback, field, maximum = 8) {
  if (value == null || String(value).trim() === "") return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > maximum) {
    throw new TypeError(`${field} must be an integer between 1 and ${maximum}`);
  }
  return parsed;
}

function timestamp(value, field) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new TypeError(`${field} must be a timestamp`);
  return parsed.toISOString();
}

function canonicalJson(value) {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(canonicalJson);
  return Object.fromEntries(
    Object.keys(value).sort().map((key) => [key, canonicalJson(value[key])]),
  );
}

function sha256(value) {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

function canonicalHash(value) {
  return sha256(JSON.stringify(canonicalJson(value)));
}

function compareText(left, right) {
  return Buffer.compare(Buffer.from(String(left), "utf8"), Buffer.from(String(right), "utf8"));
}

function uniqueSortedText(values) {
  return [...new Set((Array.isArray(values) ? values : []).map(optionalText).filter(Boolean))]
    .sort(compareText);
}

function parseDomains(value) {
  const requested = String(value ?? "channel").split(",").map(optionalText).filter(Boolean);
  if (requested.length === 0) throw new TypeError("PUBLICATION_RECONCILE_DOMAINS is empty");
  const unknown = requested.filter((domain) => !DOMAIN_SET.has(domain));
  if (unknown.length > 0) {
    throw new TypeError(`unsupported Publication Domain: ${uniqueSortedText(unknown).join(", ")}`);
  }
  return DOMAINS.filter((domain) => requested.includes(domain));
}

export function publicationCurrentReconciliationConfig(
  environment = process.env,
  { apply = false } = {},
) {
  const streamId = requiredText(environment.PUBLICATION_STREAM_ID, "PUBLICATION_STREAM_ID")
    .toLowerCase();
  if (!UUID.test(streamId)) throw new TypeError("PUBLICATION_STREAM_ID must be a UUID");
  const evidenceFile = optionalText(environment.PUBLICATION_CURRENT_RECONCILE_EVIDENCE_FILE);
  if (apply && !evidenceFile) {
    throw new TypeError("PUBLICATION_CURRENT_RECONCILE_EVIDENCE_FILE is required with --apply");
  }
  return {
    crawlerDatabaseUrl: environmentValue("CRAWLER_DATABASE_URL", { environment }),
    expectedCrawlerDatabase: safeKey(environment, "EXPECTED_CRAWLER_DATABASE"),
    expectedCrawlerChannelCount: explicitCount(environment, "EXPECTED_CRAWLER_CHANNEL_COUNT"),
    businessDatabaseUrl: environmentValue("BUSINESS_DATABASE_URL", { environment }),
    expectedBusinessDatabase: safeKey(environment, "EXPECTED_BUSINESS_DATABASE"),
    expectedBusinessChannelCount: explicitCount(environment, "EXPECTED_BUSINESS_CHANNEL_COUNT"),
    streamId,
    destination: safeKey(environment, "PUBLICATION_DESTINATION"),
    reconcileKey: safeKey(environment, "PUBLICATION_CURRENT_RECONCILE_KEY"),
    asOf: timestamp(
      environment.PUBLICATION_CURRENT_RECONCILE_AS_OF,
      "PUBLICATION_CURRENT_RECONCILE_AS_OF",
    ),
    channelIdsFile: requiredText(
      environment.PUBLICATION_CHANNEL_IDS_FILE,
      "PUBLICATION_CHANNEL_IDS_FILE",
    ),
    domains: parseDomains(environment.PUBLICATION_RECONCILE_DOMAINS),
    actor: requiredText(environment.PUBLICATION_OPERATOR, "PUBLICATION_OPERATOR"),
    reason: requiredText(environment.PUBLICATION_ACTION_REASON, "PUBLICATION_ACTION_REASON"),
    concurrency: positiveInteger(
      environment.PUBLICATION_CURRENT_RECONCILE_CONCURRENCY,
      4,
      "PUBLICATION_CURRENT_RECONCILE_CONCURRENCY",
    ),
    evidenceFile,
  };
}

export function buildPublicationCurrentReconciliationTarget(config, channelIdsValue) {
  const channelIds = uniqueSortedText(channelIdsValue);
  if (channelIds.length === 0) throw new TypeError("channelIds must be a non-empty array");
  if (channelIds.length !== channelIdsValue.length) throw new TypeError("channelIds must be unique");
  return {
    reconcile_key: config.reconcileKey,
    publication_stream_id: config.streamId,
    destination: config.destination,
    as_of: config.asOf,
    revision_type: "repair",
    domains: config.domains,
    channel_ids: channelIds,
    channel_count: channelIds.length,
    channel_set_hash: sha256(`${channelIds.join("\n")}\n`),
  };
}

function normalizedReasons(value) {
  return (Array.isArray(value) ? value : [])
    .map((reason) => canonicalJson(reason && typeof reason === "object" ? reason : {
      code: String(reason),
    }))
    .sort((left, right) => compareText(JSON.stringify(left), JSON.stringify(right)));
}

function normalizedDomainResult(value) {
  return {
    domain: requiredText(value?.domain, "reconciliation domain"),
    status: requiredText(value?.status, "reconciliation domain status"),
    result_hash: optionalText(value?.result_hash),
    data_sequence: Number(value?.data_sequence ?? 0),
    readiness_reasons: normalizedReasons(value?.readiness_reasons),
    carried_forward_fields: uniqueSortedText(value?.carried_forward_fields),
  };
}

export function normalizePublicationCurrentReconciliationResult(result, target, channelId) {
  if (result?.channel_id !== channelId) {
    fail("Publication reconciliation returned a different Channel", {
      expected: channelId,
      actual: result?.channel_id ?? null,
    });
  }
  if (String(result?.publication_stream_id ?? "").toLowerCase()
      !== target.publication_stream_id) {
    fail("Publication reconciliation returned a different Stream", {
      channel_id: channelId,
      expected: target.publication_stream_id,
      actual: result?.publication_stream_id ?? null,
    });
  }
  const byDomain = new Map((result.domains ?? []).map((item) => [item.domain, item]));
  const domains = target.domains.map((domain) => {
    if (!byDomain.has(domain)) {
      fail("Publication reconciliation omitted a requested Domain", { channel_id: channelId, domain });
    }
    const normalized = normalizedDomainResult(byDomain.get(domain));
    if (!Number.isSafeInteger(normalized.data_sequence) || normalized.data_sequence < 0) {
      fail("Publication reconciliation returned an invalid Sequence", {
        channel_id: channelId,
        domain,
      });
    }
    return normalized;
  });
  return {
    channel_id: channelId,
    status: requiredText(result.status, "reconciliation status"),
    seed_status: requiredText(result.seed_status, "reconciliation seed_status"),
    domains,
  };
}

function evidenceHash(value) {
  const { evidence_hash: ignored, ...body } = value;
  return canonicalHash(body);
}

export function buildPublicationCurrentReconciliationEvidence({
  config,
  target,
  sourceState,
  businessState,
  channels,
  generatedAt = new Date().toISOString(),
}) {
  const evidence = {
    evidence_version: EVIDENCE_VERSION,
    generated_at: timestamp(generatedAt, "generatedAt"),
    target,
    operator: config.actor,
    action_reason: config.reason,
    source_state: {
      database_name: sourceState.database_name,
      channel_count: sourceState.channel_count,
      stream_status: sourceState.stream_status,
      capture_enabled_at: sourceState.capture_enabled_at,
      minimum_writer_version: sourceState.minimum_writer_version,
      owned_channel_count: sourceState.owned_channel_count,
      online_delivery_count: sourceState.online_delivery_count,
      unexpected_open_delivery_count: sourceState.unexpected_open_delivery_count,
      domain_current_count: sourceState.domain_current_count,
    },
    business_state: {
      database_name: businessState.database_name,
      channel_count: businessState.channel_count,
      active_watermark: businessState.active_watermark,
      target_active_snapshot_count: businessState.target_active_snapshot_count,
    },
    channels,
  };
  return { ...evidence, evidence_hash: evidenceHash(evidence) };
}

function assertTarget(expected, actual) {
  if (!isDeepStrictEqual(actual, expected)) {
    fail("Publication Current reconciliation evidence targets a different operation", {
      expected,
      actual,
    });
  }
}

function validateEvidence(value, config, target) {
  if (!value || Array.isArray(value) || typeof value !== "object") {
    throw new TypeError("Publication Current reconciliation evidence must be an object");
  }
  if (value.evidence_version !== EVIDENCE_VERSION) {
    fail("Publication Current reconciliation evidence version is unsupported");
  }
  assertTarget(target, value.target);
  if (value.operator !== config.actor || value.action_reason !== config.reason) {
    fail("Publication Current reconciliation operator or reason changed after plan");
  }
  if (!Array.isArray(value.channels) || value.channels.length !== target.channel_count) {
    fail("Publication Current reconciliation evidence Channel count is invalid");
  }
  const evidenceChannelIds = value.channels.map((channel) => channel?.channel_id);
  if (!isDeepStrictEqual(evidenceChannelIds, target.channel_ids)) {
    fail("Publication Current reconciliation evidence Channel set is invalid");
  }
  if (value.evidence_hash !== evidenceHash(value)) {
    fail("Publication Current reconciliation evidence hash mismatch");
  }
  return value;
}

export async function readPublicationCurrentReconciliationEvidence(path, config, target) {
  let document;
  try {
    document = JSON.parse(await readFile(path, "utf8"));
  } catch (error) {
    throw new TypeError("cannot read Publication Current reconciliation evidence", { cause: error });
  }
  return validateEvidence(document?.evidence ?? document, config, target);
}

export function publicationCurrentReconciliationConfirmation(config, target, evidence) {
  const approved = validateEvidence(evidence, config, target);
  return [
    "RECONCILE_PUBLICATION_CURRENT",
    target.reconcile_key,
    target.channel_count,
    target.channel_set_hash,
    approved.evidence_hash,
  ].join(":");
}

function countBy(values, select) {
  const output = {};
  for (const value of values) {
    const key = select(value);
    output[key] = (output[key] ?? 0) + 1;
  }
  return output;
}

export function publicationCurrentReconciliationSummary(evidence) {
  const channels = evidence.channels ?? [];
  const preservationBaselines = channels
    .map((channel) => channel.preservation_baseline)
    .filter((baseline) => baseline && typeof baseline === "object");
  const domains = {};
  for (const domain of evidence.target.domains) {
    const results = channels.map((channel) => (
      channel.result.domains.find((item) => item.domain === domain)
    ));
    domains[domain] = {
      outcomes: countBy(results, (result) => result.status),
      carried_forward: results.filter((result) => result.carried_forward_fields.length > 0).length,
      carried_forward_no_change: results.filter((result) => (
        result.status === "no_change" && result.carried_forward_fields.length > 0
      )).length,
    };
  }
  return {
    channel_count: evidence.target.channel_count,
    channel_outcomes: countBy(channels, (channel) => channel.result.status),
    business_preservation_baselines: countBy(
      preservationBaselines,
      (baseline) => baseline.status,
    ),
    domains,
    crawler_refetch_performed: false,
  };
}

async function mapConcurrent(values, concurrency, action) {
  const results = new Array(values.length);
  let cursor = 0;
  async function worker() {
    while (true) {
      const index = cursor;
      cursor += 1;
      if (index >= values.length) return;
      try {
        results[index] = { ok: true, value: await action(values[index], index) };
      } catch (error) {
        results[index] = { ok: false, error };
      }
    }
  }
  await Promise.all(Array.from(
    { length: Math.min(concurrency, values.length) },
    () => worker(),
  ));
  return results;
}

async function inspectSourceState(pool, config, target) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
    const identity = (await client.query(
      `SELECT current_database() AS database_name,
              (SELECT count(*)::int FROM crawler.channels) AS channel_count`,
    )).rows[0];
    const stream = (await client.query(
      `SELECT status,capture_enabled_at,minimum_writer_version
       FROM publication.stream WHERE publication_stream_id=$1::uuid`,
      [target.publication_stream_id],
    )).rows[0];
    const topology = (await client.query(
      `SELECT
         count(DISTINCT target.channel_id) FILTER (
           WHERE owner.status='owned' AND owner.publication_stream_id=$1::uuid
         )::int AS owned_channel_count,
         count(DISTINCT target.channel_id) FILTER (
           WHERE delivery.mode='online' AND delivery.destination=$3
         )::int AS online_delivery_count,
         (
           SELECT count(*)::int
           FROM unnest($2::text[]) AS unexpected_target(channel_id)
           JOIN publication.channel_delivery_state unexpected_delivery
             ON unexpected_delivery.publication_stream_id=$1::uuid
            AND unexpected_delivery.channel_id=unexpected_target.channel_id
           WHERE unexpected_delivery.mode<>'sealed'
             AND unexpected_delivery.destination<>$3
         ) AS unexpected_open_delivery_count,
         count(current.domain)::int AS domain_current_count
       FROM unnest($2::text[]) AS target(channel_id)
       LEFT JOIN publication.channel_stream_state owner
         ON owner.channel_id=target.channel_id
        AND owner.publication_stream_id=$1::uuid
       LEFT JOIN publication.channel_delivery_state delivery
         ON delivery.publication_stream_id=owner.publication_stream_id
        AND delivery.channel_id=owner.channel_id
        AND delivery.destination=$3
       LEFT JOIN publication.domain_current current
         ON current.publication_stream_id=owner.publication_stream_id
        AND current.channel_id=owner.channel_id
        AND current.domain=ANY($4::text[])`,
      [target.publication_stream_id, target.channel_ids, target.destination, target.domains],
    )).rows[0];
    await client.query("COMMIT");
    const state = {
      database_name: identity?.database_name ?? null,
      channel_count: Number(identity?.channel_count ?? -1),
      stream_status: stream?.status ?? null,
      capture_enabled_at: stream?.capture_enabled_at == null
        ? null
        : new Date(stream.capture_enabled_at).toISOString(),
      minimum_writer_version: stream?.minimum_writer_version ?? null,
      owned_channel_count: Number(topology?.owned_channel_count ?? 0),
      online_delivery_count: Number(topology?.online_delivery_count ?? 0),
      unexpected_open_delivery_count: Number(topology?.unexpected_open_delivery_count ?? 0),
      domain_current_count: Number(topology?.domain_current_count ?? 0),
    };
    if (state.database_name !== config.expectedCrawlerDatabase) {
      fail("Crawler database identity mismatch", {
        actual: state.database_name,
        expected: config.expectedCrawlerDatabase,
      });
    }
    if (state.channel_count !== config.expectedCrawlerChannelCount) {
      fail("Crawler Channel count mismatch", {
        actual: state.channel_count,
        expected: config.expectedCrawlerChannelCount,
      });
    }
    if (state.stream_status !== "active" || !state.capture_enabled_at) {
      fail("Publication Stream is not active with Capture enabled");
    }
    if (state.owned_channel_count !== target.channel_count) {
      fail("not every target Channel is owned by the requested Publication Stream", state);
    }
    if (state.online_delivery_count !== target.channel_count) {
      fail("not every target Channel has Online delivery for the requested Destination", state);
    }
    if (state.unexpected_open_delivery_count !== 0) {
      fail("target Channels have an unexpected unsealed Publication Destination", state);
    }
    const expectedCurrents = target.channel_count * target.domains.length;
    if (state.domain_current_count !== expectedCurrents) {
      fail("not every target Channel has the requested Publication Domain Current", {
        actual: state.domain_current_count,
        expected: expectedCurrents,
      });
    }
    return state;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function inspectBusinessState(pool, config, target) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
    const identity = (await client.query(
      `/* publication-current-reconciliation:business-identity */
       SELECT current_database() AS database_name,
              (SELECT count(*)::int FROM public.channels) AS channel_count,
              (SELECT watermark FROM public.creator_search_active WHERE singleton=true)
                AS active_watermark`,
    )).rows[0];
    const requiresChannelPreservation = target.domains.includes("channel");
    const baselineResult = requiresChannelPreservation
      ? await client.query(BUSINESS_CHANNEL_PRESERVATION_SQL, [target.channel_ids])
      : { rows: [] };
    await client.query("COMMIT");
    const state = {
      database_name: identity?.database_name ?? null,
      channel_count: Number(identity?.channel_count ?? -1),
      active_watermark: optionalText(identity?.active_watermark),
    };
    if (state.database_name !== config.expectedBusinessDatabase) {
      fail("Business database identity mismatch", {
        actual: state.database_name,
        expected: config.expectedBusinessDatabase,
      });
    }
    if (state.channel_count !== config.expectedBusinessChannelCount) {
      fail("Business Channel count mismatch", {
        actual: state.channel_count,
        expected: config.expectedBusinessChannelCount,
      });
    }
    if (!state.active_watermark) fail("Business Active Watermark is missing");
    const baselines = requiresChannelPreservation
      ? buildBusinessChannelPreservationBaselines(
        baselineResult.rows,
        target.channel_ids,
        { databaseName: state.database_name },
      )
      : new Map();
    return {
      state: {
        ...state,
        target_active_snapshot_count: [...baselines.values()]
          .filter((baseline) => baseline.status === "available").length,
      },
      baselines,
    };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

function equivalentAppliedResult(expected, actual) {
  if (expected.channel_id !== actual.channel_id || expected.seed_status !== actual.seed_status) {
    return false;
  }
  const actualByDomain = new Map(actual.domains.map((domain) => [domain.domain, domain]));
  return expected.domains.every((domain) => {
    const current = actualByDomain.get(domain.domain);
    if (!current) return false;
    if (isDeepStrictEqual(current, domain)) return true;
    const revisionAlreadyApplied = domain.status === "revision_created"
      && current.status === "no_change";
    const preservationAlreadyApplied = domain.status === "preservation_seeded"
      && current.status === "preservation_retained";
    return (revisionAlreadyApplied || preservationAlreadyApplied)
      && current.result_hash === domain.result_hash
      && current.data_sequence === domain.data_sequence;
  });
}

function equivalentBusinessState(expected, actual, domains) {
  if (domains.includes("channel")) return isDeepStrictEqual(actual, expected);
  return actual?.database_name === expected?.database_name
    && actual?.channel_count === expected?.channel_count
    && actual?.target_active_snapshot_count === 0
    && expected?.target_active_snapshot_count === 0;
}

async function runReconciliationTransaction({
  pool,
  target,
  channelId,
  reconcile,
  lock,
  commit,
  preservationBaseline,
  expected = null,
}) {
  const client = await pool.connect();
  let finished = false;
  try {
    await client.query("BEGIN");
    await client.query(
      "SELECT set_config('publication.writer_version',$1,true)",
      [PUBLICATION_WRITER_VERSION],
    );
    await lock(client, channelId);
    const raw = await reconcile(client, {
      channelId,
      domains: target.domains,
      asOf: target.as_of,
      revisionType: target.revision_type,
      preservationBaselines: target.domains.includes("channel")
        ? { channel: preservationBaseline }
        : undefined,
    });
    const result = normalizePublicationCurrentReconciliationResult(raw, target, channelId);
    let alreadyApplied = false;
    if (expected) {
      if (isDeepStrictEqual(result, expected.result)) {
        alreadyApplied = false;
      } else if (equivalentAppliedResult(expected.result, result)) {
        alreadyApplied = true;
      } else {
        fail("Publication Current changed after the approved reconciliation plan", {
          channel_id: channelId,
          expected: expected.result,
          actual: result,
        });
      }
    }
    await client.query(commit ? "COMMIT" : "ROLLBACK");
    finished = true;
    return { channel_id: channelId, result, already_applied: alreadyApplied };
  } catch (error) {
    if (!finished) await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

export class PublicationCurrentReconciliationAdministrator {
  constructor({
    crawlerPool,
    businessPool,
    config,
    target,
    evidence = null,
    reconcile = reconcilePublication,
    lock = lockPublicationChannelMutation,
  }) {
    if (!crawlerPool?.connect || !businessPool?.connect) {
      throw new TypeError("Crawler and Business PostgreSQL Pools are required");
    }
    if (typeof reconcile !== "function" || typeof lock !== "function") {
      throw new TypeError("reconcile and lock must be functions");
    }
    this.crawlerPool = crawlerPool;
    this.businessPool = businessPool;
    this.config = config;
    this.target = target;
    this.evidence = evidence;
    this.reconcile = reconcile;
    this.lock = lock;
  }

  async inspectRollbackPreview({ generatedAt } = {}) {
    const [sourceState, business] = await Promise.all([
      inspectSourceState(this.crawlerPool, this.config, this.target),
      inspectBusinessState(this.businessPool, this.config, this.target),
    ]);
    const previews = await mapConcurrent(
      this.target.channel_ids,
      this.config.concurrency,
      (channelId) => runReconciliationTransaction({
        pool: this.crawlerPool,
        target: this.target,
        channelId,
        reconcile: this.reconcile,
        lock: this.lock,
        commit: false,
        preservationBaseline: business.baselines.get(channelId),
      }),
    );
    const failure = previews.find((item) => !item.ok);
    if (failure) throw failure.error;
    const channels = previews.map((item) => ({
      channel_id: item.value.channel_id,
      preservation_baseline: business.baselines.get(item.value.channel_id) ?? null,
      result: item.value.result,
    }));
    return buildPublicationCurrentReconciliationEvidence({
      config: this.config,
      target: this.target,
      sourceState,
      businessState: business.state,
      channels,
      generatedAt,
    });
  }

  async apply() {
    const approved = validateEvidence(this.evidence, this.config, this.target);
    const [sourceState, business] = await Promise.all([
      inspectSourceState(this.crawlerPool, this.config, this.target),
      inspectBusinessState(this.businessPool, this.config, this.target),
    ]);
    if (!isDeepStrictEqual(sourceState, approved.source_state)) {
      fail("Crawler Publication topology changed after the approved reconciliation plan", {
        approved: approved.source_state,
        actual: sourceState,
      });
    }
    if (!equivalentBusinessState(
      approved.business_state,
      business.state,
      this.target.domains,
    )) {
      fail("Business Active topology changed after the approved reconciliation plan", {
        approved: approved.business_state,
        actual: business.state,
      });
    }
    const expectedByChannel = new Map(
      approved.channels.map((channel) => [channel.channel_id, channel]),
    );
    if (this.target.domains.includes("channel")) {
      for (const channelId of this.target.channel_ids) {
        const expected = expectedByChannel.get(channelId).preservation_baseline;
        const actual = business.baselines.get(channelId);
        if (!isDeepStrictEqual(actual, expected)) {
          fail("Business preservation baseline changed after the approved reconciliation plan", {
            channel_id: channelId,
            approved: expected,
            actual,
          });
        }
      }
    }
    const applied = await mapConcurrent(
      this.target.channel_ids,
      this.config.concurrency,
      (channelId) => runReconciliationTransaction({
        pool: this.crawlerPool,
        target: this.target,
        channelId,
        reconcile: this.reconcile,
        lock: this.lock,
        commit: true,
        preservationBaseline: expectedByChannel.get(channelId).preservation_baseline,
        expected: expectedByChannel.get(channelId),
      }),
    );
    const succeeded = applied.filter((item) => item.ok).map((item) => item.value);
    const failures = applied.map((item, index) => ({ item, index }))
      .filter(({ item }) => !item.ok)
      .map(({ item, index }) => ({
        channel_id: this.target.channel_ids[index],
        error: item.error?.message ?? String(item.error),
        details: item.error?.details ?? null,
      }));
    const result = {
      succeeded: succeeded.length,
      failed: failures.length,
      already_applied: succeeded.filter((item) => item.already_applied).length,
      committed: succeeded.filter((item) => !item.already_applied).length,
      channels: succeeded,
      failures,
    };
    if (failures.length > 0) {
      throw new PublicationCurrentReconciliationPartialFailure(
        "Publication Current reconciliation completed only partially; inspect failures before retry",
        result,
      );
    }
    return result;
  }
}
