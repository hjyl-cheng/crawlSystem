import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import {
  verifyBusinessWriterDatabase,
  verifyCrawlerWriterDatabase,
} from "./databaseIdentity.js";
import { PUBLICATION_WRITER_VERSION } from "./publicationWriterVersion.js";
import { environmentValue } from "./runtimeEnvironment.js";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const SAFE_KEY = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const IMAGE_DIGEST = /^sha256:[0-9a-f]{64}$/;
const CRAWLER_BOOTSTRAP_LOCK = 781137244;
const BUSINESS_BOOTSTRAP_LOCK = 781137245;

const SOURCE_ZERO_COUNTS = Object.freeze([
  "channels",
  "candidates",
  "runs",
  "channel_ownerships",
  "deliveries",
  "domain_currents",
  "revisions",
  "outbox",
]);
const BUSINESS_ZERO_COUNTS = Object.freeze([
  "channels",
  "channel_ownerships",
  "inbox",
  "revisions",
  "activations",
  "projection_outbox",
  "entity_current",
  "video_current",
  "agent_current",
  "snapshots",
  "search_current",
]);

function requiredText(environment, name) {
  const value = String(environment[name] ?? "").trim();
  if (!value) throw new TypeError(`${name} is required`);
  return value;
}

function safeKey(environment, name) {
  const value = requiredText(environment, name);
  if (!SAFE_KEY.test(value)) {
    throw new TypeError(`${name} must use only letters, numbers, dot, underscore, or hyphen`);
  }
  return value;
}

function exactZero(environment, name) {
  if (requiredText(environment, name) !== "0") {
    throw new TypeError(`${name} must be exactly 0 for a fresh Publication bootstrap`);
  }
  return 0;
}

function immutableDigest(environment, name) {
  const value = requiredText(environment, name).toLowerCase();
  if (!IMAGE_DIGEST.test(value)) throw new TypeError(`${name} must be an immutable sha256 digest`);
  return value;
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

function sourceIdentity(environment, expectedDatabase) {
  const raw = environmentValue("PUBLICATION_SOURCE_IDENTITY_JSON", { environment });
  let identity;
  try {
    identity = JSON.parse(raw);
  } catch (error) {
    throw new TypeError("PUBLICATION_SOURCE_IDENTITY_JSON must be valid JSON", { cause: error });
  }
  if (!identity || Array.isArray(identity) || typeof identity !== "object") {
    throw new TypeError("PUBLICATION_SOURCE_IDENTITY_JSON must be a JSON object");
  }
  if (String(identity.database ?? "").trim() !== expectedDatabase) {
    throw new TypeError(
      `PUBLICATION_SOURCE_IDENTITY_JSON.database must equal ${expectedDatabase}`,
    );
  }
  return canonicalJson(identity);
}

function adminDatabaseUrl(environment, name, expectedDatabase) {
  const raw = environmentValue(name, { environment });
  let url;
  try {
    url = new URL(raw);
  } catch (error) {
    throw new TypeError(`${name} must be a valid PostgreSQL URL`, { cause: error });
  }
  if (!new Set(["postgres:", "postgresql:"]).has(url.protocol)) {
    throw new TypeError(`${name} must use postgres:// or postgresql://`);
  }
  const database = decodeURIComponent(url.pathname.replace(/^\//, ""));
  if (database !== expectedDatabase) {
    throw new TypeError(`${name} must target database ${expectedDatabase}`);
  }
  if (!url.hostname || !url.username || !url.password) {
    throw new TypeError(`${name} must include host, user, and password`);
  }
  return raw;
}

export function freshPublicationBootstrapConfig(environment = process.env) {
  const expectedCrawlerDatabase = safeKey(environment, "EXPECTED_CRAWLER_DATABASE");
  const expectedBusinessDatabase = safeKey(environment, "EXPECTED_BUSINESS_DATABASE");
  const forbiddenCrawlerDatabase = safeKey({
    FORBIDDEN_CRAWLER_DATABASE:
      environment.FORBIDDEN_CRAWLER_DATABASE || "bullmq_crawler_migration",
  }, "FORBIDDEN_CRAWLER_DATABASE");
  const forbiddenBusinessDatabase = safeKey({
    FORBIDDEN_BUSINESS_DATABASE:
      environment.FORBIDDEN_BUSINESS_DATABASE || "yewu_business",
  }, "FORBIDDEN_BUSINESS_DATABASE");
  if (expectedCrawlerDatabase === forbiddenCrawlerDatabase) {
    throw new TypeError(`refusing forbidden Crawler database ${expectedCrawlerDatabase}`);
  }
  if (expectedBusinessDatabase === forbiddenBusinessDatabase) {
    throw new TypeError(`refusing forbidden Business database ${expectedBusinessDatabase}`);
  }
  if (expectedCrawlerDatabase === expectedBusinessDatabase) {
    throw new TypeError("Crawler and Business database names must be different");
  }
  exactZero(environment, "EXPECTED_CRAWLER_CHANNEL_COUNT");
  exactZero(environment, "EXPECTED_BUSINESS_CHANNEL_COUNT");

  const streamId = requiredText(environment, "PUBLICATION_STREAM_ID").toLowerCase();
  if (!UUID.test(streamId)) throw new TypeError("PUBLICATION_STREAM_ID must be a UUID");
  const projectionMode = requiredText(
    environment,
    "PUBLICATION_BOOTSTRAP_PROJECTION_MODE",
  );
  if (!new Set(["held_shadow", "online"]).has(projectionMode)) {
    throw new TypeError(
      "PUBLICATION_BOOTSTRAP_PROJECTION_MODE must be held_shadow or online",
    );
  }
  const identity = sourceIdentity(environment, expectedCrawlerDatabase);
  return {
    crawlerAdminDatabaseUrl: adminDatabaseUrl(
      environment,
      "CRAWLER_ADMIN_DATABASE_URL",
      expectedCrawlerDatabase,
    ),
    businessAdminDatabaseUrl: adminDatabaseUrl(
      environment,
      "BUSINESS_ADMIN_DATABASE_URL",
      expectedBusinessDatabase,
    ),
    expectedCrawlerDatabase,
    expectedBusinessDatabase,
    forbiddenCrawlerDatabase,
    forbiddenBusinessDatabase,
    streamId,
    sourceDeploymentKey: safeKey(environment, "PUBLICATION_SOURCE_DEPLOYMENT_KEY"),
    sourceIdentity: identity,
    sourceIdentityHash: sha256(JSON.stringify(identity)),
    destination: safeKey(environment, "PUBLICATION_DESTINATION"),
    projectionMode,
    actor: requiredText(environment, "PUBLICATION_OPERATOR"),
    reason: requiredText(environment, "PUBLICATION_ACTION_REASON"),
    writerVersion: PUBLICATION_WRITER_VERSION,
    writerDeploymentRef: immutableDigest(environment, "PUBLICATION_WRITER_DEPLOYMENT_REF"),
    runtimeDeploymentRef: immutableDigest(environment, "PUBLICATION_RUNTIME_DEPLOYMENT_REF"),
  };
}

export function freshPublicationBootstrapConfirmation(config) {
  return [
    "BOOTSTRAP_FRESH_PUBLICATION",
    config.expectedCrawlerDatabase,
    config.expectedBusinessDatabase,
    config.streamId,
    config.sourceDeploymentKey,
    config.destination,
    config.projectionMode,
    config.sourceIdentityHash,
    config.writerDeploymentRef,
    config.runtimeDeploymentRef,
  ].join(":");
}

function fail(message, detail = null) {
  const suffix = detail == null ? "" : `: ${JSON.stringify(detail)}`;
  throw new Error(`${message}${suffix}`);
}

function zeroCounts(side, counts, names) {
  for (const name of names) {
    const value = Number(counts?.[name]);
    if (!Number.isSafeInteger(value) || value !== 0) {
      fail(`${side} ${name} must be exactly 0`, counts?.[name]);
    }
  }
}

function exactStream(rows, config, { business }) {
  if (!Array.isArray(rows)) fail(`${business ? "Business" : "Crawler"} Stream state is invalid`);
  if (rows.length === 0) return null;
  if (rows.length !== 1) {
    fail(`${business ? "Business" : "Crawler"} must contain at most one Publication Stream`, rows);
  }
  const row = rows[0];
  const side = business ? "Business" : "Crawler";
  const expected = {
    publication_stream_id: config.streamId,
    source_deployment_key: config.sourceDeploymentKey,
    source_identity_json: config.sourceIdentity,
    status: "active",
  };
  for (const [field, value] of Object.entries(expected)) {
    const actual = field === "publication_stream_id"
      ? String(row[field] ?? "").toLowerCase()
      : row[field];
    if (!isDeepStrictEqual(actual, value)) fail(`${side} ${field} mismatch`, { actual, expected: value });
  }
  if (business) {
    const versions = (row.accepted_contract_versions ?? []).map(Number);
    if (!isDeepStrictEqual(versions, [1, 2])) {
      fail("Business accepted contract versions mismatch", versions);
    }
    if (row.automatic_onboarding_projection_mode !== config.projectionMode) {
      fail("Business automatic onboarding projection mode mismatch", {
        actual: row.automatic_onboarding_projection_mode,
        expected: config.projectionMode,
      });
    }
  } else {
    if (row.minimum_writer_version !== config.writerVersion) {
      fail("Crawler minimum Writer version mismatch", row.minimum_writer_version);
    }
    if (row.capture_enabled_at == null) fail("Crawler Capture must be enabled");
    if (row.automatic_onboarding_destination !== config.destination) {
      fail("Crawler automatic onboarding destination mismatch", {
        actual: row.automatic_onboarding_destination,
        expected: config.destination,
      });
    }
  }
  return row;
}

export function validateFreshPublicationBootstrapState(state, config) {
  zeroCounts("Crawler", state?.source?.counts, SOURCE_ZERO_COUNTS);
  zeroCounts("Business", state?.business?.counts, BUSINESS_ZERO_COUNTS);
  const source = exactStream(state?.source?.streams, config, { business: false });
  const business = exactStream(state?.business?.streams, config, { business: true });
  if (source && !business) fail("Crawler Stream exists before the Business Stream");
  if (source) return "complete";
  if (business) return "business_committed";
  return "empty";
}

async function inspectSource(client) {
  const [streams, countResult] = await Promise.all([
    client.query(
      `/* fresh-publication-bootstrap:source-streams */
       SELECT publication_stream_id,source_deployment_key,source_identity_json,status,
              minimum_writer_version,capture_enabled_at,automatic_onboarding_destination
       FROM publication.stream
       ORDER BY created_at,publication_stream_id`,
    ),
    client.query(
      `/* fresh-publication-bootstrap:source-counts */
       SELECT
         (SELECT count(*)::int FROM crawler.channels) AS channels,
         (SELECT count(*)::int FROM crawler.channel_candidates) AS candidates,
         (SELECT count(*)::int FROM crawler.channel_runs) AS runs,
         (SELECT count(*)::int FROM publication.channel_stream_state) AS channel_ownerships,
         (SELECT count(*)::int FROM publication.channel_delivery_state) AS deliveries,
         (SELECT count(*)::int FROM publication.domain_current) AS domain_currents,
         (SELECT count(*)::int FROM publication.revision) AS revisions,
         (SELECT count(*)::int FROM publication.outbox) AS outbox`,
    ),
  ]);
  return { streams: streams.rows, counts: countResult.rows[0] };
}

async function inspectBusiness(client) {
  const [streams, countResult] = await Promise.all([
    client.query(
      `/* fresh-publication-bootstrap:business-streams */
       SELECT publication_stream_id,source_deployment_key,source_identity_json,status,
              accepted_contract_versions,automatic_onboarding_projection_mode
       FROM publication.stream
       ORDER BY registered_at,publication_stream_id`,
    ),
    client.query(
      `/* fresh-publication-bootstrap:business-counts */
       SELECT
         (SELECT count(*)::int FROM public.channels) AS channels,
         (SELECT count(*)::int FROM publication.channel_ownership) AS channel_ownerships,
         (SELECT count(*)::int FROM publication.inbox) AS inbox,
         (SELECT count(*)::int FROM publication.revision) AS revisions,
         (SELECT count(*)::int FROM publication.activation) AS activations,
         (SELECT count(*)::int FROM publication.projection_outbox) AS projection_outbox,
         (SELECT count(*)::int FROM result.entity_current) AS entity_current,
         (SELECT count(*)::int FROM result.video_current) AS video_current,
         (SELECT count(*)::int FROM result.agent_current) AS agent_current,
         (SELECT count(*)::int FROM public.channel_snapshots) AS snapshots,
         (SELECT count(*)::int FROM public.creator_search_live) AS search_current`,
    ),
  ]);
  return { streams: streams.rows, counts: countResult.rows[0] };
}

async function readOnlyTransaction(client, action) {
  let begun = false;
  try {
    await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
    begun = true;
    await client.query("SET LOCAL statement_timeout = '120s'");
    const result = await action();
    await client.query("COMMIT");
    begun = false;
    return result;
  } catch (error) {
    if (begun) await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

async function transaction(client, lockId, action) {
  let begun = false;
  try {
    await client.query("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
    begun = true;
    await client.query("SET LOCAL lock_timeout = '10s'");
    await client.query("SET LOCAL statement_timeout = '120s'");
    await client.query("SELECT pg_advisory_xact_lock($1)", [lockId]);
    const result = await action();
    await client.query("COMMIT");
    begun = false;
    return result;
  } catch (error) {
    if (begun) await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

async function inspectBoth(crawler, business) {
  const results = await Promise.allSettled([inspectSource(crawler), inspectBusiness(business)]);
  const failure = results.find((result) => result.status === "rejected");
  if (failure) throw failure.reason;
  return { source: results[0].value, business: results[1].value };
}

export class FreshPublicationBootstrapAdministrator {
  constructor({ crawlerClient, businessClient, config }) {
    if (!crawlerClient?.query || !businessClient?.query) {
      throw new TypeError("Crawler and Business PostgreSQL clients are required");
    }
    this.crawler = crawlerClient;
    this.business = businessClient;
    this.config = config;
  }

  async verifyIdentities() {
    const crawlerEnvironment = {
      EXPECTED_CRAWLER_DATABASE: this.config.expectedCrawlerDatabase,
      FORBIDDEN_CRAWLER_DATABASE: this.config.forbiddenCrawlerDatabase,
    };
    const businessEnvironment = {
      EXPECTED_BUSINESS_DATABASE: this.config.expectedBusinessDatabase,
      FORBIDDEN_BUSINESS_DATABASE: this.config.forbiddenBusinessDatabase,
    };
    return Promise.all([
      verifyCrawlerWriterDatabase(this.crawler.query.bind(this.crawler), crawlerEnvironment),
      verifyBusinessWriterDatabase(this.business.query.bind(this.business), businessEnvironment),
    ]);
  }

  async inspectReadOnly() {
    await this.verifyIdentities();
    const state = await Promise.all([
      readOnlyTransaction(this.crawler, () => inspectSource(this.crawler)),
      readOnlyTransaction(this.business, () => inspectBusiness(this.business)),
    ]);
    return { source: state[0], business: state[1] };
  }

  async initialize() {
    const initial = await this.inspectReadOnly();
    const initialPhase = validateFreshPublicationBootstrapState(initial, this.config);
    if (initialPhase === "complete") {
      return { business: 0, source: 0, phase: "complete", state: initial };
    }

    let businessCommitAcknowledged = initialPhase === "business_committed";
    let businessState;
    let businessWrites = 0;
    try {
      const businessResult = await transaction(
        this.business,
        BUSINESS_BOOTSTRAP_LOCK,
        async () => {
          const current = await inspectBusiness(this.business);
          validateFreshPublicationBootstrapState(
            { source: initial.source, business: current },
            this.config,
          );
          const inserted = await this.business.query(
            `/* fresh-publication-bootstrap:insert-business-stream */
             INSERT INTO publication.stream (
               publication_stream_id,source_deployment_key,source_identity_json,status,
               accepted_contract_versions,automatic_onboarding_projection_mode,
               registered_by,registered_reason,status_changed_by,status_reason
             ) VALUES (
               $1::uuid,$2,$3::jsonb,'active',ARRAY[1,2]::integer[],$4,$5,$6,$5,$7
             )
             ON CONFLICT (publication_stream_id) DO NOTHING`,
            [
              this.config.streamId,
              this.config.sourceDeploymentKey,
              JSON.stringify(this.config.sourceIdentity),
              this.config.projectionMode,
              this.config.actor,
              this.config.reason,
              JSON.stringify({
                action: "bootstrap_fresh_publication",
                runtime_deployment_ref: this.config.runtimeDeploymentRef,
                operator_reason: this.config.reason,
              }),
            ],
          );
          const final = await inspectBusiness(this.business);
          validateFreshPublicationBootstrapState(
            { source: initial.source, business: final },
            this.config,
          );
          return { writes: inserted.rowCount, state: final };
        },
      );
      businessWrites = businessResult.writes;
      businessState = businessResult.state;
      businessCommitAcknowledged = true;

      const sourceResult = await transaction(
        this.crawler,
        CRAWLER_BOOTSTRAP_LOCK,
        async () => {
          const current = await inspectSource(this.crawler);
          validateFreshPublicationBootstrapState(
            { source: current, business: businessState },
            this.config,
          );
          const inserted = await this.crawler.query(
            `/* fresh-publication-bootstrap:insert-source-stream */
             INSERT INTO publication.stream (
               publication_stream_id,source_deployment_key,source_identity_json,status,
               minimum_writer_version,capture_enabled_at,automatic_onboarding_destination,
               created_by,created_reason,status_changed_by,status_reason
             ) VALUES (
               $1::uuid,$2,$3::jsonb,'active',$4,clock_timestamp(),$5,$6,$7,$6,$8
             )
             ON CONFLICT (publication_stream_id) DO NOTHING`,
            [
              this.config.streamId,
              this.config.sourceDeploymentKey,
              JSON.stringify(this.config.sourceIdentity),
              this.config.writerVersion,
              this.config.destination,
              this.config.actor,
              this.config.reason,
              JSON.stringify({
                action: "bootstrap_fresh_publication",
                writer_deployment_ref: this.config.writerDeploymentRef,
                operator_reason: this.config.reason,
              }),
            ],
          );
          const final = await inspectSource(this.crawler);
          const phase = validateFreshPublicationBootstrapState(
            { source: final, business: businessState },
            this.config,
          );
          if (phase !== "complete") fail("fresh Publication bootstrap did not complete");
          return { writes: inserted.rowCount, state: final };
        },
      );
      const final = await this.inspectReadOnly();
      const phase = validateFreshPublicationBootstrapState(final, this.config);
      if (phase !== "complete") fail("fresh Publication bootstrap postflight did not complete");
      return {
        business: businessWrites,
        source: sourceResult.writes,
        phase,
        state: final,
      };
    } catch (error) {
      if (businessCommitAcknowledged) {
        throw new Error(
          `Business Publication Stream is committed but Crawler bootstrap did not complete; rerun the read-only plan, then repeat the same confirmed apply: ${error.message}`,
          { cause: error },
        );
      }
      throw error;
    }
  }
}

export function freshPublicationBootstrapSummary(state, config) {
  const phase = validateFreshPublicationBootstrapState(state, config);
  return {
    phase,
    publication_stream_id: config.streamId,
    source_deployment_key: config.sourceDeploymentKey,
    source_identity_hash: config.sourceIdentityHash,
    destination: config.destination,
    projection_mode: config.projectionMode,
    writer_version: config.writerVersion,
    crawler: {
      database: config.expectedCrawlerDatabase,
      stream_registered: state.source.streams.length === 1,
      capture_enabled_at: state.source.streams[0]?.capture_enabled_at ?? null,
      counts: state.source.counts,
    },
    business: {
      database: config.expectedBusinessDatabase,
      stream_registered: state.business.streams.length === 1,
      counts: state.business.counts,
    },
  };
}
