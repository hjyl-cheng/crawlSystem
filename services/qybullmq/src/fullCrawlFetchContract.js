import { createHash } from "node:crypto";
import { assertFullCrawlCanaryJob, isFullCrawlCanaryPayload } from "./fullCrawlCanary.js";

export const LEGACY_FULL_CRAWL_FETCH_CONTRACT_ID = "legacy_full_v2";
export const YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT_ID = "youtubejs_full_v1";
export const YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT_ID = "youtubejs_full_v2";
export const FULL_CRAWL_FETCH_CONTRACT_DEFAULT_ENV = "FULL_CRAWL_FETCH_CONTRACT_DEFAULT";

function canonicalValue(value) {
  if (Array.isArray(value)) return value.map(canonicalValue);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.keys(value).sort().map((key) => [key, canonicalValue(value[key])]),
  );
}

function manifestHash(manifest) {
  return `sha256:${createHash("sha256")
    .update(JSON.stringify(canonicalValue(manifest)))
    .digest("hex")}`;
}

function contract(manifest) {
  return Object.freeze({
    executor_id: manifest.executor_id,
    executor_version: manifest.executor_version,
    contract_hash: manifestHash(manifest),
  });
}

export const LEGACY_FULL_CRAWL_FETCH_MANIFEST = Object.freeze({
  executor_id: "legacy_full",
  executor_version: 2,
  channel_source: "legacy_mixed",
  uploads_source: "legacy_mixed",
  detail_source: "legacy_mixed",
  comments_source: "legacy_mixed",
  fallback: "legacy_enabled",
  detail_concurrency: "configured",
});

export const YOUTUBEJS_FULL_CRAWL_V1_FETCH_MANIFEST = Object.freeze({
  executor_id: "youtubejs_full",
  executor_version: 1,
  channel_source: "youtubejs",
  uploads_source: "youtubejs",
  detail_source: "youtubejs",
  comments_source: "youtubejs",
  fallback: "forbidden",
  detail_concurrency: 1,
});

export const YOUTUBEJS_FULL_CRAWL_FETCH_MANIFEST = Object.freeze({
  ...YOUTUBEJS_FULL_CRAWL_V1_FETCH_MANIFEST,
  executor_version: 2,
  comments_required: false,
  comments_fallback: "empty_top_to_newest_once",
});

export const LEGACY_FULL_CRAWL_FETCH_CONTRACT = contract(
  LEGACY_FULL_CRAWL_FETCH_MANIFEST,
);
export const YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT = contract(
  YOUTUBEJS_FULL_CRAWL_FETCH_MANIFEST,
);
export const YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT = contract(
  YOUTUBEJS_FULL_CRAWL_V1_FETCH_MANIFEST,
);

const CONTRACTS = new Map([
  [LEGACY_FULL_CRAWL_FETCH_CONTRACT_ID, LEGACY_FULL_CRAWL_FETCH_CONTRACT],
  [YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT_ID, YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT],
  [YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT_ID, YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT],
]);

export class FullCrawlFetchContractError extends Error {
  constructor(message, { expected = null, actual = null } = {}) {
    super(message);
    this.name = "FullCrawlFetchContractError";
    this.code = "FULL_CRAWL_FETCH_CONTRACT_CONFLICT";
    this.expected = expected;
    this.actual = actual;
  }
}

function objectValue(value) {
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : null;
    } catch {
      return null;
    }
  }
  return value && typeof value === "object" && !Array.isArray(value) ? value : null;
}

export function fullCrawlFetchContractId(value, { missingAsLegacy = true } = {}) {
  if (value == null || value === "") {
    if (missingAsLegacy) return LEGACY_FULL_CRAWL_FETCH_CONTRACT_ID;
    throw new FullCrawlFetchContractError("Full Crawl fetch contract is required");
  }
  if (typeof value === "string" && CONTRACTS.has(value.trim())) return value.trim();
  const normalized = objectValue(value);
  if (!normalized) {
    throw new FullCrawlFetchContractError("Full Crawl fetch contract is malformed", {
      actual: value,
    });
  }
  const executorId = String(normalized.executor_id ?? "").trim();
  const executorVersion = Number(normalized.executor_version);
  const id = `${executorId}_v${executorVersion}`;
  const known = CONTRACTS.get(id);
  if (!known
      || normalized.contract_hash !== known.contract_hash
      || executorId !== known.executor_id
      || executorVersion !== known.executor_version) {
    throw new FullCrawlFetchContractError(`Unsupported Full Crawl fetch contract: ${id}`, {
      actual: normalized,
    });
  }
  return id;
}

export function normalizeFullCrawlFetchContract(value, options = {}) {
  return CONTRACTS.get(fullCrawlFetchContractId(value, options));
}

export function readFullCrawlFetchContractFromIntent(value) {
  const root = objectValue(value);
  const intent = objectValue(root?.intent) ?? root;
  const explicit = Boolean(
    intent && Object.prototype.hasOwnProperty.call(intent, "fetch_contract"),
  );
  return Object.freeze({
    explicit,
    id: fullCrawlFetchContractId(explicit ? intent.fetch_contract : null),
    contract: normalizeFullCrawlFetchContract(explicit ? intent.fetch_contract : null),
  });
}

export function defaultFullCrawlFetchContract(environment = process.env) {
  const requested = String(environment?.[FULL_CRAWL_FETCH_CONTRACT_DEFAULT_ENV] ?? "").trim()
    || LEGACY_FULL_CRAWL_FETCH_CONTRACT_ID;
  return normalizeFullCrawlFetchContract(requested, { missingAsLegacy: false });
}

export function newFullCrawlFetchContractForJob(job, environment = process.env) {
  if (isFullCrawlCanaryPayload(job?.data)) {
    assertFullCrawlCanaryJob(job.name, job.data);
    return YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT;
  }
  // Repair and recovery protocols remain on the legacy executor until migrated explicitly.
  if (String(job?.name ?? "") !== "channel-snapshot") {
    return LEGACY_FULL_CRAWL_FETCH_CONTRACT;
  }
  return defaultFullCrawlFetchContract(environment);
}

export function assertSameFullCrawlFetchContract(expectedValue, actualValue) {
  const expected = normalizeFullCrawlFetchContract(expectedValue);
  const actual = normalizeFullCrawlFetchContract(actualValue);
  if (expected.contract_hash !== actual.contract_hash) {
    throw new FullCrawlFetchContractError(
      `Full Crawl fetch contract conflicts: ${fullCrawlFetchContractId(actual)} != ${fullCrawlFetchContractId(expected)}`,
      { expected, actual },
    );
  }
  return expected;
}

export function isYoutubeJsFullCrawlFetchContract(value) {
  return [YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT_ID, YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT_ID]
    .includes(fullCrawlFetchContractId(value));
}
