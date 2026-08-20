import { randomUUID } from "node:crypto";
import { observationFactsHash } from "./crawlObservationStore.js";
import { SUPPORTED_PUBLICATION_CONTRACT_VERSIONS } from "./publicationContract.js";

export const PUBLICATION_SHARD_CONTRACT_VERSION = 1;
export const PUBLICATION_SHARD_MAX_ITEMS = 100;
export const PUBLICATION_SHARD_MAX_BYTES = 4 * 1024 * 1024;

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const HASH = /^sha256:[0-9a-f]{64}$/;
const DOMAINS = new Set(["channel", "video", "agent"]);
const REVISION_TYPES = new Set(["bootstrap", "incremental", "repair", "retraction"]);
const ENVELOPE_CONTRACT_VERSIONS = new Set(SUPPORTED_PUBLICATION_CONTRACT_VERSIONS);
const RECEIPT_STATUSES = new Set(["accepted", "duplicate", "waiting_gap", "rejected", "conflict"]);
const SUCCESS_RECEIPT_STATUSES = new Set(["accepted", "duplicate", "waiting_gap"]);
const ENVELOPE_KEYS = new Set([
  "revision_id",
  "publication_stream_id",
  "revision_type",
  "channel_id",
  "domain",
  "data_sequence",
  "previous_data_sequence",
  "operation",
  "contract_version",
  "policy_version",
  "occurred_at",
  "source",
  "previous_result_hash",
  "result_hash",
  "payload_hash",
  "payload",
]);
const SHARD_KEYS = new Set([
  "shard_id",
  "created_at",
  "contract_version",
  "manifest_hash",
  "items",
]);

export class PublicationEnvelopeConflict extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "PublicationEnvelopeConflict";
    this.details = details;
  }
}

export class PublicationReceiptConflict extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "PublicationReceiptConflict";
    this.details = details;
  }
}

export class PublicationIngressHttpError extends Error {
  constructor(message, status = null) {
    super(message);
    this.name = "PublicationIngressHttpError";
    this.status = status;
  }
}

function requiredText(value, field, ErrorType = PublicationEnvelopeConflict) {
  const output = String(value ?? "").trim();
  if (!output) throw new ErrorType(`${field} is required`);
  return output;
}

function jsonObject(value, field, ErrorType = PublicationEnvelopeConflict) {
  let output = value;
  if (typeof output === "string") {
    try {
      output = JSON.parse(output);
    } catch (error) {
      throw new ErrorType(`${field} is not valid JSON: ${error.message}`);
    }
  }
  if (!output || typeof output !== "object" || Array.isArray(output)) {
    throw new ErrorType(`${field} must be an object`);
  }
  return output;
}

function timestamp(value, field, ErrorType = PublicationEnvelopeConflict) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new ErrorType(`${field} must be a timestamp`);
  return parsed.toISOString();
}

function safeInteger(value, field, { minimum = 0 } = {}) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < minimum) {
    throw new PublicationEnvelopeConflict(`${field} must be a safe integer >= ${minimum}`);
  }
  return parsed;
}

function uuid(value, field, ErrorType = PublicationEnvelopeConflict) {
  const output = requiredText(value, field, ErrorType);
  if (!UUID.test(output)) throw new ErrorType(`${field} must be a UUID`);
  return output.toLowerCase();
}

function hash(value, field, { nullable = false } = {}) {
  if (nullable && value == null) return null;
  const output = requiredText(value, field);
  if (!HASH.test(output)) throw new PublicationEnvelopeConflict(`${field} must be a SHA-256 hash`);
  return output;
}

function expectedOperation(domain, revisionType) {
  if (domain === "channel") return revisionType === "retraction" ? "retract_channel" : "replace";
  if (domain === "video") return revisionType === "bootstrap" ? "replace_window" : "apply_window_delta";
  return revisionType === "retraction" ? "retract_agent" : "replace";
}

function exactKeys(value, allowed, field) {
  const missing = [...allowed].filter((key) => !Object.hasOwn(value, key));
  const extras = Object.keys(value).filter((key) => !allowed.has(key));
  if (missing.length > 0) {
    throw new PublicationEnvelopeConflict(`${field} is missing fields: ${missing.sort().join(",")}`);
  }
  if (extras.length > 0) {
    throw new PublicationEnvelopeConflict(`${field} contains unsupported fields: ${extras.sort().join(",")}`);
  }
}

export function publicationEnvelopeFromRow(row) {
  const revisionId = uuid(row?.revision_id, "revision_id");
  const publicationStreamId = uuid(row?.publication_stream_id, "publication_stream_id");
  const revisionType = requiredText(row?.revision_type, "revision_type");
  if (!REVISION_TYPES.has(revisionType)) {
    throw new PublicationEnvelopeConflict(`unsupported revision_type: ${revisionType}`);
  }
  const channelId = requiredText(row?.channel_id, "channel_id");
  const domain = requiredText(row?.domain, "domain");
  if (!DOMAINS.has(domain)) throw new PublicationEnvelopeConflict(`unsupported domain: ${domain}`);
  const dataSequence = safeInteger(row?.data_sequence, "data_sequence", { minimum: 1 });
  const previousDataSequence = row?.previous_data_sequence == null
    ? null
    : safeInteger(row.previous_data_sequence, "previous_data_sequence");
  const operation = requiredText(row?.operation, "operation");
  const requiredOperation = expectedOperation(domain, revisionType);
  if (operation !== requiredOperation) {
    throw new PublicationEnvelopeConflict(
      `operation ${operation} is invalid for ${domain}/${revisionType}`,
    );
  }
  const contractVersion = safeInteger(row?.contract_version, "contract_version", { minimum: 1 });
  if (!ENVELOPE_CONTRACT_VERSIONS.has(contractVersion)) {
    throw new PublicationEnvelopeConflict(`unsupported contract_version: ${contractVersion}`);
  }
  const previousResultHash = hash(row?.previous_result_hash, "previous_result_hash", { nullable: true });
  if (revisionType === "bootstrap") {
    if (dataSequence !== 1 || previousDataSequence !== null || previousResultHash !== null) {
      throw new PublicationEnvelopeConflict("Bootstrap must start at Sequence 1 without a previous version");
    }
  } else if (previousDataSequence !== dataSequence - 1 || previousResultHash === null) {
    throw new PublicationEnvelopeConflict("Delta previous Sequence and Result Hash are inconsistent");
  }
  const payload = jsonObject(row?.payload_json ?? row?.payload, "payload");
  const payloadHash = hash(row?.payload_hash, "payload_hash");
  if (observationFactsHash(payload) !== payloadHash) {
    throw new PublicationEnvelopeConflict("payload_hash does not match the canonical Payload", {
      revision_id: revisionId,
    });
  }

  return {
    revision_id: revisionId,
    publication_stream_id: publicationStreamId,
    revision_type: revisionType,
    channel_id: channelId,
    domain,
    data_sequence: dataSequence,
    previous_data_sequence: previousDataSequence,
    operation,
    contract_version: contractVersion,
    policy_version: requiredText(row?.policy_version, "policy_version"),
    occurred_at: timestamp(row?.occurred_at, "occurred_at"),
    source: jsonObject(row?.source_refs ?? row?.source, "source"),
    previous_result_hash: previousResultHash,
    result_hash: hash(row?.result_hash, "result_hash"),
    payload_hash: payloadHash,
    payload,
  };
}

export function normalizePublicationEnvelope(value) {
  const envelope = jsonObject(value, "envelope");
  exactKeys(envelope, ENVELOPE_KEYS, "envelope");
  return publicationEnvelopeFromRow({
    ...envelope,
    source_refs: envelope.source,
    payload_json: envelope.payload,
  });
}

export function publicationManifestHash(items) {
  return observationFactsHash({
    contract_version: PUBLICATION_SHARD_CONTRACT_VERSION,
    items,
  });
}

export function buildPublicationShard(itemsValue, {
  shardId = randomUUID(),
  createdAt = new Date(),
} = {}) {
  const items = [...(itemsValue ?? [])];
  if (items.length === 0) throw new PublicationEnvelopeConflict("a Shard requires at least one Revision");
  if (items.length > PUBLICATION_SHARD_MAX_ITEMS) {
    throw new PublicationEnvelopeConflict("a Shard cannot contain more than 100 Revisions");
  }
  return {
    shard_id: uuid(shardId, "shard_id"),
    created_at: timestamp(createdAt, "created_at"),
    contract_version: PUBLICATION_SHARD_CONTRACT_VERSION,
    manifest_hash: publicationManifestHash(items),
    items,
  };
}

export function normalizePublicationShard(value) {
  const shard = jsonObject(value, "shard");
  exactKeys(shard, SHARD_KEYS, "shard");
  const shardId = uuid(shard.shard_id, "shard_id");
  const contractVersion = safeInteger(shard.contract_version, "contract_version", { minimum: 1 });
  if (contractVersion !== PUBLICATION_SHARD_CONTRACT_VERSION) {
    throw new PublicationEnvelopeConflict(`unsupported Shard contract_version: ${contractVersion}`);
  }
  if (!Array.isArray(shard.items) || shard.items.length === 0) {
    throw new PublicationEnvelopeConflict("Shard items must be a non-empty array");
  }
  if (shard.items.length > PUBLICATION_SHARD_MAX_ITEMS) {
    throw new PublicationEnvelopeConflict("a Shard cannot contain more than 100 Revisions");
  }
  const items = shard.items.map(normalizePublicationEnvelope);
  if (new Set(items.map((item) => item.revision_id)).size !== items.length) {
    throw new PublicationEnvelopeConflict("a Shard cannot repeat a Revision ID");
  }
  const manifestHash = hash(shard.manifest_hash, "manifest_hash");
  if (publicationManifestHash(items) !== manifestHash) {
    throw new PublicationEnvelopeConflict("manifest_hash does not match the Shard items");
  }
  return {
    shard_id: shardId,
    created_at: timestamp(shard.created_at, "created_at"),
    contract_version: contractVersion,
    manifest_hash: manifestHash,
    items,
  };
}

function shardBytes(items, createdAt) {
  return Buffer.byteLength(JSON.stringify(buildPublicationShard(items, {
    shardId: "00000000-0000-4000-8000-000000000000",
    createdAt,
  })), "utf8");
}

export function planPublicationShards(envelopesValue, {
  maxItems = PUBLICATION_SHARD_MAX_ITEMS,
  maxBytes = PUBLICATION_SHARD_MAX_BYTES,
  createdAt = new Date(),
  shardId = randomUUID,
} = {}) {
  const requestedItemLimit = Number(maxItems);
  const requestedByteLimit = Number(maxBytes);
  if (!Number.isSafeInteger(requestedItemLimit) || requestedItemLimit < 1) {
    throw new TypeError("maxItems must be a positive integer");
  }
  if (!Number.isSafeInteger(requestedByteLimit) || requestedByteLimit < 1) {
    throw new TypeError("maxBytes must be a positive integer");
  }
  const itemLimit = Math.min(PUBLICATION_SHARD_MAX_ITEMS, requestedItemLimit);
  const byteLimit = Math.min(PUBLICATION_SHARD_MAX_BYTES, requestedByteLimit);
  const normalizedCreatedAt = timestamp(createdAt, "createdAt");
  const shards = [];
  const oversized = [];
  let pending = [];

  const finish = () => {
    if (pending.length === 0) return;
    shards.push(buildPublicationShard(pending, {
      shardId: shardId(),
      createdAt: normalizedCreatedAt,
    }));
    pending = [];
  };

  for (const envelope of envelopesValue ?? []) {
    const candidate = [...pending, envelope];
    if (candidate.length <= itemLimit && shardBytes(candidate, normalizedCreatedAt) <= byteLimit) {
      pending = candidate;
      continue;
    }
    finish();
    const bytes = shardBytes([envelope], normalizedCreatedAt);
    if (bytes > byteLimit) {
      oversized.push({ envelope, bytes });
    } else {
      pending = [envelope];
    }
  }
  finish();
  return { shards, oversized };
}

export function normalizePublicationReceiptResponse(value, shard) {
  const response = jsonObject(value, "response", PublicationReceiptConflict);
  const shardId = uuid(response.shard_id, "response.shard_id", PublicationReceiptConflict);
  if (shardId !== shard.shard_id) {
    throw new PublicationReceiptConflict("Receipt response Shard ID does not match the request");
  }
  if (!Array.isArray(response.receipts)) {
    throw new PublicationReceiptConflict("response.receipts must be an array");
  }
  const expected = new Map(shard.items.map((item) => [item.revision_id, item]));
  const receipts = new Map();
  for (const raw of response.receipts) {
    const receipt = jsonObject(raw, "receipt", PublicationReceiptConflict);
    const revisionId = uuid(receipt.revision_id, "receipt.revision_id", PublicationReceiptConflict);
    if (!expected.has(revisionId)) {
      throw new PublicationReceiptConflict("Receipt references a Revision outside this Shard", {
        revision_id: revisionId,
      });
    }
    if (receipts.has(revisionId)) {
      throw new PublicationReceiptConflict("Receipt response contains a duplicate Revision", {
        revision_id: revisionId,
      });
    }
    const status = requiredText(receipt.status, "receipt.status", PublicationReceiptConflict);
    if (!RECEIPT_STATUSES.has(status)) {
      throw new PublicationReceiptConflict(`unsupported Receipt status: ${status}`);
    }
    const payloadHash = receipt.payload_hash == null
      ? null
      : requiredText(receipt.payload_hash, "receipt.payload_hash", PublicationReceiptConflict);
    if (payloadHash && payloadHash !== expected.get(revisionId).payload_hash) {
      throw new PublicationReceiptConflict("Receipt Payload Hash does not match the Revision", {
        revision_id: revisionId,
      });
    }
    receipts.set(revisionId, {
      receipt_id: requiredText(receipt.receipt_id, "receipt.receipt_id", PublicationReceiptConflict),
      revision_id: revisionId,
      status,
      persisted_at: timestamp(receipt.persisted_at, "receipt.persisted_at", PublicationReceiptConflict),
      ...(payloadHash ? { payload_hash: payloadHash } : {}),
      ...(receipt.error_code == null ? {} : {
        error_code: String(receipt.error_code).slice(0, 255),
      }),
      ...(receipt.error_message == null ? {} : {
        error_message: String(receipt.error_message).slice(0, 1000),
      }),
    });
  }
  return receipts;
}

export function durableReceiptDelivered(receipt) {
  return SUCCESS_RECEIPT_STATUSES.has(receipt?.status);
}

function loopbackHostname(hostname) {
  return new Set(["127.0.0.1", "localhost", "::1", "[::1]"]).has(hostname);
}

export function validatePublicationIngressTarget(
  endpointValue,
  tokenValue,
  { trustedInternalHttpHostname = null } = {},
) {
  const endpoint = new URL(endpointValue);
  const loopback = loopbackHostname(endpoint.hostname);
  const trustedHostname = String(trustedInternalHttpHostname ?? "").trim().toLowerCase();
  const trustedInternalHttp = endpoint.protocol === "http:"
    && !loopback
    && trustedHostname.length > 0
    && endpoint.hostname.toLowerCase() === trustedHostname;
  if (endpoint.username || endpoint.password || endpoint.hash || endpoint.search) {
    throw new TypeError("Publication Ingress URL cannot contain credentials, query, or fragment");
  }
  if (endpoint.pathname.replace(/\/$/, "") !== "/internal/publications/v1/shards") {
    throw new TypeError("Publication Ingress URL must target /internal/publications/v1/shards");
  }
  if (loopback
    ? !["http:", "https:"].includes(endpoint.protocol)
    : endpoint.protocol !== "https:" && !trustedInternalHttp) {
    throw new TypeError("remote Publication Ingress must use HTTPS");
  }
  const token = String(tokenValue ?? "").trim();
  if (!loopback && !token) throw new TypeError("a token is required for remote Publication Ingress");
  return { endpoint: endpoint.toString(), token: token || null };
}

export class HttpPublicationIngressAdapter {
  constructor({
    endpoint,
    token = null,
    trustedInternalHttpHostname = null,
    timeoutMs = 15000,
    maximumResponseBytes = 1024 * 1024,
    fetchImpl = globalThis.fetch,
  }) {
    const target = validatePublicationIngressTarget(endpoint, token, {
      trustedInternalHttpHostname,
    });
    if (typeof fetchImpl !== "function") throw new TypeError("fetch implementation is required");
    this.endpoint = target.endpoint;
    this.token = target.token;
    this.timeoutMs = Math.max(100, Number(timeoutMs) || 15000);
    this.maximumResponseBytes = Math.max(1024, Number(maximumResponseBytes) || 1024 * 1024);
    this.fetchImpl = fetchImpl;
  }

  async acceptShard(shard) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const response = await this.fetchImpl(this.endpoint, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          accept: "application/json",
          "idempotency-key": shard.shard_id,
          ...(this.token ? { authorization: `Bearer ${this.token}` } : {}),
        },
        body: JSON.stringify(shard),
        signal: controller.signal,
      });
      const body = await response.text();
      if (Buffer.byteLength(body, "utf8") > this.maximumResponseBytes) {
        throw new PublicationIngressHttpError("Publication Ingress response is too large", response.status);
      }
      let parsed;
      try {
        parsed = body ? JSON.parse(body) : {};
      } catch {
        throw new PublicationIngressHttpError("Publication Ingress returned invalid JSON", response.status);
      }
      if (!response.ok) {
        const message = String(parsed?.error ?? `Publication Ingress returned HTTP ${response.status}`)
          .slice(0, 500);
        throw new PublicationIngressHttpError(message, response.status);
      }
      return parsed;
    } finally {
      clearTimeout(timer);
    }
  }
}
