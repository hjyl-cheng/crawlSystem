import { createHash, randomUUID as nodeRandomUUID } from "node:crypto";
import { LEGACY_FULL_CRAWL_FETCH_CONTRACT } from "./fullCrawlFetchContract.js";

export const BUSINESS_RUN_INTENT_SCHEMA_VERSION = 1;

const OPTIONAL_NULL_INTENT_FIELDS = Object.freeze([
  "checkpoint_target_run_id",
  "publication_gap_domains",
  "publication_gap_root_run_id",
  "publication_gap_scope",
]);

export class BusinessRunBindingConflictError extends Error {
  constructor(businessRunKey) {
    super(`BUSINESS_RUN_KEY_CONFLICT: ${businessRunKey}`);
    this.name = "BusinessRunBindingConflictError";
    this.code = "BUSINESS_RUN_KEY_CONFLICT";
    this.businessRunKey = businessRunKey;
  }
}

function required(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${field} is required`);
  return normalized;
}

function positiveInteger(value, field) {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return number;
}

function canonicalValue(value) {
  if (Array.isArray(value)) return value.map(canonicalValue);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.keys(value).sort().map((key) => [key, canonicalValue(value[key])]),
  );
}

export function businessRunIntentHash(value) {
  return `sha256:${createHash("sha256")
    .update(JSON.stringify(canonicalValue(value)))
    .digest("hex")}`;
}

function intentJson(value) {
  if (typeof value === "string") {
    try {
      return JSON.parse(value);
    } catch {
      return null;
    }
  }
  return value && typeof value === "object" && !Array.isArray(value) ? value : null;
}

function optionalNullCompatibleIntent(value) {
  const immutableIntent = intentJson(value);
  const nestedIntent = intentJson(immutableIntent?.intent);
  if (!immutableIntent || !nestedIntent) return immutableIntent;
  const normalizedIntent = { ...nestedIntent };
  for (const field of OPTIONAL_NULL_INTENT_FIELDS) {
    if (!Object.prototype.hasOwnProperty.call(normalizedIntent, field)) {
      normalizedIntent[field] = null;
    }
  }
  if (["full", "full_repair"].includes(immutableIntent.run_kind)
      && !Object.prototype.hasOwnProperty.call(normalizedIntent, "fetch_contract")) {
    normalizedIntent.fetch_contract = LEGACY_FULL_CRAWL_FETCH_CONTRACT;
  }
  return canonicalValue({ ...immutableIntent, intent: normalizedIntent });
}

function sameImmutableIntent(row, value) {
  if (row.intent_hash === value.intentHash) return true;
  if (Number(row.intent_schema_version) !== BUSINESS_RUN_INTENT_SCHEMA_VERSION) return false;
  const storedIntent = intentJson(row.intent_json);
  if (!storedIntent || businessRunIntentHash(storedIntent) !== row.intent_hash) return false;

  // These fields were added as optional nulls without changing task semantics.
  return businessRunIntentHash(optionalNullCompatibleIntent(storedIntent))
    === businessRunIntentHash(optionalNullCompatibleIntent(value.immutableIntent));
}

function normalizedInput(input) {
  const policy = input?.policy ?? {};
  const runKind = required(input?.runKind, "runKind");
  if (!["full", "incremental", "full_repair"].includes(runKind)) {
    throw new TypeError(`unsupported runKind: ${runKind}`);
  }
  const value = {
    businessRunKey: required(input?.businessRunKey, "businessRunKey"),
    runKind,
    channelId: required(input?.channelId, "channelId"),
    candidateId: input?.candidateId == null ? null : positiveInteger(input.candidateId, "candidateId"),
    planId: String(input?.planId ?? "").trim() || null,
    fullIntentId: String(input?.fullIntentId ?? "").trim() || null,
    policy: {
      id: required(policy.id, "policy.id"),
      version: positiveInteger(policy.version, "policy.version"),
      hash: required(policy.hash, "policy.hash"),
    },
    explicitBusinessRunId: String(input?.explicitBusinessRunId ?? "").trim() || null,
    requestedStatus: input?.requestedStatus === "materialized" ? "materialized" : "reserved",
    intent: canonicalValue(input?.intent ?? {}),
  };
  const immutableIntent = canonicalValue({
    schema_version: BUSINESS_RUN_INTENT_SCHEMA_VERSION,
    business_run_key: value.businessRunKey,
    run_kind: value.runKind,
    channel_id: value.channelId,
    candidate_id: value.candidateId,
    plan_id: value.planId,
    full_intent_id: value.fullIntentId,
    identity_policy_id: value.policy.id,
    identity_policy_version: value.policy.version,
    identity_policy_hash: value.policy.hash,
    intent: value.intent,
  });
  return { ...value, immutableIntent, intentHash: businessRunIntentHash(immutableIntent) };
}

function assertSameBinding(row, value) {
  if (!sameImmutableIntent(row, value)
      || row.identity_policy_id !== value.policy.id
      || Number(row.identity_policy_version) !== value.policy.version
      || row.identity_policy_hash !== value.policy.hash
      || row.run_kind !== value.runKind
      || row.channel_id !== value.channelId
      || (row.candidate_id == null ? null : Number(row.candidate_id)) !== value.candidateId
      || (row.plan_id ?? null) !== value.planId
      || (row.full_intent_id ?? null) !== value.fullIntentId
      || (value.explicitBusinessRunId && row.business_run_id !== value.explicitBusinessRunId)) {
    throw new BusinessRunBindingConflictError(value.businessRunKey);
  }
}

export class BusinessRunBindingStore {
  constructor({ withTransaction, randomUUID = nodeRandomUUID } = {}) {
    if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
    this.withTransaction = withTransaction;
    this.randomUUID = randomUUID;
  }

  async resolve(input) {
    const value = normalizedInput(input);
    return this.withTransaction(async (client) => {
      await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [
        `business-run-binding:${value.businessRunKey}`,
      ]);
      const existing = await client.query(
        "SELECT * FROM crawler.business_run_bindings WHERE business_run_key=$1 FOR UPDATE",
        [value.businessRunKey],
      );
      let row = existing.rows[0] ?? null;
      let created = false;
      if (row) {
        assertSameBinding(row, value);
      } else {
        const businessRunId = value.explicitBusinessRunId || `run:${this.randomUUID()}`;
        const inserted = await client.query(
          `INSERT INTO crawler.business_run_bindings (
             business_run_key,business_run_id,intent_schema_version,intent_hash,intent_json,
             identity_policy_id,identity_policy_version,identity_policy_hash,run_kind,
             channel_id,candidate_id,plan_id,full_intent_id,status,materialized_at
           ) VALUES (
             $1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10,$11,$12,$13,$14,
             CASE WHEN $14='materialized' THEN now() ELSE NULL END
           ) RETURNING *`,
          [
            value.businessRunKey,
            businessRunId,
            BUSINESS_RUN_INTENT_SCHEMA_VERSION,
            value.intentHash,
            JSON.stringify(value.immutableIntent),
            value.policy.id,
            value.policy.version,
            value.policy.hash,
            value.runKind,
            value.channelId,
            value.candidateId,
            value.planId,
            value.fullIntentId,
            value.requestedStatus,
          ],
        );
        row = inserted.rows[0];
        created = true;
      }
      if (row.status === "terminal") return { created, terminal: true, binding: row };
      return { created, terminal: false, binding: row };
    });
  }
}

export async function materializeBusinessRunBinding(client, {
  businessRunKey,
  businessRunId,
} = {}) {
  const key = required(businessRunKey, "businessRunKey");
  const runId = required(businessRunId, "businessRunId");
  const updated = await client.query(
    `UPDATE crawler.business_run_bindings
     SET status='materialized',materialized_at=COALESCE(materialized_at,now()),
         terminal_reason=NULL,updated_at=now()
     WHERE business_run_key=$1 AND business_run_id=$2 AND status IN ('reserved','materialized')
     RETURNING *`,
    [key, runId],
  );
  if (updated.rowCount !== 1) throw new BusinessRunBindingConflictError(key);
  const frozen = await client.query(
    `UPDATE crawler.channel_runs run
     SET identity_policy_id=binding.identity_policy_id,
         identity_policy_version=binding.identity_policy_version,
         identity_policy_hash=binding.identity_policy_hash,
         result_json=CASE
           WHEN binding.intent_json#>'{intent,fetch_contract}' IS NULL THEN run.result_json
           ELSE jsonb_set(
             COALESCE(run.result_json,'{}'::jsonb),
             '{fetch_contract}',
             binding.intent_json#>'{intent,fetch_contract}',
             true
           )
         END,
         updated_at=now()
     FROM crawler.business_run_bindings binding
     WHERE binding.business_run_key=$1 AND binding.business_run_id=$2
       AND run.run_id=binding.business_run_id
       AND (run.identity_policy_id IS NULL OR run.identity_policy_id=binding.identity_policy_id)
       AND (run.identity_policy_version IS NULL OR run.identity_policy_version=binding.identity_policy_version)
       AND (run.identity_policy_hash IS NULL OR run.identity_policy_hash=binding.identity_policy_hash)
       AND (
         binding.intent_json#>'{intent,fetch_contract}' IS NULL
         OR run.result_json->'fetch_contract' IS NULL
         OR run.result_json->'fetch_contract'=binding.intent_json#>'{intent,fetch_contract}'
       )
     RETURNING run.run_id,run.result_json`,
    [key, runId],
  );
  if (frozen.rowCount !== 1) throw new BusinessRunBindingConflictError(key);
  return updated.rows[0];
}

export async function terminateBusinessRunBinding(client, {
  businessRunKey,
  businessRunId,
  reason,
} = {}) {
  const key = required(businessRunKey, "businessRunKey");
  const runId = required(businessRunId, "businessRunId");
  const terminalReason = required(reason, "reason");
  const updated = await client.query(
    `UPDATE crawler.business_run_bindings
     SET status='terminal',terminal_reason=$3,updated_at=now()
     WHERE business_run_key=$1 AND business_run_id=$2 AND status IN ('reserved','terminal')
     RETURNING *`,
    [key, runId, terminalReason],
  );
  if (updated.rowCount !== 1) {
    const existing = await client.query(
      "SELECT * FROM crawler.business_run_bindings WHERE business_run_key=$1 AND business_run_id=$2",
      [key, runId],
    );
    if (existing.rows[0]?.status === "materialized") return existing.rows[0];
    throw new BusinessRunBindingConflictError(key);
  }
  return updated.rows[0];
}
