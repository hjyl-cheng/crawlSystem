import { randomUUID, timingSafeEqual } from "node:crypto";
import express from "express";
import {
  BusinessPublicationContractError,
  validateBusinessPublicationEnvelope,
} from "./businessPublicationContract.js";
import { ensureAutomaticBusinessBootstrapOwnership } from "./businessPublicationOwnership.js";
import { observationFactsHash } from "./crawlObservationStore.js";
import { normalizePublicationShard } from "./publicationTransport.js";

const INGRESS_PATH = "/internal/publications/v1/shards";
const REVISION_BACKED_INBOX_STATUSES = new Set([
  "accepted",
  "waiting_gap",
  "waiting_ownership",
]);

function iso(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new TypeError("stored receipt timestamp is invalid");
  return parsed.toISOString();
}

function receiptStatus(receiveStatus, duplicate = false) {
  if (receiveStatus === "rejected" || receiveStatus === "conflict") return receiveStatus;
  if (duplicate) return "duplicate";
  return receiveStatus === "accepted" ? "accepted" : "waiting_gap";
}

function receipt(row, envelope, { duplicate = false, errorCode = null, errorMessage = null } = {}) {
  return {
    receipt_id: String(row.receipt_id),
    revision_id: envelope.revision_id,
    status: receiptStatus(row.receive_status, duplicate),
    persisted_at: iso(row.first_received_at),
    payload_hash: envelope.payload_hash,
    ...(errorCode ? { error_code: errorCode } : {}),
    ...(errorMessage ? { error_message: String(errorMessage).slice(0, 1000) } : {}),
  };
}

function issueHash(details) {
  return observationFactsHash(details);
}

async function quarantine(client, envelope, issueCode, details) {
  const hash = issueHash(details);
  await client.query(
    `/* business-publication-ingress:quarantine */
     INSERT INTO publication.quarantine (
       quarantine_id,revision_id,issue_code,issue_hash,details_json
     ) VALUES ($1,$2::uuid,$3,$4,$5::jsonb)
     ON CONFLICT (revision_id,issue_code,issue_hash) DO UPDATE
     SET last_seen_at=now()`,
    [randomUUID(), envelope.revision_id, issueCode, hash, JSON.stringify(details)],
  );
}

async function duplicateReceipt(client, existing, envelope, envelopeHash) {
  const same = existing.payload_hash === envelope.payload_hash
    && existing.envelope_hash === envelopeHash;
  await client.query(
    `/* business-publication-ingress:repeat-inbox */
     UPDATE publication.inbox
     SET receive_count=receive_count+1,last_received_at=now()
     WHERE revision_id=$1::uuid`,
    [envelope.revision_id],
  );
  if (same) {
    return receipt(existing, envelope, {
      duplicate: !["rejected", "conflict"].includes(existing.receive_status),
      errorCode: existing.error_code,
      errorMessage: existing.error_message,
    });
  }

  const conflict = await client.query(
    `/* business-publication-ingress:revision-conflict */
     INSERT INTO publication.inbox_conflict (
       conflict_id,revision_id,conflicting_payload_hash,
       conflicting_envelope_hash,conflicting_envelope
     ) VALUES ($1,$2::uuid,$3,$4,$5::jsonb)
     ON CONFLICT (revision_id,conflicting_envelope_hash) DO UPDATE
     SET receive_count=publication.inbox_conflict.receive_count+1,last_received_at=now()
     RETURNING conflict_id AS receipt_id,first_received_at`,
    [
      randomUUID(),
      envelope.revision_id,
      envelope.payload_hash,
      envelopeHash,
      JSON.stringify(envelope),
    ],
  );
  const details = {
    code: "revision_envelope_conflict",
    stored_envelope_hash: existing.envelope_hash,
    conflicting_envelope_hash: envelopeHash,
  };
  await quarantine(client, envelope, "revision_envelope_conflict", details);
  return receipt({
    ...conflict.rows[0],
    receive_status: "conflict",
  }, envelope, {
    errorCode: "revision_envelope_conflict",
    errorMessage: "Revision ID already exists with different immutable evidence",
  });
}

async function insertInbox(client, envelope, envelopeHash, {
  receiveStatus,
  errorCode = null,
  errorMessage = null,
}) {
  const result = await client.query(
    `/* business-publication-ingress:insert-inbox */
     INSERT INTO publication.inbox (
       revision_id,publication_stream_id,channel_id,domain,data_sequence,
       payload_hash,envelope_hash,received_envelope,receipt_id,
       receive_status,error_code,error_message
     ) VALUES (
       $1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8::jsonb,$9::uuid,$10,$11,$12
     )
     RETURNING receipt_id,receive_status,error_code,error_message,first_received_at`,
    [
      envelope.revision_id,
      envelope.publication_stream_id,
      envelope.channel_id,
      envelope.domain,
      envelope.data_sequence,
      envelope.payload_hash,
      envelopeHash,
      REVISION_BACKED_INBOX_STATUSES.has(receiveStatus) ? null : JSON.stringify(envelope),
      randomUUID(),
      receiveStatus,
      errorCode,
      errorMessage == null ? null : String(errorMessage).slice(0, 2000),
    ],
  );
  return result.rows[0];
}

async function insertRevision(client, envelope, envelopeHash, ingressStatus) {
  const activationStatus = ingressStatus === "accepted"
    ? "staged"
    : ingressStatus === "waiting_gap" ? "waiting_gap" : "waiting_ownership";
  await client.query(
    `/* business-publication-ingress:insert-revision */
     INSERT INTO publication.revision (
       revision_id,publication_stream_id,channel_id,domain,data_sequence,
       previous_data_sequence,revision_type,operation,contract_version,policy_version,
       occurred_at,source_json,previous_result_hash,result_hash,payload_hash,
       payload_json,envelope_hash,ingress_status,activation_status
     ) VALUES (
       $1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,
       $11::timestamptz,$12::jsonb,$13,$14,$15,$16::jsonb,$17,$18,$19
     )`,
    [
      envelope.revision_id,
      envelope.publication_stream_id,
      envelope.channel_id,
      envelope.domain,
      envelope.data_sequence,
      envelope.previous_data_sequence,
      envelope.revision_type,
      envelope.operation,
      envelope.contract_version,
      envelope.policy_version,
      envelope.occurred_at,
      JSON.stringify(envelope.source),
      envelope.previous_result_hash,
      envelope.result_hash,
      envelope.payload_hash,
      JSON.stringify(envelope.payload),
      envelopeHash,
      ingressStatus,
      activationStatus,
    ],
  );
}

async function newEnvelopeReceipt(client, envelope, envelopeHash) {
  const streamResult = await client.query(
    `/* business-publication-ingress:stream */
     SELECT status,accepted_contract_versions
     FROM publication.stream
     WHERE publication_stream_id=$1::uuid
     FOR SHARE`,
    [envelope.publication_stream_id],
  );
  const stream = streamResult.rows[0];
  let receiveStatus = "accepted";
  let errorCode = null;
  let errorMessage = null;

  if (!stream) {
    receiveStatus = "rejected";
    errorCode = "unknown_publication_stream";
    errorMessage = "Publication Stream is not registered";
  } else if (stream.status !== "active") {
    receiveStatus = "rejected";
    errorCode = "inactive_publication_stream";
    errorMessage = `Publication Stream is ${stream.status}`;
  } else if (!(stream.accepted_contract_versions ?? []).includes(envelope.contract_version)) {
    receiveStatus = "rejected";
    errorCode = "unsupported_contract_version";
    errorMessage = `Contract Version ${envelope.contract_version} is not accepted`;
  }

  if (receiveStatus === "accepted") {
    try {
      validateBusinessPublicationEnvelope(envelope);
    } catch (error) {
      if (!(error instanceof BusinessPublicationContractError)) throw error;
      receiveStatus = "rejected";
      errorCode = error.code;
      errorMessage = error.message;
    }
  }

  if (receiveStatus === "accepted") {
    const collision = await client.query(
      `/* business-publication-ingress:sequence-collision */
       SELECT revision_id,payload_hash,envelope_hash
       FROM publication.revision
       WHERE publication_stream_id=$1::uuid AND channel_id=$2
         AND domain=$3 AND data_sequence=$4`,
      [
        envelope.publication_stream_id,
        envelope.channel_id,
        envelope.domain,
        envelope.data_sequence,
      ],
    );
    if (collision.rows.length > 0) {
      receiveStatus = "conflict";
      errorCode = "stream_sequence_conflict";
      errorMessage = "Stream/Channel/Domain Sequence already belongs to another Revision";
    }
  }

  if (receiveStatus === "accepted") {
    await ensureAutomaticBusinessBootstrapOwnership(client, envelope);
    const ownership = await client.query(
      `/* business-publication-ingress:ownership */
       SELECT active_publication_stream_id,status
       FROM publication.channel_ownership
       WHERE channel_id=$1`,
      [envelope.channel_id],
    );
    const owner = ownership.rows[0];
    if (
      !owner
      || owner.status !== "active"
      || String(owner.active_publication_stream_id) !== envelope.publication_stream_id
    ) {
      receiveStatus = "waiting_ownership";
      errorCode = "waiting_ownership";
      errorMessage = "Revision is staged until this Stream owns the Channel";
    } else if (envelope.revision_type !== "bootstrap") {
      const previous = await client.query(
        `/* business-publication-ingress:previous */
         SELECT result_hash,ingress_status
         FROM publication.revision
         WHERE publication_stream_id=$1::uuid AND channel_id=$2
           AND domain=$3 AND data_sequence=$4`,
        [
          envelope.publication_stream_id,
          envelope.channel_id,
          envelope.domain,
          envelope.previous_data_sequence,
        ],
      );
      const prior = previous.rows[0];
      if (prior && prior.result_hash !== envelope.previous_result_hash) {
        receiveStatus = "conflict";
        errorCode = "previous_result_hash_conflict";
        errorMessage = "Previous Sequence exists with a different Result Hash";
      } else if (!prior || prior.ingress_status !== "accepted") {
        receiveStatus = "waiting_gap";
        errorCode = "waiting_sequence_gap";
        errorMessage = "Previous Sequence has not been accepted";
      }
    }
  }

  const inbox = await insertInbox(client, envelope, envelopeHash, {
    receiveStatus,
    errorCode,
    errorMessage,
  });
  if (REVISION_BACKED_INBOX_STATUSES.has(receiveStatus)) {
    await insertRevision(client, envelope, envelopeHash, receiveStatus);
  } else {
    await quarantine(client, envelope, errorCode, {
      code: errorCode,
      message: errorMessage,
      envelope_hash: envelopeHash,
    });
  }
  return receipt(inbox, envelope, { errorCode, errorMessage });
}

async function acceptEnvelope(client, envelope) {
  const envelopeHash = observationFactsHash(envelope);
  const existingResult = await client.query(
    `/* business-publication-ingress:existing-inbox */
     SELECT receipt_id,receive_status,error_code,error_message,
            payload_hash,envelope_hash,first_received_at
     FROM publication.inbox
     WHERE revision_id=$1::uuid
     FOR UPDATE`,
    [envelope.revision_id],
  );
  const existing = existingResult.rows[0];
  if (existing) return duplicateReceipt(client, existing, envelope, envelopeHash);
  return newEnvelopeReceipt(client, envelope, envelopeHash);
}

export class PostgresBusinessPublicationStore {
  constructor(pool) {
    if (!pool || typeof pool.connect !== "function") throw new TypeError("a PostgreSQL Pool is required");
    this.pool = pool;
  }

  async ping() {
    await this.pool.query("SELECT 1");
  }

  async acceptShard(shard) {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const lockKeys = new Set();
      for (const envelope of shard.items) {
        lockKeys.add(`revision:${envelope.revision_id}`);
        lockKeys.add(`route:${observationFactsHash({
          publication_stream_id: envelope.publication_stream_id,
          channel_id: envelope.channel_id,
          domain: envelope.domain,
        })}`);
      }
      for (const key of [...lockKeys].sort()) {
        await client.query(
          "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
          [key],
        );
      }
      const receipts = [];
      for (const envelope of shard.items) {
        receipts.push(await acceptEnvelope(client, envelope));
      }
      await client.query("COMMIT");
      return { shard_id: shard.shard_id, receipts };
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }
}

function authorized(headerValue, token) {
  const value = String(headerValue ?? "");
  const expected = `Bearer ${token}`;
  const left = Buffer.from(value);
  const right = Buffer.from(expected);
  return left.length === right.length && timingSafeEqual(left, right);
}

export function createBusinessPublicationIngressApp({
  store,
  token,
  logger = console,
  maximumBodyBytes = 4 * 1024 * 1024,
}) {
  if (!store || typeof store.acceptShard !== "function") throw new TypeError("store is required");
  const normalizedToken = String(token ?? "").trim();
  if (!normalizedToken) throw new TypeError("Ingress token is required");
  const bodyLimit = Number(maximumBodyBytes);
  if (!Number.isSafeInteger(bodyLimit) || bodyLimit < 1024 || bodyLimit > 4 * 1024 * 1024) {
    throw new TypeError("maximumBodyBytes must be between 1024 and 4194304");
  }
  const app = express();
  app.disable("x-powered-by");
  app.get("/health", async (_request, response) => {
    try {
      await store.ping?.();
      response.status(200).json({ overall: "ok", postgres: "ok" });
    } catch {
      response.status(503).json({ overall: "degraded", postgres: "fail" });
    }
  });
  app.post(
    INGRESS_PATH,
    (request, response, next) => {
      if (!authorized(request.headers.authorization, normalizedToken)) {
        response.status(401).json({ error: "unauthorized" });
        return;
      }
      next();
    },
    express.json({ limit: bodyLimit, strict: true, type: "application/json" }),
    async (request, response) => {
      let shard;
      try {
        shard = normalizePublicationShard(request.body);
      } catch (error) {
        response.status(400).json({ error: String(error?.message || error).slice(0, 500) });
        return;
      }
      try {
        const result = await store.acceptShard(shard);
        response.status(200).json(result);
      } catch (error) {
        logger.error?.(JSON.stringify({
          event: "business_publication_ingress_failed",
          shard_id: shard.shard_id,
          item_count: shard.items.length,
          error: String(error?.message || error).slice(0, 1000),
        }));
        response.status(503).json({ error: "publication ingress unavailable" });
      }
    },
  );
  app.use((error, _request, response, _next) => {
    const status = error?.type === "entity.too.large" ? 413 : 400;
    response.status(status).json({ error: status === 413 ? "request body too large" : "invalid JSON" });
  });
  return app;
}
