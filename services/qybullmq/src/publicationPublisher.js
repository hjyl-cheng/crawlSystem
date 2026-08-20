import {
  PublicationEnvelopeConflict,
  durableReceiptDelivered,
  normalizePublicationReceiptResponse,
  planPublicationShards,
  publicationEnvelopeFromRow,
} from "./publicationTransport.js";

function positiveInteger(value, fallback, maximum = Number.MAX_SAFE_INTEGER) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isSafeInteger(parsed) && parsed > 0 ? Math.min(parsed, maximum) : fallback;
}

function optionalPositiveInteger(value, field, maximum) {
  if (value == null) return undefined;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return Math.min(parsed, maximum);
}

export function publicationRetryDelayMs(attempt, {
  baseMs = 5000,
  maximumMs = 900000,
  random = Math.random,
} = {}) {
  const exponent = Math.max(0, positiveInteger(attempt, 1, 30) - 1);
  const jitter = 0.75 + (Math.max(0, Math.min(1, Number(random()) || 0)) * 0.5);
  return Math.max(1000, Math.round(Math.min(maximumMs, baseMs * (2 ** exponent)) * jitter));
}

function errorText(error) {
  return String(error?.message || error).slice(0, 2000);
}

export class PostgresPublicationOutboxStore {
  constructor({ query, withTransaction }) {
    if (typeof query !== "function" || typeof withTransaction !== "function") {
      throw new TypeError("query and withTransaction are required");
    }
    this.query = query;
    this.withTransaction = withTransaction;
  }

  async claimBatch({ destination, leaseOwner, batchSize, leaseSeconds }) {
    return this.withTransaction(async (client) => {
      await client.query(
        `/* publication-publisher:release-retries */
         UPDATE publication.outbox
         SET status='pending',updated_at=now()
         WHERE destination=$1 AND status='retry_wait' AND next_attempt_at<=now()`,
        [destination],
      );
      const result = await client.query(
        `/* publication-publisher:claim */
         WITH claimable AS (
           SELECT outbox.revision_id
           FROM publication.outbox AS outbox
           JOIN publication.revision AS revision ON revision.revision_id=outbox.revision_id
           WHERE outbox.destination=$1
             AND (
               (outbox.status='pending' AND outbox.next_attempt_at<=now())
               OR (outbox.status='leased' AND outbox.lease_expires_at<=now())
             )
           ORDER BY revision.occurred_at,revision.revision_id
           FOR UPDATE OF outbox SKIP LOCKED
           LIMIT $2
         )
         UPDATE publication.outbox AS outbox
         SET status='leased',attempts=outbox.attempts+1,
             lease_owner=$3,lease_expires_at=now()+($4::int*interval '1 second'),
             updated_at=now()
         FROM claimable,publication.revision AS revision
         WHERE outbox.destination=$1
           AND outbox.revision_id=claimable.revision_id
           AND revision.revision_id=outbox.revision_id
         RETURNING outbox.destination,outbox.revision_id,outbox.attempts,
                   outbox.lease_expires_at,revision.publication_stream_id,
                   revision.channel_id,revision.domain,revision.data_sequence,
                   revision.previous_data_sequence,revision.revision_type,
                   revision.operation,revision.contract_version,revision.policy_version,
                   revision.occurred_at,revision.source_refs,revision.previous_result_hash,
                   revision.result_hash,revision.payload_hash,revision.payload_json`,
        [destination, batchSize, leaseOwner, leaseSeconds],
      );
      return result.rows;
    });
  }

  async markDelivered({ destination, revisionId, leaseOwner, receipt }) {
    const result = await this.query(
      `/* publication-publisher:delivered */
       UPDATE publication.outbox
       SET status='delivered',lease_owner=NULL,lease_expires_at=NULL,last_error=NULL,
           receipt_id=$4,receipt_status=$5,receipt_received_at=now(),
           receipt_json=$6::jsonb,delivered_at=now(),updated_at=now()
       WHERE destination=$1 AND revision_id=$2::uuid
         AND status='leased' AND lease_owner=$3
       RETURNING status`,
      [
        destination,
        revisionId,
        leaseOwner,
        receipt.receipt_id,
        receipt.status,
        JSON.stringify(receipt),
      ],
    );
    return result.rows[0]?.status ?? null;
  }

  async markRejected({ destination, revisionId, leaseOwner, receipt }) {
    const result = await this.query(
      `/* publication-publisher:rejected */
       UPDATE publication.outbox
       SET status='dead_letter',lease_owner=NULL,lease_expires_at=NULL,
           last_error=$4,receipt_id=$5,receipt_status=$6,
           receipt_received_at=now(),receipt_json=$7::jsonb,updated_at=now()
       WHERE destination=$1 AND revision_id=$2::uuid
         AND status='leased' AND lease_owner=$3
       RETURNING status`,
      [
        destination,
        revisionId,
        leaseOwner,
        `Business Ingress ${receipt.status}: ${receipt.error_code ?? "permanent_rejection"}`,
        receipt.receipt_id,
        receipt.status,
        JSON.stringify(receipt),
      ],
    );
    return result.rows[0]?.status ?? null;
  }

  async markFailed({ destination, revisionId, leaseOwner, deadLetter, retryDelayMs, error }) {
    const result = await this.query(
      `/* publication-publisher:failed */
       UPDATE publication.outbox
       SET status=CASE WHEN $4::boolean THEN 'dead_letter' ELSE 'retry_wait' END,
           next_attempt_at=CASE
             WHEN $4::boolean THEN next_attempt_at
             ELSE now()+($5::int*interval '1 millisecond')
           END,
           lease_owner=NULL,lease_expires_at=NULL,last_error=$6,updated_at=now()
       WHERE destination=$1 AND revision_id=$2::uuid
         AND status='leased' AND lease_owner=$3
       RETURNING status`,
      [destination, revisionId, leaseOwner, deadLetter, retryDelayMs, errorText(error)],
    );
    return result.rows[0]?.status ?? null;
  }
}

export class PublicationPublisher {
  constructor({
    store,
    ingress,
    destination = "business",
    leaseOwner,
    batchSize = 100,
    leaseSeconds = 60,
    maxAttempts = 12,
    maximumShardBytes,
    retryDelay = publicationRetryDelayMs,
    clock = () => new Date(),
    logger = console,
  }) {
    if (!store || typeof store.claimBatch !== "function") throw new TypeError("store is required");
    if (!ingress || typeof ingress.acceptShard !== "function") throw new TypeError("ingress is required");
    this.store = store;
    this.ingress = ingress;
    this.destination = String(destination ?? "").trim();
    this.leaseOwner = String(leaseOwner ?? "").trim();
    if (!this.destination || !this.leaseOwner) {
      throw new TypeError("destination and leaseOwner are required");
    }
    this.batchSize = positiveInteger(batchSize, 100, 500);
    this.leaseSeconds = positiveInteger(leaseSeconds, 60, 3600);
    this.maxAttempts = positiveInteger(maxAttempts, 12, 100);
    this.maximumShardBytes = optionalPositiveInteger(
      maximumShardBytes,
      "maximumShardBytes",
      4 * 1024 * 1024,
    );
    this.retryDelay = retryDelay;
    this.clock = clock;
    this.logger = logger;
  }

  async settleFailure(row, error, summary, { permanent = false } = {}) {
    const deadLetter = permanent || Number(row.attempts) >= this.maxAttempts;
    const status = await this.store.markFailed({
      destination: this.destination,
      revisionId: row.revision_id,
      leaseOwner: this.leaseOwner,
      deadLetter,
      retryDelayMs: this.retryDelay(row.attempts),
      error,
    });
    if (status === "dead_letter") summary.dead_lettered += 1;
    else if (status === "retry_wait") summary.retried += 1;
    else summary.lease_lost += 1;
  }

  async runOnce() {
    const rows = await this.store.claimBatch({
      destination: this.destination,
      leaseOwner: this.leaseOwner,
      batchSize: this.batchSize,
      leaseSeconds: this.leaseSeconds,
    });
    const summary = {
      claimed: rows.length,
      shards: 0,
      delivered: 0,
      retried: 0,
      dead_lettered: 0,
      lease_lost: 0,
    };
    const rowByRevision = new Map();
    const envelopes = [];
    for (const row of rows) {
      try {
        const envelope = publicationEnvelopeFromRow(row);
        rowByRevision.set(envelope.revision_id, row);
        envelopes.push(envelope);
      } catch (error) {
        await this.settleFailure(row, error, summary, {
          permanent: error instanceof PublicationEnvelopeConflict,
        });
      }
    }

    const plan = planPublicationShards(envelopes, {
      maxItems: Math.min(this.batchSize, 100),
      ...(this.maximumShardBytes == null ? {} : { maxBytes: this.maximumShardBytes }),
      createdAt: this.clock(),
    });
    for (const oversized of plan.oversized) {
      const row = rowByRevision.get(oversized.envelope.revision_id);
      await this.settleFailure(
        row,
        new PublicationEnvelopeConflict(
          `Revision exceeds the Publication Shard byte limit (${oversized.bytes} bytes)`,
        ),
        summary,
        { permanent: true },
      );
    }

    for (const shard of plan.shards) {
      summary.shards += 1;
      let receipts;
      try {
        const response = await this.ingress.acceptShard(shard);
        receipts = normalizePublicationReceiptResponse(response, shard);
      } catch (error) {
        for (const envelope of shard.items) {
          await this.settleFailure(rowByRevision.get(envelope.revision_id), error, summary);
        }
        this.logger.error?.(JSON.stringify({
          event: "publication_shard_failed",
          destination: this.destination,
          shard_id: shard.shard_id,
          item_count: shard.items.length,
          error: errorText(error),
        }));
        continue;
      }

      for (const envelope of shard.items) {
        const row = rowByRevision.get(envelope.revision_id);
        const receipt = receipts.get(envelope.revision_id);
        if (!receipt) {
          await this.settleFailure(
            row,
            new Error(`Durable Receipt missing for Revision ${envelope.revision_id}`),
            summary,
          );
          continue;
        }
        const status = durableReceiptDelivered(receipt)
          ? await this.store.markDelivered({
              destination: this.destination,
              revisionId: envelope.revision_id,
              leaseOwner: this.leaseOwner,
              receipt,
            })
          : await this.store.markRejected({
              destination: this.destination,
              revisionId: envelope.revision_id,
              leaseOwner: this.leaseOwner,
              receipt,
            });
        if (status === "delivered") summary.delivered += 1;
        else if (status === "dead_letter") summary.dead_lettered += 1;
        else summary.lease_lost += 1;
      }
      this.logger.info?.(JSON.stringify({
        event: "publication_shard_receipts_stored",
        destination: this.destination,
        shard_id: shard.shard_id,
        item_count: shard.items.length,
      }));
    }
    return summary;
  }
}
