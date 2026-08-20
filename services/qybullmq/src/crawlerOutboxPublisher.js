import { OutboxEnvelopeConflict, publishCrawlerOutboxRow } from "./featureTransport.js";

function positiveInteger(value, fallback, maximum = Number.MAX_SAFE_INTEGER) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isSafeInteger(parsed) && parsed > 0 ? Math.min(parsed, maximum) : fallback;
}

export function outboxRetryDelayMs(attempt, {
  baseMs = 5000,
  maximumMs = 900000,
  random = Math.random,
} = {}) {
  const exponent = Math.max(0, positiveInteger(attempt, 1, 30) - 1);
  const jitter = 0.75 + (Math.max(0, Math.min(1, Number(random()) || 0)) * 0.5);
  return Math.max(1000, Math.round(Math.min(maximumMs, baseMs * (2 ** exponent)) * jitter));
}

export class PostgresCrawlerOutboxStore {
  constructor({ query, withTransaction }) {
    if (typeof query !== "function" || typeof withTransaction !== "function") {
      throw new TypeError("query and withTransaction are required");
    }
    this.query = query;
    this.withTransaction = withTransaction;
  }

  async claimBatch({ leaseOwner, batchSize, leaseSeconds }) {
    return this.withTransaction(async (client) => {
      const result = await client.query(
        `WITH claimable AS (
           SELECT event_id
           FROM crawler.crawler_outbox
           WHERE (status='pending' AND next_attempt_at<=now())
              OR (status='publishing' AND lease_expires_at<=now())
           ORDER BY created_at,event_id
           FOR UPDATE SKIP LOCKED
           LIMIT $1
         )
         UPDATE crawler.crawler_outbox outbox
         SET status='publishing',attempts=outbox.attempts+1,
             lease_owner=$2,lease_expires_at=now()+($3::int*interval '1 second'),
             updated_at=now()
         FROM claimable
         WHERE outbox.event_id=claimable.event_id
         RETURNING outbox.*`,
        [batchSize, leaseOwner, leaseSeconds],
      );
      return result.rows;
    });
  }

  async markPublished({ eventId, leaseOwner }) {
    const result = await this.query(
      `UPDATE crawler.crawler_outbox
       SET status='published',published_at=COALESCE(published_at,now()),
           lease_owner=NULL,lease_expires_at=NULL,last_error=NULL,updated_at=now()
       WHERE event_id=$1 AND status='publishing' AND lease_owner=$2
       RETURNING event_id`,
      [eventId, leaseOwner],
    );
    return result.rowCount === 1;
  }

  async markFailed({ eventId, leaseOwner, deadLetter, retryDelayMs, error }) {
    const result = await this.query(
      `UPDATE crawler.crawler_outbox
       SET status=CASE WHEN $3::boolean THEN 'dead_letter' ELSE 'pending' END,
           next_attempt_at=CASE
             WHEN $3::boolean THEN next_attempt_at
             ELSE now()+($4::int*interval '1 millisecond')
           END,
           lease_owner=NULL,lease_expires_at=NULL,last_error=$5,updated_at=now()
       WHERE event_id=$1 AND status='publishing' AND lease_owner=$2
       RETURNING status`,
      [eventId, leaseOwner, deadLetter, retryDelayMs, String(error?.message || error).slice(0, 2000)],
    );
    return result.rows[0]?.status ?? null;
  }
}

export class CrawlerOutboxPublisher {
  constructor({
    store,
    queue,
    leaseOwner,
    batchSize = 50,
    leaseSeconds = 60,
    maxAttempts = 12,
    retryDelay = outboxRetryDelayMs,
    logger = console,
  }) {
    if (!store || !queue) throw new TypeError("store and queue are required");
    this.store = store;
    this.queue = queue;
    this.leaseOwner = String(leaseOwner ?? "").trim();
    if (!this.leaseOwner) throw new TypeError("leaseOwner is required");
    this.batchSize = positiveInteger(batchSize, 50, 500);
    this.leaseSeconds = positiveInteger(leaseSeconds, 60, 3600);
    this.maxAttempts = positiveInteger(maxAttempts, 12, 100);
    this.retryDelay = retryDelay;
    this.logger = logger;
  }

  async runOnce() {
    const rows = await this.store.claimBatch({
      leaseOwner: this.leaseOwner,
      batchSize: this.batchSize,
      leaseSeconds: this.leaseSeconds,
    });
    const summary = { claimed: rows.length, published: 0, retried: 0, dead_lettered: 0, lease_lost: 0 };
    for (const row of rows) {
      try {
        const published = await publishCrawlerOutboxRow(this.queue, row);
        const marked = await this.store.markPublished({ eventId: row.event_id, leaseOwner: this.leaseOwner });
        if (marked) summary.published += 1;
        else summary.lease_lost += 1;
        this.logger.info?.(JSON.stringify({
          event: "crawler_outbox_published",
          event_id: row.event_id,
          job_id: published.job_id,
          lease_committed: marked,
        }));
      } catch (error) {
        const deadLetter = error instanceof OutboxEnvelopeConflict
          || Number(row.attempts) >= this.maxAttempts;
        const status = await this.store.markFailed({
          eventId: row.event_id,
          leaseOwner: this.leaseOwner,
          deadLetter,
          retryDelayMs: this.retryDelay(row.attempts),
          error,
        });
        if (status === "dead_letter") summary.dead_lettered += 1;
        else if (status === "pending") summary.retried += 1;
        else summary.lease_lost += 1;
        this.logger.error?.(JSON.stringify({
          event: "crawler_outbox_publish_failed",
          event_id: row.event_id,
          status: status ?? "lease_lost",
          error: String(error?.message || error),
        }));
      }
    }
    return summary;
  }
}
