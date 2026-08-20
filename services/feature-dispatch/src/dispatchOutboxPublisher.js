import { DispatchEnvelopeConflict, publishDispatchOutboxRow } from "./dispatchTransport.js";

const DISPATCH_CLAIM_LOCK_ID = 741603219;

function positiveInteger(value, fallback, maximum = Number.MAX_SAFE_INTEGER) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isSafeInteger(parsed) && parsed > 0 ? Math.min(parsed, maximum) : fallback;
}

export function dispatchRetryDelayMs(attempt, {
  baseMs = 5000,
  maximumMs = 900000,
  random = Math.random,
} = {}) {
  const exponent = Math.max(0, positiveInteger(attempt, 1, 30) - 1);
  const jitter = 0.75 + (Math.max(0, Math.min(1, Number(random()) || 0)) * 0.5);
  return Math.max(1000, Math.round(Math.min(maximumMs, baseMs * (2 ** exponent)) * jitter));
}

export class PostgresDispatchOutboxStore {
  constructor({ query, withTransaction }) {
    if (typeof query !== "function" || typeof withTransaction !== "function") {
      throw new TypeError("query and withTransaction are required");
    }
    this.query = query;
    this.withTransaction = withTransaction;
  }

  async claimBatch({ leaseOwner, batchSize, leaseSeconds }) {
    return this.withTransaction(async (client) => {
      await client.query("SELECT pg_advisory_xact_lock($1)", [DISPATCH_CLAIM_LOCK_ID]);
      const result = await client.query(
        `WITH claimable AS (
           SELECT outbox.dispatch_event_id,plan.scheduled_at AS plan_scheduled_at
           FROM feature_clock.dispatch_outbox outbox
           JOIN feature_clock.daily_channel_plans plan ON plan.plan_id=outbox.plan_id
           WHERE (
             (outbox.status='pending' AND outbox.next_attempt_at<=now())
             OR (outbox.status='publishing' AND outbox.lease_expires_at<=now())
           )
             AND plan.status IN ('planned','dispatching')
             AND plan.scheduled_at<=now()
             AND (now() AT TIME ZONE 'UTC')::time>=TIME '00:30'
             AND (now() AT TIME ZONE 'UTC')::time<TIME '21:30'
           ORDER BY plan.due_day,plan.dispatch_slot,plan.channel_id,
                    outbox.created_at,outbox.dispatch_event_id
           FOR UPDATE OF outbox SKIP LOCKED
           LIMIT $1
         )
         UPDATE feature_clock.dispatch_outbox outbox
         SET status='publishing',attempts=outbox.attempts+1,
             lease_owner=$2,lease_expires_at=now()+($3::int*interval '1 second'),
             updated_at=now()
         FROM claimable
         WHERE outbox.dispatch_event_id=claimable.dispatch_event_id
         RETURNING outbox.*,
                   (SELECT claimable.plan_scheduled_at
                    FROM claimable
                    WHERE claimable.dispatch_event_id=outbox.dispatch_event_id)`,
        [batchSize, leaseOwner, leaseSeconds],
      );
      if (result.rows.length === 0) return [];
      const planIds = result.rows.map((row) => row.plan_id);
      const plans = await client.query(
        `UPDATE feature_clock.daily_channel_plans
         SET status='dispatching',attempts=attempts+1,
             lease_owner=$2,lease_expires_at=now()+($3::int*interval '1 second'),
             error_code=NULL,updated_at=now()
         WHERE plan_id=ANY($1::uuid[]) AND status IN ('planned','dispatching')
         RETURNING plan_id`,
        [planIds, leaseOwner, leaseSeconds],
      );
      if (plans.rowCount !== result.rows.length) {
        throw new Error("Dispatch Plan and Outbox lease state diverged");
      }
      return result.rows;
    });
  }

  async markPublished({ dispatchEventId, leaseOwner }) {
    return this.withTransaction(async (client) => {
      const outbox = await client.query(
        `UPDATE feature_clock.dispatch_outbox
         SET status='published',published_at=COALESCE(published_at,now()),
             lease_owner=NULL,lease_expires_at=NULL,last_error=NULL,updated_at=now()
         WHERE dispatch_event_id=$1 AND status='publishing' AND lease_owner=$2
         RETURNING plan_id`,
        [dispatchEventId, leaseOwner],
      );
      if (outbox.rowCount === 0) return false;
      const plan = await client.query(
        `UPDATE feature_clock.daily_channel_plans
         SET status=CASE WHEN status='dispatching' THEN 'dispatched' ELSE status END,
             dispatched_at=COALESCE(dispatched_at,now()),
             lease_owner=NULL,lease_expires_at=NULL,
             error_code=NULL,updated_at=now()
         WHERE plan_id=$1
           AND (
             (status IN ('dispatching','running') AND lease_owner=$2)
             OR status IN ('succeeded','partial','failed')
           )
         RETURNING plan_id`,
        [outbox.rows[0].plan_id, leaseOwner],
      );
      if (plan.rowCount !== 1) throw new Error("Published Outbox lost its Plan lease");
      return true;
    });
  }

  async markFailed({ dispatchEventId, leaseOwner, deadLetter, retryDelayMs, error }) {
    return this.withTransaction(async (client) => {
      const message = String(error?.message || error).slice(0, 2000);
      const outbox = await client.query(
        `UPDATE feature_clock.dispatch_outbox
         SET status=CASE WHEN $3::boolean THEN 'dead_letter' ELSE 'pending' END,
             next_attempt_at=CASE
               WHEN $3::boolean THEN next_attempt_at
               ELSE now()+($4::int*interval '1 millisecond')
             END,
             lease_owner=NULL,lease_expires_at=NULL,last_error=$5,updated_at=now()
         WHERE dispatch_event_id=$1 AND status='publishing' AND lease_owner=$2
         RETURNING plan_id,status`,
        [dispatchEventId, leaseOwner, deadLetter, retryDelayMs, message],
      );
      if (outbox.rowCount === 0) return null;
      const planStatus = deadLetter ? "failed" : "planned";
      const errorCode = deadLetter ? "dispatch_dead_letter" : "dispatch_retry";
      const plan = await client.query(
        `UPDATE feature_clock.daily_channel_plans
         SET status=$3,lease_owner=NULL,lease_expires_at=NULL,
             error_code=$4,
             completed_at=CASE WHEN $3='failed' THEN now() ELSE completed_at END,
             updated_at=now()
         WHERE plan_id=$1 AND status='dispatching' AND lease_owner=$2
         RETURNING plan_id`,
        [outbox.rows[0].plan_id, leaseOwner, planStatus, errorCode],
      );
      if (plan.rowCount !== 1) throw new Error("Failed Outbox lost its Plan lease");
      return outbox.rows[0].status;
    });
  }
}

export class DispatchOutboxPublisher {
  constructor({
    store,
    queue,
    leaseOwner,
    batchSize = 50,
    releaseBatchSize = null,
    leaseSeconds = 60,
    maxAttempts = 12,
    retryDelay = dispatchRetryDelayMs,
    logger = console,
  }) {
    if (!store || !queue) throw new TypeError("store and queue are required");
    this.store = store;
    this.queue = queue;
    this.leaseOwner = String(leaseOwner ?? "").trim();
    if (!this.leaseOwner) throw new TypeError("leaseOwner is required");
    this.releaseBatchSize = positiveInteger(releaseBatchSize ?? batchSize, 50, 500);
    this.leaseSeconds = positiveInteger(leaseSeconds, 60, 3600);
    this.maxAttempts = positiveInteger(maxAttempts, 12, 100);
    this.retryDelay = retryDelay;
    this.logger = logger;
  }

  async runOnce({ batchSize = this.releaseBatchSize } = {}) {
    const claimLimit = Math.max(0, Math.min(
      this.releaseBatchSize,
      Number.parseInt(String(batchSize ?? ""), 10) || 0,
    ));
    if (claimLimit === 0) {
      return { claimed: 0, published: 0, retried: 0, dead_lettered: 0, lease_lost: 0 };
    }
    const rows = await this.store.claimBatch({
      leaseOwner: this.leaseOwner,
      batchSize: claimLimit,
      leaseSeconds: this.leaseSeconds,
    });
    const summary = { claimed: rows.length, published: 0, retried: 0, dead_lettered: 0, lease_lost: 0 };
    for (const row of rows) {
      try {
        const published = await publishDispatchOutboxRow(this.queue, row);
        const marked = await this.store.markPublished({
          dispatchEventId: row.dispatch_event_id,
          leaseOwner: this.leaseOwner,
        });
        if (marked) summary.published += 1;
        else summary.lease_lost += 1;
        this.logger.info?.(JSON.stringify({
          event: "dispatch_outbox_published",
          dispatch_event_id: row.dispatch_event_id,
          plan_id: row.plan_id,
          job_id: published.job_id,
          lease_committed: marked,
        }));
      } catch (error) {
        const deadLetter = error instanceof DispatchEnvelopeConflict
          || Number(row.attempts) >= this.maxAttempts;
        const status = await this.store.markFailed({
          dispatchEventId: row.dispatch_event_id,
          leaseOwner: this.leaseOwner,
          deadLetter,
          retryDelayMs: this.retryDelay(row.attempts),
          error,
        });
        if (status === "dead_letter") summary.dead_lettered += 1;
        else if (status === "pending") summary.retried += 1;
        else summary.lease_lost += 1;
        this.logger.error?.(JSON.stringify({
          event: "dispatch_outbox_publish_failed",
          dispatch_event_id: row.dispatch_event_id,
          plan_id: row.plan_id,
          status: status ?? "lease_lost",
          error: String(error?.message || error),
        }));
      }
    }
    return summary;
  }
}
