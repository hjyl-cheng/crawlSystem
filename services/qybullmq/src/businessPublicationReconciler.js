import { randomUUID } from "node:crypto";
import { PostgresBusinessPublicationActivator } from "./businessPublicationActivator.js";
import { PostgresBusinessPublicationAuditor } from "./businessPublicationAuditor.js";

const PENDING_ACTIVATION_STATUSES = "('staged','waiting_gap','waiting_ownership')";
const BLOCKED_OUTCOMES = new Set([
  "waiting_ownership",
  "cutover_required",
  "inconsistent_core_cursor",
  "waiting_core_bootstrap",
  "core_bootstrap_quarantined",
  "waiting_gap",
]);

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function integerOption(value, fallback, field, { minimum, maximum }) {
  if (value == null || String(value).trim() === "") return fallback;
  const output = Number(value);
  if (!Number.isSafeInteger(output) || output < minimum || output > maximum) {
    throw new TypeError(`${field} must be an integer between ${minimum} and ${maximum}`);
  }
  return output;
}

function errorText(error) {
  return String(error?.message || error).slice(0, 2000);
}

function instant(value) {
  const output = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(output.getTime())) throw new TypeError("clock must return a valid Date");
  return output;
}

async function mapConcurrent(items, concurrency, callback) {
  const results = new Array(items.length);
  let next = 0;
  const run = async () => {
    while (next < items.length) {
      const index = next;
      next += 1;
      results[index] = await callback(items[index], index);
    }
  };
  await Promise.all(Array.from(
    { length: Math.min(concurrency, items.length) },
    () => run(),
  ));
  return results;
}

export class PostgresBusinessPublicationReconciler {
  constructor(pool, {
    activator = new PostgresBusinessPublicationActivator(pool),
    workerId = `business-publication-reconciler:${randomUUID()}`,
    batchSize = 100,
    concurrency = 4,
    leaseSeconds = 120,
    blockedRetrySeconds = 30,
    errorRetrySeconds = 10,
    maximumErrorRetrySeconds = 300,
    auditor,
    gapAlertSeconds = 900,
    projectionStuckSeconds = 300,
    auditIntervalSeconds = 300,
    auditSampleSize = 10,
    auditErrorRetrySeconds = 30,
    auditMaximumErrorRetrySeconds = 300,
    clock = () => new Date(),
  } = {}) {
    if (!pool || typeof pool.connect !== "function" || typeof pool.query !== "function") {
      throw new TypeError("a PostgreSQL Pool is required");
    }
    if (!activator || typeof activator.activateReady !== "function") {
      throw new TypeError("an Activator is required");
    }
    if (typeof clock !== "function") throw new TypeError("clock must be a function");
    this.pool = pool;
    this.activator = activator;
    this.workerId = requiredText(workerId, "workerId");
    this.batchSize = integerOption(batchSize, 100, "batchSize", { minimum: 1, maximum: 5000 });
    this.concurrency = integerOption(concurrency, 4, "concurrency", { minimum: 1, maximum: 64 });
    this.leaseSeconds = integerOption(leaseSeconds, 120, "leaseSeconds", { minimum: 5, maximum: 3600 });
    this.blockedRetrySeconds = integerOption(
      blockedRetrySeconds,
      30,
      "blockedRetrySeconds",
      { minimum: 1, maximum: 86400 },
    );
    this.errorRetrySeconds = integerOption(
      errorRetrySeconds,
      10,
      "errorRetrySeconds",
      { minimum: 1, maximum: 3600 },
    );
    this.maximumErrorRetrySeconds = integerOption(
      maximumErrorRetrySeconds,
      300,
      "maximumErrorRetrySeconds",
      { minimum: this.errorRetrySeconds, maximum: 86400 },
    );
    this.clock = clock;
    this.auditor = auditor ?? new PostgresBusinessPublicationAuditor(pool, {
      gapAlertSeconds,
      projectionStuckSeconds,
      auditIntervalSeconds,
      auditSampleSize,
      errorRetrySeconds: auditErrorRetrySeconds,
      maximumErrorRetrySeconds: auditMaximumErrorRetrySeconds,
      clock,
    });
    if (typeof this.auditor.runIfDue !== "function") {
      throw new TypeError("an Auditor is required");
    }
  }

  async #claimChannels() {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        `/* business-publication-reconciler:seed-state */
         INSERT INTO publication.reconciliation_state (channel_id)
         SELECT revision.channel_id
         FROM publication.revision AS revision
         WHERE revision.validation_status='valid'
           AND revision.activation_status IN ${PENDING_ACTIVATION_STATUSES}
         GROUP BY revision.channel_id
         ON CONFLICT (channel_id) DO NOTHING`,
      );
      const result = await client.query(
        `/* business-publication-reconciler:claim */
         WITH claimable AS (
           SELECT state.channel_id
           FROM publication.reconciliation_state AS state
           WHERE EXISTS (
             SELECT 1 FROM publication.revision AS revision
             WHERE revision.channel_id=state.channel_id
               AND revision.validation_status='valid'
               AND revision.activation_status IN ${PENDING_ACTIVATION_STATUSES}
           )
             AND (
               (state.lease_owner IS NULL AND state.next_attempt_at<=now())
               OR (state.lease_owner IS NOT NULL AND state.lease_expires_at<=now())
             )
           ORDER BY state.next_attempt_at,state.channel_id
           FOR UPDATE OF state SKIP LOCKED
           LIMIT $1
         )
         UPDATE publication.reconciliation_state AS state
         SET lease_owner=$2,
             lease_expires_at=now()+($3::int*interval '1 second'),
             attempt_count=state.attempt_count+1,
             last_attempted_at=clock_timestamp(),updated_at=now()
         FROM claimable
         WHERE state.channel_id=claimable.channel_id
         RETURNING state.channel_id,state.consecutive_error_count`,
        [this.batchSize, this.workerId, this.leaseSeconds],
      );
      await client.query("COMMIT");
      return result.rows.map((row) => ({
        channel_id: String(row.channel_id),
        consecutive_error_count: Number(row.consecutive_error_count),
      }));
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async #completeLease(channelId, outcome) {
    const delaySeconds = BLOCKED_OUTCOMES.has(outcome) ? this.blockedRetrySeconds : 0;
    const result = await this.pool.query(
      `/* business-publication-reconciler:complete */
       UPDATE publication.reconciliation_state
       SET lease_owner=NULL,lease_expires_at=NULL,last_outcome=$3,last_error=NULL,
           consecutive_error_count=0,
           next_attempt_at=now()+($4::int*interval '1 second'),updated_at=now()
       WHERE channel_id=$1 AND lease_owner=$2
       RETURNING channel_id`,
      [channelId, this.workerId, outcome, delaySeconds],
    );
    return result.rowCount === 1;
  }

  async #failLease(claim, error) {
    const exponent = Math.min(claim.consecutive_error_count, 10);
    const retrySeconds = Math.min(
      this.maximumErrorRetrySeconds,
      this.errorRetrySeconds * (2 ** exponent),
    );
    const result = await this.pool.query(
      `/* business-publication-reconciler:failed */
       UPDATE publication.reconciliation_state
       SET lease_owner=NULL,lease_expires_at=NULL,last_outcome='error',last_error=$3,
           consecutive_error_count=consecutive_error_count+1,
           next_attempt_at=now()+($4::int*interval '1 second'),updated_at=now()
       WHERE channel_id=$1 AND lease_owner=$2
       RETURNING channel_id`,
      [claim.channel_id, this.workerId, errorText(error), retrySeconds],
    );
    return result.rowCount === 1;
  }

  async #processClaim(claim) {
    try {
      const activation = await this.activator.activateReady(claim.channel_id);
      const outcome = requiredText(activation?.status, "Activator outcome");
      if (outcome === "error") throw new TypeError("Activator outcome 'error' is reserved");
      const leaseReleased = await this.#completeLease(claim.channel_id, outcome);
      return {
        channel_id: claim.channel_id,
        status: outcome,
        applied_count: Array.isArray(activation.applied) ? activation.applied.length : 0,
        activation_id: activation.activation_id ?? null,
        lease_released: leaseReleased,
      };
    } catch (error) {
      let leaseReleased = false;
      let leaseError = null;
      try {
        leaseReleased = await this.#failLease(claim, error);
      } catch (failure) {
        leaseError = errorText(failure);
      }
      return {
        channel_id: claim.channel_id,
        status: "error",
        applied_count: 0,
        activation_id: null,
        lease_released: leaseReleased,
        error: errorText(error),
        ...(leaseError ? { lease_error: leaseError } : {}),
      };
    }
  }

  async runOnce() {
    const startedAt = instant(this.clock());
    const claims = await this.#claimChannels();
    const channels = await mapConcurrent(
      claims,
      this.concurrency,
      (claim) => this.#processClaim(claim),
    );
    const outcomes = {};
    let appliedCount = 0;
    for (const channel of channels) {
      outcomes[channel.status] = (outcomes[channel.status] ?? 0) + 1;
      appliedCount += channel.applied_count;
    }

    let audit;
    try {
      audit = await this.auditor.runIfDue();
    } catch (error) {
      audit = {
        performed: true,
        status: "failed",
        issue_count: null,
        findings: null,
        query_count: 0,
        query_duration_ms: null,
        failed_query: null,
        error: errorText(error),
      };
    }
    const finishedAt = instant(this.clock());
    return {
      started_at: startedAt.toISOString(),
      finished_at: finishedAt.toISOString(),
      duration_ms: Math.max(0, finishedAt.getTime() - startedAt.getTime()),
      claimed: claims.length,
      processed: channels.length,
      applied_revisions: appliedCount,
      outcomes,
      channels,
      audit,
    };
  }
}
