import { randomUUID } from "node:crypto";
import { PostgresBusinessPublicationActivator } from "./businessPublicationActivator.js";

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

function normalizeFinding(row) {
  return {
    count: Number(row?.count ?? 0),
    oldest_at: row?.oldest_at == null ? null : new Date(row.oldest_at).toISOString(),
    samples: Array.isArray(row?.samples) ? row.samples : [],
  };
}

async function finding(pool, sql, params) {
  const result = await pool.query(sql, params);
  return normalizeFinding(result.rows[0]);
}

async function inspectCursorCurrent(pool, sampleSize) {
  return finding(pool, `
    WITH current_rows AS (
      SELECT channel_id,'channel'::text AS domain,publication_stream_id,
             active_sequence,active_revision_id,result_hash,updated_at
      FROM result.entity_current
      UNION ALL
      SELECT channel_id,'video'::text AS domain,publication_stream_id,
             active_sequence,active_revision_id,result_hash,updated_at
      FROM result.video_current
      UNION ALL
      SELECT channel_id,'agent'::text AS domain,publication_stream_id,
             active_sequence,active_revision_id,result_hash,updated_at
      FROM result.agent_current
    ), issues AS (
      SELECT COALESCE(cursor_row.channel_id,current_row.channel_id) AS channel_id,
             COALESCE(cursor_row.domain,current_row.domain) AS domain,
             COALESCE(cursor_row.updated_at,current_row.updated_at,now()) AS observed_at,
             jsonb_build_object(
               'channel_id',COALESCE(cursor_row.channel_id,current_row.channel_id),
               'domain',COALESCE(cursor_row.domain,current_row.domain),
               'reason',CASE
                 WHEN cursor_row.channel_id IS NULL THEN 'current_without_cursor'
                 WHEN current_row.channel_id IS NULL THEN 'cursor_without_current'
                 ELSE 'cursor_current_values_differ'
               END,
               'cursor',CASE WHEN cursor_row.channel_id IS NULL THEN NULL ELSE jsonb_build_object(
                 'stream_id',cursor_row.publication_stream_id,
                 'sequence',cursor_row.active_sequence,
                 'revision_id',cursor_row.active_revision_id,
                 'result_hash',cursor_row.active_result_hash
               ) END,
               'current',CASE WHEN current_row.channel_id IS NULL THEN NULL ELSE jsonb_build_object(
                 'stream_id',current_row.publication_stream_id,
                 'sequence',current_row.active_sequence,
                 'revision_id',current_row.active_revision_id,
                 'result_hash',current_row.result_hash
               ) END
             ) AS details
      FROM publication.consumer_cursor AS cursor_row
      FULL OUTER JOIN current_rows AS current_row USING (channel_id,domain)
      WHERE cursor_row.channel_id IS NULL
         OR current_row.channel_id IS NULL
         OR cursor_row.publication_stream_id IS DISTINCT FROM current_row.publication_stream_id
         OR cursor_row.active_sequence IS DISTINCT FROM current_row.active_sequence
         OR cursor_row.active_revision_id IS DISTINCT FROM current_row.active_revision_id
         OR cursor_row.active_result_hash IS DISTINCT FROM current_row.result_hash
    ), ranked AS (
      SELECT *,row_number() OVER (ORDER BY observed_at,channel_id,domain) AS sample_rank
      FROM issues
    )
    SELECT count(*)::int AS count,min(observed_at) AS oldest_at,
           COALESCE(
             jsonb_agg(details ORDER BY observed_at,channel_id,domain)
               FILTER (WHERE sample_rank<=$1),
             '[]'::jsonb
           ) AS samples
    FROM ranked`, [sampleSize]);
}

async function inspectCursorRevision(pool, sampleSize) {
  return finding(pool, `
    WITH issues AS (
      SELECT cursor_row.channel_id,cursor_row.domain,cursor_row.updated_at AS observed_at,
             jsonb_build_object(
               'channel_id',cursor_row.channel_id,
               'domain',cursor_row.domain,
               'active_revision_id',cursor_row.active_revision_id,
               'reason',CASE
                 WHEN revision.revision_id IS NULL THEN 'revision_missing'
                 WHEN revision.activation_status<>'active' THEN 'revision_not_active'
                 WHEN activation_item.revision_id IS NULL THEN 'activation_item_missing'
                 ELSE 'cursor_revision_values_differ'
               END
             ) AS details
      FROM publication.consumer_cursor AS cursor_row
      LEFT JOIN publication.revision AS revision
        ON revision.revision_id=cursor_row.active_revision_id
      LEFT JOIN publication.activation_item AS activation_item
        ON activation_item.revision_id=cursor_row.active_revision_id
      WHERE revision.revision_id IS NULL
         OR revision.channel_id IS DISTINCT FROM cursor_row.channel_id
         OR revision.domain IS DISTINCT FROM cursor_row.domain
         OR revision.publication_stream_id IS DISTINCT FROM cursor_row.publication_stream_id
         OR revision.data_sequence IS DISTINCT FROM cursor_row.active_sequence
         OR revision.result_hash IS DISTINCT FROM cursor_row.active_result_hash
         OR revision.activation_status<>'active'
         OR activation_item.revision_id IS NULL
    ), ranked AS (
      SELECT *,row_number() OVER (ORDER BY observed_at,channel_id,domain) AS sample_rank
      FROM issues
    )
    SELECT count(*)::int AS count,min(observed_at) AS oldest_at,
           COALESCE(
             jsonb_agg(details ORDER BY observed_at,channel_id,domain)
               FILTER (WHERE sample_rank<=$1),
             '[]'::jsonb
           ) AS samples
    FROM ranked`, [sampleSize]);
}

async function inspectOwnershipCursor(pool, sampleSize) {
  return finding(pool, `
    WITH issues AS (
      SELECT cursor_row.channel_id,cursor_row.domain,cursor_row.updated_at AS observed_at,
             jsonb_build_object(
               'channel_id',cursor_row.channel_id,
               'domain',cursor_row.domain,
               'cursor_stream_id',cursor_row.publication_stream_id,
               'ownership_stream_id',ownership.active_publication_stream_id,
               'ownership_status',ownership.status
             ) AS details
      FROM publication.consumer_cursor AS cursor_row
      LEFT JOIN publication.channel_ownership AS ownership
        ON ownership.channel_id=cursor_row.channel_id
      WHERE ownership.channel_id IS NULL
         OR ownership.status<>'active'
         OR ownership.active_publication_stream_id IS DISTINCT FROM cursor_row.publication_stream_id
    ), ranked AS (
      SELECT *,row_number() OVER (ORDER BY observed_at,channel_id,domain) AS sample_rank
      FROM issues
    )
    SELECT count(*)::int AS count,min(observed_at) AS oldest_at,
           COALESCE(
             jsonb_agg(details ORDER BY observed_at,channel_id,domain)
               FILTER (WHERE sample_rank<=$1),
             '[]'::jsonb
           ) AS samples
    FROM ranked`, [sampleSize]);
}

async function inspectLongLivedGaps(pool, sampleSize, gapAlertSeconds) {
  return finding(pool, `
    WITH issues AS (
      SELECT revision.channel_id,revision.domain,min(revision.received_at) AS observed_at,
             jsonb_build_object(
               'channel_id',revision.channel_id,
               'domain',revision.domain,
               'expected_sequence',COALESCE(cursor_row.active_sequence,0)+1,
               'first_waiting_sequence',min(revision.data_sequence),
               'oldest_received_at',min(revision.received_at)
             ) AS details
      FROM publication.revision AS revision
      LEFT JOIN publication.consumer_cursor AS cursor_row
        ON cursor_row.channel_id=revision.channel_id AND cursor_row.domain=revision.domain
      WHERE revision.validation_status='valid'
        AND revision.activation_status='waiting_gap'
      GROUP BY revision.channel_id,revision.domain,cursor_row.active_sequence
      HAVING min(revision.received_at)<=now()-($2::int*interval '1 second')
    ), ranked AS (
      SELECT *,row_number() OVER (ORDER BY observed_at,channel_id,domain) AS sample_rank
      FROM issues
    )
    SELECT count(*)::int AS count,min(observed_at) AS oldest_at,
           COALESCE(
             jsonb_agg(details ORDER BY observed_at,channel_id,domain)
               FILTER (WHERE sample_rank<=$1),
             '[]'::jsonb
           ) AS samples
    FROM ranked`, [sampleSize, gapAlertSeconds]);
}

async function inspectOpenQuarantine(pool, sampleSize) {
  return finding(pool, `
    WITH issues AS (
      SELECT inbox.channel_id,inbox.domain,quarantine.first_seen_at AS observed_at,
             jsonb_build_object(
               'channel_id',inbox.channel_id,
               'domain',inbox.domain,
               'revision_id',quarantine.revision_id,
               'issue_code',quarantine.issue_code,
               'first_seen_at',quarantine.first_seen_at
             ) AS details
      FROM publication.quarantine AS quarantine
      JOIN publication.inbox AS inbox ON inbox.revision_id=quarantine.revision_id
      WHERE quarantine.status='open'
    ), ranked AS (
      SELECT *,row_number() OVER (ORDER BY observed_at,channel_id,domain) AS sample_rank
      FROM issues
    )
    SELECT count(*)::int AS count,min(observed_at) AS oldest_at,
           COALESCE(
             jsonb_agg(details ORDER BY observed_at,channel_id,domain)
               FILTER (WHERE sample_rank<=$1),
             '[]'::jsonb
           ) AS samples
    FROM ranked`, [sampleSize]);
}

async function inspectOldStreamPending(pool, sampleSize) {
  return finding(pool, `
    WITH issues AS (
      SELECT revision.channel_id,revision.domain,revision.received_at AS observed_at,
             jsonb_build_object(
               'channel_id',revision.channel_id,
               'domain',revision.domain,
               'revision_id',revision.revision_id,
               'revision_stream_id',revision.publication_stream_id,
               'active_stream_id',ownership.active_publication_stream_id,
               'sequence',revision.data_sequence
             ) AS details
      FROM publication.revision AS revision
      JOIN publication.channel_ownership AS ownership
        ON ownership.channel_id=revision.channel_id
      WHERE revision.validation_status='valid'
        AND revision.activation_status IN ${PENDING_ACTIVATION_STATUSES}
        AND revision.publication_stream_id<>ownership.active_publication_stream_id
    ), ranked AS (
      SELECT *,row_number() OVER (ORDER BY observed_at,channel_id,domain) AS sample_rank
      FROM issues
    )
    SELECT count(*)::int AS count,min(observed_at) AS oldest_at,
           COALESCE(
             jsonb_agg(details ORDER BY observed_at,channel_id,domain)
               FILTER (WHERE sample_rank<=$1),
             '[]'::jsonb
           ) AS samples
    FROM ranked`, [sampleSize]);
}

async function inspectActiveWithoutAudit(pool, sampleSize) {
  return finding(pool, `
    WITH issues AS (
      SELECT revision.channel_id,revision.domain,revision.updated_at AS observed_at,
             jsonb_build_object(
               'channel_id',revision.channel_id,
               'domain',revision.domain,
               'revision_id',revision.revision_id,
               'sequence',revision.data_sequence
             ) AS details
      FROM publication.revision AS revision
      LEFT JOIN publication.activation_item AS activation_item
        ON activation_item.revision_id=revision.revision_id
      WHERE revision.activation_status='active'
        AND activation_item.revision_id IS NULL
    ), ranked AS (
      SELECT *,row_number() OVER (ORDER BY observed_at,channel_id,domain) AS sample_rank
      FROM issues
    )
    SELECT count(*)::int AS count,min(observed_at) AS oldest_at,
           COALESCE(
             jsonb_agg(details ORDER BY observed_at,channel_id,domain)
               FILTER (WHERE sample_rank<=$1),
             '[]'::jsonb
           ) AS samples
    FROM ranked`, [sampleSize]);
}

async function inspectProjectionStuck(pool, sampleSize, projectionStuckSeconds) {
  return finding(pool, `
    WITH issues AS (
      SELECT projection.channel_id,
             CASE
               WHEN projection.status='leased' THEN projection.lease_expires_at
               WHEN projection.status='retry_wait' THEN projection.next_attempt_at
               ELSE projection.created_at
             END AS observed_at,
             jsonb_build_object(
               'channel_id',projection.channel_id,
               'projection_id',projection.projection_id,
               'activation_id',projection.activation_id,
               'status',projection.status,
               'attempts',projection.attempts,
               'next_attempt_at',projection.next_attempt_at,
               'lease_expires_at',projection.lease_expires_at
             ) AS details
      FROM publication.projection_outbox AS projection
      WHERE (projection.status='pending'
             AND projection.created_at<=now()-($2::int*interval '1 second'))
         OR (projection.status='leased' AND projection.lease_expires_at<=now())
         OR (projection.status='retry_wait' AND projection.next_attempt_at<=now()
             AND projection.updated_at<=now()-($2::int*interval '1 second'))
    ), ranked AS (
      SELECT *,row_number() OVER (ORDER BY observed_at,channel_id) AS sample_rank
      FROM issues
    )
    SELECT count(*)::int AS count,min(observed_at) AS oldest_at,
           COALESCE(
             jsonb_agg(details ORDER BY observed_at,channel_id)
               FILTER (WHERE sample_rank<=$1),
             '[]'::jsonb
           ) AS samples
    FROM ranked`, [sampleSize, projectionStuckSeconds]);
}

async function inspectProjectionDeadLetter(pool, sampleSize) {
  return finding(pool, `
    WITH issues AS (
      SELECT projection.channel_id,projection.updated_at AS observed_at,
             jsonb_build_object(
               'channel_id',projection.channel_id,
               'projection_id',projection.projection_id,
               'activation_id',projection.activation_id,
               'attempts',projection.attempts,
               'last_error',projection.last_error
             ) AS details
      FROM publication.projection_outbox AS projection
      WHERE projection.status='dead_letter'
    ), ranked AS (
      SELECT *,row_number() OVER (ORDER BY observed_at,channel_id) AS sample_rank
      FROM issues
    )
    SELECT count(*)::int AS count,min(observed_at) AS oldest_at,
           COALESCE(
             jsonb_agg(details ORDER BY observed_at,channel_id)
               FILTER (WHERE sample_rank<=$1),
             '[]'::jsonb
           ) AS samples
    FROM ranked`, [sampleSize]);
}

async function inspectBlockedActivation(pool, sampleSize, gapAlertSeconds) {
  return finding(pool, `
    WITH issues AS (
      SELECT state.channel_id,state.last_attempted_at AS observed_at,
             jsonb_build_object(
               'channel_id',state.channel_id,
               'last_outcome',state.last_outcome,
               'attempt_count',state.attempt_count,
               'last_attempted_at',state.last_attempted_at
             ) AS details
      FROM publication.reconciliation_state AS state
      WHERE state.last_outcome IN (
              'waiting_ownership','cutover_required','inconsistent_core_cursor',
              'waiting_core_bootstrap','core_bootstrap_quarantined'
            )
        AND (state.lease_owner IS NULL OR state.lease_expires_at<=now())
        AND state.last_attempted_at<=now()-($2::int*interval '1 second')
        AND EXISTS (
          SELECT 1 FROM publication.revision AS revision
          WHERE revision.channel_id=state.channel_id
            AND revision.validation_status='valid'
            AND revision.activation_status IN ${PENDING_ACTIVATION_STATUSES}
        )
    ), ranked AS (
      SELECT *,row_number() OVER (ORDER BY observed_at,channel_id) AS sample_rank
      FROM issues
    )
    SELECT count(*)::int AS count,min(observed_at) AS oldest_at,
           COALESCE(
             jsonb_agg(details ORDER BY observed_at,channel_id)
               FILTER (WHERE sample_rank<=$1),
             '[]'::jsonb
           ) AS samples
    FROM ranked`, [sampleSize, gapAlertSeconds]);
}

async function inspectActivationErrors(pool, sampleSize) {
  return finding(pool, `
    WITH issues AS (
      SELECT state.channel_id,state.last_attempted_at AS observed_at,
             jsonb_build_object(
               'channel_id',state.channel_id,
               'attempt_count',state.attempt_count,
               'consecutive_error_count',state.consecutive_error_count,
               'last_error',state.last_error,
               'next_attempt_at',state.next_attempt_at
             ) AS details
      FROM publication.reconciliation_state AS state
      WHERE state.last_outcome='error'
        AND (state.lease_owner IS NULL OR state.lease_expires_at<=now())
        AND EXISTS (
          SELECT 1 FROM publication.revision AS revision
          WHERE revision.channel_id=state.channel_id
            AND revision.validation_status='valid'
            AND revision.activation_status IN ${PENDING_ACTIVATION_STATUSES}
        )
    ), ranked AS (
      SELECT *,row_number() OVER (ORDER BY observed_at,channel_id) AS sample_rank
      FROM issues
    )
    SELECT count(*)::int AS count,min(observed_at) AS oldest_at,
           COALESCE(
             jsonb_agg(details ORDER BY observed_at,channel_id)
               FILTER (WHERE sample_rank<=$1),
             '[]'::jsonb
           ) AS samples
    FROM ranked`, [sampleSize]);
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
    gapAlertSeconds = 900,
    projectionStuckSeconds = 300,
    auditIntervalSeconds = 300,
    auditSampleSize = 10,
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
    this.gapAlertSeconds = integerOption(
      gapAlertSeconds,
      900,
      "gapAlertSeconds",
      { minimum: 0, maximum: 2592000 },
    );
    this.projectionStuckSeconds = integerOption(
      projectionStuckSeconds,
      300,
      "projectionStuckSeconds",
      { minimum: 0, maximum: 2592000 },
    );
    this.auditIntervalMs = integerOption(
      auditIntervalSeconds,
      300,
      "auditIntervalSeconds",
      { minimum: 0, maximum: 86400 },
    ) * 1000;
    this.auditSampleSize = integerOption(
      auditSampleSize,
      10,
      "auditSampleSize",
      { minimum: 1, maximum: 100 },
    );
    this.clock = clock;
    this.lastAuditAtMs = null;
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

  async #inspect() {
    const [
      cursorCurrentMismatch,
      cursorRevisionMismatch,
      ownershipCursorMismatch,
      longLivedGap,
      openQuarantine,
      oldStreamPending,
      activeWithoutActivation,
      projectionStuck,
      projectionDeadLetter,
      blockedActivation,
      activationError,
    ] = await Promise.all([
      inspectCursorCurrent(this.pool, this.auditSampleSize),
      inspectCursorRevision(this.pool, this.auditSampleSize),
      inspectOwnershipCursor(this.pool, this.auditSampleSize),
      inspectLongLivedGaps(this.pool, this.auditSampleSize, this.gapAlertSeconds),
      inspectOpenQuarantine(this.pool, this.auditSampleSize),
      inspectOldStreamPending(this.pool, this.auditSampleSize),
      inspectActiveWithoutAudit(this.pool, this.auditSampleSize),
      inspectProjectionStuck(this.pool, this.auditSampleSize, this.projectionStuckSeconds),
      inspectProjectionDeadLetter(this.pool, this.auditSampleSize),
      inspectBlockedActivation(this.pool, this.auditSampleSize, this.gapAlertSeconds),
      inspectActivationErrors(this.pool, this.auditSampleSize),
    ]);
    const findings = {
      cursor_current_mismatch: cursorCurrentMismatch,
      cursor_revision_mismatch: cursorRevisionMismatch,
      ownership_cursor_mismatch: ownershipCursorMismatch,
      long_lived_gap: longLivedGap,
      open_quarantine: openQuarantine,
      old_stream_pending: oldStreamPending,
      active_without_activation: activeWithoutActivation,
      projection_stuck: projectionStuck,
      projection_dead_letter: projectionDeadLetter,
      blocked_activation: blockedActivation,
      activation_error: activationError,
    };
    return {
      issue_count: Object.values(findings).reduce((sum, item) => sum + item.count, 0),
      findings,
    };
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

    const now = instant(this.clock());
    const auditDue = this.lastAuditAtMs == null
      || now.getTime() - this.lastAuditAtMs >= this.auditIntervalMs;
    let audit = { performed: false, issue_count: null, findings: null };
    if (auditDue) {
      const inspection = await this.#inspect();
      this.lastAuditAtMs = now.getTime();
      audit = { performed: true, ...inspection };
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
