import { performance } from "node:perf_hooks";

const PENDING_ACTIVATION_STATUSES = "('staged','waiting_gap','waiting_ownership')";

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
             cursor_row.channel_id IS NOT NULL AS has_cursor,
             current_row.channel_id IS NOT NULL AS has_current,
             CASE
               WHEN cursor_row.channel_id IS NULL THEN 'current_without_cursor'
               WHEN current_row.channel_id IS NULL THEN 'cursor_without_current'
               ELSE 'cursor_current_values_differ'
             END AS reason,
             cursor_row.publication_stream_id AS cursor_stream_id,
             cursor_row.active_sequence AS cursor_sequence,
             cursor_row.active_revision_id AS cursor_revision_id,
             cursor_row.active_result_hash AS cursor_result_hash,
             current_row.publication_stream_id AS current_stream_id,
             current_row.active_sequence AS current_sequence,
             current_row.active_revision_id AS current_revision_id,
             current_row.result_hash AS current_result_hash
      FROM publication.consumer_cursor AS cursor_row
      FULL OUTER JOIN current_rows AS current_row USING (channel_id,domain)
      WHERE cursor_row.channel_id IS NULL
         OR current_row.channel_id IS NULL
         OR cursor_row.publication_stream_id IS DISTINCT FROM current_row.publication_stream_id
         OR cursor_row.active_sequence IS DISTINCT FROM current_row.active_sequence
         OR cursor_row.active_revision_id IS DISTINCT FROM current_row.active_revision_id
         OR cursor_row.active_result_hash IS DISTINCT FROM current_row.result_hash
    ), summary AS (
      SELECT count(*)::int AS count,min(observed_at) AS oldest_at
      FROM issues
    ), samples AS (
      SELECT COALESCE(
               jsonb_agg(
                 jsonb_build_object(
                   'channel_id',channel_id,
                   'domain',domain,
                   'reason',reason,
                   'cursor',CASE WHEN has_cursor THEN jsonb_build_object(
                     'stream_id',cursor_stream_id,
                     'sequence',cursor_sequence,
                     'revision_id',cursor_revision_id,
                     'result_hash',cursor_result_hash
                   ) ELSE NULL END,
                   'current',CASE WHEN has_current THEN jsonb_build_object(
                     'stream_id',current_stream_id,
                     'sequence',current_sequence,
                     'revision_id',current_revision_id,
                     'result_hash',current_result_hash
                   ) ELSE NULL END
                 ) ORDER BY observed_at,channel_id,domain
               ),
               '[]'::jsonb
             ) AS samples
      FROM (
        SELECT *
        FROM issues
        ORDER BY observed_at,channel_id,domain
        LIMIT $1
      ) AS sample_rows
    )
    SELECT summary.count,summary.oldest_at,samples.samples
    FROM summary CROSS JOIN samples`, [sampleSize]);
}

async function inspectCursorRevision(pool, sampleSize) {
  return finding(pool, `
    WITH issues AS (
      SELECT cursor_row.channel_id,cursor_row.domain,cursor_row.updated_at AS observed_at,
             cursor_row.active_revision_id,
             CASE
               WHEN revision.revision_id IS NULL THEN 'revision_missing'
               WHEN revision.activation_status<>'active' THEN 'revision_not_active'
               WHEN activation_item.revision_id IS NULL THEN 'activation_item_missing'
               ELSE 'cursor_revision_values_differ'
             END AS reason
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
    ), summary AS (
      SELECT count(*)::int AS count,min(observed_at) AS oldest_at
      FROM issues
    ), samples AS (
      SELECT COALESCE(
               jsonb_agg(
                 jsonb_build_object(
                   'channel_id',channel_id,
                   'domain',domain,
                   'active_revision_id',active_revision_id,
                   'reason',reason
                 ) ORDER BY observed_at,channel_id,domain
               ),
               '[]'::jsonb
             ) AS samples
      FROM (
        SELECT *
        FROM issues
        ORDER BY observed_at,channel_id,domain
        LIMIT $1
      ) AS sample_rows
    )
    SELECT summary.count,summary.oldest_at,samples.samples
    FROM summary CROSS JOIN samples`, [sampleSize]);
}

async function inspectOwnershipCursor(pool, sampleSize) {
  return finding(pool, `
    WITH issues AS (
      SELECT cursor_row.channel_id,cursor_row.domain,cursor_row.updated_at AS observed_at,
             cursor_row.publication_stream_id AS cursor_stream_id,
             ownership.active_publication_stream_id AS ownership_stream_id,
             ownership.status AS ownership_status
      FROM publication.consumer_cursor AS cursor_row
      LEFT JOIN publication.channel_ownership AS ownership
        ON ownership.channel_id=cursor_row.channel_id
      WHERE ownership.channel_id IS NULL
         OR ownership.status<>'active'
         OR ownership.active_publication_stream_id IS DISTINCT FROM cursor_row.publication_stream_id
    ), summary AS (
      SELECT count(*)::int AS count,min(observed_at) AS oldest_at
      FROM issues
    ), samples AS (
      SELECT COALESCE(
               jsonb_agg(
                 jsonb_build_object(
                   'channel_id',channel_id,
                   'domain',domain,
                   'cursor_stream_id',cursor_stream_id,
                   'ownership_stream_id',ownership_stream_id,
                   'ownership_status',ownership_status
                 ) ORDER BY observed_at,channel_id,domain
               ),
               '[]'::jsonb
             ) AS samples
      FROM (
        SELECT *
        FROM issues
        ORDER BY observed_at,channel_id,domain
        LIMIT $1
      ) AS sample_rows
    )
    SELECT summary.count,summary.oldest_at,samples.samples
    FROM summary CROSS JOIN samples`, [sampleSize]);
}

async function inspectLongLivedGaps(pool, sampleSize, gapAlertSeconds) {
  return finding(pool, `
    WITH issues AS (
      SELECT revision.channel_id,revision.domain,min(revision.received_at) AS observed_at,
             COALESCE(cursor_row.active_sequence,0)+1 AS expected_sequence,
             min(revision.data_sequence) AS first_waiting_sequence,
             min(revision.received_at) AS oldest_received_at
      FROM publication.revision AS revision
      LEFT JOIN publication.consumer_cursor AS cursor_row
        ON cursor_row.channel_id=revision.channel_id AND cursor_row.domain=revision.domain
      WHERE revision.validation_status='valid'
        AND revision.activation_status='waiting_gap'
      GROUP BY revision.channel_id,revision.domain,cursor_row.active_sequence
      HAVING min(revision.received_at)<=now()-($2::int*interval '1 second')
    ), summary AS (
      SELECT count(*)::int AS count,min(observed_at) AS oldest_at
      FROM issues
    ), samples AS (
      SELECT COALESCE(
               jsonb_agg(
                 jsonb_build_object(
                   'channel_id',channel_id,
                   'domain',domain,
                   'expected_sequence',expected_sequence,
                   'first_waiting_sequence',first_waiting_sequence,
                   'oldest_received_at',oldest_received_at
                 ) ORDER BY observed_at,channel_id,domain
               ),
               '[]'::jsonb
             ) AS samples
      FROM (
        SELECT *
        FROM issues
        ORDER BY observed_at,channel_id,domain
        LIMIT $1
      ) AS sample_rows
    )
    SELECT summary.count,summary.oldest_at,samples.samples
    FROM summary CROSS JOIN samples`, [sampleSize, gapAlertSeconds]);
}

async function inspectOpenQuarantine(pool, sampleSize) {
  return finding(pool, `
    WITH issues AS (
      SELECT inbox.channel_id,inbox.domain,quarantine.first_seen_at AS observed_at,
             quarantine.revision_id,quarantine.issue_code,
             quarantine.first_seen_at
      FROM publication.quarantine AS quarantine
      JOIN publication.inbox AS inbox ON inbox.revision_id=quarantine.revision_id
      WHERE quarantine.status='open'
    ), summary AS (
      SELECT count(*)::int AS count,min(observed_at) AS oldest_at
      FROM issues
    ), samples AS (
      SELECT COALESCE(
               jsonb_agg(
                 jsonb_build_object(
                   'channel_id',channel_id,
                   'domain',domain,
                   'revision_id',revision_id,
                   'issue_code',issue_code,
                   'first_seen_at',first_seen_at
                 ) ORDER BY observed_at,channel_id,domain
               ),
               '[]'::jsonb
             ) AS samples
      FROM (
        SELECT *
        FROM issues
        ORDER BY observed_at,channel_id,domain
        LIMIT $1
      ) AS sample_rows
    )
    SELECT summary.count,summary.oldest_at,samples.samples
    FROM summary CROSS JOIN samples`, [sampleSize]);
}

async function inspectOldStreamPending(pool, sampleSize) {
  return finding(pool, `
    WITH issues AS (
      SELECT revision.channel_id,revision.domain,revision.received_at AS observed_at,
             revision.revision_id,
             revision.publication_stream_id AS revision_stream_id,
             ownership.active_publication_stream_id AS active_stream_id,
             revision.data_sequence AS sequence
      FROM publication.revision AS revision
      JOIN publication.channel_ownership AS ownership
        ON ownership.channel_id=revision.channel_id
      WHERE revision.validation_status='valid'
        AND revision.activation_status IN ${PENDING_ACTIVATION_STATUSES}
        AND revision.publication_stream_id<>ownership.active_publication_stream_id
    ), summary AS (
      SELECT count(*)::int AS count,min(observed_at) AS oldest_at
      FROM issues
    ), samples AS (
      SELECT COALESCE(
               jsonb_agg(
                 jsonb_build_object(
                   'channel_id',channel_id,
                   'domain',domain,
                   'revision_id',revision_id,
                   'revision_stream_id',revision_stream_id,
                   'active_stream_id',active_stream_id,
                   'sequence',sequence
                 ) ORDER BY observed_at,channel_id,domain
               ),
               '[]'::jsonb
             ) AS samples
      FROM (
        SELECT *
        FROM issues
        ORDER BY observed_at,channel_id,domain
        LIMIT $1
      ) AS sample_rows
    )
    SELECT summary.count,summary.oldest_at,samples.samples
    FROM summary CROSS JOIN samples`, [sampleSize]);
}

async function inspectActiveWithoutAudit(pool, sampleSize) {
  return finding(pool, `
    WITH issues AS (
      SELECT revision.channel_id,revision.domain,revision.updated_at AS observed_at,
             revision.revision_id,revision.data_sequence AS sequence
      FROM publication.revision AS revision
      WHERE revision.activation_status='active'
        AND NOT EXISTS (
          SELECT 1
          FROM publication.activation_item AS activation_item
          WHERE activation_item.revision_id=revision.revision_id
        )
    ), summary AS (
      SELECT count(*)::int AS count,min(observed_at) AS oldest_at
      FROM issues
    ), samples AS (
      SELECT COALESCE(
               jsonb_agg(
                 jsonb_build_object(
                   'channel_id',channel_id,
                   'domain',domain,
                   'revision_id',revision_id,
                   'sequence',sequence
                 ) ORDER BY observed_at,channel_id,domain
               ),
               '[]'::jsonb
             ) AS samples
      FROM (
        SELECT *
        FROM issues
        ORDER BY observed_at,channel_id,domain
        LIMIT $1
      ) AS sample_rows
    )
    SELECT summary.count,summary.oldest_at,samples.samples
    FROM summary CROSS JOIN samples`, [sampleSize]);
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
             projection.projection_id,projection.activation_id,
             projection.status,projection.attempts,
             projection.next_attempt_at,projection.lease_expires_at
      FROM publication.projection_outbox AS projection
      WHERE (projection.status='pending'
             AND projection.created_at<=now()-($2::int*interval '1 second'))
         OR (projection.status='leased' AND projection.lease_expires_at<=now())
         OR (projection.status='retry_wait' AND projection.next_attempt_at<=now()
             AND projection.updated_at<=now()-($2::int*interval '1 second'))
    ), summary AS (
      SELECT count(*)::int AS count,min(observed_at) AS oldest_at
      FROM issues
    ), samples AS (
      SELECT COALESCE(
               jsonb_agg(
                 jsonb_build_object(
                   'channel_id',channel_id,
                   'projection_id',projection_id,
                   'activation_id',activation_id,
                   'status',status,
                   'attempts',attempts,
                   'next_attempt_at',next_attempt_at,
                   'lease_expires_at',lease_expires_at
                 ) ORDER BY observed_at,channel_id
               ),
               '[]'::jsonb
             ) AS samples
      FROM (
        SELECT *
        FROM issues
        ORDER BY observed_at,channel_id
        LIMIT $1
      ) AS sample_rows
    )
    SELECT summary.count,summary.oldest_at,samples.samples
    FROM summary CROSS JOIN samples`, [sampleSize, projectionStuckSeconds]);
}

async function inspectProjectionDeadLetter(pool, sampleSize) {
  return finding(pool, `
    WITH summary AS (
      SELECT count(*)::int AS count,min(updated_at) AS oldest_at
      FROM publication.projection_outbox AS projection
      WHERE projection.status='dead_letter'
    ), samples AS (
      SELECT COALESCE(
               jsonb_agg(
                 jsonb_build_object(
                   'channel_id',channel_id,
                   'projection_id',projection_id,
                   'activation_id',activation_id,
                   'attempts',attempts,
                   'last_error',last_error
                 ) ORDER BY updated_at,channel_id
               ),
               '[]'::jsonb
             ) AS samples
      FROM (
        SELECT channel_id,updated_at,projection_id,activation_id,attempts,last_error
        FROM publication.projection_outbox
        WHERE status='dead_letter'
        ORDER BY updated_at,channel_id
        LIMIT $1
      ) AS sample_rows
    )
    SELECT summary.count,summary.oldest_at,samples.samples
    FROM summary CROSS JOIN samples`, [sampleSize]);
}

async function inspectBlockedActivation(pool, sampleSize, gapAlertSeconds) {
  return finding(pool, `
    WITH issues AS (
      SELECT state.channel_id,state.last_attempted_at AS observed_at,
             state.last_outcome,state.attempt_count,state.last_attempted_at
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
    ), summary AS (
      SELECT count(*)::int AS count,min(observed_at) AS oldest_at
      FROM issues
    ), samples AS (
      SELECT COALESCE(
               jsonb_agg(
                 jsonb_build_object(
                   'channel_id',channel_id,
                   'last_outcome',last_outcome,
                   'attempt_count',attempt_count,
                   'last_attempted_at',last_attempted_at
                 ) ORDER BY observed_at,channel_id
               ),
               '[]'::jsonb
             ) AS samples
      FROM (
        SELECT *
        FROM issues
        ORDER BY observed_at,channel_id
        LIMIT $1
      ) AS sample_rows
    )
    SELECT summary.count,summary.oldest_at,samples.samples
    FROM summary CROSS JOIN samples`, [sampleSize, gapAlertSeconds]);
}

async function inspectActivationErrors(pool, sampleSize) {
  return finding(pool, `
    WITH issues AS (
      SELECT state.channel_id,state.last_attempted_at AS observed_at,
             state.attempt_count,state.consecutive_error_count,
             state.last_error,state.next_attempt_at
      FROM publication.reconciliation_state AS state
      WHERE state.last_outcome='error'
        AND (state.lease_owner IS NULL OR state.lease_expires_at<=now())
        AND EXISTS (
          SELECT 1 FROM publication.revision AS revision
          WHERE revision.channel_id=state.channel_id
            AND revision.validation_status='valid'
            AND revision.activation_status IN ${PENDING_ACTIVATION_STATUSES}
        )
    ), summary AS (
      SELECT count(*)::int AS count,min(observed_at) AS oldest_at
      FROM issues
    ), samples AS (
      SELECT COALESCE(
               jsonb_agg(
                 jsonb_build_object(
                   'channel_id',channel_id,
                   'attempt_count',attempt_count,
                   'consecutive_error_count',consecutive_error_count,
                   'last_error',last_error,
                   'next_attempt_at',next_attempt_at
                 ) ORDER BY observed_at,channel_id
               ),
               '[]'::jsonb
             ) AS samples
      FROM (
        SELECT *
        FROM issues
        ORDER BY observed_at,channel_id
        LIMIT $1
      ) AS sample_rows
    )
    SELECT summary.count,summary.oldest_at,samples.samples
    FROM summary CROSS JOIN samples`, [sampleSize]);
}

function taggedClient(client, key) {
  return {
    query(sql, params) {
      return client.query(`/* business-publication-auditor:${key} */${sql}`, params);
    },
  };
}

export async function inspectBusinessPublication(client, {
  sampleSize = 10,
  gapAlertSeconds = 900,
  projectionStuckSeconds = 300,
  monotonicClock = () => performance.now(),
} = {}) {
  const inspections = [
    ["cursor_current_mismatch", (queryable) => inspectCursorCurrent(queryable, sampleSize)],
    ["cursor_revision_mismatch", (queryable) => inspectCursorRevision(queryable, sampleSize)],
    ["ownership_cursor_mismatch", (queryable) => inspectOwnershipCursor(queryable, sampleSize)],
    ["long_lived_gap", (queryable) => (
      inspectLongLivedGaps(queryable, sampleSize, gapAlertSeconds)
    )],
    ["open_quarantine", (queryable) => inspectOpenQuarantine(queryable, sampleSize)],
    ["old_stream_pending", (queryable) => inspectOldStreamPending(queryable, sampleSize)],
    ["active_without_activation", (queryable) => inspectActiveWithoutAudit(queryable, sampleSize)],
    ["projection_stuck", (queryable) => (
      inspectProjectionStuck(queryable, sampleSize, projectionStuckSeconds)
    )],
    ["projection_dead_letter", (queryable) => inspectProjectionDeadLetter(queryable, sampleSize)],
    ["blocked_activation", (queryable) => (
      inspectBlockedActivation(queryable, sampleSize, gapAlertSeconds)
    )],
    ["activation_error", (queryable) => inspectActivationErrors(queryable, sampleSize)],
  ];
  const findings = {};
  const queryDurationMs = {};
  for (const [key, inspect] of inspections) {
    const startedAt = monotonicClock();
    try {
      findings[key] = await inspect(taggedClient(client, key));
      queryDurationMs[key] = Math.max(0, Math.round(monotonicClock() - startedAt));
    } catch (error) {
      const failure = new Error(
        `Business Publication audit query ${key} failed: ${String(error?.message || error)}`,
        { cause: error },
      );
      failure.failedQuery = key;
      failure.completedQueryCount = Object.keys(findings).length;
      failure.queryDurationMs = { ...queryDurationMs };
      throw failure;
    }
  }
  return {
    issue_count: Object.values(findings).reduce((sum, item) => sum + item.count, 0),
    findings,
    query_count: inspections.length,
    query_duration_ms: queryDurationMs,
  };
}
