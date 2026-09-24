import { INCREMENTAL_QUEUE } from './incrementalPlan.js';

// Cursors retain PostgreSQL microseconds: converting started_at to a JS Date
// would repeat records that share a millisecond. A cursor belongs to one window.
export async function loadIncrementalBudgetPage(pool, { since, until, limit = 25, cursor = null }) {
  const start = new Date(since), end = new Date(until);
  if (!Number.isFinite(+start) || !Number.isFinite(+end) || +end <= +start || +end - +start > 86400000
      || !Number.isSafeInteger(limit) || limit < 1 || limit > 50) {
    throw new Error('bounded 24h window and limit 1..50 required');
  }
  const window = { since: start.toISOString(), until: end.toISOString() };
  let after = null;
  if (cursor !== null) {
    try {
      after = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
      if (after.since !== window.since || after.until !== window.until
          || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/.test(after.started_at)
          || !Number.isFinite(Date.parse(after.started_at))
          || Date.parse(after.started_at) < +start || Date.parse(after.started_at) >= +end
          || !/^incremental:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(after.run_id)) throw new Error();
    } catch { throw new Error('invalid budget reconciliation cursor or mismatched window'); }
  }
  const rows = (await pool.query(`WITH failures AS MATERIALIZED (
    SELECT DISTINCT job_id FROM crawler.task_events WHERE queue_name=$3
      AND created_at >= $1 AND created_at < $2 AND status='failed'
      AND error_message LIKE 'Rota Business Run budget exhausted%'
    ) SELECT r.run_id,r.plan_id,d.job_id,d.payload_json,
    to_char(r.started_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS cursor_started_at,
    (SELECT max(a.job_attempt)+1 FROM crawler.channel_execution_attempts a WHERE a.channel_id=r.channel_id AND a.business_run_id=r.run_id) activation
    FROM failures f JOIN feature_clock.dispatch_outbox d ON d.job_id=f.job_id
    JOIN crawler.channel_runs r ON r.plan_id=d.plan_id
    JOIN crawler.business_run_bindings b ON b.business_run_id=r.run_id
    WHERE r.crawl_mode='incremental' AND r.status='failed' AND b.status='materialized'
      AND r.started_at >= $1 AND r.started_at < $2
      AND ($5::timestamptz IS NULL OR (r.started_at,r.run_id)>($5::timestamptz,$6::text))
    ORDER BY r.started_at,r.run_id LIMIT $4`,
  [window.since, window.until, INCREMENTAL_QUEUE, limit + 1, after?.started_at ?? null, after?.run_id ?? null])).rows;
  const candidates = rows.slice(0, limit), last = candidates.at(-1);
  return {
    candidates,
    next_cursor: rows.length > limit ? Buffer.from(JSON.stringify({
      ...window, started_at: last.cursor_started_at, run_id: last.run_id,
    })).toString('base64url') : null,
  };
}
