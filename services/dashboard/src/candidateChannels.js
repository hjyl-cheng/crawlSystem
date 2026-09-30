// Query Discover candidates in crawler.channel_candidates. Legacy migration rows
// share the table, so every read is first scoped to Query dispatch batches
// (batches with query pages) and then to the search-discovery source. Batch IDs
// are bound as an array: a subquery hides from the planner that one legacy
// batch holds nearly every row, and it then scans the whole table.
export const CANDIDATE_SOURCE = 'youtube_search_discovery';
export const CANDIDATE_STATUSES = Object.freeze(['discovered', 'queued', 'validating', 'accepted', 'rejected', 'existing', 'failed']);
export const CANDIDATE_PENDING_STATUSES = Object.freeze(['discovered', 'queued', 'validating']);
export const CANDIDATE_STATUS_LABELS = Object.freeze({
  discovered: '待验证', queued: '排队验证', validating: '验证中', accepted: '已入库',
  rejected: '不符合', existing: '已在库', failed: '验证失败',
});

function intValue(value, fallback, min, max) {
  const n = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : fallback;
}

function dayValue(value) {
  const text = String(value ?? '').trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(text) && !Number.isNaN(Date.parse(`${text}T00:00:00Z`))
    && new Date(`${text}T00:00:00Z`).toISOString().startsWith(text) ? text : '';
}

export function candidateFilters(query = {}) {
  const status = String(query.status ?? '').trim();
  const batch = String(query.batch ?? '').trim();
  return {
    limit: intValue(query.limit, 100, 1, 500), offset: intValue(query.offset, 0, 0, 1_000_000),
    batch: batch.length <= 200 ? batch : '', status: CANDIDATE_STATUSES.includes(status) ? status : '',
    search: String(query.q ?? '').trim().slice(0, 200), from: dayValue(query.from), to: dayValue(query.to),
  };
}

// Days are Beijing dates, as every dashboard time is shown in Beijing time.
export function candidateScope(filters, batchIds, { includeStatus = true } = {}) {
  const scoped = filters.batch ? batchIds.filter(id => id === filters.batch) : batchIds;
  const args = [scoped, CANDIDATE_SOURCE];
  const where = ['c.dispatch_batch_id=ANY($1::text[])', "c.source_json->>'source'=$2"];
  if (includeStatus && filters.status) { args.push(filters.status); where.push(`c.status=$${args.length}`); }
  if (filters.search) {
    args.push(`%${filters.search.replace(/[\\%_]/g, char => `\\${char}`)}%`);
    const p = `$${args.length}`;
    where.push(`(c.source_json->>'query_text' ILIKE ${p} OR c.channel_id ILIKE ${p} OR c.handle ILIKE ${p} OR c.title ILIKE ${p})`);
  }
  if (filters.from) { args.push(filters.from); where.push(`c.created_at>=($${args.length}::date::timestamp AT TIME ZONE 'Asia/Shanghai')`); }
  if (filters.to) { args.push(filters.to); where.push(`c.created_at<(($${args.length}::date+1)::timestamp AT TIME ZONE 'Asia/Shanghai')`); }
  return { where: where.join(' AND '), args };
}

export function candidateListSql(filters, batchIds) {
  const { where, args } = candidateScope(filters, batchIds);
  args.push(filters.limit + 1, filters.offset);
  return { args, sql: `SELECT c.candidate_id,c.dispatch_batch_id,c.channel_id,c.channel_url,c.handle,c.title,c.avatar_url,
      c.search_subscriber_count,c.search_subscriber_count_text,c.is_verified,c.status,c.reject_reason,c.error_message,
      c.created_at,c.validation_finished_at,c.accepted_at,
      c.source_json->>'query_text' AS query_text,c.source_json->>'query_id' AS query_id,
      CASE WHEN c.source_json->>'rank_position' ~ '^[0-9]{1,9}$' THEN (c.source_json->>'rank_position')::int END AS rank_position,
      page.page_no
    FROM crawler.channel_candidates c
    LEFT JOIN crawler.query_pages page ON page.page_id=c.source_json->>'page_id'
    WHERE ${where}
    ORDER BY c.created_at DESC,c.candidate_id DESC
    LIMIT $${args.length - 1} OFFSET $${args.length}` };
}

export const CANDIDATE_BATCHES_SQL = `SELECT b.dispatch_batch_id,b.status,b.started_at,b.finished_at
  FROM crawler.query_dispatch_batches b
  WHERE EXISTS(SELECT 1 FROM crawler.query_pages p WHERE p.dispatch_batch_id=b.dispatch_batch_id)
  ORDER BY b.started_at DESC,b.dispatch_batch_id DESC`;

// The page itself never counts; statistics load separately through the cache.
export async function loadCandidateList({ read, filters }) {
  const batches = (await read(CANDIDATE_BATCHES_SQL, [])).rows;
  const list = candidateListSql(filters, batches.map(batch => batch.dispatch_batch_id));
  const rows = (await read(list.sql, list.args)).rows;
  return { batches: batches.slice(0, 30), candidates: rows.slice(0, filters.limit), hasNext: rows.length > filters.limit };
}

export async function loadCandidateStatistics({ readStatistics, cache, filters }) {
  const entry = await cache.get(JSON.stringify(['candidate-status', filters.batch, filters.search, filters.from, filters.to]), async () => {
    const batchIds = (await readStatistics(CANDIDATE_BATCHES_SQL, [])).rows.map(batch => batch.dispatch_batch_id);
    const { where, args } = candidateScope(filters, batchIds, { includeStatus: false });
    const result = await readStatistics(`SELECT c.status,count(*)::bigint AS n FROM crawler.channel_candidates c
      WHERE ${where} GROUP BY c.status`, args);
    return Object.fromEntries(result.rows.map(row => [row.status, Number(row.n)]));
  });
  const byStatus = Object.fromEntries(CANDIDATE_STATUSES.map(status => [status, entry.value[status] ?? 0]));
  const all = Object.values(byStatus).reduce((sum, n) => sum + n, 0);
  const pending = CANDIDATE_PENDING_STATUSES.reduce((sum, status) => sum + byStatus[status], 0);
  return { byStatus, all, pending, total: filters.status ? byStatus[filters.status] : all,
    generatedAt: entry.generatedAt, stale: entry.stale };
}

// A bounded, read-only transaction for list requests.
export async function readCandidatesWithin(pool, sql, args, timeout = '15s') {
  const client = await pool.connect();
  try {
    await client.query('BEGIN READ ONLY');
    await client.query(`SET LOCAL statement_timeout='${timeout}'`);
    const result = await client.query(sql, args);
    await client.query('COMMIT');
    return result;
  } catch (error) { await client.query('ROLLBACK').catch(() => {}); throw error; }
  finally { client.release(); }
}
