import assert from "node:assert/strict";

// Keep three complete releases, the initialization baseline, and every applied
// cohort rollback reference. Release metadata and change journals are never pruned.
const RETENTION_SQL = `
  WITH recent AS (
    SELECT r.watermark FROM public.creator_search_releases r
    WHERE r.storage_mode='shadow' AND EXISTS (
      SELECT 1 FROM public.creator_search_current c WHERE c.watermark=r.watermark
    ) ORDER BY r.created_at DESC,r.watermark DESC LIMIT 3
  ), retained AS (
    SELECT initialized_watermark AS watermark FROM publication.creator_search_storage_state
    UNION SELECT a.watermark FROM public.creator_search_active a WHERE EXISTS (
      SELECT 1 FROM public.creator_search_current c WHERE c.watermark=a.watermark
    )
    UNION SELECT previous_watermark FROM publication.projection_cutover WHERE status='applied'
    UNION SELECT watermark FROM recent
  ) SELECT watermark FROM retained WHERE watermark IS NOT NULL ORDER BY watermark`;

async function state(client, database) {
  const row = (await client.query(`SELECT current_database() AS database,
    identity.database_kind,identity.database_name AS identity_database,s.*,
    a.watermark AS active_watermark
    FROM publication.database_identity identity
    CROSS JOIN publication.creator_search_storage_state s
    CROSS JOIN public.creator_search_active a
    WHERE identity.singleton AND s.singleton AND a.singleton`)).rows[0];
  assert.ok(row, "Business search storage identity is missing");
  assert.equal(row.database, database, "Unexpected database");
  assert.equal(row.database_kind, "business");
  assert.equal(row.identity_database, database);
  assert.equal(row.write_mode, "incremental", "Prune requires incremental writes");
  assert.equal(row.read_mode, "live", "Prune requires Live reads");
  assert.ok(row.cutover_at, "Storage cutover must be recorded");
  return row;
}

async function rowCounts(client, watermarks) {
  return (await client.query(`SELECT requested.watermark,
    (SELECT count(*)::int FROM public.creator_search_current c
     WHERE c.watermark=requested.watermark) AS row_count
    FROM unnest($1::text[]) requested(watermark) ORDER BY requested.watermark`,
  [watermarks])).rows;
}

export async function planLegacySearchPrune(client, database) {
  await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try {
    await client.query("SET LOCAL statement_timeout='120s'");
    const storage = await state(client, database);
    const retainedIds = (await client.query(RETENTION_SQL)).rows.map((r) => r.watermark);
    const inventory = (await client.query(`WITH counts AS (
      SELECT watermark,count(*)::int row_count FROM public.creator_search_current GROUP BY watermark
    ) SELECT r.watermark,r.status,r.storage_mode,r.created_at,c.row_count
      FROM counts c JOIN public.creator_search_releases r USING(watermark)
      ORDER BY r.created_at,r.watermark`)).rows;
    const retained = await rowCounts(client, retainedIds);
    for (const r of retained) {
      assert.ok(r.watermark === storage.initialized_watermark
        ? r.row_count === storage.initialized_row_count : r.row_count > 0,
      `Retained rollback snapshot is incomplete: ${r.watermark}`);
    }
    const candidates = inventory.filter((r) => !retainedIds.includes(r.watermark));
    for (const r of candidates) {
      assert.equal(r.status, "retired");
      assert.equal(r.storage_mode, "shadow");
      assert.ok(new Date(r.created_at) < new Date(storage.cutover_at));
    }
    const baseline = (await client.query(`SELECT count(*)::int AS live_count,
      md5(string_agg(md5((to_jsonb(l)-'watermark')::text),'' ORDER BY channel_id)) AS live_fingerprint
      FROM public.creator_search_live l`)).rows[0];
    return {
      format: "creator-search-legacy-prune-v1", database,
      observed_at: new Date().toISOString(),
      initialized_watermark: storage.initialized_watermark,
      initialized_row_count: storage.initialized_row_count,
      cutover_at: storage.cutover_at,
      retained, candidates, ...baseline,
      delete_rows: candidates.reduce((n, r) => n + r.row_count, 0),
    };
  } finally {
    await client.query("ROLLBACK");
  }
}

export async function pruneLegacySearchBatch(client, {
  plan, database, watermark, expectedRemaining, batchSize = 5000, actor, reason,
}) {
  assert.equal(plan.format, "creator-search-legacy-prune-v1");
  assert.equal(plan.database, database);
  assert.ok(actor?.trim() && reason?.trim(), "Prune actor and reason are required");
  assert.ok(Number.isSafeInteger(batchSize) && batchSize > 0 && batchSize <= 10000);
  const target = plan.candidates.find((r) => r.watermark === watermark);
  assert.ok(target, "Target is not in the fixed prune plan");
  assert.ok(Number.isSafeInteger(expectedRemaining) && expectedRemaining > 0
    && expectedRemaining <= target.row_count);
  await client.query("BEGIN");
  try {
    await client.query("SET LOCAL lock_timeout='250ms'");
    await client.query("SET LOCAL statement_timeout='10s'");
    // Same order as the existing prune function. Never queue ahead of a publisher.
    for (const query of [
      "SELECT pg_try_advisory_xact_lock(781137233) AS acquired",
      "SELECT pg_try_advisory_xact_lock(hashtext('kol_demo:creator-search-publish')) AS acquired",
    ]) {
      if (!(await client.query(query)).rows[0].acquired) {
        await client.query("ROLLBACK");
        return { outcome: "busy", deleted: 0 };
      }
    }
    const current = await state(client, database);
    assert.equal(current.initialized_watermark, plan.initialized_watermark);
    assert.equal(current.initialized_row_count, plan.initialized_row_count);
    assert.equal(new Date(current.cutover_at).toISOString(), new Date(plan.cutover_at).toISOString(),
      "Storage cutover changed after planning");
    const mandatory = (await client.query(RETENTION_SQL)).rows.map((r) => r.watermark);
    const retainedIds = [...new Set([...mandatory, ...plan.retained.map((r) => r.watermark)])].sort();
    assert.ok(!retainedIds.includes(watermark), "Target is a protected rollback release");
    const retained = await rowCounts(client, retainedIds);
    for (const r of retained) {
      const expected = plan.retained.find((p) => p.watermark === r.watermark);
      if (expected) assert.equal(r.row_count, expected.row_count, "Retained snapshot count changed");
      assert.ok(r.watermark === current.initialized_watermark
        ? r.row_count === current.initialized_row_count : r.row_count > 0,
      "Retained rollback snapshot is incomplete");
    }
    const release = (await client.query(`SELECT status,storage_mode,created_at
      FROM public.creator_search_releases WHERE watermark=$1 FOR UPDATE`, [watermark])).rows[0];
    assert.equal(release?.status, "retired", "Target must still be retired");
    assert.equal(release.storage_mode, "shadow");
    assert.equal(new Date(release.created_at).toISOString(), new Date(target.created_at).toISOString());
    assert.ok(new Date(release.created_at) < new Date(current.cutover_at));
    const actual = (await rowCounts(client, [watermark]))[0].row_count;
    assert.equal(actual, expectedRemaining, "Target count changed after planning");
    const deleted = (await client.query(`WITH victims AS MATERIALIZED (
      SELECT ctid FROM public.creator_search_current WHERE watermark=$1
      ORDER BY channel_id LIMIT $2
    ) DELETE FROM public.creator_search_current c USING victims v
      WHERE c.ctid=v.ctid AND c.watermark=$1`, [watermark, batchSize])).rowCount;
    assert.equal(deleted, Math.min(batchSize, expectedRemaining));
    await client.query(`INSERT INTO publication.creator_search_legacy_prune_audit
      (active_watermark,retained_watermarks,deleted_row_count,pruned_by,prune_reason)
      VALUES ($1,$2,$3,$4,$5)`,
    [current.active_watermark, retainedIds, deleted, actor.trim(), `${reason.trim()}; target=${watermark}`]);
    await client.query("COMMIT");
    return { outcome: "pruned", deleted, remaining: expectedRemaining - deleted };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}
