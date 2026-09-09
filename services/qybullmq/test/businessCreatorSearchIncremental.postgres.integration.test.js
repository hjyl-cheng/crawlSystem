import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { performance } from "node:perf_hooks";
import test from "node:test";
import pg from "pg";
import { generatePublicationReadinessReport } from "../src/publicationReadinessReport.js";

// Bootstrap an isolated PostgreSQL 18 database with database/bootstrap/business.sql.
const databaseUrl = process.env.CREATOR_SEARCH_INCREMENTAL_POSTGRES_TEST_URL;

test("real Search publishing preserves results while writes scale with changed channels", {
  skip: !databaseUrl,
  timeout: 180_000,
}, async (t) => {
  const url = new URL(databaseUrl);
  assert.match(url.pathname, /_test$/);
  assert.ok(["localhost", "127.0.0.1", "::1"].includes(url.hostname));
  const client = new pg.Client({ connectionString: databaseUrl });
  await client.connect();
  try {
    await client.query("BEGIN");
    await client.query("SET LOCAL statement_timeout='60s'");
    assert.equal(Number((await client.query("SELECT count(*) FROM public.channels")).rows[0].count), 0,
      "use an empty, bootstrapped test database");
    await client.query(await readFile(new URL("../src/businessCreatorSearchIncrementalSchema.sql", import.meta.url), "utf8"));
    const total = 1000;
    const ids = Array.from({ length: total }, (_, i) => `UC_search_test_${String(i + 1).padStart(6, "0")}`);
    const batch = async (id) => client.query(`INSERT INTO public.import_batches
      (id,source_file,source_sha256,captured_at,raw_payload)
      VALUES ($1,'search-storage-test',$2,'2026-09-09T00:00:00Z','{}')`,
    [id, createHash("sha256").update(id).digest("hex")]);
    await batch("search-baseline");
    await client.query(`INSERT INTO public.channels(channel_id) SELECT unnest($1::text[])`, [ids]);
    await client.query(`INSERT INTO public.channel_snapshots
      (id,channel_id,import_batch_id,captured_at,title,handle,country_text,raw_channel,
       subscriber_count,subscriber_count_status,channel_observed_at,subscriber_count_observed_at)
      SELECT 'snapshot-'||channel_id,channel_id,'search-baseline','2026-09-09T00:00:00Z',
        CASE WHEN ordinal%2=0 THEN 'Receitas ' ELSE 'Music ' END||ordinal,
        '@test'||ordinal,CASE WHEN ordinal%3=0 THEN 'Brazil' ELSE 'United States' END,
        '{}',ordinal*100,'exact','2026-09-09T00:00:00Z','2026-09-09T00:00:00Z'
      FROM unnest($1::text[]) WITH ORDINALITY AS item(channel_id,ordinal)`, [ids]);
    await client.query(`INSERT INTO public.channel_profile_facts
      (id,channel_id,channel_snapshot_id,field_key,value_json,source,confidence)
      SELECT 'language-'||channel_id,channel_id,'snapshot-'||channel_id,'creator_language',
        to_jsonb(CASE WHEN ordinal%3=0 THEN 'pt' ELSE 'en' END::text),'test','high'
      FROM unnest($1::text[]) WITH ORDINALITY AS item(channel_id,ordinal)`, [ids]);
    const publish = async (watermark, upserts, removed = []) => {
      await batch(watermark);
      // Follow the production contract: every upsert has a new Snapshot owned
      // by this publication batch; previously published Snapshots stay immutable.
      await client.query(`INSERT INTO public.channel_snapshots
        SELECT (jsonb_populate_record(NULL::public.channel_snapshots,
          to_jsonb(previous)||jsonb_build_object(
            'id',$1::text||'-'||previous.channel_id,'import_batch_id',$1::text,
            'captured_at',previous.captured_at+interval '1 second',
            'title','Updated receitas','subscriber_count',200000))).*
        FROM (SELECT DISTINCT ON(channel_id) * FROM public.channel_snapshots
          WHERE channel_id=ANY($2) ORDER BY channel_id,captured_at DESC,id DESC) previous`,
      [watermark, upserts]);
      const before = Number((await client.query(`SELECT n_tup_ins FROM pg_stat_xact_user_tables
        WHERE schemaname='public' AND relname='creator_search_current'`)).rows[0].n_tup_ins);
      const started = performance.now();
      await client.query("SELECT public.refresh_creator_search_release_v9($1,$2,$3)", [watermark, upserts, removed]);
      const elapsed = performance.now() - started;
      const after = Number((await client.query(`SELECT n_tup_ins FROM pg_stat_xact_user_tables
        WHERE schemaname='public' AND relname='creator_search_current'`)).rows[0].n_tup_ins);
      return { current_inserted: after - before, elapsed_ms: Math.round(elapsed) };
    };
    await client.query("SELECT public.refresh_creator_search_release_v9($1,$2,'{}')", ["search-baseline", ids]);
    const documents = async () => (await client.query(`SELECT to_jsonb(s)-'watermark' AS document
      FROM public.creator_search_live s ORDER BY channel_id`)).rows;
    const baseline = await documents();
    const searches = async () => {
      const results = [];
      for (const predicate of ["true", "search_text ILIKE '%receitas%'", "country='BR' AND language='pt'", "subscribers BETWEEN 1000 AND 9000"]) {
        for (const offset of [0, 20]) {
          results.push((await client.query(`SELECT channel_id,name,country,language,subscribers
            FROM public.creator_search_live WHERE ${predicate}
            ORDER BY subscribers DESC NULLS LAST,channel_id LIMIT 20 OFFSET $1`, [offset])).rows);
        }
      }
      return results;
    };
    const baselineSearches = await searches();
    const readiness = (channelIds = null) => generatePublicationReadinessReport({
      crawlerQuery: async () => ({ rows: [] }),
      businessQuery: (sql, values) => client.query(sql, values),
      channelIds,
    });
    assert.equal((await readiness()).scope.business_active_channel_count, total);
    const changed = ids.slice(0, 10);
    let shadow;
    let expected;
    let expectedSearches;
    await t.test("legacy control copies the full 1,000-channel release", async () => {
      await client.query("SAVEPOINT shadow_control");
      shadow = await publish("search-update", changed);
      assert.equal(shadow.current_inserted, total);
      expected = await documents();
      expectedSearches = await searches();
      await client.query("ROLLBACK TO SAVEPOINT shadow_control");
    });
    await t.test("cutover rejects stale versions and equal-count data mismatch", async () => {
      for (const mismatch of ["watermark", "data"]) {
        await client.query("SAVEPOINT invalid_cutover");
        if (mismatch === "data") await client.query("UPDATE public.creator_search_live SET name='wrong' WHERE channel_id=$1", [ids[0]]);
        await assert.rejects(client.query("SELECT public.activate_creator_search_incremental_v1($1,$2,'test','test')",
          [mismatch === "watermark" ? "stale" : "search-baseline", total]), mismatch === "watermark" ? /watermark changed/ : /differs from the active Legacy/);
        await client.query("ROLLBACK TO SAVEPOINT invalid_cutover");
      }
      assert.deepEqual(await documents(), baseline);
    });
    await client.query("SELECT public.activate_creator_search_incremental_v1('search-baseline',$1,'test','bounded Search writes')", [total]);
    await t.test("incremental publishing writes ten rows and preserves search and details", async () => {
      const unchangedBefore = (await client.query("SELECT channel_id,ctid FROM public.creator_search_live WHERE NOT(channel_id=ANY($1)) ORDER BY channel_id", [changed])).rows;
      const incremental = await publish("search-update", changed);
      assert.equal(incremental.current_inserted, changed.length);
      assert.deepEqual(await documents(), expected);
      assert.deepEqual(await searches(), expectedSearches);
      assert.deepEqual((await client.query("SELECT channel_id,ctid FROM public.creator_search_live WHERE NOT(channel_id=ANY($1)) ORDER BY channel_id", [changed])).rows, unchangedBefore);
      assert.equal(Number((await client.query("SELECT count(*) FROM public.creator_search_current WHERE watermark='search-update'")).rows[0].count), 0);
      assert.equal(Number((await client.query("SELECT count(*) FROM public.creator_search_live s JOIN public.channel_snapshots c ON c.id=s.snapshot_id")).rows[0].count), total);
      t.diagnostic(JSON.stringify({ channels: total, changed: changed.length, shadow, incremental }));
    });
    await t.test("readiness reports see current Business channels after incremental publication", async () => {
      assert.equal((await readiness()).scope.business_active_channel_count, total);
      const filtered = await readiness([ids[0], ids[50]]);
      assert.deepEqual(filtered.scope.target_only_channel_ids, [ids[0], ids[50]]);
    });
    await t.test("later batches and removals retain unchanged channels without new full snapshots", async () => {
      for (let round = 0; round < 3; round += 1) {
        const stats = await publish(`search-followup-${round}`, ids.slice(10, 20));
        assert.equal(stats.current_inserted, 10);
      }
      const removal = await publish("search-remove", [], [ids[20]]);
      assert.equal(removal.current_inserted, 0);
      assert.equal(Number((await client.query("SELECT count(*) FROM public.creator_search_live")).rows[0].count), total - 1);
      assert.equal(Number((await client.query("SELECT count(*) FROM public.creator_search_current")).rows[0].count), total);
    });
    await t.test("rollback restores the full original search results, then incremental can reactivate", async () => {
      await client.query("SELECT public.rollback_creator_search_incremental_storage_v1('search-baseline',$1,'test','verify rollback')", [total]);
      assert.deepEqual(await documents(), baseline);
      assert.deepEqual(await searches(), baselineSearches);
      assert.equal((await readiness()).scope.business_active_channel_count, total);
      assert.deepEqual((await client.query("SELECT write_mode,read_mode FROM publication.creator_search_storage_state")).rows[0], { write_mode: "shadow", read_mode: "legacy" });
      await client.query("SELECT public.activate_creator_search_incremental_v1('search-baseline',$1,'test','reactivate')", [total]);
      assert.equal((await client.query("SELECT write_mode FROM publication.creator_search_storage_state")).rows[0].write_mode, "incremental");
    });
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    await client.end();
  }
});
