import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { planLegacySearchPrune, pruneLegacySearchBatch } from "../src/businessCreatorSearchLegacyPruner.js";

const databaseUrl = process.env.CREATOR_SEARCH_PRUNE_POSTGRES_TEST_URL;

test("bounded Legacy cleanup preserves real published data and usable rollback releases", {
  skip: !databaseUrl, timeout: 120_000,
}, async (t) => {
  const url = new URL(databaseUrl);
  assert.match(url.pathname, /_test$/);
  assert.ok(["localhost", "127.0.0.1", "::1"].includes(url.hostname));
  const database = decodeURIComponent(url.pathname.slice(1));
  const client = new pg.Client({ connectionString: databaseUrl });
  const blocker = new pg.Client({ connectionString: databaseUrl });
  await client.connect(); await blocker.connect();
  const ids = ["UC_prune_1", "UC_prune_2", "UC_prune_3"];
  try {
    assert.equal((await client.query("SELECT count(*)::int count FROM public.channels")).rows[0].count, 0,
      "Use a fresh bootstrapped dedicated test database");
    await client.query(`INSERT INTO publication.database_identity
      (singleton,database_kind,database_name) VALUES (true,'business',current_database())
      ON CONFLICT(singleton) DO UPDATE SET database_name=excluded.database_name`);
    await client.query("INSERT INTO public.channels(channel_id) SELECT unnest($1::text[])", [ids]);
    const publish = async (watermark) => {
      await client.query(`INSERT INTO public.import_batches
        (id,source_file,source_sha256,captured_at,raw_payload)
        VALUES ($1,'prune-test',$2,clock_timestamp(),'{}')`,
      [watermark, createHash("sha256").update(watermark).digest("hex")]);
      await client.query(`INSERT INTO public.channel_snapshots
        (id,channel_id,import_batch_id,captured_at,title,handle,country_text,raw_channel,channel_observed_at)
        SELECT $1||'-'||id,id,$1,statement_timestamp(),$1,'@prune','Brazil','{}',statement_timestamp()
        FROM unnest($2::text[]) id`, [watermark, ids]);
      await client.query("SELECT public.refresh_creator_search_release_v9($1,$2,'{}')", [watermark, ids]);
    };
    for (let n = 1; n <= 6; n += 1) await publish(`prune-${n}`);
    await client.query("SELECT public.activate_creator_search_incremental_v1('prune-6',3,'test','prune verification')");
    const plan = await planLegacySearchPrune(client, database);
    assert.equal(plan.delete_rows, 9);
    assert.equal(plan.initialized_row_count, 0);
    assert.deepEqual(plan.candidates.map((r) => r.watermark), ["prune-1", "prune-2", "prune-3"]);
    const options = { plan, database, watermark: "prune-1", expectedRemaining: 3, batchSize: 2,
      actor: "prune-test", reason: "isolated bounded prune verification" };
    const fingerprints = async () => (await client.query(`SELECT
      (SELECT jsonb_agg(to_jsonb(c) ORDER BY channel_id) FROM public.channels c) channels,
      (SELECT jsonb_agg(to_jsonb(s) ORDER BY id) FROM public.channel_snapshots s) snapshots,
      (SELECT count(*) FROM public.content_snapshots) contents,
      (SELECT jsonb_agg(to_jsonb(l) ORDER BY channel_id) FROM public.creator_search_live l) live,
      (SELECT jsonb_agg(to_jsonb(r) ORDER BY watermark) FROM public.creator_search_releases r) releases,
      (SELECT jsonb_agg(to_jsonb(c) ORDER BY watermark,channel_id) FROM publication.creator_search_changes c) changes,
      (SELECT jsonb_agg(to_jsonb(c) ORDER BY watermark,channel_id) FROM public.creator_search_current c
       WHERE watermark IN ('prune-4','prune-5','prune-6')) retained`)).rows[0];
    const before = await fingerprints();
    await t.test("rejects protected targets, stale counts, and unsafe storage mode", async () => {
      await assert.rejects(pruneLegacySearchBatch(client, { ...options, watermark: "prune-6" }), /fixed prune plan/);
      await assert.rejects(pruneLegacySearchBatch(client, { ...options, expectedRemaining: 2 }), /count changed/);
      await client.query("UPDATE publication.creator_search_storage_state SET write_mode='shadow',read_mode='legacy'");
      await assert.rejects(pruneLegacySearchBatch(client, options), /incremental writes/);
      await client.query("UPDATE publication.creator_search_storage_state SET write_mode='incremental',read_mode='live'");
      assert.deepEqual(await fingerprints(), before);
    });
    await t.test("yields immediately to a publisher holding the shared lock", async () => {
      await blocker.query("BEGIN");
      await blocker.query("SELECT pg_advisory_xact_lock(hashtext('kol_demo:creator-search-publish'))");
      const started = Date.now();
      assert.deepEqual(await pruneLegacySearchBatch(client, options), { outcome: "busy", deleted: 0 });
      assert.ok(Date.now() - started < 1000);
      await blocker.query("ROLLBACK");
    });
    await t.test("commits only a bounded batch and records its audit atomically", async () => {
      assert.deepEqual(await pruneLegacySearchBatch(client, options), { outcome: "pruned", deleted: 2, remaining: 1 });
      await assert.rejects(pruneLegacySearchBatch(client, options), /count changed/);
      assert.equal((await client.query("SELECT sum(deleted_row_count)::int n FROM publication.creator_search_legacy_prune_audit")).rows[0].n, 2);
      assert.equal((await pruneLegacySearchBatch(client, { ...options, expectedRemaining: 1 })).deleted, 1);
      for (const watermark of ["prune-2", "prune-3"]) {
        assert.equal((await pruneLegacySearchBatch(client, { ...options, watermark, batchSize: 10 })).deleted, 3);
      }
      assert.deepEqual(await fingerprints(), before);
      assert.equal((await planLegacySearchPrune(client, database)).delete_rows, 0);
    });
    await t.test("new incremental publication and real rollback work after cleanup", async () => {
      await publish("prune-incremental");
      assert.equal((await client.query("SELECT count(*)::int n FROM public.creator_search_current WHERE watermark='prune-incremental'")).rows[0].n, 0);
      assert.equal((await planLegacySearchPrune(client, database)).delete_rows, 0);
      await client.query("SELECT public.rollback_creator_search_incremental_storage_v1('prune-4',3,'test','verify retained older rollback')");
      assert.equal((await client.query("SELECT watermark FROM public.creator_search_active")).rows[0].watermark, "prune-4");
      const equality = (await client.query(`SELECT count(*)::int n FROM public.creator_search_live l
        FULL JOIN (SELECT * FROM public.creator_search_current WHERE watermark='prune-4') c USING(channel_id)
        WHERE (to_jsonb(l)-'watermark') IS DISTINCT FROM (to_jsonb(c)-'watermark')`)).rows[0].n;
      assert.equal(equality, 0);
    });
  } finally {
    await blocker.query("ROLLBACK").catch(() => {});
    await blocker.end(); await client.end();
  }
});
