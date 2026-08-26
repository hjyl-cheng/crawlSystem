import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { BusinessCreatorSearchStorageAdministrator } from "../src/businessCreatorSearchStorageAdmin.js";

const { Pool } = pg;
const integrationUrl = process.env.CREATOR_SEARCH_STORAGE_POSTGRES_TEST_URL;
const SEARCH_PUBLISH_LOCK = "kol_demo:creator-search-publish";

async function waitUntilBlockedBy(pool, applicationName, blockerPid) {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    const result = await pool.query(
      `SELECT pg_blocking_pids(pid)::text[] AS blocker_pids
       FROM pg_stat_activity
       WHERE datname=current_database() AND application_name=$1`,
      [applicationName],
    );
    if (result.rows.some((row) => (
      (row.blocker_pids ?? []).includes(String(blockerPid))
    ))) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`Creator Search storage administrator was not blocked by ${blockerPid}`);
}

test("Creator Search rollback takes a fresh snapshot after waiting for the publish lock", {
  skip: !integrationUrl,
  timeout: 15_000,
}, async () => {
  const suffix = randomUUID().replaceAll("-", "");
  const waiterApplicationName = `creator-search-storage-waiter-${suffix}`;
  const setupPool = new Pool({ connectionString: integrationUrl, max: 2 });
  const observerPool = new Pool({ connectionString: integrationUrl, max: 2 });
  const waiterPool = new Pool({
    connectionString: integrationUrl,
    application_name: waiterApplicationName,
    max: 1,
  });
  const blocker = await setupPool.connect();
  let blockerTransaction = false;
  let blockerLockHeld = false;
  let rollbackPromise = null;
  try {
    const databaseName = (await setupPool.query(
      "SELECT current_database() AS database_name",
    )).rows[0].database_name;
    assert.match(databaseName, /_test$/i, "integration URL must target a *_test database");
    await setupPool.query(`
      DROP SCHEMA IF EXISTS publication CASCADE;
      DROP SCHEMA IF EXISTS public CASCADE;
      CREATE SCHEMA public;
      CREATE SCHEMA publication;

      CREATE TABLE public.channels (channel_id text PRIMARY KEY);
      CREATE TABLE public.creator_search_active (
        singleton boolean PRIMARY KEY,
        watermark text NOT NULL
      );
      CREATE TABLE public.creator_search_releases (
        watermark text PRIMARY KEY,
        previous_watermark text,
        changed_channel_count integer
      );
      CREATE TABLE public.creator_search_current (
        watermark text NOT NULL,
        channel_id text NOT NULL,
        payload text,
        PRIMARY KEY (watermark,channel_id)
      );
      CREATE TABLE public.creator_search_live (
        channel_id text PRIMARY KEY,
        watermark text NOT NULL,
        payload text
      );
      CREATE TABLE publication.database_identity (
        singleton boolean PRIMARY KEY,
        database_kind text NOT NULL,
        database_name text NOT NULL
      );
      CREATE TABLE publication.creator_search_storage_state (
        singleton boolean PRIMARY KEY,
        write_mode text NOT NULL,
        read_mode text NOT NULL
      );
      CREATE TABLE publication.creator_search_changes (
        watermark text NOT NULL,
        channel_id text NOT NULL,
        before_document jsonb,
        PRIMARY KEY (watermark,channel_id)
      );
      CREATE TABLE publication.projection_outbox (status text NOT NULL);
      CREATE TABLE publication.channel_ownership (
        status text NOT NULL,
        projection_mode text NOT NULL
      );

      INSERT INTO publication.database_identity
        (singleton,database_kind,database_name)
      VALUES (true,'business',current_database());
      INSERT INTO publication.creator_search_storage_state
        (singleton,write_mode,read_mode)
      VALUES (true,'incremental','live');
      INSERT INTO public.creator_search_releases
        (watermark,previous_watermark,changed_channel_count)
      VALUES ('target',NULL,0),('active','target',1);
      INSERT INTO public.creator_search_active (singleton,watermark)
      VALUES (true,'active');
      INSERT INTO publication.creator_search_changes
        (watermark,channel_id,before_document)
      VALUES ('active','channel-a',NULL);

      CREATE FUNCTION public.activate_creator_search_incremental_v1(
        text,integer,text,text
      ) RETURNS text LANGUAGE sql AS
      $$ SELECT 'incremental'::text $$;
      CREATE FUNCTION public.rollback_creator_search_incremental_storage_v1(
        text,integer,text,text
      ) RETURNS integer LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION 'rollback function must not be called after target drift';
      END
      $$;
    `);

    const administrator = new BusinessCreatorSearchStorageAdministrator({
      pool: waiterPool,
      config: {
        databaseUrl: integrationUrl,
        expectedDatabase: databaseName,
        expectedBusinessChannelCount: 0,
        actor: "storage-integration-test",
        reason: "verify post-lock snapshot",
        rollbackWatermark: null,
      },
    });

    await blocker.query("SELECT pg_advisory_lock(hashtext($1))", [SEARCH_PUBLISH_LOCK]);
    blockerLockHeld = true;
    const blockerPid = Number((await blocker.query(
      "SELECT pg_backend_pid() AS pid",
    )).rows[0].pid);
    await blocker.query("BEGIN");
    blockerTransaction = true;
    await blocker.query(
      `UPDATE publication.creator_search_changes
       SET before_document=$1::jsonb
       WHERE watermark='active' AND channel_id='channel-a'`,
      [JSON.stringify({ channel_id: "channel-a", payload: "restored" })],
    );

    rollbackPromise = administrator.rollback({
      expectedActiveWatermark: "active",
      expectedCurrentLiveCount: 0,
      targetWatermark: "target",
      expectedTarget: {
        rollback_target_exists: true,
        rollback_target_reachable: true,
        rollback_target_count: 0,
        rollback_target_expected_count: 0,
        rollback_target_parity_diffs: 0,
        rollback_chain_errors: 0,
      },
    });
    await waitUntilBlockedBy(observerPool, waiterApplicationName, blockerPid);

    await blocker.query("COMMIT");
    blockerTransaction = false;
    await blocker.query(
      "SELECT pg_advisory_unlock(hashtext($1)) AS unlocked",
      [SEARCH_PUBLISH_LOCK],
    );
    blockerLockHeld = false;

    await assert.rejects(
      rollbackPromise,
      /rollback target changed after the approved plan: rollback_target_expected_count/,
    );
    rollbackPromise = null;
    const storage = (await setupPool.query(
      `SELECT state.write_mode,state.read_mode,active.watermark
       FROM publication.creator_search_storage_state state
       CROSS JOIN public.creator_search_active active
       WHERE state.singleton=true AND active.singleton=true`,
    )).rows[0];
    assert.deepEqual(storage, {
      write_mode: "incremental",
      read_mode: "live",
      watermark: "active",
    });
  } finally {
    if (blockerTransaction) await blocker.query("ROLLBACK").catch(() => {});
    if (blockerLockHeld) {
      await blocker.query(
        "SELECT pg_advisory_unlock(hashtext($1))",
        [SEARCH_PUBLISH_LOCK],
      ).catch(() => {});
    }
    if (rollbackPromise) await rollbackPromise.catch(() => {});
    blocker.release();
    await waiterPool.end();
    await observerPool.end();
    await setupPool.end();
  }
});
