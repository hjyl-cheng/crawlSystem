import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import test from "node:test";
import {
  migrationCandidateStatusSql,
  migrationDisplayStatusSql,
  migrationDoneSql,
  migrationIncompleteSql,
  migrationLifecycleState,
} from "./migrationCompletion.js";
import { loadMigrationChannelInventory } from "./migrationInventory.js";
import { readMigrationInventory } from "./migrationInventoryRead.js";

const databaseUrl = String(process.env.MIGRATION_INVENTORY_POSTGRES_TEST_URL || "").trim();

function assertDedicatedTestDatabase(value) {
  const url = new URL(value);
  const databaseName = decodeURIComponent(url.pathname.replace(/^\//, ""));
  assert.match(databaseName, /test/i, "integration database name must contain test");
  assert.ok(
    ["127.0.0.1", "localhost", "::1"].includes(url.hostname),
    "integration database must be local",
  );
}

function planNodes(plan) {
  const nodes = [];
  const visit = (node) => {
    if (!node || typeof node !== "object") return;
    nodes.push(node);
    for (const child of node.Plans || []) visit(child);
  };
  visit(plan);
  return nodes;
}

function planContains(plan, predicate) {
  return planNodes(plan).some(predicate);
}

async function assertLifecycleSqlMatchesJavaScript(pool) {
  const fixtures = [
    {
      name: "unstarted",
      candidate_id: null,
      candidate_status: null,
      channel_status: null,
      promotion_candidate_id: null,
      finalized_status: null,
    },
    {
      name: "finishing",
      candidate_id: "12",
      candidate_status: "accepted",
      channel_status: "active",
      promotion_candidate_id: "12",
      finalized_status: "pending",
    },
    {
      name: "done",
      candidate_id: "13",
      candidate_status: "accepted",
      channel_status: "dormant",
      promotion_candidate_id: "13",
      finalized_status: "ready_partial",
    },
    {
      name: "not-promoted",
      candidate_id: "14",
      candidate_status: "accepted",
      channel_status: "active",
      promotion_candidate_id: "99",
      finalized_status: "pending",
    },
  ];
  const result = await pool.query(
    `WITH fixture AS (
       SELECT *
       FROM jsonb_to_recordset($1::jsonb) AS item(
         name text,candidate_id text,candidate_status text,channel_status text,
         promotion_candidate_id text,finalized_status text
       )
     )
     SELECT fixture.name,
            ${migrationCandidateStatusSql("candidate", "channel", "finalized")}
              AS candidate_status,
            ${migrationDisplayStatusSql("candidate", "channel", "finalized")}
              AS status,
            ${migrationIncompleteSql("candidate", "channel", "finalized")}
              AS migration_incomplete,
            ${migrationDoneSql("candidate", "channel", "finalized")}
              AS migration_done
     FROM fixture
     LEFT JOIN LATERAL (
       SELECT fixture.candidate_id,fixture.candidate_status AS status
     ) candidate ON true
     LEFT JOIN LATERAL (
       SELECT fixture.channel_status AS status,
              fixture.promotion_candidate_id AS registry_promotion_candidate_id
     ) channel ON true
     LEFT JOIN LATERAL (
       SELECT fixture.finalized_status AS status
     ) finalized ON true
     ORDER BY fixture.name`,
    [JSON.stringify(fixtures)],
  );

  const actualByName = new Map(result.rows.map((row) => [row.name, row]));
  for (const fixture of fixtures) {
    const expected = migrationLifecycleState({
      candidateId: fixture.candidate_id,
      candidateStatus: fixture.candidate_status,
      channelStatus: fixture.channel_status,
      promotionCandidateId: fixture.promotion_candidate_id,
      finalizedStatus: fixture.finalized_status,
    });
    const actual = actualByName.get(fixture.name);
    assert.deepEqual({
      candidateStatus: actual.candidate_status,
      status: actual.status,
      migrationIncomplete: actual.migration_incomplete,
      migrationDone: actual.migration_done,
    }, expected, fixture.name);
  }
}

test("PostgreSQL pages 410k Migration inventory rows in Target only", {
  skip: databaseUrl ? false : "MIGRATION_INVENTORY_POSTGRES_TEST_URL is not configured",
  timeout: 120_000,
}, async (t) => {
  assertDedicatedTestDatabase(databaseUrl);
  const pg = await import("pg");
  const Pool = pg.default?.Pool || pg.Pool;
  const pool = new Pool({ connectionString: databaseUrl, max: 4 });
  t.after(async () => {
    await pool.query("DROP SCHEMA IF EXISTS crawler CASCADE");
    await pool.end();
  });

  await pool.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  await pool.query(`CREATE SCHEMA crawler;
    CREATE TABLE crawler.migration_channel_inventory_syncs (
      source_id text PRIMARY KEY,
      source_database text NOT NULL,
      source_database_oid oid NOT NULL,
      status text NOT NULL,
      eligible_count bigint NOT NULL,
      completed_at timestamptz,
      last_error text
    );
    CREATE TABLE crawler.migration_channel_inventory (
      source_id text NOT NULL REFERENCES crawler.migration_channel_inventory_syncs(source_id),
      source_candidate_id bigint NOT NULL,
      channel_id text NOT NULL,
      channel_url text NOT NULL,
      handle text,
      title text,
      avatar_url text,
      search_subscriber_count bigint,
      priority integer NOT NULL,
      source_candidate_status text NOT NULL,
      source_updated_at timestamptz,
      synced_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (source_id,channel_id),
      UNIQUE (source_id,source_candidate_id)
    );
    CREATE INDEX idx_crawler_migration_inventory_page
      ON crawler.migration_channel_inventory (
        source_id,priority DESC,source_candidate_id ASC
      );
    CREATE TABLE crawler.migration_channel_intents (
      migration_intent_id bigint PRIMARY KEY,
      source_id text NOT NULL,
      source_candidate_id bigint NOT NULL,
      channel_id text NOT NULL,
      target_candidate_id bigint,
      updated_at timestamptz NOT NULL
    );
    CREATE TABLE crawler.channel_candidates (
      candidate_id bigint PRIMARY KEY,
      status text NOT NULL,
      updated_at timestamptz NOT NULL
    );
    CREATE TABLE crawler.migration_system_retry_items (
      system_retry_id bigint PRIMARY KEY,
      candidate_id bigint NOT NULL,
      status text NOT NULL
    );
    CREATE TABLE crawler.channels (
      channel_id text PRIMARY KEY,
      status text NOT NULL,
      registry_promotion_candidate_id bigint,
      reject_reason text,
      agent_status text,
      latest_run_id text,
      updated_at timestamptz NOT NULL
    );
    CREATE TABLE crawler.channel_runs (
      run_id text PRIMARY KEY,
      status text,
      detail_status text
    );
    CREATE TABLE crawler.finalized_profiles (
      channel_id text PRIMARY KEY,
      status text,
      quality_json jsonb NOT NULL DEFAULT '{}'::jsonb
    );
    CREATE TABLE crawler.contents (
      content_key text PRIMARY KEY,
      channel_id text NOT NULL,
      run_id text,
      content_type text NOT NULL
    );
    CREATE INDEX idx_crawler_contents_current_run
      ON crawler.contents (channel_id,run_id);`);

  const sourceId = "integration-source-v1";
  await pool.query(
    `INSERT INTO crawler.migration_channel_inventory_syncs (
       source_id,source_database,source_database_oid,status,eligible_count,completed_at
     ) VALUES ($1,'migration_source_test',16384,'ready',410292,now())`,
    [sourceId],
  );
  await pool.query(
    `INSERT INTO crawler.migration_channel_inventory (
       source_id,source_candidate_id,channel_id,channel_url,handle,title,
       search_subscriber_count,priority,source_candidate_status,source_updated_at
     )
     SELECT $1,series.value,
            'UC'||lpad(series.value::text,22,'0'),
            'https://www.youtube.com/channel/UC'||lpad(series.value::text,22,'0'),
            '@channel'||series.value::text,
            'Channel '||series.value::text,
            CASE WHEN series.value % 2=0 THEN series.value * 10 ELSE NULL END,
            100,'discovered',now()
     FROM generate_series(1,410292) AS series(value)`,
    [sourceId],
  );
  await pool.query(
    `UPDATE crawler.migration_channel_inventory inventory
     SET title='Sparse inventory match '||inventory.source_candidate_id::text
     FROM generate_series(1,50) AS sparse(value)
     WHERE inventory.source_id=$1
       AND inventory.source_candidate_id=1000+(sparse.value*1000)`,
    [sourceId],
  );
  await pool.query(
    `INSERT INTO crawler.channel_candidates (candidate_id,status,updated_at)
     SELECT 1000000+series.value,
            CASE
              WHEN series.value<=292 OR series.value>=296 THEN 'accepted'
              WHEN series.value=293 THEN 'queued'
              WHEN series.value=294 THEN 'validating'
              ELSE 'failed'
            END,
            now()
     FROM generate_series(1,300) AS series(value)`,
  );
  await pool.query(
    `INSERT INTO crawler.migration_channel_intents (
       migration_intent_id,source_id,source_candidate_id,channel_id,
       target_candidate_id,updated_at
     )
     SELECT series.value,$1,series.value,
            'UC'||lpad(series.value::text,22,'0'),1000000+series.value,now()
     FROM generate_series(1,300) AS series(value)`,
    [sourceId],
  );
  await pool.query(
    `INSERT INTO crawler.channels (
       channel_id,status,registry_promotion_candidate_id,agent_status,latest_run_id,updated_at
     )
     SELECT 'UC'||lpad(series.value::text,22,'0'),'active',1000000+series.value,
            CASE WHEN series.value<=292 THEN 'done' ELSE 'pending' END,
            CASE WHEN series.value=296 THEN 'run-296' ELSE NULL END,
            now()
     FROM generate_series(1,300) AS series(value)
     WHERE series.value<=292 OR series.value>=296`,
  );
  await pool.query(
    `INSERT INTO crawler.finalized_profiles (channel_id,status)
     SELECT 'UC'||lpad(series.value::text,22,'0'),'ready_auto'
     FROM generate_series(1,292) AS series(value)`,
  );
  await pool.query(
    `INSERT INTO crawler.channel_runs (run_id,status,detail_status)
     VALUES ('run-296','running','done')`,
  );
  await pool.query(
    `INSERT INTO crawler.contents (content_key,channel_id,run_id,content_type)
     VALUES
       ('296-video','UC0000000000000000000296','run-296','video'),
       ('296-short','UC0000000000000000000296','run-296','short'),
       ('296-live','UC0000000000000000000296','run-296','live')`,
  );
  await pool.query("ANALYZE crawler.migration_channel_inventory");
  await assertLifecycleSqlMatchesJavaScript(pool);

  const calls = [];
  const startedAt = performance.now();
  const result = await loadMigrationChannelInventory({
    read: async (sql, params) => {
      calls.push({ sql, params });
      return readMigrationInventory(pool, sql, params);
    },
    sourceId,
    expectedSourceDatabase: "migration_source_test",
    expectedSourceDatabaseOid: "16384",
    filters: {
      search: "",
      channelStatus: "all",
      agentStatus: "",
      finalStatus: "",
      limit: 50,
      offset: 0,
    },
  });
  const elapsedMs = performance.now() - startedAt;
  t.diagnostic(`Target-only 410292-row first page: ${elapsedMs.toFixed(1)}ms`);

  assert.equal(result.channels.length, 50);
  assert.equal(result.channels[0].candidate_id, "293");
  assert.equal(result.total, 410000);
  assert.equal(result.stats.migration_done, 292);
  assert.equal(result.stats.finishing, 5);
  assert.ok(elapsedMs < 5000, `Target-only first page took ${elapsedMs.toFixed(1)}ms`);
  const pageCall = calls.find(({ sql }) => sql.includes("filtered_page AS"));
  const planResult = await readMigrationInventory(pool,
    `EXPLAIN (ANALYZE, FORMAT JSON) ${pageCall.sql}`,
    pageCall.params,
  );
  const plan = planResult.rows[0]["QUERY PLAN"][0].Plan;
  assert.ok(
    planContains(plan, (node) => (
      node["Index Name"] === "idx_crawler_migration_inventory_page"
    )),
    "inventory page index must be used",
  );
  const pageContentJoin = planNodes(plan).find((node) => (
    node["Node Type"] === "Nested Loop"
    && node["Join Type"] === "Left"
    && node.Plans?.length >= 2
    && planContains(node.Plans[0], (child) => child["Node Type"] === "Limit")
    && planContains(node.Plans[1], (child) => child["Relation Name"] === "contents")
  ));
  assert.ok(pageContentJoin, "page Limit must be the outer side of the contents join");
  assert.equal(
    planContains(pageContentJoin.Plans[0], (node) => node["Relation Name"] === "contents"),
    false,
    "contents must not be aggregated before pagination",
  );
  const pageLimit = planNodes(pageContentJoin.Plans[0])
    .find((node) => node["Node Type"] === "Limit");
  assert.equal(pageLimit["Actual Rows"], 50);
  const contentAggregate = planNodes(pageContentJoin.Plans[1]).find((node) => (
    node["Node Type"] === "Aggregate"
    && planContains(node, (child) => child["Relation Name"] === "contents")
  ));
  assert.ok(contentAggregate, "contents aggregate must be on the lateral inner side");
  assert.equal(contentAggregate["Actual Loops"], 50);

  const secondPage = await loadMigrationChannelInventory({
    read: (sql, params) => readMigrationInventory(pool, sql, params),
    sourceId,
    expectedSourceDatabase: "migration_source_test",
    expectedSourceDatabaseOid: "16384",
    filters: { channelStatus: "all", limit: 50, offset: 50 },
  });
  const consecutiveIds = [...result.channels, ...secondPage.channels]
    .map((channel) => channel.candidate_id);
  assert.equal(new Set(consecutiveIds).size, 100, "consecutive pages must not overlap");
  assert.deepEqual(
    consecutiveIds,
    Array.from({ length: 100 }, (_, index) => String(293 + index)),
    "equal-priority pages must have no omissions",
  );

  const sparse = await loadMigrationChannelInventory({
    read: (sql, params) => readMigrationInventory(pool, sql, params),
    sourceId,
    expectedSourceDatabase: "migration_source_test",
    expectedSourceDatabaseOid: "16384",
    filters: {
      search: "Sparse inventory match",
      channelStatus: "all",
      limit: 50,
      offset: 0,
    },
  });
  assert.equal(sparse.total, 50);
  assert.equal(sparse.channels.length, 50);
  assert.deepEqual(
    sparse.channels.map((channel) => channel.candidate_id),
    Array.from({ length: 50 }, (_, index) => String(2000 + (index * 1000))),
  );

  const finishing = await loadMigrationChannelInventory({
    read: (sql, params) => readMigrationInventory(pool, sql, params),
    sourceId,
    expectedSourceDatabase: "migration_source_test",
    expectedSourceDatabaseOid: "16384",
    filters: {
      search: "UC0000000000000000000296",
      channelStatus: "finishing",
      limit: 50,
      offset: 0,
    },
  });
  assert.equal(finishing.total, 1);
  assert.equal(finishing.channels[0].content_count, "3");
  assert.equal(finishing.channels[0].video_count, "1");
  assert.equal(finishing.channels[0].short_count, "1");
  assert.equal(finishing.channels[0].live_count, "1");
});
