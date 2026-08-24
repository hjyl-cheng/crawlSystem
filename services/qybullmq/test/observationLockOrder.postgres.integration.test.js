import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { executeIncrementalVideo } from "../src/incrementalVideo.js";
import { applyMigrationActivityGate } from "../src/migrationActivityGate.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const { Pool } = pg;
const integrationUrl = process.env.INCREMENTAL_POSTGRES_TEST_URL;

function deferred() {
  let resolve;
  const promise = new Promise((done) => { resolve = done; });
  return { promise, resolve };
}

function isVideoCursorLock(sql) {
  return sql.includes("FROM crawler.channel_domain_cursors") && sql.includes("FOR UPDATE");
}

async function waitUntilBlockedBy(pool, blockedPid, blockerPid) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const blockers = (await pool.query(
      "SELECT pg_blocking_pids($1::int)::text[] AS blocker_pids",
      [blockedPid],
    )).rows[0]?.blocker_pids ?? [];
    if (blockers.includes(String(blockerPid))) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`backend ${blockedPid} was not blocked by backend ${blockerPid}`);
}

async function assertChannelWriteLocked(pool, channelId) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await assert.rejects(
      client.query(
        `SELECT channel_id FROM crawler.channels
         WHERE channel_id=$1
         FOR NO KEY UPDATE NOWAIT`,
        [channelId],
      ),
      (error) => error?.code === "55P03",
    );
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
  }
}

test("Migration and Incremental serialize Channel before Video cursor without deadlock", {
  skip: !integrationUrl,
  timeout: 15_000,
}, async () => {
  const pool = new Pool({
    connectionString: integrationUrl,
    max: 8,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const suffix = randomUUID().replaceAll("-", "");
  const batchId = `observation-lock-order:${suffix}`;
  const channelId = `UClockorder${suffix}`;
  const migrationRunId = `migration:lock-order:${suffix}`;
  const incrementalRunId = `incremental:lock-order:${suffix}`;
  const planId = randomUUID();
  const anchorVideoId = `lock-order-anchor-${suffix}`;
  const migrationChannelRequested = deferred();
  const incrementalCursorHeld = deferred();
  const releaseIncrementalCursor = deferred();
  let migrationPid = null;
  let incrementalCursorPid = null;
  let cursorPaused = false;
  let migration = null;
  let incremental = null;

  const runMigration = async () => {
    const client = await pool.connect();
    migrationPid = Number((await client.query("SELECT pg_backend_pid() AS pid")).rows[0].pid);
    const wrapped = {
      async query(sql, params = []) {
        const statement = String(sql);
        if (statement.includes("FOR UPDATE OF run,channel")) {
          migrationChannelRequested.resolve();
        }
        return client.query(sql, params);
      },
    };
    try {
      await client.query("BEGIN");
      await client.query("SET LOCAL deadlock_timeout='50ms'");
      await client.query("SET LOCAL statement_timeout='10s'");
      const result = await applyMigrationActivityGate(wrapped, {
        runId: migrationRunId,
        detailStatus: "done",
        evaluatedAt: "2026-08-24T00:00:00.000Z",
        activityEvidence: {
          complete: true,
          source: "lock_order_integration_test",
          referenceDay: "2026-08-24",
          recentPublishedContentCount: 0,
          uncertainContentCount: 0,
          inspectedContentCount: 0,
          excludedUpcomingCount: 0,
        },
      });
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  };

  const incrementalTransactions = async (action) => {
    const client = await pool.connect();
    const backendPid = Number((await client.query("SELECT pg_backend_pid() AS pid")).rows[0].pid);
    const wrapped = {
      async query(sql, params = []) {
        const statement = String(sql);
        if (isVideoCursorLock(statement)) {
          const result = await client.query(sql, params);
          if (!cursorPaused) {
            cursorPaused = true;
            incrementalCursorPid = backendPid;
            incrementalCursorHeld.resolve();
            await releaseIncrementalCursor.promise;
          }
          return result;
        }
        return client.query(sql, params);
      },
    };
    try {
      await client.query("BEGIN");
      await client.query("SET LOCAL deadlock_timeout='50ms'");
      await client.query("SET LOCAL statement_timeout='10s'");
      const result = await action(wrapped);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  };

  const runIncremental = () => executeIncrementalVideo({
    plan: {
      job_id: `incremental__observation_lock_order__${suffix}`,
      plan_id: planId,
      plan_day: "2026-08-24",
      scheduled_at: "2026-08-24T00:00:00.000Z",
      channel_id: channelId,
      capacity: { factor: 1, player_cap: 0, next_cap: 0, version: "capacity-1" },
      planner_config_version: "video-plan-1",
    },
    runId: incrementalRunId,
    startedAt: "2026-08-24T00:00:00.000Z",
    query: (sql, params) => pool.query(sql, params),
    withTransaction: incrementalTransactions,
    getChannelSnapshot: async () => ({
      scanUploads: async () => ({
        playlist_id: `UU${channelId.slice(2)}`,
        entries: [{ id: anchorVideoId, position: 1, title: "Anchor" }],
        pages: 1,
        item_count: 1,
        parse_gap_count: 0,
        anchor_matched: true,
        matched_anchor_id: anchorVideoId,
        stop_reason: "anchor_matched",
        terminal_reason: "anchor_matched",
        complete: true,
        raw: { engine: "youtubei.js@test" },
      }),
    }),
    fetchDetail: async () => {
      throw new Error("lock-order fixture must not fetch Player detail");
    },
    now: () => new Date("2026-08-24T00:00:00.000Z"),
    crawlerVersion: "qy-v16-integration-test",
  });

  try {
    await pool.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,result_json
       ) VALUES ($1,$1,'discovery_closed','{}'::jsonb)`,
      [batchId],
    );
    const candidate = await pool.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,accepted_at
       ) VALUES ($1,$1,$2,$3,'accepted',now())
       RETURNING candidate_id`,
      [batchId, channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Observation lock-order integration','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,status,crawl_mode,content_limit,detail_status,
         plan_id,plan_day,trigger_reason,task_mask,scheduled_at,
         clock_version,policy_version,planner_config_version,capacity_version,
         crawler_version,started_at
       ) VALUES (
         $1,$2,'running','incremental',0,'pending',$3,'2026-08-24','clock_due',
         '{"video":true}'::jsonb,'2026-08-24T00:00:00Z',7,'v16-rule-1',
         'video-plan-1','capacity-1','test',now()
       )`,
      [incrementalRunId, channelId, planId],
    );
    await pool.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,content_limit,
         detail_status,started_at,result_json
       ) VALUES (
         $1,$2,$3,'waiting_agent','full',30,'done','2026-08-24T00:00:00Z',
         '{"migration_activity_gate":{"required":true,"decision":"pending","max_age_days":90}}'
       )`,
      [migrationRunId, channelId, candidate.rows[0].candidate_id],
    );
    await pool.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,run_id,content_type,content_type_source,
         source_content_id,title,url,published_at,published_at_status,
         published_at_source,published_at_precision,access_status,
         access_status_source,first_seen_at,last_seen_at
       ) VALUES (
         $1,$2,$3,'video','youtube_watch_canonical',$4,'Anchor',$5,
         '2026-01-01T00:00:00Z','exact','youtubejs_player','second','public',
         'youtubejs_player',now(),now()
       )`,
      [
        `${channelId}:video:${anchorVideoId}`,
        channelId,
        incrementalRunId,
        anchorVideoId,
        `https://www.youtube.com/watch?v=${anchorVideoId}`,
      ],
    );
    await pool.query(
      `INSERT INTO crawler.channel_domain_cursors (
         channel_id,observation_kind,anchor_video_ids
       ) VALUES ($1,'video',ARRAY[$2]::text[])`,
      [channelId, anchorVideoId],
    );

    incremental = runIncremental();
    await incrementalCursorHeld.promise;
    await assertChannelWriteLocked(pool, channelId);
    migration = runMigration();
    await migrationChannelRequested.promise;
    await waitUntilBlockedBy(pool, migrationPid, incrementalCursorPid);
    releaseIncrementalCursor.resolve();

    const settled = await Promise.allSettled([migration, incremental]);
    const failures = settled
      .filter((result) => result.status === "rejected")
      .map((result) => result.reason?.code ?? result.reason?.message ?? String(result.reason));
    assert.deepEqual(failures, [], `concurrent writers failed: ${failures.join(", ")}`);
    assert.equal(settled[0].value.decision, "dormant");
    assert.equal(settled[1].value.outcome, "complete");

    assert.deepEqual((await pool.query(
      `SELECT kind_sequence::int,run_id,outcome
       FROM crawler.crawl_observations
       WHERE channel_id=$1 AND observation_kind='video'
       ORDER BY kind_sequence`,
      [channelId],
    )).rows, [
      { kind_sequence: 1, run_id: incrementalRunId, outcome: "complete" },
      { kind_sequence: 2, run_id: migrationRunId, outcome: "complete" },
    ]);
    assert.equal((await pool.query(
      `SELECT latest_sequence FROM crawler.channel_domain_cursors
       WHERE channel_id=$1 AND observation_kind='video'`,
      [channelId],
    )).rows[0].latest_sequence, "2");
  } finally {
    releaseIncrementalCursor.resolve();
    await Promise.allSettled([migration, incremental].filter(Boolean));
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.query(
      "DELETE FROM crawler.query_dispatch_batches WHERE dispatch_batch_id=$1",
      [batchId],
    ).catch(() => {});
    await pool.end();
  }
});
