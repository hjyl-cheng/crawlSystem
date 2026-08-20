import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import test from "node:test";
import pg from "pg";

const { Client } = pg;
const execute = promisify(execFile);
const integrationUrl = process.env.BASELINE_POSTGRES_TEST_URL;

test("controlled Baseline export freezes real Outbox sequences and is resumable", {
  skip: !integrationUrl,
  timeout: 120_000,
}, async () => {
  const client = new Client({ connectionString: integrationUrl, options: "-c timezone=UTC" });
  const temporary = await mkdtemp(join(tmpdir(), "qy-v16-baseline-integration-"));
  const outputDirectory = join(temporary, "bundle");
  const baselineVersion = "integration-baseline-v1";
  const featureIntegrationUrl = process.env.BASELINE_FEATURE_POSTGRES_TEST_URL;
  const featureEngineRoot = process.env.FEATURE_ENGINE_ROOT;
  let database;
  await client.connect();
  try {
    const safety = await client.query(
      `SELECT current_database() AS database_name,
              current_setting('TimeZone')='UTC' AS timezone_utc,
              (SELECT count(*) FROM crawler.channels)::int AS channel_count,
              (SELECT count(*) FROM crawler.baseline_exports)::int AS export_count`,
    );
    database = String(safety.rows[0].database_name);
    if (!safety.rows[0].timezone_utc
        || Number(safety.rows[0].channel_count) !== 0
        || Number(safety.rows[0].export_count) !== 0) {
      throw new Error("BASELINE_POSTGRES_TEST_URL must point to a clean isolated UTC Crawler DB");
    }
    await client.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,status,keywords,available_tabs,
         subscriber_count,subscriber_count_status,total_view_count,total_view_count_status,
         total_video_count,total_video_count_status
       ) VALUES
       ('UC-baseline-a','https://youtube.test/a','Channel A','active',ARRAY['code'],ARRAY['videos'],
        100,'exact',1000,'exact',10,'exact'),
       ('UC-baseline-b','https://youtube.test/b','Channel B','active',ARRAY[]::text[],ARRAY[]::text[],
        200,'unresolved',NULL,'unresolved',NULL,'unavailable'),
       ('UC-baseline-rejected','https://youtube.test/rejected','Rejected Channel','rejected',
        ARRAY[]::text[],ARRAY[]::text[],NULL,'unresolved',NULL,'unresolved',NULL,'unresolved')`,
    );
    await client.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,content_type,source_content_id,title,published_at,
         published_at_status,published_at_precision,is_recent,first_seen_at,last_seen_at
       ) VALUES (
         'video:a:1','UC-baseline-a','video','video-a-1','Video A',
         '2026-07-20T00:00:00Z','exact','second',true,
         '2026-07-20T00:00:00Z','2026-07-20T00:00:00Z'
       ), (
         'video:a:old','UC-baseline-a','video','video-a-old','Old Video A',
         '2026-05-20T00:00:00Z','exact','second',true,
         '2026-05-20T00:00:00Z','2026-05-20T00:00:00Z'
       )`,
    );
    await client.query(
      `INSERT INTO crawler.agent_profiles (
         channel_id,agent_mode,input_url,status,metrics_json,agent_model
       ) VALUES (
         'UC-baseline-a','basic','https://youtube.test/a','success',
         '{"audience_profile_agent":{"channel_categories":{"value":{"level_1":"Tech","level_2":["AI"]},"evidence":["category evidence"]},"channel_tags":{"value":{"tags":["software"]},"evidence":["tag evidence"]},"active_subscriber_ratio":{"value":25,"evidence":["ratio evidence"]}}}'::jsonb,
         'integration-agent'
       )`,
    );
    const asOfAt = (await client.query("SELECT clock_timestamp() AS value")).rows[0].value.toISOString();
    const environment = {
      ...process.env,
      DATABASE_URL: integrationUrl,
      EXPECTED_CRAWLER_DATABASE: database,
      EXPECTED_CRAWLER_CHANNEL_COUNT: "2",
      BASELINE_VERSION: baselineVersion,
      BASELINE_AS_OF_UTC: asOfAt,
      BASELINE_OUTPUT_DIRECTORY: outputDirectory,
      CONFIRM_V16_BASELINE_EXPORT: `${database}/${baselineVersion}/2`,
    };
    await client.query("SELECT pg_advisory_lock(781137219)");
    try {
      await assert.rejects(
        execute(
          process.execPath,
          ["scripts/exportV16BaselineBundle.mjs", "--apply"],
          { cwd: process.cwd(), env: environment },
        ),
        /another V16 Baseline export is already running/,
      );
    } finally {
      await client.query("SELECT pg_advisory_unlock(781137219)");
    }
    const first = await execute(
      process.execPath,
      ["scripts/exportV16BaselineBundle.mjs", "--apply"],
      { cwd: process.cwd(), env: environment },
    );
    const firstResult = JSON.parse(first.stdout.trim());
    assert.equal(firstResult.channel_count, 2);
    assert.equal(firstResult.event_count, 5);
    assert.deepEqual(firstResult.created_observations, {
      about: 2,
      video: 2,
      agent: 1,
    });
    const events = (await readFile(join(outputDirectory, "events.ndjson"), "utf8"))
      .trim().split("\n").map(JSON.parse);
    assert.equal(events.some((event) => event.observation_kind === "profile"), false);
    assert.equal(events.some((event) => (
      event.channel_id === "UC-baseline-b"
      && event.observation_kind === "about"
      && event.payload.subscriber_count === null
    )), true);
    const channelAVideo = events.find((event) => (
      event.channel_id === "UC-baseline-a" && event.observation_kind === "video"
    ));
    assert.equal(channelAVideo.payload.discovery.payload.first_seen_count, 2);
    assert.equal(channelAVideo.payload.recent_sampling.payload.recent_count, 1);
    const audit = await client.query(
      `SELECT
         (SELECT count(*) FROM crawler.channel_about_metric_snapshots)::int AS snapshots,
         (SELECT count(*) FROM crawler.crawl_observations)::int AS observations,
         (SELECT count(*) FROM crawler.baseline_export_events)::int AS exported_events,
         (SELECT count(*) FROM crawler.channel_domain_cursors
          WHERE channel_id='UC-baseline-rejected')::int AS rejected_cursors,
         (SELECT status FROM crawler.baseline_exports WHERE baseline_version=$1) AS status`,
      [baselineVersion],
    );
    assert.deepEqual(audit.rows[0], {
      snapshots: 1,
      observations: 5,
      exported_events: 5,
      rejected_cursors: 0,
      status: "ready",
    });

    if (featureIntegrationUrl) {
      if (!featureEngineRoot) {
        throw new Error("FEATURE_ENGINE_ROOT is required with BASELINE_FEATURE_POSTGRES_TEST_URL");
      }
      const featureDatabase = String(process.env.EXPECTED_FEATURE_DATABASE ?? "").trim();
      if (!featureDatabase) {
        throw new Error("EXPECTED_FEATURE_DATABASE is required for cross-language validation");
      }
      const pythonPath = [
        join(featureEngineRoot, "feature_engine/src"),
        join(featureEngineRoot, "feature_engine/.deps"),
        process.env.PYTHONPATH,
      ].filter(Boolean).join(":");
      const crossLanguage = await execute(
        process.env.PYTHON_BIN || "python3",
        [join(featureEngineRoot, "feature_engine/scripts/validate_qy_baseline_bootstrap.py")],
        {
          cwd: featureEngineRoot,
          env: {
            ...process.env,
            PYTHONPATH: pythonPath,
            FEATURE_DATABASE_URL: featureIntegrationUrl,
            EXPECTED_FEATURE_DATABASE: featureDatabase,
            QY_BASELINE_MANIFEST_PATH: join(outputDirectory, "manifest.json"),
            EXPECTED_BASELINE_SOURCE_DATABASE: database,
            EXPECTED_BASELINE_CHANNEL_COUNT: "2",
          },
        },
      );
      const crossLanguageResult = JSON.parse(crossLanguage.stdout.trim());
      assert.equal(crossLanguageResult.feature_bootstrap_status, "succeeded");
      assert.equal(crossLanguageResult.event_count, 5);
      assert.equal(crossLanguageResult.channel_count, 2);
      assert.equal(crossLanguageResult.checkpoint_count, 5);
    }

    await client.query("DELETE FROM crawler.crawler_outbox");
    assert.equal(
      Number((await client.query(
        "SELECT count(*) AS count FROM crawler.baseline_export_events",
      )).rows[0].count),
      0,
    );

    const repeated = await execute(
      process.execPath,
      ["scripts/exportV16BaselineBundle.mjs", "--apply"],
      { cwd: process.cwd(), env: environment },
    );
    const repeatedResult = JSON.parse(repeated.stdout.trim());
    assert.equal(repeatedResult.resumed_from, "ready");
    assert.equal(repeatedResult.reused_output, true);
    assert.equal(
      Number((await client.query("SELECT count(*) AS count FROM crawler.crawl_observations")).rows[0].count),
      5,
    );
  } finally {
    await client.query(
      "DELETE FROM crawler.baseline_exports WHERE baseline_version=$1",
      [baselineVersion],
    ).catch(() => {});
    await client.query(
      "DELETE FROM crawler.channels WHERE channel_id=ANY($1::text[])",
      [["UC-baseline-a", "UC-baseline-b", "UC-baseline-rejected"]],
    ).catch(() => {});
    await client.end().catch(() => {});
    await rm(temporary, { recursive: true, force: true });
  }
});
