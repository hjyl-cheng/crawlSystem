import assert from "node:assert/strict";
import test from "node:test";
import pg from "pg";

import { dataApiCircuitState } from "../src/dataApiCircuit.js";

const { Client } = pg;
const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();

function assertDedicatedLocalTestDatabase(value) {
  const url = new URL(value);
  assert.match(decodeURIComponent(url.pathname), /test/i);
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
}

test("Data API circuit ignores malformed and non-detail historical request evidence", {
  skip: databaseUrl ? false : "MANAGED_JOB_TEST_DATABASE_URL is not configured",
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const client = new Client({ connectionString: databaseUrl });
  await client.connect();
  t.after(async () => {
    await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
    await client.end();
  });
  await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  await client.query("CREATE SCHEMA crawler");
  await client.query(
    `CREATE TABLE crawler.task_events (
       queue_name text NOT NULL,
       status text NOT NULL,
       payload_json jsonb NOT NULL,
       created_at timestamptz NOT NULL DEFAULT now()
     )`,
  );
  const payloads = [
    { channel_execution_attempt: { youtube_requests: { failure_count: "not-a-number" } } },
    { channel_execution_attempt: { youtube_requests: { failure_count: "1" } } },
    { channel_execution_attempt: { youtube_requests: { failure_count: 1 } } },
    {
      channel_execution_attempt: {
        youtube_requests: { request_count: 1, failure_count: 1 },
      },
    },
    {
      channel_execution_attempt: {
        youtube_requests: {
          request_count: 1,
          failure_count: 1,
          failure_evidence: [{
            source: "youtubejs_fetch",
            target_url: "https://www.youtube.com/youtubei/v1/browse",
          }],
        },
      },
    },
    {
      channel_execution_attempt: {
        youtube_requests: {
          request_count: 1,
          failure_count: 1,
          failure_evidence: [{ source: "yt_dlp_uploads" }],
        },
      },
    },
    {
      channel_execution_attempt: {
        youtube_requests: {
          request_count: 1,
          failure_count: 1,
          failure_evidence: [{
            source: "youtubejs_fetch",
            target_url: "https://www.youtube.com/youtubei/v1/player",
          }],
        },
      },
    },
  ];
  for (const payload of payloads) {
    await client.query(
      `INSERT INTO crawler.task_events (queue_name,status,payload_json)
       VALUES ('youtube-channel-crawl','failed',$1::jsonb)`,
      [JSON.stringify(payload)],
    );
  }

  const state = await dataApiCircuitState({
    query: client.query.bind(client),
    proxyCapacity: { active: 4, roles: { channel: { ready: 2 } } },
    detailExecutionQueue: "youtube-channel-crawl",
    detailExecutionRole: "channel",
  });

  assert.equal(state.recent_detail_failures, 1);
  assert.equal(state.open, false);
});
