import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { applyVideoActivityLifecycle } from "../src/videoActivityLifecycle.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const { Pool } = pg;
const integrationUrl = process.env.INCREMENTAL_POSTGRES_TEST_URL;

test("Incremental activity pagination reads one PostgreSQL cursor snapshot", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({
    connectionString: integrationUrl,
    max: 3,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCactivitysnapshot${suffix}`;
  const recentContentKey = `${channelId}:video:z-recent`;
  let reader = null;
  let readerOpen = false;

  try {
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Activity snapshot integration','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,content_type,source_content_id,title,
         published_at,published_at_status,published_at_precision,published_at_source
       ) VALUES
       ($1,$2,'video','a-old','Old','2026-01-01T00:00:00Z','exact','second','test'),
       ($3,$2,'video','z-recent','Recent','2026-07-22T00:00:00Z','exact','second','test')`,
      [`${channelId}:video:a-old`, channelId, recentContentKey],
    );

    reader = await pool.connect();
    await reader.query("BEGIN");
    readerOpen = true;
    assert.equal(
      (await reader.query("SHOW transaction_isolation")).rows[0].transaction_isolation,
      "read committed",
    );

    let evidencePageCount = 0;
    const transactionClient = {
      async query(sql, params = []) {
        const result = await reader.query(sql, params);
        if (String(sql).includes("FROM crawler.contents")) {
          evidencePageCount += 1;
          if (evidencePageCount === 1) {
            await pool.query(
              "UPDATE crawler.contents SET published_at='2026-01-02T00:00:00Z' WHERE content_key=$1",
              [recentContentKey],
            );
          }
        }
        return result;
      },
    };

    const result = await applyVideoActivityLifecycle(transactionClient, {
      channelId,
      observedAt: "2026-07-23T12:00:00.000Z",
      discoveryComplete: true,
      evidenceScanRowLimit: 10,
      evidenceScanPageSize: 1,
      evidenceScanTimeBudgetMs: 5_000,
    });

    assert.equal(result.lifecycle_status, "active");
    assert.equal(result.conclusive, true);
    assert.equal(result.evidence_scan_complete, true);
    assert.equal(result.evidence_scan_page_count, 2);
    assert.equal(result.relation_counts.inside, 1);
    assert.equal(result.relation_counts.outside, 1);

    await reader.query("COMMIT");
    readerOpen = false;
    assert.equal(
      (await pool.query(
        "SELECT published_at FROM crawler.contents WHERE content_key=$1",
        [recentContentKey],
      )).rows[0].published_at.toISOString(),
      "2026-01-02T00:00:00.000Z",
    );
  } finally {
    if (readerOpen) await reader?.query("ROLLBACK").catch(() => {});
    reader?.release();
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.end();
  }
});

test("Incremental activity timeout leaves the PostgreSQL transaction usable", {
  skip: !integrationUrl,
  timeout: 5_000,
}, async () => {
  const pool = new Pool({
    connectionString: integrationUrl,
    max: 3,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCactivitytimeout${suffix}`;
  let blocker = null;
  let blockerOpen = false;
  let reader = null;
  let readerOpen = false;

  try {
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Activity timeout integration','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,content_type,source_content_id,title,
         published_at,published_at_status,published_at_precision,published_at_source
       ) VALUES
       ($1,$2,'video','old','Old','2026-01-01T00:00:00Z','exact','second','test')`,
      [`${channelId}:video:old`, channelId],
    );

    blocker = await pool.connect();
    await blocker.query("BEGIN");
    blockerOpen = true;
    await blocker.query("LOCK TABLE crawler.contents IN ACCESS EXCLUSIVE MODE");

    reader = await pool.connect();
    await reader.query("BEGIN");
    readerOpen = true;
    const result = await applyVideoActivityLifecycle(reader, {
      channelId,
      observedAt: "2026-07-23T12:00:00.000Z",
      discoveryComplete: true,
      evidenceScanRowLimit: 10,
      evidenceScanPageSize: 1,
      evidenceScanTimeBudgetMs: 50,
    });

    assert.equal(result.lifecycle_status, "active");
    assert.equal(result.conclusive, false);
    assert.equal(result.evidence_scan_complete, false);
    assert.equal(result.evidence_scan_stop_reason, "time_budget");
    assert.ok(result.evidence_scan_elapsed_ms >= 50);
    assert.equal((await reader.query("SHOW statement_timeout")).rows[0].statement_timeout, "0");
    assert.equal((await reader.query("SELECT 1 AS usable")).rows[0].usable, 1);

    await reader.query("COMMIT");
    readerOpen = false;
    await blocker.query("ROLLBACK");
    blockerOpen = false;
  } finally {
    if (readerOpen) await reader?.query("ROLLBACK").catch(() => {});
    reader?.release();
    if (blockerOpen) await blocker?.query("ROLLBACK").catch(() => {});
    blocker?.release();
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.end();
  }
});
