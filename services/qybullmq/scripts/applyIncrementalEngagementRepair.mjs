import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { databaseUrl } from "../src/databaseConnection.js";
import { lockPublicationChannelMutation } from "../src/publicationChannelMutationLock.js";
import { refreshVideoPublicationItemHashes } from "../src/videoPublicationItemStore.js";
import { reconcilePublication } from "../src/publicationReconciler.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const [evidencePath, mode = "plan", expectedHash] = process.argv.slice(2);
assert.ok(evidencePath && ["plan", "apply"].includes(mode));
const evidenceText = await readFile(evidencePath, "utf8");
const evidenceHash = createHash("sha256").update(evidenceText).digest("hex");
if (mode === "apply") assert.equal(evidenceHash, expectedHash, "apply requires the reviewed evidence hash");
const evidence = evidenceText.trim().split("\n").map(JSON.parse);
assert.equal(evidence.length, 620);
assert.equal(new Set(evidence.map((row) => row.video_id)).size, 620);
const byId = new Map(evidence.map((row) => [row.video_id, row]));
const client = new pg.Client({ connectionString: databaseUrl(),
  options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}` });
await client.connect();
const result = { evidence_hash: evidenceHash, mode, updated: 0, stale: 0, missing_content: 0,
  failed: 0, unresolved: 0, terminal: 0, channels: [] };
try {
  const targets = (await client.query(`SELECT i.video_id,r.channel_id,
      i.field_status_json->>'like_count'='unobserved' AS like_missing,
      i.field_status_json->>'comment_count'='unobserved' AS comment_missing
    FROM crawler.incremental_youtubejs_video_items i JOIN crawler.channel_runs r USING(run_id)
    WHERE r.plan_day='2026-09-07' AND i.status='captured'
      AND (i.field_status_json->>'like_count'='unobserved' OR i.field_status_json->>'comment_count'='unobserved')
    ORDER BY r.channel_id,i.video_id`)).rows;
  assert.equal(targets.length, 620);
  const channels = new Map();
  for (const row of targets) {
    const observation = byId.get(row.video_id);
    assert.ok(observation);
    assert.equal(observation.like_missing, row.like_missing);
    assert.equal(observation.comment_missing, row.comment_missing);
    const group = channels.get(row.channel_id) ?? [];
    group.push({ ...row, observation });
    channels.set(row.channel_id, group);
  }
  for (const [channelId, rows] of channels) {
    await client.query("BEGIN");
    try {
      await client.query("SET LOCAL lock_timeout='10s'");
      await client.query("SELECT set_config('publication.writer_version',$1,true)", [PUBLICATION_WRITER_VERSION]);
      await lockPublicationChannelMutation(client, channelId);
      const keys = [];
      for (const row of rows) {
        const observation = row.observation;
        if (observation.status !== "captured") { result.failed++; continue; }
        const d = observation.detail;
        if (!["public", "unlisted"].includes(d.access_status)) { result.terminal++; continue; }
        const like = row.like_missing && Number.isSafeInteger(d.like_count) && d.like_count >= 0;
        const comment = row.comment_missing && Number.isSafeInteger(d.comment_count) && d.comment_count >= 0;
        if ((row.like_missing && !like) || (row.comment_missing && !comment)) result.unresolved++;
        if (!like && !comment) continue;
        assert.ok(!like || ["exact", "zero_from_empty"].includes(d.like_count_status));
        assert.ok(!comment || ["exact", "zero_from_surface", "zero_from_upcoming", "zero_from_empty", "disabled"].includes(d.comment_count_status));
        assert.ok(!comment || (d.comments_disabled === true ? d.comment_count === 0 && d.comment_count_status === "disabled" : d.comment_count_status !== "disabled"));
        assert.ok(Number.isFinite(Date.parse(observation.observed_at)));
        const existing = (await client.query(`SELECT content_key,next_last_observed_at,player_last_observed_at,
          like_count,like_count_status,comment_count,comment_count_status,comments_disabled,raw_json
          FROM crawler.contents WHERE channel_id=$1 AND source_content_id=$2 FOR UPDATE`,
        [channelId, row.video_id])).rows;
        if (!existing.length) { result.missing_content++; continue; }
        for (const content of existing) {
          if (content.raw_json?.engagement_repair?.evidence_hash === evidenceHash) continue;
          if ([content.next_last_observed_at, content.player_last_observed_at].some((at) => at && new Date(at) > new Date(observation.observed_at))) {
            result.stale++; continue;
          }
          const audit = { evidence_hash: evidenceHash, observed_at: observation.observed_at,
            previous: { like_count: content.like_count, like_count_status: content.like_count_status,
              comment_count: content.comment_count, comment_count_status: content.comment_count_status,
              comments_disabled: content.comments_disabled },
            like_count: d.like_count, like_count_status: d.like_count_status, like_count_source: d.like_count_source,
            comment_count: d.comment_count, comment_count_status: d.comment_count_status,
            comment_count_source: d.comment_count_source, comments_disabled: d.comments_disabled };
          await client.query(`UPDATE crawler.contents SET
            like_count=CASE WHEN $2 THEN $3 ELSE like_count END,
            like_count_status=CASE WHEN $2 THEN $4 ELSE like_count_status END,
            like_count_source=CASE WHEN $2 THEN $5 ELSE like_count_source END,
            comment_count=CASE WHEN $6 THEN $7 ELSE comment_count END,
            comment_count_status=CASE WHEN $6 THEN $8 ELSE comment_count_status END,
            comment_count_source=CASE WHEN $6 THEN $9 ELSE comment_count_source END,
            comments_disabled=CASE WHEN $6 THEN $10 ELSE comments_disabled END,
            raw_json=COALESCE(raw_json,'{}'::jsonb)||jsonb_build_object('engagement_repair',$11::jsonb)
            WHERE content_key=$1`, [content.content_key, like, like ? d.like_count : null,
            d.like_count_status, d.like_count_source, comment, comment ? d.comment_count : null,
            d.comment_count_status, d.comment_count_source, d.comments_disabled, JSON.stringify(audit)]);
          keys.push(content.content_key);
          result.updated++;
        }
      }
      if (keys.length) {
        await refreshVideoPublicationItemHashes(client, keys);
        const publication = await reconcilePublication(client, {
          channelId, domains: ["video"], asOf: new Date().toISOString(), revisionType: "repair",
        });
        result.channels.push({ channel_id: channelId, status: publication.status,
          diagnostics: publication.status === "not_ready" ? publication : undefined,
          revision_ids: (publication.revisions ?? []).map((r) => r.revision_id) });
        // Existing window gaps must not be bypassed to force delivery. Keep
        // repaired facts and report channels that the normal publisher blocks.
        assert.ok(["revised", "no_change", "not_ready"].includes(publication.status), `unexpected publication status for ${channelId}`);
      }
      await client.query(mode === "apply" ? "COMMIT" : "ROLLBACK");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    }
  }
  console.log(JSON.stringify(result));
} finally {
  await client.end();
}
