import assert from "node:assert/strict";
import test from "node:test";
import { refreshVideoPublicationItemHashes } from "../src/videoPublicationItemStore.js";

function row(id, overrides = {}) {
  return {
    channel_id: "UCstore",
    content_key: `UCstore:video:${id}`,
    source_content_id: id,
    content_type: "video",
    title: id,
    url: `https://www.youtube.com/watch?v=${id}`,
    published_at: "2026-07-20T08:00:00.000Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "youtube_player",
    duration_seconds: 120,
    duration_status: "exact",
    duration_source: "youtube_player",
    view_count: 100,
    view_count_status: "exact",
    view_count_source: "youtube_player",
    like_count: 10,
    like_count_status: "exact",
    like_count_source: "youtube_player",
    comment_count: 1,
    comment_count_status: "exact",
    comment_count_source: "youtube_next",
    comments_disabled: false,
    description: "",
    description_status: "empty",
    description_source: "youtube_player",
    access_status: "public",
    access_status_source: "youtube_player",
    is_members_only: false,
    last_seen_at: "2026-07-26T10:00:00.000Z",
    ...overrides,
  };
}

test("Item Hash Store persists ready hashes and clears stale hashes for incomplete Items", async () => {
  let updates = null;
  const rows = [
    row("ready"),
    row("incomplete", { title: null, publication_item_hash: "sha256:" + "a".repeat(64) }),
  ];
  const client = {
    async query(sql, params) {
      if (sql.includes("SELECT to_jsonb(content)")) {
        return { rows: rows.map((item) => ({ row: item })) };
      }
      if (sql.includes("jsonb_to_recordset")) {
        updates = JSON.parse(params[0]);
        return { rowCount: 2, rows: [] };
      }
      throw new Error(`unexpected query: ${sql}`);
    },
  };

  const result = await refreshVideoPublicationItemHashes(client, [
    rows[1].content_key,
    rows[0].content_key,
    rows[0].content_key,
  ]);

  assert.equal(result.requested_count, 2);
  assert.equal(result.ready_count, 1);
  assert.equal(result.incomplete_count, 1);
  assert.match(updates[0].publication_item_hash, /^sha256:[0-9a-f]{64}$/);
  assert.equal(updates[1].publication_item_hash, null);
});

test("Item Hash Store is a no-op for an empty key set", async () => {
  const result = await refreshVideoPublicationItemHashes({
    async query() {
      throw new Error("query should not be called");
    },
  }, []);
  assert.equal(result.requested_count, 0);
  assert.equal(result.changed_count, 0);
});
