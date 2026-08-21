import assert from "node:assert/strict";
import test from "node:test";
import {
  CHANNEL_CURRENT_CONTENT_SQL,
  CHANNEL_CURRENT_CONTENT_STATS_SQL,
  loadChannelCurrentContent,
} from "./channelCurrentContent.js";

test("Channel detail loads the canonical Channel content catalog instead of one Run", async () => {
  const calls = [];
  const queryDb = async (sql, params) => {
    calls.push({ sql, params });
    return sql === CHANNEL_CURRENT_CONTENT_STATS_SQL
      ? { rows: [{ content_type: "video", total: "2" }] }
      : { rows: [{ source_content_id: "newest-video" }] };
  };

  const loaded = await loadChannelCurrentContent(queryDb, " channel-id ");

  assert.deepEqual(loaded, {
    contentStats: [{ content_type: "video", total: "2" }],
    contents: [{ source_content_id: "newest-video" }],
  });
  assert.equal(calls.length, 2);
  assert.deepEqual(calls.map((call) => call.params), [["channel-id"], ["channel-id"]]);
  for (const call of calls) {
    assert.match(call.sql, /FROM crawler\.contents/);
    assert.match(call.sql, /WHERE channel_id = \$1/);
    assert.doesNotMatch(call.sql, /run_id/);
  }
  assert.match(
    CHANNEL_CURRENT_CONTENT_SQL,
    /ORDER BY published_at DESC NULLS LAST, last_seen_at DESC, source_content_id/,
  );
});
