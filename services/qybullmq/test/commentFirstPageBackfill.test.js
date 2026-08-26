import assert from "node:assert/strict";
import test from "node:test";
import {
  COMMENT_FIRST_PAGE_BACKFILL_SOURCE,
  backfillOneCommentFirstPage,
  commentFirstPageNeedsBackfill,
  loadCommentBackfillTargets,
  persistCommentFirstPage,
  shouldPersistCommentBackfill,
} from "../src/commentFirstPageBackfill.js";
import { runWithChannelExecution } from "../src/channelExecutionContext.js";

const storedPage = {
  version: 1,
  collected_at: "2026-08-17T12:00:00.000Z",
  sort: "TOP_COMMENTS",
  total_count: 12,
  returned_count: 1,
  comments: [{ comment_id: "Ugxd1", position: 1, text: "First comment" }],
};

test("empty or missing first pages need backfill", () => {
  assert.equal(commentFirstPageNeedsBackfill(null), true);
  assert.equal(commentFirstPageNeedsBackfill({
    version: 1,
    sort: "TOP_COMMENTS",
    returned_count: 0,
    comments: [],
  }), true);
  assert.equal(commentFirstPageNeedsBackfill({
    version: 1,
    sort: "TOP_COMMENTS",
    returned_count: 0,
    comments: [],
  }, { commentCountStatus: "zero_from_surface" }), false);
  assert.equal(commentFirstPageNeedsBackfill({
    version: 1,
    sort: "TOP_COMMENTS",
    returned_count: 0,
    comments: [],
  }, { commentsDisabled: true }), false);
  assert.equal(commentFirstPageNeedsBackfill(storedPage), false);
  assert.equal(commentFirstPageNeedsBackfill({
    version: 1,
    sort: "TOP_COMMENTS",
    total_count: 12,
    returned_count: 0,
    comments: [],
    resolution: {
      status: "confirmed_no_visible_threads",
      observed_comment_count: 12,
      checked_at: "2026-08-18T06:00:00.000Z",
      next_retry_at: "2099-08-25T06:00:00.000Z",
    },
  }), false);
  assert.equal(commentFirstPageNeedsBackfill({
    version: 1,
    sort: "TOP_COMMENTS",
    total_count: 12,
    returned_count: 0,
    comments: [],
    resolution: {
      status: "confirmed_no_visible_threads",
      observed_comment_count: 12,
      checked_at: "2026-08-18T06:00:00.000Z",
      next_retry_at: "2099-08-25T06:00:00.000Z",
    },
  }, { commentCount: 13 }), true);
});

test("only a resolved getComments page or disabled comments is persisted", () => {
  assert.equal(shouldPersistCommentBackfill({
    comments_first_page: storedPage,
    comment_count: 12,
  }), true);
  assert.equal(shouldPersistCommentBackfill({
    comments_disabled: true,
    comments_first_page: {
      version: 1,
      collected_at: "2026-08-17T12:00:00.000Z",
      sort: "TOP_COMMENTS",
      total_count: 0,
      returned_count: 0,
      comments: [],
    },
  }), true);
  assert.equal(shouldPersistCommentBackfill({
    comments_disabled: true,
    comments_first_page: {
      version: 1,
      collected_at: "2026-08-17T12:00:00.000Z",
      sort: "TOP_COMMENTS",
      total_count: 0,
      returned_count: 0,
      comments: [],
    },
  }, { existingCommentCount: 12 }), false);
  assert.equal(shouldPersistCommentBackfill({
    comments_first_page: {
      version: 1,
      collected_at: "2026-08-17T12:00:00.000Z",
      sort: "TOP_COMMENTS",
      total_count: 0,
      returned_count: 0,
      comments: [],
    },
    comment_count: 0,
    comment_count_status: "zero_from_surface",
  }), true);
  assert.equal(shouldPersistCommentBackfill({
    youtubejs_comments_error: "Comments page did not have any content.",
    comments_first_page: storedPage,
  }), false);
  assert.equal(shouldPersistCommentBackfill({ comment_count: 0 }), false);
});

test("loadCommentBackfillTargets keeps only empty first pages from the requested batch", async () => {
  const queries = [];
  const rows = await loadCommentBackfillTargets(async (sql, params) => {
    queries.push({ sql, params });
    return {
      rows: [{
        content_key: "UC1:video:abc",
        channel_id: "UC1",
        source_content_id: "abc",
        comments_first_page: {
          version: 1,
          sort: "TOP_COMMENTS",
          returned_count: 0,
          comments: [],
        },
      }],
    };
  }, { batchId: "youtubejs-comment-keep-20-20260817-v1", limit: 20 });
  assert.equal(rows.length, 1);
  assert.equal(queries[0].params[0], "youtubejs-comment-keep-20-20260817-v1");
  assert.match(queries[0].sql, /dispatch_batch_id/);
  assert.match(queries[0].sql, /confirmed_no_visible_threads/);
  assert.match(queries[0].sql, /next_retry_at/);
  assert.ok(
    queries[0].sql.indexOf("confirmed_no_visible_threads") < queries[0].sql.indexOf("LIMIT $2"),
    "future-dated confirmations must be filtered before LIMIT is applied",
  );
});

test("loadCommentBackfillTargets selects incremental first-seen videos by plan day", async () => {
  const queries = [];
  const rows = await loadCommentBackfillTargets(async (sql, params) => {
    queries.push({ sql, params });
    return {
      rows: [{
        content_key: "UC1:video:today",
        channel_id: "UC1",
        source_content_id: "today",
        comment_count: 3,
        comment_count_status: "exact",
        comments_disabled: false,
        comments_first_page: null,
      }],
    };
  }, { batchId: null, planDay: "2026-08-19", limit: 20 });

  assert.equal(rows.length, 1);
  assert.deepEqual(queries[0].params, ["2026-08-19", 20]);
  assert.match(queries[0].sql, /content\.run_id\s*=\s*run\.run_id/);
  assert.match(queries[0].sql, /run\.plan_day\s*=\s*\$1::date/);
  assert.match(queries[0].sql, /content\.first_seen_at\s*>=\s*\$1::date/);
  assert.match(queries[0].sql, /COALESCE\(target\.comment_count_status,\s*'unresolved'\)/);
  assert.doesNotMatch(queries[0].sql, /content_candidates/);
  assert.doesNotMatch(queries[0].sql, /dispatch_batch_id/);
});

test("loadCommentBackfillTargets partitions a plan day into deterministic shards", async () => {
  const queries = [];
  await loadCommentBackfillTargets(async (sql, params) => {
    queries.push({ sql, params });
    return { rows: [] };
  }, {
    batchId: null,
    planDay: "2026-08-19",
    limit: 5000,
    shardCount: 10,
    shardIndex: 3,
  });

  assert.deepEqual(queries[0].params, ["2026-08-19", 5000, 10, 3]);
  assert.match(
    queries[0].sql,
    /mod\(abs\(hashtext\(content\.content_key\)::bigint\),\s*\$3::integer\)\s*=\s*\$4::integer/,
  );
});

test("persistCommentFirstPage updates only comment fields", async () => {
  const queries = [];
  const result = await persistCommentFirstPage({
    async query(sql, params) {
      queries.push({ sql, params });
      return { rowCount: 1, rows: [{ content_key: params[0] }] };
    },
  }, {
    contentKey: "UC1:video:abc",
    channelId: "UC1",
    sourceContentId: "abc",
    detail: {
      comment_count: 12,
      comment_count_status: "exact",
      comment_count_source: "youtubejs_comments",
      comments_disabled: false,
      comments_first_page: storedPage,
    },
  });
  assert.equal(result.updated, true);
  assert.match(queries[0].sql, /comments_first_page = \$4::jsonb/);
  assert.doesNotMatch(queries[0].sql, /title=/);
  assert.doesNotMatch(queries[0].sql, /comment_count\s*=/);
  assert.doesNotMatch(queries[0].sql, /comments_disabled\s*=/);
  assert.match(queries[0].sql, /COALESCE\(comment_count,\s*0\)\s*=\s*0/);
  assert.equal(JSON.parse(queries[0].params[3]).comments[0].comment_id, "Ugxd1");
  assert.equal(JSON.parse(queries[0].params[4]).source, COMMENT_FIRST_PAGE_BACKFILL_SOURCE);
});

test("backfillOneCommentFirstPage keeps going when getInfo throws", async () => {
  const persisted = [];
  const result = await backfillOneCommentFirstPage({
    content_key: "UC1:video:abc",
    channel_id: "UC1",
    source_content_id: "abc",
  }, {
    fetchDetail: async () => {
      throw new Error('init["status"] must be in the range of 200 to 599, inclusive.');
    },
    persist: async (_client, payload) => {
      persisted.push(payload);
      return { updated: true, reason: "updated" };
    },
  });
  assert.equal(result.updated, false);
  assert.equal(result.reason, "comment_detail_error");
  assert.match(result.comments_error, /status/);
  assert.equal(persisted.length, 0);
});

test("backfillOneCommentFirstPage propagates channel cancellation instead of returning an error row", async () => {
  const controller = new AbortController();
  const reason = new Error("cancel comment backfill batch");
  let forwardedSignal = null;

  await assert.rejects(
    runWithChannelExecution({ abort_signal: controller.signal }, () => (
      backfillOneCommentFirstPage({
        content_key: "UC1:video:cancelled",
        channel_id: "UC1",
        source_content_id: "cancelled",
      }, {
        fetchDetail: async (_videoId, { signal } = {}) => {
          forwardedSignal = signal ?? null;
          controller.abort(reason);
          throw reason;
        },
      })
    )),
    (error) => error === reason,
  );
  assert.equal(forwardedSignal, controller.signal);
});

test("backfillOneCommentFirstPage rejects an empty page that conflicts with a stored positive count", async () => {
  const persisted = [];
  const result = await backfillOneCommentFirstPage({
    content_key: "UC1:video:abc",
    channel_id: "UC1",
    source_content_id: "abc",
    comment_count: 125,
    comment_count_status: "exact",
    comments_disabled: false,
  }, {
    fetchDetail: async () => ({
      comments_disabled: true,
      comment_count: null,
      comment_count_status: "disabled",
      comments_first_page: {
        version: 1,
        collected_at: "2026-08-19T00:00:00.000Z",
        sort: "TOP_COMMENTS",
        total_count: 0,
        returned_count: 0,
        comments: [],
      },
    }),
    persist: async (_client, payload) => {
      persisted.push(payload);
      return { updated: true, reason: "updated" };
    },
  });

  assert.equal(result.updated, false);
  assert.equal(result.reason, "comment_page_conflicts_with_stored_count");
  assert.equal(persisted.length, 0);
});

test("backfillOneCommentFirstPage does not write an unresolved getComments result", async () => {
  const persisted = [];
  const result = await backfillOneCommentFirstPage({
    content_key: "UC1:video:abc",
    channel_id: "UC1",
    source_content_id: "abc",
  }, {
    fetchDetail: async () => ({
      youtubejs_comments_error: "Comments page did not have any content.",
    }),
    persist: async (_client, payload) => {
      persisted.push(payload);
      return { updated: true, reason: "updated" };
    },
  });
  assert.equal(result.updated, false);
  assert.equal(result.reason, "youtubejs_comments_error");
  assert.equal(persisted.length, 0);
});
