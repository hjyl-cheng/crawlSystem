import assert from "node:assert/strict";
import test from "node:test";
import {
  fullVideoStorageAction,
  normalizeFullVideoViewCount,
  upsertFullVideoContent,
} from "../src/fullVideoContentStore.js";

test("authoritative content type survives inconclusive access without becoming publishable", () => {
  assert.deepEqual(fullVideoStorageAction({
    candidate: {},
    classification: {
      source: "youtube_watch_shorts_eligible",
      content_type: "short",
      authoritative: true,
      canonical_url: "https://www.youtube.com/shorts/T2wgC0GkQKc",
    },
    access: { access_status: "unknown" },
  }), {
    kind: "classified_only",
    content_type: "short",
    type_source: "youtube_watch_shorts_eligible",
  });
});

test("new content storage eligibility is independent from authoritative type evidence", () => {
  const classification = {
    source: "youtube_watch_canonical",
    content_type: "video",
    authoritative: true,
  };
  for (const accessStatus of ["public", "unlisted", "members_only"]) {
    assert.equal(fullVideoStorageAction({
      candidate: {},
      classification,
      access: { access_status: accessStatus },
    }).kind, "upsert");
  }
  for (const accessStatus of ["unknown", "login_required", "private", "unavailable"]) {
    assert.deepEqual(fullVideoStorageAction({
      candidate: {},
      classification,
      access: { access_status: accessStatus },
    }), {
      kind: "classified_only",
      content_type: "video",
      type_source: "youtube_watch_canonical",
    });
  }
  assert.deepEqual(fullVideoStorageAction({
    candidate: {},
    classification: { ...classification, authoritative: false },
    access: { access_status: "public" },
  }), { kind: "unresolved" });
});

test("Full Video view normalization persists exact extractor text as a number", () => {
  assert.deepEqual(normalizeFullVideoViewCount({
    view_count_text: "12,345 views",
    view_count_status: "exact",
    view_count_source: "youtubejs_player",
  }, { locale: "en" }), {
    value: 12345,
    text: "12,345 views",
    status: "exact",
    source: "youtubejs_player",
  });
});

test("Full Video view normalization marks compact list counts as estimated", () => {
  assert.deepEqual(normalizeFullVideoViewCount({
    view_count_text: "2.5K views",
    view_count_status: "exact",
    view_count_source: "youtube_uploads",
  }, { locale: "en" }), {
    value: 2500,
    text: "2.5K views",
    status: "estimated",
    source: "youtube_uploads",
  });
});

test("Full Video view normalization does not label an unparseable value exact", () => {
  assert.deepEqual(normalizeFullVideoViewCount({
    view_count_text: "not-a-count",
    view_count_status: "exact",
    view_count_source: "unknown_layout",
  }, { locale: "en" }), {
    value: null,
    text: "not-a-count",
    status: "unresolved",
    source: "unknown_layout",
  });
});

test("Full Video upsert keeps one database identity when its Content type changes", async () => {
  const statements = [];
  const contentKey = await upsertFullVideoContent({
    async query(sql) {
      statements.push(sql);
      return sql.includes("INSERT INTO crawler.contents")
        ? { rows: [{ content_key: "UCidentity:video:same-id" }] }
        : { rows: [] };
    },
  }, {
    candidate: {
      channel_id: "UCidentity",
      run_id: null,
      content_type: "short",
      type_source: "youtube_shorts_tab",
      type_authoritative: true,
      source_content_id: "same-id",
      position: 1,
      title: "Same Content",
    },
    state: { detail: {}, access: { access_status: "public" } },
  });

  assert.equal(contentKey, "UCidentity:video:same-id");
  assert.match(statements[0], /ON CONFLICT \(channel_id, source_content_id\)/);
  assert.match(statements[0], /content_type=CASE[\s\S]*WHEN \$45::boolean THEN EXCLUDED\.content_type/);
  assert.doesNotMatch(statements[0], /content_type=EXCLUDED\.content_type/);
});

test("Full Video migration upsert persists the normalized first comment page", async () => {
  const page = {
    version: 1,
    collected_at: "2026-08-18T00:00:00.000Z",
    sort: "TOP_COMMENTS",
    total_count: 1,
    returned_count: 1,
    comments: [{
      comment_id: "Ugxd1",
      position: 1,
      text: "First comment",
    }],
  };
  let insertSql = null;
  let insertValues = null;
  const client = {
    async query(sql, values) {
      if (sql.includes("INSERT INTO crawler.contents")) {
        insertSql = sql;
        insertValues = values;
        return { rows: [{ content_key: "UCcomments:video:video-id" }] };
      }
      return { rows: [] };
    },
  };

  await upsertFullVideoContent(client, {
    candidate: {
      channel_id: "UCcomments",
      run_id: "run-comments",
      content_type: "video",
      type_source: "youtube_watch_canonical",
      type_authoritative: true,
      source_content_id: "video-id",
      position: 1,
      title: "Video with comments",
    },
    state: {
      detail: { comments_first_page: page },
      access: { access_status: "public" },
    },
  });

  assert.match(insertSql, /comments_first_page/);
  assert.deepEqual(JSON.parse(insertValues[30]), page);
  assert.match(
    insertSql,
    /WHEN COALESCE\(\(crawler\.contents\.comments_first_page->>'returned_count'\)::integer,0\)>0[\s\S]*THEN crawler\.contents\.comments_first_page/,
  );
  assert.match(
    insertSql,
    /WHEN COALESCE\(\(EXCLUDED\.comments_first_page->>'returned_count'\)::integer,0\)>0[\s\S]*THEN EXCLUDED\.comments_first_page/,
  );
  assert.doesNotMatch(
    insertSql,
    /comments_first_page=COALESCE\(EXCLUDED\.comments_first_page,crawler\.contents\.comments_first_page\)/,
  );
});

test("Full Video storage persists disabled comments as an authoritative zero", async () => {
  let insertSql = null;
  let insertValues = null;
  const client = {
    async query(sql, values) {
      if (sql.includes("INSERT INTO crawler.contents")) {
        insertSql = sql;
        insertValues = values;
        return { rows: [{ content_key: "UCcomments:video:disabled" }] };
      }
      return { rows: [] };
    },
  };

  await upsertFullVideoContent(client, {
    candidate: {
      channel_id: "UCcomments",
      run_id: "run-comments-disabled",
      content_type: "video",
      type_source: "youtube_watch_canonical",
      type_authoritative: true,
      source_content_id: "disabled",
      position: 1,
      title: "Comments disabled",
    },
    state: {
      detail: {
        comments_disabled: true,
        comment_count: null,
        comment_count_source: "youtubejs_comments",
      },
      access: { access_status: "public" },
    },
  });

  assert.equal(insertValues[26], 0);
  assert.equal(insertValues[27], "disabled");
  assert.equal(insertValues[28], true);
  assert.match(insertSql, /WHEN EXCLUDED\.comments_disabled=true THEN 0/);
});
