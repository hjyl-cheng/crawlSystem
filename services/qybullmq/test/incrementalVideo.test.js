import assert from "node:assert/strict";
import test from "node:test";
import {
  executeIncrementalVideo as executeIncrementalVideoWithSystemClock,
  fetchIncrementalVideoDetail,
} from "../src/incrementalVideo.js";

function executeIncrementalVideo(options) {
  return executeIncrementalVideoWithSystemClock({
    ...options,
    now: () => new Date(options.startedAt),
  });
}

function plan() {
  return {
    job_id: "incremental__UCvideo__20260720__clock_7__hash",
    plan_id: "4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d",
    plan_day: "2026-07-20",
    scheduled_at: "2026-07-20T13:25:40.000Z",
    channel_id: "UCvideo",
    capacity: { factor: 1, player_cap: 20, next_cap: 8, version: "capacity-1" },
    planner_config_version: "video-plan-1",
  };
}

function detail(videoId, viewCount, { contentType = "video" } = {}) {
  const isShort = contentType === "short";
  const isLive = contentType === "live";
  return {
    id: videoId,
    title: `Title ${videoId}`,
    published_at: "2026-07-19T00:00:00.000Z",
    published_at_precision: "second",
    view_count: viewCount,
    comment_count: 2,
    comment_count_status: "exact",
    comment_count_source: "youtubejs_comments",
    comments_disabled: null,
    comments_first_page: {
      version: 1,
      collected_at: "2026-07-20T00:00:00.000Z",
      sort: "TOP_COMMENTS",
      total_count: 2,
      returned_count: 1,
      comments: [{ comment_id: `comment-${videoId}`, text: `Comment ${videoId}` }],
    },
    availability: "public",
    access_status: "public",
    content_type_signals: {
      source: "youtubei_player",
      canonical_url: isShort
        ? `https://www.youtube.com/shorts/${videoId}`
        : `https://www.youtube.com/watch?v=${videoId}`,
      is_shorts_eligible: isShort,
      is_live_content: isLive,
      is_live: false,
      is_upcoming: false,
      is_live_now: false,
    },
    extractor_version: "youtubei.js@test",
  };
}

function databaseFixture({ beforeTransaction = null } = {}) {
  const state = {
    candidates: new Set(["candidate-only"]),
    contents: [
      {
        content_key: "UCvideo:video:known-anchor",
        channel_id: "UCvideo",
        source_content_id: "known-anchor",
        content_type: "video",
        published_at: "2026-07-10T00:00:00.000Z",
        last_seen_at: "2026-07-20T00:00:00.000Z",
        view_count: 200,
        player_last_observed_at: "2026-07-20T00:00:00.000Z",
        next_last_observed_at: null,
        video_change_probability: 0,
        like_count: null,
        comment_count: null,
      },
      {
        content_key: "UCvideo:video:old-video",
        channel_id: "UCvideo",
        source_content_id: "old-video",
        content_type: "video",
        published_at: "2026-07-09T00:00:00.000Z",
        last_seen_at: "2026-07-20T00:00:00.000Z",
        view_count: 90,
        player_last_observed_at: null,
        next_last_observed_at: null,
        like_count: null,
        comment_count: null,
      },
    ],
    sql: [],
    outbox: [],
    observationSummaries: [],
    cursorKind: null,
    cursorUpdates: [],
    cursorAnchors: ["known-anchor", "old-video"],
    enrichPending: new Set(),
    channelStatus: "active",
  };

  const client = {
    async query(sql, params = []) {
      state.sql.push(sql);
      if (sql.includes("INSERT INTO crawler.crawl_observation_keys")) {
        return { rowCount: 1, rows: [{ observation_id: params[1] }] };
      }
      if (sql.includes("INSERT INTO crawler.channel_domain_cursors")) {
        state.cursorKind = params[1];
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("SELECT * FROM crawler.channel_domain_cursors")) {
        return { rowCount: 1, rows: [{ latest_sequence: 0 }] };
      }
      if (sql.includes("SELECT status,reject_reason,dormant_reason")) {
        return {
          rowCount: 1,
          rows: [{
            status: state.channelStatus,
            reject_reason: null,
            dormant_reason: null,
            dormant_since: null,
            dormant_recheck_day: null,
            dormant_last_probe_at: null,
            dormant_cycle: 0,
          }],
        };
      }
      if (sql.includes("count(DISTINCT source_content_id)::int")) {
        const recent = state.contents.filter((row) => row.published_at != null).length;
        return { rowCount: 1, rows: [{ recent_published_content_count: recent }] };
      }
      if (sql.includes("UPDATE crawler.channels") && sql.includes("SET status='active'")) {
        state.channelStatus = "active";
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("UPDATE crawler.channels") && sql.includes("SET status='dormant'")) {
        state.channelStatus = "dormant";
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("UPDATE crawler.channel_domain_cursors")) {
        state.cursorUpdates.push({
          outcome: params[4],
          anchorVideoIds: params[6],
          sourceCursor: params[7],
        });
        if (Array.isArray(params[6])) state.cursorAnchors = [...params[6]];
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("FROM crawler.contents") && sql.includes("source_content_id=ANY")) {
        const known = new Set(state.contents.map((row) => row.source_content_id));
        const rows = params[1].filter((id) => known.has(id)).map((video_id) => ({ video_id }));
        return { rowCount: rows.length, rows };
      }
      if (sql.includes("UPDATE crawler.contents content") && sql.includes("jsonb_to_recordset")) {
        for (const input of JSON.parse(params[2])) {
          const row = state.contents.find((item) => item.source_content_id === input.video_id);
          if (row && input.content_type) row.content_type = input.content_type;
        }
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("INSERT INTO crawler.contents")) {
        state.contents.push({
          content_key: params[0],
          channel_id: params[1],
          source_content_id: params[5],
          content_type: params[3],
          published_at: params[15],
          view_count: params[23],
          player_last_observed_at: params[39],
          next_last_observed_at: params[40] ? params[39] : null,
          like_count: params[27],
          comment_count: params[30],
          comments_disabled: params[32],
          comments_first_page: params[43] == null ? null : JSON.parse(params[43]),
          access_status: params[35],
        });
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("AS enrich_pending") && sql.includes("FROM crawler.contents content")) {
        const cutoff = new Date(`${params[1]}T00:00:00.000Z`);
        cutoff.setUTCDate(cutoff.getUTCDate() - Number(params[2]));
        const pendingBypassesWindow = sql.includes("WHERE candidate.enrich_pending");
        const rows = state.contents
          .filter((row) => {
            const isPending = state.enrichPending.has(row.content_key);
            const published = row.published_at == null ? null : new Date(row.published_at);
            const isRecent = published != null && published >= cutoff;
            return isRecent || (pendingBypassesWindow && isPending);
          })
          .map((row) => ({
            ...row,
            enrich_pending: state.enrichPending.has(row.content_key),
          }));
        return { rowCount: rows.length, rows };
      }
      if (sql.includes("SELECT * FROM crawler.contents") && sql.includes("FOR UPDATE")) {
        return { rowCount: params[0].length, rows: state.contents.filter((row) => params[0].includes(row.content_key)) };
      }
      if (sql.includes("UPDATE crawler.contents\n     SET content_type=CASE")) {
        const row = state.contents.find((item) => item.content_key === params[0]);
        row.content_type = params[35] ?? row.content_type;
        row.title = params[9] ?? row.title;
        row.thumbnail_url = params[10] ?? row.thumbnail_url;
        if (params[12]) row.description = params[11];
        if (params[15]) row.hashtags = params[14];
        if (params[17]) row.keywords = params[16];
        row.published_at = params[18] ?? row.published_at;
        row.published_at_source = params[19] ?? row.published_at_source;
        row.published_at_precision = params[20] ?? row.published_at_precision;
        row.duration_seconds = params[22] ?? row.duration_seconds;
        row.view_count = params[2];
        row.view_count_source = params[24] ?? row.view_count_source;
        row.comments_disabled = params[5];
        row.comments_first_page = params[38] == null ? row.comments_first_page : JSON.parse(params[38]);
        row.video_change_probability = params[8];
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("UPDATE crawler.content_enrich_tasks") && sql.includes("status='done'")) {
        state.enrichPending.delete(params[0]);
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("INSERT INTO crawler.crawler_outbox")) {
        state.outbox.push(JSON.parse(params[7]));
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("INSERT INTO crawler.crawl_observations")) {
        state.observationSummaries.push(JSON.parse(params[14]));
        return { rowCount: 1, rows: [] };
      }
      return { rowCount: 1, rows: [] };
    },
  };

  return {
    state,
    withTransaction: async (action) => {
      if (beforeTransaction) await beforeTransaction(state);
      return action(client);
    },
    query: async (sql, params = []) => {
      state.sql.push(sql);
      if (sql.includes("unnest(cursor.anchor_video_ids)")) {
        const byId = new Map(state.contents.map((row) => [row.source_content_id, row]));
        return {
          rows: [{
            anchors: state.cursorAnchors
              .map((video_id) => ({ video_id, published_at: byId.get(video_id)?.published_at ?? null }))
              .filter((row) => row.published_at != null),
          }],
        };
      }
      if (sql.includes("SELECT video_id,published_at")) {
        return {
          rows: [...state.contents]
            .sort((left, right) => right.published_at.localeCompare(left.published_at))
            .map((row) => ({ video_id: row.source_content_id, published_at: row.published_at })),
        };
      }
      if (sql.includes("FROM crawler.contents") && sql.includes("source_content_id=ANY")) {
        const known = new Set(state.contents.map((row) => row.source_content_id));
        return { rows: params[1].filter((id) => known.has(id)).map((video_id) => ({ video_id })) };
      }
      if (sql.includes("SELECT * FROM crawler.contents")) {
        return { rows: state.contents.map((row) => ({ ...row })) };
      }
      throw new Error(`unexpected query: ${sql}`);
    },
  };
}

test("Video execution deduplicates only against Contents before sampling old Videos", async () => {
  const fixture = databaseFixture();
  const fetched = [];
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:plan",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads(options) {
        assert.deepEqual(options.anchors, [
          { id: "known-anchor", published_day: "2026-07-10" },
          { id: "old-video", published_day: "2026-07-09" },
        ]);
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "new-video", position: 1, content_type: "video", title: "New" },
            { id: "candidate-only", position: 2, content_type: "video", title: "Known Candidate" },
            { id: "known-anchor", position: 3, content_type: "video", title: "Known" },
          ],
          pages: 1,
          item_count: 3,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return detail(videoId, videoId === "new-video" ? 10 : 100);
    },
  });

  assert.equal(result.outcome, "complete");
  assert.equal(result.first_seen_count, 2);
  assert.deepEqual(fetched, ["new-video", "candidate-only", "old-video"]);
  assert.equal(fixture.state.contents.filter((row) => row.source_content_id === "new-video").length, 1);
  assert.equal(fixture.state.contents.filter((row) => row.source_content_id === "candidate-only").length, 1);
  assert.equal(fixture.state.contents.find((row) => row.source_content_id === "new-video").comments_disabled, null);
  assert.equal(
    fixture.state.contents.find((row) => row.source_content_id === "new-video")
      .comments_first_page?.comments?.[0]?.comment_id,
    "comment-new-video",
  );
  assert.equal(fixture.state.contents.find((row) => row.source_content_id === "old-video").view_count, 100);
  assert.equal(
    fixture.state.contents.find((row) => row.source_content_id === "old-video")
      .comments_first_page?.comments?.[0]?.comment_id,
    "comment-old-video",
  );
  assert.equal(fetched.includes("candidate-only"), true);
  assert.deepEqual(fixture.state.outbox.map((event) => event.observation_kind), [
    "video",
  ]);
  assert.equal(fixture.state.outbox[0].payload.discovery.outcome, "complete");
  assert.equal(fixture.state.outbox[0].payload.recent_sampling.outcome, "complete");
  assert.equal(fixture.state.outbox[0].plan_id, plan().plan_id);
  assert.equal(fixture.state.observationSummaries[0].discovery.parse_gap_count, 0);
  assert.equal(fixture.state.sql.some((sql) => /snapshot/i.test(sql)), false);
  assert.equal(fixture.state.sql.some((sql) => sql.includes("channel_video_catalog")), false);
  const discoveryWrite = fixture.state.sql.findIndex((sql) => sql.includes("INSERT INTO crawler.contents"));
  const discoverySql = fixture.state.sql[discoveryWrite];
  assert.match(
    discoverySql,
    /comments_disabled=CASE[\s\S]*EXCLUDED\.comments_disabled IS NOT NULL/,
  );
  assert.match(
    discoverySql,
    /access_status=CASE[\s\S]*EXCLUDED\.access_status='unknown'[\s\S]*crawler\.contents\.access_status/,
  );
  assert.match(discoverySql, /ON CONFLICT \(channel_id,source_content_id\)/);
  assert.match(
    discoverySql,
    /content_type=CASE[\s\S]*WHEN \$43::boolean THEN EXCLUDED\.content_type[\s\S]*ELSE crawler\.contents\.content_type/,
  );
  assert.doesNotMatch(discoverySql, /content_type=EXCLUDED\.content_type/);
  const samplingRead = fixture.state.sql.findIndex((sql) => (
    sql.includes("FROM crawler.contents") && sql.includes("published_at>=")
  ));
  assert.equal(discoveryWrite >= 0 && samplingRead > discoveryWrite, true);
  const crawlerOutbox = fixture.state.sql.findIndex((sql) => (
    sql.includes("INSERT INTO crawler.crawler_outbox")
  ));
  const publication = fixture.state.sql.findIndex((sql) => (
    sql.includes("publication-reconciler:find-owner")
  ));
  assert.ok(crawlerOutbox >= 0 && publication > crawlerOutbox);
});

test("Video execution classifies every First-Seen Video independently of the stored Video refresh cap", async () => {
  const fixture = databaseFixture();
  const fetched = [];
  const result = await executeIncrementalVideo({
    plan: {
      ...plan(),
      capacity: { ...plan().capacity, player_cap: 1, next_cap: 0 },
    },
    runId: "incremental:first-seen-independent-cap",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "new-video-a", position: 1, content_type: null, title: "New A" },
            { id: "new-video-b", position: 2, content_type: null, title: "New B" },
            { id: "known-anchor", position: 3, content_type: null, title: "Known" },
          ],
          pages: 1,
          item_count: 3,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return detail(videoId, videoId === "old-video" ? 100 : 10);
    },
  });

  assert.equal(result.outcome, "complete");
  assert.equal(result.first_seen_count, 2);
  assert.deepEqual(fetched, ["new-video-a", "new-video-b", "old-video"]);
  assert.equal(
    fixture.state.contents.filter((row) => row.source_content_id.startsWith("new-video-")).length,
    2,
  );
  assert.equal(fixture.state.contents.find((row) => row.source_content_id === "old-video").view_count, 100);
});

test("Video execution rechecks First-Seen after acquiring its transaction", async () => {
  const fixture = databaseFixture({
    beforeTransaction(state) {
      state.contents.push({
        content_key: "UCvideo:video:race-video",
        channel_id: "UCvideo",
        source_content_id: "race-video",
        content_type: "video",
        published_at: "2026-07-19T00:00:00.000Z",
        last_seen_at: "2026-07-20T00:00:00.000Z",
        view_count: 10,
        player_last_observed_at: "2026-07-20T00:00:00.000Z",
        next_last_observed_at: null,
        video_change_probability: 0,
        like_count: null,
        comment_count: null,
      });
    },
  });
  const fetched = [];
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:race-plan",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "race-video", position: 1, content_type: "video", title: "Race" },
            { id: "known-anchor", position: 2, content_type: "video", title: "Known" },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          crossed_anchor_ids: [],
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return detail(videoId, videoId === "old-video" ? 100 : 10);
    },
  });

  assert.equal(result.outcome, "complete");
  assert.equal(result.first_seen_count, 0);
  assert.equal(fixture.state.contents.filter((row) => row.source_content_id === "race-video").length, 1);
  assert.equal(fetched.includes("race-video"), true);
  assert.equal(fixture.state.sql.some((sql) => sql.includes("channel_video_catalog")), false);
});

test("Video execution keeps its backup Anchor pool when a later Anchor closes Discovery", async () => {
  const fixture = databaseFixture();
  const anchorIds = Array.from(
    { length: 20 },
    (_, index) => `anchor-${String(index + 1).padStart(2, "0")}`,
  );
  fixture.state.cursorAnchors = [...anchorIds];
  fixture.state.contents = anchorIds.map((sourceContentId, index) => ({
    content_key: `UCvideo:video:${sourceContentId}`,
    channel_id: "UCvideo",
    source_content_id: sourceContentId,
    content_type: "video",
    published_at: `2026-07-${String(20 - index).padStart(2, "0")}T00:00:00.000Z`,
    last_seen_at: "2026-07-20T00:00:00.000Z",
    view_count: 100,
    player_last_observed_at: "2026-07-20T00:00:00.000Z",
    next_last_observed_at: "2026-07-20T00:00:00.000Z",
    video_change_probability: 0,
    like_count: null,
    comment_count: null,
  }));

  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:backup-anchor-pool",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "new-video", position: 1, content_type: "video", published_day: "2026-07-20" },
            { id: "anchor-05", position: 2, content_type: "video", published_day: "2026-07-16" },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "anchor-05",
          crossed_anchor_ids: anchorIds.slice(0, 4),
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => detail(videoId, 100),
  });

  assert.equal(result.outcome, "complete");
  assert.deepEqual(fixture.state.cursorUpdates[0].anchorVideoIds, [
    "new-video",
    ...anchorIds.slice(0, 19),
  ]);
});

test("Video execution updates Content type without creating a second Video identity", async () => {
  const fixture = databaseFixture();
  fixture.state.contents.push({
    content_key: "UCvideo:video:type-changing",
    channel_id: "UCvideo",
    source_content_id: "type-changing",
    content_type: "video",
    published_at: "2026-07-18T00:00:00.000Z",
    last_seen_at: "2026-07-20T00:00:00.000Z",
    view_count: 100,
    player_last_observed_at: null,
    next_last_observed_at: null,
    video_change_probability: 0,
    like_count: null,
    comment_count: null,
  });

  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:type-change",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "type-changing", position: 1, content_type: "short", published_day: "2026-07-18" },
            { id: "known-anchor", position: 2, content_type: "video", published_day: "2026-07-10" },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => detail(videoId, 100, {
      contentType: videoId === "type-changing" ? "short" : "video",
    }),
  });

  assert.equal(result.outcome, "complete");
  assert.equal(
    fixture.state.contents.filter((row) => row.source_content_id === "type-changing").length,
    1,
  );
  assert.equal(
    fixture.state.contents.find((row) => row.source_content_id === "type-changing").content_type,
    "short",
  );
});

test("Video sampling recovers queued detail when published_at is unresolved", async () => {
  const contentKey = "UCvideo:video:missing-published-at";
  const fixture = databaseFixture({
    beforeTransaction(state) {
      state.contents.push({
        content_key: contentKey,
        channel_id: "UCvideo",
        source_content_id: "missing-published-at",
        content_type: "video",
        published_at: null,
        last_seen_at: "2026-07-20T00:00:00.000Z",
        view_count: null,
        player_last_observed_at: null,
        next_last_observed_at: null,
        video_change_probability: null,
        like_count: null,
        comment_count: null,
      });
      state.enrichPending.add(contentKey);
    },
  });
  const fetched = [];

  const result = await executeIncrementalVideo({
    plan: {
      ...plan(),
      capacity: { ...plan().capacity, player_cap: 1, next_cap: 0 },
    },
    runId: "incremental:recover-missing-published-at",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [{
            id: "known-anchor",
            position: 1,
            content_type: "video",
            title: "Known",
            published_day: "2026-07-10",
          }],
          pages: 1,
          item_count: 1,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return {
        ...detail(videoId, 123),
        title: "Recovered title",
        thumbnail_url: "https://i.ytimg.com/vi/missing-published-at/maxresdefault.jpg",
        description: "Recovered description",
        hashtags: ["recovered"],
        hashtags_observed: true,
        keywords: ["one", "two"],
        keywords_observed: true,
        duration_seconds: 91,
        duration_source: "yt_dlp",
        published_at_source: "yt_dlp_timestamp",
        view_count_source: "yt_dlp",
        ytdlp_client: "web_safari",
      };
    },
  });

  assert.equal(result.outcome, "complete");
  assert.deepEqual(fetched, ["missing-published-at"]);
  const recovered = fixture.state.contents.find((row) => row.content_key === contentKey);
  assert.equal(recovered.title, "Recovered title");
  assert.equal(recovered.published_at, "2026-07-19T00:00:00.000Z");
  assert.equal(recovered.published_at_source, "yt_dlp_timestamp");
  assert.equal(recovered.duration_seconds, 91);
  assert.equal(recovered.view_count_source, "yt_dlp");
  assert.equal(fixture.state.enrichPending.has(contentKey), false);
});

test("Video detail falls back to yt-dlp when YouTube.js is challenged", async () => {
  const calls = [];
  const result = await fetchIncrementalVideoDetail("new-video", {
    fetchYoutubeJs: async () => {
      calls.push("youtubejs");
      throw new Error("YouTube bot challenge HTTP 200 for /youtubei/v1/player");
    },
    fetchYtDlp: async (videoId, url) => {
      calls.push("yt-dlp");
      assert.equal(videoId, "new-video");
      assert.equal(url, "https://www.youtube.com/watch?v=new-video");
      return detail(videoId, 42);
    },
  });

  assert.deepEqual(calls, ["youtubejs", "yt-dlp"]);
  assert.equal(result.view_count, 42);
});

test("Video detail falls back to yt-dlp when YouTube.js returns incomplete facts", async () => {
  const calls = [];
  const result = await fetchIncrementalVideoDetail("partial-video", {
    fetchYoutubeJs: async () => {
      calls.push("youtubejs");
      return { id: "partial-video", published_at: "2026-07-19T00:00:00.000Z" };
    },
    fetchYtDlp: async () => {
      calls.push("yt-dlp");
      return detail("partial-video", 84);
    },
  });

  assert.deepEqual(calls, ["youtubejs", "yt-dlp"]);
  assert.equal(result.view_count, 84);
});

test("Video discovery does not persist an upcoming live before it starts", async () => {
  const fixture = databaseFixture();
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:upcoming-live",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            {
              id: "upcoming-live",
              position: 1,
              content_type: "live",
              is_upcoming: true,
              title: "Scheduled",
              published_day: "2026-07-20",
            },
            {
              id: "known-anchor",
              position: 2,
              content_type: "video",
              title: "Known",
              published_day: "2026-07-10",
            },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => videoId === "upcoming-live"
      ? { id: videoId, is_upcoming: true, live_status: "is_upcoming" }
      : detail(videoId, 100),
  });

  assert.equal(result.first_seen_count, 0);
  assert.equal(
    fixture.state.contents.some((row) => row.source_content_id === "upcoming-live"),
    false,
  );
});

test("Video discovery does not persist a live broadcast while it is in progress", async () => {
  const fixture = databaseFixture();
  const fetched = [];
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:active-live",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            {
              id: "active-live",
              position: 1,
              content_type: "live",
              is_live: true,
              title: "Live now",
              published_day: "2026-07-20",
            },
            {
              id: "known-anchor",
              position: 2,
              content_type: "video",
              title: "Known",
              published_day: "2026-07-10",
            },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return videoId === "active-live" ? {
          ...detail(videoId, 100),
          is_live: true,
          live_status: "is_live",
          live_started_at: "2026-07-20T00:00:00.000Z",
        }
        : detail(videoId, 100);
    },
  });

  assert.equal(result.first_seen_count, 0);
  assert.equal(
    fixture.state.contents.some((row) => row.source_content_id === "active-live"),
    false,
  );
  assert.equal(fetched.includes("active-live"), false);
});

test("Video discovery stores an unlisted detail without treating it as public", async () => {
  const fixture = databaseFixture();
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:unlisted-video",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "unlisted-video", position: 1, content_type: "video", published_day: "2026-07-19" },
            { id: "known-anchor", position: 2, content_type: "video", published_day: "2026-07-10" },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => ({
      ...detail(videoId, 100),
      availability: videoId === "unlisted-video" ? "unlisted" : "public",
      access_status: videoId === "unlisted-video" ? "unlisted" : "public",
      playability_status: "OK",
    }),
  });

  assert.equal(result.outcome, "complete");
  assert.equal(
    fixture.state.contents.find((row) => row.source_content_id === "unlisted-video").access_status,
    "unlisted",
  );
});

test("an incomplete Uploads scan does not advance the incremental cursor", async () => {
  const fixture = databaseFixture();
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:incomplete-scan",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "new-1", position: 1, content_type: "video", published_day: "2026-07-20" },
            { id: "new-2", position: 2, content_type: "video", published_day: "2026-07-19" },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: false,
          matched_anchor_id: null,
          stop_reason: "max_items",
          terminal_reason: "max_items",
          complete: false,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => ({
      ...detail(videoId, 100),
      published_at: videoId === "old-video"
        ? "2026-07-09T00:00:00.000Z"
        : "2026-07-19T00:00:00.000Z",
    }),
  });

  assert.equal(result.outcome, "partial");
  assert.equal(fixture.state.cursorUpdates.length, 1);
  assert.equal(fixture.state.cursorUpdates[0].anchorVideoIds, null);
  assert.equal(fixture.state.cursorUpdates[0].sourceCursor, null);

  let secondScanAnchors = null;
  await executeIncrementalVideo({
    plan: {
      ...plan(),
      job_id: "incremental__UCvideo__20260721__clock_8__hash",
      plan_id: "bb4ca796-c784-4c7d-b1cf-7a46a8944575",
      plan_day: "2026-07-21",
      scheduled_at: "2026-07-21T13:25:40.000Z",
    },
    runId: "incremental:incomplete-scan-retry",
    startedAt: "2026-07-21T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads(options) {
        secondScanAnchors = options.anchors;
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "missed-between", position: 1, content_type: "video", published_day: "2026-07-20" },
            { id: "new-1", position: 2, content_type: "video", published_day: "2026-07-20" },
            { id: "known-anchor", position: 3, content_type: "video", published_day: "2026-07-10" },
          ],
          pages: 1,
          item_count: 3,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => ({
      ...detail(videoId, 100),
      published_at: videoId === "old-video"
        ? "2026-07-09T00:00:00.000Z"
        : "2026-07-19T00:00:00.000Z",
    }),
  });

  assert.deepEqual(secondScanAnchors, [
    { id: "known-anchor", published_day: "2026-07-10" },
    { id: "old-video", published_day: "2026-07-09" },
  ]);
});

test("Catch-up exhaustion classifies the latest 30 Videos in one discovery run", async () => {
  const fixture = databaseFixture();
  const fetched = [];
  const scannedEntries = Array.from({ length: 150 }, (_, index) => ({
    id: `catchup-${String(index + 1).padStart(3, "0")}`,
    position: index + 1,
    content_type: "video",
    published_day: "2026-07-20",
  }));

  const executeAttempt = (runId, jobId) => executeIncrementalVideo({
    plan: { ...plan(), job_id: jobId },
    runId,
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: scannedEntries,
          pages: 3,
          first_page_item_count: 100,
          catch_up_item_count: 50,
          item_count: 150,
          parse_gap_count: 0,
          anchor_matched: false,
          matched_anchor_id: null,
          stop_reason: "catchup_limit",
          terminal_reason: "catchup_limit",
          complete: false,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return detail(videoId, 100);
    },
  });

  const result = await executeAttempt(
    "incremental:catchup-latest-30",
    "incremental__UCvideo__20260720__clock_7__latest-30",
  );
  assert.equal(result.outcome, "complete");
  assert.equal(result.first_seen_count, 30);
  assert.deepEqual(
    fixture.state.contents
      .filter((row) => row.source_content_id.startsWith("catchup-"))
      .map((row) => row.source_content_id),
    scannedEntries.slice(0, 30).map((entry) => entry.id),
  );
  assert.equal(fetched.some((id) => scannedEntries.slice(30).some((entry) => entry.id === id)), false);
  assert.equal(fixture.state.cursorUpdates.length, 1);
  assert.equal(fixture.state.cursorUpdates[0].outcome, "complete");
  assert.deepEqual(
    fixture.state.cursorUpdates[0].anchorVideoIds,
    scannedEntries.slice(0, 20).map((entry) => entry.id),
  );
  assert.deepEqual(JSON.parse(fixture.state.cursorUpdates[0].sourceCursor), {
    playlist_id: "UUvideo",
    matched_anchor_id: null,
    crossed_anchor_ids: [],
    terminal_reason: "gap_abandoned_latest_30",
    gap_abandonment: {
      policy_version: "latest-30-on-catchup-limit-v1",
      source_stop_reason: "catchup_limit",
      scanned_item_count: 150,
      first_page_item_count: 100,
      catch_up_item_count: 50,
      catch_up_item_limit: 50,
      selected_item_count: 30,
    },
  });
  assert.equal(fixture.state.outbox.length, 1);
  assert.equal(fixture.state.outbox[0].outcome, "complete");
  const discovery = fixture.state.outbox[0].payload.discovery.payload;
  assert.equal(discovery.stop_reason, "gap_abandoned_latest_30");
  assert.equal(discovery.items, 30);
  assert.deepEqual(discovery.gap_abandonment, {
    policy_version: "latest-30-on-catchup-limit-v1",
    source_stop_reason: "catchup_limit",
    scanned_item_count: 150,
    first_page_item_count: 100,
    catch_up_item_count: 50,
    catch_up_item_limit: 50,
    selected_item_count: 30,
    scanned_video_ids: scannedEntries.map((entry) => entry.id),
    selected_video_ids: scannedEntries.slice(0, 30).map((entry) => entry.id),
    abandoned_anchor_ids: ["known-anchor", "old-video"],
  });
});

test("Catch-up exhaustion with parse gaps remains Partial and has no Content side effects", async () => {
  const fixture = databaseFixture();
  const initialContents = structuredClone(fixture.state.contents);
  const fetched = [];
  const scannedEntries = Array.from({ length: 150 }, (_, index) => ({
    id: `parse-gap-${index + 1}`,
    position: index + 1,
    content_type: "video",
    published_day: "2026-07-20",
  }));

  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:catchup-parse-gap",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: scannedEntries,
          pages: 3,
          first_page_item_count: 100,
          catch_up_item_count: 50,
          item_count: 150,
          parse_gap_count: 1,
          anchor_matched: false,
          matched_anchor_id: null,
          stop_reason: "catchup_limit",
          terminal_reason: "catchup_limit",
          complete: false,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return detail(videoId, 100);
    },
  });

  assert.equal(result.outcome, "partial");
  assert.deepEqual(fetched, []);
  assert.deepEqual(fixture.state.contents, initialContents);
  assert.equal(fixture.state.cursorUpdates[0].anchorVideoIds, null);
  assert.equal(fixture.state.cursorUpdates[0].sourceCursor, null);
  assert.equal(fixture.state.outbox[0].payload.discovery.payload.stop_reason, "catchup_limit");
});

test("Catch-up cannot abandon a gap before the configured budget is exhausted", async () => {
  const fixture = databaseFixture();
  const initialContents = structuredClone(fixture.state.contents);
  const fetched = [];
  const scannedEntries = Array.from({ length: 149 }, (_, index) => ({
    id: `early-stop-${index + 1}`,
    position: index + 1,
    content_type: "video",
    published_day: "2026-07-20",
  }));

  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:catchup-before-limit",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: scannedEntries,
          pages: 3,
          first_page_item_count: 100,
          catch_up_item_count: 49,
          item_count: 149,
          parse_gap_count: 0,
          anchor_matched: false,
          matched_anchor_id: null,
          stop_reason: "catchup_limit",
          terminal_reason: "catchup_limit",
          complete: false,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return detail(videoId, 100);
    },
  });

  assert.equal(result.outcome, "partial");
  assert.deepEqual(fetched, []);
  assert.deepEqual(fixture.state.contents, initialContents);
  assert.equal(fixture.state.enrichPending.size, 0);
  assert.equal(fixture.state.cursorUpdates[0].anchorVideoIds, null);
  assert.equal(fixture.state.cursorUpdates[0].sourceCursor, null);
  assert.equal(fixture.state.outbox[0].payload.discovery.payload.stop_reason, "catchup_limit");
});

test("Video discovery does not invent a video type when Watch detail collection fails", async () => {
  const fixture = databaseFixture();
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:publish-day-fallback",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            {
              id: "new-video",
              position: 1,
              content_type: null,
              title: "New",
              published_day: "2026-07-19",
            },
            {
              id: "known-anchor",
              position: 2,
              content_type: "video",
              title: "Known",
              published_day: "2026-07-10",
            },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async () => {
      throw new Error("detail unavailable");
    },
  });

  assert.equal(result.outcome, "partial");
  assert.equal(result.first_seen_count, 0);
  assert.equal(
    fixture.state.contents.some((row) => row.source_content_id === "new-video"),
    false,
  );
  assert.deepEqual(
    fixture.state.outbox[0].payload.discovery.payload.unresolved_video_ids,
    ["new-video"],
  );
  assert.equal(fixture.state.cursorUpdates[0].sourceCursor, null);
});

test("Video detail route challenges abort the cycle before a Partial Observation is committed", async () => {
  const fixture = databaseFixture();
  const challenge = Object.assign(
    new Error("YouTube bot challenge HTTP 200 for /youtubei/v1/player"),
    {
      youtube_failure_evidence: {
        status: 200,
        body: "Sign in to confirm you're not a bot",
        source: "youtubejs_player",
        target_url: "https://www.youtube.com/youtubei/v1/player",
        client: "WEB",
      },
    },
  );

  await assert.rejects(
    executeIncrementalVideo({
      plan: plan(),
      runId: "incremental:route-challenge",
      startedAt: "2026-07-20T00:00:00.000Z",
      query: fixture.query,
      withTransaction: fixture.withTransaction,
      getChannelSnapshot: async () => ({
        async scanUploads() {
          return {
            playlist_id: "UUvideo",
            entries: [
              {
                id: "challenged-video",
                position: 1,
                content_type: null,
                title: "Challenged",
                published_day: "2026-07-19",
              },
              {
                id: "known-anchor",
                position: 2,
                content_type: "video",
                title: "Known",
                published_day: "2026-07-10",
              },
            ],
            pages: 1,
            item_count: 2,
            parse_gap_count: 0,
            anchor_matched: true,
            matched_anchor_id: "known-anchor",
            stop_reason: "anchor_matched",
            terminal_reason: "anchor_matched",
            complete: true,
            raw: { engine: "youtubei.js@test" },
          };
        },
      }),
      fetchDetail: async () => {
        throw challenge;
      },
    }),
    (error) => error === challenge,
  );

  assert.equal(fixture.state.outbox.length, 0);
  assert.equal(fixture.state.cursorUpdates.length, 0);
});
