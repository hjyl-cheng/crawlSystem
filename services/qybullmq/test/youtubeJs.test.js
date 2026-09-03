import assert from "node:assert/strict";
import test from "node:test";
import {
  collectYoutubeJsUploadBundle,
  isYoutubeJsBotChallenge,
  normalizeYoutubeJsFeedItem,
  normalizeYoutubeJsVideoInfo,
  parseYoutubeJsCount,
  scanYoutubeJsFeed,
  youtubeJsAboutLinkObservation,
  youtubeJsAboutLinks,
  youtubeJsChannelMetadata,
  youtubeJsChannelVerification,
  youtubeJsDetailEnabled,
} from "../src/youtubeJs.js";
import { isParserContractError } from "../src/localizedParsing.js";

function infoFixture(values = {}) {
  return {
    page: [{
      microformat: {
        publish_date: "2026-07-10T00:00:09Z",
        upload_date: "2026-07-10",
        length_seconds: 2111,
        view_count: 12383,
        thumbnails: [{ url: "https://i.ytimg.com/vi/example/maxresdefault.jpg", width: 1280, height: 720 }],
        channel: { id: "UCexample" },
      },
    }],
    basic_info: {
      id: "Cg4FvuCLtxg",
      channel_id: "UCexample",
      title: "Example",
      short_description: "Body #Roblox #\u8bdd\u9898",
      tags: ["Roblox Game", "Tutorial"],
      duration: 2111,
      view_count: 12383,
      like_count: 1487,
      is_live: false,
      is_live_content: false,
      is_upcoming: false,
      ...values.basic_info,
    },
    playability_status: { status: "OK", ...values.playability_status },
    comments_entry_point_header: Object.prototype.hasOwnProperty.call(values, "comments_entry_point_header")
      ? values.comments_entry_point_header
      : { comment_count: "87 comments" },
  };
}

test("video details default to yt-dlp while YouTube.js remains available for channels", () => {
  const previous = process.env.YOUTUBEJS_EXTRACTOR_MODE;
  delete process.env.YOUTUBEJS_EXTRACTOR_MODE;
  try {
    assert.equal(youtubeJsDetailEnabled(), false);
  } finally {
    if (previous === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE;
    else process.env.YOUTUBEJS_EXTRACTOR_MODE = previous;
  }
});

test("Channel verification uses an observed PageHeader attachment without guessing", () => {
  assert.deepEqual(youtubeJsChannelVerification({
    content: { title: { text: { runs: [{ text: "Verified", attachment: {} }] } } },
  }), { value: true, status: "verified" });
  assert.deepEqual(youtubeJsChannelVerification({
    content: { title: { text: { runs: [{ text: "Ordinary channel" }] } } },
  }), { value: false, status: "not_verified" });
  assert.deepEqual(youtubeJsChannelVerification({}), { value: null, status: "unknown" });
});

function channelMetadataFixture({
  headerSubscribers = null,
  metadataParts = [],
  subscribeButtonSubscribers = null,
} = {}) {
  return {
    metadata: {
      external_id: "UCoDbwUudtB0QTXS66v1zo-w",
      title: "Olkabone",
      vanity_channel_url: "https://www.youtube.com/@Olkabone",
    },
    header: {
      subscribers: headerSubscribers,
      content: {
        metadata: {
          metadata_rows: [{
            metadata_parts: metadataParts.map((text) => ({ text })),
          }],
        },
      },
    },
    subscribe_button: {
      subscribers: subscribeButtonSubscribers,
    },
  };
}

test("Channel metadata ignores a Handle-shaped subscriber field and uses the labelled count", () => {
  const metadata = youtubeJsChannelMetadata(
    "UCoDbwUudtB0QTXS66v1zo-w",
    channelMetadataFixture({
      headerSubscribers: "@Olkabone",
      metadataParts: ["@Olkabone", "1.35M subscribers", "2K videos"],
    }),
    null,
    { strictSubscriberCount: false },
  );

  assert.equal(metadata.handle, "@Olkabone");
  assert.equal(metadata.subscriber_count, 1_350_000);
  assert.equal(metadata.subscriber_count_text, "1.35M subscribers");
  assert.equal(metadata.subscriber_count_source, "youtube_channel_header");
});

test("Video-only Channel metadata treats a lone Handle as no subscriber observation", () => {
  const metadata = youtubeJsChannelMetadata(
    "UCoDbwUudtB0QTXS66v1zo-w",
    channelMetadataFixture({
      headerSubscribers: "@Olkabone",
      metadataParts: ["@Olkabone", "2K videos"],
    }),
    null,
    { strictSubscriberCount: false },
  );

  assert.equal(metadata.subscriber_count, null);
  assert.equal(metadata.subscriber_count_text, null);
  assert.equal(metadata.subscriber_count_source, null);
});

test("Video count text cannot become a Channel subscriber count", () => {
  const metadata = youtubeJsChannelMetadata(
    "UCvideosOnly",
    channelMetadataFixture({
      headerSubscribers: "341 videos",
      metadataParts: ["@videosOnly", "341 videos"],
    }),
    null,
    { strictSubscriberCount: false },
  );

  assert.equal(metadata.subscriber_count, null);
  assert.equal(metadata.subscriber_count_text, null);
  assert.equal(metadata.subscriber_count_source, null);
  assert.equal(metadata.video_count_text, "341 videos");
});

test("About subscriber values remain strict when the About domain is requested", () => {
  assert.throws(
    () => youtubeJsChannelMetadata(
      "UCstrict",
      channelMetadataFixture({ headerSubscribers: "@strict" }),
      { metadata: { subscriber_count: "subscribers hidden in a new format" } },
      { strictSubscriberCount: true },
    ),
    (error) => isParserContractError(error),
  );
});

test("About links retain the parsed Text endpoint as the real target", () => {
  const links = youtubeJsAboutLinks({
    links: [{
      title: { toString: () => "Website" },
      link: {
        toString: () => "example.com",
        endpoint: {
          metadata: {
            url: "/redirect?q=https%3A%2F%2Fexample.com%2Fcontact",
          },
        },
      },
      favicon: [{ url: "https://example.com/favicon.png", width: 32, height: 32 }],
    }],
  });
  assert.equal(links[0].display_url, "example.com");
  assert.equal(
    links[0].target_url,
    "https://www.youtube.com/redirect?q=https%3A%2F%2Fexample.com%2Fcontact",
  );
  assert.equal(links[0].url, links[0].target_url);
});

test("a successful About surface distinguishes an empty Link collection from a failed request", () => {
  assert.deepEqual(youtubeJsAboutLinkObservation({ metadata: {} }), {
    links: [],
    status: "observed",
  });
  assert.deepEqual(youtubeJsAboutLinkObservation(null), {
    links: [],
    status: "unresolved",
  });
});

function uploadFixture(id, path, values = {}) {
  return {
    id,
    title: values.title || id,
    endpoint: { metadata: { url: path } },
    duration: { seconds: values.duration ?? 30 },
    is_live: values.is_live ?? false,
    is_upcoming: values.is_upcoming ?? false,
    published: values.published ?? null,
  };
}

test("Uploads preserves only explicit type evidence without requesting content tabs", async () => {
  let playlistRequests = 0;
  let shortsRequests = 0;
  const client = {
    async getPlaylist(playlistId) {
      playlistRequests += 1;
      assert.equal(playlistId, "UUexample");
      return {
        items: [
          uploadFixture("short-id", "/watch?v=short-id", { published: "2 days ago" }),
          uploadFixture("video-id", "/watch?v=video-id", { duration: 600 }),
          uploadFixture("live-id", "/watch?v=live-id", { is_live: true }),
        ],
        has_continuation: false,
      };
    },
  };
  const fetchShorts = async () => {
    shortsRequests += 1;
    return {
      items: [uploadFixture("short-id", "/shorts/short-id")],
      has_continuation: false,
    };
  };

  const bundle = await collectYoutubeJsUploadBundle(client, "UCexample", 30, {
    hasShorts: true,
    fetchShorts,
    locale: "en",
    now: Date.parse("2026-07-23T12:00:00Z"),
  });

  assert.equal(playlistRequests, 1);
  assert.equal(shortsRequests, 0);
  assert.deepEqual(bundle.entries.map((entry) => entry.content_type), [null, null, "live"]);
  assert.deepEqual(bundle.entries.map((entry) => entry.type_source), [
    null,
    null,
    "youtube_uploads_live_flag",
  ]);
  assert.equal(bundle.entries[0].source_url, "https://www.youtube.com/watch?v=short-id");
  assert.equal(bundle.entries[0].published_at, "2026-07-21");
  assert.equal(bundle.entries[0].published_at_status, "relative");
  assert.equal(bundle.entries[0].published_at_precision, "date_only");
  assert.equal(bundle.entries[0].published_at_source, "youtube_uploads_relative_time");
  assert.equal(bundle.activityEvidenceComplete, true);
  assert.equal(bundle.uploads.stop_reason, "list_end");
  assert.equal(bundle.uploads.complete, true);
  assert.deepEqual(bundle.contentTypeCounts, { video: 0, short: 0, live: 1, unresolved: 2 });
});

test("Uploads parse gaps disable the migration activity short-circuit", async () => {
  const client = {
    async getPlaylist() {
      return {
        items: [
          { title: "missing id" },
          uploadFixture("old", "/watch?v=old", { published: "4 months ago" }),
        ],
        has_continuation: false,
      };
    },
  };
  const bundle = await collectYoutubeJsUploadBundle(client, "UCgap", 30, {
    locale: "en",
    now: Date.parse("2026-07-23T12:00:00Z"),
  });
  assert.equal(bundle.entries[0].published_at, "2026-03-25");
  assert.equal(bundle.uploads.parse_gap_count, 1);
  assert.equal(bundle.uploads.stop_reason, "parse_gap");
  assert.equal(bundle.uploads.terminal_reason, "list_end");
  assert.equal(bundle.activityEvidenceComplete, false);
});

test("Uploads records an explicit item-cap boundary when continuation remains", async () => {
  const client = {
    async getPlaylist() {
      return {
        items: [
          uploadFixture("video-1", "/watch?v=video-1"),
          uploadFixture("video-2", "/watch?v=video-2"),
          uploadFixture("video-3", "/watch?v=video-3"),
        ],
        has_continuation: true,
      };
    },
  };
  const bundle = await collectYoutubeJsUploadBundle(client, "UCcapped", 2);

  assert.equal(bundle.entries.length, 2);
  assert.equal(bundle.uploads.stop_reason, "max_items");
  assert.equal(bundle.uploads.complete, false);
});

test("Uploads recognizes a reel endpoint as a Shorts URL fallback", () => {
  const entry = normalizeYoutubeJsFeedItem({
    id: "reel-id",
    endpoint: { name: "reelWatchEndpoint", metadata: {} },
  });
  assert.equal(entry.url, "https://www.youtube.com/shorts/reel-id");
  assert.equal(entry.content_type, "short");
});

test("feed items retain their Video ID while only explicit Upload signals set a type", () => {
  const entries = [
    normalizeYoutubeJsFeedItem({
      video_id: "video-id-01",
      endpoint: { metadata: { url: "/watch?v=video-id-01" } },
    }),
    normalizeYoutubeJsFeedItem({
      id: "short-id-01",
      endpoint: { name: "reelWatchEndpoint", metadata: {} },
    }),
    normalizeYoutubeJsFeedItem({
      content_id: "live-id-001",
      endpoint: { metadata: { url: "/watch?v=live-id-001" } },
      is_live: true,
    }),
  ];

  assert.deepEqual(entries.map((entry) => entry.id), [
    "video-id-01",
    "short-id-01",
    "live-id-001",
  ]);
  assert.deepEqual(entries.map((entry) => entry.content_type), [
    null,
    "short",
    "live",
  ]);
});

test("Uploads fails loudly when a channel exposes content but the playlist is empty", async () => {
  const client = {
    async getPlaylist() {
      return { items: [], has_continuation: false };
    },
  };
  await assert.rejects(
    collectYoutubeJsUploadBundle(client, "UCempty", 30),
    /uploads playlist UUempty was empty/,
  );
});

test("incremental Upload scan continues until a database Anchor is found", async () => {
  let continuationCalls = 0;
  const second = {
    videos: [
      uploadFixture("new-3", "/watch?v=new-3", { published: "2026-07-11" }),
      uploadFixture("known-anchor", "/watch?v=known-anchor", { published: "2026-07-10" }),
      uploadFixture("older", "/watch?v=older", { published: "2026-07-09" }),
    ],
    has_continuation: true,
  };
  const first = {
    videos: [
      uploadFixture("new-1", "/watch?v=new-1", { published: "2026-07-13" }),
      uploadFixture("new-2", "/watch?v=new-2", { published: "2026-07-12" }),
    ],
    has_continuation: true,
    async getContinuation() { continuationCalls += 1; return second; },
  };

  const scan = await scanYoutubeJsFeed(first, {
    anchors: [{ id: "known-anchor", published_day: "2026-07-10" }],
    maxPages: 10,
    maxItems: 100,
    now: Date.parse("2026-07-20T00:00:00.000Z"),
  });
  assert.equal(continuationCalls, 1);
  assert.deepEqual(scan.entries.map((item) => item.id), [
    "new-1",
    "new-2",
    "new-3",
    "known-anchor",
  ]);
  assert.equal(scan.matched_anchor_id, "known-anchor");
  assert.equal(scan.complete, true);
  assert.equal(scan.stop_reason, "anchor_matched");
});

test("incremental Upload scan enters Catch-up after checking the complete first page", async () => {
  const third = {
    videos: [uploadFixture("fallback-anchor", "/watch?v=fallback-anchor")],
    has_continuation: true,
  };
  const second = {
    videos: [
      uploadFixture("catch-up-1", "/watch?v=catch-up-1"),
      uploadFixture("catch-up-2", "/watch?v=catch-up-2"),
    ],
    has_continuation: true,
    async getContinuation() { return third; },
  };
  const first = {
    videos: [
      uploadFixture("new-1", "/watch?v=new-1"),
      uploadFixture("new-2", "/watch?v=new-2"),
    ],
    has_continuation: true,
    async getContinuation() { return second; },
  };

  const scan = await scanYoutubeJsFeed(first, {
    anchors: [{ id: "fallback-anchor", published_day: "2026-07-10" }],
    catchUpMaxItems: 50,
  });

  assert.equal(scan.matched_anchor_id, "fallback-anchor");
  assert.equal(scan.complete, true);
  assert.equal(scan.pages, 3);
  assert.equal(scan.first_page_item_count, 2);
  assert.equal(scan.catch_up_item_count, 3);
});

test("incremental Upload Catch-up stops before an Anchor beyond its 50-ID budget", async () => {
  const catchUpEntries = Array.from({ length: 51 }, (_, index) => {
    const position = index + 1;
    const id = position === 51 ? "too-deep-anchor" : `catch-up-${position}`;
    return uploadFixture(id, `/watch?v=${id}`);
  });
  const first = {
    videos: [
      uploadFixture("new-1", "/watch?v=new-1"),
      uploadFixture("new-2", "/watch?v=new-2"),
    ],
    has_continuation: true,
    async getContinuation() {
      return { videos: catchUpEntries, has_continuation: true };
    },
  };

  const scan = await scanYoutubeJsFeed(first, {
    anchors: [{ id: "too-deep-anchor", published_day: "2026-07-10" }],
    catchUpMaxItems: 50,
  });

  assert.equal(scan.matched_anchor_id, null);
  assert.equal(scan.complete, false);
  assert.equal(scan.stop_reason, "catchup_limit");
  assert.equal(scan.first_page_item_count, 2);
  assert.equal(scan.catch_up_item_count, 50);
  assert.equal(scan.item_count, 52);
});

test("incremental Upload Catch-up accepts an Anchor at its 50th ID", async () => {
  const catchUpEntries = Array.from({ length: 50 }, (_, index) => {
    const position = index + 1;
    const id = position === 50 ? "boundary-anchor" : `catch-up-${position}`;
    return uploadFixture(id, `/watch?v=${id}`);
  });
  const scan = await scanYoutubeJsFeed({
    videos: [uploadFixture("new-1", "/watch?v=new-1")],
    has_continuation: true,
    async getContinuation() {
      return { videos: catchUpEntries, has_continuation: true };
    },
  }, {
    anchors: [{ id: "boundary-anchor", published_day: "2026-07-10" }],
    catchUpMaxItems: 50,
  });

  assert.equal(scan.matched_anchor_id, "boundary-anchor");
  assert.equal(scan.complete, true);
  assert.equal(scan.stop_reason, "anchor_matched");
  assert.equal(scan.catch_up_item_count, 50);
});

test("incremental Upload scan abandons V30 only after crossing its publication day", async () => {
  let continuationCalls = 0;
  const second = {
    videos: [
      uploadFixture("between", "/watch?v=between", { published: "2026-07-09" }),
      uploadFixture("V29", "/watch?v=V29", { published: "2026-07-08" }),
      uploadFixture("older", "/watch?v=older", { published: "2026-07-07" }),
    ],
    has_continuation: true,
  };
  const first = {
    videos: [
      uploadFixture("V31", "/watch?v=V31", { published: "2026-07-12" }),
      uploadFixture("same-day", "/watch?v=same-day", { published: "2026-07-10" }),
    ],
    has_continuation: true,
    async getContinuation() { continuationCalls += 1; return second; },
  };

  const scan = await scanYoutubeJsFeed(first, {
    anchors: [
      { id: "V30", published_day: "2026-07-10" },
      { id: "V29", published_day: "2026-07-08" },
    ],
    now: Date.parse("2026-07-20T00:00:00.000Z"),
  });

  assert.equal(continuationCalls, 1);
  assert.equal(scan.matched_anchor_id, "V29");
  assert.deepEqual(scan.crossed_anchor_ids, ["V30"]);
  assert.deepEqual(scan.entries.map((item) => item.id), ["V31", "same-day", "between", "V29"]);
  assert.equal(scan.complete, true);
});

test("incremental Upload scan reuses a same-day V29 only after V30 is crossed", async () => {
  const scan = await scanYoutubeJsFeed({
    videos: [
      uploadFixture("V31", "/watch?v=V31", { published: "2026-07-11" }),
      uploadFixture("V29", "/watch?v=V29", { published: "2026-07-10" }),
      uploadFixture("boundary", "/watch?v=boundary", { published: "2026-07-09" }),
    ],
    has_continuation: true,
  }, {
    anchors: [
      { id: "V30", published_day: "2026-07-10" },
      { id: "V29", published_day: "2026-07-10" },
    ],
    now: Date.parse("2026-07-20T00:00:00.000Z"),
  });

  assert.equal(scan.matched_anchor_id, "V29");
  assert.deepEqual(scan.crossed_anchor_ids, ["V30"]);
  assert.deepEqual(scan.entries.map((item) => item.id), ["V31", "V29"]);
  assert.equal(scan.stop_reason, "anchor_matched");
  assert.equal(scan.complete, true);
});

test("incremental Upload scan matches a fallback Anchor when feed dates are absent", async () => {
  const scan = await scanYoutubeJsFeed({
    videos: [
      uploadFixture("new-1", "/watch?v=new-1"),
      uploadFixture("V29", "/watch?v=V29"),
      uploadFixture("older", "/watch?v=older"),
    ],
    has_continuation: true,
  }, {
    anchors: [
      { id: "V30", published_day: "2026-07-10" },
      { id: "V29", published_day: "2026-07-09" },
    ],
    maxItems: 2,
  });

  assert.equal(scan.matched_anchor_id, "V29");
  assert.deepEqual(scan.crossed_anchor_ids, ["V30"]);
  assert.deepEqual(scan.entries.map((item) => item.id), ["new-1", "V29"]);
  assert.equal(scan.stop_reason, "anchor_matched");
  assert.equal(scan.complete, true);
});

test("incremental Upload scan can match every one of its 20 backup Anchors", async () => {
  const anchors = Array.from({ length: 20 }, (_, index) => ({
    id: `anchor-${String(index + 1).padStart(2, "0")}`,
    published_day: `2026-07-${String(20 - index).padStart(2, "0")}`,
  }));

  for (const [matchedIndex, anchor] of anchors.entries()) {
    const scan = await scanYoutubeJsFeed({
      videos: [
        uploadFixture("new-item", "/watch?v=new-item"),
        uploadFixture(anchor.id, `/watch?v=${anchor.id}`),
      ],
      has_continuation: true,
    }, { anchors, maxItems: 2 });

    assert.equal(scan.matched_anchor_id, anchor.id, `backup ${matchedIndex + 1}`);
    assert.deepEqual(
      scan.crossed_anchor_ids,
      anchors.slice(0, matchedIndex).map((item) => item.id),
      `backup ${matchedIndex + 1}`,
    );
    assert.equal(scan.stop_reason, "anchor_matched", `backup ${matchedIndex + 1}`);
    assert.equal(scan.complete, true, `backup ${matchedIndex + 1}`);
  }
});

test("incremental Upload scan never advances when Catch-up cannot find any backup Anchor", async () => {
  const anchors = Array.from({ length: 20 }, (_, index) => ({
    id: `anchor-${String(index + 1).padStart(2, "0")}`,
    published_day: `2026-07-${String(20 - index).padStart(2, "0")}`,
  }));
  const scan = await scanYoutubeJsFeed({
    videos: [
      uploadFixture("unknown-1", "/watch?v=unknown-1", { published: "2026-06-30" }),
      uploadFixture("unknown-2", "/shorts/unknown-2", { published: "2026-06-29" }),
      uploadFixture("unknown-3", "/watch?v=unknown-3", {
        is_live: true,
        published: "2026-06-28",
      }),
    ],
    has_continuation: true,
    async getContinuation() {
      return {
        videos: Array.from({ length: 50 }, (_, index) => {
          const id = `catch-up-unknown-${index + 1}`;
          return uploadFixture(id, `/watch?v=${id}`);
        }),
        has_continuation: true,
      };
    },
  }, { anchors, catchUpMaxItems: 50 });

  assert.equal(scan.matched_anchor_id, null);
  assert.deepEqual(scan.crossed_anchor_ids, []);
  assert.equal(scan.stop_reason, "catchup_limit");
  assert.equal(scan.catch_up_item_count, 50);
  assert.equal(scan.complete, false);
});

test("incremental Upload scan keeps looking for an exact ID after publication dates cross Anchors", async () => {
  let continuationCalls = 0;
  const scan = await scanYoutubeJsFeed({
    videos: [
      uploadFixture("V31", "/watch?v=V31", { published: "2026-07-11" }),
      uploadFixture("boundary", "/watch?v=boundary", { published: "2026-07-07" }),
    ],
    has_continuation: true,
    async getContinuation() {
      continuationCalls += 1;
      return {
        videos: [
          uploadFixture("V29", "/watch?v=V29", { published: "2026-07-08" }),
          uploadFixture("older", "/watch?v=older", { published: "2026-07-06" }),
        ],
        has_continuation: true,
      };
    },
  }, {
    anchors: [
      { id: "V30", published_day: "2026-07-10" },
      { id: "V29", published_day: "2026-07-08" },
    ],
    now: Date.parse("2026-07-20T00:00:00.000Z"),
  });

  assert.equal(continuationCalls, 1);
  assert.deepEqual(scan.entries.map((item) => item.id), ["V31", "boundary", "V29"]);
  assert.deepEqual(scan.crossed_anchor_ids, ["V30"]);
  assert.equal(scan.matched_anchor_id, "V29");
  assert.equal(scan.stop_reason, "anchor_matched");
  assert.equal(scan.complete, true);
});

test("incremental Upload scan checks the complete first page before applying its Catch-up cap", async () => {
  const scan = await scanYoutubeJsFeed({
    videos: [
      uploadFixture("new-1", "/watch?v=new-1"),
      uploadFixture("new-2", "/watch?v=new-2"),
      uploadFixture("known-anchor", "/watch?v=known-anchor"),
    ],
    has_continuation: true,
  }, {
    anchors: [{ id: "known-anchor", published_day: "2026-07-10" }],
    maxPages: 10,
    catchUpMaxItems: 2,
  });
  assert.deepEqual(scan.entries.map((item) => item.id), ["new-1", "new-2", "known-anchor"]);
  assert.equal(scan.complete, true);
  assert.equal(scan.stop_reason, "anchor_matched");
});

test("incremental Upload scan treats a parse gap as Partial coverage", async () => {
  const scan = await scanYoutubeJsFeed({
    videos: [{ title: "missing id" }, uploadFixture("known-anchor", "/watch?v=known-anchor")],
    has_continuation: false,
  }, { anchors: [{ id: "known-anchor", published_day: "2026-07-10" }] });
  assert.equal(scan.anchor_matched, true);
  assert.equal(scan.complete, false);
  assert.equal(scan.stop_reason, "parse_gap");
  assert.equal(scan.parse_gap_count, 1);
});

test("parseYoutubeJsCount handles exact and localized abbreviated counts", () => {
  assert.equal(parseYoutubeJsCount("1,234 views"), 1234);
  assert.equal(parseYoutubeJsCount("1,2 mil visualizacoes"), 1200);
  assert.equal(parseYoutubeJsCount("2.5M views"), 2_500_000);
  assert.equal(parseYoutubeJsCount(0), 0);
  assert.equal(parseYoutubeJsCount("No views"), null);
});

test("normalizeYoutubeJsVideoInfo preserves second timestamps and exact engagement", () => {
  const detail = normalizeYoutubeJsVideoInfo(infoFixture());
  assert.equal(detail.published_at, "2026-07-10T00:00:09.000Z");
  assert.equal(detail.published_at_status, "exact");
  assert.equal(detail.published_at_precision, "second");
  assert.equal(detail.duration_seconds, 2111);
  assert.equal(detail.duration_source, "youtubejs_player");
  assert.equal(detail.view_count, 12383);
  assert.equal(detail.view_count_text, "12383");
  assert.equal(detail.view_count_status, "exact");
  assert.equal(detail.like_count, 1487);
  assert.equal(detail.comment_count, 87);
  assert.equal(detail.comments_disabled, false);
  assert.equal(detail.availability, "public");
  assert.equal(detail.description, "Body #Roblox #\u8bdd\u9898");
  assert.equal(detail.description_status, "exact");
  assert.equal(detail.description_source, "youtubejs_player");
  assert.deepEqual(detail.hashtags, ["#Roblox", "#\u8bdd\u9898"]);
  assert.equal(detail.hashtags_observed, true);
  assert.deepEqual(detail.keywords, ["Roblox Game", "Tutorial"]);
  assert.equal(detail.keywords_observed, true);
  assert.match(detail.thumbnail_url, /maxresdefault/);
});

test("normalizeYoutubeJsVideoInfo treats an empty Short description as resolved", () => {
  const detail = normalizeYoutubeJsVideoInfo(infoFixture({ basic_info: {
    title: "Short #RobloxShorts",
    short_description: "",
  } }));
  assert.equal(detail.description, "");
  assert.equal(detail.description_status, "empty");
  assert.deepEqual(detail.hashtags, ["#RobloxShorts"]);
});

test("normalizeYoutubeJsVideoInfo keeps missing comments unresolved", () => {
  const info = infoFixture({ comments_entry_point_header: null });
  const unresolved = normalizeYoutubeJsVideoInfo(info);
  assert.equal(unresolved.comment_count, null);
  assert.equal(unresolved.comments_disabled, null);
  assert.equal(unresolved.comment_count_status, "unresolved");
  assert.equal(unresolved.comments_first_page, null);

  const emptySurface = normalizeYoutubeJsVideoInfo(info, { header: {}, contents: [] });
  assert.equal(emptySurface.comment_count, 0);
  assert.equal(emptySurface.comments_disabled, false);
  assert.equal(emptySurface.comment_count_status, "zero_from_surface");
  assert.equal(emptySurface.comments_first_page.returned_count, 0);
  assert.deepEqual(emptySurface.comments_first_page.comments, []);

  const disabled = normalizeYoutubeJsVideoInfo(info, {
    contents: { message: "Comments are turned off." },
  });
  assert.equal(disabled.comments_disabled, true);
  assert.equal(disabled.comment_count_status, "disabled");
  assert.equal(disabled.comment_count, 0);
});

test("normalizeYoutubeJsVideoInfo keeps a stored first comment page", () => {
  const page = {
    version: 1,
    collected_at: "2026-08-17T12:00:00.000Z",
    sort: "TOP_COMMENTS",
    total_count: 12,
    returned_count: 1,
    comments: [{
      comment_id: "Ugxd1",
      position: 1,
      text: "First comment",
    }],
  };
  const detail = normalizeYoutubeJsVideoInfo(infoFixture({
    comments_entry_point_header: null,
  }), page);
  assert.equal(detail.comment_count, 12);
  assert.equal(detail.comment_count_source, "youtubejs_comments");
  assert.equal(detail.comments_first_page, page);
});

test("a header count without first-page comments stays unresolved instead of becoming zero", () => {
  const detail = normalizeYoutubeJsVideoInfo(infoFixture({
    comments_entry_point_header: null,
  }), {
    version: 1,
    collected_at: "2026-08-17T12:00:00.000Z",
    sort: "TOP_COMMENTS",
    total_count: 12,
    returned_count: 0,
    comments: [],
  });
  assert.equal(detail.comment_count, 12);
  assert.equal(detail.comment_count_status, "unresolved");
  assert.equal(detail.comments_disabled, false);
});

test("normalizeYoutubeJsVideoInfo maps getComments header and contents into comments_first_page", () => {
  const detail = normalizeYoutubeJsVideoInfo(infoFixture({
    comments_entry_point_header: null,
  }), {
    header: { count: { text: "12 Comments" }, comments_count: { text: "12" } },
    contents: [{
      type: "CommentThread",
      comment: {
        comment_id: "Ugxd1",
        content: { text: "First comment" },
        published_time: "2 days ago",
        like_count: "3",
        reply_count: "0",
        author: { id: "UCauthor", name: "Viewer" },
      },
    }],
  });
  assert.equal(detail.comment_count, 12);
  assert.equal(detail.comment_count_source, "youtubejs_comments");
  assert.equal(detail.comments_first_page.version, 1);
  assert.equal(detail.comments_first_page.sort, "TOP_COMMENTS");
  assert.equal(detail.comments_first_page.returned_count, 1);
  assert.equal(detail.comments_first_page.comments[0].text, "First comment");
});

test("an empty comments endpoint resolves an age-gated video as comments disabled", () => {
  const info = infoFixture({
    basic_info: { is_family_safe: false },
    comments_entry_point_header: null,
    playability_status: { status: "LOGIN_REQUIRED", reason: "Sign in to confirm your age" },
  });
  const detail = normalizeYoutubeJsVideoInfo(info, null, {
    commentsError: "Comments page did not have any content.",
  });
  assert.equal(detail.comment_count, 0);
  assert.equal(detail.comments_disabled, true);
  assert.equal(detail.comment_count_status, "disabled");
  assert.equal(detail.comments_status_source, "youtubejs_comments_age_gate_empty");
});

test("an empty comments endpoint is not enough to mark an ordinary unavailable video disabled", () => {
  const info = infoFixture({
    comments_entry_point_header: null,
    playability_status: { status: "UNPLAYABLE", reason: "Video unavailable" },
  });
  const detail = normalizeYoutubeJsVideoInfo(info, null, {
    commentsError: "Comments page did not have any content.",
  });
  assert.equal(detail.comments_disabled, null);
  assert.equal(detail.comment_count_status, "unresolved");
});

test("an observed but unsupported localized comment count fails loudly", () => {
  assert.throws(
    () => normalizeYoutubeJsVideoInfo(infoFixture({
      comments_entry_point_header: { comment_count: "new localized count format" },
    })),
    (error) => isParserContractError(error) && error.field === "comment_count",
  );
});

test("normalizeYoutubeJsVideoInfo does not treat zero duration as complete", () => {
  const detail = normalizeYoutubeJsVideoInfo(infoFixture({ basic_info: { duration: 0 } }));
  assert.equal(detail.duration_seconds, null);
  assert.equal(detail.length_text, null);
});

test("normalizeYoutubeJsVideoInfo exposes upcoming and restricted states", () => {
  const detail = normalizeYoutubeJsVideoInfo(infoFixture({
    basic_info: {
      is_upcoming: true,
      is_live_content: true,
      start_timestamp: new Date("2026-07-12T23:00:00Z"),
    },
    playability_status: { status: "LOGIN_REQUIRED", reason: "Members-only content" },
  }));
  assert.equal(detail.live_status, "is_upcoming");
  assert.equal(detail.live_scheduled_at, "2026-07-12T23:00:00.000Z");
  assert.equal(detail.availability, "subscriber_only");
});

test("normalizeYoutubeJsVideoInfo preserves an explicit unlisted access state", () => {
  const info = infoFixture();
  info.page[0].microformat.is_unlisted = true;

  const detail = normalizeYoutubeJsVideoInfo(info);

  assert.equal(detail.is_unlisted, true);
  assert.equal(detail.availability, "unlisted");
});

test("normalizeYoutubeJsVideoInfo retains genuine date-only precision", () => {
  const info = infoFixture();
  info.page[0].microformat.publish_date = "2026-07-10";
  const detail = normalizeYoutubeJsVideoInfo(info);
  assert.equal(detail.published_at, "2026-07-10");
  assert.equal(detail.published_at_status, "exact");
  assert.equal(detail.published_at_precision, "date_only");
});

test("generic WEB playability errors do not override a complete public metadata surface", () => {
  const detail = normalizeYoutubeJsVideoInfo(infoFixture({
    playability_status: { status: "UNPLAYABLE", reason: "Video unavailable" },
  }));
  assert.equal(detail.availability, "public");
});

test("an unsupported non-public playability reason fails instead of becoming unknown", () => {
  const info = infoFixture({
    basic_info: { view_count: null },
    playability_status: { status: "UNPLAYABLE", reason: "알 수 없는 새로운 제한 형식" },
  });
  delete info.page[0].microformat.publish_date;
  delete info.page[0].microformat.upload_date;
  delete info.page[0].microformat.view_count;
  assert.throws(
    () => normalizeYoutubeJsVideoInfo(info),
    (error) => isParserContractError(error),
  );
});

test("an unknown LOGIN_REQUIRED reason fails instead of becoming a generic auth state", () => {
  const info = infoFixture({
    basic_info: { view_count: null },
    playability_status: { status: "LOGIN_REQUIRED", reason: "\uc54c \uc218 \uc5c6\ub294 \uc0c8\ub85c\uc6b4 \ub85c\uadf8\uc778 \uc81c\ud55c" },
  });
  delete info.page[0].microformat.publish_date;
  delete info.page[0].microformat.upload_date;
  delete info.page[0].microformat.view_count;

  assert.throws(
    () => normalizeYoutubeJsVideoInfo(info),
    (error) => isParserContractError(error),
  );
});

test("an unknown LOGIN_REQUIRED reason cannot override a complete public metadata surface", () => {
  const detail = normalizeYoutubeJsVideoInfo(infoFixture({
    playability_status: { status: "LOGIN_REQUIRED", reason: "알 수 없는 새로운 로그인 제한" },
  }));

  assert.equal(detail.availability, "public");
  assert.equal(detail.access_status, "public");
  assert.equal(detail.playability_kind, "inconclusive");
  assert.equal(detail.playability_reason_code, "unsupported_login_required_reason");
});

test("bot challenge detection distinguishes IP blocks from ordinary sign-in restrictions", () => {
  assert.equal(
    isYoutubeJsBotChallenge("LOGIN_REQUIRED", "Faca login para confirmar que voce nao e um robo"),
    true,
  );
  assert.equal(
    isYoutubeJsBotChallenge("LOGIN_REQUIRED", "Faça login para confirmar que você não é um bot"),
    true,
  );
  assert.equal(
    isYoutubeJsBotChallenge("LOGIN_REQUIRED", "Sign in to confirm you're not a bot"),
    true,
  );
  assert.equal(isYoutubeJsBotChallenge("LOGIN_REQUIRED", "Sign in to confirm your age"), false);
  assert.equal(isYoutubeJsBotChallenge("OK", "Sign in to confirm you're not a bot"), false);
});
